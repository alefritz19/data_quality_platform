#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Abschließende Prüfung vor der Visualisierung (Gatekeeper-Schritt).
Führt sämtliche Qualitätsprüfungen auf den aufbereiteten Staging-Daten erneut aus
(stage='final_gate'). Nur Tabellen, die alle Freigabekriterien erfüllen, werden
in das Schema 'mart' und nach 'data/mart/*.parquet' übernommen.
Abweichungen und Sperrgründe werden in 'metadata.quality_log' und
'metadata.mart_gate_log' festgehalten.
"""

import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional
from src.config import AppConfig, load_config
from src.database import get_connection, init_database
from src.quality_checks import run_quality_checks_on_table

MAX_ALLOWED_DEFECT_SHARE_PCT = 50.0


def verify_and_promote_table(
    short_table_name: str,
    run_id: Optional[str] = None,
    config: Optional[AppConfig] = None,
) -> Dict[str, object]:
    """
    Prüft eine Staging-Tabelle erneut auf alle Qualitätsregeln und gibt sie nur
    bei Bestehen für das Schema 'mart' (Visualisierung und Bereitstellung) frei.
    """
    cfg = config or load_config()
    init_database(cfg)
    active_run_id = run_id or datetime.now(timezone.utc).strftime("gate_%Y%m%d_%H%M%S")
    checked_at = datetime.now(timezone.utc).replace(tzinfo=None)

    source_table = f"staging.{short_table_name}"
    mart_table = f"mart.{short_table_name}"

    findings = run_quality_checks_on_table(
        table_name=source_table,
        source_name=short_table_name,
        stage="final_gate",
        run_id=active_run_id,
        config=cfg,
    )

    blocking_reasons: List[str] = []
    total_rows = int(findings[0]["total_count"]) if findings else 0

    if total_rows == 0:
        blocking_reasons.append("Tabelle enthält keine Datenzeilen (0 Zeilen).")

    for item in findings:
        rule = str(item["rule_name"])
        col = str(item["column_name"])
        affected = int(item["affected_count"])
        share = float(item["share_pct"])

        if rule == "pii_pattern_detected" and affected > 0:
            blocking_reasons.append(
                f"Sperrgrund: {affected} unzulässige Muster personenbezogener Daten (PII) in Spalte '{col}'."
            )
        elif rule in ("missing_values", "invalid_type_or_format") and share > MAX_ALLOWED_DEFECT_SHARE_PCT:
            blocking_reasons.append(
                f"Sperrgrund: Spalte '{col}' überschreitet zulässige Fehlerrate ({share:.2f} % > {MAX_ALLOWED_DEFECT_SHARE_PCT:.0f} % bei Regel '{rule}')."
            )
        elif rule == "implausible_negative_values" and share > 20.0:
            blocking_reasons.append(
                f"Sperrgrund: Zu viele unplausible negative Werte ({share:.2f} %) in Spalte '{col}'."
            )

    passed = len(blocking_reasons) == 0
    reasons_text = (
        "Alle Freigabeprüfungen bestanden."
        if passed
        else " | ".join(blocking_reasons)
    )
    gate_id = f"{active_run_id}_{short_table_name}_{uuid.uuid4().hex[:6]}"
    mart_parquet_path = cfg.mart_dir / f"{short_table_name}.parquet"

    conn = get_connection(cfg)
    try:
        if passed:
            escaped_pq = str(mart_parquet_path).replace("'", "''")
            conn.execute(
                f"CREATE OR REPLACE TABLE {mart_table} AS SELECT * FROM {source_table};"
            )
            conn.execute(
                f"COPY {mart_table} TO '{escaped_pq}' (FORMAT PARQUET, COMPRESSION ZSTD);"
            )
        else:
            conn.execute(f"DROP TABLE IF EXISTS {mart_table};")
            if mart_parquet_path.exists():
                mart_parquet_path.unlink()

        conn.execute(
            """
            INSERT INTO metadata.mart_gate_log (
                gate_id, run_id, source_table, mart_table,
                passed, blocking_reasons, total_rows, checked_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """,
            [
                gate_id,
                active_run_id,
                source_table,
                mart_table,
                passed,
                reasons_text,
                total_rows,
                checked_at,
            ],
        )
    finally:
        conn.close()

    return {
        "gate_id": gate_id,
        "run_id": active_run_id,
        "source_table": source_table,
        "mart_table": mart_table,
        "passed": passed,
        "blocking_reasons": reasons_text,
        "total_rows": total_rows,
        "mart_parquet_path": str(mart_parquet_path) if passed else None,
    }


def run_final_gate_all(config: Optional[AppConfig] = None) -> List[Dict[str, object]]:
    """Führt die abschließende Freigabeprüfung für sämtliche Staging-Tabellen aus."""
    cfg = config or load_config()
    init_database(cfg)
    run_id = datetime.now(timezone.utc).strftime("gate_%Y%m%d_%H%M%S")

    conn = get_connection(cfg)
    try:
        staging_tables = [
            row[0]
            for row in conn.execute(
                """
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'staging';
                """
            ).fetchall()
        ]
    finally:
        conn.close()

    results: List[Dict[str, object]] = []
    for tbl in sorted(staging_tables):
        results.append(verify_and_promote_table(tbl, run_id=run_id, config=cfg))
    return results


if __name__ == "__main__":
    gate_results = run_final_gate_all()
    print(f"Geprüfte Tabellen vor Visualisierung: {len(gate_results)}")
    for res in gate_results:
        status = "FREIGEGEBEN" if res["passed"] else "GESPERRT"
        print(f"  [{status}] {res['source_table']} -> {res['mart_table']} | {res['blocking_reasons']}")
