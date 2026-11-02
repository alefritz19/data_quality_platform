#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Regelbasierte Qualitätsprüfungen (Stufe 1) und Protokollierung in 'metadata.quality_log'.
Jeder Befund wird mit Quelle, Tabelle, Spalte, Regel, Anzahl, prozentualem Anteil und
nachvollziehbarer Begründung gespeichert.
Erkennt wiederkehrende Probleme über mehrere Läufe oder Quellen hinweg und markiert sie
als 'PRIORISIERT'. Personenbezogene Muster (E-Mail, Telefon, IBAN) werden nur gezählt,
niemals im Klartext ausgegeben.
"""

import re
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional
import numpy as np
import pandas as pd
import pandera as pa
from src.config import AppConfig, load_config
from src.database import get_connection, init_database

EMAIL_REGEX = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
IBAN_REGEX = re.compile(r"\b[A-Z]{2}\d{2}[A-Z0-9]{11,30}\b")
PHONE_REGEX = re.compile(r"(?:\+49|0049|0)\s?\d{2,4}[\s/-]?\d{4,9}")
TRUNCATED_REGEX = re.compile(r"(\.\.\.|…|\ufffd|\\\s*$)")

NON_NEGATIVE_KEYWORDS = ("mwh", "load", "generation", "amount", "price", "quantity", "count")


def _check_if_recurring(
    conn,
    table_name: str,
    column_name: str,
    rule_name: str,
    current_run_id: str,
) -> bool:
    """
    Prüft in 'metadata.quality_log', ob dieses Problem bereits in früheren Läufen
    oder in anderen Tabellen/Quellen mit Befunden (> 0) aufgetreten ist.
    """
    prev_same_table = conn.execute(
        """
        SELECT COUNT(*) FROM metadata.quality_log
        WHERE table_name = ? AND column_name = ? AND rule_name = ?
          AND affected_count > 0 AND run_id != ?;
        """,
        [table_name, column_name, rule_name, current_run_id],
    ).fetchone()[0]

    cross_source = conn.execute(
        """
        SELECT COUNT(DISTINCT table_name) FROM metadata.quality_log
        WHERE column_name = ? AND rule_name = ?
          AND affected_count > 0 AND table_name != ?;
        """,
        [column_name, rule_name, table_name],
    ).fetchone()[0]

    return bool(prev_same_table > 0 or cross_source > 0)


def validate_with_pandera(df: pd.DataFrame) -> Dict[str, int]:
    """Führt eine strukturelle Schema-Basisprüfung mit Pandera über alle Spalten aus."""
    columns_schema = {
        col: pa.Column(nullable=True, required=True) for col in df.columns
    }
    schema = pa.DataFrameSchema(columns=columns_schema, strict=False)
    schema.validate(df, lazy=True)
    return {"validated_columns": len(df.columns)}


def run_quality_checks_on_table(
    table_name: str,
    source_name: Optional[str] = None,
    stage: str = "staging",
    run_id: Optional[str] = None,
    config: Optional[AppConfig] = None,
) -> List[Dict[str, object]]:
    """
    Führt alle regelbasierten Qualitätsprüfungen auf einer DuckDB-Tabelle aus
    und schreibt sämtliche Ergebnisse in 'metadata.quality_log'.
    """
    cfg = config or load_config()
    init_database(cfg)
    active_run_id = run_id or datetime.now(timezone.utc).strftime("run_%Y%m%d_%H%M%S")
    checked_at = datetime.now(timezone.utc).replace(tzinfo=None)
    src_label = source_name or table_name

    conn = get_connection(cfg)
    try:
        df = conn.execute(f"SELECT * FROM {table_name};").df()
        total_rows = len(df)
        if total_rows == 0:
            return []

        validate_with_pandera(df)
        findings: List[Dict[str, object]] = []

        def record_finding(
            col_name: str,
            rule_name: str,
            affected: int,
            explanation: str,
        ) -> None:
            share_pct = round((affected / total_rows) * 100.0, 2) if total_rows > 0 else 0.0
            recurring = (
                _check_if_recurring(conn, table_name, col_name, rule_name, active_run_id)
                if affected > 0
                else False
            )
            priority = "PRIORISIERT" if (affected > 0 and (recurring or share_pct >= 10.0)) else "NORMAL"
            check_id = f"{active_run_id}_{stage}_{table_name}_{col_name}_{rule_name}_{uuid.uuid4().hex[:6]}"

            conn.execute(
                """
                INSERT INTO metadata.quality_log (
                    check_id, run_id, stage, source_name, table_name,
                    column_name, rule_name, affected_count, total_count,
                    share_pct, is_recurring, priority_level, explanation, checked_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                [
                    check_id,
                    active_run_id,
                    stage,
                    src_label,
                    table_name,
                    col_name,
                    rule_name,
                    int(affected),
                    int(total_rows),
                    share_pct,
                    recurring,
                    priority,
                    explanation,
                    checked_at,
                ],
            )
            findings.append(
                {
                    "check_id": check_id,
                    "run_id": active_run_id,
                    "stage": stage,
                    "source_name": src_label,
                    "table_name": table_name,
                    "column_name": col_name,
                    "rule_name": rule_name,
                    "affected_count": int(affected),
                    "total_count": int(total_rows),
                    "share_pct": share_pct,
                    "is_recurring": recurring,
                    "priority_level": priority,
                    "explanation": explanation,
                }
            )

        # 1. Tabellenweite Prüfung auf Duplikate
        dup_count = int(df.duplicated().sum())
        record_finding(
            col_name="_all_columns_",
            rule_name="duplicate_rows",
            affected=dup_count,
            explanation=f"{dup_count} vollständig identische Duplikat-Zeilen erkannt.",
        )

        # 2. Spaltenweise Prüfungen
        for col in df.columns:
            series = df[col]

            # 2a. Fehlende Werte (NULL oder leerer String)
            if pd.api.types.is_string_dtype(series) or series.dtype == object:
                missing_mask = series.isna() | (series.astype(str).str.strip() == "")
            else:
                missing_mask = series.isna()
            missing_cnt = int(missing_mask.sum())
            record_finding(
                col_name=col,
                rule_name="missing_values",
                affected=missing_cnt,
                explanation=f"{missing_cnt} von {total_rows} Werten fehlen in Spalte '{col}'.",
            )

            # 2b. Nicht konvertierbare Typ-/Formatfehler aus staging_loader abrufen
            type_err_cnt = conn.execute(
                """
                SELECT COUNT(*) FROM metadata.unparseable_rows
                WHERE table_name = ? AND column_name = ?;
                """,
                [table_name, col],
            ).fetchone()[0]
            if type_err_cnt > 0:
                record_finding(
                    col_name=col,
                    rule_name="invalid_type_or_format",
                    affected=int(type_err_cnt),
                    explanation=(
                        f"{type_err_cnt} Werte in Spalte '{col}' hatten ein ungültiges "
                        "Zahlen- oder Datumsformat (siehe Abschnitt 'Nicht verarbeitbar')."
                    ),
                )

            # 2c. Numerische Prüfungen: unplausible Werte sowie IQR- und z-Score-Ausreißer
            if pd.api.types.is_numeric_dtype(series) and not pd.api.types.is_bool_dtype(series):
                clean_num = series.dropna().astype(float)
                if len(clean_num) > 0:
                    col_lower = col.lower()
                    if any(kw in col_lower for kw in NON_NEGATIVE_KEYWORDS):
                        neg_cnt = int((clean_num < 0).sum())
                        if neg_cnt > 0:
                            record_finding(
                                col_name=col,
                                rule_name="implausible_negative_values",
                                affected=neg_cnt,
                                explanation=(
                                    f"{neg_cnt} unplausible negative Werte (< 0) in "
                                    f"physikalischer/finanzieller Kennzahl '{col}'."
                                ),
                            )

                if len(clean_num) >= 10:
                    q1 = float(np.percentile(clean_num, 25))
                    q3 = float(np.percentile(clean_num, 75))
                    iqr = q3 - q1
                    lower_bound = q1 - 1.5 * iqr
                    upper_bound = q3 + 1.5 * iqr
                    mean_val = float(clean_num.mean())
                    std_val = float(clean_num.std())

                    iqr_mask = (clean_num < lower_bound) | (clean_num > upper_bound)
                    if std_val > 0:
                        z_scores = np.abs((clean_num - mean_val) / std_val)
                        z_mask = z_scores > 3.0
                    else:
                        z_mask = pd.Series(False, index=clean_num.index)

                    outlier_cnt = int((iqr_mask | z_mask).sum())
                    record_finding(
                        col_name=col,
                        rule_name="statistical_outliers_iqr_zscore",
                        affected=outlier_cnt,
                        explanation=(
                            f"{outlier_cnt} statistische Ausreißer außerhalb des IQR-Intervalls "
                            f"[{lower_bound:.2f}, {upper_bound:.2f}] oder mit |z-Score| > 3,0."
                        ),
                    )

            # 2d. Textprüfungen: abgeschnittene Inhalte & personenbezogene Daten (PII)
            if pd.api.types.is_string_dtype(series) or series.dtype == object:
                non_empty_str = series.dropna().astype(str)
                if len(non_empty_str) > 0:
                    trunc_cnt = int(
                        non_empty_str.apply(lambda s: bool(TRUNCATED_REGEX.search(s))).sum()
                    )
                    if trunc_cnt > 0:
                        record_finding(
                            col_name=col,
                            rule_name="truncated_or_garbled_content",
                            affected=trunc_cnt,
                            explanation=(
                                f"{trunc_cnt} Einträge in Spalte '{col}' enthalten Abbruchmarker "
                                "('...', '…') oder defekte Zeichenkodierung."
                            ),
                        )

                    pii_cnt = int(
                        non_empty_str.apply(
                            lambda s: bool(
                                EMAIL_REGEX.search(s)
                                or IBAN_REGEX.search(s)
                                or PHONE_REGEX.search(s)
                            )
                        ).sum()
                    )
                    if pii_cnt > 0:
                        record_finding(
                            col_name=col,
                            rule_name="pii_pattern_detected",
                            affected=pii_cnt,
                            explanation=(
                                f"{pii_cnt} Einträge in Spalte '{col}' enthalten Muster "
                                "personenbezogener Daten (E-Mail, Telefon oder IBAN). "
                                "Werte wurden nur markiert und nicht im Klartext ausgegeben."
                            ),
                        )

        return findings
    finally:
        conn.close()


def run_all_staging_quality_checks(
    stage: str = "staging", config: Optional[AppConfig] = None
) -> List[Dict[str, object]]:
    """Führt Qualitätsprüfungen für alle Tabellen im DuckDB-Schema 'staging' aus."""
    cfg = config or load_config()
    init_database(cfg)
    run_id = datetime.now(timezone.utc).strftime("run_%Y%m%d_%H%M%S")

    conn = get_connection(cfg)
    try:
        tables = [
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

    all_findings: List[Dict[str, object]] = []
    for tbl in sorted(tables):
        all_findings.extend(
            run_quality_checks_on_table(
                table_name=f"staging.{tbl}",
                source_name=tbl,
                stage=stage,
                run_id=run_id,
                config=cfg,
            )
        )
    return all_findings


def get_unparseable_summary(config: Optional[AppConfig] = None) -> Dict[str, List[Dict[str, object]]]:
    """Liefert alle Einträge für den Bericht-Abschnitt 'Nicht verarbeitbar'."""
    cfg = config or load_config()
    init_database(cfg)
    conn = get_connection(cfg)
    try:
        files_df = conn.execute(
            "SELECT * FROM metadata.unparseable_files ORDER BY detected_at DESC;"
        ).df()
        rows_df = conn.execute(
            "SELECT * FROM metadata.unparseable_rows ORDER BY detected_at DESC;"
        ).df()
        return {
            "unparseable_files": files_df.to_dict(orient="records"),
            "unparseable_rows": rows_df.to_dict(orient="records"),
        }
    finally:
        conn.close()


if __name__ == "__main__":
    results = run_all_staging_quality_checks()
    print(f"Ausgeführte Qualitätsprüfungen: {len(results)}")
    for r in results:
        if r["affected_count"] > 0:
            print(
                f"  [{r['priority_level']}] {r['table_name']}.{r['column_name']} | "
                f"{r['rule_name']}: {r['affected_count']} ({r['share_pct']} %)"
            )
