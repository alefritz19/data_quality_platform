#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dagster Asset Checks für die Datenqualitäts-Plattform.
Prüft auf Asset-Ebene, ob alle Tabellen die Gatekeeper-Prüfung bestanden haben
und ob personenbezogene Datenmuster (PII) erkannt wurden.
"""

from dagster import AssetCheckResult, AssetCheckSeverity, asset_check
from dagster_app.assets import quality_check_results, verified_mart_tables
from src.config import load_config
from src.database import get_connection, init_database


@asset_check(asset=quality_check_results, description="Prüft auf erkannte PII-Muster im Staging.")
def check_no_pii_in_staging() -> AssetCheckResult:
    """Stellt sicher, dass keine personenbezogenen Muster (E-Mail, Telefon, IBAN) vorliegen."""
    cfg = load_config()
    init_database(cfg)
    conn = get_connection(cfg)
    try:
        pii_hits = conn.execute(
            """
            SELECT COALESCE(SUM(affected_count), 0)
            FROM metadata.quality_log
            WHERE rule_name = 'pii_pattern_detected' AND stage = 'staging';
            """
        ).fetchone()[0]
    finally:
        conn.close()

    passed = int(pii_hits) == 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={"pii_matches_count": int(pii_hits)},
        description=(
            "Keine personenbezogenen Muster erkannt."
            if passed
            else f"{pii_hits} Einträge mit möglichen personenbezogenen Mustern markiert."
        ),
    )


@asset_check(
    asset=verified_mart_tables,
    description="Prüft, ob mindestens eine Tabelle die abschließende Freigabe bestanden hat.",
)
def check_mart_gate_passed() -> AssetCheckResult:
    """Prüft in 'metadata.mart_gate_log', ob geprüfte Daten für das Dashboard bereitstehen."""
    cfg = load_config()
    init_database(cfg)
    conn = get_connection(cfg)
    try:
        passed_tables = conn.execute(
            "SELECT COUNT(*) FROM metadata.mart_gate_log WHERE passed = true;"
        ).fetchone()[0]
        blocked_tables = conn.execute(
            "SELECT COUNT(*) FROM metadata.mart_gate_log WHERE passed = false;"
        ).fetchone()[0]
    finally:
        conn.close()

    ok = int(passed_tables) > 0
    return AssetCheckResult(
        passed=ok,
        severity=AssetCheckSeverity.WARN if ok else AssetCheckSeverity.ERROR,
        metadata={
            "passed_tables": int(passed_tables),
            "blocked_tables": int(blocked_tables),
        },
        description=f"Freigegebene Mart-Tabellen: {passed_tables}, Gesperrte Tabellen: {blocked_tables}.",
    )
