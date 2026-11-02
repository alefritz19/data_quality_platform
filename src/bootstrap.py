#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Automatische Erstinitialisierung (Bootstrap) und Demo-Zurücksetzung.
Sorgt dafür, dass die Plattform direkt nach 'docker compose up' ohne manuelle
Schritte reale öffentliche Daten abruft, nach Parquet konvertiert, prüft und
im Dashboard bereitstellt.
"""

from pathlib import Path
from typing import Dict, Optional
from src.config import AppConfig, load_config
from src.database import get_connection, init_database
from src.export_manager import cleanup_expired_exports, create_export_package
from src.file_converter import process_inbox
from src.final_gate import run_final_gate_all
from src.ingest_source import fetch_all_public_sources
from src.quality_checks import run_all_staging_quality_checks
from src.report_generator import build_quality_report
from src.staging_loader import stage_all_parquet_files


def run_end_to_end_pipeline(config: Optional[AppConfig] = None) -> Dict[str, object]:
    """Führt die gesamte Pipeline von der realen Quelle bis zum Exportpaket aus."""
    cfg = config or load_config()
    init_database(cfg)

    # 1. Reale Datenquellen (SMARD + DWD) abrufen und in den Eingangsordner legen
    sources_info = fetch_all_public_sources(cfg)
    for src_item in sources_info:
        raw_csv = Path(str(src_item["raw_csv_path"]))
        inbox_copy = cfg.inbox_dir / raw_csv.name
        inbox_copy.write_bytes(raw_csv.read_bytes())

    # 2. Dateiannahme und Parquet-Konvertierung
    conversions = process_inbox(cfg)

    # 3. Staging in DuckDB
    staged = stage_all_parquet_files(cfg)

    # 4. Qualitätsprüfungen (Stufe 1)
    findings = run_all_staging_quality_checks(stage="staging", config=cfg)

    # 5. Abschließende Freigabeprüfung vor Visualisierung (Gatekeeper)
    gate_results = run_final_gate_all(cfg)

    # 6. Regelbasierter Qualitätsbericht
    report_info = build_quality_report(cfg)

    # 7. Erstes Exportpaket für freigegebene Tabellen bereitstellen
    exported = []
    for g in gate_results:
        if g.get("passed"):
            short_name = str(g["mart_table"]).replace("mart.", "")
            exported.append(create_export_package(short_name, config=cfg))

    cleanup_expired_exports(cfg)

    return {
        "sources": sources_info,
        "conversions_count": len(conversions),
        "staged_tables_count": len(staged),
        "findings_count": len(findings),
        "gate_results": gate_results,
        "report_path": report_info["report_md_path"],
        "exports_created": len(exported),
    }


def ensure_initialized(config: Optional[AppConfig] = None) -> bool:
    """Prüft beim Start, ob bereits Mart-Tabellen existieren, und initialisiert sonst automatisch."""
    cfg = config or load_config()
    init_database(cfg)
    conn = get_connection(cfg)
    try:
        mart_cnt = conn.execute(
            """
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = 'mart';
            """
        ).fetchone()[0]
    finally:
        conn.close()

    if int(mart_cnt) == 0:
        print("Keine Mart-Tabellen gefunden. Starte automatische Erstinitialisierung...")
        run_end_to_end_pipeline(cfg)
        return True
    return False


if __name__ == "__main__":
    summary = run_end_to_end_pipeline()
    print(f"Pipeline-Durchlauf abgeschlossen: {summary['report_path']}")
