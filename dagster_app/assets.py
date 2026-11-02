#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dagster Assets für die Datenqualitäts-Plattform.
Bildet die gesamte Verarbeitungskette ab:
1. raw_public_sources (Abruf realer öffentlicher Daten)
2. parquet_conversions (Dateiannahme aus 'inbox' und Chunk-Konvertierung nach Parquet)
3. staging_tables (Typisierung und Laden in DuckDB 'staging')
4. quality_check_results (Regelbasierte Qualitätsprüfungen in 'metadata.quality_log')
5. verified_mart_tables (Abschließende Freigabeprüfung vor Visualisierung nach 'mart')
"""

import shutil
from pathlib import Path
from typing import Dict, List
from dagster import AssetExecutionContext, MetadataValue, Output, asset
from src.config import load_config
from src.file_converter import process_inbox
from src.final_gate import run_final_gate_all
from src.ingest_source import fetch_smard_raw_data
from src.quality_checks import run_all_staging_quality_checks
from src.staging_loader import stage_all_parquet_files


@asset(group_name="data_quality_pipeline")
def raw_public_sources(context: AssetExecutionContext) -> Output[Dict[str, object]]:
    """Lädt reale Strommarktdaten von SMARD.de und stellt sie im Eingangsordner bereit."""
    cfg = load_config()
    result = fetch_smard_raw_data(cfg)
    raw_csv = Path(str(result["raw_csv_path"]))

    # Kopie in den überwachten Eingangsordner legen, damit die universelle
    # Parquet-Konvertierung (inkl. Größen- und Zeitmessung) durchlaufen wird
    inbox_copy = cfg.inbox_dir / raw_csv.name
    shutil.copy2(str(raw_csv), str(inbox_copy))

    context.log.info(
        f"Quelle {result['source_id']} abgerufen ({result['rows_fetched']} Zeilen)."
    )
    return Output(
        value=result,
        metadata={
            "source_id": str(result["source_id"]),
            "rows_fetched": int(result["rows_fetched"]),
            "file_size_bytes": int(result["file_size_bytes"]),
        },
    )


@asset(deps=[raw_public_sources], group_name="data_quality_pipeline")
def parquet_conversions(
    context: AssetExecutionContext,
) -> Output[List[Dict[str, object]]]:
    """Konvertiert alle Dateien aus 'data/inbox' in Chunks nach Parquet."""
    cfg = load_config()
    converted = process_inbox(cfg)
    ok_count = sum(1 for item in converted if item.get("status") == "converted")
    rejected_count = sum(1 for item in converted if item.get("status") == "rejected")

    context.log.info(
        f"Parquet-Konvertierung abgeschlossen: {ok_count} konvertiert, {rejected_count} abgelehnt."
    )
    return Output(
        value=converted,
        metadata={
            "converted_files": ok_count,
            "rejected_files": rejected_count,
            "details": MetadataValue.json(converted),
        },
    )


@asset(deps=[parquet_conversions], group_name="data_quality_pipeline")
def staging_tables(
    context: AssetExecutionContext,
) -> Output[List[Dict[str, object]]]:
    """Lädt und typisiert die Parquet-Dateien in das DuckDB-Schema 'staging'."""
    cfg = load_config()
    staged = stage_all_parquet_files(cfg)
    total_rows = sum(int(item.get("rows_loaded", 0)) for item in staged)
    unparseable_cnt = sum(int(item.get("unparseable_values_logged", 0)) for item in staged)

    context.log.info(
        f"Staging abgeschlossen: {len(staged)} Tabellen, {total_rows} Zeilen, "
        f"{unparseable_cnt} nicht konvertierbare Einzelwerte protokolliert."
    )
    return Output(
        value=staged,
        metadata={
            "tables_count": len(staged),
            "total_rows_loaded": total_rows,
            "unparseable_values_logged": unparseable_cnt,
        },
    )


@asset(deps=[staging_tables], group_name="data_quality_pipeline")
def quality_check_results(
    context: AssetExecutionContext,
) -> Output[List[Dict[str, object]]]:
    """Führt alle regelbasierten Qualitätsprüfungen auf den Staging-Tabellen aus."""
    cfg = load_config()
    findings = run_all_staging_quality_checks(stage="staging", config=cfg)
    issues_with_hits = [f for f in findings if int(f.get("affected_count", 0)) > 0]
    prioritized_hits = [
        f for f in issues_with_hits if f.get("priority_level") == "PRIORISIERT"
    ]

    context.log.info(
        f"Qualitätsprüfung: {len(findings)} Regeln geprüft, "
        f"{len(issues_with_hits)} Auffälligkeiten ({len(prioritized_hits)} priorisiert)."
    )
    return Output(
        value=findings,
        metadata={
            "total_checks_executed": len(findings),
            "findings_with_issues": len(issues_with_hits),
            "prioritized_findings": len(prioritized_hits),
        },
    )


@asset(deps=[quality_check_results], group_name="data_quality_pipeline")
def verified_mart_tables(
    context: AssetExecutionContext,
) -> Output[List[Dict[str, object]]]:
    """
    Abschließender Prüfschritt vor der Visualisierung (Gatekeeper).
    Übernimmt nur erfolgreich geprüfte Tabellen in das Schema 'mart'.
    """
    cfg = load_config()
    gate_results = run_final_gate_all(cfg)
    passed_cnt = sum(1 for g in gate_results if g.get("passed") is True)
    blocked_cnt = len(gate_results) - passed_cnt

    context.log.info(
        f"Gatekeeper-Prüfung vor Visualisierung: {passed_cnt} freigegeben, {blocked_cnt} gesperrt."
    )
    return Output(
        value=gate_results,
        metadata={
            "promoted_to_mart": passed_cnt,
            "blocked_tables": blocked_cnt,
            "gate_summary": MetadataValue.json(gate_results),
        },
    )
