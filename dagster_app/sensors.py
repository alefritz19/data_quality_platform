#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dagster Sensor für das überwachte Eingangsverzeichnis ('data/inbox').
Erkennt automatisch neu abgelegte Dateien (CSV, Excel, JSON, Parquet) und startet
die Konvertierungs-, Staging-, Qualitäts- und Freigabe-Pipeline.
"""

from dagster import AssetSelection, RunRequest, SensorEvaluationContext, SkipReason, define_asset_job, sensor
from src.config import load_config

inbox_processing_job = define_asset_job(
    name="inbox_processing_job",
    selection=AssetSelection.keys(
        "parquet_conversions",
        "staging_tables",
        "quality_check_results",
        "verified_mart_tables",
        "quality_report",
    ),
    description="Verarbeitet neue Dateien im Eingangsordner bis zur Freigabe in 'mart'.",
)

full_pipeline_job = define_asset_job(
    name="full_pipeline_job",
    selection=AssetSelection.groups("data_quality_pipeline"),
    description="Lädt externe Quellen und führt die gesamte Qualitäts-Pipeline aus.",
)


@sensor(
    job=inbox_processing_job,
    minimum_interval_seconds=30,
    description="Überwacht 'data/inbox' auf neue Dateien und startet die Parquet-Konvertierung.",
)
def inbox_file_sensor(context: SensorEvaluationContext):
    """Prüft das Eingangsverzeichnis auf neue Dateien."""
    cfg = load_config()
    if cfg.app_env == "demo" and cfg.demo_read_only:
        return SkipReason("Demo-Kontext ist schreibgeschützt (DEMO_READ_ONLY=true).")

    candidates = [
        p
        for p in sorted(cfg.inbox_dir.iterdir())
        if p.is_file() and p.name != ".gitkeep"
    ]
    if not candidates:
        return SkipReason("Keine neuen Dateien im Eingangsverzeichnis 'data/inbox'.")

    file_signature = ",".join(f"{p.name}:{p.stat().st_mtime_ns}" for p in candidates)
    if context.cursor == file_signature:
        return SkipReason("Dateien im Eingangsordner wurden bereits eingeplant.")

    context.update_cursor(file_signature)
    return RunRequest(
        run_key=file_signature,
        tags={"trigger": "inbox_sensor", "files_count": str(len(candidates))},
    )
