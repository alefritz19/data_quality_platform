#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Zentrale Konfigurationsverwaltung.
Lädt Einstellungen aus Umgebungsvariablen (.env, .env.prod, .env.demo).
Unterschiede zwischen Produktiv- und Demo-Kontext entstehen ausschließlich hierüber.
"""

import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class AppConfig:
    """Datenträger für alle Laufzeitparameter der Plattform."""

    app_env: str
    data_dir: Path
    inbox_dir: Path
    raw_dir: Path
    staging_dir: Path
    mart_dir: Path
    export_dir: Path
    rejected_dir: Path
    duckdb_path: Path
    log_file: Path
    log_level: str
    chunk_size_rows: int
    max_upload_size_mb: int
    export_retention_hours: int
    export_max_total_mb: int
    demo_read_only: bool
    admin_pin: str
    smard_weeks_count: int


def load_config() -> AppConfig:
    """Liest die aktuelle Umgebungskonfiguration und stellt Verzeichnisse bereit."""
    app_env = os.getenv("APP_ENV", "demo").strip().lower()
    data_dir = Path(os.getenv("DATA_DIR", "./data")).resolve()
    inbox_dir = Path(os.getenv("INBOX_DIR", str(data_dir / "inbox"))).resolve()
    raw_dir = (data_dir / "raw").resolve()
    staging_dir = (data_dir / "staging").resolve()
    mart_dir = (data_dir / "mart").resolve()
    export_dir = Path(os.getenv("EXPORT_DIR", str(data_dir / "export"))).resolve()
    rejected_dir = (data_dir / "rejected").resolve()
    duckdb_path = Path(
        os.getenv("DUCKDB_PATH", str(mart_dir / "quality_platform.duckdb"))
    ).resolve()
    log_file = Path(os.getenv("LOG_FILE", "./logs/platform.log")).resolve()

    for directory in (
        data_dir,
        inbox_dir,
        raw_dir,
        staging_dir,
        mart_dir,
        export_dir,
        rejected_dir,
        log_file.parent,
    ):
        directory.mkdir(parents=True, exist_ok=True)

    default_weeks = 4 if app_env == "prod" else 1

    return AppConfig(
        app_env=app_env,
        data_dir=data_dir,
        inbox_dir=inbox_dir,
        raw_dir=raw_dir,
        staging_dir=staging_dir,
        mart_dir=mart_dir,
        export_dir=export_dir,
        rejected_dir=rejected_dir,
        duckdb_path=duckdb_path,
        log_file=log_file,
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        chunk_size_rows=int(os.getenv("CHUNK_SIZE_ROWS", "100000")),
        max_upload_size_mb=int(os.getenv("MAX_UPLOAD_SIZE_MB", "20")),
        export_retention_hours=int(os.getenv("EXPORT_RETENTION_HOURS", "24")),
        export_max_total_mb=int(os.getenv("EXPORT_MAX_TOTAL_MB", "500")),
        demo_read_only=os.getenv("DEMO_READ_ONLY", "true").strip().lower() == "true",
        admin_pin=os.getenv("ADMIN_PIN", "change_me_in_local_env"),
        smard_weeks_count=int(os.getenv("SMARD_WEEKS_COUNT", str(default_weeks))),
    )
