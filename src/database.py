#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DuckDB-Datenbankverwaltung und Initialisierung der Metadaten-Tabellen.
Erstellt die Schemata für raw, staging, mart und metadata sowie die Tabelle 'quellen'.
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
import duckdb
from src.config import AppConfig, load_config


def get_connection(config: Optional[AppConfig] = None) -> duckdb.DuckDBPyConnection:
    """Öffnet eine Verbindung zur konfigurierten DuckDB-Instanz."""
    cfg = config or load_config()
    cfg.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(cfg.duckdb_path))


def init_database(config: Optional[AppConfig] = None) -> None:
    """Initialisiert alle erforderlichen Schemata und die Quellentabelle 'quellen'."""
    conn = get_connection(config)
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw;")
        conn.execute("CREATE SCHEMA IF NOT EXISTS staging;")
        conn.execute("CREATE SCHEMA IF NOT EXISTS mart;")
        conn.execute("CREATE SCHEMA IF NOT EXISTS metadata;")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS metadata.quellen (
                source_id VARCHAR PRIMARY KEY,
                name VARCHAR NOT NULL,
                url VARCHAR NOT NULL,
                license VARCHAR NOT NULL,
                fetched_at TIMESTAMP NOT NULL,
                raw_file_path VARCHAR NOT NULL,
                file_size_bytes BIGINT NOT NULL,
                row_count BIGINT NOT NULL
            );
            """
        )
    finally:
        conn.close()


def register_source(
    source_id: str,
    name: str,
    url: str,
    license_name: str,
    raw_file_path: Path,
    row_count: int,
    config: Optional[AppConfig] = None,
) -> None:
    """Dokumentiert einen Datenabruf nachvollziehbar in der Tabelle 'metadata.quellen'."""
    init_database(config)
    file_size = raw_file_path.stat().st_size if raw_file_path.exists() else 0
    fetched_at = datetime.now(timezone.utc).replace(tzinfo=None)

    conn = get_connection(config)
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO metadata.quellen (
                source_id, name, url, license, fetched_at,
                raw_file_path, file_size_bytes, row_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """,
            [
                source_id,
                name,
                url,
                license_name,
                fetched_at,
                str(raw_file_path),
                file_size,
                row_count,
            ],
        )
    finally:
        conn.close()
