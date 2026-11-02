#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DuckDB-Datenbankverwaltung und Initialisierung der Metadaten-Tabellen.
Erstellt die Schemata für raw, staging, mart und metadata sowie die Tabellen
'quellen', 'file_conversions' und 'unparseable_files'.
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
    """Initialisiert alle erforderlichen Schemata und die Metadaten-Tabellen."""
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

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS metadata.file_conversions (
                file_id VARCHAR PRIMARY KEY,
                original_filename VARCHAR NOT NULL,
                file_format VARCHAR NOT NULL,
                size_before_bytes BIGINT NOT NULL,
                size_after_bytes BIGINT NOT NULL,
                compression_ratio_pct DOUBLE NOT NULL,
                row_count BIGINT NOT NULL,
                chunks_count INTEGER NOT NULL,
                duration_ms DOUBLE NOT NULL,
                raw_path VARCHAR NOT NULL,
                parquet_path VARCHAR NOT NULL,
                converted_at TIMESTAMP NOT NULL
            );
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS metadata.unparseable_files (
                file_id VARCHAR PRIMARY KEY,
                original_filename VARCHAR NOT NULL,
                file_format VARCHAR NOT NULL,
                rejected_path VARCHAR NOT NULL,
                reason VARCHAR NOT NULL,
                detected_at TIMESTAMP NOT NULL
            );
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS metadata.unparseable_rows (
                record_id VARCHAR PRIMARY KEY,
                source_name VARCHAR NOT NULL,
                table_name VARCHAR NOT NULL,
                row_index BIGINT NOT NULL,
                column_name VARCHAR NOT NULL,
                raw_value VARCHAR,
                reason VARCHAR NOT NULL,
                detected_at TIMESTAMP NOT NULL
            );
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS metadata.quality_log (
                check_id VARCHAR PRIMARY KEY,
                run_id VARCHAR NOT NULL,
                stage VARCHAR NOT NULL,
                source_name VARCHAR NOT NULL,
                table_name VARCHAR NOT NULL,
                column_name VARCHAR NOT NULL,
                rule_name VARCHAR NOT NULL,
                affected_count BIGINT NOT NULL,
                total_count BIGINT NOT NULL,
                share_pct DOUBLE NOT NULL,
                is_recurring BOOLEAN NOT NULL,
                priority_level VARCHAR NOT NULL,
                explanation VARCHAR NOT NULL,
                checked_at TIMESTAMP NOT NULL
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


def log_file_conversion(
    file_id: str,
    original_filename: str,
    file_format: str,
    size_before_bytes: int,
    size_after_bytes: int,
    row_count: int,
    chunks_count: int,
    duration_ms: float,
    raw_path: Path,
    parquet_path: Path,
    config: Optional[AppConfig] = None,
) -> None:
    """Protokolliert eine erfolgreiche Parquet-Konvertierung inklusive Größe und Dauer."""
    init_database(config)
    ratio = (
        round((1.0 - (size_after_bytes / size_before_bytes)) * 100.0, 2)
        if size_before_bytes > 0
        else 0.0
    )
    converted_at = datetime.now(timezone.utc).replace(tzinfo=None)

    conn = get_connection(config)
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO metadata.file_conversions (
                file_id, original_filename, file_format,
                size_before_bytes, size_after_bytes, compression_ratio_pct,
                row_count, chunks_count, duration_ms,
                raw_path, parquet_path, converted_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            [
                file_id,
                original_filename,
                file_format,
                size_before_bytes,
                size_after_bytes,
                ratio,
                row_count,
                chunks_count,
                round(duration_ms, 2),
                str(raw_path),
                str(parquet_path),
                converted_at,
            ],
        )
    finally:
        conn.close()


def log_unparseable_file(
    file_id: str,
    original_filename: str,
    file_format: str,
    rejected_path: Path,
    reason: str,
    config: Optional[AppConfig] = None,
) -> None:
    """Protokolliert nicht verarbeitbare Dateien mit Begründung und Herkunft."""
    init_database(config)
    detected_at = datetime.now(timezone.utc).replace(tzinfo=None)

    conn = get_connection(config)
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO metadata.unparseable_files (
                file_id, original_filename, file_format,
                rejected_path, reason, detected_at
            ) VALUES (?, ?, ?, ?, ?, ?);
            """,
            [
                file_id,
                original_filename,
                file_format,
                str(rejected_path),
                reason,
                detected_at,
            ],
        )
    finally:
        conn.close()
