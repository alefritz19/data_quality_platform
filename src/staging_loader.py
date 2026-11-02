#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Staging-Verarbeitung mit DuckDB.
Liest konvertierte Parquet-Dateien, normalisiert Spaltennamen, erkennt und konvertiert
deutsche sowie internationale Datums- und Zahlenformate und lädt das Ergebnis in das
DuckDB-Schema 'staging'. Nicht konvertierbare Einzelwerte werden mit Herkunft und
Begründung in 'metadata.unparseable_rows' protokolliert, damit keine Daten still verschwinden.
"""

import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import pandas as pd
from src.config import AppConfig, load_config
from src.database import get_connection, init_database

GERMAN_NUMBER_PATTERN = re.compile(r"^[+-]?(\d{1,3}(\.\d{3})+|\d+)(,\d+)?$")
STANDARD_NUMBER_PATTERN = re.compile(r"^[+-]?\d+(\.\d+)?$")
DATE_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
    "%d.%m.%Y %H:%M:%S",
    "%d.%m.%Y",
    "%Y/%m/%d",
)


def normalize_column_name(col_name: str) -> str:
    """Bereinigt Spaltennamen auf Kleinbuchstaben und Unterstriche."""
    cleaned = re.sub(r"[^a-zA-Z0-9_]+", "_", str(col_name).strip().lower())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned or "unnamed_column"


def _try_parse_number(val: object) -> Tuple[Optional[float], bool]:
    """Prüft und konvertiert deutsche ('1.234,56') oder internationale Zahlenformate."""
    if val is None or pd.isna(val):
        return None, True
    if isinstance(val, (int, float)):
        return float(val), True

    text = str(val).strip()
    if not text or text.lower() in ("null", "none", "nan", "-"):
        return None, True

    if STANDARD_NUMBER_PATTERN.match(text):
        return float(text), True

    if GERMAN_NUMBER_PATTERN.match(text):
        normalized = text.replace(".", "").replace(",", ".")
        return float(normalized), True

    return None, False


def _try_parse_date(val: object) -> Tuple[Optional[datetime], bool]:
    """Prüft und konvertiert gängige deutsche und ISO-Datumsformate."""
    if val is None or pd.isna(val):
        return None, True
    if isinstance(val, datetime):
        return val, True

    text = str(val).strip()
    if not text or text.lower() in ("null", "none", "nan", "-"):
        return None, True

    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt), True
        except ValueError:
            continue
    return None, False


def stage_parquet_to_duckdb(
    parquet_path: Path,
    source_name: str,
    table_name: Optional[str] = None,
    config: Optional[AppConfig] = None,
) -> Dict[str, object]:
    """
    Typisiert eine Parquet-Datei, protokolliert nicht konvertierbare Werte in
    'metadata.unparseable_rows' und speichert die Tabelle in 'staging.<table_name>'.
    """
    cfg = config or load_config()
    init_database(cfg)

    df = pd.read_parquet(parquet_path)
    df.columns = [normalize_column_name(c) for c in df.columns]

    target_table = normalize_column_name(table_name or parquet_path.stem)
    unparseable_entries: List[tuple] = []
    detected_at = datetime.now(timezone.utc).replace(tzinfo=None)

    for col in df.columns:
        series = df[col]
        if pd.api.types.is_numeric_dtype(series) or pd.api.types.is_datetime64_any_dtype(series):
            continue

        non_null = series.dropna().astype(str).str.strip()
        non_null = non_null[~non_null.str.lower().isin(["", "null", "none", "nan", "-"])]
        if len(non_null) == 0:
            continue

        sample = non_null.head(200)
        num_hits = sum(1 for v in sample if _try_parse_number(v)[1])
        date_hits = sum(1 for v in sample if _try_parse_date(v)[1])

        # Spalte ist mehrheitlich numerisch (>= 70 %)
        if num_hits / len(sample) >= 0.70 and "date" not in col and "time" not in col:
            parsed_col: List[Optional[float]] = []
            for idx, raw_val in enumerate(series):
                num_val, ok = _try_parse_number(raw_val)
                parsed_col.append(num_val)
                if not ok:
                    rec_id = f"{target_table}_{col}_{idx}"
                    unparseable_entries.append(
                        (
                            rec_id,
                            source_name,
                            f"staging.{target_table}",
                            int(idx),
                            col,
                            str(raw_val)[:250],
                            "Ungültiges Zahlenformat in numerischer Spalte",
                            detected_at,
                        )
                    )
            df[col] = pd.Series(parsed_col, dtype="Float64")

        # Spalte ist mehrheitlich ein Datum (>= 70 %)
        elif date_hits / len(sample) >= 0.70:
            parsed_dates: List[Optional[datetime]] = []
            for idx, raw_val in enumerate(series):
                dt_val, ok = _try_parse_date(raw_val)
                parsed_dates.append(dt_val)
                if not ok:
                    rec_id = f"{target_table}_{col}_{idx}"
                    unparseable_entries.append(
                        (
                            rec_id,
                            source_name,
                            f"staging.{target_table}",
                            int(idx),
                            col,
                            str(raw_val)[:250],
                            "Ungültiges Datumsformat in Zeitstempel-Spalte",
                            detected_at,
                        )
                    )
            df[col] = pd.to_datetime(parsed_dates)
        else:
            df[col] = series.astype(str).str.strip().where(series.notna(), None)

    typed_parquet_path = cfg.staging_dir / f"typed_{target_table}.parquet"
    df.to_parquet(typed_parquet_path, index=False, compression="zstd")

    conn = get_connection(cfg)
    try:
        escaped_path = str(typed_parquet_path).replace("'", "''")
        conn.execute(
            f"""
            CREATE OR REPLACE TABLE staging.{target_table} AS
            SELECT * FROM read_parquet('{escaped_path}');
            """
        )

        if unparseable_entries:
            conn.executemany(
                """
                INSERT OR REPLACE INTO metadata.unparseable_rows (
                    record_id, source_name, table_name, row_index,
                    column_name, raw_value, reason, detected_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                """,
                unparseable_entries,
            )
    finally:
        conn.close()

    return {
        "table_name": f"staging.{target_table}",
        "typed_parquet_path": str(typed_parquet_path),
        "rows_loaded": len(df),
        "columns_count": len(df.columns),
        "unparseable_values_logged": len(unparseable_entries),
    }


def stage_all_parquet_files(config: Optional[AppConfig] = None) -> List[Dict[str, object]]:
    """Lädt alle konvertierten Parquet-Dateien aus 'data/staging/' in DuckDB."""
    cfg = config or load_config()
    results: List[Dict[str, object]] = []

    for pq_file in sorted(cfg.staging_dir.glob("*.parquet")):
        if pq_file.name.startswith("typed_"):
            continue
        results.append(
            stage_parquet_to_duckdb(
                parquet_path=pq_file,
                source_name=pq_file.stem,
                table_name=pq_file.stem,
                config=cfg,
            )
        )
    return results


if __name__ == "__main__":
    staged = stage_all_parquet_files()
    print(f"Geladene Staging-Tabellen: {len(staged)}")
    for entry in staged:
        print(entry)
