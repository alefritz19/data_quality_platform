#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dateiannahme, Chunk-Verarbeitung und universelle Parquet-Konvertierung.
Überwacht das Eingangsverzeichnis ('data/inbox'), sichert Originaldateien unverändert
in 'data/raw/' und konvertiert CSV-, TSV-, Excel-, JSON- und Parquet-Dateien
speicherschonend in Chunks nach 'data/staging/*.parquet'.
Nicht verarbeitbare Dateien werden in 'data/rejected/' abgelegt und protokolliert.
"""

import math
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from src.config import AppConfig, load_config
from src.database import log_file_conversion, log_unparseable_file

SUPPORTED_EXTENSIONS = {
    ".csv": "csv",
    ".tsv": "tsv",
    ".txt": "csv",
    ".xlsx": "excel",
    ".xls": "excel",
    ".json": "json",
    ".jsonl": "jsonl",
    ".ndjson": "jsonl",
    ".parquet": "parquet",
}


def _convert_csv_chunked(
    source_path: Path, target_parquet: Path, chunk_size: int, sep: Optional[str] = None
) -> Tuple[int, int]:
    """Konvertiert große CSV-/TSV-Dateien in Chunks nach Parquet."""
    # Für Standard-CSV nutzt DuckDB paralleles Streaming direkt nach Parquet (ZSTD)
    if sep is None:
        conn = duckdb.connect(":memory:")
        try:
            src_escaped = str(source_path).replace("'", "''")
            dst_escaped = str(target_parquet).replace("'", "''")
            conn.execute(
                f"""
                COPY (
                    SELECT * FROM read_csv_auto('{src_escaped}', all_varchar=true, ignore_errors=false)
                ) TO '{dst_escaped}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {chunk_size});
                """
            )
            row_count = conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{dst_escaped}');"
            ).fetchone()[0]
            chunks_count = max(1, math.ceil(row_count / chunk_size))
            return int(row_count), int(chunks_count)
        finally:
            conn.close()

    # Fallback über Pandas-Chunk-Iterator bei spezifischem Trennzeichen
    total_rows = 0
    chunks_count = 0
    writer: Optional[pq.ParquetWriter] = None
    try:
        for chunk_df in pd.read_csv(
            source_path, sep=sep, dtype=str, chunksize=chunk_size, on_bad_lines="raise"
        ):
            chunk_df = chunk_df.astype(str).where(chunk_df.notna(), None)
            table = pa.Table.from_pandas(chunk_df, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(target_parquet, table.schema, compression="zstd")
            writer.write_table(table)
            total_rows += len(chunk_df)
            chunks_count += 1
    finally:
        if writer is not None:
            writer.close()

    if total_rows == 0:
        raise ValueError("CSV-Datei enthält keine Datenzeilen.")
    return total_rows, chunks_count


def _convert_excel_chunked(
    source_path: Path, target_parquet: Path, chunk_size: int
) -> Tuple[int, int]:
    """Liest Excel-Dateien (.xlsx/.xls) und schreibt sie in Chunks als Parquet."""
    df = pd.read_excel(source_path, dtype=str)
    if df.empty:
        raise ValueError("Excel-Arbeitsmappe enthält keine Datenzeilen.")

    df = df.where(df.notna(), None)
    total_rows = len(df)
    chunks_count = 0
    writer: Optional[pq.ParquetWriter] = None

    try:
        for start_idx in range(0, total_rows, chunk_size):
            slice_df = df.iloc[start_idx : start_idx + chunk_size]
            table = pa.Table.from_pandas(slice_df, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(target_parquet, table.schema, compression="zstd")
            writer.write_table(table)
            chunks_count += 1
    finally:
        if writer is not None:
            writer.close()

    return total_rows, chunks_count


def _convert_json_chunked(
    source_path: Path, target_parquet: Path, chunk_size: int
) -> Tuple[int, int]:
    """Konvertiert JSON- oder JSON-Lines-Dateien über DuckDB nach Parquet."""
    conn = duckdb.connect(":memory:")
    try:
        src_escaped = str(source_path).replace("'", "''")
        dst_escaped = str(target_parquet).replace("'", "''")
        conn.execute(
            f"""
            COPY (
                SELECT * FROM read_json_auto('{src_escaped}')
            ) TO '{dst_escaped}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {chunk_size});
            """
        )
        row_count = conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{dst_escaped}');"
        ).fetchone()[0]
        if row_count == 0:
            raise ValueError("JSON-Datei enthält keine Datensätze.")
        chunks_count = max(1, math.ceil(row_count / chunk_size))
        return int(row_count), int(chunks_count)
    finally:
        conn.close()


def _copy_parquet(
    source_path: Path, target_parquet: Path, chunk_size: int
) -> Tuple[int, int]:
    """Prüft eine eingehende Parquet-Datei und übernimmt sie mit ZSTD-Komprimierung."""
    conn = duckdb.connect(":memory:")
    try:
        src_escaped = str(source_path).replace("'", "''")
        dst_escaped = str(target_parquet).replace("'", "''")
        conn.execute(
            f"""
            COPY (
                SELECT * FROM read_parquet('{src_escaped}')
            ) TO '{dst_escaped}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {chunk_size});
            """
        )
        row_count = conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{dst_escaped}');"
        ).fetchone()[0]
        chunks_count = max(1, math.ceil(row_count / chunk_size))
        return int(row_count), int(chunks_count)
    finally:
        conn.close()


def process_single_file(
    file_path: Path, config: Optional[AppConfig] = None
) -> Dict[str, object]:
    """
    Nimmt eine einzelne Datei an, prüft Größenlimit und Format, sichert das Original
    in 'data/raw/' und erzeugt eine Parquet-Datei in 'data/staging/'.
    """
    cfg = config or load_config()
    start_time = time.perf_counter()
    timestamp_tag = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    stem = file_path.stem
    ext = file_path.suffix.lower()
    file_id = f"{stem}_{timestamp_tag}"

    if not file_path.exists() or not file_path.is_file():
        raise FileNotFoundError(f"Datei nicht gefunden: {file_path}")

    size_before = file_path.stat().st_size
    max_bytes = cfg.max_upload_size_mb * 1024 * 1024

    # 1. Größenlimit prüfen
    if size_before > max_bytes:
        rejected_target = cfg.rejected_dir / f"{file_id}{ext}"
        shutil.move(str(file_path), str(rejected_target))
        reason = (
            f"Dateigröße ({size_before / (1024 * 1024):.2f} MB) überschreitet "
            f"das zulässige Limit von {cfg.max_upload_size_mb} MB."
        )
        log_unparseable_file(file_id, file_path.name, ext, rejected_target, reason, cfg)
        return {"status": "rejected", "file_id": file_id, "reason": reason}

    # 2. Unterstütztes Format prüfen
    if ext not in SUPPORTED_EXTENSIONS:
        rejected_target = cfg.rejected_dir / f"{file_id}{ext}"
        shutil.move(str(file_path), str(rejected_target))
        reason = f"Nicht unterstütztes Dateiformat '{ext}'."
        log_unparseable_file(file_id, file_path.name, ext, rejected_target, reason, cfg)
        return {"status": "rejected", "file_id": file_id, "reason": reason}

    file_format = SUPPORTED_EXTENSIONS[ext]
    raw_target = cfg.raw_dir / f"{file_id}{ext}"
    parquet_target = cfg.staging_dir / f"{file_id}.parquet"

    # 3. Originaldatei unverändert nach data/raw/ kopieren
    shutil.copy2(str(file_path), str(raw_target))

    # 4. Konvertierung nach Parquet in Chunks
    try:
        if file_format == "csv":
            row_count, chunks_count = _convert_csv_chunked(
                raw_target, parquet_target, cfg.chunk_size_rows
            )
        elif file_format == "tsv":
            row_count, chunks_count = _convert_csv_chunked(
                raw_target, parquet_target, cfg.chunk_size_rows, sep="\t"
            )
        elif file_format == "excel":
            row_count, chunks_count = _convert_excel_chunked(
                raw_target, parquet_target, cfg.chunk_size_rows
            )
        elif file_format in ("json", "jsonl"):
            row_count, chunks_count = _convert_json_chunked(
                raw_target, parquet_target, cfg.chunk_size_rows
            )
        else:
            row_count, chunks_count = _copy_parquet(
                raw_target, parquet_target, cfg.chunk_size_rows
            )
    except Exception as exc:
        if raw_target.exists():
            raw_target.unlink()
        if parquet_target.exists():
            parquet_target.unlink()
        rejected_target = cfg.rejected_dir / f"{file_id}{ext}"
        shutil.move(str(file_path), str(rejected_target))
        reason = f"Konvertierungsfehler ({file_format}): {exc}"
        log_unparseable_file(file_id, file_path.name, file_format, rejected_target, reason, cfg)
        return {"status": "rejected", "file_id": file_id, "reason": reason}

    # Eingangsdatei aus inbox entfernen, da Original in raw liegt
    if file_path.exists() and file_path.parent.resolve() == cfg.inbox_dir.resolve():
        file_path.unlink()

    duration_ms = (time.perf_counter() - start_time) * 1000.0
    size_after = parquet_target.stat().st_size

    log_file_conversion(
        file_id=file_id,
        original_filename=file_path.name,
        file_format=file_format,
        size_before_bytes=size_before,
        size_after_bytes=size_after,
        row_count=row_count,
        chunks_count=chunks_count,
        duration_ms=duration_ms,
        raw_path=raw_target,
        parquet_path=parquet_target,
        config=cfg,
    )

    return {
        "status": "converted",
        "file_id": file_id,
        "original_filename": file_path.name,
        "file_format": file_format,
        "size_before_bytes": size_before,
        "size_after_bytes": size_after,
        "row_count": row_count,
        "chunks_count": chunks_count,
        "duration_ms": round(duration_ms, 2),
        "raw_path": str(raw_target),
        "parquet_path": str(parquet_target),
    }


def process_inbox(config: Optional[AppConfig] = None) -> List[Dict[str, object]]:
    """Verarbeitet alle neuen Dateien im Eingangsverzeichnis ('data/inbox')."""
    cfg = config or load_config()
    results: List[Dict[str, object]] = []

    for candidate in sorted(cfg.inbox_dir.iterdir()):
        if candidate.name == ".gitkeep" or candidate.is_dir():
            continue
        results.append(process_single_file(candidate, cfg))

    return results


if __name__ == "__main__":
    summary = process_inbox()
    print(f"Verarbeitete Dateien im Eingangsordner: {len(summary)}")
    for item in summary:
        print(item)
