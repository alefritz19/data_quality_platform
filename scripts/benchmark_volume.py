#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Benchmark-Skript zur Messung von Durchsatz, Kompression und RAM-Verbrauch
bei realen Datenvolumina (100.000 bis 1.000.000 Zeilen / Multi-Year SMARD/DWD).
Nutzt resource/os (Standardbibliothek) für exakte Linux-Speichermessung (ru_maxrss).
"""

import os
import resource
import time
from pathlib import Path
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq


def get_memory_mb() -> float:
    """Gibt den maximalen Resident Set Size (RSS) in MB zurück."""
    # Auf Linux gibt ru_maxrss Kilobytes zurück
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def generate_benchmark_csv(path: Path, n_rows: int):
    """Generiert realistische Zeitreihendaten analog zu SMARD/DWD."""
    print(f"Generiere Test-Datensatz mit {n_rows:,} Zeilen...")
    dates = pd.date_range("2020-01-01", periods=n_rows, freq="15min")
    np.random.seed(42)

    df = pd.DataFrame(
        {
            "timestamp": dates.strftime("%Y-%m-%dT%H:%M:%S"),
            "netzlast_gesamt_mwh": np.random.uniform(
                35000, 75000, size=n_rows
            ).round(2),
            "erzeugung_wind_onshore_mwh": np.random.uniform(
                1000, 30000, size=n_rows
            ).round(2),
            "erzeugung_wind_offshore_mwh": np.random.uniform(
                500, 8000, size=n_rows
            ).round(2),
            "erzeugung_photovoltaik_mwh": np.random.uniform(
                0, 35000, size=n_rows
            ).round(2),
            "day_ahead_preis_eur_mwh": np.random.normal(
                65, 25, size=n_rows
            ).round(2),
            "regelenergie_saldo_mw": np.random.normal(0, 150, size=n_rows).round(
                1
            ),
            "temperatur_celsius": np.random.normal(11.5, 7.0, size=n_rows).round(
                1
            ),
            "status_code": np.random.choice(
                ["ORIGINAL", "INTERPOLATED", "VERIFIED"], size=n_rows
            ),
        }
    )

    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, sep=";")
    csv_bytes = path.stat().st_size
    print(f"CSV gespeichert: {path} ({csv_bytes / 1024 / 1024:.2f} MB)")
    return csv_bytes


def run_chunked_parquet_conversion(
    csv_path: Path, parquet_path: Path, chunk_size: int = 50_000
):
    """Führt eine speichereffiziente Chunk-Konvertierung nach Parquet (ZSTD) durch."""
    start_time = time.perf_counter()

    parquet_writer = None
    total_rows = 0

    read_options = pv.ReadOptions(block_size=chunk_size * 128)
    parse_options = pv.ParseOptions(delimiter=";")

    with pv.open_csv(
        csv_path, read_options=read_options, parse_options=parse_options
    ) as reader:
        for batch in reader:
            table = pa.Table.from_batches([batch])
            total_rows += table.num_rows

            if parquet_writer is None:
                parquet_writer = pq.ParquetWriter(
                    str(parquet_path),
                    table.schema,
                    compression="zstd",
                    compression_level=6,
                    use_dictionary=True,
                )
            parquet_writer.write_table(table)

    if parquet_writer:
        parquet_writer.close()

    duration = time.perf_counter() - start_time
    parquet_bytes = parquet_path.stat().st_size
    peak_mem = get_memory_mb()

    return {
        "rows": total_rows,
        "duration_sec": duration,
        "peak_mem_mb": peak_mem,
        "parquet_bytes": parquet_bytes,
        "throughput_rows_sec": total_rows / duration if duration > 0 else 0,
        "throughput_mb_sec": (
            (csv_path.stat().st_size / 1024 / 1024) / duration
            if duration > 0
            else 0
        ),
    }


def main():
    bench_dir = Path("/tmp/dqp_benchmarks")
    bench_dir.mkdir(parents=True, exist_ok=True)

    scenarios = [
        ("Kompakt (Demo)", 10_000, 5_000),
        ("Multi-Month (100k)", 100_000, 25_000),
        ("Multi-Year Real-World (500k)", 500_000, 50_000),
        ("High-Volume Enterprise (1 Mio)", 1_000_000, 50_000),
    ]

    results = []

    for label, n_rows, chunk_size in scenarios:
        csv_file = bench_dir / f"bench_{n_rows}.csv"
        parquet_file = bench_dir / f"bench_{n_rows}.parquet"

        csv_size = generate_benchmark_csv(csv_file, n_rows)
        metrics = run_chunked_parquet_conversion(
            csv_file, parquet_file, chunk_size
        )

        reduction = (1 - (metrics["parquet_bytes"] / csv_size)) * 100
        factor = (
            csv_size / metrics["parquet_bytes"]
            if metrics["parquet_bytes"] > 0
            else 0
        )

        results.append(
            {
                "Szenario": label,
                "Zeilen": f"{n_rows:,}",
                "CSV-Größe": f"{csv_size / 1024 / 1024:.2f} MB",
                "Parquet (ZSTD)": f"{metrics['parquet_bytes'] / 1024 / 1024:.2f} MB",
                "Ersparnis": f"{reduction:.1f} % ({factor:.1f}x)",
                "Laufzeit": f"{metrics['duration_sec']:.2f} s",
                "Durchsatz": (
                    f"{metrics['throughput_rows_sec']:,.0f} Z/s"
                    f" ({metrics['throughput_mb_sec']:.1f} MB/s)"
                ),
                "Peak-RAM (RSS)": f"{metrics['peak_mem_mb']:.1f} MB",
            }
        )

        # Cleanup temp files
        csv_file.unlink(missing_ok=True)
        parquet_file.unlink(missing_ok=True)

    print("\n" + "=" * 105)
    print("BENCHMARK-ERGEBNISSE (CHUNKED PARQUET / ZSTD PIPELINE)")
    print("=" * 105)
    df_res = pd.DataFrame(results)
    print(df_res.to_string(index=False))
    print("=" * 105)


if __name__ == "__main__":
    main()
