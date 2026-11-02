#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Datenabruf der ersten realen, öffentlichen Datenquelle:
SMARD.de (Bundesnetzagentur) – Strommarktdaten Deutschland.
Lizenz: Creative Commons Namensnennung 4.0 International (CC BY 4.0).
Speichert die Rohdaten unverändert in 'data/raw/' und registriert sie in 'metadata.quellen'.
"""

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional
import requests
from src.config import AppConfig, load_config
from src.database import register_source

# SMARD Filter-IDs:
# 410  = Netzlast (Realisierter Stromverbrauch Gesamt in MWh)
# 4068 = Photovoltaik-Einspeisung (MWh)
# 4067 = Wind-Onshore-Einspeisung (MWh)
SMARD_BASE_URL = "https://www.smard.de/app/chart_data"
SMARD_SOURCE_NAME = "Bundesnetzagentur | SMARD.de – Strommarktdaten Deutschland"
SMARD_LICENSE = "CC BY 4.0 (Creative Commons Namensnennung 4.0 International)"
SMARD_SERIES = {
    "410": "grid_load_mwh",
    "4068": "solar_generation_mwh",
    "4067": "wind_onshore_mwh",
}


def fetch_smard_timestamps(filter_id: str = "410", region: str = "DE") -> List[int]:
    """Ruft das öffentliche Zeitstempel-Verzeichnis der Bundesnetzagentur (SMARD) ab."""
    index_url = f"{SMARD_BASE_URL}/{filter_id}/{region}/index_hour.json"
    response = requests.get(index_url, timeout=20)
    response.raise_for_status()
    payload = response.json()
    timestamps = payload.get("timestamps", [])
    if not timestamps:
        raise ValueError(f"Keine Zeitstempel von SMARD erhalten ({index_url}).")
    return sorted(timestamps)


def fetch_smard_raw_data(config: Optional[AppConfig] = None) -> Dict[str, object]:
    """
    Lädt reale Stundendaten des deutschen Strommarkts von SMARD.de herunter,
    speichert das Original-JSON sowie die Roh-CSV unverändert in 'data/raw/'
    und trägt den Abruf in die Tabelle 'metadata.quellen' ein.
    """
    cfg = config or load_config()
    timestamps = fetch_smard_timestamps("410", "DE")
    selected_timestamps = timestamps[-cfg.smard_weeks_count :]

    merged_records: Dict[int, Dict[str, Optional[float]]] = {}
    raw_json_snapshots: Dict[str, list] = {}
    last_url = ""

    for filter_id, column_name in SMARD_SERIES.items():
        raw_json_snapshots[filter_id] = []
        for ts in selected_timestamps:
            chunk_url = f"{SMARD_BASE_URL}/{filter_id}/DE/{filter_id}_DE_hour_{ts}.json"
            last_url = chunk_url
            resp = requests.get(chunk_url, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            raw_json_snapshots[filter_id].append(data)

            for entry in data.get("series", []):
                epoch_ms, val = entry[0], entry[1]
                if epoch_ms not in merged_records:
                    merged_records[epoch_ms] = {col: None for col in SMARD_SERIES.values()}
                merged_records[epoch_ms][column_name] = val

    fetch_tag = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    raw_json_path = cfg.raw_dir / f"smard_de_hour_{fetch_tag}.json"
    raw_csv_path = cfg.raw_dir / f"smard_de_hour_{fetch_tag}.csv"

    # 1. Unverändertes Original-JSON in data/raw/ sichern
    raw_json_path.write_text(
        json.dumps(raw_json_snapshots, ensure_ascii=False), encoding="utf-8"
    )

    # 2. Unveränderte Roh-CSV mit Original-Zeitstempeln und Originalwerten speichern
    sorted_epochs = sorted(merged_records.keys())
    with raw_csv_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(
            [
                "timestamp_ms",
                "datetime_utc",
                "region",
                "grid_load_mwh",
                "solar_generation_mwh",
                "wind_onshore_mwh",
            ]
        )
        for epoch_ms in sorted_epochs:
            dt_iso = datetime.fromtimestamp(
                epoch_ms / 1000.0, tz=timezone.utc
            ).strftime("%Y-%m-%d %H:%M:%S")
            vals = merged_records[epoch_ms]
            writer.writerow(
                [
                    epoch_ms,
                    dt_iso,
                    "DE",
                    vals["grid_load_mwh"],
                    vals["solar_generation_mwh"],
                    vals["wind_onshore_mwh"],
                ]
            )

    source_id = f"smard_de_hour_{selected_timestamps[-1]}"
    register_source(
        source_id=source_id,
        name=SMARD_SOURCE_NAME,
        url=last_url,
        license_name=SMARD_LICENSE,
        raw_file_path=raw_csv_path,
        row_count=len(sorted_epochs),
        config=cfg,
    )

    return {
        "source_id": source_id,
        "raw_csv_path": str(raw_csv_path),
        "raw_json_path": str(raw_json_path),
        "rows_fetched": len(sorted_epochs),
        "file_size_bytes": raw_csv_path.stat().st_size,
    }


if __name__ == "__main__":
    result = fetch_smard_raw_data()
    print(
        f"Quelle erfolgreich geladen: {result['source_id']} | "
        f"Zeilen: {result['rows_fetched']} | "
        f"Datei: {result['raw_csv_path']} ({result['file_size_bytes']} Bytes)"
    )
