#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Datenabruf realer, öffentlicher Datenquellen:
1. SMARD.de (Bundesnetzagentur) – Strommarktdaten Deutschland (CC BY 4.0)
2. Deutscher Wetterdienst (DWD, Station 01420 Frankfurt/Main via BrightSky DWD-Mirror, GeoNutzV / CC BY 4.0)
Speichert die Rohdaten unverändert in 'data/raw/' und registriert sie in 'metadata.quellen'.
"""

import csv
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional
import requests
from src.config import AppConfig, load_config
from src.database import register_source

SMARD_BASE_URL = "https://www.smard.de/app/chart_data"
SMARD_SOURCE_NAME = "Bundesnetzagentur | SMARD.de – Strommarktdaten Deutschland"
SMARD_LICENSE = "CC BY 4.0 (Creative Commons Namensnennung 4.0 International)"
SMARD_SERIES = {
    "410": "grid_load_mwh",
    "4068": "solar_generation_mwh",
    "4067": "wind_onshore_mwh",
}

DWD_API_URL = "https://api.brightsky.dev/weather"
DWD_SOURCE_NAME = "Deutscher Wetterdienst (DWD) – Messstation 01420 Frankfurt am Main"
DWD_LICENSE = "GeoNutzV / CC BY 4.0 (DWD Open Data)"
DWD_STATION_ID = "01420"


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

    raw_json_path = cfg.raw_dir / "smard_strommarkt_de.json"
    raw_csv_path = cfg.raw_dir / "smard_strommarkt_de.csv"

    raw_json_path.write_text(
        json.dumps(raw_json_snapshots, ensure_ascii=False), encoding="utf-8"
    )

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


def fetch_dwd_weather_raw_data(config: Optional[AppConfig] = None) -> Dict[str, object]:
    """
    Lädt reale stündliche Wetterbeobachtungen des Deutschen Wetterdienstes (DWD,
    Station 01420 Frankfurt/Main) herunter, sichert sie in 'data/raw/' und
    registriert die Quelle in 'metadata.quellen'.
    """
    cfg = config or load_config()
    days_back = 7 * cfg.smard_weeks_count
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=days_back)

    date_str = start_dt.strftime("%Y-%m-%d")
    last_date_str = end_dt.strftime("%Y-%m-%d")
    request_url = (
        f"{DWD_API_URL}?dwd_station_id={DWD_STATION_ID}"
        f"&date={date_str}&last_date={last_date_str}"
    )

    resp = requests.get(request_url, timeout=25)
    resp.raise_for_status()
    payload = resp.json()
    weather_rows = payload.get("weather", [])
    if not weather_rows:
        raise ValueError(f"Keine Wetterdaten von DWD Station {DWD_STATION_ID} erhalten.")

    raw_json_path = cfg.raw_dir / "dwd_wetter_frankfurt.json"
    raw_csv_path = cfg.raw_dir / "dwd_wetter_frankfurt.csv"

    raw_json_path.write_text(
        json.dumps(payload, ensure_ascii=False), encoding="utf-8"
    )

    with raw_csv_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(
            [
                "datetime_utc",
                "dwd_station_id",
                "temperature_c",
                "wind_speed_kmh",
                "solar_irradiation_kwh_m2",
                "precipitation_mm",
                "relative_humidity_pct",
                "cloud_cover_pct",
            ]
        )
        for rec in weather_rows:
            ts_raw = str(rec.get("timestamp", "")).replace("T", " ")[:19]
            writer.writerow(
                [
                    ts_raw,
                    DWD_STATION_ID,
                    rec.get("temperature"),
                    rec.get("wind_speed"),
                    rec.get("solar"),
                    rec.get("precipitation"),
                    rec.get("relative_humidity"),
                    rec.get("cloud_cover"),
                ]
            )

    source_id = f"dwd_station_{DWD_STATION_ID}_{last_date_str}"
    register_source(
        source_id=source_id,
        name=DWD_SOURCE_NAME,
        url=request_url,
        license_name=DWD_LICENSE,
        raw_file_path=raw_csv_path,
        row_count=len(weather_rows),
        config=cfg,
    )

    return {
        "source_id": source_id,
        "raw_csv_path": str(raw_csv_path),
        "raw_json_path": str(raw_json_path),
        "rows_fetched": len(weather_rows),
        "file_size_bytes": raw_csv_path.stat().st_size,
    }


def fetch_all_public_sources(config: Optional[AppConfig] = None) -> List[Dict[str, object]]:
    """Ruft alle angebundenen realen öffentlichen Datenquellen (SMARD + DWD) ab."""
    cfg = config or load_config()
    results: List[Dict[str, object]] = []
    results.append(fetch_smard_raw_data(cfg))
    results.append(fetch_dwd_weather_raw_data(cfg))
    return results


if __name__ == "__main__":
    all_res = fetch_all_public_sources()
    for item in all_res:
        print(
            f"Quelle geladen: {item['source_id']} | "
            f"Zeilen: {item['rows_fetched']} | "
            f"Datei: {item['raw_csv_path']} ({item['file_size_bytes']} Bytes)"
        )
