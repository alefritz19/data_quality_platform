#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Verwaltung der temporären Datenbereitstellung ('data/export').
Erzeugt versandfertige Pakete aus geprüften Mart-Parquet-Dateien inklusive
Qualitätsbericht (.md und .json) und setzt die Aufbewahrungsfristen
(72h / 5000 MB in 'prod', 24h / 500 MB in 'demo') mit automatischer Löschung durch.
"""

import shutil
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional
from src.config import AppConfig, load_config
from src.database import get_connection, init_database
from src.report_generator import build_quality_report


def cleanup_expired_exports(config: Optional[AppConfig] = None) -> Dict[str, int]:
    """
    Löscht abgelaufene Exportpakete sowie die ältesten Pakete bei Überschreitung
    des konfigurierten Gesamtspeicherlimits ('export_max_total_mb').
    """
    cfg = config or load_config()
    init_database(cfg)
    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    max_bytes = cfg.export_max_total_mb * 1024 * 1024

    deleted_expired = 0
    deleted_quota = 0

    conn = get_connection(cfg)
    try:
        active_rows = conn.execute(
            """
            SELECT package_id, parquet_path, report_path, bundle_zip_path, size_bytes, expires_at
            FROM metadata.export_packages
            WHERE is_deleted = false
            ORDER BY created_at ASC;
            """
        ).fetchall()

        remaining_rows = []
        for pkg_id, pq_path, rep_path, zip_path, sz_bytes, exp_at in active_rows:
            if exp_at <= now_utc:
                for file_str in (pq_path, rep_path, zip_path):
                    p = Path(file_str)
                    if p.exists():
                        p.unlink()
                conn.execute(
                    "UPDATE metadata.export_packages SET is_deleted = true WHERE package_id = ?;",
                    [pkg_id],
                )
                deleted_expired += 1
            else:
                remaining_rows.append((pkg_id, pq_path, rep_path, zip_path, int(sz_bytes)))

        # Speicherlimit prüfen und älteste Pakete zuerst entfernen
        total_active_bytes = sum(item[4] for item in remaining_rows)
        for pkg_id, pq_path, rep_path, zip_path, sz_bytes in remaining_rows:
            if total_active_bytes <= max_bytes:
                break
            for file_str in (pq_path, rep_path, zip_path):
                p = Path(file_str)
                if p.exists():
                    p.unlink()
            conn.execute(
                "UPDATE metadata.export_packages SET is_deleted = true WHERE package_id = ?;",
                [pkg_id],
            )
            total_active_bytes -= sz_bytes
            deleted_quota += 1
    finally:
        conn.close()

    return {
        "deleted_expired": deleted_expired,
        "deleted_quota": deleted_quota,
    }


def create_export_package(
    short_table_name: str,
    selected_columns: Optional[List[str]] = None,
    config: Optional[AppConfig] = None,
) -> Dict[str, object]:
    """
    Erstellt ein temporäres Bereitstellungspaket für eine freigegebene Mart-Tabelle
    bestehend aus Parquet-Datei, Qualitätsbericht und einem kombinierten ZIP-Archiv.
    """
    cfg = config or load_config()
    init_database(cfg)
    cleanup_expired_exports(cfg)

    created_at = datetime.now(timezone.utc).replace(tzinfo=None)
    expires_at = created_at + timedelta(hours=cfg.export_retention_hours)
    ts_label = created_at.strftime("%Y%m%d_%H%M%S")
    package_id = f"export_{short_table_name}_{ts_label}"

    export_parquet = cfg.export_dir / f"{package_id}.parquet"
    export_report_md = cfg.export_dir / f"{package_id}_bericht.md"
    export_bundle_zip = cfg.export_dir / f"{package_id}_komplettpaket.zip"

    conn = get_connection(cfg)
    try:
        if selected_columns:
            safe_cols = ", ".join(f'"{c}"' for c in selected_columns)
        else:
            safe_cols = "*"
        escaped_pq = str(export_parquet).replace("'", "''")
        conn.execute(
            f"""
            COPY (SELECT {safe_cols} FROM mart.{short_table_name})
            TO '{escaped_pq}' (FORMAT PARQUET, COMPRESSION ZSTD);
            """
        )
    finally:
        conn.close()

    report_info = build_quality_report(cfg)
    shutil.copy2(str(report_info["report_md_path"]), str(export_report_md))

    with zipfile.ZipFile(export_bundle_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.write(export_parquet, arcname=f"{short_table_name}.parquet")
        zf.write(export_report_md, arcname="qualitaetsbericht.md")
        json_rep = Path(str(report_info["report_json_path"]))
        if json_rep.exists():
            zf.write(json_rep, arcname="qualitaetsbericht.json")

    total_pkg_bytes = (
        export_parquet.stat().st_size
        + export_report_md.stat().st_size
        + export_bundle_zip.stat().st_size
    )

    conn = get_connection(cfg)
    try:
        conn.execute(
            """
            INSERT INTO metadata.export_packages (
                package_id, table_name, parquet_path, report_path,
                bundle_zip_path, size_bytes, created_at, expires_at, is_deleted
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, false);
            """,
            [
                package_id,
                f"mart.{short_table_name}",
                str(export_parquet),
                str(export_report_md),
                str(export_bundle_zip),
                total_pkg_bytes,
                created_at,
                expires_at,
            ],
        )
    finally:
        conn.close()

    cleanup_expired_exports(cfg)

    return {
        "package_id": package_id,
        "table_name": f"mart.{short_table_name}",
        "parquet_path": str(export_parquet),
        "report_path": str(export_report_md),
        "bundle_zip_path": str(export_bundle_zip),
        "size_bytes": total_pkg_bytes,
        "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "expires_at": expires_at.strftime("%Y-%m-%d %H:%M:%S UTC"),
    }


def get_export_storage_status(config: Optional[AppConfig] = None) -> Dict[str, object]:
    """Berechnet die aktuelle Belegung und verbleibende Kapazität des Exportbereichs."""
    cfg = config or load_config()
    init_database(cfg)
    cleanup_expired_exports(cfg)

    conn = get_connection(cfg)
    try:
        df_active = conn.execute(
            """
            SELECT package_id, table_name, parquet_path, report_path,
                   bundle_zip_path, size_bytes, created_at, expires_at
            FROM metadata.export_packages
            WHERE is_deleted = false
            ORDER BY created_at DESC;
            """
        ).df()
    finally:
        conn.close()

    used_bytes = int(df_active["size_bytes"].sum()) if not df_active.empty else 0
    used_mb = round(used_bytes / (1024 * 1024), 2)

    return {
        "active_packages_count": len(df_active),
        "used_mb": used_mb,
        "max_mb": cfg.export_max_total_mb,
        "retention_hours": cfg.export_retention_hours,
        "packages": df_active.to_dict(orient="records"),
    }


if __name__ == "__main__":
    status = get_export_storage_status()
    print(
        f"Exportbereich: {status['active_packages_count']} aktive Pakete | "
        f"Belegt: {status['used_mb']} / {status['max_mb']} MB | "
        f"Aufbewahrung: {status['retention_hours']} Stunden"
    )
