#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Lernende Anomalieprüfung (Stufe 2: Isolation Forest mit Begründungspflicht).
Markiert mehrdimensionale Auffälligkeiten auf numerischen Spalten, ohne Daten
jemals still zu verändern oder zu löschen.
Für jede markierte Zeile wird zwingend eine nachvollziehbare, metrikbasierte
Begründung (Hauptspalte, beobachteter Wert, Median, z-Score und IQR-Abstand)
in 'metadata.ml_anomaly_flags' und zusammenfassend in 'metadata.quality_log'
gespeichert.
"""

import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from src.config import AppConfig, load_config
from src.database import get_connection, init_database

EXCLUDED_ID_COLUMNS = ("timestamp_ms", "dwd_station_id", "row_index", "id")


def run_isolation_forest_on_table(
    table_name: str,
    run_id: Optional[str] = None,
    contamination: float = 0.03,
    config: Optional[AppConfig] = None,
) -> Dict[str, object]:
    """
    Führt Isolation Forest auf den numerischen Kennzahlen einer Tabelle aus,
    ermittelt für jeden Treffer die hauptverantwortliche Spalte samt Kennzahl-Regel
    und protokolliert die Markierungen transparent.
    """
    cfg = config or load_config()
    init_database(cfg)
    active_run_id = run_id or datetime.now(timezone.utc).strftime("ml_%Y%m%d_%H%M%S")
    flagged_at = datetime.now(timezone.utc).replace(tzinfo=None)

    conn = get_connection(cfg)
    try:
        df = conn.execute(f"SELECT * FROM {table_name};").df()
        total_rows = len(df)
        if total_rows < 20:
            return {"table_name": table_name, "flagged_count": 0, "status": "skipped_too_small"}

        num_cols = [
            c
            for c in df.columns
            if pd.api.types.is_numeric_dtype(df[c])
            and not pd.api.types.is_bool_dtype(df[c])
            and c.lower() not in EXCLUDED_ID_COLUMNS
        ]
        if not num_cols:
            return {"table_name": table_name, "flagged_count": 0, "status": "no_numeric_columns"}

        work_df = df[num_cols].astype(float).copy()
        medians: Dict[str, float] = {}
        means: Dict[str, float] = {}
        stds: Dict[str, float] = {}
        iqrs: Dict[str, float] = {}

        for col in num_cols:
            col_series = work_df[col].dropna()
            med = float(col_series.median()) if len(col_series) > 0 else 0.0
            mean_val = float(col_series.mean()) if len(col_series) > 0 else 0.0
            std_val = float(col_series.std()) if len(col_series) > 1 else 1.0
            if std_val == 0.0 or np.isnan(std_val):
                std_val = 1.0
            q25 = float(np.percentile(col_series, 25)) if len(col_series) > 0 else 0.0
            q75 = float(np.percentile(col_series, 75)) if len(col_series) > 0 else 1.0
            iqr_val = max(q75 - q25, 1e-6)

            medians[col] = med
            means[col] = mean_val
            stds[col] = std_val
            iqrs[col] = iqr_val
            work_df[col] = work_df[col].fillna(med)

        model = IsolationForest(
            n_estimators=150,
            contamination=contamination,
            random_state=42,
        )
        preds = model.fit_predict(work_df.values)
        scores = model.decision_function(work_df.values)

        flagged_entries: List[tuple] = []
        primary_col_counts: Dict[str, int] = {}

        for idx in range(total_rows):
            if preds[idx] != -1:
                continue

            # Erklärbare Ursache bestimmen: Welche Spalte hat die größte relative Abweichung (|z-Score|)?
            best_col = num_cols[0]
            best_abs_z = -1.0
            best_z = 0.0
            best_iqr_factor = 0.0

            for col in num_cols:
                val = float(work_df.iloc[idx][col])
                z_val = (val - means[col]) / stds[col]
                if abs(z_val) > best_abs_z:
                    best_abs_z = abs(z_val)
                    best_z = z_val
                    best_col = col
                    best_iqr_factor = abs(val - medians[col]) / iqrs[col]

            obs_val = float(work_df.iloc[idx][best_col])
            med_val = medians[best_col]
            anom_score = float(scores[idx])
            primary_col_counts[best_col] = primary_col_counts.get(best_col, 0) + 1

            direction = "oberhalb" if obs_val >= med_val else "unterhalb"
            reason_rule = (
                f"Markiert durch Isolation Forest (Score {anom_score:.3f}): "
                f"Spalte '{best_col}' liegt mit {obs_val:.2f} deutlich {direction} "
                f"des Medians ({med_val:.2f}) | z-Score: {best_z:+.2f} | "
                f"Abweichung: {best_iqr_factor:.2f}x IQR."
            )

            flag_id = f"{active_run_id}_{table_name}_{idx}_{uuid.uuid4().hex[:6]}"
            flagged_entries.append(
                (
                    flag_id,
                    active_run_id,
                    table_name,
                    int(idx),
                    best_col,
                    round(obs_val, 4),
                    round(med_val, 4),
                    round(best_z, 4),
                    round(anom_score, 4),
                    reason_rule,
                    flagged_at,
                )
            )

        if flagged_entries:
            conn.executemany(
                """
                INSERT OR REPLACE INTO metadata.ml_anomaly_flags (
                    flag_id, run_id, table_name, row_index, primary_column,
                    observed_value, column_median, z_score, anomaly_score,
                    reason_rule, flagged_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                flagged_entries,
            )

            # Zusammenfassung je Hauptspalte auch im zentralen quality_log hinterlegen
            for col_name, hit_cnt in primary_col_counts.items():
                share_pct = round((hit_cnt / total_rows) * 100.0, 2)
                check_id = f"{active_run_id}_ml_{table_name}_{col_name}_{uuid.uuid4().hex[:6]}"
                explanation = (
                    f"{hit_cnt} Zeilen von Isolation Forest als mehrdimensionale Auffälligkeit "
                    f"markiert (Hauptursache in Spalte '{col_name}', Median={medians[col_name]:.2f}; "
                    "Einzelnachweise mit z-Score und IQR-Faktor in 'metadata.ml_anomaly_flags')."
                )
                conn.execute(
                    """
                    INSERT INTO metadata.quality_log (
                        check_id, run_id, stage, source_name, table_name,
                        column_name, rule_name, affected_count, total_count,
                        share_pct, is_recurring, priority_level, explanation, checked_at
                    ) VALUES (?, ?, 'ml_stage2', ?, ?, ?, 'isolation_forest_anomaly_flag', ?, ?, ?, false, 'NORMAL', ?, ?);
                    """,
                    [
                        check_id,
                        active_run_id,
                        table_name.replace("staging.", ""),
                        table_name,
                        col_name,
                        int(hit_cnt),
                        int(total_rows),
                        share_pct,
                        explanation,
                        flagged_at,
                    ],
                )

        return {
            "table_name": table_name,
            "total_rows": total_rows,
            "flagged_count": len(flagged_entries),
            "primary_columns": primary_col_counts,
            "status": "completed",
        }
    finally:
        conn.close()


def run_all_ml_anomaly_checks(config: Optional[AppConfig] = None) -> List[Dict[str, object]]:
    """Führt die lernende Anomalieprüfung (Stufe 2) für alle Staging-Tabellen aus."""
    cfg = config or load_config()
    init_database(cfg)
    run_id = datetime.now(timezone.utc).strftime("ml_%Y%m%d_%H%M%S")

    conn = get_connection(cfg)
    try:
        staging_tables = [
            row[0]
            for row in conn.execute(
                """
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'staging';
                """
            ).fetchall()
        ]
    finally:
        conn.close()

    results: List[Dict[str, object]] = []
    for tbl in sorted(staging_tables):
        results.append(
            run_isolation_forest_on_table(
                table_name=f"staging.{tbl}",
                run_id=run_id,
                config=cfg,
            )
        )
    return results


if __name__ == "__main__":
    summary = run_all_ml_anomaly_checks()
    for item in summary:
        print(
            f"Isolation Forest [{item['table_name']}]: "
            f"{item['flagged_count']} Auffälligkeiten mit Begründung markiert."
        )
