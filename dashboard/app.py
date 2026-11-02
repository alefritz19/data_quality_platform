#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Streamlit-Anwendung der Plattform für Datenqualität.
Fokus auf 100 % Transparenz: Wählbare KPI-Karten, strukturierte Spalten- und
Aggregations-Tabellen, Einzelzeilen-Filter für Lücken und Anomalien sowie
kombinierte Diagramm-Tabellen-Ansichten auf geprüften 'mart'-Daten.
"""

import sys
from pathlib import Path
from typing import List
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.anomaly_model import run_all_ml_anomaly_checks
from src.config import load_config
from src.database import get_connection, init_database
from src.export_manager import create_export_package, get_export_storage_status
from src.file_converter import process_inbox
from src.final_gate import run_final_gate_all
from src.ingest_source import fetch_all_public_sources
from src.quality_checks import run_all_staging_quality_checks
from src.report_generator import build_quality_report
from src.staging_loader import stage_all_parquet_files

TECHNICAL_ID_COLUMNS = {"timestamp_ms", "dwd_station_id", "row_index", "id"}

st.set_page_config(
    page_title="Plattform für Datenqualität | DuckDB · Parquet · Dagster",
    layout="wide",
    initial_sidebar_state="expanded",
)


def fetch_dataframe(query: str, params: list = None) -> pd.DataFrame:
    """Liest eine Abfrage aus DuckDB und gibt ein Pandas-DataFrame zurück."""
    cfg = load_config()
    init_database(cfg)
    conn = get_connection(cfg)
    try:
        if params:
            return conn.execute(query, params).df()
        return conn.execute(query).df()
    except Exception as exc:
        st.error(f"Datenbankabfrage fehlgeschlagen: {exc}")
        return pd.DataFrame()
    finally:
        conn.close()


def list_mart_tables() -> List[str]:
    """Gibt alle für die Visualisierung freigegebenen Tabellen im Schema 'mart' zurück."""
    df = fetch_dataframe(
        """
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'mart'
        ORDER BY table_name;
        """
    )
    if df.empty:
        return []
    return df["table_name"].tolist()


def build_column_transparency_table(
    df_mart: pd.DataFrame,
    table_name: str,
    df_quality: pd.DataFrame,
    df_ml_flags: pd.DataFrame,
) -> pd.DataFrame:
    """
    Erzeugt den vollständigen Spalten-Steckbrief (Transparenz-Matrix) für eine Tabelle:
    Datentyp, Gesamtzeilen, gültige Werte, fehlende Werte, Vollständigkeit (%),
    Min, Median, Durchschnitt, Max, Summe sowie Anzahl der Qualitäts- und ML-Befunde.
    """
    total_rows = len(df_mart)
    staging_name = f"staging.{table_name}"
    rows = []

    for col in df_mart.columns:
        series = df_mart[col]
        if pd.api.types.is_string_dtype(series) or series.dtype == object:
            missing_cnt = int((series.isna() | (series.astype(str).str.strip() == "")).sum())
        else:
            missing_cnt = int(series.isna().sum())

        valid_cnt = total_rows - missing_cnt
        completeness_pct = round((valid_cnt / total_rows) * 100.0, 2) if total_rows > 0 else 0.0

        is_num = (
            pd.api.types.is_numeric_dtype(series)
            and not pd.api.types.is_bool_dtype(series)
            and col.lower() not in TECHNICAL_ID_COLUMNS
        )
        clean_num = series.dropna().astype(float) if is_num else pd.Series(dtype=float)

        min_val = round(float(clean_num.min()), 3) if len(clean_num) > 0 else None
        med_val = round(float(clean_num.median()), 3) if len(clean_num) > 0 else None
        mean_val = round(float(clean_num.mean()), 3) if len(clean_num) > 0 else None
        max_val = round(float(clean_num.max()), 3) if len(clean_num) > 0 else None
        sum_val = round(float(clean_num.sum()), 2) if len(clean_num) > 0 else None

        rule_outliers = 0
        if not df_quality.empty:
            sub_q = df_quality[
                (df_quality["table_name"] == staging_name)
                & (df_quality["column_name"] == col)
                & (df_quality["stage"] == "final_gate")
                & (df_quality["rule_name"] == "statistical_outliers_iqr_zscore")
            ]
            if not sub_q.empty:
                rule_outliers = int(sub_q.iloc[0]["affected_count"])

        ml_hits = 0
        if not df_ml_flags.empty:
            sub_ml = df_ml_flags[
                (df_ml_flags["table_name"] == staging_name)
                & (df_ml_flags["primary_column"] == col)
            ]
            ml_hits = len(sub_ml)

        status_text = (
            "100 % vollständig"
            if missing_cnt == 0 and rule_outliers == 0 and ml_hits == 0
            else f"Fehlend: {missing_cnt} | Ausreißer: {rule_outliers} | ML-Flags: {ml_hits}"
        )

        rows.append(
            {
                "Spalte": col,
                "Rolle / Typ": (
                    "Technische ID"
                    if col.lower() in TECHNICAL_ID_COLUMNS
                    else ("Zeitstempel" if "date" in col.lower() or "time" in col.lower() else ("Kennzahl (numerisch)" if is_num else "Text / Kategorie"))
                ),
                "Gesamtzeilen": total_rows,
                "Gültige Werte": valid_cnt,
                "Fehlende Werte": missing_cnt,
                "Vollständigkeit (%)": completeness_pct,
                "Minimum": min_val,
                "Median": med_val,
                "Durchschnitt (Ø)": mean_val,
                "Maximum": max_val,
                "Summe": sum_val,
                "Statistische Ausreißer (Stufe 1)": rule_outliers,
                "Isolation-Forest-Flags (Stufe 2)": ml_hits,
                "Transparenz-Status": status_text,
            }
        )
    return pd.DataFrame(rows)


cfg = load_config()
init_database(cfg)

# ── SEITENLEISTE: KONTEXT, NAVIGATION & GESCHÜTZTE AKTIONEN ──────────────────
with st.sidebar:
    st.markdown("### Plattform für Datenqualität")
    st.caption(f"Umgebung: `{cfg.app_env.upper()}` | Engine: `DuckDB + Parquet`")
    st.markdown("---")

    selected_section = st.radio(
        "Ansicht wählen",
        [
            "1. Übersicht & Quellen",
            "2. Qualitätsprüfungen (quality_log)",
            "3. Nicht verarbeitbar",
            "4. Freigegebene Daten: KPI-Karten, Tabellen & Diagramme",
            "5. Regelbasierter Qualitätsbericht",
            "6. Bereitstellung & Download fertiger Daten",
        ],
    )

    st.markdown("---")
    st.markdown("#### Dateiannahme & Pipeline-Steuerung")

    pin_input = ""
    if cfg.demo_read_only:
        pin_input = st.text_input(
            "Admin-PIN (Schreibschutz aktiv):",
            type="password",
            help="Im öffentlichen Demo-Kontext sind Schreib- und Trigger-Funktionen gesperrt.",
        )

    is_authorized = (not cfg.demo_read_only) or (pin_input == cfg.admin_pin)

    uploaded_file = st.file_uploader(
        f"Datei in Eingangsordner legen (max. {cfg.max_upload_size_mb} MB)",
        type=["csv", "tsv", "xlsx", "xls", "json", "jsonl", "parquet"],
        disabled=not is_authorized,
    )
    if uploaded_file is not None and is_authorized:
        target_inbox_file = cfg.inbox_dir / uploaded_file.name
        target_inbox_file.write_bytes(uploaded_file.getvalue())
        st.success(f"Datei '{uploaded_file.name}' in 'data/inbox' abgelegt.")

    if st.button("Reale Quellen (SMARD + DWD) + Pipeline ausführen", use_container_width=True):
        if not is_authorized:
            st.error("Schreibschutz aktiv: Bitte gültige Admin-PIN eingeben.")
        else:
            with st.spinner("Lade reale Daten (SMARD + DWD), konvertiere nach Parquet und prüfe Qualität..."):
                sources_list = fetch_all_public_sources(cfg)
                for src_info in sources_list:
                    raw_csv = Path(str(src_info["raw_csv_path"]))
                    inbox_copy = cfg.inbox_dir / raw_csv.name
                    inbox_copy.write_bytes(raw_csv.read_bytes())
                process_inbox(cfg)
                stage_all_parquet_files(cfg)
                run_all_staging_quality_checks(stage="staging", config=cfg)
                run_all_ml_anomaly_checks(cfg)
                run_final_gate_all(cfg)
                build_quality_report(cfg)
                st.success("Pipeline-Durchlauf und Freigabeprüfung abgeschlossen.")

    if st.button("Nur Eingangsordner (inbox) verarbeiten", use_container_width=True):
        if not is_authorized:
            st.error("Schreibschutz aktiv: Bitte gültige Admin-PIN eingeben.")
        else:
            with st.spinner("Konvertiere Eingangsdateien nach Parquet und führe Prüfungen aus..."):
                process_inbox(cfg)
                stage_all_parquet_files(cfg)
                run_all_staging_quality_checks(stage="staging", config=cfg)
                run_all_ml_anomaly_checks(cfg)
                run_final_gate_all(cfg)
                build_quality_report(cfg)
                st.success("Eingangsordner erfolgreich verarbeitet.")


# ── DATEN FÜR GLOBALE KPIS LADEN ─────────────────────────────────────────────
df_sources = fetch_dataframe("SELECT * FROM metadata.quellen ORDER BY fetched_at DESC;")
df_conversions = fetch_dataframe(
    "SELECT * FROM metadata.file_conversions ORDER BY converted_at DESC;"
)
df_quality = fetch_dataframe(
    "SELECT * FROM metadata.quality_log ORDER BY checked_at DESC;"
)
df_ml_flags_all = fetch_dataframe(
    "SELECT * FROM metadata.ml_anomaly_flags ORDER BY flagged_at DESC, ABS(z_score) DESC;"
)
df_gate = fetch_dataframe(
    "SELECT * FROM metadata.mart_gate_log ORDER BY checked_at DESC;"
)
df_unparseable_files = fetch_dataframe(
    "SELECT * FROM metadata.unparseable_files ORDER BY detected_at DESC;"
)
df_unparseable_rows = fetch_dataframe(
    "SELECT * FROM metadata.unparseable_rows ORDER BY detected_at DESC;"
)
mart_tables = list_mart_tables()

issues_df = (
    df_quality[df_quality["affected_count"] > 0]
    if not df_quality.empty
    else pd.DataFrame()
)
prio_issues_count = (
    int((issues_df["priority_level"] == "PRIORISIERT").sum())
    if not issues_df.empty
    else 0
)
avg_compression = (
    float(df_conversions["compression_ratio_pct"].mean())
    if not df_conversions.empty
    else 0.0
)
avg_duration_ms = (
    float(df_conversions["duration_ms"].mean())
    if not df_conversions.empty
    else 0.0
)
unparseable_total = len(df_unparseable_files) + len(df_unparseable_rows)

# ── GLOBALE SYSTEM-KPI-KARTEN ────────────────────────────────────────────────
st.title("Transparente Plattform für Datenqualität")
st.caption(
    "Ausschließliche Verarbeitung realer Datenquellen · Automatische Parquet-Konvertierung · "
    "Mehrstufige Qualitätsprüfung · Wählbare KPI-Karten & Transparenz-Tabellen"
)

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Erfasste Quellen", f"{len(df_sources)}")
k2.metric(
    "Parquet-Konvertierungen",
    f"{len(df_conversions)}",
    f"Ø {avg_compression:.1f} % kleiner ({avg_duration_ms:.0f} ms)",
)
k3.metric("Freigegebene Mart-Tabellen", f"{len(mart_tables)}")
k4.metric(
    "Qualitätsbefunde (> 0)",
    f"{len(issues_df)}",
    f"davon {prio_issues_count} priorisiert",
)
k5.metric(
    "Nicht verarbeitbar",
    f"{unparseable_total}",
    f"{len(df_unparseable_files)} Dateien / {len(df_unparseable_rows)} Werte",
)

st.markdown("---")


# ══════════════════════════════════════════════════════════════════════════════
# ANSICHT 1: ÜBERSICHT, QUELLEN & PARQUET-KONVERTIERUNG
# ══════════════════════════════════════════════════════════════════════════════
if selected_section.startswith("1."):
    st.subheader("1. Dokumentierte Datenquellen (`metadata.quellen`)")
    if df_sources.empty:
        st.info("Noch keine Datenquellen protokolliert.")
    else:
        st.dataframe(df_sources, use_container_width=True, hide_index=True)

    st.subheader("2. Dateiannahme & Parquet-Konvertierung (`metadata.file_conversions`)")
    if df_conversions.empty:
        st.info("Noch keine Parquet-Konvertierungen durchgeführt.")
    else:
        st.dataframe(
            df_conversions[
                [
                    "file_id",
                    "original_filename",
                    "file_format",
                    "size_before_bytes",
                    "size_after_bytes",
                    "compression_ratio_pct",
                    "row_count",
                    "chunks_count",
                    "duration_ms",
                    "converted_at",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )

    st.subheader("3. Abschließende Freigabeprüfung vor Visualisierung (`metadata.mart_gate_log`)")
    if df_gate.empty:
        st.info("Noch keine Freigabeprüfung protokolliert.")
    else:
        st.dataframe(df_gate, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# ANSICHT 2: QUALITÄTSPRÜFUNGEN (QUALITY_LOG & ISOLATION FOREST)
# ══════════════════════════════════════════════════════════════════════════════
elif selected_section.startswith("2."):
    st.subheader("Zweistufige Qualitätsprüfung: Regelwerk (Stufe 1) & Isolation Forest (Stufe 2)")

    if df_quality.empty:
        st.info("Noch keine Qualitätsprüfungen vorhanden.")
    else:
        # KPI-Karten für die Qualitätsprüfung
        stage1_hits = int(
            (
                (df_quality["stage"] == "final_gate")
                & (df_quality["affected_count"] > 0)
            ).sum()
        )
        stage2_hits = len(df_ml_flags_all)
        missing_total = int(
            df_quality[
                (df_quality["stage"] == "final_gate")
                & (df_quality["rule_name"] == "missing_values")
            ]["affected_count"].sum()
        )
        outlier_total = int(
            df_quality[
                (df_quality["stage"] == "final_gate")
                & (df_quality["rule_name"] == "statistical_outliers_iqr_zscore")
            ]["affected_count"].sum()
        )

        qk1, qk2, qk3, qk4 = st.columns(4)
        qk1.metric(
            "Aktive Regel-Befunde (final_gate)",
            f"{stage1_hits}",
            f"davon {prio_issues_count} priorisiert",
        )
        qk2.metric(
            "Erkannte fehlende Werte (gesamt)",
            f"{missing_total}",
            "Regel: missing_values",
        )
        qk3.metric(
            "Statistische Ausreißer (IQR / z-Score)",
            f"{outlier_total}",
            "Stufe 1: Regelbasiert",
        )
        qk4.metric(
            "Isolation-Forest-Markierungen",
            f"{stage2_hits}",
            "Stufe 2: Mit Kennzahl-Begründung",
        )

        st.markdown("#### 1. Kompakte Zusammenfassung der Befunde nach Tabelle, Spalte & Regel")
        summary_q = (
            df_quality[df_quality["affected_count"] > 0]
            .groupby(
                ["table_name", "column_name", "rule_name", "priority_level"],
                as_index=False,
            )
            .agg(
                Betroffene_Zeilen=("affected_count", "max"),
                Gesamt_Zeilen=("total_count", "max"),
                Anteil_Prozent=("share_pct", "max"),
                Wiederkehrend=("is_recurring", "max"),
                Begründung=("explanation", "first"),
            )
            .sort_values(["priority_level", "Anteil_Prozent"], ascending=[False, False])
        )
        st.dataframe(summary_q, use_container_width=True, hide_index=True)

        st.markdown("#### 2. Detailliertes Prüfprotokoll (`metadata.quality_log`) mit Filter")
        f_col1, f_col2, f_col3, f_col4 = st.columns(4)
        with f_col1:
            only_with_hits = st.checkbox("Nur Befunde mit Anzahl > 0", value=True)
        with f_col2:
            stage_filter = st.selectbox(
                "Prüfstufe (Stage)",
                ["ALLE"] + sorted(df_quality["stage"].unique().tolist()),
            )
        with f_col3:
            prio_filter = st.selectbox(
                "Priorität",
                ["ALLE", "PRIORISIERT", "NORMAL"],
            )
        with f_col4:
            rule_filter = st.selectbox(
                "Regel",
                ["ALLE"] + sorted(df_quality["rule_name"].unique().tolist()),
            )

        filtered = df_quality.copy()
        if only_with_hits:
            filtered = filtered[filtered["affected_count"] > 0]
        if stage_filter != "ALLE":
            filtered = filtered[filtered["stage"] == stage_filter]
        if prio_filter != "ALLE":
            filtered = filtered[filtered["priority_level"] == prio_filter]
        if rule_filter != "ALLE":
            filtered = filtered[filtered["rule_name"] == rule_filter]

        st.dataframe(
            filtered[
                [
                    "checked_at",
                    "stage",
                    "priority_level",
                    "is_recurring",
                    "table_name",
                    "column_name",
                    "rule_name",
                    "affected_count",
                    "total_count",
                    "share_pct",
                    "explanation",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )

        # Wählbares Diagramm für Qualitätsbefunde (Balken-, Linien-, Kreisdiagramm)
        q_chart_choice = st.selectbox(
            "Qualitätsdiagramm wählen:",
            [
                "Balkendiagramm: Befunde (Anzahl) je Regel",
                "Liniendiagramm: Anteil betroffener Zeilen (%) je Spalte",
                "Kreisdiagramm: Verteilung der Befunde nach Spalte",
                "Kein Diagramm anzeigen",
            ],
        )
        if not filtered.empty and q_chart_choice.startswith("Balkendiagramm"):
            agg_rule = (
                filtered.groupby("rule_name", as_index=False)["affected_count"]
                .sum()
                .sort_values("affected_count", ascending=True)
            )
            fig_q = px.bar(
                agg_rule,
                x="affected_count",
                y="rule_name",
                orientation="h",
                title="Anzahl betroffener Einträge je Qualitätsregel",
                labels={"affected_count": "Anzahl Befunde", "rule_name": "Regel"},
            )
            fig_q.update_layout(height=340)
            st.plotly_chart(fig_q, use_container_width=True)
        elif not filtered.empty and q_chart_choice.startswith("Liniendiagramm"):
            agg_line = (
                filtered.groupby("column_name", as_index=False)["share_pct"]
                .max()
                .sort_values("column_name")
            )
            fig_q = px.line(
                agg_line,
                x="column_name",
                y="share_pct",
                markers=True,
                title="Maximaler Anteil betroffener Zeilen (%) je Spalte",
                labels={"column_name": "Spalte", "share_pct": "Anteil (%)"},
            )
            fig_q.update_layout(height=340)
            st.plotly_chart(fig_q, use_container_width=True)
        elif not filtered.empty and q_chart_choice.startswith("Kreisdiagramm"):
            agg_col = (
                filtered.groupby("column_name", as_index=False)["affected_count"]
                .sum()
            )
            fig_q = px.pie(
                agg_col,
                names="column_name",
                values="affected_count",
                title="Anteil der Qualitätsbefunde nach Spalte",
            )
            fig_q.update_layout(height=340)
            st.plotly_chart(fig_q, use_container_width=True)

        st.markdown("---")
        st.markdown("#### 3. Lernende Anomalieprüfung (Stufe 2: Isolation Forest mit Begründungspflicht)")
        if df_ml_flags_all.empty:
            st.info("Noch keine Markierungen aus der lernenden Anomalieprüfung vorhanden.")
        else:
            st.caption(
                "Das Modell verändert oder löscht keine Daten. Jede Markierung enthält die "
                "verantwortliche Hauptspalte, den beobachteten Wert, den Median sowie z-Score und IQR-Abstand."
            )
            ml_tbl_filter = st.selectbox(
                "Tabelle für Isolation-Forest-Markierungen filtern:",
                ["ALLE"] + sorted(df_ml_flags_all["table_name"].unique().tolist()),
            )
            df_ml_show = (
                df_ml_flags_all
                if ml_tbl_filter == "ALLE"
                else df_ml_flags_all[df_ml_flags_all["table_name"] == ml_tbl_filter]
            )
            st.dataframe(
                df_ml_show[
                    [
                        "flagged_at",
                        "table_name",
                        "row_index",
                        "primary_column",
                        "observed_value",
                        "column_median",
                        "z_score",
                        "anomaly_score",
                        "reason_rule",
                    ]
                ],
                use_container_width=True,
                hide_index=True,
            )


# ══════════════════════════════════════════════════════════════════════════════
# ANSICHT 3: ABSCHNITT 'NICHT VERARBEITBAR'
# ══════════════════════════════════════════════════════════════════════════════
elif selected_section.startswith("3."):
    st.subheader("Abschnitt: Nicht verarbeitbar (Herkunft & Begründung)")
    st.write(
        "Daten oder Dateien, die nicht konvertiert oder berechnet werden konnten, "
        "werden hier vollständig mit Quelle, Spalte und Fehlerursache ausgewiesen."
    )

    st.markdown("#### 1. Abgelehnte / nicht lesbare Dateien (`metadata.unparseable_files`)")
    if df_unparseable_files.empty:
        st.success("Keine abgelehnten Dateien vorhanden.")
    else:
        st.dataframe(df_unparseable_files, use_container_width=True, hide_index=True)

    st.markdown("#### 2. Nicht konvertierbare Einzelwerte (`metadata.unparseable_rows`)")
    if df_unparseable_rows.empty:
        st.success("Keine nicht konvertierbaren Einzelwerte vorhanden.")
    else:
        st.dataframe(df_unparseable_rows, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# ANSICHT 4: FREIGEGEBENE MART-DATEN — KPI-AUSWAHL, TABELLEN & DIAGRAMME
# ══════════════════════════════════════════════════════════════════════════════
elif selected_section.startswith("4."):
    st.subheader("Freigegebene Daten (`mart`): Wählbare KPI-Karten, Transparenz-Tabellen & Diagramme")

    if not mart_tables:
        st.warning(
            "Aktuell sind keine Tabellen im Schema `mart` freigegeben. "
            "Nur Daten, die den abschließenden Prüfschritt bestanden haben, erscheinen hier."
        )
    else:
        top_c1, top_c2 = st.columns([1, 2])
        with top_c1:
            chosen_table = st.selectbox("1. Freigegebene Mart-Tabelle wählen:", mart_tables)
        df_mart_full = fetch_dataframe(f"SELECT * FROM mart.{chosen_table};")

        all_cols = df_mart_full.columns.tolist()
        metric_cols = [
            c
            for c in all_cols
            if pd.api.types.is_numeric_dtype(df_mart_full[c])
            and not pd.api.types.is_bool_dtype(df_mart_full[c])
            and c.lower() not in TECHNICAL_ID_COLUMNS
        ]
        if not metric_cols:
            metric_cols = [
                c for c in all_cols if pd.api.types.is_numeric_dtype(df_mart_full[c])
            ]

        with top_c2:
            view_mode = st.selectbox(
                "2. Haupt-Darstellungsart wählen (Tabellen & KPIs stehen im Zentrum):",
                [
                    "1. KPI-Karten + Spalten-Steckbrief & Transparenz-Matrix (Standard – 100 % Übersicht)",
                    "2. KPI-Karten + Aggregierte Auswertungstabelle (Tages- / Gruppen-Kennzahlen)",
                    "3. KPI-Karten + Einzelzeilen-Inspektor (Filter für fehlende Werte & ML-Anomalien)",
                    "4. Kombinierte Ansicht: Aggregierte Datentabelle + Diagramm (Linie / Balken / Verteilung / Anteile)",
                ],
            )

        # ── TEIL A: KONFIGURIERBARE KPI-KARTEN FÜR DIE GEWÄHLTE TABELLE ────────
        st.markdown("#### A. Wählbare KPI-Karten der Tabelle (`mart." + chosen_table + "`)")
        kpi_cfg1, kpi_cfg2 = st.columns([2, 1])
        with kpi_cfg1:
            selected_kpi_cols = st.multiselect(
                "Kennzahlen (Spalten) für KPI-Karten auswählen:",
                options=metric_cols,
                default=metric_cols[:6],
            )
        with kpi_cfg2:
            kpi_agg_mode = st.selectbox(
                "Hauptwert auf den KPI-Karten:",
                [
                    "Durchschnitt (Ø)",
                    "Median",
                    "Maximum",
                    "Minimum",
                    "Summe (Gesamt)",
                    "Daten-Vollständigkeit (%)",
                ],
            )

        if selected_kpi_cols:
            staging_ref = f"staging.{chosen_table}"
            cols_per_row = min(len(selected_kpi_cols), 4)
            kpi_columns = st.columns(cols_per_row)
            total_r = len(df_mart_full)

            for idx, col_name in enumerate(selected_kpi_cols):
                target_card = kpi_columns[idx % cols_per_row]
                s_col = df_mart_full[col_name]
                clean_s = s_col.dropna().astype(float)
                valid_n = len(clean_s)
                missing_n = total_r - valid_n
                comp_pct = (valid_n / total_r * 100.0) if total_r > 0 else 0.0

                ml_cnt = 0
                if not df_ml_flags_all.empty:
                    ml_cnt = len(
                        df_ml_flags_all[
                            (df_ml_flags_all["table_name"] == staging_ref)
                            & (df_ml_flags_all["primary_column"] == col_name)
                        ]
                    )

                if valid_n == 0:
                    main_str = "Keine gültigen Werte"
                    sub_str = f"100 % fehlend ({missing_n}/{total_r})"
                else:
                    if kpi_agg_mode == "Durchschnitt (Ø)":
                        main_str = f"Ø {clean_s.mean():,.2f}"
                    elif kpi_agg_mode == "Median":
                        main_str = f"Median {clean_s.median():,.2f}"
                    elif kpi_agg_mode == "Maximum":
                        main_str = f"Max {clean_s.max():,.2f}"
                    elif kpi_agg_mode == "Minimum":
                        main_str = f"Min {clean_s.min():,.2f}"
                    elif kpi_agg_mode == "Summe (Gesamt)":
                        main_str = f"∑ {clean_s.sum():,.2f}"
                    else:
                        main_str = f"{comp_pct:.1f} % gültig"

                    sub_str = (
                        f"Gültig: {valid_n}/{total_r} ({comp_pct:.1f} %) | "
                        f"Min: {clean_s.min():,.1f} · Max: {clean_s.max():,.1f} | "
                        f"ML-Flags: {ml_cnt}"
                    )

                target_card.metric(label=col_name, value=main_str, delta=sub_str, delta_color="off")

        st.markdown("---")

        # ── TEIL B: GEWÄHLTE HAUPT-DARSTELLUNGSART ────────────────────────────
        transparency_df = build_column_transparency_table(
            df_mart_full, chosen_table, df_quality, df_ml_flags_all
        )

        # MODUS 1: SPALTEN-STECKBRIEF & TRANSPARENZ-MATRIX
        if view_mode.startswith("1."):
            st.markdown(
                "#### B. Spalten-Steckbrief & Transparenz-Matrix (vollständige Kennzahlen- und Qualitätsübersicht je Spalte)"
            )
            st.caption(
                "Diese Tabelle funktioniert unabhängig vom Dateityp für jede Datenquelle und zeigt "
                "auf einen Blick Vollständigkeit, statistische Kennzahlen sowie erkannte Ausreißer."
            )
            st.dataframe(transparency_df, use_container_width=True, hide_index=True)

            st.markdown("#### C. Vorschau der freigegebenen Datentabelle (`mart." + chosen_table + "`)")
            st.dataframe(df_mart_full.head(100), use_container_width=True, hide_index=True)

        # MODUS 2: AGGREGIERTE AUSWERTUNGSTABELLE (TAGES- / GRUPPEN-KENNZAHLEN)
        elif view_mode.startswith("2."):
            st.markdown("#### B. Aggregierte Kennzahlen-Tabelle (Tages- oder Kategorien-Übersicht)")
            time_cols = [
                c for c in all_cols if "date" in c.lower() or "time" in c.lower()
            ]
            agg_c1, agg_c2 = st.columns(2)
            with agg_c1:
                group_col = st.selectbox(
                    "Gruppierungsspalte (z. B. Zeitstempel für Tageswerte oder Kategorie):",
                    options=time_cols + [c for c in all_cols if c not in time_cols],
                )
            with agg_c2:
                target_metric = st.selectbox(
                    "Auszuwertende Kennzahl für die Gruppentabelle:",
                    options=metric_cols if metric_cols else all_cols,
                )

            work_df = df_mart_full.copy()
            if "date" in group_col.lower() or "time" in group_col.lower():
                work_df["_gruppe_"] = pd.to_datetime(
                    work_df[group_col], errors="coerce"
                ).dt.strftime("%Y-%m-%d")
                group_label = f"Tag ({group_col})"
            else:
                work_df["_gruppe_"] = work_df[group_col].astype(str)
                group_label = group_col

            work_df["_num_"] = pd.to_numeric(work_df[target_metric], errors="coerce")
            grouped_table = (
                work_df.groupby("_gruppe_", dropna=False)
                .agg(
                    Gesamt_Zeilen=("_num_", "size"),
                    Gueltige_Werte=("_num_", "count"),
                    Minimum=("_num_", "min"),
                    Median=("_num_", "median"),
                    Durchschnitt=("_num_", "mean"),
                    Maximum=("_num_", "max"),
                    Summe=("_num_", "sum"),
                )
                .reset_index()
                .rename(columns={"_gruppe_": group_label})
            )
            grouped_table["Fehlende_Werte"] = (
                grouped_table["Gesamt_Zeilen"] - grouped_table["Gueltige_Werte"]
            )
            grouped_table["Vollstaendigkeit_Prozent"] = (
                grouped_table["Gueltige_Werte"] / grouped_table["Gesamt_Zeilen"] * 100.0
            ).round(2)

            for num_c in ["Minimum", "Median", "Durchschnitt", "Maximum", "Summe"]:
                grouped_table[num_c] = grouped_table[num_c].round(3)

            st.dataframe(grouped_table, use_container_width=True, hide_index=True)

        # MODUS 3: EINZELZEILEN-INSPEKTOR (MIT TRANSPARENZ-FILTER FÜR LÜCKEN & ANOMALIEN)
        elif view_mode.startswith("3."):
            st.markdown(
                "#### B. Einzelzeilen-Inspektor mit Transparenz-Filter (Lücken, Ausreißer & ML-Markierungen)"
            )
            f1, f2 = st.columns([2, 1])
            with f1:
                row_filter_mode = st.selectbox(
                    "Zeilenfilter wählen:",
                    [
                        "1. Alle Zeilen anzeigen",
                        "2. Nur Zeilen mit fehlenden Werten (NULL / Lücken transparent prüfen)",
                        "3. Nur Zeilen mit Isolation-Forest-Markierung (inkl. Begründungstext)",
                        "4. Nur vollständig gefüllte Zeilen (ohne fehlende Werte)",
                    ],
                )
            with f2:
                max_rows_show = st.selectbox("Max. Zeilen in Ansicht:", [100, 300, 1000], index=1)

            inspected_df = df_mart_full.copy()
            inspected_df.insert(0, "row_index", range(len(inspected_df)))

            staging_ref = f"staging.{chosen_table}"
            ml_sub = (
                df_ml_flags_all[df_ml_flags_all["table_name"] == staging_ref][
                    ["row_index", "primary_column", "z_score", "reason_rule"]
                ]
                if not df_ml_flags_all.empty
                else pd.DataFrame()
            )
            if not ml_sub.empty:
                inspected_df = inspected_df.merge(ml_sub, on="row_index", how="left")

            if row_filter_mode.startswith("2."):
                mask_missing = df_mart_full[metric_cols].isna().any(axis=1)
                inspected_df = inspected_df[mask_missing.values]
                st.info(
                    f"Gefundene Zeilen mit mindestens einem fehlenden Kennzahlenwert: **{len(inspected_df)} von {len(df_mart_full)}**"
                )
            elif row_filter_mode.startswith("3."):
                if "reason_rule" in inspected_df.columns:
                    inspected_df = inspected_df[inspected_df["reason_rule"].notna()]
                else:
                    inspected_df = inspected_df.iloc[0:0]
                st.info(
                    f"Durch Isolation Forest markierte Zeilen mit Begründung: **{len(inspected_df)}**"
                )
            elif row_filter_mode.startswith("4."):
                mask_complete = ~df_mart_full[metric_cols].isna().any(axis=1)
                inspected_df = inspected_df[mask_complete.values]
                st.success(
                    f"Vollständig gefüllte Zeilen ohne Lücken: **{len(inspected_df)} von {len(df_mart_full)}**"
                )

            st.dataframe(
                inspected_df.head(int(max_rows_show)),
                use_container_width=True,
                hide_index=True,
            )

        # MODUS 4: KOMBINIERTE ANSICHT (AGGREGIERTE TABELLE + PASSENDES DIAGRAMM NEBENEINANDER)
        elif view_mode.startswith("4."):
            st.markdown(
                "#### B. Kombinierte Ansicht: Aggregierte Datentabelle + Diagramm nebeneinander"
            )
            time_cols = [
                c for c in all_cols if "date" in c.lower() or "time" in c.lower()
            ]
            x_time_col = time_cols[0] if time_cols else all_cols[0]
            comb_metric = st.selectbox(
                "Kennzahl für die kombinierte Tabellen- & Diagrammansicht:",
                options=metric_cols if metric_cols else all_cols,
                key="comb_metric_select",
            )
            plot_df_c = df_mart_full.copy()
            plot_df_c["_val_"] = pd.to_numeric(plot_df_c[comb_metric], errors="coerce")
            plot_df_c["Datum"] = pd.to_datetime(
                plot_df_c[x_time_col], errors="coerce"
            ).dt.strftime("%Y-%m-%d")
            agg_df_c = (
                plot_df_c.groupby("Datum", as_index=False)
                .agg(
                    Gueltige_Stunden=("_val_", "count"),
                    Fehlende_Stunden=("_val_", lambda s: int(s.isna().sum())),
                    Minimum=("_val_", "min"),
                    Durchschnitt=("_val_", "mean"),
                    Median=("_val_", "median"),
                    Maximum=("_val_", "max"),
                    Summe=("_val_", "sum"),
                )
                .round(3)
            )
            col_left, col_right = st.columns([1.15, 1.0])
            with col_left:
                st.markdown(f"**Tages-Aggregationstabelle für `{comb_metric}`**")
                st.dataframe(agg_df_c, use_container_width=True, hide_index=True, height=380)
            with col_right:
                st.markdown(f"**Tagesverlauf (Min / Durchschnitt / Max) für `{comb_metric}`**")
                fig_comb = px.line(
                    agg_df_c.dropna(subset=["Durchschnitt"]),
                    x="Datum",
                    y=["Minimum", "Durchschnitt", "Maximum"],
                    markers=True,
                    title=f"Tages-Korridor (Min / Ø / Max): {comb_metric}",
                )
                fig_comb.update_layout(height=380)
                st.plotly_chart(fig_comb, use_container_width=True)

        # ── TEIL C: IMMER SICHTBARES, WÄHLBARES DIAGRAMM (LINIE, BALKEN, KREIS) ─
        st.markdown("---")
        st.markdown("#### Wählbares Diagramm (Liniendiagramm · Balkendiagramm · Kreisdiagramm)")
        if not df_mart_full.empty:
            time_cols = [
                c for c in all_cols if "date" in c.lower() or "time" in c.lower()
            ]
            default_x_idx = all_cols.index(time_cols[0]) if time_cols else 0

            c_type, c_x, c_y, c_res = st.columns(4)
            with c_type:
                plot_type = st.selectbox(
                    "Diagrammtyp:",
                    [
                        "Liniendiagramm",
                        "Balkendiagramm",
                        "Kreisdiagramm",
                        "Kein Diagramm",
                    ],
                )
            with c_x:
                x_col = st.selectbox(
                    "Kategorie / Zeitachse (X):",
                    all_cols,
                    index=default_x_idx,
                )
            with c_y:
                y_col = st.selectbox(
                    "Kennzahl (Y):",
                    metric_cols if metric_cols else all_cols,
                )
            with c_res:
                agg_choice = st.selectbox(
                    "Auflösung / Gruppierung:",
                    [
                        "Tageswerte (Ø pro Tag – übersichtlich)",
                        "Originalwerte (Stündlich / Einzelzeilen)",
                    ],
                )

            if plot_type != "Kein Diagramm" and x_col and y_col:
                chart_df = df_mart_full.copy()
                chart_df["_y_num_"] = pd.to_numeric(chart_df[y_col], errors="coerce")
                is_time_x = "date" in x_col.lower() or "time" in x_col.lower()

                if agg_choice.startswith("Tageswerte") and is_time_x:
                    chart_df["Tag"] = pd.to_datetime(
                        chart_df[x_col], errors="coerce"
                    ).dt.strftime("%Y-%m-%d")
                    plot_data = (
                        chart_df.groupby("Tag", as_index=False)
                        .agg(
                            Durchschnitt=("_y_num_", "mean"),
                            Minimum=("_y_num_", "min"),
                            Maximum=("_y_num_", "max"),
                            Gueltige_Werte=("_y_num_", "count"),
                        )
                        .round(3)
                    )
                    x_plot = "Tag"
                    y_plot = "Durchschnitt"
                    title_suffix = "Tagesdurchschnitt"
                else:
                    plot_data = chart_df.dropna(subset=["_y_num_"]).head(300).copy()
                    plot_data[x_col] = plot_data[x_col].astype(str)
                    x_plot = x_col
                    y_plot = "_y_num_"
                    title_suffix = "Einzelwerte"

                if plot_type == "Liniendiagramm":
                    fig_m = px.line(
                        plot_data.dropna(subset=[y_plot]),
                        x=x_plot,
                        y=y_plot,
                        markers=True,
                        title=f"Liniendiagramm: {y_col} nach {x_plot} ({title_suffix} · mart.{chosen_table})",
                        labels={x_plot: x_plot, y_plot: y_col},
                    )
                    fig_m.update_layout(height=400)
                    st.plotly_chart(fig_m, use_container_width=True)

                elif plot_type == "Balkendiagramm":
                    fig_m = px.bar(
                        plot_data.dropna(subset=[y_plot]),
                        x=x_plot,
                        y=y_plot,
                        text_auto=".2f" if len(plot_data) <= 35 else False,
                        title=f"Balkendiagramm: {y_col} nach {x_plot} ({title_suffix} · mart.{chosen_table})",
                        labels={x_plot: x_plot, y_plot: y_col},
                    )
                    fig_m.update_layout(height=400)
                    st.plotly_chart(fig_m, use_container_width=True)

                elif plot_type == "Kreisdiagramm":
                    # Wenn X-Achse zu viele Einzelwerte hat (z. B. Zeitstempel), automatisch in 4 transparente Wertebereiche + Lücken gruppieren
                    unique_x = chart_df[x_col].nunique(dropna=True)
                    if is_time_x or unique_x > 12:
                        valid_vals = chart_df["_y_num_"].dropna()
                        if len(valid_vals) > 0:
                            q25, q50, q75 = np.percentile(valid_vals, [25, 50, 75])

                            def _make_bucket(val: float) -> str:
                                if pd.isna(val):
                                    return "5. Fehlender Wert (NULL)"
                                if val <= q25:
                                    return f"1. Niedrig (<= {q25:.2f})"
                                if val <= q50:
                                    return f"2. Unteres Mittel ({q25:.2f} – {q50:.2f})"
                                if val <= q75:
                                    return f"3. Oberes Mittel ({q50:.2f} – {q75:.2f})"
                                return f"4. Spitzenbereich (> {q75:.2f})"

                            buckets = chart_df["_y_num_"].apply(_make_bucket)
                            pie_data = buckets.value_counts().reset_index()
                            pie_data.columns = ["Wertebereich", "Anzahl_Stunden"]
                            fig_m = px.pie(
                                pie_data,
                                names="Wertebereich",
                                values="Anzahl_Stunden",
                                hole=0.3,
                                title=f"Kreisdiagramm: Verteilung von '{y_col}' nach Wertebereichen & Lücken (mart.{chosen_table})",
                            )
                            fig_m.update_layout(height=420)
                            st.plotly_chart(fig_m, use_container_width=True)
                        else:
                            st.info("Keine gültigen Zahlenwerte für Kreisdiagramm vorhanden.")
                    else:
                        pie_agg = (
                            chart_df.groupby(x_col, as_index=False)["_y_num_"]
                            .sum()
                            .rename(columns={"_y_num_": y_col})
                        )
                        fig_m = px.pie(
                            pie_agg,
                            names=x_col,
                            values=y_col,
                            hole=0.3,
                            title=f"Kreisdiagramm: Anteile von '{y_col}' nach '{x_col}' (mart.{chosen_table})",
                        )
                        fig_m.update_layout(height=420)
                        st.plotly_chart(fig_m, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# ANSICHT 5: REGELBASIERTER QUALITÄTSBERICHT (OHNE KI)
# ══════════════════════════════════════════════════════════════════════════════
elif selected_section.startswith("5."):
    st.subheader("Regelbasierter Qualitätsbericht mit nachvollziehbaren Empfehlungen")
    rep = build_quality_report(cfg)
    st.markdown(str(rep["markdown"]))


# ══════════════════════════════════════════════════════════════════════════════
# ANSICHT 6: BEREITSTELLUNG & DOWNLOAD FERTIGER DATEN
# ══════════════════════════════════════════════════════════════════════════════
elif selected_section.startswith("6."):
    st.subheader("Temporäre Bereitstellung fertiger Parquet-Daten & Berichte (`data/export`)")
    export_status = get_export_storage_status(cfg)

    e1, e2, e3 = st.columns(3)
    e1.metric("Aktive Exportpakete", f"{export_status['active_packages_count']}")
    e2.metric(
        "Belegter Exportspeicher",
        f"{export_status['used_mb']:.2f} / {export_status['max_mb']} MB",
    )
    e3.metric(
        "Automatische Löschfrist (TTL)",
        f"{export_status['retention_hours']} Stunden",
    )

    st.markdown("---")
    if not mart_tables:
        st.warning("Keine freigegebenen Tabellen im Schema `mart` zum Exportieren verfügbar.")
    else:
        exp_table = st.selectbox("Freigegebene Mart-Tabelle für den Export wählen:", mart_tables)
        df_cols_preview = fetch_dataframe(f"SELECT * FROM mart.{exp_table} LIMIT 1;")
        available_cols = df_cols_preview.columns.tolist() if not df_cols_preview.empty else []
        chosen_cols = st.multiselect(
            "Gewünschte Spalten auswählen (leer lassen für alle Spalten):",
            options=available_cols,
            default=available_cols,
        )

        if st.button("Versandfertiges Paket (Parquet + Bericht) erzeugen"):
            pkg = create_export_package(
                short_table_name=exp_table,
                selected_columns=chosen_cols if chosen_cols else None,
                config=cfg,
            )
            st.success(
                f"Paket `{pkg['package_id']}` erstellt (gültig bis {pkg['expires_at']})."
            )
            export_status = get_export_storage_status(cfg)

    st.markdown("#### Aktuell verfügbare Download-Pakete")
    packages = export_status.get("packages", [])
    if not packages:
        st.info("Noch keine Exportpakete im temporären Bereich vorhanden.")
    else:
        for pkg in packages:
            pq_file = Path(str(pkg["parquet_path"]))
            rep_file = Path(str(pkg["report_path"]))
            zip_file = Path(str(pkg["bundle_zip_path"]))
            with st.expander(
                f"📦 {pkg['package_id']} | Tabelle: {pkg['table_name']} | "
                f"Größe: {int(pkg['size_bytes']) / 1024:.1f} KB | "
                f"Löschung am: {pkg['expires_at']}",
                expanded=True,
            ):
                d1, d2, d3 = st.columns(3)
                if pq_file.exists():
                    d1.download_button(
                        label="⬇️ Parquet-Datei herunterladen",
                        data=pq_file.read_bytes(),
                        file_name=pq_file.name,
                        mime="application/octet-stream",
                        key=f"dl_pq_{pkg['package_id']}",
                    )
                if rep_file.exists():
                    d2.download_button(
                        label="⬇️ Qualitätsbericht (.md) herunterladen",
                        data=rep_file.read_bytes(),
                        file_name=rep_file.name,
                        mime="text/markdown",
                        key=f"dl_md_{pkg['package_id']}",
                    )
                if zip_file.exists():
                    d3.download_button(
                        label="⬇️ Komplettpaket (.zip) herunterladen",
                        data=zip_file.read_bytes(),
                        file_name=zip_file.name,
                        mime="application/zip",
                        key=f"dl_zip_{pkg['package_id']}",
                    )
