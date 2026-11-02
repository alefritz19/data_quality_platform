#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Streamlit-Anwendung der Plattform für Datenqualität.
Enthält eine obere Steuerleiste (Sprachwahl DE/EN und Farbschema Hell/Dunkel),
wählbare KPI-Karten, strukturierte Transparenz- und Aggregationstabellen sowie
immer sichtbare Linien-, Balken- und Kreisdiagramme auf geprüften 'mart'-Daten.
"""

import sys
from pathlib import Path
from typing import Dict, List
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
import streamlit.components.v1 as components

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
CALM_BLUE_PALETTE = ["#4A86C5", "#6BA3D6", "#8BBCE3", "#A9D0EE", "#3B6E9C", "#78A8C8"]

st.set_page_config(
    page_title="Plattform für Datenqualität | DuckDB · Parquet · Dagster",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── SPRACHWÖRTERBUCH (DEUTSCH / ENGLISCH) ────────────────────────────────────
TEXTS: Dict[str, Dict[str, str]] = {
    "DE": {
        "app_title": "Transparente Plattform für Datenqualität",
        "app_subtitle": (
            "Ausschließliche Verarbeitung realer Datenquellen · Automatische Parquet-Konvertierung · "
            "Mehrstufige Qualitätsprüfung · Wählbare KPI-Karten & Transparenz-Tabellen"
        ),
        "sidebar_title": "Plattform für Datenqualität",
        "nav_label": "Ansicht wählen",
        "nav_1": "1. Übersicht & Quellen",
        "nav_2": "2. Qualitätsprüfungen (quality_log)",
        "nav_3": "3. Nicht verarbeitbar",
        "nav_4": "4. Freigegebene Daten: KPI-Karten, Tabellen & Diagramme",
        "nav_5": "5. Regelbasierter Qualitätsbericht",
        "nav_6": "6. Bereitstellung & Download fertiger Daten",
        "intake_title": "Dateiannahme & Pipeline-Steuerung",
        "upload_label": "Datei in Eingangsordner legen",
        "btn_run_all": "Reale Quellen (SMARD + DWD) + Pipeline ausführen",
        "btn_run_inbox": "Nur Eingangsordner (inbox) verarbeiten",
        "kpi_sources": "Erfasste Quellen",
        "kpi_parquet": "Parquet-Konvertierungen",
        "kpi_mart": "Freigegebene Mart-Tabellen",
        "kpi_issues": "Qualitätsbefunde (> 0)",
        "kpi_unparseable": "Nicht verarbeitbar",
        "top_lang": "Sprache / Language",
        "top_theme": "Farbton / Theme",
        "theme_light": "Hell (Weiß / Hellblau)",
        "theme_dark": "Dunkel (Schieferblau)",
        "sec1_1": "1. Dokumentierte Datenquellen (`metadata.quellen`)",
        "sec1_2": "2. Dateiannahme & Parquet-Konvertierung (`metadata.file_conversions`)",
        "sec1_3": "3. Abschließende Freigabeprüfung vor Visualisierung (`metadata.mart_gate_log`)",
        "sec2_title": "Zweistufige Qualitätsprüfung: Regelwerk (Stufe 1) & Isolation Forest (Stufe 2)",
        "sec3_title": "Abschnitt: Nicht verarbeitbar (Herkunft & Begründung)",
        "sec4_title": "Freigegebene Daten (`mart`): Wählbare KPI-Karten, Transparenz-Tabellen & Diagramme",
        "sec5_title": "Regelbasierter Qualitätsbericht mit nachvollziehbaren Empfehlungen",
        "sec6_title": "Temporäre Bereitstellung fertiger Parquet-Daten & Berichte (`data/export`)",
    },
    "EN": {
        "app_title": "Transparent Data Quality Platform",
        "app_subtitle": (
            "Real Public Data Sources Only · Automated Parquet Conversion · "
            "Two-Stage Quality Verification · Selectable KPI Cards & Transparency Tables"
        ),
        "sidebar_title": "Data Quality Platform",
        "nav_label": "Select View",
        "nav_1": "1. Overview & Data Sources",
        "nav_2": "2. Quality Checks (quality_log)",
        "nav_3": "3. Unparseable Files & Values",
        "nav_4": "4. Verified Mart Data: KPI Cards, Tables & Charts",
        "nav_5": "5. Rule-Based Quality Report",
        "nav_6": "6. Export & Download Ready Data",
        "intake_title": "File Intake & Pipeline Control",
        "upload_label": "Place file into inbox folder",
        "btn_run_all": "Fetch Real Sources (SMARD + DWD) & Run Pipeline",
        "btn_run_inbox": "Process Inbox Folder Only",
        "kpi_sources": "Registered Sources",
        "kpi_parquet": "Parquet Conversions",
        "kpi_mart": "Verified Mart Tables",
        "kpi_issues": "Quality Findings (> 0)",
        "kpi_unparseable": "Unparseable Items",
        "top_lang": "Language / Sprache",
        "top_theme": "Theme / Farbton",
        "theme_light": "Light (White / Sky Blue)",
        "theme_dark": "Dark (Slate Blue)",
        "sec1_1": "1. Documented Data Sources (`metadata.quellen`)",
        "sec1_2": "2. File Intake & Parquet Conversion (`metadata.file_conversions`)",
        "sec1_3": "3. Final Verification Gate Before Visualization (`metadata.mart_gate_log`)",
        "sec2_title": "Two-Stage Quality Verification: Rules (Stage 1) & Isolation Forest (Stage 2)",
        "sec3_title": "Section: Unparseable Items (Origin & Reason)",
        "sec4_title": "Verified Data (`mart`): Selectable KPI Cards, Transparency Tables & Charts",
        "sec5_title": "Rule-Based Quality Report with Traceable Recommendations",
        "sec6_title": "Temporary Export & Download of Verified Parquet Data (`data/export`)",
    },
}

if "ui_lang" not in st.session_state:
    st.session_state["ui_lang"] = "DE"
if "ui_theme" not in st.session_state:
    st.session_state["ui_theme"] = "LIGHT"


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
    Erzeugt den vollständigen Spalten-Steckbrief (Transparenz-Matrix) für eine Tabelle.
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
                    else (
                        "Zeitstempel"
                        if "date" in col.lower() or "time" in col.lower()
                        else ("Kennzahl (numerisch)" if is_num else "Text / Kategorie")
                    )
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

# ── VEREINTE, NAHTLOSE KOPFLEISTE MIT GRÖSSEREM, FEINLINIGEM EMBLEM ──────────
T = TEXTS[st.session_state["ui_lang"]]

LOGO_SVG = (
    '<svg width="56" height="56" viewBox="0 0 56 56" fill="none" '
    'xmlns="http://www.w3.org/2000/svg" style="flex-shrink:0;">'
    '<defs>'
    '<linearGradient id="goldGrad" x1="0" y1="0" x2="56" y2="56" gradientUnits="userSpaceOnUse">'
    '<stop offset="0%" stop-color="#FFE875"/>'
    '<stop offset="50%" stop-color="#F59E0B"/>'
    '<stop offset="100%" stop-color="#D97706"/>'
    '</linearGradient>'
    '<linearGradient id="innerBg" x1="6" y1="6" x2="50" y2="50" gradientUnits="userSpaceOnUse">'
    '<stop offset="0%" stop-color="#1E3246"/>'
    '<stop offset="100%" stop-color="#12202F"/>'
    '</linearGradient>'
    '</defs>'
    '<!-- Äußeres & inneres Hexagon mit feinen goldenen Linien -->'
    '<polygon points="28,3 50,15.5 50,40.5 28,53 6,40.5 6,15.5" fill="url(#innerBg)" stroke="url(#goldGrad)" stroke-width="1.6"/>'
    '<polygon points="28,6.5 46.8,17.2 46.8,38.8 28,49.5 9.2,38.8 9.2,17.2" fill="none" stroke="#FDE047" stroke-opacity="0.45" stroke-width="0.8"/>'
    '<!-- Feines Koordinaten-Basisraster -->'
    '<line x1="15" y1="39" x2="41" y2="39" stroke="#93C5FD" stroke-opacity="0.4" stroke-width="0.9"/>'
    '<!-- Vier schlanke, präzise Datensäulen -->'
    '<rect x="16.5" y="30" width="3.2" height="9" rx="0.8" fill="#60A5FA" opacity="0.9"/>'
    '<rect x="22.8" y="25" width="3.2" height="14" rx="0.8" fill="#38BDF8" opacity="0.95"/>'
    '<rect x="29.1" y="20" width="3.2" height="19" rx="0.8" fill="#10B981" opacity="0.95"/>'
    '<rect x="35.4" y="15" width="3.2" height="24" rx="0.8" fill="#FACC15"/>'
    '<!-- Feine goldene Qualitäts- und Trendlinie mit Messpunkten -->'
    '<path d="M16.5 25.5 L24.4 20.5 L30.7 22.5 L38.5 12.5" stroke="#FEF08A" stroke-width="1.35" stroke-linecap="round" stroke-linejoin="round"/>'
    '<circle cx="16.5" cy="25.5" r="1.4" fill="#60A5FA" stroke="#FEF08A" stroke-width="0.9"/>'
    '<circle cx="24.4" cy="20.5" r="1.4" fill="#38BDF8" stroke="#FEF08A" stroke-width="0.9"/>'
    '<circle cx="30.7" cy="22.5" r="1.4" fill="#10B981" stroke="#FEF08A" stroke-width="0.9"/>'
    '<circle cx="38.5" cy="12.5" r="1.8" fill="#10B981" stroke="#FEF08A" stroke-width="1.1"/>'
    '</svg>'
)

header_container = st.container()
with header_container:
    tb1, tb2, tb3 = st.columns([3.5, 0.85, 1.15], vertical_alignment="center")
    with tb1:
        st.markdown(
            f"<div id='fixed-top-header-marker' class='unified-header-wrap'>"
            f"{LOGO_SVG}"
            f"<div class='unified-header-text'>"
            f"<div class='unified-header-title'>{T['app_title']}</div>"
            f"<div class='unified-header-sub'>"
            f"Umgebung: <code>{cfg.app_env.upper()}</code> &nbsp;·&nbsp; "
            f"Engine: <code>DuckDB + Parquet (ZSTD)</code> &nbsp;·&nbsp; "
            f"Quellen: <code>SMARD.de & DWD 01420</code>"
            f"</div>"
            f"</div>"
            f"</div>",
            unsafe_allow_html=True,
        )
    with tb2:
        lang_choice = st.radio(
            "Sprache",
            options=["DE", "EN"],
            format_func=lambda x: f"🌐 {x}",
            index=0 if st.session_state["ui_lang"] == "DE" else 1,
            horizontal=True,
            label_visibility="collapsed",
            key="lang_radio",
        )
        if lang_choice != st.session_state["ui_lang"]:
            st.session_state["ui_lang"] = lang_choice
            st.rerun()
    with tb3:
        theme_choice = st.radio(
            "Farbton",
            options=["LIGHT", "DARK"],
            format_func=lambda x: "☀️ Hell" if x == "LIGHT" else "🌙 Dunkel",
            index=0 if st.session_state["ui_theme"] == "LIGHT" else 1,
            horizontal=True,
            label_visibility="collapsed",
            key="theme_radio",
        )
        if theme_choice != st.session_state["ui_theme"]:
            st.session_state["ui_theme"] = theme_choice
            st.rerun()

T = TEXTS[st.session_state["ui_lang"]]
is_dark = st.session_state["ui_theme"] == "DARK"
plotly_tpl = "plotly_dark" if is_dark else "plotly_white"
chart_paper_bg = "#20232A" if is_dark else "#FFFFFF"
chart_plot_bg = "#181A1F" if is_dark else "#F9FBFF"

# ── ZWEI-FARBEN-DESIGN (HELL: WEISS/HELLBLAU | DUNKEL: MODERN MATTE DARK) ────
if not is_dark:
    st.markdown(
        """
        <style>
            /* Standard-Streamlit-Kopfzeile ausblenden */
            [data-testid="stHeader"] {
                display: none !important;
            }
            /* Hauptcontainer auf 100 % Breite */
            .block-container,
            [data-testid="stAppViewBlockContainer"],
            [data-testid="stMainBlockContainer"] {
                max-width: 100% !important;
                padding-top: 0rem !important;
                padding-left: 2.2rem !important;
                padding-right: 2.2rem !important;
            }
            [data-testid="stMain"], section.main {
                overflow-x: hidden !important;
            }
            /* Haupthintergrund: klares Weiß */
            .stApp, [data-testid="stAppViewContainer"] {
                background-color: #FFFFFF !important;
                color: #2B3E54 !important;
            }
            /* Nahtlos durchgehende Kopfleiste: vertikal exakt in der Mitte */
            .block-container > div[data-testid="stVerticalBlock"] > div:has(#fixed-top-header-marker),
            [data-testid="stMainBlockContainer"] > div[data-testid="stVerticalBlock"] > div:has(#fixed-top-header-marker),
            div[data-testid="stVerticalBlockBorderWrapper"]:has(#fixed-top-header-marker),
            div[data-testid="stLayoutWrapper"]:has(#fixed-top-header-marker) {
                position: sticky !important;
                top: 0 !important;
                z-index: 9999 !important;
                background-color: #E8F2FC !important;
                border-bottom: 1px solid #AFCBE8 !important;
                width: calc(100% + 4.4rem) !important;
                max-width: none !important;
                margin-top: 0rem !important;
                margin-left: -2.2rem !important;
                margin-right: -2.2rem !important;
                margin-bottom: 18px !important;
                padding: 14px 2.2rem 14px 2.2rem !important;
                box-shadow: 0 0 0 100vmax #E8F2FC !important;
                clip-path: inset(0 -100vmax) !important;
            }
            .unified-header-wrap {
                display: flex !important;
                align-items: center !important;
                gap: 15px !important;
                margin: 0 !important;
                padding: 2px 0 !important;
            }
            .unified-header-text {
                display: flex !important;
                flex-direction: column !important;
                justify-content: center !important;
                gap: 4px !important;
                margin: 0 !important;
                padding: 0 !important;
            }
            .unified-header-title {
                color: #26425E !important;
                font-size: 1.52rem !important;
                font-weight: 500 !important;
                line-height: 1.2 !important;
                margin: 0 !important;
                padding: 0 !important;
            }
            .unified-header-sub {
                color: #3E5E7E !important;
                font-size: 0.85rem !important;
                line-height: 1.25 !important;
                margin: 0 !important;
                padding: 0 !important;
                white-space: nowrap !important;
            }
            /* NUR die Radio-Buttons in der oberen Kopfleiste rechtsbündig ausrichten */
            div[data-testid="stHorizontalBlock"]:has(#fixed-top-header-marker) [data-testid="stRadio"] {
                margin: 0 !important;
                padding: 0 !important;
            }
            div[data-testid="stHorizontalBlock"]:has(#fixed-top-header-marker) [data-testid="stRadio"] > div {
                justify-content: flex-end !important;
                flex-wrap: nowrap !important;
                align-items: center !important;
            }
            /* Linkes Menü: klar linksbündig und mit sauberer dunklerer Konturlinie abgegrenzt */
            [data-testid="stSidebar"] [data-testid="stRadio"] > div {
                justify-content: flex-start !important;
                align-items: flex-start !important;
            }
            [data-testid="stSidebar"] {
                background-color: #E8F2FC !important;
                border-right: 1px solid #AFCBE8 !important;
                box-shadow: none !important;
            }
            [data-testid="stSidebar"] > div:first-child {
                background-color: #E8F2FC !important;
            }
            [data-testid="stSidebarCollapseButton"] {
                display: none !important;
            }
            [data-testid="stSidebar"] [data-testid="stSidebarUserContent"] {
                padding-top: 1.1rem !important;
            }
            [data-testid="stSidebar"] h1,
            [data-testid="stSidebar"] h2,
            [data-testid="stSidebar"] h3,
            [data-testid="stSidebar"] h4,
            [data-testid="stSidebar"] label,
            [data-testid="stSidebar"] .stMarkdown p {
                color: #2C4866 !important;
            }
            /* Überschriften im Hauptbereich mit feiner Unterlinie */
            h1 {
                color: #2C4866 !important;
                font-weight: 500 !important;
            }
            h2, h3 {
                color: #2E4C6B !important;
                font-weight: 500 !important;
                border-bottom: 1px solid #E3EEF9 !important;
                padding-bottom: 6px !important;
            }
            h4 {
                color: #325070 !important;
                font-weight: 500 !important;
            }
            /* KPI-Karten mit feinem Akzentrand links */
            [data-testid="stMetric"] {
                background-color: #F7FAFF !important;
                border: 1px solid #D0E3F7 !important;
                border-left: 4px solid #4A86C5 !important;
                border-radius: 8px !important;
                padding: 12px 15px !important;
                box-shadow: 0 1px 4px rgba(44, 72, 102, 0.04) !important;
            }
            [data-testid="stMetricLabel"] {
                color: #365678 !important;
                font-weight: 500 !important;
            }
            [data-testid="stMetricValue"] {
                color: #243B55 !important;
                font-weight: 500 !important;
            }
            [data-testid="stMetricDelta"] {
                background-color: #E3EFFB !important;
                color: #35628C !important;
                border-radius: 5px !important;
                padding: 2px 6px !important;
            }
            [data-testid="stMetricDelta"] svg {
                fill: #35628C !important;
            }
            /* Multiselect-Tags in sanftem Mittelblau statt Rot */
            span[data-baseweb="tag"] {
                background-color: #4A86C5 !important;
                color: #FFFFFF !important;
                border-radius: 5px !important;
            }
            span[data-baseweb="tag"] span, span[data-baseweb="tag"] svg {
                color: #FFFFFF !important;
                fill: #FFFFFF !important;
            }
            /* Auswahlfelder */
            div[data-baseweb="select"] > div {
                border-color: #C5DCF2 !important;
                background-color: #F9FCFF !important;
            }
            /* Buttons: helleres, harmonisches Himmel-/Mittelblau (sticht nicht grell hervor) */
            .stButton > button,
            [data-testid="stSidebar"] .stButton > button {
                background-color: #4A86C5 !important;
                color: #FFFFFF !important;
                border: 1px solid #3D73AD !important;
                border-radius: 6px !important;
                font-weight: 500 !important;
            }
            .stButton > button p,
            .stButton > button span,
            [data-testid="stSidebar"] .stButton > button p,
            [data-testid="stSidebar"] .stButton > button span {
                color: #FFFFFF !important;
            }
            .stButton > button:hover,
            [data-testid="stSidebar"] .stButton > button:hover {
                background-color: #3D73AD !important;
                color: #FFFFFF !important;
                border-color: #326091 !important;
            }
            [data-testid="stDownloadButton"] > button,
            [data-testid="stFileUploader"] button {
                background-color: #F0F7FF !important;
                color: #2C4866 !important;
                border: 1px solid #A8CBF0 !important;
                border-radius: 6px !important;
            }
            [data-testid="stDownloadButton"] > button p,
            [data-testid="stDownloadButton"] > button span,
            [data-testid="stFileUploader"] button p,
            [data-testid="stFileUploader"] button span {
                color: #2C4866 !important;
            }
            [data-testid="stAlert"] {
                background-color: #F2F8FF !important;
                color: #2C4866 !important;
                border: 1px solid #CBE0F5 !important;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )
else:
    st.markdown(
        """
        <style>
            /* Modern Matte Dark (angenehmes mattes Anthrazit/Graphit statt Total-Black oder Blau) */
            [data-testid="stHeader"] {
                display: none !important;
            }
            .block-container,
            [data-testid="stAppViewBlockContainer"],
            [data-testid="stMainBlockContainer"] {
                max-width: 100% !important;
                padding-top: 0rem !important;
                padding-left: 2.2rem !important;
                padding-right: 2.2rem !important;
            }
            [data-testid="stMain"], section.main {
                overflow-x: hidden !important;
            }
            .stApp, [data-testid="stAppViewContainer"] {
                background-color: #181A1F !important;
                color: #F3F4F6 !important;
            }
            /* Alle Textelemente, Labels, Radio-Optionen, Checkboxen und Captions im Matte-Dark-Modus klar weiß */
            .stApp p,
            .stApp span,
            .stApp label,
            .stApp div,
            .stApp li,
            [data-testid="stMarkdownContainer"] p,
            [data-testid="stMarkdownContainer"] li,
            [data-testid="stCaptionContainer"] p,
            [data-testid="stWidgetLabel"] p,
            [data-testid="stRadio"] label p,
            [data-testid="stRadio"] label span,
            [data-testid="stCheckbox"] label p,
            [data-testid="stCheckbox"] label span {
                color: #F3F4F6 !important;
            }
            code {
                background-color: #282C34 !important;
                color: #93C5FD !important;
                border: 1px solid #3B4048 !important;
            }
            /* Nahtlos durchgehende Kopfleiste im Matte-Dark-Modus, vertikal exakt mittig */
            .block-container > div[data-testid="stVerticalBlock"] > div:has(#fixed-top-header-marker),
            [data-testid="stMainBlockContainer"] > div[data-testid="stVerticalBlock"] > div:has(#fixed-top-header-marker),
            div[data-testid="stVerticalBlockBorderWrapper"]:has(#fixed-top-header-marker),
            div[data-testid="stLayoutWrapper"]:has(#fixed-top-header-marker) {
                position: sticky !important;
                top: 0 !important;
                z-index: 9999 !important;
                background-color: #21252B !important;
                border-bottom: 1px solid #333842 !important;
                width: calc(100% + 4.4rem) !important;
                max-width: none !important;
                margin-top: 0rem !important;
                margin-left: -2.2rem !important;
                margin-right: -2.2rem !important;
                margin-bottom: 18px !important;
                padding: 14px 2.2rem 14px 2.2rem !important;
                box-shadow: 0 0 0 100vmax #21252B !important;
                clip-path: inset(0 -100vmax) !important;
            }
            .unified-header-wrap {
                display: flex !important;
                align-items: center !important;
                gap: 15px !important;
                margin: 0 !important;
                padding: 2px 0 !important;
            }
            .unified-header-text {
                display: flex !important;
                flex-direction: column !important;
                justify-content: center !important;
                gap: 4px !important;
                margin: 0 !important;
                padding: 0 !important;
            }
            .unified-header-title {
                color: #FFFFFF !important;
                font-size: 1.52rem !important;
                font-weight: 500 !important;
                line-height: 1.2 !important;
                margin: 0 !important;
                padding: 0 !important;
            }
            .unified-header-sub {
                color: #D1D5DB !important;
                font-size: 0.85rem !important;
                line-height: 1.25 !important;
                margin: 0 !important;
                padding: 0 !important;
                white-space: nowrap !important;
            }
            div[data-testid="stHorizontalBlock"]:has(#fixed-top-header-marker) [data-testid="stRadio"] {
                margin: 0 !important;
                padding: 0 !important;
            }
            div[data-testid="stHorizontalBlock"]:has(#fixed-top-header-marker) [data-testid="stRadio"] > div {
                justify-content: flex-end !important;
                flex-wrap: nowrap !important;
                align-items: center !important;
            }
            [data-testid="stSidebar"] [data-testid="stRadio"] > div {
                justify-content: flex-start !important;
                align-items: flex-start !important;
            }
            [data-testid="stSidebar"] {
                background-color: #21252B !important;
                border-right: 1px solid #333842 !important;
                box-shadow: none !important;
            }
            [data-testid="stSidebar"] > div:first-child {
                background-color: #21252B !important;
            }
            [data-testid="stSidebarCollapseButton"] {
                display: none !important;
            }
            [data-testid="stSidebar"] [data-testid="stSidebarUserContent"] {
                padding-top: 1.1rem !important;
            }
            [data-testid="stSidebar"] h1,
            [data-testid="stSidebar"] h2,
            [data-testid="stSidebar"] h3,
            [data-testid="stSidebar"] h4,
            [data-testid="stSidebar"] label,
            [data-testid="stSidebar"] p,
            [data-testid="stSidebar"] span {
                color: #F3F4F6 !important;
            }
            h1 {
                color: #FFFFFF !important;
                font-weight: 500 !important;
            }
            h2, h3 {
                color: #F3F4F6 !important;
                font-weight: 500 !important;
                border-bottom: 1px solid #2C313A !important;
                padding-bottom: 6px !important;
            }
            h4 {
                color: #E5E7EB !important;
                font-weight: 500 !important;
            }
            [data-testid="stMetric"] {
                background-color: #21252B !important;
                border: 1px solid #333842 !important;
                border-left: 4px solid #4A86C5 !important;
                border-radius: 8px !important;
                padding: 12px 15px !important;
            }
            [data-testid="stMetricLabel"], [data-testid="stMetricLabel"] p {
                color: #D1D5DB !important;
                font-weight: 500 !important;
            }
            [data-testid="stMetricValue"], [data-testid="stMetricValue"] div {
                color: #FFFFFF !important;
                font-weight: 500 !important;
            }
            [data-testid="stMetricDelta"], [data-testid="stMetricDelta"] div {
                background-color: #2C313C !important;
                color: #93C5FD !important;
                border-radius: 5px !important;
                padding: 2px 6px !important;
            }
            div[data-baseweb="select"] > div {
                background-color: #21252B !important;
                border-color: #3B4048 !important;
                color: #FFFFFF !important;
            }
            [data-testid="stFileUploader"] section {
                background-color: #21252B !important;
                border-color: #3B4048 !important;
                color: #FFFFFF !important;
            }
            [data-testid="stFileUploader"] button,
            [data-testid="stDownloadButton"] > button {
                background-color: #2C313A !important;
                color: #FFFFFF !important;
                border: 1px solid #4A86C5 !important;
                border-radius: 6px !important;
            }
            span[data-baseweb="tag"] {
                background-color: #3D73AD !important;
                color: #FFFFFF !important;
                border-radius: 5px !important;
            }
            .stButton > button,
            [data-testid="stSidebar"] .stButton > button {
                background-color: #3D73AD !important;
                color: #FFFFFF !important;
                border: 1px solid #4E86C2 !important;
                border-radius: 6px !important;
                font-weight: 500 !important;
            }
            .stButton > button p,
            .stButton > button span,
            [data-testid="stSidebar"] .stButton > button p,
            [data-testid="stSidebar"] .stButton > button span {
                color: #FFFFFF !important;
            }
            [data-testid="stAlert"] {
                background-color: #21252B !important;
                color: #F3F4F6 !important;
                border: 1px solid #3B4048 !important;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )

# ── SEITENLEISTE: KONTEXT, NAVIGATION & GESCHÜTZTE AKTIONEN ──────────────────
with st.sidebar:
    selected_section = st.radio(
        T["nav_label"],
        [
            T["nav_1"],
            T["nav_2"],
            T["nav_3"],
            T["nav_4"],
            T["nav_5"],
            T["nav_6"],
        ],
    )

    st.markdown("---")
    st.markdown(f"#### {T['intake_title']}")

    pin_input = ""
    if cfg.demo_read_only:
        pin_input = st.text_input(
            "Admin-PIN (Schreibschutz aktiv):",
            type="password",
            help="Im öffentlichen Demo-Kontext sind Schreib- und Trigger-Funktionen gesperrt.",
        )

    is_authorized = (not cfg.demo_read_only) or (pin_input == cfg.admin_pin)

    uploaded_file = st.file_uploader(
        f"{T['upload_label']} (max. {cfg.max_upload_size_mb} MB)",
        type=["csv", "tsv", "xlsx", "xls", "json", "jsonl", "parquet"],
        disabled=not is_authorized,
    )
    if uploaded_file is not None and is_authorized:
        target_inbox_file = cfg.inbox_dir / uploaded_file.name
        target_inbox_file.write_bytes(uploaded_file.getvalue())
        st.success(f"Datei '{uploaded_file.name}' in 'data/inbox' abgelegt.")

    if st.button(T["btn_run_all"], use_container_width=True):
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

    if st.button(T["btn_run_inbox"], use_container_width=True):
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

    components.html(
        """
        <script>
        try {
            const doc = window.parent.document;
            const sb = doc.querySelector('[data-testid="stSidebar"]');
            if (sb && sb.getAttribute('aria-expanded') === 'false') {
                const btn = doc.querySelector('[data-testid="collapsedControl"] button, [data-testid="stExpandSidebarButton"], [data-testid="collapsedControl"]');
                if (btn) btn.click();
            }
        } catch (e) {}
        </script>
        """,
        height=0,
    )


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
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric(T["kpi_sources"], f"{len(df_sources)}")
k2.metric(
    T["kpi_parquet"],
    f"{len(df_conversions)}",
    f"Ø {avg_compression:.1f} % kleiner ({avg_duration_ms:.0f} ms)",
)
k3.metric(T["kpi_mart"], f"{len(mart_tables)}")
k4.metric(
    T["kpi_issues"],
    f"{len(issues_df)}",
    f"davon {prio_issues_count} priorisiert",
)
k5.metric(
    T["kpi_unparseable"],
    f"{unparseable_total}",
    f"{len(df_unparseable_files)} Dateien / {len(df_unparseable_rows)} Werte",
)

st.markdown("---")


# ══════════════════════════════════════════════════════════════════════════════
# ANSICHT 1: ÜBERSICHT, QUELLEN & PARQUET-KONVERTIERUNG
# ══════════════════════════════════════════════════════════════════════════════
if selected_section.startswith("1."):
    st.subheader(T["sec1_1"])
    if df_sources.empty:
        st.info("Noch keine Datenquellen protokolliert.")
    else:
        st.dataframe(df_sources, use_container_width=True, hide_index=True)

    st.subheader(T["sec1_2"])
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

    st.subheader(T["sec1_3"])
    if df_gate.empty:
        st.info("Noch keine Freigabeprüfung protokolliert.")
    else:
        st.dataframe(df_gate, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# ANSICHT 2: QUALITÄTSPRÜFUNGEN (QUALITY_LOG & ISOLATION FOREST)
# ══════════════════════════════════════════════════════════════════════════════
elif selected_section.startswith("2."):
    st.subheader(T["sec2_title"])

    if df_quality.empty:
        st.info("Noch keine Qualitätsprüfungen vorhanden.")
    else:
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
                template=plotly_tpl,
                color_discrete_sequence=CALM_BLUE_PALETTE,
            )
            fig_q.update_layout(height=340, paper_bgcolor=chart_paper_bg, plot_bgcolor=chart_plot_bg)
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
                template=plotly_tpl,
                color_discrete_sequence=CALM_BLUE_PALETTE,
            )
            fig_q.update_layout(height=340, paper_bgcolor=chart_paper_bg, plot_bgcolor=chart_plot_bg)
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
                template=plotly_tpl,
                color_discrete_sequence=CALM_BLUE_PALETTE,
            )
            fig_q.update_layout(height=340, paper_bgcolor=chart_paper_bg)
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
    st.subheader(T["sec3_title"])
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
    st.subheader(T["sec4_title"])

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
                    template=plotly_tpl,
                    color_discrete_sequence=CALM_BLUE_PALETTE,
                )
                fig_comb.update_layout(height=380, paper_bgcolor=chart_paper_bg, plot_bgcolor=chart_plot_bg)
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
                        template=plotly_tpl,
                        color_discrete_sequence=CALM_BLUE_PALETTE,
                    )
                    fig_m.update_layout(height=400, paper_bgcolor=chart_paper_bg, plot_bgcolor=chart_plot_bg)
                    st.plotly_chart(fig_m, use_container_width=True)

                elif plot_type == "Balkendiagramm":
                    fig_m = px.bar(
                        plot_data.dropna(subset=[y_plot]),
                        x=x_plot,
                        y=y_plot,
                        text_auto=".2f" if len(plot_data) <= 35 else False,
                        title=f"Balkendiagramm: {y_col} nach {x_plot} ({title_suffix} · mart.{chosen_table})",
                        labels={x_plot: x_plot, y_plot: y_col},
                        template=plotly_tpl,
                        color_discrete_sequence=CALM_BLUE_PALETTE,
                    )
                    fig_m.update_layout(height=400, paper_bgcolor=chart_paper_bg, plot_bgcolor=chart_plot_bg)
                    st.plotly_chart(fig_m, use_container_width=True)

                elif plot_type == "Kreisdiagramm":
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
                                template=plotly_tpl,
                                color_discrete_sequence=CALM_BLUE_PALETTE,
                            )
                            fig_m.update_layout(height=420, paper_bgcolor=chart_paper_bg)
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
                            template=plotly_tpl,
                            color_discrete_sequence=CALM_BLUE_PALETTE,
                        )
                        fig_m.update_layout(height=420, paper_bgcolor=chart_paper_bg)
                        st.plotly_chart(fig_m, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# ANSICHT 5: REGELBASIERTER QUALITÄTSBERICHT (OHNE KI)
# ══════════════════════════════════════════════════════════════════════════════
elif selected_section.startswith("5."):
    st.subheader(T["sec5_title"])
    rep = build_quality_report(cfg)
    st.markdown(str(rep["markdown"]))


# ══════════════════════════════════════════════════════════════════════════════
# ANSICHT 6: BEREITSTELLUNG & DOWNLOAD FERTIGER DATEN
# ══════════════════════════════════════════════════════════════════════════════
elif selected_section.startswith("6."):
    st.subheader(T["sec6_title"])
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
