#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Streamlit-Anwendung der Plattform für Datenqualität.
Zeigt ausschließlich geprüfte Daten aus dem Schema 'mart' sowie nachvollziehbare
Protokolle aus 'metadata' (Quellen, Parquet-Konvertierung, Qualitätsprüfungen,
Gatekeeper-Freigabe und Abschnitt 'Nicht verarbeitbar').
"""

import sys
from pathlib import Path
from typing import List
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
            "4. Freigegebene Mart-Daten & Diagramme",
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

# ── PFLICHT-KPI-KARTEN ───────────────────────────────────────────────────────
st.title("Transparente Plattform für Datenqualität")
st.caption(
    "Ausschließliche Verarbeitung realer Datenquellen · Automatische Parquet-Konvertierung · "
    "Mehrstufige Qualitätsprüfung vor Visualisierung"
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
# ANSICHT 2: QUALITÄTSPRÜFUNGEN (QUALITY_LOG)
# ══════════════════════════════════════════════════════════════════════════════
elif selected_section.startswith("2."):
    st.subheader("Protokollierte Qualitätsprüfungen (`metadata.quality_log`)")

    if df_quality.empty:
        st.info("Noch keine Qualitätsprüfungen vorhanden.")
    else:
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
                    "source_name",
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

        # Wählbares Diagramm für Qualitätsbefunde
        chart_choice = st.selectbox(
            "Optionales Qualitätsdiagramm wählen:",
            [
                "Kein Diagramm anzeigen",
                "Balkendiagramm: Befunde (Anzahl) je Regel",
                "Kreisdiagramm: Verteilung der Befunde nach Spalte",
            ],
        )
        if not filtered.empty and chart_choice.startswith("Balkendiagramm"):
            agg_rule = (
                filtered.groupby("rule_name", as_index=False)["affected_count"]
                .sum()
                .sort_values("affected_count", ascending=True)
            )
            fig = px.bar(
                agg_rule,
                x="affected_count",
                y="rule_name",
                orientation="h",
                title="Anzahl betroffener Einträge je Qualitätsregel",
                labels={"affected_count": "Anzahl Befunde", "rule_name": "Regel"},
            )
            fig.update_layout(height=340)
            st.plotly_chart(fig, use_container_width=True)
        elif not filtered.empty and chart_choice.startswith("Kreisdiagramm"):
            agg_col = (
                filtered.groupby("column_name", as_index=False)["affected_count"]
                .sum()
            )
            fig = px.pie(
                agg_col,
                names="column_name",
                values="affected_count",
                title="Anteil der Qualitätsbefunde nach Spalte",
            )
            fig.update_layout(height=340)
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")
        st.subheader("Lernende Anomalieprüfung (Stufe 2: Isolation Forest mit Begründung)")
        df_ml_flags = fetch_dataframe(
            "SELECT * FROM metadata.ml_anomaly_flags ORDER BY flagged_at DESC, ABS(z_score) DESC LIMIT 200;"
        )
        if df_ml_flags.empty:
            st.info("Noch keine Markierungen aus der lernenden Anomalieprüfung vorhanden.")
        else:
            st.caption(
                "Das Modell verändert oder löscht keine Daten. Jede Markierung enthält die "
                "verantwortliche Hauptspalte, den beobachteten Wert, den Median sowie z-Score und IQR-Abstand."
            )
            st.dataframe(
                df_ml_flags[
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
# ANSICHT 4: FREIGEGEBENE MART-DATEN & WÄHLBARE DIAGRAMME
# ══════════════════════════════════════════════════════════════════════════════
elif selected_section.startswith("4."):
    st.subheader("Freigegebene Daten im Schema `mart` (nach Bestehen der Abschlussprüfung)")

    if not mart_tables:
        st.warning(
            "Aktuell sind keine Tabellen im Schema `mart` freigegeben. "
            "Nur Daten, die den abschließenden Prüfschritt bestanden haben, erscheinen hier."
        )
    else:
        chosen_table = st.selectbox("Freigegebene Mart-Tabelle auswählen:", mart_tables)
        row_limit = st.selectbox("Zeilenanzahl in Tabellenansicht:", [100, 500, 2000], index=0)

        df_mart = fetch_dataframe(
            f"SELECT * FROM mart.{chosen_table} LIMIT {int(row_limit)};"
        )
        st.dataframe(df_mart, use_container_width=True, hide_index=True)

        st.markdown("---")
        st.markdown("#### Wählbares Diagramm (vom Nutzer konfigurierbar)")
        if not df_mart.empty:
            c_type, c_x, c_y = st.columns(3)
            with c_type:
                plot_type = st.selectbox(
                    "Diagrammtyp:",
                    ["Kein Diagramm", "Balkendiagramm", "Kreisdiagramm"],
                )
            all_cols = df_mart.columns.tolist()
            num_cols = [
                c for c in all_cols if pd.api.types.is_numeric_dtype(df_mart[c])
            ]
            with c_x:
                x_col = st.selectbox("Kategorie / Zeitachse (X):", all_cols)
            with c_y:
                y_col = st.selectbox(
                    "Kennzahl (Y):",
                    num_cols if num_cols else all_cols,
                )

            if plot_type == "Balkendiagramm" and x_col and y_col:
                fig_m = px.bar(
                    df_mart.head(100),
                    x=x_col,
                    y=y_col,
                    title=f"{y_col} nach {x_col} (mart.{chosen_table})",
                )
                fig_m.update_layout(height=360)
                st.plotly_chart(fig_m, use_container_width=True)
            elif plot_type == "Kreisdiagramm" and x_col and y_col:
                fig_m = px.pie(
                    df_mart.head(50),
                    names=x_col,
                    values=y_col,
                    title=f"Anteile von {y_col} nach {x_col} (mart.{chosen_table})",
                )
                fig_m.update_layout(height=360)
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
