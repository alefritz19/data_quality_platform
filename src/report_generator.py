#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Regelbasierter Berichtsgenerator (ohne KI).
Erzeugt einen vollständig deterministischen, nachvollziehbaren Qualitätsbericht
im Markdown- und JSON-Format. Jede Aussage und Empfehlung verweist direkt auf
eine gespeicherte Prüfung ('check_id') in 'metadata.quality_log' oder auf Einträge
in 'metadata.file_conversions' und 'Nicht verarbeitbar'.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional
from src.config import AppConfig, load_config
from src.database import get_connection, init_database

RULE_RECOMMENDATIONS = {
    "missing_values": (
        "Pflichtfeldprüfung in der Datenquelle ergänzen oder fachlichen Standardwert definieren."
    ),
    "duplicate_rows": (
        "Primärschlüssel-Constraint oder Deduplizierung beim Export der Quelle einrichten."
    ),
    "invalid_type_or_format": (
        "Exportformat der Quelle vereinheitlichen (ISO-8601 für Datum, Punkt/Komma-Konvention prüfen)."
    ),
    "implausible_negative_values": (
        "Mess- oder Buchungslogik der Quelle auf Vorzeichenfehler und Rückbuchungen prüfen."
    ),
    "statistical_outliers_iqr_zscore": (
        "Spitzenwerte fachlich validieren (echte Lastspitze vs. Sensor-/Übertragungsfehler)."
    ),
    "truncated_or_garbled_content": (
        "Feldlängenbegrenzung und UTF-8-Zeichenkodierung beim Datenexport prüfen."
    ),
    "pii_pattern_detected": (
        "Spalte vor der Weiterverarbeitung pseudonymisieren oder aus dem Datenexport entfernen."
    ),
}


def build_quality_report(config: Optional[AppConfig] = None) -> Dict[str, object]:
    """
    Berechnet alle Kennzahlen, Anteile, priorisierten Probleme, nicht verarbeitbaren
    Daten sowie regelbasierten Empfehlungen und speichert den Bericht.
    """
    cfg = config or load_config()
    init_database(cfg)
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    conn = get_connection(cfg)
    try:
        sources = conn.execute(
            "SELECT * FROM metadata.quellen ORDER BY fetched_at DESC;"
        ).df().to_dict(orient="records")

        conversions = conn.execute(
            "SELECT * FROM metadata.file_conversions ORDER BY converted_at DESC;"
        ).df().to_dict(orient="records")

        gate_logs = conn.execute(
            "SELECT * FROM metadata.mart_gate_log ORDER BY checked_at DESC;"
        ).df().to_dict(orient="records")

        quality_rows = conn.execute(
            """
            SELECT * FROM metadata.quality_log
            WHERE affected_count > 0
            ORDER BY is_recurring DESC, share_pct DESC, affected_count DESC;
            """
        ).df().to_dict(orient="records")

        unparseable_files = conn.execute(
            "SELECT * FROM metadata.unparseable_files ORDER BY detected_at DESC;"
        ).df().to_dict(orient="records")

        unparseable_rows = conn.execute(
            "SELECT * FROM metadata.unparseable_rows ORDER BY detected_at DESC;"
        ).df().to_dict(orient="records")
    finally:
        conn.close()

    prioritized_issues = [
        q for q in quality_rows if q.get("priority_level") == "PRIORISIERT"
    ]

    recommendations: List[Dict[str, object]] = []
    for q in quality_rows:
        rule = str(q["rule_name"])
        base_action = RULE_RECOMMENDATIONS.get(
            rule, "Datenquelle und Spaltendefinition manuell prüfen."
        )
        recommendations.append(
            {
                "check_id": str(q["check_id"]),
                "source_name": str(q["source_name"]),
                "table_name": str(q["table_name"]),
                "column_name": str(q["column_name"]),
                "rule_name": rule,
                "share_pct": float(q["share_pct"]),
                "affected_count": int(q["affected_count"]),
                "priority_level": str(q["priority_level"]),
                "recommendation_text": (
                    f"[{q['priority_level']}] Tabelle '{q['table_name']}', Spalte '{q['column_name']}' "
                    f"weist {q['affected_count']} Befunde ({q['share_pct']:.2f} %) bei Regel '{rule}' auf "
                    f"(Prüf-ID: {q['check_id']}) -> Empfehlung: {base_action}"
                ),
            }
        )

    lines: List[str] = [
        "# Regelbasierter Datenqualitäts- und Verarbeitungsbericht",
        f"**Erstellt am:** {generated_at} | **Umgebung:** `{cfg.app_env}`",
        "",
        "## 1. Datenquellen (`metadata.quellen`)",
    ]
    if not sources:
        lines.append("* Keine Quellen registriert.")
    else:
        for src in sources:
            lines.append(
                f"* **{src['name']}** (`{src['source_id']}`) – Lizenz: `{src['license']}` | "
                f"Zeilen: {src['row_count']} | Rohdatei: `{Path(str(src['raw_file_path'])).name}` "
                f"({src['file_size_bytes']} Bytes)"
            )

    lines.extend(["", "## 2. Dateiannahme & Parquet-Konvertierung (`metadata.file_conversions`)"])
    if not conversions:
        lines.append("* Keine Dateikonvertierungen protokolliert.")
    else:
        for conv in conversions:
            lines.append(
                f"* Datei `{conv['original_filename']}` ({conv['file_format']}): "
                f"Vorher {conv['size_before_bytes']} Bytes -> Nachher (Parquet) {conv['size_after_bytes']} Bytes "
                f"(**{conv['compression_ratio_pct']:.2f} % Ersparnis**), "
                f"{conv['row_count']} Zeilen in {conv['chunks_count']} Chunk(s), "
                f"Dauer: **{conv['duration_ms']:.2f} ms**."
            )

    lines.extend(["", "## 3. Abschließende Freigabe vor Visualisierung (`metadata.mart_gate_log`)"])
    if not gate_logs:
        lines.append("* Keine Freigabeprüfungen protokolliert.")
    else:
        for g in gate_logs:
            state = "FREIGEGEBEN" if g["passed"] else "GESPERRT"
            lines.append(
                f"* **[{state}]** `{g['source_table']}` -> `{g['mart_table']}` "
                f"({g['total_rows']} Zeilen) – Begründung: {g['blocking_reasons']} "
                f"(Gate-ID: `{g['gate_id']}`)"
            )

    lines.extend(["", "## 4. Priorisierte und wiederkehrende Qualitätsprobleme"])
    if not prioritized_issues:
        lines.append("* Keine priorisierten oder wiederkehrenden Probleme festgestellt.")
    else:
        for p in prioritized_issues:
            rec_label = "wiederkehrend" if p["is_recurring"] else "Schwellenwert überschritten"
            lines.append(
                f"* **[PRIORISIERT – {rec_label}]** Quelle `{p['source_name']}`, "
                f"Tabelle `{p['table_name']}`, Spalte `{p['column_name']}`, Regel `{p['rule_name']}`: "
                f"**{p['affected_count']} von {p['total_count']} Zeilen ({p['share_pct']:.2f} %)** – "
                f"{p['explanation']} (Prüf-ID: `{p['check_id']}`)"
            )

    lines.extend(["", "## 5. Alle festgestellten Qualitätsbefunde (nach Anteil sortiert)"])
    if not quality_rows:
        lines.append("* Keine Abweichungen in den geprüften Tabellen festgestellt.")
    else:
        for q in quality_rows:
            lines.append(
                f"* `{q['table_name']}.{q['column_name']}` | Regel `{q['rule_name']}` "
                f"(Stufe `{q['stage']}`): **{q['affected_count']} ({q['share_pct']:.2f} %)** – "
                f"{q['explanation']} [Nachweis: `{q['check_id']}`]"
            )

    lines.extend(["", "## 6. Abschnitt: Nicht verarbeitbar (Herkunft & Ursache)"])
    if not unparseable_files and not unparseable_rows:
        lines.append("* Alle eingegangenen Dateien und Einzelwerte konnten vollständig verarbeitet werden.")
    else:
        for uf in unparseable_files:
            lines.append(
                f"* **[Abgelehnte Datei]** `{uf['original_filename']}` (Format `{uf['file_format']}`) -> "
                f"Ursache: {uf['reason']} (Ablage: `{uf['rejected_path']}`)"
            )
        for ur in unparseable_rows[:50]:
            lines.append(
                f"* **[Nicht konvertierbarer Wert]** Quelle `{ur['source_name']}`, "
                f"Tabelle `{ur['table_name']}`, Zeile `{ur['row_index']}`, Spalte `{ur['column_name']}`, "
                f"Rohwert `{ur['raw_value']}` -> Ursache: {ur['reason']}"
            )
        if len(unparseable_rows) > 50:
            lines.append(
                f"* *(Weitere {len(unparseable_rows) - 50} nicht konvertierbare Einzelwerte in "
                "`metadata.unparseable_rows` dokumentiert.)*"
            )

    lines.extend(["", "## 7. Konkrete Empfehlungen (aus festen Regeln)"])
    if not recommendations:
        lines.append("* Keine Maßnahmen erforderlich.")
    else:
        for rec in recommendations:
            lines.append(f"* {rec['recommendation_text']}")

    markdown_text = "\n".join(lines) + "\n"

    report_md_path = cfg.mart_dir / "quality_report_latest.md"
    report_json_path = cfg.mart_dir / "quality_report_latest.json"

    report_payload = {
        "generated_at": generated_at,
        "environment": cfg.app_env,
        "sources_count": len(sources),
        "conversions_count": len(conversions),
        "findings_count": len(quality_rows),
        "prioritized_count": len(prioritized_issues),
        "unparseable_files_count": len(unparseable_files),
        "unparseable_rows_count": len(unparseable_rows),
        "recommendations": recommendations,
        "markdown": markdown_text,
    }

    report_md_path.write_text(markdown_text, encoding="utf-8")
    report_json_path.write_text(
        json.dumps(report_payload, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )

    return {
        "report_md_path": str(report_md_path),
        "report_json_path": str(report_json_path),
        "findings_count": len(quality_rows),
        "prioritized_count": len(prioritized_issues),
        "recommendations_count": len(recommendations),
        "markdown": markdown_text,
    }


if __name__ == "__main__":
    rep = build_quality_report()
    print(f"Bericht erstellt: {rep['report_md_path']} ({rep['recommendations_count']} Empfehlungen)")
