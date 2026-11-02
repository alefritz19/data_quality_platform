# Transparente Plattform für Datenqualität und Parquet-Verarbeitung

Technische Anwendung zur Dateiannahme, Parquet-Konvertierung (ZSTD) und mehrstufigen Qualitätsprüfung realer, öffentlicher Datensätze mit Python, DuckDB, Dagster und Streamlit.

Der Fokus des Projekts liegt auf Nachvollziehbarkeit: Daten werden beim Einlesen nicht stillschweigend verändert oder gelöscht. Stattdessen werden Konvertierungskennzahlen, erkannte Lücken, statistische Auffälligkeiten und nicht verarbeitbare Einträge mit Quelle, Spalte und Ursache in DuckDB-Metadatentabellen protokolliert.

---

## Technischer Aufbau & Funktionen

* **Nachvollziehbare Datenquellen (`metadata.quellen`):** Verarbeitung öffentlicher Originaldaten der Bundesnetzagentur (SMARD.de) und des Deutschen Wetterdienstes (DWD, Messstation 01420 Frankfurt am Main). Jede Quelle wird mit Name, Abruf-URL, Lizenz und Zeitstempel dokumentiert.
* **Dateiannahme & Parquet-Konvertierung (`metadata.file_conversions`):** Überwachter Eingangsordner (`data/inbox`) für `CSV`, `TSV`, `XLSX`, `XLS`, `JSON`, `JSONL` und `Parquet`. Große Dateien werden in konfigurierbaren Chunks verarbeitet und mit ZSTD-Kompression als Parquet gespeichert. Dateigröße vor/nach der Konvertierung, Kompressionsrate und Laufzeit in Millisekunden werden gemessen.
* **Gesonderter Ausweis nicht verarbeitbarer Daten (`metadata.unparseable_files`, `metadata.unparseable_rows`):** Beschädigte Dateien oder einzelne Werte, die sich bei der Typisierung im Staging nicht konvertieren lassen, werden nicht verworfen, sondern mit Rohwert, Zieltyp und Fehlergrund protokolliert.
* **Zweistufige Qualitätsprüfung:**
  1. **Stufe 1 – Regelbasierte Prüfung (`metadata.quality_log`):** Prüfung auf fehlende Werte (`NULL`), Duplikate, unplausible Negativwerte, statistische Ausreißer (IQR-Verfahren `1,5 × IQR` und `|z-Score| > 3,0`) sowie einfache reguläre Ausdrücke für gängige Muster (E-Mail, IBAN, Telefonnummern). Wiederkehrende Befunde werden markiert.
  2. **Stufe 2 – Lernende Anomalieprüfung (`metadata.ml_anomaly_flags`):** Ergänzender `IsolationForest`-Lauf auf numerischen Zeitreihen. Das Modell verändert keine Daten; jede Markierung wird nur zusammen mit der hauptverantwortlichen Spalte, dem beobachteten Wert, dem Spaltenmedian sowie dem z-Score und IQR-Abstand gespeichert.
* **Freigabeprüfung vor Visualisierung (`metadata.mart_gate_log`):** Tabellen werden erst nach bestandenem Prüfschritt (`final_gate`) in das Schema `mart` übernommen und im Dashboard freigeschaltet.
* **Tabellen- und KPI-zentriertes Dashboard (`dashboard/app.py`):**
  * Frei wählbare KPI-Karten für jede freigegebene Tabelle (Durchschnitt, Median, Minimum, Maximum, Summe, Vollständigkeit in %).
  * Spalten-Steckbrief & Transparenz-Matrix mit Kennzahlen und Befunden je Spalte.
  * Aggregierte Tages-/Gruppentabellen, Einzelzeilen-Inspektor (Filter für Lücken und Isolation-Forest-Markierungen) sowie wählbare Linien-, Balken- und Kreisdiagramme.
  * Umschaltbare Oberfläche (Deutsch / Englisch sowie heller und dunkler Modus).
* **Temporäre Datenbereitstellung (`data/export`):** Bereitstellung geprüfter Parquet-Dateien und Markdown-Qualitätsberichte als Download-Paket (`.parquet`, `.md`, `.zip`) mit automatischer Löschfrist (TTL) und Speicherbegrenzung.
* **Getrennte Laufzeitumgebungen (`PROD` / `DEMO`):** Steuerung über Umgebungsvariablen (`APP_ENV`, `DEMO_READ_ONLY`). In der Demo-Umgebung sind Schreib- und Pipeline-Aktionen standardmäßig gesperrt.

---

## Gemessene Laufzeit- und Konvertierungswerte (reale Datensätze)

| Datensatz (`mart`) | Quelle & Lizenz | Zeilen | CSV (`raw`) | Parquet (`ZSTD`) | Platzersparnis | Konvertierungsdauer |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `smard_strommarkt_de` | Bundesnetzagentur \| SMARD.de (CC BY 4.0) | 673 | 38.294 B | 8.666 B | **77,37 %** | **89,9 ms** |
| `dwd_wetter_frankfurt` | Deutscher Wetterdienst – Station 01420 (GeoNutzV / CC BY 4.0) | 673 | 34.670 B | 6.492 B | **81,27 %** | **92,7 ms** |

---

## Projektstruktur

```text
data_quality_platform/
├── src/                        # Kernmodule der Pipeline
│   ├── config.py               # Zentrale Konfiguration über Umgebungsvariablen (PROD / DEMO)
│   ├── db.py                   # DuckDB-Schemata (metadata, staging, mart)
│   ├── ingest_source.py        # Abruf öffentlicher Datenquellen (SMARD.de & DWD 01420)
│   ├── file_converter.py       # Eingangsordner-Verarbeitung, Chunking & Parquet-Konvertierung
│   ├── staging_loader.py       # Typisierung nach staging & Erfassung nicht konvertierbarer Werte
│   ├── quality_checks.py       # Stufe 1: Regelbasierte Qualitätsprüfungen & quality_log
│   ├── ml_anomaly_detector.py  # Stufe 2: Isolation Forest mit Kennzahl-Begründung
│   ├── final_gate.py           # Abschließende Freigabeprüfung vor Übernahme nach mart
│   ├── report_generator.py     # Regelbasierter Qualitätsbericht (Markdown)
│   └── export_manager.py       # Export-Pakete, TTL-Bereinigung & Speicherlimits
├── dagster_app/                # Dagster Assets, Asset Checks, Schedules & Inbox-Sensor
├── dashboard/                  # Streamlit-Oberfläche (dashboard/app.py)
├── config/                     # Umgebungsdateien (.env.prod.example, .env.demo.example)
├── docker-compose.prod.yml     # Produktiv-Kontur (72h TTL, 5 GB Exportlimit, bis 500 MB Upload)
├── docker-compose.demo.yml     # Demo-Kontur (24h TTL, 500 MB Exportlimit, Schreibschutz aktiv)
├── requirements.txt            # Python-Abhängigkeiten
└── README.md                   # Projektdokumentation
```

---

## Lokaler Start & Betrieb (`PROD` und `DEMO`)

### 1. Abhängigkeiten installieren
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Datenabruf und Pipeline-Durchlauf über die Kommandozeile
```bash
python3 -m src.ingest_source
python3 -m src.file_converter
python3 -m src.staging_loader
python3 -m src.quality_checks
python3 -m src.ml_anomaly_detector
python3 -m src.final_gate
python3 -m src.report_generator
```

### 3. Hauptversion (`PROD`, Port `8502`) und Demoversion (`DEMO`, Port `8503`) starten
```bash
# Hauptversion (PROD) mit vollem Schreib- und Pipeline-Zugriff auf Port 8502:
APP_ENV=prod DATA_DIR=./data/prod DEMO_READ_ONLY=false \
  streamlit run dashboard/app.py --server.port 8502

# Öffentliche Demoversion (DEMO) mit aktiviertem Schreibschutz auf Port 8503:
APP_ENV=demo DATA_DIR=./data/demo DEMO_READ_ONLY=true ADMIN_PIN=2026 \
  streamlit run dashboard/app.py --server.port 8503
```

---

## Umgesetzte Meilensteine (14-Schritte-Plan)

* [x] **Schritt 1:** Projektgerüst, Git, `.gitignore`, `.env.example`, `CLAUDE.md`
* [x] **Schritt 2:** Erste reale Datenquelle anbinden (`raw` + Tabelle `metadata.quellen`)
* [x] **Schritt 3:** Dateiannahme (`inbox`), Parquet-Konvertierung und Chunking
* [x] **Schritt 4:** Staging mit DuckDB (`staging` + Protokollierung nicht konvertierbarer Werte)
* [x] **Schritt 5:** Qualitätsprüfungen und `metadata.quality_log` (inkl. Abschnitt „Nicht verarbeitbar“)
* [x] **Schritt 6:** Abschließende Prüfung vor der Visualisierung (`final_gate` → `mart`)
* [x] **Schritt 7:** Dagster: Assets, Asset Checks, Schedules, Sensor für den Eingangsordner
* [x] **Schritt 8:** Streamlit: KPI-Karten, Tabellen, wählbare Diagramme (`dashboard/app.py`)
* [x] **Schritt 9:** Regelbasierter Bericht mit konkreten Empfehlungen (`src/report_generator.py`)
* [x] **Schritt 10:** Bereitstellung fertiger Daten (Download, TTL-Auto-Löschung und Speicherlimits)
* [x] **Schritt 11:** Demo- und Prod-Kontur (`docker-compose.prod.yml`, `docker-compose.demo.yml`, Limits, Schreibschutz)
* [x] **Schritt 12:** Weitere reale Quellen hinzufügen (Deutscher Wetterdienst / DWD Station 01420 Frankfurt/Main)
* [x] **Schritt 13:** Lernende Anomalieprüfung (zweite Stufe: Isolation Forest mit erklärbarer Begründung in `metadata.ml_anomaly_flags`)
* [x] **Schritt 14:** Deployment von `PROD` (`8502`) und `DEMO` (`8503`) auf dem Server sowie Dokumentation der realen Messwerte

## Lizenz

MIT License – siehe [LICENSE](LICENSE).
