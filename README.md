# Transparente Plattform für Datenqualität und Parquet-Verarbeitung

Technische Anwendung zur Dateiannahme, Parquet-Konvertierung (ZSTD) und mehrstufigen Qualitätsprüfung realer, öffentlicher Datensätze mit Python, DuckDB, Dagster und Streamlit.

Der Fokus des Projekts liegt auf Nachvollziehbarkeit: Daten werden beim Einlesen nicht stillschweigend verändert oder gelöscht. Stattdessen werden Konvertierungskennzahlen, erkannte Lücken, statistische Auffälligkeiten und nicht verarbeitbare Einträge mit Quelle, Spalte und Ursache in DuckDB-Metadatentabellen protokolliert.

---

## Impressionen der Plattform (DEMO-Laufzeitumgebung)

### 1. Übersicht, Quellenerfassung & Parquet-Konvertierung
Dokumentation der Originalquellen (Bundesnetzagentur / SMARD.de & Deutscher Wetterdienst / DWD), automatisches Chunking, Parquet-ZSTD-Kompression und Freigabeprüfung (`final_gate`).

![Übersicht & Datenquellen](assets/01_uebersicht_quellen.png)

---

### 2. Zweistufige Qualitätsprüfung (`quality_log` & `Isolation Forest`)
Stufe 1 (Regelwerk) prüft fehlende Werte, Duplikate, Wertebereiche und statistische Ausreißer (`1,5 × IQR`, `|z-Score| > 3,0`). Stufe 2 nutzt Isolation Forest auf Zeitreihen, ohne Daten zu verändern.

![Qualitätsprüfungen](assets/02_qualitaetspruefungen.png)

---

### 3. Wählbare KPI-Karten, Transparenz-Matrix & Dateneinsicht
Flexibel konfigurierbare Kennzahlenkarten für jede freigegebene Tabelle im Schema `mart`, Spalten-Steckbrief mit Vollständigkeitsgrad und Dateninspektion.

![Wählbare KPI-Karten & Transparenz-Matrix](assets/04_kpi_karten_matrix.png)

![Datenvorschau & konfigurierbare Diagramme](assets/05_daten_vorschau_diagramme.png)

---

### 4. Transparenter Umgang mit nicht verarbeitbaren Einträgen
Dateien oder Einzelwerte, die beim Staging nicht konvertiert werden können, werden gesondert mit Herkunft und Fehlerursache erfasst.

![Nicht verarbeitbare Daten](assets/03_nicht_verarbeitbar.png)

---

### 5. Regelbasierter Qualitätsbericht & Bereitstellung mit automatischer Löschfrist
Automatisierte Erstellung von Markdown-Qualitätsberichten und Bereitstellung fertiger Download-Pakete (`.parquet`, `.md`, `.zip`) mit TTL-Speicherbegrenzung.

![Regelbasierter Qualitätsbericht](assets/06_qualitaetsbericht.png)

![Bereitstellung & Download](assets/07_bereitstellung_download.png)

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
├── assets/                     # Screenshot-Dokumentation der Oberfläche
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
APP_ENV=demo DATA_DIR=./data/demo DEMO_READ_ONLY=true ADMIN_PIN=<eigener_pin> \
  streamlit run dashboard/app.py --server.port 8503
```

---

## Lizenz

MIT License – siehe [LICENSE](LICENSE).
