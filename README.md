# Plattform für Datenqualität und Parquet-Verarbeitung

Plattform zur Annahme, Parquet-Konvertierung und mehrstufigen Qualitätsprüfung realer, öffentlicher Datensätze mit DuckDB, Dagster und Streamlit.

## Kernfunktionen (im Aufbau nach 14-Schritte-Plan)

* **Dateiannahme & Parquet-Konvertierung:** Überwachter Eingangsordner für gängige Formate (CSV, Excel, JSON). Verarbeitung großer Dateien in Chunks und Speicherung als Parquet. Messung von Dateigröße (vorher/nachher) und Konvertierungsdauer.
* **Nachvollziehbare Datenquellen:** Verarbeitung ausschließlich realer, öffentlicher Datenquellen (z. B. Bundesnetzagentur / SMARD, Deutscher Wetterdienst / DWD). Jede Quelle wird mit Name, URL, Lizenz und Abrufdatum in der Tabelle `quellen` protokolliert.
* **Mehrstufige Qualitätsprüfung:** Protokollierung jedes Befunds mit Quelle, Tabelle, Spalte, Regel, Anzahl und prozentualem Anteil im `quality_log`. Nicht verarbeitbare Zeilen werden gesondert mit Ursache ausgewiesen.
* **Abschließende Prüfung vor Visualisierung:** Eigener Prüfschritt vor der Freigabe für das Dashboard.
* **Temporäre Datenbereitstellung:** Geprüfte Parquet-Dateien und Berichte stehen zeitlich begrenzt zum Download bereit (`prod`: 72 Stunden / max. 5 GB; `demo`: 24 Stunden / max. 500 MB) und werden danach automatisch gelöscht.
* **Getrennte Umgebungen:** Vollständige Trennung zwischen Produktiv-Orchestrierung (`docker-compose.prod.yml`) und ressourcenbegrenzter Demo-Orchestrierung (`docker-compose.demo.yml`).

## Projektstruktur

```text
data_quality_platform/
├── src/                  # Gemeinsamer Code (Ingest, Parquet-Konvertierung, Checks, Berichte)
├── dagster_app/          # Dagster Assets, Asset Checks, Schedules, Sensors
├── dashboard/            # Streamlit-Anwendung
├── config/               # Umgebungskonfigurationen
├── data/                 # Lokale Datenschichten (nicht in Git)
│   ├── inbox/            # Eingangsordner für neue Dateien
│   ├── raw/              # Unveränderte Originaldaten
│   ├── staging/          # Bereinigte und typisierte Parquet-Daten
│   ├── mart/             # DuckDB-Datenbank, Kennzahlen und quality_log
│   ├── export/           # Temporäre Bereitstellung geprüfter Daten
│   └── rejected/         # Nicht verarbeitbare Daten mit Fehlerursache
├── logs/                 # Laufzeitprotokolle (nicht in Git)
├── .env.example          # Konfigurationsvorlage ohne Geheimnisse
├── CLAUDE.md             # Verbindliche Architektur- und Arbeitsregeln
├── LICENSE               # MIT-Lizenz
└── README.md             # Projektdokumentation
```

## Status der Umsetzung

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
* [ ] **Schritt 12:** Weitere reale Quellen hinzufügen

## Angebundene Datenquellen (`metadata.quellen`)

| Kennung | Quelle | Lizenz | Gespeicherte Felder (`raw`) |
| :--- | :--- | :--- | :--- |
| `smard_de_hour` | Bundesnetzagentur \| SMARD.de – Strommarktdaten Deutschland | CC BY 4.0 | `timestamp_ms`, `datetime_utc`, `region`, `grid_load_mwh`, `solar_generation_mwh`, `wind_onshore_mwh` |

## Lizenz

MIT License – siehe [LICENSE](LICENSE).
