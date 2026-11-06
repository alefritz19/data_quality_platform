# Projektregeln und Architektur (ARCHITECTURE.md)

## 1. Ziel des Projekts
Transparente Plattform für Datenqualität auf Basis ausschließlich realer, öffentlicher Datenquellen (keine synthetischen Daten). Die Plattform lädt Rohdaten, konvertiert Dateien nach Parquet, profiliert sie, findet Qualitätsprobleme und Anomalien, führt eine abschließende Prüfung vor der Visualisierung durch und erzeugt einen nachvollziehbaren Bericht mit Empfehlungen.

Kernprinzip: **Transparenz** – für jedes Problem ist klar dokumentiert, aus welcher Quelle, Tabelle, Spalte und Regel es stammt.

## 2. Verbindliche Arbeitsregeln
1. **Strikt bei der Aufgabe bleiben:** Nichts ungefragt ändern oder erweitern.
2. **Erst Plan, dann Code:** Vor jeder größeren Änderung kurzer Plan (max. 5 Punkte), auf Bestätigung warten.
3. **Kleine Schritte:** Eine Aufgabe pro Durchgang. Danach kurz erklären, was geändert wurde und wie man es testet.
4. **Keine neuen Abhängigkeiten ohne Rückfrage.**
5. **Nie Secrets committen:** `.env`, `.env.prod` und `.env.demo` bleiben in `.gitignore`. Im Repository liegt nur `.env.example` mit Platzhaltern.
6. **Kurz und direkt antworten.**
7. **Sprache:** Code-Kommentare, Dokumentation und README auf Deutsch. Variablen-, Modul- und Funktionsnamen auf Englisch.
8. **Ressourcen schonen:** Keine ganzen Dateien ausgeben, wenn ein Diff genügt.
9. **Keine Übertreibungen:** Keine unbelegten Marketing-Begriffe. Nur überprüfbare Aussagen mit konkreten Zahlen.

## 3. Technischer Stack
* **Sprache:** Python 3.12 (`linux/arm64`-kompatibel)
* **Orchestrierung:** Dagster (Assets, Asset Checks, Schedules, Sensors)
* **Verarbeitungs-Engine:** DuckDB
* **Speicherformat:** Apache Parquet (Zwischenschichten und Export)
* **Qualitätsregeln:** Pandera / eigene regelbasierte Prüfungen (Stufe 1), Isolation Forest mit Begründungspflicht (Stufe 2)
* **Visualisierung:** Streamlit + Plotly
* **Container:** Docker Compose (`docker-compose.prod.yml` und `docker-compose.demo.yml`)

## 4. Zwei getrennte Orchestrierungen (prod vs. demo)
* **Produktiv (`prod`):** `docker-compose.prod.yml`, `.env.prod`, volle Historie, Aufbewahrung fertiger Exporte: **72 Stunden**, Limit: **5.000 MB** (max. 500 MB pro Datei).
* **Demo (`demo`):** `docker-compose.demo.yml`, `.env.demo`, fester Ausschnitt realer Daten, öffentlich lesbar, Schreibschutz aktiv (`DEMO_READ_ONLY=true`), Aufbewahrung: **24 Stunden**, Limit: **500 MB** (max. 20 MB pro Datei), feste CPU-/RAM-Limits.

## 5. Reihenfolge der Umsetzung (14 Schritte)
1. Projektgerüst, Git, `.gitignore`, `.env.example`, `ARCHITECTURE.md` (Abgeschlossen)
2. Eine einzige reale Quelle anbinden (`raw`, inkl. Quellentabelle)
3. Dateiannahme: Eingangsordner (`inbox`), Konvertierung nach Parquet, Chunking
4. Staging mit DuckDB (`staging`)
5. Qualitätsprüfungen und `quality_log` (inkl. Abschnitt „Nicht verarbeitbar“)
6. Abschließende Prüfung vor der Visualisierung (Gatekeeper-Schritt)
7. Dagster: Assets, Asset Checks, Sensor für den Eingangsordner
8. Streamlit: KPIs, Tabellen, wählbare Diagramme
9. Regelbasierter Bericht mit konkreten Empfehlungen
10. Bereitstellung fertiger Daten (Download, temporäre Aufbewahrung mit Auto-Löschung)
11. Demo-Kontur: eigene Compose-Datei, Ressourcen-Limits, Schreibschutz
12. Weitere reale Quellen hinzufügen
13. Lernende Anomalieprüfung (Stufe 2: Isolation Forest mit erklärbarer Begründung)
14. Deployment auf Oracle Cloud ARM64 Server, README mit gemessenen Zahlen
