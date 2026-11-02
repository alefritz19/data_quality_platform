#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Automatisierte Unit-Tests für die Stufe-1-Qualitätsprüfungen (quality_checks.py).
Prüft die Kernfunktionen der Pipeline in isolierter DuckDB-Testumgebung:
  - Vollständigkeit (NULL-Werte)
  - Tabellenweite Duplikaterkennung
  - Statistische Ausreißer nach IQR- und z-Score-Verfahren
"""

import duckdb
import numpy as np
import pandas as pd
import pytest

from src.config import AppConfig
from src.database import init_database
from src.quality_checks import run_quality_checks_on_table


@pytest.fixture
def test_env(tmp_path):
    """Erzeugt eine isolierte temporäre AppConfig & DuckDB-Datenbank für Tests."""
    db_file = tmp_path / "test_platform.duckdb"
    cfg = AppConfig(
        app_env="test",
        data_dir=tmp_path / "data",
        inbox_dir=tmp_path / "data" / "inbox",
        raw_dir=tmp_path / "data" / "raw",
        staging_dir=tmp_path / "data" / "staging",
        mart_dir=tmp_path / "data" / "mart",
        export_dir=tmp_path / "data" / "export",
        rejected_dir=tmp_path / "data" / "rejected",
        duckdb_path=db_file,
        log_file=tmp_path / "logs" / "test.log",
        log_level="DEBUG",
        chunk_size_rows=1000,
        max_upload_size_mb=10,
        export_retention_hours=1,
        export_max_total_mb=100,
        demo_read_only=False,
        admin_pin="test_pin",
        smard_weeks_count=1,
    )
    init_database(cfg)
    return cfg


def test_missing_values_and_duplicates(test_env):
    """Testet Erkennung von Lücken (missing_values) und Duplikaten (duplicate_rows)."""
    df = pd.DataFrame(
        {
            "zeit": ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-03"],
            "messwert": [10.5, np.nan, 12.0, 12.0],  # 1 NaN, 1 Duplikat
        }
    )

    conn = duckdb.connect(str(test_env.duckdb_path))
    conn.execute("CREATE TABLE staging.test_table AS SELECT * FROM df;")
    conn.close()

    findings = run_quality_checks_on_table("staging.test_table", config=test_env)

    # Duplikat-Befund prüfen
    dup_find = [f for f in findings if f["rule_name"] == "duplicate_rows"]
    assert len(dup_find) == 1
    assert dup_find[0]["affected_count"] == 1

    # Missing-Value-Befund prüfen
    miss_find = [
        f
        for f in findings
        if f["rule_name"] == "missing_values" and f["column_name"] == "messwert"
    ]
    assert len(miss_find) == 1
    assert miss_find[0]["affected_count"] == 1


def test_iqr_outliers_detection(test_env):
    """Testet die statistische Ausreißererkennung über 1.5x IQR und z-Score."""
    np.random.seed(42)
    normal_vals = np.random.normal(loc=50, scale=2, size=60).tolist()
    normal_vals.append(999.0)  # Eindeutiger Ausreißer

    df = pd.DataFrame(
        {
            "id": list(range(len(normal_vals))),
            "sensor": normal_vals,
        }
    )

    conn = duckdb.connect(str(test_env.duckdb_path))
    conn.execute("CREATE TABLE staging.test_outliers AS SELECT * FROM df;")
    conn.close()

    findings = run_quality_checks_on_table("staging.test_outliers", config=test_env)

    outlier_find = [
        f
        for f in findings
        if f["rule_name"] == "statistical_outliers_iqr_zscore"
        and f["column_name"] == "sensor"
    ]
    assert len(outlier_find) == 1
    assert outlier_find[0]["affected_count"] >= 1
