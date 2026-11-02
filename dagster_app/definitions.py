#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Zentrale Dagster-Definitionen (Assets, Asset Checks, Jobs, Schedules, Sensors).
"""

from dagster import Definitions
from dagster_app.assets import (
    parquet_conversions,
    quality_check_results,
    quality_report,
    raw_public_sources,
    staging_tables,
    verified_mart_tables,
)
from dagster_app.checks import check_mart_gate_passed, check_no_pii_in_staging
from dagster_app.schedules import (
    daily_public_sources_schedule,
    demo_periodic_reset_schedule,
)
from dagster_app.sensors import full_pipeline_job, inbox_file_sensor, inbox_processing_job

defs = Definitions(
    assets=[
        raw_public_sources,
        parquet_conversions,
        staging_tables,
        quality_check_results,
        verified_mart_tables,
        quality_report,
    ],
    asset_checks=[
        check_no_pii_in_staging,
        check_mart_gate_passed,
    ],
    jobs=[
        full_pipeline_job,
        inbox_processing_job,
    ],
    schedules=[
        daily_public_sources_schedule,
        demo_periodic_reset_schedule,
    ],
    sensors=[
        inbox_file_sensor,
    ],
)
