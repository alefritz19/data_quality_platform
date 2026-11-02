#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dagster Zeitpläne (Schedules) für den regelmäßigen Abruf öffentlicher Quellen.
"""

from dagster import DefaultScheduleStatus, ScheduleDefinition
from dagster_app.sensors import full_pipeline_job

daily_public_sources_schedule = ScheduleDefinition(
    job=full_pipeline_job,
    cron_schedule="0 6 * * *",
    execution_timezone="Europe/Berlin",
    default_status=DefaultScheduleStatus.STOPPED,
    description="Täglicher Abruf der öffentlichen Datenquellen um 06:00 Uhr (Produktiv-Kontext).",
)

demo_periodic_reset_schedule = ScheduleDefinition(
    job=full_pipeline_job,
    cron_schedule="0 */12 * * *",
    execution_timezone="Europe/Berlin",
    default_status=DefaultScheduleStatus.STOPPED,
    description="Regelmäßige Zurücksetzung und Bereinigung des Demo-Kontexts alle 12 Stunden.",
)
