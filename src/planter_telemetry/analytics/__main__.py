"""Entrypoint: python -m planter_telemetry.analytics"""

import asyncio

from planter_telemetry.analytics.config import AnalyticsSettings
from planter_telemetry.analytics.service import run
from planter_telemetry.jsonlog import configure

if __name__ == "__main__":
    # Logging setup lives here (not in run()) so in-process test runs don't
    # fight pytest over the root logger.
    configure(service="analytics")
    asyncio.run(run(AnalyticsSettings()))
