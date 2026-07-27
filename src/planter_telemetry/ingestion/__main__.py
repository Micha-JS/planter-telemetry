"""Entrypoint: python -m planter_telemetry.ingestion"""

import asyncio

from planter_telemetry.ingestion.config import IngestSettings
from planter_telemetry.ingestion.service import run
from planter_telemetry.jsonlog import configure

if __name__ == "__main__":
    # Logging setup lives here (not in run()) so in-process test runs don't
    # fight pytest over the root logger.
    configure(service="ingestion")
    asyncio.run(run(IngestSettings()))
