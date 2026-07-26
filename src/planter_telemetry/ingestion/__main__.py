"""Entrypoint: python -m planter_telemetry.ingestion"""

import asyncio
import logging

from planter_telemetry.ingestion.config import IngestSettings
from planter_telemetry.ingestion.service import run

if __name__ == "__main__":
    # basicConfig lives here (not in run()) so in-process test runs don't
    # fight pytest over the root logger.
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(run(IngestSettings()))
