"""Entrypoint: python -m planter_telemetry.simulator"""

from planter_telemetry.jsonlog import configure
from planter_telemetry.simulator.config import SimulatorSettings
from planter_telemetry.simulator.publisher import run

if __name__ == "__main__":
    # Logging setup lives here (not in run()) so in-process test runs don't
    # fight pytest over the root logger — same placement as ingestion.
    configure(service="simulator")
    run(SimulatorSettings())
