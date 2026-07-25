"""Entrypoint: python -m planter_telemetry.simulator"""

from planter_telemetry.simulator.config import SimulatorSettings
from planter_telemetry.simulator.publisher import run

if __name__ == "__main__":
    run(SimulatorSettings())
