"""Entrypoint: python -m planter_telemetry.cli"""

import sys

from planter_telemetry.cli.app import main

if __name__ == "__main__":
    sys.exit(main())
