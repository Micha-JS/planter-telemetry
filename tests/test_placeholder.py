"""Placeholder test so CI has something to run from M0 onward."""

from importlib.metadata import version

import planter_telemetry


def test_version_matches_package_metadata() -> None:
    assert planter_telemetry.__version__ == version("planter-telemetry")
