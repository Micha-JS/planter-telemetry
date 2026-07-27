"""Pure ops helpers: /healthz payload truth table, metrics registry view."""

import pytest
from prometheus_client import generate_latest

from planter_telemetry.ingestion.ops import HealthState, build_registry, healthz_payload
from planter_telemetry.ingestion.service import Counters


@pytest.mark.parametrize(
    ("broker", "db", "expected_status"),
    [
        (True, True, 200),
        (True, False, 503),
        (False, True, 503),
        (False, False, 503),
    ],
)
def test_healthz_payload_truth_table(broker: bool, db: bool, expected_status: int) -> None:
    health = HealthState(broker_connected=broker, db_connected=db)
    status, body = healthz_payload(health, Counters())
    assert status == expected_status
    assert body["status"] == ("healthy" if status == 200 else "unhealthy")
    assert body["broker_connected"] is broker
    assert body["db_connected"] is db


def test_healthz_payload_carries_counters() -> None:
    counters = Counters(ingested=7, deduplicated=2, dead_lettered=1, out_of_order=3)
    _, body = healthz_payload(HealthState(), counters)
    assert body["counters"] == {
        "ingested": 7,
        "deduplicated": 2,
        "dead_lettered": 1,
        "out_of_order": 3,
    }


def test_registry_is_a_live_view_over_counters() -> None:
    counters = Counters()
    registry = build_registry(counters)
    counters.ingested = 41
    counters.ingested += 1  # mutated after registration: the view must track it
    text = generate_latest(registry).decode()
    assert "planter_ingestion_ingested_total 42.0" in text
    assert "planter_ingestion_out_of_order_total 0.0" in text
    assert "python_info" in text  # platform basics ride along
