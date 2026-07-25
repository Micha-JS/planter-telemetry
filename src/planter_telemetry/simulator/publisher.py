"""The impure edge: wall clock, sleeps, and MQTT publishing.

Everything below this module runs in virtual time; this is where virtual
time is mapped onto the wall clock via the acceleration factor. Note that
with acceleration > 1, measured_at timestamps run ahead of the wall clock —
acceptable for the synthetic demo, and it keeps the generation layers pure.
"""

import logging
import time
from datetime import UTC, datetime

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

from planter_telemetry.contract import telemetry_topic
from planter_telemetry.simulator.config import SimulatorSettings
from planter_telemetry.simulator.stream import build_streams
from planter_telemetry.simulator.wire import corrupt, encode

logger = logging.getLogger("planter_telemetry.simulator")


def connect_with_retry(
    settings: SimulatorSettings, attempts: int = 30, delay_seconds: float = 1.0
) -> mqtt.Client:
    """Connect to the broker, retrying while it starts up.

    Mirrors the repo's imperative-readiness style (compose has no
    healthchecks yet). After the initial connect, paho's network loop
    handles reconnects on broker restarts.
    """
    client = mqtt.Client(CallbackAPIVersion.VERSION2)
    for attempt in range(1, attempts + 1):
        try:
            client.connect(settings.mqtt_host, settings.mqtt_port)
        except OSError:
            if attempt == attempts:
                raise
            logger.info(
                "broker %s:%d not ready (attempt %d/%d), retrying",
                settings.mqtt_host,
                settings.mqtt_port,
                attempt,
                attempts,
            )
            time.sleep(delay_seconds)
        else:
            client.loop_start()
            return client
    raise AssertionError("unreachable")


def run(settings: SimulatorSettings) -> None:
    """Publish the emission stream forever, pacing it by the wall clock."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logger.info(
        "starting: %d devices, seed=%d, acceleration=%gx, interval=%gs (virtual)",
        settings.device_count,
        settings.seed,
        settings.acceleration,
        settings.interval_seconds,
    )
    client = connect_with_retry(settings)
    anchor = datetime.now(UTC)
    wall_start = time.monotonic()
    try:
        for emission in build_streams(settings, anchor):
            virtual_elapsed = (emission.publish_at - anchor).total_seconds()
            wall_target = virtual_elapsed / settings.acceleration
            sleep_for = wall_target - (time.monotonic() - wall_start)
            if sleep_for > 0:
                time.sleep(sleep_for)

            payload = encode(emission.reading)
            if emission.corruption is not None:
                payload = corrupt(payload, emission.corruption)
            topic = telemetry_topic(emission.reading.device_id)
            client.publish(topic, payload, qos=1)
            logger.info(
                "%s %s%s water=%.1f%% battery=%.3fV measured_at=%s",
                topic,
                emission.kind,
                f" corruption={emission.corruption.value}" if emission.corruption else "",
                emission.reading.water_level,
                emission.reading.battery_voltage,
                emission.reading.measured_at.isoformat(),
            )
    except KeyboardInterrupt:
        logger.info("interrupted, disconnecting")
    finally:
        client.loop_stop()
        client.disconnect()
