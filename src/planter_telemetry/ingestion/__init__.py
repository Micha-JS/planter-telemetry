"""Ingestion service: validated, idempotent MQTT-to-TimescaleDB writes.

Layered like the simulator so the interesting parts are pure and
unit-testable without a broker or a database:

- core: classify one (topic, payload) into a valid reading or a dead letter
- db: the database edge — idempotent inserts and the dead-letter sink
- service: the impure wiring — MQTT consumption, reconnects, shutdown

Delivery semantics (QoS 1, persistent session, at-least-once + idempotent
writes) are documented in docs/ingestion.md.
"""
