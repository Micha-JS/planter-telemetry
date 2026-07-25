"""Adversarial device simulator: N fake planters publishing telemetry to MQTT.

Layered so the interesting parts are pure and unit-testable without a broker:

- dynamics: per-device physics (depletion, refills, battery decay) in virtual time
- stream: injected imperfections (duplicates, out-of-order, malformed, missed
  check-ins) and the multi-device merge
- wire: JSON encoding and payload corruption
- publisher: the only impure layer — wall clock, sleeps, MQTT
"""
