"""Capture/replay CLI.

Pure core, effects at the edges — the same split as the simulator and the
ingestion service:

- records.py — JSONL codec for captured messages (pure)
- schedule.py — replay pacing math (pure)
- sample.py — offline sample generation from the seeded simulator (pure)
- capture.py / replay.py — MQTT and clock edges
- app.py — argparse wiring and the console-script entrypoint

Replay publishes through MQTT only, never into the database: replayed
traffic exercises the same ingestion path as live traffic, and idempotency
is what makes that safe.
"""
