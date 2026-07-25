.PHONY: lint typecheck test up down smoke sim-smoke

lint:
	uv run ruff format --check .
	uv run ruff check .

typecheck:
	uv run mypy

test:
	uv run pytest

up:
	docker compose up -d

down:
	docker compose down

# End-to-end broker check: wait until the broker accepts connections, then do a
# pub/sub round-trip through it. Uses the clients bundled in the image.
# Starts only the broker and subscribes to an exact topic so the simulator's
# telemetry stream can never leak into the assertion.
smoke:
	docker compose up -d mosquitto
	docker compose exec -T mosquitto sh -ec ' \
		for i in $$(seq 1 30); do \
			mosquitto_pub -t planter/ready -m ping 2>/dev/null && break; \
			sleep 1; \
		done; \
		mosquitto_sub -t "planter/smoke" -C 1 -W 10 -v > /tmp/received & \
		sleep 1; \
		mosquitto_pub -t planter/smoke -m hello; \
		wait $$!; \
		grep -qx "planter/smoke hello" /tmp/received'
	docker compose down

# Full-stack check: broker + simulator via compose, then capture a slice of the
# telemetry stream and confirm real schema JSON is flowing on the v1 topic.
sim-smoke:
	docker compose up -d --build
	docker compose exec -T mosquitto sh -ec ' \
		mosquitto_sub -t "planter/v1/+/telemetry" -C 8 -W 60 -v > /tmp/sim; \
		grep -q "schema_version" /tmp/sim'
	docker compose down
