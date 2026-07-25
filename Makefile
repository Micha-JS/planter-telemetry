.PHONY: lint typecheck test up down smoke

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
smoke: up
	docker compose exec -T mosquitto sh -ec ' \
		for i in $$(seq 1 30); do \
			mosquitto_pub -t planter/ready -m ping 2>/dev/null && break; \
			sleep 1; \
		done; \
		mosquitto_sub -t "planter/#" -C 1 -W 10 -v > /tmp/received & \
		sleep 1; \
		mosquitto_pub -t planter/smoke -m hello; \
		wait $$!; \
		grep -qx "planter/smoke hello" /tmp/received'
	docker compose down
