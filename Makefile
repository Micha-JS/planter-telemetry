.PHONY: lint typecheck test integration up down down-clean migrate smoke sim-smoke ingest-smoke grafana-smoke

lint:
	uv run ruff format --check .
	uv run ruff check .

typecheck:
	uv run mypy

test:
	uv run pytest

# Real broker + real TimescaleDB via testcontainers; needs a docker daemon.
integration:
	uv run pytest -m integration

up:
	docker compose up -d

down:
	docker compose down

# Also removes the pgdata volume: the next `up` starts from an empty,
# freshly migrated database — the pre-M3 "pristine demo" behavior.
down-clean:
	docker compose down -v

# Migrate the compose database from the host (the stack runs this itself via
# the migrate service; this target is for ad-hoc use against port 5433).
migrate:
	uv run python -m planter_telemetry.migrate

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
		for i in $$(seq 1 30); do \
			mosquitto_pub -t planter/ready -m ping 2>/dev/null && break; \
			sleep 1; \
		done; \
		mosquitto_sub -t "planter/v1/+/telemetry" -C 8 -W 60 -v > /tmp/sim; \
		grep -q "schema_version" /tmp/sim'
	docker compose down

# Full-pipeline check: simulator → broker → ingestion → TimescaleDB. The row
# count must GROW past a baseline taken right after startup — the persisted
# volume may already hold rows from an earlier run (e.g. sim-smoke), and a
# mere count > 0 would pass on that stale data without proving this run's
# ingestion works. The baseline query needs no retry: `up -d` only returns
# once ingestion has started, which implies migrate completed against a
# healthy database. The ingestion container must also still be running
# afterwards (it never crashes on the simulator's injected garbage).
ingest-smoke:
	docker compose up -d --build
	baseline=$$(docker compose exec -T timescaledb \
		psql -U planter -d planter -tAc "SELECT count(*) FROM telemetry"); \
	for i in $$(seq 1 60); do \
		count=$$(docker compose exec -T timescaledb \
			psql -U planter -d planter -tAc "SELECT count(*) FROM telemetry" 2>/dev/null) \
			&& [ "$$count" -gt "$$baseline" ] && break; \
		sleep 1; \
		if [ $$i -eq 60 ]; then echo "no new telemetry rows after 60s" >&2; exit 1; fi; \
	done
	docker compose ps --status running --format '{{.Service}}' | grep -qx ingestion
	docker compose down

# Provisioning end-to-end: grafana's healthcheck runs the datasource health
# check (SELECT 1 against TimescaleDB as grafana_reader), so "healthy" already
# proves migrate created the role and the provisioned datasource connects.
# Then: the dashboard loads anonymously by uid, the datasource exists by uid
# (admin API), and a real query through grafana's /api/ds/query returns a
# NONZERO telemetry count — an empty database also produces data frames, so
# the assertion must see actual rows (polled, since on a fresh stack the
# first readings may still be landing). All HTTP runs inside the grafana
# container via busybox wget — no host dependencies.
grafana-smoke:
	docker compose up -d --build
	for i in $$(seq 1 60); do \
		status=$$(docker inspect -f '{{.State.Health.Status}}' \
			$$(docker compose ps -q grafana)); \
		[ "$$status" = "healthy" ] && break; \
		sleep 2; \
		if [ $$i -eq 60 ]; then \
			echo "grafana never became healthy" >&2; \
			docker compose logs grafana >&2; exit 1; \
		fi; \
	done
	docker compose exec -T grafana wget -q -O /dev/null \
		http://127.0.0.1:3000/api/dashboards/uid/planter-fleet
	docker compose exec -T grafana sh -ec 'wget -q -O - \
		"http://admin:$$GF_SECURITY_ADMIN_PASSWORD@127.0.0.1:3000/api/datasources/uid/planter-timescaledb" \
		| grep -q "\"uid\":\"planter-timescaledb\""'
	for i in $$(seq 1 30); do \
		count=$$(docker compose exec -T grafana sh -ec 'wget -q -O - \
			--header "Content-Type: application/json" \
			--post-data "{\"queries\":[{\"refId\":\"A\",\"datasource\":{\"uid\":\"planter-timescaledb\"},\"format\":\"table\",\"rawSql\":\"SELECT count(*) FROM telemetry\"}]}" \
			http://127.0.0.1:3000/api/ds/query' \
			| sed -n 's/.*"values":\[\[\([0-9]*\).*/\1/p'); \
		[ -n "$$count" ] && [ "$$count" -gt 0 ] && break; \
		sleep 2; \
		if [ $$i -eq 30 ]; then \
			echo "no telemetry rows visible through grafana (count=$${count:-unparsed})" >&2; \
			exit 1; \
		fi; \
	done
	docker compose down
