.PHONY: lint typecheck test integration up down down-clean migrate sample smoke sim-smoke ingest-smoke grafana-smoke replay-smoke hardware-up hardware-down hardware-passwd hardware-config-check

# What ingesting samples/telemetry-window.jsonl must produce, pinned in
# src/planter_telemetry/cli/sample.py and verified by tests/test_sample.py:
# 200 messages = 185 unique valid rows + 13 duplicates + 2 dead letters.
SAMPLE_ROWS = 185
SAMPLE_VALID_MSGS = 198
SAMPLE_WINDOW = measured_at >= '2026-01-01' AND measured_at < '2026-02-01'

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

# Regenerate the committed sample capture (deterministic; tests/test_sample.py
# asserts the committed file matches this output byte-for-byte).
sample:
	uv run planter-telemetry sample --out samples/telemetry-window.jsonl

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

# The M5 done-criteria, asserted rather than claimed:
#   1. every long-running service reports healthy (migrate is one-shot and
#      gated by service_completed_successfully instead);
#   2. the committed sample window replayed through MQTT lands end-to-end
#      (the CLI runs inside the ingestion container, sample piped via stdin
#      — no curl/wget/image additions), visible through grafana;
#   3. replaying the SAME file again leaves the telemetry checksum
#      untouched. ingest_events is an arrival log and grows by design, so
#      it serves only as the progress signal for the second pass.
# The sample's Jan-2026 window is disjoint from live simulator data, so the
# windowed queries are unambiguous even on a persisted volume — and because
# replay is idempotent, this whole target is safely rerunnable.
replay-smoke:
	docker compose up -d --build
	for svc in mosquitto timescaledb ingestion simulator grafana; do \
		for i in $$(seq 1 60); do \
			status=$$(docker inspect -f '{{.State.Health.Status}}' \
				$$(docker compose ps -q $$svc)); \
			[ "$$status" = "healthy" ] && break; \
			sleep 2; \
			if [ $$i -eq 60 ]; then \
				echo "$$svc never became healthy (status=$$status)" >&2; \
				docker compose logs $$svc >&2; exit 1; \
			fi; \
		done; \
	done
	docker compose exec -T ingestion uv run --no-sync planter-telemetry replay \
		--host mosquitto --no-delay - < samples/telemetry-window.jsonl
	for i in $$(seq 1 60); do \
		count=$$(docker compose exec -T timescaledb psql -U planter -d planter \
			-tAc "SELECT count(*) FROM telemetry WHERE $(SAMPLE_WINDOW)"); \
		[ "$$count" -eq $(SAMPLE_ROWS) ] && break; \
		sleep 1; \
		if [ $$i -eq 60 ]; then \
			echo "replayed window incomplete: $$count of $(SAMPLE_ROWS) rows" >&2; \
			exit 1; \
		fi; \
	done
	checksum1=$$(docker compose exec -T timescaledb psql -U planter -d planter \
		-tAc "SELECT md5(string_agg(device_id||'|'||measured_at||'|'||water_level||'|'||battery_voltage, ',' ORDER BY device_id, measured_at)) FROM telemetry WHERE $(SAMPLE_WINDOW)"); \
	dedupe1=$$(docker compose exec -T timescaledb psql -U planter -d planter \
		-tAc "SELECT count(*) FROM ingest_events WHERE event = 'deduplicated' AND $(SAMPLE_WINDOW)"); \
	docker compose exec -T ingestion uv run --no-sync planter-telemetry replay \
		--host mosquitto --no-delay - < samples/telemetry-window.jsonl; \
	for i in $$(seq 1 60); do \
		dedupe2=$$(docker compose exec -T timescaledb psql -U planter -d planter \
			-tAc "SELECT count(*) FROM ingest_events WHERE event = 'deduplicated' AND $(SAMPLE_WINDOW)"); \
		[ "$$dedupe2" -ge $$((dedupe1 + $(SAMPLE_VALID_MSGS))) ] && break; \
		sleep 1; \
		if [ $$i -eq 60 ]; then \
			echo "second replay not fully processed ($$dedupe2 dedupe events, want >= $$((dedupe1 + $(SAMPLE_VALID_MSGS))))" >&2; \
			exit 1; \
		fi; \
	done; \
	checksum2=$$(docker compose exec -T timescaledb psql -U planter -d planter \
		-tAc "SELECT md5(string_agg(device_id||'|'||measured_at||'|'||water_level||'|'||battery_voltage, ',' ORDER BY device_id, measured_at)) FROM telemetry WHERE $(SAMPLE_WINDOW)"); \
	[ -n "$$checksum1" ] && [ "$$checksum1" = "$$checksum2" ] || { \
		echo "replaying the same window twice changed telemetry: $$checksum1 != $$checksum2" >&2; \
		exit 1; }
	count=$$(docker compose exec -T grafana sh -ec 'wget -q -O - \
		--header "Content-Type: application/json" \
		--post-data "{\"queries\":[{\"refId\":\"A\",\"datasource\":{\"uid\":\"planter-timescaledb\"},\"format\":\"table\",\"rawSql\":\"SELECT count(*) FROM telemetry WHERE measured_at >= '"'"'2026-01-01'"'"' AND measured_at < '"'"'2026-02-01'"'"'\"}]}" \
		http://127.0.0.1:3000/api/ds/query' \
		| sed -n 's/.*"values":\[\[\([0-9]*\).*/\1/p'); \
	[ -n "$$count" ] && [ "$$count" -eq $(SAMPLE_ROWS) ] || { \
		echo "replayed window not visible through grafana (count=$${count:-unparsed})" >&2; \
		exit 1; }
	docker compose down

# --------------------------------------------------------------- hardware mode
# Opt-in: the demo stack plus an authenticated LAN listener on 1884 for real
# ESP32 pods. The default demo path never touches these targets or files.
# See docs/hardware-bridge.md.
HW_COMPOSE = docker compose -f docker-compose.yml -f docker-compose.hardware.yml

# Preflight the password file so mosquitto fails helpfully, not mysteriously
# (the broker exits at boot when password_file is missing).
hardware-up:
	@test -f mosquitto/auth/passwd || { \
		echo "mosquitto/auth/passwd missing — create your first device user:" >&2; \
		echo "  make hardware-passwd DEVICE=<device_id>" >&2; \
		exit 1; }
	$(HW_COMPOSE) up -d --build

hardware-down:
	$(HW_COMPOSE) down

# Create or update one device credential (interactive password prompt), then
# hot-reload the broker if it is running (SIGHUP re-reads password_file and
# acl_file). Runs mosquitto_passwd inside the broker image so the file's
# ownership and permissions come out broker-readable on every platform.
# The MQTT username must equal the device_id (see docs/message-contract.md).
hardware-passwd:
	@test -n "$(DEVICE)" || { echo "usage: make hardware-passwd DEVICE=<device_id>" >&2; exit 1; }
	@echo "$(DEVICE)" | grep -Eq '^[a-z0-9][a-z0-9_-]{0,31}$$' || { \
		echo "DEVICE must match ^[a-z0-9][a-z0-9_-]{0,31}$$ (username == device_id)" >&2; \
		exit 1; }
	docker run --rm -it -v "$(CURDIR)/mosquitto/auth":/mosquitto/auth eclipse-mosquitto:2 \
		sh -ec 'touch /mosquitto/auth/passwd && mosquitto_passwd /mosquitto/auth/passwd $(DEVICE)'
	@$(HW_COMPOSE) kill -s SIGHUP mosquitto 2>/dev/null \
		&& echo "broker reloaded (SIGHUP)" \
		|| echo "broker not running — credentials take effect on next hardware-up"

# Validates hardware mode without running it through the demo compose project:
#   1. both compose file combinations parse, and the merge does what the
#      override relies on (hardware conf replaces the demo conf, 1884 is
#      published, the loopback-only 1883 binding survives);
#   2. the hardware mosquitto config actually boots a broker — mosquitto has
#      no config-lint flag, so a bare `docker run` (no compose project, no
#      name/network collisions) with a THROWAWAY passwd file proves it, plus:
#      anonymous is refused on 1884, an authenticated QoS-1 publish on 1884
#      reaches a subscriber on 1883 (cross-listener routing + passwd parsing
#      + ACL grant), and a publish to another device's topic never arrives
#      (MQTT 3.1.1 drops ACL-denied publishes silently, so absence is the
#      only observable; mosquitto_sub -W exits nonzero on timeout, hence
#      the `|| true`). Never touches mosquitto/auth/passwd.
hardware-config-check:
	docker compose config -q
	$(HW_COMPOSE) config -q
	$(HW_COMPOSE) config | grep -q 'mosquitto.hardware.conf'
	$(HW_COMPOSE) config | grep -q 'published: "1884"'
	$(HW_COMPOSE) config | grep -q 'host_ip: 127.0.0.1'
	set -e; \
	tmp=$$(mktemp -d /tmp/planter-hwcheck.XXXXXX); \
	cid=""; \
	trap 'docker rm -f $$cid >/dev/null 2>&1 || true; rm -rf "$$tmp"' EXIT; \
	docker run --rm -v "$$tmp":/auth eclipse-mosquitto:2 \
		sh -ec 'mosquitto_passwd -c -b /auth/passwd ci-device ci-password && chmod 644 /auth/passwd'; \
	cid=$$(docker run -d \
		-v "$(CURDIR)/mosquitto/mosquitto.hardware.conf":/mosquitto/config/mosquitto.conf:ro \
		-v "$(CURDIR)/mosquitto/auth/acl.conf":/mosquitto/auth/acl.conf:ro \
		-v "$$tmp/passwd":/mosquitto/auth/passwd:ro \
		eclipse-mosquitto:2); \
	for i in $$(seq 1 30); do \
		docker exec $$cid mosquitto_pub -p 1883 -t planter/ready -m ping 2>/dev/null && break; \
		sleep 1; \
		if [ $$i -eq 30 ]; then \
			echo "hardware broker never came up" >&2; \
			docker logs $$cid >&2; exit 1; \
		fi; \
	done; \
	if docker exec $$cid mosquitto_pub -p 1884 -t planter/ready -m ping 2>/dev/null; then \
		echo "anonymous publish ACCEPTED on 1884 — auth is broken" >&2; exit 1; \
	fi; \
	docker exec $$cid sh -ec ' \
		mosquitto_sub -p 1883 -t planter/v1/ci-device/telemetry -C 1 -W 10 > /tmp/got & \
		sleep 1; \
		mosquitto_pub -p 1884 -u ci-device -P ci-password -q 1 \
			-t planter/v1/ci-device/telemetry -m hw-ok; \
		wait $$!; \
		grep -qx hw-ok /tmp/got'; \
	docker exec $$cid sh -ec ' \
		mosquitto_sub -p 1883 -t planter/v1/other-device/telemetry -C 1 -W 5 > /tmp/leak & \
		sleep 1; \
		mosquitto_pub -p 1884 -u ci-device -P ci-password -q 1 \
			-t planter/v1/other-device/telemetry -m leaked || true; \
		wait $$! || true; \
		test ! -s /tmp/leak'
