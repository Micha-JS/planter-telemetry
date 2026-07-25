# Shared image for the repo's Python services (simulator now, ingestion in M2 —
# same image, different command). uv version matches the CI pin.
FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.11.14 /uv /bin/uv

WORKDIR /app

# Dependency layer first so code changes don't re-resolve the environment.
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project

# hatchling needs README.md (project.readme) and src (version regex) to build.
COPY README.md ./
COPY src ./src
RUN uv sync --locked --no-dev --no-editable

CMD ["uv", "run", "--no-sync", "python", "-m", "planter_telemetry.simulator"]
