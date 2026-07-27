"""Unit: structural lint of the provisioned Grafana configuration.

Dashboards are config-as-code; this is their `alembic check`. Everything a
reviewer would otherwise verify by clicking through a running Grafana is
asserted here instead: datasource references go by provisioned uid (never by
name, never as unresolved ${DS_*} import variables), the demo ergonomics
(time range, refresh) stay tuned to the simulator's accelerated clock, and
every panel query is documented in docs/dashboard-queries.md so SQL review
never requires digging through dashboard JSON.
"""

import json
import re
from pathlib import Path
from typing import Any

import yaml

_ROOT = Path(__file__).parent.parent
_DASHBOARD = _ROOT / "grafana" / "dashboards" / "planter-fleet.json"
_DATASOURCE_YML = _ROOT / "grafana" / "provisioning" / "datasources" / "timescaledb.yml"
_PROVIDER_YML = _ROOT / "grafana" / "provisioning" / "dashboards" / "planter.yml"
_QUERY_DOC = _ROOT / "docs" / "dashboard-queries.md"
_COMPOSE = _ROOT / "docker-compose.yml"

_DATASOURCE_UID = "planter-timescaledb"
_DATASOURCE_REF = {"type": "postgres", "uid": _DATASOURCE_UID}


def _dashboard() -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads(_DASHBOARD.read_text())
    return loaded


def _panels() -> list[dict[str, Any]]:
    """All panels, flattened through (collapsed) row panels."""
    flat: list[dict[str, Any]] = []
    for panel in _dashboard()["panels"]:
        flat.append(panel)
        flat.extend(panel.get("panels", []))
    return flat


def _normalize(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip()


def test_dashboard_identity_and_demo_ergonomics() -> None:
    dashboard = _dashboard()
    assert dashboard["uid"] == "planter-fleet"
    assert dashboard["title"] == "Planter Fleet"
    assert isinstance(dashboard["schemaVersion"], int)
    # The simulator's clock runs 180x ahead of the wall clock from wall-now:
    # the range must reach into the future or the dashboard shows nothing,
    # and 5s refresh = 15 virtual minutes = 3 new points per device.
    assert dashboard["time"] == {"from": "now-1h", "to": "now+2d"}
    assert dashboard["refresh"] == "5s"
    # Device timestamps must not be shifted by the viewer's browser locale.
    assert dashboard["timezone"] == "utc"


def test_panels_have_unique_ids_and_grid_positions() -> None:
    panels = _panels()
    ids = [panel["id"] for panel in panels]
    assert len(ids) == len(set(ids))
    for panel in panels:
        assert {"h", "w", "x", "y"} <= panel["gridPos"].keys(), panel["title"]


def test_every_datasource_reference_is_the_provisioned_uid() -> None:
    """By uid, never by name — provisioning guarantees the uid; display names
    are free to change. And no ${DS_*} template inputs: those come from
    Grafana's share-export format and would prompt on import."""
    assert "${DS_" not in _DASHBOARD.read_text()
    for panel in _panels():
        if panel["type"] == "row":
            continue
        assert panel["datasource"] == _DATASOURCE_REF, panel["title"]
        for target in panel["targets"]:
            assert target["datasource"] == _DATASOURCE_REF, panel["title"]


def test_every_panel_query_is_documented() -> None:
    """Each rawSql must appear (whitespace-normalized) in the query doc, so
    panel SQL is reviewable without reading dashboard JSON."""
    documented = _normalize(_QUERY_DOC.read_text())
    for panel in _panels():
        for target in panel.get("targets", []):
            assert _normalize(target["rawSql"]) in documented, panel["title"]


def test_datasource_provisioning() -> None:
    config = yaml.safe_load(_DATASOURCE_YML.read_text())
    assert config["apiVersion"] == 1
    (datasource,) = config["datasources"]
    assert datasource["uid"] == _DATASOURCE_UID
    assert datasource["type"] == "postgres"
    assert datasource["editable"] is False
    # Credentials come from the container environment (compose sets them),
    # never as literals in the provisioning file.
    assert datasource["user"] == "$GRAFANA_DB_USER"
    assert datasource["secureJsonData"]["password"] == "$GRAFANA_DB_PASSWORD"
    assert datasource["jsonData"]["timescaledb"] is True


def test_dashboard_provider_matches_compose_mount() -> None:
    provider_config = yaml.safe_load(_PROVIDER_YML.read_text())
    assert provider_config["apiVersion"] == 1
    (provider,) = provider_config["providers"]
    # The repo JSON is the only source of truth.
    assert provider["allowUiUpdates"] is False
    assert provider["disableDeletion"] is True

    compose = yaml.safe_load(_COMPOSE.read_text())
    grafana = compose["services"]["grafana"]
    mounts = dict(volume.split(":", 1) for volume in grafana["volumes"])
    assert mounts["./grafana/dashboards"].startswith(provider["options"]["path"])
    assert _DASHBOARD.parent == _ROOT / "grafana" / "dashboards"
