"""AGV middleware configuration for the five-node dynamic-rerouting scenario.

This module deliberately contains configuration and pure validation helpers only.
MQTT, Firebase, routing decisions, mission state transitions, and timers live in
``main.py``.

Scenario
--------
Nodes: PURPLE, ORANGE, BLUE, GREEN, RED

Base graph (all bidirectional):
    PURPLE --1-- ORANGE --1-- BLUE --1-- GREEN --1-- RED
                              \\----------5----------/

Mission defaults (documentation/test fallback only):
    AGV1: PURPLE -> GREEN
    AGV2: PURPLE -> RED, after AGV1 enters the BLUE -> GREEN segment

At runtime each AGV must report its own goal in the status payload.  The
configured targets below remain only as compatibility/test defaults and are not
used for automatic dispatch until the AGV confirms a goal.

The expensive BLUE-RED edge represents the long outer bypass.  Under normal
conditions Dijkstra chooses BLUE-GREEN-RED.  Once AGV1 reports that it is moving
from BLUE toward GREEN, Windows latches AGV2's release condition and treats
BLUE-GREEN/GREEN as reserved, so AGV2 receives the BLUE-RED bypass.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Final, Iterable, List, Mapping, Optional, Sequence, Tuple

try:
    from dotenv import load_dotenv
except ImportError:  # Keep --check usable before optional dependencies are installed.
    def load_dotenv(*args: Any, **kwargs: Any) -> bool:  # type: ignore[no-redef]
        return False


# ---------------------------------------------------------------------------
# Environment helpers
# ---------------------------------------------------------------------------

KST: Final = timezone(timedelta(hours=9))


def _find_project_root(start: Path) -> Path:
    """Find the nearest parent containing .env; otherwise use ``start``."""
    for path in (start, *start.parents):
        if (path / ".env").exists():
            return path
    return start


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off", "n"}


def _env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)).strip())


def _env_float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)).strip())


PROJECT_ROOT: Final[Path] = _find_project_root(Path(__file__).resolve().parent)
ENV_PATH: Final[Path] = PROJECT_ROOT / ".env"
load_dotenv(ENV_PATH)

FIREBASE_CREDENTIAL_PATH: Final[str] = os.getenv(
    "FIREBASE_CREDENTIAL_PATH", "configs/serviceAccountKey.json"
).strip()
FIREBASE_DATABASE_URL: Final[Optional[str]] = (
    os.getenv("FIREBASE_DATABASE_URL", "").strip() or None
)
FIREBASE_WRITE_ENABLED: Final[bool] = _env_bool("FIREBASE_WRITE_ENABLED", False)
LOAD_MAP_FROM_FIREBASE: Final[bool] = _env_bool("LOAD_MAP_FROM_FIREBASE", True)

# Keep compatibility with the AGV notebook's temporary broker override, but do
# not duplicate this selection logic in main.py.
MQTT_BROKER_HOST: Final[str] = (
    os.getenv("MQTT_BROKER_HOST_TEMP", "").strip()
    or os.getenv("MQTT_BROKER_HOST", "10.32.240.196").strip()
)
MQTT_BROKER_PORT: Final[int] = _env_int("MQTT_BROKER_PORT", 1883)
MQTT_KEEPALIVE_SEC: Final[int] = _env_int("MQTT_KEEPALIVE_SEC", 30)
CLIENT_ID_PREFIX: Final[str] = os.getenv("MQTT_CLIENT_ID_PREFIX", "agv_middleware").strip()


# ---------------------------------------------------------------------------
# MQTT topics
# ---------------------------------------------------------------------------

COMMAND_TOPIC: Final[str] = "agv/{robot_id}/command"
ROUTE_TOPIC: Final[str] = "agv/{robot_id}/route"

STATUS_SUB_TOPIC: Final[str] = "agv/+/status"
SENSING_SUB_TOPIC: Final[str] = "agv/+/sensing"
EVENT_SUB_TOPIC: Final[str] = "agv/+/event"
ROUTE_ACK_SUB_TOPIC: Final[str] = "agv/+/route_ack"
REQUEST_MAP_TOPIC: Final[str] = "agv/system/request_map"
ROBOT_PRESENCE_REQUEST_TOPIC: Final[str] = "agv/system/robot_presence/request"
ROBOT_PRESENCE_ACK_TOPIC: Final[str] = "agv/system/robot_presence/ack"

MAP_PUB_TOPIC: Final[str] = "agv/system/map"
REROUTE_LOG_TOPIC: Final[str] = "agv/system/reroute_log"

DEFAULT_SUBSCRIBE_TOPICS: Final[Tuple[str, ...]] = (
    STATUS_SUB_TOPIC,
    SENSING_SUB_TOPIC,
    EVENT_SUB_TOPIC,
    ROUTE_ACK_SUB_TOPIC,
    REQUEST_MAP_TOPIC,
    ROBOT_PRESENCE_REQUEST_TOPIC,
)

MQTT_STATUS_QOS: Final[int] = _env_int("MQTT_STATUS_QOS", 0)
MQTT_CONTROL_QOS: Final[int] = _env_int("MQTT_CONTROL_QOS", 0)
MQTT_ROUTE_QOS: Final[int] = _env_int("MQTT_ROUTE_QOS", 0)
MQTT_OPERATOR_QOS: Final[int] = _env_int("MQTT_OPERATOR_QOS", 1)
MAP_SNAPSHOT_RETAIN: Final[bool] = _env_bool("MAP_SNAPSHOT_RETAIN", True)
PUBLISH_MAP_ON_CONNECT: Final[bool] = _env_bool("PUBLISH_MAP_ON_CONNECT", True)


# ---------------------------------------------------------------------------
# Firebase paths
# ---------------------------------------------------------------------------

TABLE_SENSING: Final[str] = "sensingTable"
TABLE_SENSING_RAW: Final[str] = "sensingRawTable"
TABLE_ROBOTS: Final[str] = "robots"
TABLE_COMMAND: Final[str] = "commandTable"
TABLE_EVENT: Final[str] = "eventTable"
TABLE_INVALID_EVENT: Final[str] = "invalidEventTable"
TABLE_MAP: Final[str] = "mapTable"
TABLE_ROUTE: Final[str] = "routeTable"
TABLE_ROUTE_ACK: Final[str] = "routeAckTable"
TABLE_REROUTE_LOG: Final[str] = "rerouteLogTable"
TABLE_FLEET_STATE: Final[str] = "fleetStateTable"
TABLE_TARGET_RESERVATION: Final[str] = "targetReservationTable"
TABLE_OPERATOR_ACTION: Final[str] = "operatorActionTable"

MAP_FIREBASE_WRITE_MODE: Final[str] = os.getenv(
    "MAP_FIREBASE_WRITE_MODE", "replace"
).strip().lower()


# ---------------------------------------------------------------------------
# Map and mission data
# ---------------------------------------------------------------------------

STAGE_ID: Final[str] = "five_node_dynamic_reroute_v1"
LAYOUT_ID: Final[str] = os.getenv("LAYOUT_ID", STAGE_ID).strip()
MAP_VERSION: Final[int] = _env_int("MAP_VERSION", 3)

AGV1_ID: Final[str] = "AGV1"
AGV2_ID: Final[str] = "AGV2"

VALID_NODES: Final[frozenset[str]] = frozenset(
    {"PURPLE", "ORANGE", "BLUE", "GREEN", "RED"}
)
INVALID_NODE_NAMES: Final[frozenset[str]] = frozenset(
    {"", "-", "UNKNOWN", "NONE", "NULL", "DEST", "DESTINATION", "NAN"}
)

# The IDs are stable Firebase/MQTT map identifiers.  The runtime may change only
# mutable fields such as status, blocked_by, and updated_at.
DEFAULT_MAP_EDGES: Final[Dict[str, Dict[str, Any]]] = {
    "PURPLE-ORANGE": {
        "from": "PURPLE",
        "to": "ORANGE",
        "status": "open",
        "cost": 1.0,
        "bidirectional": True,
        "kind": "main",
    },
    "ORANGE-BLUE": {
        "from": "ORANGE",
        "to": "BLUE",
        "status": "open",
        "cost": 1.0,
        "bidirectional": True,
        "kind": "main",
    },
    "BLUE-GREEN": {
        "from": "BLUE",
        "to": "GREEN",
        "status": "open",
        "cost": 1.0,
        "bidirectional": True,
        "kind": "main",
    },
    "GREEN-RED": {
        "from": "GREEN",
        "to": "RED",
        "status": "open",
        "cost": 1.0,
        "bidirectional": True,
        "kind": "main",
    },
    "BLUE-RED": {
        "from": "BLUE",
        "to": "RED",
        "status": "open",
        "cost": 5.0,
        "bidirectional": True,
        "kind": "outer_bypass",
        "note": "Long physical bypass without intermediate color nodes",
    },
}

# Mission dependencies are data, not hard-coded control branches.  Supported
# conditions in main.py are: arrived, at_node, phase, and entered_edge.
# ``target`` is a documentation/test fallback.  Runtime dispatch is gated until
# a valid goal is received from the AGV status payload.
#
# AGV2's value is a minimum automatic-start delay measured from AGV1's first
# fresh moving/progress status. Existing dependency, route ACK, occupancy, and
# obstacle gates remain authoritative, so the actual start may occur later.
AGV2_START_DELAY_SEC: Final[float] = _env_float("AGV2_START_DELAY_SEC", 7.0)

ROBOT_MISSIONS: Final[Dict[str, Dict[str, Any]]] = {
    AGV1_ID: {
        "start": "PURPLE",
        "target": "GREEN",
        "auto_start": True,
        "initial_start_delay_sec": 0.0,
        "start_delay_after_robot": None,
        "dependencies": [],
    },
    AGV2_ID: {
        "start": "PURPLE",
        "target": "RED",
        "auto_start": True,
        "initial_start_delay_sec": AGV2_START_DELAY_SEC,
        "start_delay_after_robot": AGV1_ID,
        "dependencies": [
            {
                "robot_id": AGV1_ID,
                "condition": "entered_edge",
                "from": "BLUE",
                "to": "GREEN",
                "latch": True,
                "reserve_transit": True,
                # Scenario 1 only.  The owner mission is AGV2 and the
                # dependency robot is AGV1.  When both AGVs report RED, this
                # dependency becomes inactive, so Scenario 2 is driven by the
                # actual GREEN-RED obstacle rather than a pre-existing hold.
                "when_mission_goal": "RED",
                "when_robot_goal": "GREEN",
            },
        ],
    },
}

ROBOT_IDS: Final[Tuple[str, ...]] = tuple(ROBOT_MISSIONS.keys())
DEFAULT_START_NODES: Final[Dict[str, str]] = {
    robot_id: str(spec["start"]) for robot_id, spec in ROBOT_MISSIONS.items()
}
DEFAULT_TARGET_NODES: Final[Dict[str, str]] = {
    robot_id: str(spec["target"]) for robot_id, spec in ROBOT_MISSIONS.items()
}

# Reference routes are documentation/test expectations only.  Runtime routing
# always uses the graph and current fleet state.
REFERENCE_ROUTES: Final[Dict[str, List[str]]] = {
    AGV1_ID: ["PURPLE", "ORANGE", "BLUE", "GREEN"],
    AGV2_ID: ["PURPLE", "ORANGE", "BLUE", "GREEN", "RED"],
}
EXPECTED_BYPASS_ROUTE: Final[List[str]] = ["PURPLE", "ORANGE", "BLUE", "RED"]
DEFAULT_ROUTES: Final[Dict[str, List[str]]] = REFERENCE_ROUTES  # compatibility alias
DECISION_NODES: Final[frozenset[str]] = frozenset({"BLUE"})


# ---------------------------------------------------------------------------
# Robot identity compatibility
# ---------------------------------------------------------------------------

LOGICAL_TO_WIRE_ID: Final[Dict[str, str]] = {
    AGV1_ID: os.getenv("AGV1_MQTT_TOPIC_ID", "AGV_01").strip(),
    AGV2_ID: os.getenv("AGV2_MQTT_TOPIC_ID", "AGV_02").strip(),
}

ROBOT_ID_ALIASES: Final[Dict[str, str]] = {
    "AGV1": AGV1_ID,
    "AGV01": AGV1_ID,
    "AGV_01": AGV1_ID,
    "1": AGV1_ID,
    "AGV2": AGV2_ID,
    "AGV02": AGV2_ID,
    "AGV_02": AGV2_ID,
    "2": AGV2_ID,
}
for _logical_id, _wire_id in LOGICAL_TO_WIRE_ID.items():
    ROBOT_ID_ALIASES[_wire_id.strip().upper()] = _logical_id


# ---------------------------------------------------------------------------
# Control and safety policy
# ---------------------------------------------------------------------------

AUTO_START_ENABLED: Final[bool] = _env_bool("AUTO_START_ENABLED", True)
REQUIRE_ALL_ROBOT_STATUS_BEFORE_AUTOSTART: Final[bool] = _env_bool(
    "REQUIRE_ALL_ROBOT_STATUS_BEFORE_AUTOSTART", True
)

# The AGV is the source of truth for the active goal.  The middleware accepts
# the first valid field below and keeps the last valid goal when a later status
# omits it.
REQUIRE_GOAL_IN_STATUS: Final[bool] = _env_bool("REQUIRE_GOAL_IN_STATUS", True)

# When the AGV must move before it can detect the first PURPLE marker and read
# its real mission goal, the middleware may issue a short provisional route.
# This breaks the startup cycle without pretending that the provisional target
# is the real mission destination. The AGV reports the real goal when PURPLE is
# detected; the middleware then performs the normal line_stop -> route -> ACK ->
# line_start handshake for the real mission.
BOOTSTRAP_ROUTE_BEFORE_GOAL: Final[bool] = _env_bool(
    "BOOTSTRAP_ROUTE_BEFORE_GOAL", True
)
BOOTSTRAP_START_NODE: Final[str] = os.getenv(
    "BOOTSTRAP_START_NODE", "PURPLE"
).strip().upper()
BOOTSTRAP_TARGET_NODE: Final[str] = os.getenv(
    "BOOTSTRAP_TARGET_NODE", "ORANGE"
).strip().upper()
BOOTSTRAP_ROUTE: Final[List[str]] = [
    item.strip().upper()
    for item in os.getenv(
        "BOOTSTRAP_ROUTE",
        "PURPLE,ORANGE,BLUE,RED,GREEN",
    ).split(",")
    if item.strip()
]
BOOTSTRAP_GOAL_CONFIRM_NODE: Final[str] = os.getenv(
    "BOOTSTRAP_GOAL_CONFIRM_NODE", "PURPLE"
).strip().upper()
BOOTSTRAP_REQUIRE_STOPPED_CONFIRMATION: Final[bool] = _env_bool(
    "BOOTSTRAP_REQUIRE_STOPPED_CONFIRMATION", False
)

GOAL_STATUS_FIELDS: Final[Tuple[str, ...]] = (
    "goal_node",
    "goal",
    "target_node",
    "target",
    "destination_node",
    "destination",
)

# These are the only autonomous stop/start commands emitted by the middleware.
LINE_STOP_COMMAND: Final[str] = os.getenv("LINE_STOP_COMMAND", "line_stop").strip().lower()
LINE_START_COMMAND: Final[str] = os.getenv("LINE_START_COMMAND", "line_start").strip().lower()
AGV2_START_COMMAND_DELAY_SEC: Final[float] = _env_float("AGV2_START_COMMAND_DELAY_SEC", 10.0)
DEFAULT_CONTROL_SPEED: Final[float] = _env_float("DEFAULT_CONTROL_SPEED", 0.30)

REQUIRE_FRESH_STOP_CONFIRMATION: Final[bool] = _env_bool(
    "REQUIRE_FRESH_STOP_CONFIRMATION", True
)
STOP_CONFIRM_TIMEOUT_SEC: Final[float] = _env_float("STOP_CONFIRM_TIMEOUT_SEC", 3.0)
ROUTE_ACK_TIMEOUT_SEC: Final[float] = _env_float("ROUTE_ACK_TIMEOUT_SEC", 3.0)
START_CONFIRM_TIMEOUT_SEC: Final[float] = _env_float("START_CONFIRM_TIMEOUT_SEC", 3.0)
MAX_CONTROL_RETRIES: Final[int] = _env_int("MAX_CONTROL_RETRIES", 2)
WATCHDOG_ENABLED: Final[bool] = _env_bool("WATCHDOG_ENABLED", True)
WATCHDOG_INTERVAL_SEC: Final[float] = _env_float("WATCHDOG_INTERVAL_SEC", 0.25)
STATUS_STALE_SEC: Final[float] = _env_float("STATUS_STALE_SEC", 5.0)
# Deprecated compatibility value. Stale/arrived occupancy is no longer auto-cleared
# merely because this duration elapsed; a fresh status at another node releases it.
OCCUPANCY_HOLD_SEC: Final[float] = _env_float("OCCUPANCY_HOLD_SEC", 30.0)
WAITING_PATH_RETRY_SEC: Final[float] = _env_float("WAITING_PATH_RETRY_SEC", 1.0)
CAUTION_COST_MULTIPLIER: Final[float] = _env_float("CAUTION_COST_MULTIPLIER", 3.0)
REPLAN_MOVING_ON_OCCUPANCY_CHANGE: Final[bool] = _env_bool(
    "REPLAN_MOVING_ON_OCCUPANCY_CHANGE", True
)
REPLAN_ON_EDGE_OPEN: Final[bool] = _env_bool("REPLAN_ON_EDGE_OPEN", False)
ALLOW_SAFE_PREFIX_ROUTE: Final[bool] = _env_bool("ALLOW_SAFE_PREFIX_ROUTE", True)
PREFER_SAFE_PREFIX_OVER_ALTERNATE: Final[bool] = _env_bool(
    "PREFER_SAFE_PREFIX_OVER_ALTERNATE", False
)

# Obstacle events identify the physical segment as previous_node <->
# current_node.  An explicit edge remains a compatibility fallback; the old
# current_node <-> next_node interpretation can be disabled once every AGV has
# migrated.
OBSTACLE_EDGE_MODE: Final[str] = os.getenv(
    "OBSTACLE_EDGE_MODE", "previous_current"
).strip().lower()
ALLOW_LEGACY_CURRENT_NEXT_EDGE_FALLBACK: Final[bool] = _env_bool(
    "ALLOW_LEGACY_CURRENT_NEXT_EDGE_FALLBACK", True
)
OBSTACLE_CLEAR_STABLE_SEC: Final[float] = _env_float(
    "OBSTACLE_CLEAR_STABLE_SEC", 1.0
)

# ``shared`` records multiple destination claims for the demo but does not use
# them as an exclusion constraint.  Actual node occupancy is still exclusive.
# Switch to ``exclusive`` after a reliable release/queue policy is connected.
TARGET_RESERVATION_MODE: Final[str] = os.getenv(
    "TARGET_RESERVATION_MODE", "shared"
).strip().lower()

ROUTE_ACK_ACCEPTED_STATUSES: Final[frozenset[str]] = frozenset(
    {"accepted", "ok", "success", "applied"}
)
ROUTE_ACK_REJECTED_STATUSES: Final[frozenset[str]] = frozenset(
    {"rejected", "failed", "error", "timeout", "nack", "invalid_route"}
)
ARRIVED_STATUS_VALUES: Final[frozenset[str]] = frozenset(
    {"arrived", "done", "finished", "mission_complete", "completed"}
)
STOPPED_STATUS_VALUES: Final[frozenset[str]] = frozenset(
    {"idle", "stopped", "stop", "paused", "waiting", "arrived", "done", "finished"}
)
MOVING_STATUS_VALUES: Final[frozenset[str]] = frozenset(
    {"moving", "running", "line_tracing", "line_tracking", "driving"}
)
# ``DEST`` is a route-cursor marker in the current AGV status payload, not a
# standalone proof of physical arrival.
TERMINAL_NEXT_NODE_VALUES: Final[frozenset[str]] = frozenset(
    {"", "-", "DEST", "DESTINATION", "NONE", "NULL"}
)
# Restricted fallback for fake/legacy AGVs that do not emit status=arrived.
# Broad pause/wait states are intentionally excluded.
ARRIVAL_FALLBACK_STOPPED_STATUS_VALUES: Final[frozenset[str]] = frozenset(
    {"idle", "stopped", "stop"}
)

EDGE_BLOCK_EVENT_TYPES: Final[frozenset[str]] = frozenset(
    {"obstacle_detected", "edge_blocked", "blocked_edge", "local_stop_obstacle"}
)
EDGE_OPEN_EVENT_TYPES: Final[frozenset[str]] = frozenset(
    {"obstacle_cleared", "edge_cleared", "edge_opened", "clear_obstacle"}
)


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def now_iso() -> str:
    return datetime.now(KST).isoformat(timespec="milliseconds")


def new_client_id(prefix: str = CLIENT_ID_PREFIX) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def normalize_robot_id(value: Any) -> str:
    if value is None:
        return "UNKNOWN"
    raw = str(value).strip().upper()
    if not raw:
        return "UNKNOWN"
    return ROBOT_ID_ALIASES.get(raw, raw)


def wire_robot_id(value: Any) -> str:
    logical_id = normalize_robot_id(value)
    return LOGICAL_TO_WIRE_ID.get(logical_id, str(value).strip() or logical_id)


def command_topic(robot_id: Any) -> str:
    return COMMAND_TOPIC.format(robot_id=wire_robot_id(robot_id))


def route_topic(robot_id: Any) -> str:
    return ROUTE_TOPIC.format(robot_id=wire_robot_id(robot_id))


def robot_id_from_topic(topic: str) -> str:
    parts = str(topic).split("/")
    return parts[1] if len(parts) >= 3 else "UNKNOWN"


def normalize_node(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()


def is_valid_node(value: Any) -> bool:
    node = normalize_node(value)
    return node in VALID_NODES and node not in INVALID_NODE_NAMES


def extract_goal_node(payload: Mapping[str, Any]) -> str:
    """Return the first valid AGV-reported goal from the supported aliases."""
    for field_name in GOAL_STATUS_FIELDS:
        node = normalize_node(payload.get(field_name))
        if is_valid_node(node):
            return node
    return ""


def normalize_route(value: Any) -> List[str]:
    """Normalize a route from a sequence or ``A->B->C``/``A,B,C`` string."""
    raw_items: Iterable[Any]
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip().upper().replace(" ", "")
        if "->" in text:
            raw_items = text.split("->")
        elif "," in text:
            raw_items = text.split(",")
        else:
            raw_items = text.split("-")
    elif isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        raw_items = value
    else:
        return []

    result: List[str] = []
    for item in raw_items:
        node = normalize_node(item)
        if is_valid_node(node):
            result.append(node)
    return result


def parse_edge_key(raw_edge: Any) -> Optional[Tuple[str, str]]:
    """Parse an edge from ``A-B``, ``A->B`` or ``{'from': A, 'to': B}``."""
    if isinstance(raw_edge, Mapping):
        a = normalize_node(raw_edge.get("from"))
        b = normalize_node(raw_edge.get("to"))
    else:
        text = str(raw_edge or "").strip().upper().replace(" ", "")
        separator = "->" if "->" in text else "-"
        if separator not in text:
            return None
        a, b = (normalize_node(item) for item in text.split(separator, 1))
    if not is_valid_node(a) or not is_valid_node(b) or a == b:
        return None
    return a, b


def canonical_edge_id(
    from_node: Any,
    to_node: Any,
    edges: Optional[Mapping[str, Mapping[str, Any]]] = None,
) -> Optional[str]:
    """Return the configured edge ID, respecting directed/bidirectional edges."""
    edge_map = edges if edges is not None else DEFAULT_MAP_EDGES
    a = normalize_node(from_node)
    b = normalize_node(to_node)
    if not is_valid_node(a) or not is_valid_node(b) or a == b:
        return None

    for edge_id, edge in edge_map.items():
        src = normalize_node(edge.get("from"))
        dst = normalize_node(edge.get("to"))
        if src == a and dst == b:
            return str(edge_id)
        if bool(edge.get("bidirectional", False)) and src == b and dst == a:
            return str(edge_id)
    return None


def route_edges(
    route: Sequence[Any],
    edges: Optional[Mapping[str, Mapping[str, Any]]] = None,
) -> List[str]:
    normalized = normalize_route(route)
    result: List[str] = []
    for a, b in zip(normalized, normalized[1:]):
        edge_id = canonical_edge_id(a, b, edges)
        if edge_id is None:
            raise ValueError(f"Unknown route segment: {a}-{b}")
        result.append(edge_id)
    return result


def route_cost(
    route: Sequence[Any],
    edges: Optional[Mapping[str, Mapping[str, Any]]] = None,
) -> float:
    edge_map = edges if edges is not None else DEFAULT_MAP_EDGES
    return sum(float(edge_map[edge_id].get("cost", 1.0)) for edge_id in route_edges(route, edge_map))


def get_default_map_edges() -> Dict[str, Dict[str, Any]]:
    return {edge_id: dict(edge) for edge_id, edge in DEFAULT_MAP_EDGES.items()}


def get_mission(robot_id: Any) -> Dict[str, Any]:
    return dict(ROBOT_MISSIONS.get(normalize_robot_id(robot_id), {}))


def get_reference_route(robot_id: Any) -> List[str]:
    return list(REFERENCE_ROUTES.get(normalize_robot_id(robot_id), []))


def get_default_route(robot_id: Any) -> List[str]:
    """Compatibility helper; runtime must still calculate its own route."""
    return get_reference_route(robot_id)


def missing_required_env() -> List[str]:
    missing: List[str] = []
    if not MQTT_BROKER_HOST:
        missing.append("MQTT_BROKER_HOST")
    if not FIREBASE_DATABASE_URL:
        missing.append("FIREBASE_DATABASE_URL")
    if not FIREBASE_CREDENTIAL_PATH:
        missing.append("FIREBASE_CREDENTIAL_PATH")
    return missing


def _validate_dependency_cycles(errors: List[str]) -> None:
    graph: Dict[str, List[str]] = {}
    for robot_id, mission in ROBOT_MISSIONS.items():
        graph[robot_id] = [
            normalize_robot_id(item.get("robot_id"))
            for item in mission.get("dependencies", [])
            if isinstance(item, Mapping)
        ]

    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(node: str) -> None:
        if node in visiting:
            errors.append(f"Mission dependency cycle detected at {node}")
            return
        if node in visited:
            return
        visiting.add(node)
        for child in graph.get(node, []):
            visit(child)
        visiting.remove(node)
        visited.add(node)

    for robot_id in ROBOT_IDS:
        visit(robot_id)


def validate_settings() -> None:
    """Validate static protocol, map, and mission consistency."""
    errors: List[str] = []

    if LINE_STOP_COMMAND != "line_stop":
        errors.append("LINE_STOP_COMMAND must remain 'line_stop' for the current AGV protocol")
    if LINE_START_COMMAND != "line_start":
        errors.append("LINE_START_COMMAND must remain 'line_start'; resume is not supported")
    if MAP_FIREBASE_WRITE_MODE not in {"replace", "merge"}:
        errors.append("MAP_FIREBASE_WRITE_MODE must be 'replace' or 'merge'")
    if OBSTACLE_EDGE_MODE not in {"previous_current"}:
        errors.append("OBSTACLE_EDGE_MODE must be 'previous_current'")
    if TARGET_RESERVATION_MODE not in {"disabled", "shared", "exclusive"}:
        errors.append(
            "TARGET_RESERVATION_MODE must be 'disabled', 'shared', or 'exclusive'"
        )
    if not GOAL_STATUS_FIELDS:
        errors.append("GOAL_STATUS_FIELDS must contain at least one field")
    if BOOTSTRAP_ROUTE_BEFORE_GOAL:
        for name, node in (
            ("BOOTSTRAP_START_NODE", BOOTSTRAP_START_NODE),
            ("BOOTSTRAP_TARGET_NODE", BOOTSTRAP_TARGET_NODE),
            ("BOOTSTRAP_GOAL_CONFIRM_NODE", BOOTSTRAP_GOAL_CONFIRM_NODE),
        ):
            if not is_valid_node(node):
                errors.append(f"{name} must be a configured valid node: {node!r}")
        if BOOTSTRAP_START_NODE == BOOTSTRAP_TARGET_NODE:
            errors.append("BOOTSTRAP_START_NODE and BOOTSTRAP_TARGET_NODE must differ")
        if canonical_edge_id(BOOTSTRAP_START_NODE, BOOTSTRAP_TARGET_NODE) is None:
            errors.append(
                "Bootstrap route must use a configured edge: "
                f"{BOOTSTRAP_START_NODE}->{BOOTSTRAP_TARGET_NODE}"
            )
        normalized_bootstrap_route = normalize_route(BOOTSTRAP_ROUTE)
        if normalized_bootstrap_route != BOOTSTRAP_ROUTE or len(BOOTSTRAP_ROUTE) < 2:
            errors.append(f"BOOTSTRAP_ROUTE must contain at least two valid nodes: {BOOTSTRAP_ROUTE!r}")
        else:
            if BOOTSTRAP_ROUTE[0] != BOOTSTRAP_START_NODE:
                errors.append(
                    "BOOTSTRAP_ROUTE must start at BOOTSTRAP_START_NODE: "
                    f"{BOOTSTRAP_ROUTE[0]} != {BOOTSTRAP_START_NODE}"
                )
            try:
                route_edges(BOOTSTRAP_ROUTE)
            except ValueError as exc:
                errors.append(f"BOOTSTRAP_ROUTE has invalid segment: {exc}")

    for edge_id, edge in DEFAULT_MAP_EDGES.items():
        src = normalize_node(edge.get("from"))
        dst = normalize_node(edge.get("to"))
        if not is_valid_node(src) or not is_valid_node(dst) or src == dst:
            errors.append(f"Invalid edge nodes: {edge_id} -> {src}, {dst}")
        if canonical_edge_id(src, dst) != edge_id:
            errors.append(f"Edge ID mismatch: {edge_id} for {src}-{dst}")
        try:
            cost = float(edge.get("cost", 0))
            if cost <= 0:
                errors.append(f"Edge cost must be positive: {edge_id}")
        except (TypeError, ValueError):
            errors.append(f"Edge cost must be numeric: {edge_id}")

    for robot_id, mission in ROBOT_MISSIONS.items():
        if normalize_robot_id(robot_id) != robot_id:
            errors.append(f"Mission robot ID is not canonical: {robot_id}")
        start = normalize_node(mission.get("start"))
        target = normalize_node(mission.get("target"))
        if not is_valid_node(start) or not is_valid_node(target):
            errors.append(f"Invalid mission nodes: {robot_id} {start}->{target}")

        try:
            initial_delay = float(mission.get("initial_start_delay_sec", 0.0) or 0.0)
        except (TypeError, ValueError):
            errors.append(
                f"initial_start_delay_sec must be numeric: {robot_id} -> "
                f"{mission.get('initial_start_delay_sec')!r}"
            )
            initial_delay = 0.0
        if initial_delay < 0:
            errors.append(
                f"initial_start_delay_sec must be >= 0: {robot_id} -> {initial_delay}"
            )
        delay_after_raw = mission.get("start_delay_after_robot")
        if initial_delay > 0:
            delay_after_robot = normalize_robot_id(delay_after_raw)
            if delay_after_robot not in ROBOT_MISSIONS:
                errors.append(
                    f"Unknown start_delay_after_robot: {robot_id} -> {delay_after_raw!r}"
                )
            elif delay_after_robot == robot_id:
                errors.append(
                    f"start_delay_after_robot cannot reference itself: {robot_id}"
                )

        for dependency in mission.get("dependencies", []):
            if not isinstance(dependency, Mapping):
                errors.append(f"Invalid dependency object: {robot_id} -> {dependency!r}")
                continue
            dependency_robot = normalize_robot_id(dependency.get("robot_id"))
            condition = str(dependency.get("condition") or "").strip().lower()
            if dependency_robot not in ROBOT_MISSIONS:
                errors.append(f"Unknown dependency robot: {robot_id} -> {dependency_robot}")
            if condition not in {"arrived", "at_node", "phase", "entered_edge"}:
                errors.append(f"Unsupported dependency condition: {robot_id} -> {condition}")
            for goal_guard in ("when_robot_goal", "when_mission_goal"):
                if goal_guard in dependency and not is_valid_node(
                    dependency.get(goal_guard)
                ):
                    errors.append(
                        f"Invalid dependency {goal_guard}: {robot_id} -> "
                        f"{dependency.get(goal_guard)!r}"
                    )
            if condition == "entered_edge":
                from_node = normalize_node(
                    dependency.get("from") or dependency.get("from_node")
                )
                to_node = normalize_node(
                    dependency.get("to") or dependency.get("to_node")
                )
                if (
                    not is_valid_node(from_node)
                    or not is_valid_node(to_node)
                    or from_node == to_node
                ):
                    errors.append(
                        f"Invalid entered_edge dependency: {robot_id} -> "
                        f"{from_node or '?'}->{to_node or '?'}"
                    )
                elif canonical_edge_id(from_node, to_node) is None:
                    errors.append(
                        f"Unknown entered_edge dependency edge: "
                        f"{robot_id} -> {from_node}->{to_node}"
                    )
                for flag_name in ("latch", "reserve_transit"):
                    if flag_name in dependency and not isinstance(
                        dependency.get(flag_name), bool
                    ):
                        errors.append(
                            f"entered_edge {flag_name} must be bool: "
                            f"{robot_id} -> {dependency.get(flag_name)!r}"
                        )

    _validate_dependency_cycles(errors)

    for robot_id, route in REFERENCE_ROUTES.items():
        if robot_id not in ROBOT_MISSIONS:
            errors.append(f"Reference route has unknown robot: {robot_id}")
            continue
        normalized = normalize_route(route)
        if normalized != route:
            errors.append(f"Reference route has invalid node: {robot_id} -> {route}")
        try:
            route_edges(normalized)
        except ValueError as exc:
            errors.append(f"Reference route has invalid segment: {robot_id} -> {exc}")
        if normalized and normalized[0] != DEFAULT_START_NODES[robot_id]:
            errors.append(f"Reference route start mismatch: {robot_id}")
        if normalized and normalized[-1] != DEFAULT_TARGET_NODES[robot_id]:
            errors.append(f"Reference route target mismatch: {robot_id}")

    try:
        normal_cost = route_cost(REFERENCE_ROUTES[AGV2_ID])
        bypass_cost = route_cost(EXPECTED_BYPASS_ROUTE)
        if bypass_cost <= normal_cost:
            errors.append(
                f"BLUE-RED bypass must be more expensive than the normal route: "
                f"normal={normal_cost}, bypass={bypass_cost}"
            )
    except Exception as exc:
        errors.append(f"Unable to validate normal/bypass route costs: {exc}")

    positive_values = {
        "DEFAULT_CONTROL_SPEED": DEFAULT_CONTROL_SPEED,
        "STOP_CONFIRM_TIMEOUT_SEC": STOP_CONFIRM_TIMEOUT_SEC,
        "ROUTE_ACK_TIMEOUT_SEC": ROUTE_ACK_TIMEOUT_SEC,
        "START_CONFIRM_TIMEOUT_SEC": START_CONFIRM_TIMEOUT_SEC,
        "AGV2_START_COMMAND_DELAY_SEC": AGV2_START_COMMAND_DELAY_SEC,
        "WATCHDOG_INTERVAL_SEC": WATCHDOG_INTERVAL_SEC,
        "STATUS_STALE_SEC": STATUS_STALE_SEC,
        "OCCUPANCY_HOLD_SEC": OCCUPANCY_HOLD_SEC,
        "OBSTACLE_CLEAR_STABLE_SEC": OBSTACLE_CLEAR_STABLE_SEC,
    }
    for name, value in positive_values.items():
        if value <= 0:
            errors.append(f"{name} must be > 0")
    if MAX_CONTROL_RETRIES < 0:
        errors.append("MAX_CONTROL_RETRIES must be >= 0")
    if MQTT_OPERATOR_QOS not in {0, 1, 2}:
        errors.append("MQTT_OPERATOR_QOS must be 0, 1, or 2")
    if ARRIVAL_FALLBACK_STOPPED_STATUS_VALUES & MOVING_STATUS_VALUES:
        errors.append("Arrival fallback stopped values must not overlap moving values")
    if "DEST" not in TERMINAL_NEXT_NODE_VALUES:
        errors.append("TERMINAL_NEXT_NODE_VALUES must include DEST for the current AGV status schema")

    if errors:
        raise ValueError("Invalid middleware settings:\n- " + "\n- ".join(errors))


# Backward-compatible entry point used by older launch scripts.
def validate_stage1_settings() -> None:
    validate_settings()
