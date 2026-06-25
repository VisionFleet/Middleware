"""Windows middleware for graph-based AGV routing and ACK-gated dispatch.

Core responsibilities
---------------------
* Maintain the five-node graph and dynamic edge status.
* Gate automatic dispatch on fresh AGV status messages.
* Preserve last-known occupancy and latch mission-arrival occupancy until a fresh move is observed.
* Latch configured edge-entry dependencies and reserve the active edge/next node.
* Calculate routes with Dijkstra while excluding blocked/reserved edges and occupied nodes.
* Manage each robot's mission independently.
* Dispatch safely in this order:
      line_stop -> fresh stopped status -> route -> accepted route_ack
      -> line_start -> line_tracing status
* Update Firebase and MQTT map/reroute views.

Deliberate scope boundary
-------------------------
This middleware selects the node sequence.  The AGV-side driving code must still
interpret BLUE->GREEN versus BLUE->RED and select the correct physical branch.
A route ACK proves only that the route was received/applied by the AGV process;
it does not prove physical drivability, shortest-path optimality, or collision
freedom.
"""

from __future__ import annotations

import argparse
import copy
import heapq
import json
import os
import sys
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

try:
    import setting as cfg
except ImportError as exc:
    raise SystemExit(
        "setting.py를 찾을 수 없습니다. main.py와 같은 폴더에 두세요."
    ) from exc

try:
    import firebase_admin
    from firebase_admin import credentials, db
except ImportError:  # --check / --no-firebase modes remain available.
    firebase_admin = None  # type: ignore[assignment]
    credentials = None  # type: ignore[assignment]
    db = None  # type: ignore[assignment]

try:
    import paho.mqtt.client as mqtt
except ImportError:
    mqtt = None  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Common utilities
# ---------------------------------------------------------------------------


def now_iso() -> str:
    return cfg.now_iso() if hasattr(cfg, "now_iso") else datetime.now().isoformat(timespec="milliseconds")


def compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


def pretty_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, default=str)


def safe_firebase_key(value: Any) -> str:
    text = str(value or "UNKNOWN")
    for character in [".", "#", "$", "[", "]", "/"]:
        text = text.replace(character, "_")
    return text[:180]


def make_id(prefix: str) -> str:
    stamp = datetime.now(cfg.KST).strftime("%Y%m%d_%H%M%S_%f")
    return f"{prefix}_{stamp}_{uuid.uuid4().hex[:4]}"


def decode_json_payload(raw_payload: bytes) -> Optional[Dict[str, Any]]:
    raw = raw_payload.decode("utf-8", errors="replace")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        print(f"[WARN] JSON decode failed: {exc}; raw={raw!r}")
        return None
    if not isinstance(payload, dict):
        print(f"[WARN] MQTT payload must be a JSON object: {payload!r}")
        return None
    return payload


def mqtt_reason_ok(reason_code: Any) -> bool:
    try:
        return int(reason_code) == 0
    except Exception:
        return str(reason_code).strip().lower() in {"0", "success"}


def topic_robot_id(topic: str) -> str:
    return cfg.robot_id_from_topic(topic) if hasattr(cfg, "robot_id_from_topic") else "UNKNOWN"


def status_age_sec(state: Mapping[str, Any]) -> Optional[float]:
    try:
        return max(0.0, time.monotonic() - float(state.get("cache_updated_monotonic")))
    except (TypeError, ValueError):
        return None


def normalize_event_type(value: Any) -> str:
    return str(value or "unknown_event").strip().lower()


# ---------------------------------------------------------------------------
# Firebase adapter
# ---------------------------------------------------------------------------


class FirebaseStore:
    """Small adapter that keeps Firebase optional and explicit."""

    def __init__(self, *, enabled: bool, write_enabled: bool) -> None:
        self.enabled = enabled
        self.write_enabled = write_enabled
        self.initialized = False

    def initialize(self) -> bool:
        if not self.enabled:
            print("[Firebase] disabled")
            return False
        if firebase_admin is None or credentials is None or db is None:
            raise RuntimeError("firebase-admin이 설치되어 있지 않습니다: pip install firebase-admin")
        if not cfg.FIREBASE_DATABASE_URL:
            raise RuntimeError("FIREBASE_DATABASE_URL이 .env에 없습니다.")

        credential_path = Path(cfg.FIREBASE_CREDENTIAL_PATH)
        if not credential_path.is_absolute():
            credential_path = Path(cfg.PROJECT_ROOT) / credential_path
        if not credential_path.exists():
            raise FileNotFoundError(f"Firebase credential file not found: {credential_path}")

        if not firebase_admin._apps:
            certificate = credentials.Certificate(str(credential_path))
            firebase_admin.initialize_app(
                certificate,
                {"databaseURL": cfg.FIREBASE_DATABASE_URL},
            )
        self.initialized = True
        print(f"[Firebase] initialized; write_enabled={self.write_enabled}")
        return True

    def set(self, path: str, value: Any) -> None:
        if not self.initialized or not self.write_enabled:
            return
        db.reference(path).set(value)

    def update(self, path: str, value: Mapping[str, Any]) -> None:
        if not self.initialized or not self.write_enabled:
            return
        db.reference(path).update(dict(value))

    def get(self, path: str) -> Any:
        if not self.initialized:
            return None
        return db.reference(path).get()


# ---------------------------------------------------------------------------
# Graph routing
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PathPlan:
    route: Tuple[str, ...]
    cost: float
    blocked_edges: Tuple[str, ...]
    reserved_edges: Tuple[str, ...]
    occupied_nodes: Tuple[str, ...]
    is_partial: bool = False
    final_target: Optional[str] = None
    partial_reason: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "route": list(self.route),
            "cost": self.cost,
            "blocked_edges": list(self.blocked_edges),
            "reserved_edges": list(self.reserved_edges),
            "occupied_nodes": list(self.occupied_nodes),
            "is_partial": self.is_partial,
            "final_target": self.final_target,
            "partial_reason": self.partial_reason,
        }


class GraphRouter:
    """Dijkstra router over the configured edge dictionary."""

    @staticmethod
    def shortest_path(
        edges: Mapping[str, Mapping[str, Any]],
        start: Any,
        target: Any,
        *,
        blocked_edges: Iterable[str] = (),
        reserved_edges: Iterable[str] = (),
        occupied_nodes: Iterable[str] = (),
    ) -> Optional[PathPlan]:
        start_node = cfg.normalize_node(start)
        target_node = cfg.normalize_node(target)
        if not cfg.is_valid_node(start_node) or not cfg.is_valid_node(target_node):
            raise ValueError(f"Invalid route endpoints: {start!r} -> {target!r}")

        blocked_edge_ids = {str(item) for item in blocked_edges}
        reserved_edge_ids = {str(item) for item in reserved_edges}
        excluded_edge_ids = blocked_edge_ids | reserved_edge_ids
        blocked_nodes = {cfg.normalize_node(item) for item in occupied_nodes if cfg.is_valid_node(item)}
        blocked_nodes.discard(start_node)

        if start_node == target_node:
            return PathPlan(
                route=(start_node,),
                cost=0.0,
                blocked_edges=tuple(sorted(blocked_edge_ids)),
                reserved_edges=tuple(sorted(reserved_edge_ids)),
                occupied_nodes=tuple(sorted(blocked_nodes)),
            )
        if target_node in blocked_nodes:
            return None

        adjacency: Dict[str, List[Tuple[str, float, str]]] = {
            node: [] for node in cfg.VALID_NODES
        }
        for edge_id, edge in edges.items():
            edge_id_text = str(edge_id)
            status = str(edge.get("status", "open")).strip().lower()
            if edge_id_text in excluded_edge_ids or status == "blocked":
                continue

            source = cfg.normalize_node(edge.get("from"))
            destination = cfg.normalize_node(edge.get("to"))
            if not cfg.is_valid_node(source) or not cfg.is_valid_node(destination):
                continue
            try:
                cost = float(edge.get("cost", 1.0))
            except (TypeError, ValueError):
                continue
            if cost <= 0:
                continue
            if status == "caution":
                cost *= float(cfg.CAUTION_COST_MULTIPLIER)

            adjacency[source].append((destination, cost, edge_id_text))
            if bool(edge.get("bidirectional", False)):
                adjacency[destination].append((source, cost, edge_id_text))

        # cost, hops, path tuple, current node.  The path tuple makes equal-cost
        # results deterministic without introducing scenario-specific ordering.
        queue: List[Tuple[float, int, Tuple[str, ...], str]] = [
            (0.0, 0, (start_node,), start_node)
        ]
        best: Dict[str, Tuple[float, int]] = {start_node: (0.0, 0)}

        while queue:
            cost_so_far, hops, path, current = heapq.heappop(queue)
            recorded = best.get(current)
            if recorded is not None and (cost_so_far, hops) > recorded:
                continue
            if current == target_node:
                return PathPlan(
                    route=path,
                    cost=cost_so_far,
                    blocked_edges=tuple(sorted(blocked_edge_ids)),
                    reserved_edges=tuple(sorted(reserved_edge_ids)),
                    occupied_nodes=tuple(sorted(blocked_nodes)),
                )

            for neighbor, edge_cost, _edge_id in adjacency.get(current, []):
                if neighbor in blocked_nodes or neighbor in path:
                    continue
                new_cost = cost_so_far + edge_cost
                new_hops = hops + 1
                previous = best.get(neighbor)
                if previous is not None and (new_cost, new_hops) >= previous:
                    continue
                best[neighbor] = (new_cost, new_hops)
                heapq.heappush(
                    queue,
                    (new_cost, new_hops, (*path, neighbor), neighbor),
                )
        return None


# ---------------------------------------------------------------------------
# Payload normalization
# ---------------------------------------------------------------------------


def node_or_empty(value: Any) -> str:
    node = cfg.normalize_node(value)
    return node if cfg.is_valid_node(node) else ""


def extract_route(payload: Mapping[str, Any]) -> List[str]:
    for key in ("current_route", "route", "new_route", "path"):
        route = cfg.normalize_route(payload.get(key))
        if route:
            return route
    return []


def payload_timestamp(payload: Mapping[str, Any]) -> str:
    return str(payload.get("updated_at") or payload.get("timestamp") or now_iso())


def normalize_robot_from_message(topic: str, payload: Mapping[str, Any]) -> Tuple[str, str, str]:
    wire_from_topic = topic_robot_id(topic)
    raw_robot_id = str(payload.get("robot_id") or wire_from_topic or "UNKNOWN")
    logical_id = cfg.normalize_robot_id(raw_robot_id)
    if logical_id == "UNKNOWN":
        logical_id = cfg.normalize_robot_id(wire_from_topic)
    return logical_id, wire_from_topic, raw_robot_id


def infer_nodes(payload: Mapping[str, Any], route: Sequence[str]) -> Tuple[str, str, str]:
    previous_node = node_or_empty(
        payload.get("previous_node")
        or payload.get("prev_node")
        or payload.get("last_node")
    )
    current_node = node_or_empty(payload.get("current_node"))
    next_node = node_or_empty(payload.get("next_node"))
    try:
        route_index = int(payload.get("route_index", 0) or 0)
    except (TypeError, ValueError):
        route_index = 0

    if route:
        route_index = max(0, min(route_index, len(route) - 1))
        current_node = current_node or route[route_index]
        if not previous_node and route_index > 0:
            previous_node = route[route_index - 1]
        if not next_node and route_index + 1 < len(route):
            next_node = route[route_index + 1]
    return previous_node, current_node, next_node


def normalize_status_payload(topic: str, payload: Mapping[str, Any]) -> Dict[str, Any]:
    logical_id, wire_id, raw_id = normalize_robot_from_message(topic, payload)
    route = extract_route(payload)
    previous_node, current_node, next_node = infer_nodes(payload, route)
    goal_node = cfg.extract_goal_node(payload)
    normalized = copy.deepcopy(dict(payload))
    normalized.update(
        {
            "type": str(payload.get("type") or "status"),
            "robot_id": logical_id,
            "raw_robot_id": raw_id,
            "mqtt_robot_id": wire_id,
            "current_route_normalized": route,
            "previous_node_normalized": previous_node,
            "current_node_normalized": current_node,
            "next_node_normalized": next_node,
            "goal_node_normalized": goal_node,
            "payload_time": payload_timestamp(payload),
            "cache_updated_at": now_iso(),
            "cache_updated_monotonic": time.monotonic(),
        }
    )
    normalized.setdefault("current_route", route)
    normalized.setdefault("updated_at", normalized["payload_time"])
    return normalized


def normalize_sensing_payload(topic: str, payload: Mapping[str, Any]) -> Dict[str, Any]:
    logical_id, wire_id, raw_id = normalize_robot_from_message(topic, payload)
    normalized = copy.deepcopy(dict(payload))
    normalized.update(
        {
            "type": str(payload.get("type") or "sensing"),
            "robot_id": logical_id,
            "raw_robot_id": raw_id,
            "mqtt_robot_id": wire_id,
            "previous_node_normalized": node_or_empty(
                payload.get("previous_node")
                or payload.get("prev_node")
                or payload.get("last_node")
            ),
            "current_node_normalized": node_or_empty(payload.get("current_node")),
            "next_node_normalized": node_or_empty(payload.get("next_node")),
            "payload_time": payload_timestamp(payload),
            "received_at": now_iso(),
        }
    )
    return normalized


def canonical_event_edge_with_source(
    payload: Mapping[str, Any],
    edges: Mapping[str, Mapping[str, Any]],
) -> Tuple[Optional[str], str]:
    """Resolve an obstacle segment, preferring explicit event fields."""
    previous_node = (
        payload.get("previous_node_normalized")
        or payload.get("previous_node")
        or payload.get("prev_node")
        or payload.get("last_node")
    )
    current_node = payload.get("current_node_normalized") or payload.get("current_node")
    derived = cfg.canonical_edge_id(previous_node, current_node, edges)
    if derived:
        return derived, "previous_current"

    parsed = cfg.parse_edge_key(payload.get("blocked_edge") or payload.get("edge"))
    if parsed:
        explicit = cfg.canonical_edge_id(parsed[0], parsed[1], edges)
        if explicit:
            return explicit, "explicit_edge"

    if cfg.ALLOW_LEGACY_CURRENT_NEXT_EDGE_FALLBACK:
        legacy = cfg.canonical_edge_id(
            current_node,
            payload.get("next_node_normalized") or payload.get("next_node"),
            edges,
        )
        if legacy:
            return legacy, "legacy_current_next"
    return None, "unresolved"


def canonical_event_edge(payload: Mapping[str, Any], edges: Mapping[str, Mapping[str, Any]]) -> Optional[str]:
    return canonical_event_edge_with_source(payload, edges)[0]


def normalize_event_payload(
    topic: str,
    payload: Mapping[str, Any],
    edges: Mapping[str, Mapping[str, Any]],
) -> Dict[str, Any]:
    logical_id, wire_id, raw_id = normalize_robot_from_message(topic, payload)
    previous_node = node_or_empty(
        payload.get("previous_node")
        or payload.get("prev_node")
        or payload.get("last_node")
    )
    current_node = node_or_empty(payload.get("current_node"))
    next_node = node_or_empty(payload.get("next_node"))
    edge_id, edge_source = canonical_event_edge_with_source(payload, edges)
    normalized = copy.deepcopy(dict(payload))
    normalized.update(
        {
            "event_id": str(payload.get("event_id") or make_id("EVT")),
            "type": normalize_event_type(payload.get("type")),
            "robot_id": logical_id,
            "raw_robot_id": raw_id,
            "mqtt_robot_id": wire_id,
            "previous_node_normalized": previous_node,
            "current_node_normalized": current_node,
            "next_node_normalized": next_node,
            "normalized_edge_id": edge_id,
            "edge_resolution_source": edge_source,
            "payload_time": payload_timestamp(payload),
            "received_at": now_iso(),
        }
    )
    normalized.setdefault("timestamp", normalized["payload_time"])
    return normalized


def route_id_from_ack(payload: Mapping[str, Any]) -> Optional[str]:
    value = payload.get("received_route_id") or payload.get("route_id")
    if value is None:
        return None
    text = str(value).strip()
    return text if text and text.lower() not in {"none", "null", "-"} else None


def normalize_route_ack_payload(topic: str, payload: Mapping[str, Any]) -> Dict[str, Any]:
    logical_id, wire_id, raw_id = normalize_robot_from_message(topic, payload)
    normalized = copy.deepcopy(dict(payload))
    normalized.update(
        {
            "type": str(payload.get("type") or "route_ack"),
            "ack_id": str(payload.get("ack_id") or make_id("ACK")),
            "robot_id": logical_id,
            "raw_robot_id": raw_id,
            "mqtt_robot_id": wire_id,
            "normalized_route_id": route_id_from_ack(payload),
            "received_route_normalized": cfg.normalize_route(payload.get("received_route")),
            "payload_time": payload_timestamp(payload),
            "received_at": now_iso(),
        }
    )
    normalized.setdefault("timestamp", normalized["payload_time"])
    return normalized


def state_previous_node(state: Mapping[str, Any]) -> str:
    return node_or_empty(
        state.get("previous_node_normalized")
        or state.get("previous_node")
        or state.get("prev_node")
        or state.get("last_node")
    )


def state_current_node(state: Mapping[str, Any]) -> str:
    return node_or_empty(state.get("current_node_normalized") or state.get("current_node"))


def state_goal_node(state: Mapping[str, Any]) -> str:
    return node_or_empty(state.get("goal_node_normalized") or cfg.extract_goal_node(state))


def state_next_node_text(state: Mapping[str, Any]) -> str:
    value = state.get("next_node_normalized") or state.get("next_node")
    node = node_or_empty(value)
    return node or str(value or "").strip().upper()


def state_status_text(state: Mapping[str, Any]) -> str:
    return str(state.get("status") or "").strip().lower()


def state_mode_text(state: Mapping[str, Any]) -> str:
    return str(state.get("mode") or "").strip().lower()


def status_is_fresh(
    state: Mapping[str, Any],
    *,
    max_age_sec: Optional[float] = None,
) -> bool:
    """Return whether this Windows-received status observation is still fresh."""
    age = status_age_sec(state)
    threshold = cfg.STATUS_STALE_SEC if max_age_sec is None else float(max_age_sec)
    return age is not None and age <= threshold


def status_is_stopped(state: Mapping[str, Any]) -> bool:
    """Broad stop confirmation used after a Windows-issued ``line_stop``."""
    if state_status_text(state) in cfg.STOPPED_STATUS_VALUES:
        return True
    if state_mode_text(state) in cfg.STOPPED_STATUS_VALUES:
        return True
    if state.get("robot_pause") is True:
        return True
    if "robot_run" in state and state.get("robot_run") is False:
        return True
    return False


def status_is_moving(state: Mapping[str, Any]) -> bool:
    if state_status_text(state) in cfg.MOVING_STATUS_VALUES:
        return True
    if state_mode_text(state) in cfg.MOVING_STATUS_VALUES:
        return True
    return state.get("robot_run") is True and state.get("robot_pause") is not True


def status_is_arrived(state: Mapping[str, Any], target_node: str) -> bool:
    """Conservatively recognize mission completion from the existing AGV status schema.

    ``next_node=DEST`` only means the route cursor is at its final element; it is
    not, by itself, proof that the vehicle has stopped.  Explicit ``arrived`` is
    preferred.  A restricted idle/stopped fallback remains for compatible fake
    AGVs that do not emit the explicit value.
    """
    target = cfg.normalize_node(target_node)
    if state_current_node(state) != target:
        return False

    status_text = state_status_text(state)
    mode_text = state_mode_text(state)
    explicit_arrival = (
        status_text in cfg.ARRIVED_STATUS_VALUES
        or mode_text in cfg.ARRIVED_STATUS_VALUES
    )
    if explicit_arrival:
        return state.get("robot_run") is not True and not status_is_moving(state)

    route = extract_route(state)
    try:
        route_index = int(state.get("route_index", -1))
    except (TypeError, ValueError):
        route_index = -1

    terminal_route_position = bool(
        route
        and route[-1] == target
        and route_index >= len(route) - 1
        and state_next_node_text(state) in cfg.TERMINAL_NEXT_NODE_VALUES
    )
    strictly_stationary = bool(
        state.get("robot_run") is False
        and not status_is_moving(state)
        and (
            status_text in cfg.ARRIVAL_FALLBACK_STOPPED_STATUS_VALUES
            or mode_text in cfg.ARRIVAL_FALLBACK_STOPPED_STATUS_VALUES
        )
    )
    return terminal_route_position and strictly_stationary


# ---------------------------------------------------------------------------
# Mission runtime state
# ---------------------------------------------------------------------------


PHASE_WAITING_GOAL = "waiting_goal"
PHASE_WAITING_STATUS = "waiting_status"
PHASE_WAITING_DEPENDENCY = "waiting_dependency"
PHASE_WAITING_START_DELAY = "waiting_start_delay"
PHASE_STOPPING = "waiting_stop_confirmation"
PHASE_WAITING_ROUTE_ACK = "waiting_route_ack"
PHASE_STARTING = "waiting_line_tracing_confirmation"
PHASE_MOVING = "moving"
PHASE_ARRIVED = "arrived"
PHASE_WAITING_PATH = "waiting_path"
PHASE_WAITING_OBSTACLE_CLEAR = "waiting_obstacle_clear"
PHASE_HOLD = "hold"
PHASE_OUT_OF_SERVICE = "out_of_service"
PHASE_FAULT = "fault"

STOP_PURPOSE_ROUTE_UPDATE = "route_update"
STOP_PURPOSE_DEPENDENCY_HOLD = "dependency_hold"


@dataclass
class MissionRuntime:
    robot_id: str
    start_node: str
    target_node: str
    auto_start: bool
    dependencies: List[Dict[str, Any]] = field(default_factory=list)
    initial_start_delay_sec: float = 0.0
    start_delay_after_robot: Optional[str] = None
    first_progress_at: Optional[str] = None
    first_progress_monotonic: Optional[float] = None
    initial_dispatch_started: bool = False
    default_target_node: str = ""
    goal_confirmed: bool = False
    goal_source: str = "config_fallback_unconfirmed"
    goal_confirmed_by_agv: bool = False
    bootstrap_active: bool = False
    bootstrap_completed_at: Optional[str] = None
    goal_updated_at: Optional[str] = None
    goal_revision: int = 0
    phase: str = PHASE_WAITING_GOAL
    phase_reason: str = "startup"
    active_route: List[str] = field(default_factory=list)
    active_route_id: Optional[str] = None
    active_route_is_partial: bool = False
    active_route_final_target: Optional[str] = None
    pending_route: List[str] = field(default_factory=list)
    pending_route_id: Optional[str] = None
    pending_route_payload: Optional[Dict[str, Any]] = None
    pending_route_is_partial: bool = False
    pending_route_final_target: Optional[str] = None
    pending_route_partial_reason: Optional[str] = None
    pending_reason: Optional[str] = None
    stop_purpose: Optional[str] = None
    stop_command_id: Optional[str] = None
    start_command_id: Optional[str] = None
    stop_requested_monotonic: Optional[float] = None
    route_sent_monotonic: Optional[float] = None
    start_sent_monotonic: Optional[float] = None
    stop_retries: int = 0
    route_retries: int = 0
    start_retries: int = 0
    last_plan_signature: Optional[str] = None
    last_plan_at_monotonic: Optional[float] = None
    last_plan_cost: Optional[float] = None
    last_error: Optional[str] = None
    last_transition_at: str = field(default_factory=now_iso)
    completed_at: Optional[str] = None
    fault_stop_sent: bool = False

    def public_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        for key in (
            "stop_requested_monotonic",
            "route_sent_monotonic",
            "start_sent_monotonic",
            "last_plan_at_monotonic",
            "first_progress_monotonic",
            "pending_route_payload",
        ):
            data.pop(key, None)
        return data


# ---------------------------------------------------------------------------
# Middleware application
# ---------------------------------------------------------------------------


class MiddlewareApp:
    def __init__(
        self,
        firebase: FirebaseStore,
        *,
        auto_start_enabled: bool,
        publish_map_on_connect: bool,
    ) -> None:
        self.firebase = firebase
        self.auto_start_enabled = auto_start_enabled
        self.publish_map_on_connect = publish_map_on_connect
        self.client: Any = None
        self.lock = threading.RLock()
        self.started_monotonic = time.monotonic()
        self.map_edges: Dict[str, Dict[str, Any]] = cfg.get_default_map_edges()
        self.robot_states: Dict[str, Dict[str, Any]] = {}
        self.sensing_states: Dict[str, Dict[str, Any]] = {}
        self.event_states: Dict[str, Dict[str, Any]] = {}
        self.route_ack_states: Dict[str, Dict[str, Any]] = {}
        self.latched_occupancies: Dict[str, Dict[str, Any]] = {}
        # An operator may mark a robot removed only after physically taking it
        # off the track. This runtime-local override intentionally resets to
        # ``present`` whenever the middleware restarts.
        self.track_presence: Dict[str, Dict[str, Any]] = {
            robot_id: {
                "robot_id": robot_id,
                "state": "present",
                "source": "middleware_startup",
                "updated_at": now_iso(),
            }
            for robot_id in cfg.ROBOT_MISSIONS
        }
        # Obstacle holds keep the reporting AGV at the last safe node until the
        # blocked edge has remained clear for the configured stability window.
        self.obstacle_holds: Dict[str, Dict[str, Any]] = {}
        self.pending_edge_clears: Dict[str, Dict[str, Any]] = {}
        # Destination reservations are multi-owner claims in shared mode.  They
        # are intentionally distinct from physical occupancy.
        self.target_reservations: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self.target_reservation_sequence = 0
        # Windows-internal one-shot progress facts. These are never published as
        # a new AGV MQTT message or command type.
        self.dependency_latches: Dict[str, Dict[str, Any]] = {}
        self.missions: Dict[str, MissionRuntime] = {}
        for robot_id, mission in cfg.ROBOT_MISSIONS.items():
            default_target = cfg.normalize_node(mission["target"])
            bootstrap_active = bool(
                not cfg.REQUIRE_GOAL_IN_STATUS
                and getattr(cfg, "BOOTSTRAP_ROUTE_BEFORE_GOAL", False)
            )
            goal_confirmed = bool(bootstrap_active or not cfg.REQUIRE_GOAL_IN_STATUS)
            bootstrap_route = cfg.normalize_route(getattr(cfg, "BOOTSTRAP_ROUTE", []))
            initial_target = (
                bootstrap_route[-1]
                if bootstrap_active
                else default_target
            )
            initial_start = (
                bootstrap_route[0]
                if bootstrap_active
                else cfg.normalize_node(mission["start"])
            )
            self.missions[robot_id] = MissionRuntime(
                robot_id=robot_id,
                start_node=initial_start,
                target_node=initial_target,
                default_target_node=default_target,
                goal_confirmed=goal_confirmed,
                goal_source=(
                    "bootstrap_route"
                    if bootstrap_active
                    else (
                        "config_default" if goal_confirmed
                        else "config_fallback_unconfirmed"
                    )
                ),
                goal_confirmed_by_agv=False,
                bootstrap_active=bootstrap_active,
                auto_start=bool(mission.get("auto_start", True)),
                dependencies=copy.deepcopy(list(mission.get("dependencies", []))),
                initial_start_delay_sec=max(
                    0.0, float(mission.get("initial_start_delay_sec", 0.0) or 0.0)
                ),
                start_delay_after_robot=(
                    cfg.normalize_robot_id(mission.get("start_delay_after_robot"))
                    if mission.get("start_delay_after_robot")
                    else None
                ),
                phase=(PHASE_WAITING_STATUS if goal_confirmed else PHASE_WAITING_GOAL),
            )

        self.watchdog_stop_event = threading.Event()
        self.watchdog_thread: Optional[threading.Thread] = None
        self.startup_start_sent = False

    # ----- MQTT/Firebase plumbing -------------------------------------------------

    def attach_client(self, client: Any) -> None:
        self.client = client

    def publish_json(
        self,
        topic: str,
        payload: Mapping[str, Any],
        *,
        qos: int = 0,
        retain: bool = False,
    ) -> bool:
        if self.client is None:
            raise RuntimeError("MQTT client is not attached")
        text = compact_json(payload)
        result = self.client.publish(topic, text, qos=qos, retain=retain)
        rc = int(getattr(result, "rc", result if isinstance(result, int) else 0))
        print(f"[MQTT TX] topic={topic} rc={rc} payload={text}")
        return rc == 0

    def persist_fleet_state(self) -> None:
        snapshot = self.fleet_state_snapshot()
        self.firebase.set(f"{cfg.TABLE_FLEET_STATE}/state", snapshot)

    def fleet_state_snapshot(self) -> Dict[str, Any]:
        with self.lock:
            return {
                "stage_id": cfg.STAGE_ID,
                "map_version": cfg.MAP_VERSION,
                "blocked_edges": sorted(self.blocked_edges()),
                "reserved_edges": self.reserved_edges_snapshot(),
                "target_reservation_mode": cfg.TARGET_RESERVATION_MODE,
                "target_reservations": self.target_reservations_snapshot(),
                "occupied_nodes": self.occupied_nodes_snapshot(),
                "occupancy_records": self.occupancy_records_snapshot(),
                "track_presence": self.track_presence_snapshot(),
                "start_delays": self.start_delay_snapshot(),
                "obstacle_holds": self.obstacle_holds_snapshot(),
                "pending_edge_clears": self.pending_edge_clears_snapshot(),
                "dependency_latches": copy.deepcopy(self.dependency_latches),
                "missions": {
                    robot_id: mission.public_dict()
                    for robot_id, mission in self.missions.items()
                },
                "updated_at": now_iso(),
            }

    def record_reroute_log(self, event: str, **details: Any) -> Dict[str, Any]:
        payload = {
            "log_id": make_id("REROUTE"),
            "event": event,
            "timestamp": now_iso(),
            **copy.deepcopy(details),
        }
        self.firebase.set(
            f"{cfg.TABLE_REROUTE_LOG}/{safe_firebase_key(payload['log_id'])}",
            payload,
        )
        if self.client is not None:
            try:
                self.publish_json(cfg.REROUTE_LOG_TOPIC, payload)
            except Exception as exc:
                print(f"[WARN] reroute log MQTT publish failed: {exc!r}")
        return payload

    # ----- Map -------------------------------------------------------------------

    def blocked_edges(self) -> Set[str]:
        return {
            edge_id
            for edge_id, edge in self.map_edges.items()
            if str(edge.get("status", "open")).strip().lower() == "blocked"
        }

    def load_map_from_firebase(self) -> None:
        if not cfg.LOAD_MAP_FROM_FIREBASE:
            return
        stored = self.firebase.get(f"{cfg.TABLE_MAP}/edges")
        if not isinstance(stored, Mapping):
            return

        loaded_count = 0
        with self.lock:
            for edge_id, base_edge in self.map_edges.items():
                candidate = stored.get(edge_id)
                if not isinstance(candidate, Mapping):
                    continue
                source = cfg.normalize_node(candidate.get("from"))
                destination = cfg.normalize_node(candidate.get("to"))
                if source != base_edge["from"] or destination != base_edge["to"]:
                    print(f"[WARN] ignoring Firebase edge with mismatched endpoints: {edge_id}")
                    continue
                merged = dict(base_edge)
                status = str(candidate.get("status", "open")).strip().lower()
                if status in {"open", "caution", "blocked"}:
                    merged["status"] = status
                for key in ("blocked_by", "blocked_event_id", "updated_at", "reason"):
                    if key in candidate:
                        merged[key] = candidate[key]
                self.map_edges[edge_id] = merged
                loaded_count += 1
        print(f"[MAP] loaded {loaded_count} known edges from Firebase")

    def map_snapshot(self) -> Dict[str, Any]:
        with self.lock:
            edges = copy.deepcopy(self.map_edges)
            occupied = self.occupied_nodes_snapshot()
            track_presence = self.track_presence_snapshot()
            start_delays = self.start_delay_snapshot()
        timestamp = now_iso()
        meta = {
            "stage_id": cfg.STAGE_ID,
            "layout_id": cfg.LAYOUT_ID,
            "version": cfg.MAP_VERSION,
            "map_version": cfg.MAP_VERSION,
            "nodes": sorted(cfg.VALID_NODES),
            "decision_nodes": sorted(cfg.DECISION_NODES),
            "edge_ids": sorted(edges.keys()),
            "edge_count": len(edges),
            "blocked_edges": sorted(
                edge_id
                for edge_id, edge in edges.items()
                if str(edge.get("status", "open")).lower() == "blocked"
            ),
            "reserved_edges": self.reserved_edges_snapshot(),
            "target_reservation_mode": cfg.TARGET_RESERVATION_MODE,
            "target_reservations": self.target_reservations_snapshot(),
            "occupied_nodes": occupied,
            "track_presence": track_presence,
            "start_delays": start_delays,
            "obstacle_holds": self.obstacle_holds_snapshot(),
            "pending_edge_clears": self.pending_edge_clears_snapshot(),
            "missions": {
                robot_id: mission.public_dict()
                for robot_id, mission in self.missions.items()
            },
            "reference_routes": copy.deepcopy(cfg.REFERENCE_ROUTES),
            "expected_bypass_route": list(cfg.EXPECTED_BYPASS_ROUTE),
            "updated_at": timestamp,
            "source": "main.py_graph_router",
        }
        return {
            "type": "map_snapshot",
            "stage_id": cfg.STAGE_ID,
            "layout_id": cfg.LAYOUT_ID,
            "map_version": cfg.MAP_VERSION,
            "edges": edges,
            "meta": meta,
            "timestamp": timestamp,
        }

    def save_map_to_firebase(self) -> Dict[str, Any]:
        snapshot = self.map_snapshot()
        if cfg.MAP_FIREBASE_WRITE_MODE == "merge":
            for edge_id, edge in snapshot["edges"].items():
                self.firebase.set(
                    f"{cfg.TABLE_MAP}/edges/{safe_firebase_key(edge_id)}",
                    edge,
                )
        else:
            self.firebase.set(f"{cfg.TABLE_MAP}/edges", snapshot["edges"])
        self.firebase.set(f"{cfg.TABLE_MAP}/meta", snapshot["meta"])
        return snapshot

    def publish_map_snapshot(self) -> bool:
        snapshot = self.map_snapshot()
        return self.publish_json(
            cfg.MAP_PUB_TOPIC,
            snapshot,
            retain=cfg.MAP_SNAPSHOT_RETAIN,
        )

    def set_edge_status(
        self,
        edge_id: str,
        status: str,
        *,
        event: Mapping[str, Any],
    ) -> bool:
        normalized_status = status.strip().lower()
        if normalized_status not in {"open", "caution", "blocked"}:
            raise ValueError(f"Unsupported edge status: {status}")

        with self.lock:
            if edge_id not in self.map_edges:
                return False
            edge = self.map_edges[edge_id]
            old_status = str(edge.get("status", "open")).strip().lower()
            edge["status"] = normalized_status
            edge["updated_at"] = now_iso()
            edge["reason"] = event.get("type")
            if normalized_status == "blocked":
                edge["blocked_by"] = event.get("robot_id")
                edge["blocked_event_id"] = event.get("event_id")
            else:
                edge.pop("blocked_by", None)
                edge.pop("blocked_event_id", None)
            for key in (
                "clear_pending",
                "clear_pending_since",
                "clear_stable_after_sec",
                "clear_event_id",
            ):
                edge.pop(key, None)
            changed = old_status != normalized_status
            updated_edge = copy.deepcopy(edge)

        self.firebase.set(
            f"{cfg.TABLE_MAP}/edges/{safe_firebase_key(edge_id)}",
            updated_edge,
        )
        self.firebase.set(f"{cfg.TABLE_MAP}/meta", self.map_snapshot()["meta"])
        print(f"[MAP] edge={edge_id} {old_status}->{normalized_status}")
        return changed

    def obstacle_holds_snapshot(self) -> Dict[str, Dict[str, Any]]:
        return {
            robot_id: copy.deepcopy(record)
            for robot_id, record in sorted(self.obstacle_holds.items())
        }

    def pending_edge_clears_snapshot(self) -> Dict[str, Dict[str, Any]]:
        now = time.monotonic()
        result: Dict[str, Dict[str, Any]] = {}
        for edge_id, record in sorted(self.pending_edge_clears.items()):
            public = {
                key: copy.deepcopy(value)
                for key, value in record.items()
                if key not in {"requested_monotonic", "ready_monotonic", "event"}
            }
            try:
                public["ready_in_sec"] = round(
                    max(0.0, float(record["ready_monotonic"]) - now),
                    3,
                )
            except (KeyError, TypeError, ValueError):
                public["ready_in_sec"] = None
            result[edge_id] = public
        return result

    def resolve_event_edge(
        self,
        event: Dict[str, Any],
    ) -> Tuple[Optional[str], str, str]:
        """Resolve edge/source/hold node without letting cached status override explicit fields."""
        robot_id = cfg.normalize_robot_id(event.get("robot_id"))
        state = self.robot_state(robot_id)

        event_previous = node_or_empty(
            event.get("previous_node_normalized")
            or event.get("previous_node")
            or event.get("prev_node")
            or event.get("last_node")
        )
        event_current = node_or_empty(
            event.get("current_node_normalized") or event.get("current_node")
        )
        event_next = node_or_empty(
            event.get("next_node_normalized") or event.get("next_node")
        )
        state_previous = state_previous_node(state)
        state_current = state_current_node(state)
        state_next = node_or_empty(state_next_node_text(state))

        edge_id: Optional[str] = None
        source = "unresolved"
        previous_node = event_previous
        current_node = event_current
        next_node = event_next

        if event_previous and event_current:
            edge_id = cfg.canonical_edge_id(
                event_previous, event_current, self.map_edges
            )
            if edge_id:
                source = "previous_current"

        if edge_id is None:
            parsed = cfg.parse_edge_key(event.get("blocked_edge") or event.get("edge"))
            if parsed:
                edge_id = cfg.canonical_edge_id(parsed[0], parsed[1], self.map_edges)
                if edge_id:
                    source = "explicit_edge"
                    previous_node = parsed[0]
                    current_node = parsed[1]

        if edge_id is None and cfg.ALLOW_LEGACY_CURRENT_NEXT_EDGE_FALLBACK:
            legacy_current = event_current or state_current
            legacy_next = event_next or state_next
            edge_id = cfg.canonical_edge_id(
                legacy_current, legacy_next, self.map_edges
            )
            if edge_id:
                source = "legacy_current_next"
                current_node = legacy_current
                next_node = legacy_next

        if edge_id is None and state_previous and state_current:
            previous_node = state_previous
            current_node = state_current
            next_node = event_next or state_next
            edge_id = cfg.canonical_edge_id(
                state_previous, state_current, self.map_edges
            )
            if edge_id:
                source = "cached_previous_current"

        if source in {"previous_current", "cached_previous_current"}:
            hold_node = previous_node
        elif source == "explicit_edge" and cfg.is_valid_node(event_current):
            # When AGV reports blocked_edge plus current_node, current_node is
            # the last safe physical node. Do not default to the edge's second
            # endpoint or GREEN-RED may be held at RED and look already arrived.
            hold_node = event_current
        else:
            hold_node = current_node
        if not cfg.is_valid_node(hold_node):
            hold_node = previous_node or current_node

        event.update(
            {
                "previous_node_normalized": previous_node,
                "current_node_normalized": current_node,
                "next_node_normalized": next_node,
                "normalized_edge_id": edge_id,
                "edge_resolution_source": source,
                "hold_node": hold_node,
            }
        )
        return edge_id, source, hold_node

    def update_edge_clear_pending_metadata(
        self,
        edge_id: str,
        record: Optional[Mapping[str, Any]],
    ) -> None:
        with self.lock:
            edge = self.map_edges.get(edge_id)
            if edge is None:
                return
            if record is None:
                for key in (
                    "clear_pending",
                    "clear_pending_since",
                    "clear_stable_after_sec",
                    "clear_event_id",
                ):
                    edge.pop(key, None)
            else:
                edge["clear_pending"] = True
                edge["clear_pending_since"] = record.get("requested_at")
                edge["clear_stable_after_sec"] = record.get("stable_after_sec")
                edge["clear_event_id"] = record.get("event_id")
                edge["updated_at"] = now_iso()
            updated_edge = copy.deepcopy(edge)
        self.firebase.set(
            f"{cfg.TABLE_MAP}/edges/{safe_firebase_key(edge_id)}",
            updated_edge,
        )

    def cancel_pending_edge_clear(self, edge_id: str, *, reason: str) -> bool:
        with self.lock:
            previous = self.pending_edge_clears.pop(edge_id, None)
            for hold in self.obstacle_holds.values():
                if hold.get("edge_id") == edge_id:
                    hold["status"] = "blocked"
                    hold["updated_at"] = now_iso()
        if previous is None:
            return False
        self.update_edge_clear_pending_metadata(edge_id, None)
        self.record_reroute_log(
            "edge_clear_cancelled",
            edge_id=edge_id,
            event_id=previous.get("event_id"),
            reason=reason,
        )
        return True

    def register_obstacle_hold(
        self,
        robot_id: Any,
        edge_id: str,
        event: Mapping[str, Any],
        *,
        hold_node: str,
    ) -> bool:
        logical_id = cfg.normalize_robot_id(robot_id)
        if self.robot_is_removed(logical_id):
            return False
        mission = self.missions.get(logical_id)
        if mission is None:
            return False
        normalized_hold_node = cfg.normalize_node(hold_node)
        if not cfg.is_valid_node(normalized_hold_node):
            normalized_hold_node = state_current_node(self.robot_state(logical_id))
        if not cfg.is_valid_node(normalized_hold_node):
            return False

        self.clear_pending_handshake(mission)
        mission.pending_reason = f"waiting_obstacle_clear:{edge_id}"
        mission.completed_at = None
        mission.last_error = None
        self.release_latched_occupancy(
            logical_id,
            reason=f"obstacle_hold:{edge_id}",
        )
        record = {
            "robot_id": logical_id,
            "edge_id": edge_id,
            "hold_node": normalized_hold_node,
            "occupies_node": False,
            "previous_node": event.get("previous_node_normalized"),
            "reported_current_node": event.get("current_node_normalized"),
            "edge_resolution_source": event.get("edge_resolution_source"),
            "event_id": event.get("event_id"),
            "status": "blocked",
            "detected_at": now_iso(),
            "updated_at": now_iso(),
        }
        self.obstacle_holds[logical_id] = record
        self.transition(
            mission,
            PHASE_WAITING_OBSTACLE_CLEAR,
            f"obstacle_hold:{edge_id}:{event.get('event_id')}",
        )
        self.record_reroute_log("obstacle_hold_started", **record)
        return True

    def release_obstacle_hold(self, robot_id: Any, *, reason: str) -> bool:
        logical_id = cfg.normalize_robot_id(robot_id)
        previous = self.obstacle_holds.pop(logical_id, None)
        if previous is None:
            return False
        self.record_reroute_log(
            "obstacle_hold_released",
            robot_id=logical_id,
            edge_id=previous.get("edge_id"),
            hold_node=previous.get("hold_node"),
            reason=reason,
        )
        return True

    def resume_after_obstacle_clear(self, robot_id: Any, edge_id: str) -> bool:
        logical_id = cfg.normalize_robot_id(robot_id)
        mission = self.missions.get(logical_id)
        if mission is None or mission.phase != PHASE_WAITING_OBSTACLE_CLEAR:
            return False
        if not mission.active_route or not mission.active_route_id:
            return False

        current = self.planning_current_node(logical_id)
        route = self.route_suffix(mission.active_route, current)
        if len(route) < 2:
            return False
        try:
            route_edges = set(cfg.route_edges(route, self.map_edges))
        except ValueError:
            return False
        if edge_id not in route_edges:
            return False
        if self.route_invalid_reason(mission):
            return False

        command_payload = self.build_command_payload(
            logical_id,
            cfg.LINE_START_COMMAND,
            reason=f"obstacle_clear_stable:{edge_id}",
            speed=cfg.DEFAULT_CONTROL_SPEED,
            linked_route_id=mission.active_route_id,
        )
        mission.start_command_id = str(command_payload["command_id"])
        mission.start_sent_monotonic = time.monotonic()
        mission.start_retries = 0
        if not self.publish_command_payload(logical_id, command_payload):
            self.fail_mission(mission, f"line_start_publish_failed:obstacle_clear:{edge_id}")
            return False
        if not mission.active_route_is_partial:
            self.reserve_target(
                mission,
                route_id=mission.active_route_id,
                status="active",
                reason=f"obstacle_clear_start:{edge_id}",
            )
        self.transition(mission, PHASE_STARTING, f"obstacle_clear_start:{edge_id}")
        self.record_reroute_log(
            "obstacle_clear_start_sent",
            robot_id=logical_id,
            edge_id=edge_id,
            route_id=mission.active_route_id,
            route=route,
            command_id=mission.start_command_id,
        )
        return True

    def schedule_edge_clear(
        self,
        edge_id: str,
        event: Mapping[str, Any],
    ) -> Dict[str, Any]:
        with self.lock:
            existing = self.pending_edge_clears.get(edge_id)
            if existing is not None:
                return copy.deepcopy(existing)
            requested_monotonic = time.monotonic()
            record = {
                "edge_id": edge_id,
                "event_id": event.get("event_id"),
                "robot_id": event.get("robot_id"),
                "requested_at": now_iso(),
                "requested_monotonic": requested_monotonic,
                "ready_monotonic": requested_monotonic
                + float(cfg.OBSTACLE_CLEAR_STABLE_SEC),
                "stable_after_sec": float(cfg.OBSTACLE_CLEAR_STABLE_SEC),
                "status": "clear_pending",
                "event": copy.deepcopy(dict(event)),
            }
            self.pending_edge_clears[edge_id] = record
            for hold in self.obstacle_holds.values():
                if hold.get("edge_id") == edge_id:
                    hold["status"] = "clear_pending"
                    hold["clear_event_id"] = event.get("event_id")
                    hold["updated_at"] = now_iso()
        self.update_edge_clear_pending_metadata(edge_id, record)
        self.record_reroute_log(
            "edge_clear_pending",
            edge_id=edge_id,
            event_id=event.get("event_id"),
            stable_after_sec=cfg.OBSTACLE_CLEAR_STABLE_SEC,
        )
        return copy.deepcopy(record)

    def process_pending_edge_clears(
        self,
        *,
        now_monotonic: Optional[float] = None,
    ) -> List[str]:
        current = time.monotonic() if now_monotonic is None else float(now_monotonic)
        ready: List[Dict[str, Any]] = []
        with self.lock:
            for edge_id, record in list(self.pending_edge_clears.items()):
                if current < float(record.get("ready_monotonic", current + 1.0)):
                    continue
                ready.append(self.pending_edge_clears.pop(edge_id))

        opened_edges: List[str] = []
        for record in ready:
            edge_id = str(record["edge_id"])
            event = copy.deepcopy(record.get("event") or {})
            event["type"] = "obstacle_clear_stable"
            changed = self.set_edge_status(edge_id, "open", event=event)
            resume_robot_ids: List[str] = []
            replan_robot_ids: List[str] = []
            with self.lock:
                for robot_id, hold in self.obstacle_holds.items():
                    if hold.get("edge_id") != edge_id:
                        continue
                    hold["status"] = "clear_confirmed"
                    hold["cleared_at"] = now_iso()
                    hold["updated_at"] = now_iso()
                    mission = self.missions.get(robot_id)
                    if mission and mission.phase == PHASE_WAITING_OBSTACLE_CLEAR:
                        resume_robot_ids.append(robot_id)
            if self.client is not None:
                self.publish_map_snapshot()
            self.record_reroute_log(
                "edge_clear_stable",
                edge_id=edge_id,
                event_id=record.get("event_id"),
                changed=changed,
                resumed_robots=resume_robot_ids,
            )
            for robot_id in resume_robot_ids:
                if self.resume_after_obstacle_clear(robot_id, edge_id):
                    continue
                mission = self.missions.get(robot_id)
                if mission and mission.phase == PHASE_WAITING_OBSTACLE_CLEAR:
                    self.transition(
                        mission,
                        PHASE_WAITING_PATH,
                        f"obstacle_clear_stable:{edge_id}",
                    )
                replan_robot_ids.append(robot_id)
            for robot_id in replan_robot_ids:
                self.request_replan(
                    robot_id,
                    reason=f"obstacle_clear_stable:{edge_id}",
                    force=True,
                )
            opened_edges.append(edge_id)

        if opened_edges:
            self.evaluate_fleet(
                trigger=f"edge_clear_stable:{','.join(opened_edges)}",
                force_waiting_path=True,
            )
        return opened_edges

    # ----- Fleet observations -----------------------------------------------------

    def robot_state(self, robot_id: Any) -> Dict[str, Any]:
        return self.robot_states.get(cfg.normalize_robot_id(robot_id), {})

    def planning_current_node(self, robot_id: Any) -> str:
        logical_id = cfg.normalize_robot_id(robot_id)
        if self.robot_is_removed(logical_id):
            return ""
        hold = self.obstacle_holds.get(logical_id)
        if hold and cfg.is_valid_node(hold.get("hold_node")):
            return cfg.normalize_node(hold.get("hold_node"))
        observed = state_current_node(self.robot_state(logical_id))
        if observed:
            return observed
        mission = self.missions.get(logical_id)
        if mission and mission.bootstrap_active:
            # Before the camera sees the first PURPLE marker there is no physical
            # graph node to report. Only the bootstrap route may use the configured
            # PURPLE graph entry as its planning origin.
            return cfg.normalize_node(cfg.BOOTSTRAP_START_NODE)
        return ""

    def mission_has_goal(self, mission: MissionRuntime) -> bool:
        """Return whether a target exists for planning, including bootstrap."""
        return bool(mission.goal_confirmed and cfg.is_valid_node(mission.target_node))

    def mission_has_operational_goal(self, mission: MissionRuntime) -> bool:
        """Return whether the real AGV-reported mission goal is available."""
        if not self.mission_has_goal(mission):
            return False
        if mission.bootstrap_active:
            return False
        if cfg.REQUIRE_GOAL_IN_STATUS:
            return mission.goal_confirmed_by_agv
        # Without bootstrap mode, REQUIRE_GOAL_IN_STATUS=false preserves the
        # legacy behavior in which the configured target is operational.
        return bool(
            mission.goal_confirmed_by_agv
            or mission.goal_source == "config_default"
        )

    def update_mission_goal_from_status(
        self,
        robot_id: Any,
        state: Mapping[str, Any],
    ) -> bool:
        """Apply an AGV-reported goal and return whether dispatch state changed.

        In bootstrap mode a goal is accepted only after the AGV has physically
        acquired PURPLE. By default that confirmation must also be stationary,
        so the provisional route can never carry the robot past the first graph
        node while the real destination is still unknown.
        """
        logical_id = cfg.normalize_robot_id(robot_id)
        if self.robot_is_removed(logical_id):
            return False
        mission = self.missions[logical_id]
        reported_goal = state_goal_node(state)
        if not reported_goal:
            return False

        was_bootstrap = mission.bootstrap_active
        if was_bootstrap:
            confirm_node = cfg.normalize_node(cfg.BOOTSTRAP_GOAL_CONFIRM_NODE)
            if state_current_node(state) != confirm_node:
                return False
            if (
                cfg.BOOTSTRAP_REQUIRE_STOPPED_CONFIRMATION
                and not status_is_stopped(state)
            ):
                return False

        old_goal = cfg.normalize_node(mission.target_node)
        was_confirmed = mission.goal_confirmed
        was_confirmed_by_agv = mission.goal_confirmed_by_agv
        goal_changed = old_goal != reported_goal
        newly_confirmed = not was_confirmed or not was_confirmed_by_agv or was_bootstrap
        if not goal_changed and not newly_confirmed:
            mission.goal_source = "agv_status"
            mission.goal_updated_at = str(state.get("payload_time") or now_iso())
            return False

        if goal_changed or was_bootstrap:
            self.remove_target_reservation(
                logical_id,
                reason=f"goal_update:{old_goal}->{reported_goal}",
            )
            self.release_latched_occupancy(
                logical_id,
                reason=f"goal_update:{old_goal}->{reported_goal}",
            )

        mission.target_node = reported_goal
        mission.goal_confirmed = True
        mission.goal_confirmed_by_agv = True
        mission.goal_source = "agv_status"
        mission.goal_updated_at = str(state.get("payload_time") or now_iso())
        mission.goal_revision += 1
        mission.completed_at = None
        mission.last_error = None
        mission.fault_stop_sent = False
        mission.bootstrap_active = False
        if was_bootstrap:
            mission.bootstrap_completed_at = now_iso()

        reason = (
            f"bootstrap_goal_confirmed:{reported_goal}"
            if was_bootstrap
            else (
                f"goal_changed:{old_goal}->{reported_goal}"
                if goal_changed
                else f"goal_received:{reported_goal}"
            )
        )
        if mission.phase == PHASE_WAITING_OBSTACLE_CLEAR:
            mission.phase_reason = reason
            mission.last_transition_at = now_iso()
        elif mission.phase == PHASE_STOPPING:
            # The already outstanding stop handshake can directly produce the
            # final route from PURPLE once its fresh stopped status is processed.
            mission.pending_reason = reason
        elif mission.phase == PHASE_MOVING:
            # Keep moving for this handler invocation. evaluate_fleet will issue
            # line_stop because the provisional route no longer ends at the real
            # goal. In the intended AGV flow, the AGV has already locally stopped
            # at PURPLE, so handle_status moves this to HOLD and replans below.
            mission.phase_reason = reason
            mission.last_transition_at = now_iso()
        elif mission.phase in {PHASE_WAITING_ROUTE_ACK, PHASE_STARTING}:
            self.clear_pending_handshake(mission)
            self.transition(mission, PHASE_HOLD, reason)
        else:
            self.clear_pending_handshake(mission)
            self.transition(mission, PHASE_WAITING_STATUS, reason)

        self.record_reroute_log(
            "mission_goal_updated",
            robot_id=logical_id,
            old_goal=old_goal,
            new_goal=reported_goal,
            newly_confirmed=newly_confirmed,
            bootstrap_completed=was_bootstrap,
            goal_revision=mission.goal_revision,
        )
        return True

    def robot_status_issue(self, robot_id: Any) -> Optional[str]:
        """Explain why a status cannot be used for a new automatic dispatch."""
        logical_id = cfg.normalize_robot_id(robot_id)
        if self.robot_is_removed(logical_id):
            return None
        state = self.robot_state(logical_id)
        if not state:
            return "missing"
        age = status_age_sec(state)
        if age is None:
            return "missing_windows_receive_time"
        if age > cfg.STATUS_STALE_SEC:
            return f"stale:{age:.2f}s"
        mission = self.missions.get(logical_id)
        if not state_current_node(state):
            if mission is None or not mission.bootstrap_active:
                return "invalid_current_node"
        if cfg.REQUIRE_GOAL_IN_STATUS and (
            mission is None or not self.mission_has_operational_goal(mission)
        ):
            return "missing_goal"
        return None

    def dependency_latch_key(self, dependency: Mapping[str, Any]) -> str:
        dependency_robot = cfg.normalize_robot_id(dependency.get("robot_id"))
        from_node = cfg.normalize_node(
            dependency.get("from") or dependency.get("from_node")
        )
        to_node = cfg.normalize_node(
            dependency.get("to") or dependency.get("to_node")
        )
        return f"{dependency_robot}:{from_node}->{to_node}"

    def dependency_applies_to_mission(
        self,
        owner_mission: MissionRuntime,
        dependency: Mapping[str, Any],
    ) -> bool:
        """Return whether a dependency applies to the confirmed real goals.

        Bootstrap movement only acquires the first PURPLE marker. It must not
        activate scenario-specific edge reservations based on provisional
        targets. Dependencies become authoritative after the referenced AGVs
        report their real goals.
        """
        if self.robot_is_removed(owner_mission.robot_id):
            return False
        if owner_mission.bootstrap_active:
            return False

        active_when_goals = dependency.get("active_when_goals")
        if isinstance(active_when_goals, Mapping):
            for robot_value, expected_value in active_when_goals.items():
                robot_id = cfg.normalize_robot_id(robot_value)
                expected_goal = cfg.normalize_node(expected_value)
                goal_mission = self.missions.get(robot_id)
                if not expected_goal or goal_mission is None:
                    continue
                if not self.mission_has_operational_goal(goal_mission):
                    if goal_mission.bootstrap_active:
                        return False
                    # In normal goal-required mode, keep the dependency active
                    # conservatively until the referenced goal arrives.
                    continue
                if goal_mission.target_node != expected_goal:
                    return False

        # Backward-compatible aliases for earlier configuration drafts.
        expected_owner_goal = cfg.normalize_node(
            dependency.get("when_mission_goal")
        )
        if expected_owner_goal:
            if self.mission_has_operational_goal(owner_mission):
                if owner_mission.target_node != expected_owner_goal:
                    return False
            elif owner_mission.bootstrap_active:
                return False

        dependency_robot = cfg.normalize_robot_id(dependency.get("robot_id"))
        dependency_mission = self.missions.get(dependency_robot)
        expected_robot_goal = cfg.normalize_node(
            dependency.get("when_robot_goal")
        )
        if expected_robot_goal:
            if dependency_mission is None:
                return True
            if self.mission_has_operational_goal(dependency_mission):
                if dependency_mission.target_node != expected_robot_goal:
                    return False
            elif dependency_mission.bootstrap_active:
                return False

        return True

    def dependency_may_apply_after_goal_confirmation(
        self,
        owner_mission: MissionRuntime,
        dependency: Mapping[str, Any],
    ) -> bool:
        """Return False only when confirmed goals definitively exclude a dependency.

        During bootstrap the dependent AGV may not have reported its real goal yet.
        We still latch observed edge-entry facts so a later Scenario-1 goal cannot
        miss an AGV1 BLUE->GREEN transition that already happened.  The latch is
        inert until ``dependency_applies_to_mission`` becomes true.
        """
        if self.robot_is_removed(owner_mission.robot_id):
            return False

        active_when_goals = dependency.get("active_when_goals")
        if isinstance(active_when_goals, Mapping):
            for robot_value, expected_value in active_when_goals.items():
                robot_id = cfg.normalize_robot_id(robot_value)
                expected_goal = cfg.normalize_node(expected_value)
                goal_mission = self.missions.get(robot_id)
                if (
                    expected_goal
                    and goal_mission is not None
                    and self.mission_has_operational_goal(goal_mission)
                    and goal_mission.target_node != expected_goal
                ):
                    return False

        expected_owner_goal = cfg.normalize_node(
            dependency.get("when_mission_goal")
        )
        if (
            expected_owner_goal
            and self.mission_has_operational_goal(owner_mission)
            and owner_mission.target_node != expected_owner_goal
        ):
            return False

        dependency_robot = cfg.normalize_robot_id(dependency.get("robot_id"))
        dependency_mission = self.missions.get(dependency_robot)
        expected_robot_goal = cfg.normalize_node(
            dependency.get("when_robot_goal")
        )
        if (
            expected_robot_goal
            and dependency_mission is not None
            and self.mission_has_operational_goal(dependency_mission)
            and dependency_mission.target_node != expected_robot_goal
        ):
            return False

        return True

    def entered_edge_dependencies_for_robot(
        self,
        robot_id: Any,
        *,
        include_potential: bool = False,
    ) -> List[Dict[str, Any]]:
        logical_id = cfg.normalize_robot_id(robot_id)
        if self.robot_is_removed(logical_id):
            return []
        result: List[Dict[str, Any]] = []
        for owner_mission in self.missions.values():
            if self.robot_is_removed(owner_mission.robot_id):
                continue
            for dependency in owner_mission.dependencies:
                if not isinstance(dependency, Mapping):
                    continue
                if include_potential:
                    if not self.dependency_may_apply_after_goal_confirmation(
                        owner_mission, dependency
                    ):
                        continue
                elif not self.dependency_applies_to_mission(
                    owner_mission, dependency
                ):
                    continue
                condition = str(dependency.get("condition") or "").strip().lower()
                dependency_robot = cfg.normalize_robot_id(dependency.get("robot_id"))
                if condition == "entered_edge" and dependency_robot == logical_id:
                    result.append(dict(dependency))
        return result

    def status_matches_entered_edge(
        self,
        state: Mapping[str, Any],
        dependency: Mapping[str, Any],
    ) -> bool:
        from_node = cfg.normalize_node(
            dependency.get("from") or dependency.get("from_node")
        )
        to_node = cfg.normalize_node(
            dependency.get("to") or dependency.get("to_node")
        )
        explicit_line_tracing = (
            state_status_text(state) in cfg.MOVING_STATUS_VALUES
            or state_mode_text(state) in cfg.MOVING_STATUS_VALUES
        )
        return bool(
            cfg.is_valid_node(from_node)
            and cfg.is_valid_node(to_node)
            and status_is_fresh(state)
            and explicit_line_tracing
            and state.get("robot_run") is True
            and state.get("robot_pause") is not True
            and state_current_node(state) == from_node
            and state_next_node_text(state) == to_node
        )

    def update_dependency_latches_from_status(
        self,
        robot_id: Any,
        state: Mapping[str, Any],
    ) -> None:
        logical_id = cfg.normalize_robot_id(robot_id)
        if self.robot_is_removed(logical_id):
            return
        for dependency in self.entered_edge_dependencies_for_robot(
            logical_id,
            include_potential=True,
        ):
            if not bool(dependency.get("latch", True)):
                continue
            if not self.status_matches_entered_edge(state, dependency):
                continue
            key = self.dependency_latch_key(dependency)
            if key in self.dependency_latches:
                continue
            from_node = cfg.normalize_node(
                dependency.get("from") or dependency.get("from_node")
            )
            to_node = cfg.normalize_node(
                dependency.get("to") or dependency.get("to_node")
            )
            edge_id = cfg.canonical_edge_id(from_node, to_node, self.map_edges)
            record = {
                "key": key,
                "robot_id": logical_id,
                "condition": "entered_edge",
                "from_node": from_node,
                "to_node": to_node,
                "edge_id": edge_id,
                "latched_at": now_iso(),
            }
            self.dependency_latches[key] = record
            print(
                f"[DEPENDENCY] latched {key} edge={edge_id} "
                f"status={state_status_text(state)}"
            )
            self.record_reroute_log("dependency_latched", **record)

    def active_transit_reservation(
        self,
        robot_id: Any,
    ) -> Optional[Dict[str, Any]]:
        logical_id = cfg.normalize_robot_id(robot_id)
        if self.robot_is_removed(logical_id):
            return None
        state = self.robot_state(logical_id)
        if not state or not status_is_fresh(state) or not status_is_moving(state):
            return None
        for dependency in self.entered_edge_dependencies_for_robot(logical_id):
            if not bool(dependency.get("reserve_transit", False)):
                continue
            if not self.status_matches_entered_edge(state, dependency):
                continue
            from_node = cfg.normalize_node(
                dependency.get("from") or dependency.get("from_node")
            )
            to_node = cfg.normalize_node(
                dependency.get("to") or dependency.get("to_node")
            )
            edge_id = cfg.canonical_edge_id(from_node, to_node, self.map_edges)
            if edge_id is None:
                continue
            return {
                "robot_id": logical_id,
                "from_node": from_node,
                "to_node": to_node,
                "edge_id": edge_id,
                "source": "moving_edge_reservation",
            }
        return None

    def reserved_edges_for(self, robot_id: Any) -> Set[str]:
        logical_id = cfg.normalize_robot_id(robot_id)
        reserved: Set[str] = set()
        for other_id in set(cfg.ROBOT_IDS) | set(self.robot_states):
            if other_id == logical_id:
                continue
            reservation = self.active_transit_reservation(other_id)
            if reservation and reservation.get("edge_id"):
                reserved.add(str(reservation["edge_id"]))
        return reserved

    def reserved_edges_snapshot(self) -> Dict[str, List[str]]:
        result: Dict[str, List[str]] = {}
        for robot_id in set(cfg.ROBOT_IDS) | set(self.robot_states):
            reservation = self.active_transit_reservation(robot_id)
            if not reservation:
                continue
            edge_id = str(reservation.get("edge_id") or "")
            if edge_id:
                result.setdefault(edge_id, []).append(cfg.normalize_robot_id(robot_id))
        for edge_id in result:
            result[edge_id].sort()
        return dict(sorted(result.items()))

    def target_reservations_snapshot(self) -> Dict[str, Dict[str, Any]]:
        result: Dict[str, Dict[str, Any]] = {}
        for node, records in sorted(self.target_reservations.items()):
            ordered_items = sorted(
                (
                    (robot_id, record)
                    for robot_id, record in records.items()
                    if not self.robot_is_removed(robot_id)
                ),
                key=lambda item: (
                    int(item[1].get("sequence", 0) or 0),
                    item[0],
                ),
            )
            public_records = {
                robot_id: copy.deepcopy(record)
                for robot_id, record in ordered_items
            }
            if not public_records:
                continue
            owners = [robot_id for robot_id, _record in ordered_items]
            result[node] = {
                "mode": cfg.TARGET_RESERVATION_MODE,
                "owners": owners,
                "claim_order": owners,
                "records": public_records,
            }
        return result

    def persist_target_reservations(self) -> None:
        self.firebase.set(
            f"{cfg.TABLE_TARGET_RESERVATION}/reservations",
            self.target_reservations_snapshot(),
        )

    def remove_target_reservation(self, robot_id: Any, *, reason: str) -> bool:
        logical_id = cfg.normalize_robot_id(robot_id)
        removed = False
        for node in list(self.target_reservations):
            records = self.target_reservations[node]
            record = records.pop(logical_id, None)
            if record is not None:
                removed = True
                self.record_reroute_log(
                    "target_reservation_removed",
                    robot_id=logical_id,
                    target_node=node,
                    previous_status=record.get("status"),
                    reason=reason,
                )
            if not records:
                self.target_reservations.pop(node, None)
        if removed:
            self.persist_target_reservations()
        return removed

    def reserve_target(
        self,
        mission: MissionRuntime,
        *,
        route_id: Optional[str],
        status: str,
        reason: str,
    ) -> None:
        if self.robot_is_removed(mission.robot_id):
            return
        if cfg.TARGET_RESERVATION_MODE == "disabled":
            return
        # The PURPLE acquisition route is not a mission destination and must not
        # create ORANGE/GREEN/RED claims or influence collision planning.
        if mission.bootstrap_active or not self.mission_has_operational_goal(mission):
            return
        target_node = cfg.normalize_node(mission.target_node)
        if not cfg.is_valid_node(target_node):
            return

        # A robot may have only one current destination claim.  Shared mode
        # allows several different robots to claim the same node.
        for node in list(self.target_reservations):
            if node == target_node:
                continue
            records = self.target_reservations[node]
            records.pop(mission.robot_id, None)
            if not records:
                self.target_reservations.pop(node, None)

        records = self.target_reservations.setdefault(target_node, {})
        previous = records.get(mission.robot_id, {})
        if previous.get("sequence") is None:
            self.target_reservation_sequence += 1
        sequence = int(
            previous.get("sequence") or self.target_reservation_sequence
        )
        record = {
            "robot_id": mission.robot_id,
            "target_node": target_node,
            "route_id": route_id,
            "status": status,
            "mode": cfg.TARGET_RESERVATION_MODE,
            "sequence": sequence,
            "reserved_at": previous.get("reserved_at") or now_iso(),
            "updated_at": now_iso(),
            "reason": reason,
        }
        records[mission.robot_id] = record
        self.persist_target_reservations()
        self.record_reroute_log(
            "target_reserved",
            **record,
            owners=sorted(records),
        )

    def target_reserved_nodes_for(self, robot_id: Any) -> Set[str]:
        if cfg.TARGET_RESERVATION_MODE != "exclusive":
            return set()
        logical_id = cfg.normalize_robot_id(robot_id)
        reserved: Set[str] = set()
        for node, records in self.target_reservations.items():
            for owner, record in records.items():
                if owner == logical_id or self.robot_is_removed(owner):
                    continue
                if str(record.get("status") or "") in {
                    "pending_ack",
                    "active",
                    "arrived",
                }:
                    reserved.add(node)
                    break
        return reserved

    def latch_occupancy(self, robot_id: Any, node: Any, *, reason: str) -> None:
        logical_id = cfg.normalize_robot_id(robot_id)
        if self.robot_is_removed(logical_id):
            return
        normalized_node = cfg.normalize_node(node)
        if not cfg.is_valid_node(normalized_node):
            return
        previous = self.latched_occupancies.get(logical_id)
        self.latched_occupancies[logical_id] = {
            "robot_id": logical_id,
            "node": normalized_node,
            "reason": reason,
            "latched_at": now_iso(),
        }
        if previous is None or previous.get("node") != normalized_node:
            print(
                f"[OCCUPANCY] latched robot={logical_id} node={normalized_node} reason={reason}"
            )

    def release_latched_occupancy(self, robot_id: Any, *, reason: str) -> bool:
        logical_id = cfg.normalize_robot_id(robot_id)
        previous = self.latched_occupancies.pop(logical_id, None)
        if previous is None:
            return False
        print(
            f"[OCCUPANCY] released robot={logical_id} node={previous.get('node')} reason={reason}"
        )
        return True

    def track_presence_snapshot(self) -> Dict[str, Dict[str, Any]]:
        with self.lock:
            return copy.deepcopy(self.track_presence)

    def robot_is_removed(self, robot_id: Any) -> bool:
        logical_id = cfg.normalize_robot_id(robot_id)
        record = self.track_presence.get(logical_id, {})
        return str(record.get("state") or "present").strip().lower() == "removed"

    def _publish_robot_presence_ack(
        self,
        request: Mapping[str, Any],
        *,
        status: str,
        reason: str,
        released_latched_node: Optional[str] = None,
    ) -> None:
        if self.client is None:
            return
        logical_id = cfg.normalize_robot_id(request.get("robot_id"))
        payload = {
            "type": "robot_presence_ack",
            "request_id": request.get("request_id"),
            "action": request.get("action"),
            "robot_id": cfg.wire_robot_id(logical_id),
            "logical_robot_id": logical_id,
            "track_state": (
                self.track_presence.get(logical_id, {}).get("state") or "present"
            ),
            "status": status,
            "reason": reason,
            "released_latched_node": released_latched_node,
            "timestamp": now_iso(),
        }
        try:
            self.publish_json(
                cfg.ROBOT_PRESENCE_ACK_TOPIC,
                payload,
                qos=cfg.MQTT_OPERATOR_QOS,
            )
        except Exception as exc:
            print(f"[WARN] presence ACK publish failed: {exc!r}")

    def _record_operator_action(
        self,
        request: Mapping[str, Any],
        *,
        status: str,
        reason: str,
        released_latched_node: Optional[str] = None,
    ) -> None:
        request_id = str(request.get("request_id") or make_id("PRESENCE"))
        record = {
            "type": "operator_robot_presence_action",
            "request_id": request_id,
            "action": request.get("action"),
            "robot_id": request.get("robot_id"),
            "issued_by": request.get("issued_by"),
            "operator_confirmed_physical_removal": bool(
                request.get("operator_confirmed_physical_removal")
            ),
            "status": status,
            "reason": reason,
            "released_latched_node": released_latched_node,
            "requested_at": request.get("timestamp"),
            "updated_at": now_iso(),
        }
        self.firebase.set(
            f"{cfg.TABLE_OPERATOR_ACTION}/{safe_firebase_key(request_id)}",
            record,
        )

    def handle_robot_presence_request(self, payload: Mapping[str, Any]) -> None:
        """Remove a robot from planning only after physical removal is confirmed.

        A fresh moving status is a hard rejection. Once accepted, the robot is
        excluded from occupancy, reservations, dependency gates, watchdog
        faults, and automatic dispatch. Last observations remain available for
        audit. The override resets to ``present`` on middleware restart.
        """
        request_id = str(payload.get("request_id") or make_id("PRESENCE"))
        logical_id = cfg.normalize_robot_id(
            payload.get("logical_robot_id") or payload.get("robot_id")
        )
        action = str(payload.get("action") or "").strip().lower()
        request = {
            "request_id": request_id,
            "action": action,
            "robot_id": logical_id,
            "issued_by": str(payload.get("issued_by") or "unknown"),
            "operator_confirmed_physical_removal": (
                payload.get("operator_confirmed_physical_removal") is True
            ),
            "timestamp": str(payload.get("timestamp") or now_iso()),
        }

        def reject(reject_reason: str) -> None:
            try:
                self._record_operator_action(
                    request, status="rejected", reason=reject_reason
                )
            except Exception as exc:
                print(f"[WARN] presence rejection DB log failed: {exc!r}")
            self._publish_robot_presence_ack(
                request, status="rejected", reason=reject_reason
            )
            print(
                f"[PRESENCE] rejected robot={logical_id} request={request_id} "
                f"reason={reject_reason}"
            )

        if action != "mark_removed":
            reject("unsupported_action")
            return
        if logical_id not in self.missions:
            reject("unknown_robot")
            return
        if not request["operator_confirmed_physical_removal"]:
            reject("physical_removal_not_confirmed")
            return

        state = self.robot_state(logical_id)
        if state and status_is_fresh(state) and status_is_moving(state):
            reject("fresh_status_reports_robot_moving")
            return

        newly_removed = False
        released_node: Optional[str] = None
        with self.lock:
            if self.robot_is_removed(logical_id):
                status = "noop"
                reason = "already_marked_removed"
            else:
                mission = self.missions[logical_id]
                previous_phase = mission.phase
                last_node = state_current_node(state)
                latched = copy.deepcopy(self.latched_occupancies.get(logical_id))
                if latched and cfg.is_valid_node(latched.get("node")):
                    released_node = cfg.normalize_node(latched.get("node"))

                self.clear_pending_handshake(mission)
                mission.phase = PHASE_OUT_OF_SERVICE
                mission.phase_reason = "operator_confirmed_physical_removal"
                mission.last_transition_at = now_iso()
                mission.last_error = None
                mission.fault_stop_sent = False

                self.track_presence[logical_id] = {
                    "robot_id": logical_id,
                    "state": "removed",
                    "source": "operator_confirmation",
                    "request_id": request_id,
                    "issued_by": request["issued_by"],
                    "last_reported_node": last_node or None,
                    "previous_mission_phase": previous_phase,
                    "updated_at": now_iso(),
                }
                self.release_latched_occupancy(
                    logical_id,
                    reason=f"operator_confirmed_physical_removal:{request_id}",
                )
                # Progress facts owned by the removed robot must not hold another
                # robot indefinitely. Dependencies on a removed robot are also
                # treated as satisfied by dependency_satisfied().
                for key, record in list(self.dependency_latches.items()):
                    if (
                        cfg.normalize_robot_id(record.get("robot_id")) == logical_id
                        or str(key).startswith(f"{logical_id}:")
                    ):
                        self.dependency_latches.pop(key, None)

                status = "accepted"
                reason = (
                    "removed_from_track_with_fresh_stopped_status"
                    if state and status_is_fresh(state)
                    else "removed_from_track_by_operator_override_without_fresh_status"
                )
                newly_removed = True

        if newly_removed:
            try:
                self.remove_target_reservation(
                    logical_id,
                    reason=f"operator_confirmed_physical_removal:{request_id}",
                )
            except Exception as exc:
                print(f"[WARN] target reservation release failed: {exc!r}")
            try:
                self.release_obstacle_hold(
                    logical_id,
                    reason=f"operator_confirmed_physical_removal:{request_id}",
                )
            except Exception as exc:
                print(f"[WARN] obstacle hold release failed: {exc!r}")

        # ACK first so an unavailable Firebase service cannot leave the GUI
        # waiting even though the in-memory safety state was already committed.
        self._publish_robot_presence_ack(
            request,
            status=status,
            reason=reason,
            released_latched_node=released_node,
        )

        for operation_name, operation in (
            (
                "operator_action_log",
                lambda: self._record_operator_action(
                    request,
                    status=status,
                    reason=reason,
                    released_latched_node=released_node,
                ),
            ),
            ("fleet_state", self.persist_fleet_state),
            ("firebase_map", self.save_map_to_firebase),
            (
                "mqtt_map",
                lambda: self.publish_map_snapshot() if self.client is not None else None,
            ),
            (
                "reroute_log",
                lambda: self.record_reroute_log(
                    "robot_removed_from_track",
                    robot_id=logical_id,
                    request_id=request_id,
                    status=status,
                    reason=reason,
                    released_latched_node=released_node,
                ),
            ),
        ):
            try:
                operation()
            except Exception as exc:
                print(f"[WARN] presence {operation_name} failed: {exc!r}")

        print(
            f"[PRESENCE] robot={logical_id} state=removed status={status} "
            f"request={request_id} released_latched_node={released_node}"
        )
        self.evaluate_fleet(
            trigger=f"operator_removed:{logical_id}",
            force_waiting_path=True,
        )

    def update_first_progress_from_status(
        self,
        robot_id: Any,
        state: Mapping[str, Any],
    ) -> bool:
        """Record the first observed physical progress for delay references."""
        logical_id = cfg.normalize_robot_id(robot_id)
        if self.robot_is_removed(logical_id):
            return False
        mission = self.missions.get(logical_id)
        if mission is None or mission.first_progress_monotonic is not None:
            return False

        current_node = state_current_node(state)
        progressed = bool(
            status_is_moving(state)
            or (current_node and current_node != mission.start_node)
            or state_status_text(state) in cfg.ARRIVED_STATUS_VALUES
            or state_mode_text(state) in cfg.ARRIVED_STATUS_VALUES
        )
        if not progressed:
            return False

        try:
            observed_monotonic = float(state.get("cache_updated_monotonic"))
        except (TypeError, ValueError):
            observed_monotonic = time.monotonic()
        mission.first_progress_monotonic = observed_monotonic
        mission.first_progress_at = str(state.get("payload_time") or now_iso())
        print(
            f"[START_DELAY] progress anchor robot={logical_id} "
            f"at={mission.first_progress_at}"
        )
        return True

    def initial_start_delay_status(
        self,
        mission: MissionRuntime,
        *,
        now_monotonic: Optional[float] = None,
    ) -> Dict[str, Any]:
        delay_sec = max(0.0, float(mission.initial_start_delay_sec or 0.0))
        reference_robot = (
            cfg.normalize_robot_id(mission.start_delay_after_robot)
            if mission.start_delay_after_robot
            else None
        )
        result: Dict[str, Any] = {
            "robot_id": mission.robot_id,
            "configured_delay_sec": delay_sec,
            "after_robot": reference_robot,
            "initial_dispatch_started": mission.initial_dispatch_started,
            "ready": True,
            "remaining_sec": 0.0,
            "reason": "disabled_or_initial_dispatch_already_started",
        }
        if delay_sec <= 0.0 or mission.initial_dispatch_started:
            return result

        if reference_robot and self.robot_is_removed(reference_robot):
            result["reason"] = "reference_robot_removed"
            return result

        if reference_robot:
            reference_mission = self.missions.get(reference_robot)
            anchor = (
                reference_mission.first_progress_monotonic
                if reference_mission is not None
                else None
            )
            result["reference_progress_at"] = (
                reference_mission.first_progress_at
                if reference_mission is not None
                else None
            )
            if anchor is None:
                result.update(
                    {
                        "ready": False,
                        "remaining_sec": delay_sec,
                        "reason": f"waiting_for_{reference_robot}_first_progress",
                    }
                )
                return result
        else:
            anchor = self.started_monotonic
            result["reference_progress_at"] = None

        now_value = time.monotonic() if now_monotonic is None else float(now_monotonic)
        elapsed = max(0.0, now_value - float(anchor))
        remaining = max(0.0, delay_sec - elapsed)
        result.update(
            {
                "ready": remaining <= 0.0,
                "elapsed_sec": round(elapsed, 3),
                "remaining_sec": round(remaining, 3),
                "reason": (
                    "delay_elapsed"
                    if remaining <= 0.0
                    else f"waiting_initial_delay_after_{reference_robot or 'middleware_start'}"
                ),
            }
        )
        return result

    def start_delay_snapshot(self) -> Dict[str, Dict[str, Any]]:
        now_value = time.monotonic()
        return {
            robot_id: self.initial_start_delay_status(
                mission, now_monotonic=now_value
            )
            for robot_id, mission in sorted(self.missions.items())
        }

    def update_latched_occupancy_from_status(
        self,
        robot_id: Any,
        state: Mapping[str, Any],
    ) -> None:
        """Release an arrival latch only after a fresh status proves the AGV moved."""
        logical_id = cfg.normalize_robot_id(robot_id)
        if self.robot_is_removed(logical_id):
            return
        latched = self.latched_occupancies.get(logical_id)
        if not latched:
            return
        current_node = state_current_node(state)
        latched_node = cfg.normalize_node(latched.get("node"))
        if current_node and current_node != latched_node and status_is_fresh(state):
            self.release_latched_occupancy(
                logical_id,
                reason=f"fresh_status_moved:{latched_node}->{current_node}",
            )

    def occupancy_record(self, robot_id: Any) -> Optional[Dict[str, Any]]:
        logical_id = cfg.normalize_robot_id(robot_id)
        if self.robot_is_removed(logical_id):
            return None
        state = self.robot_state(logical_id)
        age = status_age_sec(state) if state else None
        fresh = bool(state and status_is_fresh(state))
        latched = self.latched_occupancies.get(logical_id)

        if latched and cfg.is_valid_node(latched.get("node")):
            return {
                "robot_id": logical_id,
                "node": cfg.normalize_node(latched.get("node")),
                "source": "mission_arrived_latch",
                "fresh_status": fresh,
                "status_age_sec": None if age is None else round(age, 3),
                "reason": latched.get("reason"),
                "latched_at": latched.get("latched_at"),
            }

        obstacle_hold = self.obstacle_holds.get(logical_id)
        if obstacle_hold:
            if not obstacle_hold.get("occupies_node", False):
                return None
            if cfg.is_valid_node(obstacle_hold.get("hold_node")):
                return {
                    "robot_id": logical_id,
                    "node": cfg.normalize_node(obstacle_hold.get("hold_node")),
                    "reported_current_node": state_current_node(state),
                    "blocked_edge": obstacle_hold.get("edge_id"),
                    "source": "obstacle_hold",
                    "fresh_status": fresh,
                    "status_age_sec": None if age is None else round(age, 3),
                    "hold_status": obstacle_hold.get("status"),
                    "detected_at": obstacle_hold.get("detected_at"),
                }

        transit = self.active_transit_reservation(logical_id)
        if transit:
            # For planning, release the source node after the AGV reports that it
            # is moving on the configured edge, and reserve the destination node.
            # The reported current node is retained for observability.
            return {
                "robot_id": logical_id,
                "node": transit["to_node"],
                "reported_current_node": transit["from_node"],
                "next_node": transit["to_node"],
                "reserved_edge": transit["edge_id"],
                "source": transit["source"],
                "fresh_status": fresh,
                "status_age_sec": None if age is None else round(age, 3),
            }

        node = state_current_node(state)
        if not node:
            return None
        return {
            "robot_id": logical_id,
            "node": node,
            "source": "fresh_status" if fresh else "last_known_status",
            "fresh_status": fresh,
            "status_age_sec": None if age is None else round(age, 3),
        }

    def occupancy_records_snapshot(self) -> Dict[str, Dict[str, Any]]:
        robot_ids = (
            set(cfg.ROBOT_IDS)
            | set(self.robot_states)
            | set(self.latched_occupancies)
            | set(self.obstacle_holds)
        )
        result: Dict[str, Dict[str, Any]] = {}
        for robot_id in sorted(robot_ids):
            record = self.occupancy_record(robot_id)
            if record:
                result[robot_id] = record
        return result

    def occupied_nodes_for(self, robot_id: Any) -> Set[str]:
        logical_id = cfg.normalize_robot_id(robot_id)
        occupied: Set[str] = set()
        for other_id, record in self.occupancy_records_snapshot().items():
            if other_id == logical_id:
                continue
            node = cfg.normalize_node(record.get("node"))
            if cfg.is_valid_node(node):
                occupied.add(node)
        occupied.update(self.target_reserved_nodes_for(logical_id))
        return occupied

    def occupied_nodes_snapshot(self) -> Dict[str, List[str]]:
        result: Dict[str, List[str]] = {}
        for robot_id, record in self.occupancy_records_snapshot().items():
            node = cfg.normalize_node(record.get("node"))
            if cfg.is_valid_node(node):
                result.setdefault(node, []).append(robot_id)
        for node in result:
            result[node].sort()
        return dict(sorted(result.items()))

    def all_required_statuses_available(self) -> Tuple[bool, Dict[str, str]]:
        issues = {
            robot_id: issue
            for robot_id in cfg.ROBOT_IDS
            if not self.robot_is_removed(robot_id)
            if (issue := self.robot_status_issue(robot_id)) is not None
        }
        if cfg.REQUIRE_ALL_ROBOT_STATUS_BEFORE_AUTOSTART and issues:
            return False, issues
        return True, issues

    def dependency_satisfied(self, dependency: Mapping[str, Any]) -> bool:
        dependency_robot = cfg.normalize_robot_id(dependency.get("robot_id"))
        if self.robot_is_removed(dependency_robot):
            return True
        mission = self.missions.get(dependency_robot)
        state = self.robot_state(dependency_robot)
        condition = str(dependency.get("condition") or "arrived").strip().lower()
        if mission is None:
            return False
        if condition == "arrived":
            return self.mission_has_operational_goal(mission) and (
                mission.phase == PHASE_ARRIVED
                or (status_is_fresh(state) and status_is_arrived(state, mission.target_node))
            )
        if condition == "at_node":
            return status_is_fresh(state) and (
                state_current_node(state) == cfg.normalize_node(dependency.get("node"))
            )
        if condition == "phase":
            expected = dependency.get("phase")
            if isinstance(expected, Sequence) and not isinstance(expected, str):
                return mission.phase in {str(item) for item in expected}
            return mission.phase == str(expected)
        if condition == "entered_edge":
            if bool(dependency.get("latch", True)):
                return self.dependency_latch_key(dependency) in self.dependency_latches
            return self.status_matches_entered_edge(state, dependency)
        return False

    def dependencies_satisfied(self, mission: MissionRuntime) -> bool:
        if self.robot_is_removed(mission.robot_id):
            return True
        return all(
            (not self.dependency_applies_to_mission(mission, item))
            or self.dependency_satisfied(item)
            for item in mission.dependencies
        )

    def dependency_barrier_edges(self, mission: MissionRuntime) -> Set[str]:
        """Treat unsatisfied edge dependencies as a temporary route barrier."""
        barriers: Set[str] = set()
        for dependency in mission.dependencies:
            condition = str(dependency.get("condition") or "").strip().lower()
            if condition != "entered_edge":
                continue
            if not self.dependency_applies_to_mission(mission, dependency):
                continue
            if self.dependency_satisfied(dependency):
                continue
            from_node = cfg.normalize_node(
                dependency.get("from") or dependency.get("from_node")
            )
            to_node = cfg.normalize_node(
                dependency.get("to") or dependency.get("to_node")
            )
            edge_id = cfg.canonical_edge_id(from_node, to_node, self.map_edges)
            if edge_id:
                barriers.add(edge_id)
        return barriers

    def nominal_shortest_route(self, start: Any, target: Any) -> List[str]:
        """Shortest route over configured topology, ignoring temporary blockers."""
        start_node = cfg.normalize_node(start)
        target_node = cfg.normalize_node(target)
        if not cfg.is_valid_node(start_node) or not cfg.is_valid_node(target_node):
            return []
        adjacency: Dict[str, List[Tuple[str, float]]] = {
            node: [] for node in cfg.VALID_NODES
        }
        for edge in self.map_edges.values():
            source = cfg.normalize_node(edge.get("from"))
            destination = cfg.normalize_node(edge.get("to"))
            if not cfg.is_valid_node(source) or not cfg.is_valid_node(destination):
                continue
            try:
                cost = float(edge.get("cost", 1.0))
            except (TypeError, ValueError):
                continue
            if cost <= 0:
                continue
            adjacency[source].append((destination, cost))
            if bool(edge.get("bidirectional", False)):
                adjacency[destination].append((source, cost))

        queue: List[Tuple[float, int, Tuple[str, ...], str]] = [
            (0.0, 0, (start_node,), start_node)
        ]
        best: Dict[str, Tuple[float, int]] = {start_node: (0.0, 0)}
        while queue:
            cost_so_far, hops, path, current = heapq.heappop(queue)
            recorded = best.get(current)
            if recorded is not None and (cost_so_far, hops) > recorded:
                continue
            if current == target_node:
                return list(path)
            for neighbor, edge_cost in adjacency.get(current, []):
                if neighbor in path:
                    continue
                new_cost = cost_so_far + edge_cost
                new_hops = hops + 1
                previous = best.get(neighbor)
                if previous is not None and (new_cost, new_hops) >= previous:
                    continue
                best[neighbor] = (new_cost, new_hops)
                heapq.heappush(queue, (new_cost, new_hops, (*path, neighbor), neighbor))
        return []

    def safe_prefix_plan(
        self,
        robot_id: Any,
        *,
        current_node: str,
        target_node: str,
        reserved_edges: Set[str],
        dependency_barriers: Set[str],
        occupied_nodes: Set[str],
    ) -> Optional[PathPlan]:
        if not getattr(cfg, "ALLOW_SAFE_PREFIX_ROUTE", True):
            return None
        nominal_route = self.nominal_shortest_route(current_node, target_node)
        if len(nominal_route) < 2:
            return None

        blocked_edges = self.blocked_edges()
        blocked_nodes = {
            cfg.normalize_node(item)
            for item in occupied_nodes
            if cfg.is_valid_node(item)
        }
        blocked_nodes.discard(current_node)
        prefix = [nominal_route[0]]
        partial_reason: Optional[str] = None

        for source, destination in zip(nominal_route, nominal_route[1:]):
            edge_id = cfg.canonical_edge_id(source, destination, self.map_edges)
            if edge_id is None:
                partial_reason = f"unknown_edge:{source}->{destination}"
                break
            edge = self.map_edges.get(edge_id, {})
            edge_status = str(edge.get("status", "open")).strip().lower()
            if edge_id in dependency_barriers:
                partial_reason = f"dependency_edge:{edge_id}"
                break
            if edge_id in reserved_edges:
                partial_reason = f"reserved_edge:{edge_id}"
                break
            if edge_id in blocked_edges or edge_status == "blocked":
                partial_reason = f"blocked_edge:{edge_id}"
                break
            if destination in blocked_nodes:
                partial_reason = f"occupied_node:{destination}"
                break
            prefix.append(destination)

        if partial_reason is None or len(prefix) < 2:
            return None
        try:
            cost = cfg.route_cost(prefix, self.map_edges)
        except Exception:
            cost = float(len(prefix) - 1)
        return PathPlan(
            route=tuple(prefix),
            cost=cost,
            blocked_edges=tuple(sorted(blocked_edges)),
            reserved_edges=tuple(sorted(reserved_edges)),
            occupied_nodes=tuple(sorted(blocked_nodes)),
            is_partial=True,
            final_target=target_node,
            partial_reason=partial_reason,
        )

    # ----- Planning ---------------------------------------------------------------

    def plan_signature(self, robot_id: Any) -> str:
        logical_id = cfg.normalize_robot_id(robot_id)
        mission = self.missions[logical_id]
        current = self.planning_current_node(logical_id)
        return compact_json(
            {
                "current": current,
                "target": mission.target_node,
                "blocked_edges": sorted(self.blocked_edges()),
                "reserved_edges": sorted(self.reserved_edges_for(logical_id)),
                "occupied_nodes": sorted(self.occupied_nodes_for(logical_id)),
            }
        )

    def calculate_plan(self, robot_id: Any) -> Optional[PathPlan]:
        logical_id = cfg.normalize_robot_id(robot_id)
        if self.robot_is_removed(logical_id):
            return None
        mission = self.missions[logical_id]
        current_node = self.planning_current_node(logical_id)
        if not current_node or not self.mission_has_goal(mission):
            return None
        occupied_nodes = self.occupied_nodes_for(logical_id)
        dependency_barriers = self.dependency_barrier_edges(mission)
        reserved_edges = self.reserved_edges_for(logical_id) | dependency_barriers
        if mission.bootstrap_active:
            route = cfg.normalize_route(getattr(cfg, "BOOTSTRAP_ROUTE", []))
            if not route:
                return None
            route = self.route_suffix(route, current_node)
            if len(route) < 2:
                return None
            return PathPlan(
                route=tuple(route),
                cost=cfg.route_cost(route, self.map_edges),
                blocked_edges=tuple(),
                reserved_edges=tuple(),
                occupied_nodes=tuple(),
            )
        prefix_plan = self.safe_prefix_plan(
            logical_id,
            current_node=current_node,
            target_node=mission.target_node,
            reserved_edges=reserved_edges,
            dependency_barriers=dependency_barriers,
            occupied_nodes=occupied_nodes,
        )
        full_plan = GraphRouter.shortest_path(
            self.map_edges,
            current_node,
            mission.target_node,
            blocked_edges=self.blocked_edges(),
            reserved_edges=reserved_edges,
            occupied_nodes=occupied_nodes,
        )
        if (
            prefix_plan is not None
            and getattr(cfg, "PREFER_SAFE_PREFIX_OVER_ALTERNATE", True)
            and (
                full_plan is None
                or str(prefix_plan.partial_reason or "").startswith(
                    ("dependency_edge:", "reserved_edge:", "occupied_node:")
                )
            )
        ):
            return prefix_plan
        return full_plan or prefix_plan

    def route_suffix(self, route: Sequence[str], current_node: str) -> List[str]:
        normalized = cfg.normalize_route(route)
        if current_node in normalized:
            return normalized[normalized.index(current_node) :]
        return normalized

    def route_invalid_reason(self, mission: MissionRuntime) -> Optional[str]:
        state = self.robot_state(mission.robot_id)
        current = self.planning_current_node(mission.robot_id)
        route = self.route_suffix(mission.active_route, current)
        if not self.mission_has_goal(mission):
            return "missing_goal"
        if (
            mission.active_route
            and cfg.normalize_node(mission.active_route[-1]) != mission.target_node
            and not mission.active_route_is_partial
        ):
            return f"goal_changed:{mission.active_route[-1]}->{mission.target_node}"
        if len(route) < 2:
            return None
        try:
            route_edge_ids = cfg.route_edges(route, self.map_edges)
        except ValueError as exc:
            return f"active_route_invalid:{exc}"

        blocked = self.blocked_edges() & set(route_edge_ids)
        if blocked:
            return f"blocked_edge_ahead:{','.join(sorted(blocked))}"

        reserved = self.reserved_edges_for(mission.robot_id) & set(route_edge_ids)
        if reserved:
            return f"reserved_edge_ahead:{','.join(sorted(reserved))}"

        if cfg.REPLAN_MOVING_ON_OCCUPANCY_CHANGE:
            occupied_ahead = self.occupied_nodes_for(mission.robot_id) & set(route[1:])
            if occupied_ahead:
                return f"occupied_node_ahead:{','.join(sorted(occupied_ahead))}"
        return None

    # ----- Outbound payloads ------------------------------------------------------

    def build_command_payload(
        self,
        robot_id: Any,
        command: str,
        *,
        reason: str,
        speed: Optional[float] = None,
        linked_route_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        logical_id = cfg.normalize_robot_id(robot_id)
        wire_id = cfg.wire_robot_id(logical_id)
        payload: Dict[str, Any] = {
            "type": "command",
            "command_id": make_id("CMD"),
            "robot_id": wire_id,
            "logical_robot_id": logical_id,
            "command": command,
            "issued_by": "windows_middleware",
            "reason": reason,
            "timestamp": now_iso(),
        }
        if speed is not None:
            payload["speed"] = speed
        if linked_route_id:
            payload["linked_route_id"] = linked_route_id
        return payload

    def build_route_payload(
        self,
        robot_id: Any,
        route: Sequence[str],
        *,
        reason: str,
        route_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        logical_id = cfg.normalize_robot_id(robot_id)
        wire_id = cfg.wire_robot_id(logical_id)
        normalized_route = cfg.normalize_route(route)
        if not normalized_route:
            raise ValueError("Route must contain at least one valid node")
        path_edges = cfg.route_edges(normalized_route, self.map_edges) if len(normalized_route) >= 2 else []
        rid = route_id or make_id("ROUTE")
        return {
            "type": "reroute",
            "route_id": rid,
            "robot_id": wire_id,
            "target_robot_id": wire_id,
            "logical_robot_id": logical_id,
            "route": normalized_route,
            "new_route": normalized_route,
            "path_edges": path_edges,
            "from_node": normalized_route[0],
            "target_node": normalized_route[-1],
            "reason": reason,
            "ack_required": True,
            "timestamp": now_iso(),
        }

    def publish_command_payload(self, robot_id: Any, payload: Dict[str, Any]) -> bool:
        command_id = str(payload.get("command_id") or make_id("CMD"))
        payload["command_id"] = command_id
        self.firebase.set(
            f"{cfg.TABLE_COMMAND}/{safe_firebase_key(command_id)}",
            payload,
        )
        return self.publish_json(
            cfg.command_topic(robot_id),
            payload,
            qos=cfg.MQTT_CONTROL_QOS,
        )

    def publish_route_payload(self, robot_id: Any, payload: Dict[str, Any]) -> bool:
        route_id = str(payload.get("route_id") or make_id("ROUTE"))
        payload["route_id"] = route_id
        self.firebase.set(
            f"{cfg.TABLE_ROUTE}/{safe_firebase_key(route_id)}",
            {**payload, "status": "sent"},
        )
        return self.publish_json(
            cfg.route_topic(robot_id),
            payload,
            qos=cfg.MQTT_ROUTE_QOS,
        )

    def publish_startup_start(self) -> None:
        if self.startup_start_sent:
            return
        self.startup_start_sent = True
        def publish_one(robot_id: str) -> None:
            payload = self.build_command_payload(
                robot_id,
                "start",
                reason="middleware_startup",
            )
            if not self.publish_command_payload(robot_id, payload):
                print(f"[WARN] startup start publish failed robot={robot_id}")

        for robot_id in cfg.ROBOT_IDS:
            delay_sec = (
                float(cfg.AGV2_START_COMMAND_DELAY_SEC)
                if robot_id == cfg.AGV2_ID
                else 0.0
            )
            if delay_sec > 0.0:
                print(f"[START] delayed startup start robot={robot_id} delay={delay_sec:.1f}s")
                threading.Timer(delay_sec, publish_one, args=(robot_id,)).start()
            else:
                publish_one(robot_id)

    # ----- State transitions ------------------------------------------------------

    def transition(
        self,
        mission: MissionRuntime,
        phase: str,
        reason: str,
        *,
        error: Optional[str] = None,
    ) -> None:
        old_phase = mission.phase
        mission.phase = phase
        mission.phase_reason = reason
        mission.last_transition_at = now_iso()
        if error is not None:
            mission.last_error = error
        print(
            f"[MISSION] robot={mission.robot_id} {old_phase}->{phase} reason={reason}"
            + (f" error={error}" if error else "")
        )
        self.persist_fleet_state()

    def clear_pending_handshake(self, mission: MissionRuntime) -> None:
        mission.pending_route = []
        mission.pending_route_id = None
        mission.pending_route_payload = None
        mission.pending_route_is_partial = False
        mission.pending_route_final_target = None
        mission.pending_route_partial_reason = None
        mission.stop_purpose = None
        mission.stop_command_id = None
        mission.start_command_id = None
        mission.stop_requested_monotonic = None
        mission.route_sent_monotonic = None
        mission.start_sent_monotonic = None
        mission.stop_retries = 0
        mission.route_retries = 0
        mission.start_retries = 0

    def mark_arrived(self, mission: MissionRuntime, *, reason: str) -> None:
        if self.robot_is_removed(mission.robot_id):
            return
        if mission.bootstrap_active:
            self.clear_pending_handshake(mission)
            self.transition(
                mission,
                PHASE_HOLD,
                f"bootstrap_route_ended_without_confirmed_goal:{reason}",
            )
            return
        if not self.mission_has_operational_goal(mission):
            self.update_mission_goal_from_status(
                mission.robot_id,
                self.robot_state(mission.robot_id),
            )
        if not self.mission_has_operational_goal(mission):
            self.transition(mission, PHASE_WAITING_GOAL, f"arrived_without_goal:{reason}")
            return
        mission.initial_dispatch_started = True
        self.latch_occupancy(
            mission.robot_id,
            mission.target_node,
            reason=f"mission_arrived:{reason}",
        )
        self.reserve_target(
            mission,
            route_id=mission.active_route_id,
            status="arrived",
            reason=reason,
        )
        if mission.phase == PHASE_ARRIVED:
            return
        self.clear_pending_handshake(mission)
        mission.completed_at = now_iso()
        mission.last_error = None
        self.transition(mission, PHASE_ARRIVED, reason)
        self.record_reroute_log(
            "mission_arrived",
            robot_id=mission.robot_id,
            target_node=mission.target_node,
            active_route=mission.active_route,
            active_route_id=mission.active_route_id,
            reason=reason,
        )

    def fail_mission(self, mission: MissionRuntime, reason: str) -> None:
        if self.robot_is_removed(mission.robot_id):
            return
        self.clear_pending_handshake(mission)
        self.transition(mission, PHASE_FAULT, reason, error=reason)
        self.record_reroute_log(
            "mission_fault",
            robot_id=mission.robot_id,
            reason=reason,
        )
        if self.client is not None and not mission.fault_stop_sent:
            stop_payload = self.build_command_payload(
                mission.robot_id,
                cfg.LINE_STOP_COMMAND,
                reason=f"fault_{reason}",
                speed=0.0,
            )
            mission.fault_stop_sent = self.publish_command_payload(
                mission.robot_id,
                stop_payload,
            )

    def send_dependency_hold_stop(self, mission: MissionRuntime, *, reason: str) -> bool:
        """Stop a robot that moved before its dependency was released.

        This uses the existing ``line_stop`` command and status confirmation; no
        command ACK or new MQTT message type is introduced.
        """
        if self.robot_is_removed(mission.robot_id):
            return False
        if (
            mission.phase == PHASE_STOPPING
            and mission.stop_purpose == STOP_PURPOSE_DEPENDENCY_HOLD
        ):
            return False

        # Cancel any route/start handshake so a late route_ack cannot release it.
        mission.pending_route = []
        mission.pending_route_id = None
        mission.pending_route_payload = None
        mission.pending_route_is_partial = False
        mission.pending_route_final_target = None
        mission.pending_route_partial_reason = None
        mission.route_sent_monotonic = None
        mission.start_sent_monotonic = None
        mission.start_command_id = None
        mission.route_retries = 0
        mission.start_retries = 0
        mission.pending_reason = reason
        mission.stop_purpose = STOP_PURPOSE_DEPENDENCY_HOLD

        payload = self.build_command_payload(
            mission.robot_id,
            cfg.LINE_STOP_COMMAND,
            reason=reason,
            speed=0.0,
        )
        mission.stop_command_id = str(payload["command_id"])
        mission.stop_requested_monotonic = time.monotonic()
        mission.stop_retries = 0
        if not self.publish_command_payload(mission.robot_id, payload):
            self.fail_mission(mission, f"dependency_stop_publish_failed:{reason}")
            return False
        self.transition(mission, PHASE_STOPPING, reason)

        state = self.robot_state(mission.robot_id)
        if not cfg.REQUIRE_FRESH_STOP_CONFIRMATION and status_is_stopped(state):
            self.on_stop_confirmed(mission)
        return True

    def request_replan(self, robot_id: Any, *, reason: str, force: bool = False) -> bool:
        logical_id = cfg.normalize_robot_id(robot_id)
        mission = self.missions.get(logical_id)
        if (
            mission is None
            or self.robot_is_removed(logical_id)
            or mission.phase in {PHASE_ARRIVED, PHASE_OUT_OF_SERVICE, PHASE_FAULT}
        ):
            return False
        if not self.mission_has_goal(mission):
            self.transition(mission, PHASE_WAITING_GOAL, f"{reason}:missing_goal")
            return False
        if mission.phase == PHASE_WAITING_OBSTACLE_CLEAR:
            return False
        state = self.robot_state(logical_id)
        if not state:
            self.transition(mission, PHASE_WAITING_STATUS, f"{reason}:no_status")
            return False
        if not self.dependencies_satisfied(mission):
            if status_is_moving(state):
                return self.send_dependency_hold_stop(
                    mission,
                    reason=f"{reason}:dependency",
                )
            prefix_plan = self.calculate_plan(logical_id)
            if prefix_plan is None or not prefix_plan.is_partial:
                self.transition(mission, PHASE_WAITING_DEPENDENCY, f"{reason}:dependency")
                return False

        delay_status = self.initial_start_delay_status(mission)
        if (
            not delay_status["ready"]
            and mission.phase not in {PHASE_MOVING, PHASE_STARTING}
        ):
            delay_reason = (
                f"{reason}:start_delay:{delay_status.get('reason')}:"
                f"remaining={delay_status.get('remaining_sec')}s"
            )
            if mission.phase != PHASE_WAITING_START_DELAY:
                self.transition(mission, PHASE_WAITING_START_DELAY, delay_reason)
            else:
                mission.phase_reason = delay_reason
            return False

        # A stale position must never initiate a new dispatch.  An already moving
        # AGV may still be stopped; its fresh post-stop status will gate planning.
        status_issue = self.robot_status_issue(logical_id)
        if status_issue and mission.phase not in {PHASE_MOVING, PHASE_STARTING}:
            self.transition(
                mission,
                PHASE_WAITING_STATUS,
                f"{reason}:status_not_ready:{status_issue}",
            )
            return False
        if mission.phase == PHASE_STOPPING and not force:
            mission.pending_reason = reason
            return False

        mission.pending_reason = reason
        mission.stop_purpose = STOP_PURPOSE_ROUTE_UPDATE
        # Invalidate an outstanding route ACK so a late ACK cannot start the robot.
        mission.pending_route_id = None
        mission.pending_route_payload = None
        mission.pending_route_is_partial = False
        mission.pending_route_final_target = None
        mission.pending_route_partial_reason = None
        mission.route_sent_monotonic = None
        mission.start_sent_monotonic = None
        mission.start_command_id = None
        mission.route_retries = 0
        mission.start_retries = 0

        stop_payload = self.build_command_payload(
            logical_id,
            cfg.LINE_STOP_COMMAND,
            reason=f"prepare_route:{reason}",
            speed=0.0,
        )
        mission.stop_command_id = str(stop_payload["command_id"])
        mission.stop_requested_monotonic = time.monotonic()
        mission.stop_retries = 0
        if not self.publish_command_payload(logical_id, stop_payload):
            self.fail_mission(mission, f"line_stop_publish_failed:{reason}")
            return False
        self.transition(mission, PHASE_STOPPING, reason)

        if not cfg.REQUIRE_FRESH_STOP_CONFIRMATION and status_is_stopped(state):
            self.on_stop_confirmed(mission)
        return True

    def on_stop_confirmed(self, mission: MissionRuntime) -> None:
        if self.robot_is_removed(mission.robot_id):
            return
        stop_purpose = mission.stop_purpose or STOP_PURPOSE_ROUTE_UPDATE
        stop_reason = mission.pending_reason or stop_purpose
        mission.stop_command_id = None
        mission.stop_requested_monotonic = None
        mission.stop_retries = 0
        mission.stop_purpose = None

        if stop_purpose == STOP_PURPOSE_DEPENDENCY_HOLD:
            mission.pending_reason = None
            self.transition(
                mission,
                PHASE_WAITING_DEPENDENCY,
                f"dependency_stop_confirmed:{stop_reason}",
            )
            self.record_reroute_log(
                "dependency_stop_confirmed",
                robot_id=mission.robot_id,
                current_node=state_current_node(self.robot_state(mission.robot_id)),
                reason=stop_reason,
            )
            return

        plan_signature = self.plan_signature(mission.robot_id)
        plan = self.calculate_plan(mission.robot_id)
        mission.last_plan_signature = plan_signature
        mission.last_plan_at_monotonic = time.monotonic()

        if plan is None:
            mission.pending_route = []
            mission.pending_route_id = None
            mission.pending_route_payload = None
            mission.pending_route_is_partial = False
            mission.pending_route_final_target = None
            mission.pending_route_partial_reason = None
            mission.last_plan_cost = None
            reason = mission.pending_reason or "no_path"
            self.transition(mission, PHASE_WAITING_PATH, f"no_path:{reason}")
            self.record_reroute_log(
                "no_path",
                robot_id=mission.robot_id,
                current_node=self.planning_current_node(mission.robot_id),
                target_node=mission.target_node,
                blocked_edges=sorted(self.blocked_edges()),
                reserved_edges=sorted(self.reserved_edges_for(mission.robot_id)),
                occupied_nodes=sorted(self.occupied_nodes_for(mission.robot_id)),
                reason=reason,
            )
            return

        route = list(plan.route)
        if len(route) == 1 and route[0] == mission.target_node:
            self.mark_arrived(mission, reason="stop_confirmed_at_target")
            return

        reason = mission.pending_reason or "route_update"
        route_payload = self.build_route_payload(
            mission.robot_id,
            route,
            reason=reason,
        )
        if plan.is_partial:
            route_payload["route_scope"] = "safe_prefix"
            route_payload["final_target_node"] = plan.final_target or mission.target_node
            route_payload["partial_reason"] = plan.partial_reason
        mission.pending_route = route
        mission.pending_route_id = str(route_payload["route_id"])
        mission.pending_route_payload = copy.deepcopy(route_payload)
        mission.pending_route_is_partial = bool(plan.is_partial)
        mission.pending_route_final_target = plan.final_target
        mission.pending_route_partial_reason = plan.partial_reason
        mission.last_plan_cost = plan.cost
        mission.route_sent_monotonic = time.monotonic()
        mission.route_retries = 0
        if not self.publish_route_payload(mission.robot_id, route_payload):
            self.fail_mission(mission, f"route_publish_failed:{reason}")
            return
        if not plan.is_partial:
            self.reserve_target(
                mission,
                route_id=mission.pending_route_id,
                status="pending_ack",
                reason=reason,
            )
        self.transition(mission, PHASE_WAITING_ROUTE_ACK, reason)
        self.record_reroute_log(
            "route_planned",
            robot_id=mission.robot_id,
            route_id=mission.pending_route_id,
            route=route,
            path_edges=route_payload["path_edges"],
            cost=plan.cost,
            blocked_edges=list(plan.blocked_edges),
            reserved_edges=list(plan.reserved_edges),
            occupied_nodes=list(plan.occupied_nodes),
            is_partial=plan.is_partial,
            final_target=plan.final_target,
            partial_reason=plan.partial_reason,
            reason=reason,
        )

    def start_after_ack(self, mission: MissionRuntime, ack: Mapping[str, Any]) -> None:
        if self.robot_is_removed(mission.robot_id):
            return
        route_id = mission.pending_route_id
        if not route_id:
            return
        mission.active_route = list(mission.pending_route)
        mission.active_route_id = route_id
        mission.active_route_is_partial = mission.pending_route_is_partial
        mission.active_route_final_target = mission.pending_route_final_target
        command_payload = self.build_command_payload(
            mission.robot_id,
            cfg.LINE_START_COMMAND,
            reason=f"route_ack_accepted:{route_id}",
            speed=cfg.DEFAULT_CONTROL_SPEED,
            linked_route_id=route_id,
        )
        mission.start_command_id = str(command_payload["command_id"])
        mission.start_sent_monotonic = time.monotonic()
        mission.start_retries = 0
        if not self.publish_command_payload(mission.robot_id, command_payload):
            self.fail_mission(mission, f"line_start_publish_failed:{route_id}")
            return
        if not mission.active_route_is_partial:
            self.reserve_target(
                mission,
                route_id=route_id,
                status="active",
                reason=f"route_ack:{ack.get('ack_id')}",
            )
        self.transition(mission, PHASE_STARTING, f"route_ack:{ack.get('ack_id')}")
        self.record_reroute_log(
            "route_ack_accepted_start_sent",
            robot_id=mission.robot_id,
            route_id=route_id,
            ack_id=ack.get("ack_id"),
            route=mission.active_route,
            is_partial=mission.active_route_is_partial,
            final_target=mission.active_route_final_target,
            command_id=mission.start_command_id,
        )

    # ----- Automatic fleet evaluation -------------------------------------------

    def evaluate_fleet(self, *, trigger: str, force_waiting_path: bool = False) -> None:
        if not self.auto_start_enabled or self.client is None:
            return
        with self.lock:
            fleet_ready, status_issues = self.all_required_statuses_available()
            if not fleet_ready:
                print(
                    f"[FLEET] automatic dispatch gated by status readiness: "
                    f"{status_issues}; trigger={trigger}"
                )

            for robot_id in cfg.ROBOT_IDS:
                mission = self.missions[robot_id]
                if self.robot_is_removed(robot_id):
                    if mission.phase != PHASE_OUT_OF_SERVICE:
                        mission.phase = PHASE_OUT_OF_SERVICE
                        mission.phase_reason = "track_presence_removed"
                        mission.last_transition_at = now_iso()
                    continue

                state = self.robot_state(robot_id)
                if not state:
                    if mission.phase != PHASE_WAITING_STATUS:
                        self.transition(mission, PHASE_WAITING_STATUS, f"{trigger}:missing_status")
                    continue

                if not self.mission_has_goal(mission):
                    if mission.phase != PHASE_WAITING_GOAL:
                        self.transition(mission, PHASE_WAITING_GOAL, f"{trigger}:missing_goal")
                    continue
                if (
                    mission.robot_id not in self.obstacle_holds
                    and self.mission_has_operational_goal(mission)
                    and status_is_fresh(state)
                    and status_is_arrived(state, mission.target_node)
                ):
                    self.mark_arrived(mission, reason=f"status_arrived:{trigger}")
                    continue
                if mission.phase in {
                    PHASE_ARRIVED,
                    PHASE_OUT_OF_SERVICE,
                    PHASE_FAULT,
                    PHASE_WAITING_OBSTACLE_CLEAR,
                }:
                    continue

                if not self.dependencies_satisfied(mission):
                    dependency_stop_pending = (
                        mission.phase == PHASE_STOPPING
                        and mission.stop_purpose == STOP_PURPOSE_DEPENDENCY_HOLD
                    )
                    if status_is_moving(state):
                        if not dependency_stop_pending:
                            self.send_dependency_hold_stop(
                                mission,
                                reason=f"dependency_not_satisfied:{trigger}",
                            )
                    elif not dependency_stop_pending and mission.phase != PHASE_WAITING_DEPENDENCY:
                        prefix_plan = self.calculate_plan(robot_id)
                        if prefix_plan is None or not prefix_plan.is_partial:
                            # Invalidate any not-yet-started route before entering hold.
                            if mission.phase in {PHASE_WAITING_ROUTE_ACK, PHASE_STARTING}:
                                self.clear_pending_handshake(mission)
                            self.transition(
                                mission,
                                PHASE_WAITING_DEPENDENCY,
                                f"dependency_not_satisfied:{trigger}",
                            )
                            continue
                    elif dependency_stop_pending:
                        continue

                if not mission.auto_start:
                    continue
                if mission.phase in {
                    PHASE_STOPPING,
                    PHASE_WAITING_ROUTE_ACK,
                    PHASE_STARTING,
                }:
                    continue
                if mission.phase == PHASE_MOVING:
                    invalid_reason = self.route_invalid_reason(mission)
                    if invalid_reason:
                        self.request_replan(
                            robot_id,
                            reason=f"{invalid_reason}:{trigger}",
                            force=True,
                        )
                    continue
                if mission.phase == PHASE_HOLD:
                    continue

                delay_status = self.initial_start_delay_status(mission)
                if not delay_status["ready"]:
                    delay_reason = (
                        f"start_delay:{delay_status.get('reason')}:"
                        f"remaining={delay_status.get('remaining_sec')}s:{trigger}"
                    )
                    if mission.phase != PHASE_WAITING_START_DELAY:
                        self.transition(mission, PHASE_WAITING_START_DELAY, delay_reason)
                    else:
                        mission.phase_reason = delay_reason
                    continue

                # Only new dispatch/retry decisions are blocked by fleet freshness.
                own_status_issue = self.robot_status_issue(robot_id)
                if not fleet_ready or own_status_issue:
                    if mission.phase != PHASE_WAITING_STATUS:
                        reason = own_status_issue or "another_robot_status_not_ready"
                        self.transition(
                            mission,
                            PHASE_WAITING_STATUS,
                            f"{trigger}:status_not_ready:{reason}",
                        )
                    continue

                if mission.phase == PHASE_WAITING_PATH:
                    signature = self.plan_signature(robot_id)
                    elapsed = None
                    if mission.last_plan_at_monotonic is not None:
                        elapsed = time.monotonic() - mission.last_plan_at_monotonic
                    if (
                        not force_waiting_path
                        and signature == mission.last_plan_signature
                        and elapsed is not None
                        and elapsed < cfg.WAITING_PATH_RETRY_SEC
                    ):
                        continue

                self.request_replan(
                    robot_id,
                    reason=f"auto_dispatch:{trigger}",
                    force=mission.phase == PHASE_WAITING_PATH,
                )

    # ----- Inbound handlers -------------------------------------------------------

    def handle_status(self, payload: Mapping[str, Any], topic: str) -> None:
        normalized = normalize_status_payload(topic, payload)
        robot_id = normalized["robot_id"]
        if robot_id not in self.missions:
            print(f"[WARN] status from unconfigured robot: {robot_id}")
            return

        goal_changed = False
        goal_requires_replan = False
        with self.lock:
            self.robot_states[robot_id] = normalized
            self.update_first_progress_from_status(robot_id, normalized)
            mission = self.missions[robot_id]

            if self.robot_is_removed(robot_id):
                mission.phase = PHASE_OUT_OF_SERVICE
                mission.phase_reason = "track_presence_removed"
                mission.last_transition_at = now_iso()
            else:
                old_goal = mission.target_node
                was_goal_confirmed = mission.goal_confirmed
                was_goal_confirmed_by_agv = mission.goal_confirmed_by_agv
                was_bootstrap = mission.bootstrap_active
                goal_updated = self.update_mission_goal_from_status(robot_id, normalized)
                goal_changed = bool(
                    goal_updated
                    and was_goal_confirmed
                    and cfg.normalize_node(old_goal) != mission.target_node
                )
                goal_requires_replan = bool(
                    goal_updated
                    and (
                        was_bootstrap
                        or goal_changed
                        or not was_goal_confirmed_by_agv
                    )
                )
                self.update_latched_occupancy_from_status(robot_id, normalized)
                self.update_dependency_latches_from_status(robot_id, normalized)

                obstacle_held = robot_id in self.obstacle_holds
                if (
                    not obstacle_held
                    and self.mission_has_operational_goal(mission)
                    and status_is_arrived(normalized, mission.target_node)
                ):
                    self.mark_arrived(mission, reason="status_arrived")
                elif mission.phase == PHASE_WAITING_OBSTACLE_CLEAR:
                    # A local stop/status update must never auto-release the reporter.
                    pass
                elif mission.phase == PHASE_STOPPING and status_is_stopped(normalized):
                    is_fresh_confirmation = True
                    if cfg.REQUIRE_FRESH_STOP_CONFIRMATION:
                        try:
                            is_fresh_confirmation = (
                                float(normalized["cache_updated_monotonic"])
                                >= float(mission.stop_requested_monotonic or 0.0)
                            )
                        except (TypeError, ValueError):
                            is_fresh_confirmation = False
                    if is_fresh_confirmation:
                        self.on_stop_confirmed(mission)
                elif mission.phase == PHASE_STARTING and status_is_moving(normalized):
                    mission.initial_dispatch_started = True
                    mission.pending_route = []
                    mission.pending_route_id = None
                    mission.pending_route_payload = None
                    mission.pending_route_is_partial = False
                    mission.pending_route_final_target = None
                    mission.pending_route_partial_reason = None
                    mission.start_sent_monotonic = None
                    mission.stop_requested_monotonic = None
                    mission.last_error = None
                    self.release_obstacle_hold(
                        robot_id,
                        reason="line_tracing_confirmed_after_clear",
                    )
                    self.transition(mission, PHASE_MOVING, "line_tracing_confirmed")
                elif mission.phase == PHASE_MOVING and status_is_moving(normalized):
                    mission.initial_dispatch_started = True
                elif mission.phase == PHASE_MOVING and status_is_stopped(normalized):
                    partial_end = (
                        mission.active_route_is_partial
                        and mission.active_route
                        and state_current_node(normalized)
                        == cfg.normalize_node(mission.active_route[-1])
                    )
                    if partial_end:
                        mission.active_route_is_partial = False
                        mission.active_route_final_target = None
                        self.transition(mission, PHASE_WAITING_PATH, "safe_prefix_completed")
                    else:
                        # Do not blindly restart after a manual/local stop. A matching
                        # obstacle event or explicit map change will request a replan.
                        self.transition(mission, PHASE_HOLD, "unexpected_or_external_stop")

        self.firebase.update(f"{cfg.TABLE_ROBOTS}/{safe_firebase_key(robot_id)}", normalized)
        self.firebase.update(f"{cfg.TABLE_SENSING}/{safe_firebase_key(robot_id)}", normalized)
        print(
            f"[STATUS] robot={robot_id} status={normalized.get('status')} mode={normalized.get('mode')} "
            f"node={normalized.get('previous_node_normalized')}->"
            f"{normalized.get('current_node_normalized')}->"
            f"{normalized.get('next_node_normalized')} "
            f"goal={self.missions[robot_id].target_node} "
            f"phase={self.missions[robot_id].phase} "
            f"track_state={self.track_presence.get(robot_id, {}).get('state', 'present')}"
        )
        if (
            not self.robot_is_removed(robot_id)
            and goal_requires_replan
            and self.missions[robot_id].phase == PHASE_HOLD
            and robot_id not in self.obstacle_holds
        ):
            self.request_replan(
                robot_id,
                reason=f"goal_confirmed_from_status:{self.missions[robot_id].target_node}",
                force=True,
            )
        self.evaluate_fleet(trigger=f"status:{robot_id}")

    def handle_sensing(self, payload: Mapping[str, Any], topic: str) -> None:
        normalized = normalize_sensing_payload(topic, payload)
        robot_id = normalized["robot_id"]
        self.sensing_states[robot_id] = normalized
        self.firebase.set(
            f"{cfg.TABLE_SENSING_RAW}/{safe_firebase_key(robot_id)}",
            normalized,
        )
        self.firebase.set(
            f"{cfg.TABLE_SENSING}/{safe_firebase_key(robot_id)}/last_sensing",
            normalized,
        )
        print(
            f"[SENSING] robot={robot_id} obstacle={normalized.get('obstacle')} "
            f"distance={normalized.get('distance')} tof={normalized.get('tof_distance_mm')}"
        )

    def handle_event(self, payload: Mapping[str, Any], topic: str) -> None:
        normalized = normalize_event_payload(topic, payload, self.map_edges)
        event_id = str(normalized["event_id"])
        event_type = str(normalized["type"])
        robot_id = cfg.normalize_robot_id(normalized.get("robot_id"))
        edge_id, edge_source, hold_node = self.resolve_event_edge(normalized)

        self.event_states[event_id] = normalized
        self.firebase.set(
            f"{cfg.TABLE_EVENT}/{safe_firebase_key(event_id)}",
            normalized,
        )
        print(
            f"[EVENT] id={event_id} robot={robot_id} type={event_type} "
            f"edge={edge_id} source={edge_source} hold_node={hold_node}"
        )

        is_block_event = event_type in cfg.EDGE_BLOCK_EVENT_TYPES
        is_open_event = event_type in cfg.EDGE_OPEN_EVENT_TYPES
        if not is_block_event and not is_open_event:
            return
        if self.robot_is_removed(robot_id):
            self.record_reroute_log(
                "edge_event_ignored_removed_robot",
                event_id=event_id,
                robot_id=robot_id,
                event_type=event_type,
                edge_id=edge_id,
            )
            print(
                f"[EVENT] ignored map mutation from removed robot={robot_id} "
                f"event={event_id}"
            )
            return

        if not edge_id:
            invalid = {
                **normalized,
                "invalid_reason": (
                    "event_does_not_identify_previous_current_or_known_edge"
                ),
            }
            self.firebase.set(
                f"{cfg.TABLE_INVALID_EVENT}/{safe_firebase_key(event_id)}",
                invalid,
            )
            print(
                "[WARN] edge event ignored because no configured edge was "
                f"identified: {event_id}"
            )
            return

        if is_open_event:
            # Do not open the map immediately.  One clear event starts the
            # stability window; a new block event cancels it.
            record = self.schedule_edge_clear(edge_id, normalized)
            if self.client is not None:
                self.publish_map_snapshot()
            self.record_reroute_log(
                "map_edge_clear_requested",
                event_id=event_id,
                robot_id=robot_id,
                edge_id=edge_id,
                status="clear_pending",
                stable_after_sec=record.get("stable_after_sec"),
            )
            return

        self.cancel_pending_edge_clear(
            edge_id,
            reason=f"new_block_event:{event_id}",
        )
        changed = self.set_edge_status(edge_id, "blocked", event=normalized)

        # The reporting AGV is held at the last safe node.  It is deliberately
        # excluded from the normal reroute loop until the edge is stably clear.
        reporting_hold_registered = self.register_obstacle_hold(
            robot_id,
            edge_id,
            normalized,
            hold_node=hold_node,
        )

        if self.client is not None:
            self.publish_map_snapshot()

        affected_robots: List[str] = []
        with self.lock:
            for mission in self.missions.values():
                if mission.robot_id == robot_id or self.robot_is_removed(mission.robot_id):
                    continue
                if mission.phase in {PHASE_ARRIVED, PHASE_OUT_OF_SERVICE, PHASE_FAULT}:
                    continue
                candidate_route = (
                    mission.pending_route
                    if mission.phase in {PHASE_WAITING_ROUTE_ACK, PHASE_STARTING}
                    else mission.active_route
                )
                if not candidate_route:
                    candidate_route = extract_route(self.robot_state(mission.robot_id))
                current = self.planning_current_node(mission.robot_id)
                candidate_route = self.route_suffix(candidate_route, current)
                try:
                    candidate_edges = set(
                        cfg.route_edges(candidate_route, self.map_edges)
                    )
                except ValueError:
                    candidate_edges = set()
                if edge_id not in candidate_edges:
                    continue
                if self.request_replan(
                    mission.robot_id,
                    reason=f"edge_blocked:{edge_id}:{event_id}",
                    force=True,
                ):
                    affected_robots.append(mission.robot_id)

        self.record_reroute_log(
            "map_edge_status_changed",
            event_id=event_id,
            robot_id=robot_id,
            edge_id=edge_id,
            edge_resolution_source=edge_source,
            hold_node=hold_node,
            status="blocked",
            changed=changed,
            reporting_hold_registered=reporting_hold_registered,
            affected_robots=affected_robots,
        )
        self.evaluate_fleet(
            trigger=f"event:{event_type}:{edge_id}",
            force_waiting_path=True,
        )

    def handle_route_ack(self, payload: Mapping[str, Any], topic: str) -> None:
        normalized = normalize_route_ack_payload(topic, payload)
        ack_id = str(normalized["ack_id"])
        robot_id = normalized["robot_id"]
        route_id = normalized.get("normalized_route_id")
        status = str(normalized.get("status") or "").strip().lower()
        self.route_ack_states[ack_id] = normalized
        self.firebase.set(
            f"{cfg.TABLE_ROUTE_ACK}/{safe_firebase_key(ack_id)}",
            normalized,
        )
        if route_id:
            self.firebase.update(
                f"{cfg.TABLE_ROUTE}/{safe_firebase_key(route_id)}",
                {
                    "status": status or "ack_received",
                    "ack_id": ack_id,
                    "ack_robot_id": robot_id,
                    "ack_reason": normalized.get("reason"),
                    "ack_at": normalized.get("payload_time") or now_iso(),
                },
            )

        with self.lock:
            mission = self.missions.get(robot_id)
            if mission is None:
                print(f"[WARN] route_ack from unconfigured robot: {robot_id}")
                return
            if self.robot_is_removed(robot_id):
                print(
                    f"[ROUTE_ACK] ignored for removed robot={robot_id} ack={ack_id}"
                )
                return
            matches = (
                mission.phase == PHASE_WAITING_ROUTE_ACK
                and mission.pending_route_id is not None
                and str(route_id or "") == str(mission.pending_route_id)
            )
            if not matches:
                print(
                    f"[ROUTE_ACK] ignored stale/unmatched ack={ack_id} robot={robot_id} "
                    f"route_id={route_id} pending={mission.pending_route_id} phase={mission.phase}"
                )
                return

            if status in cfg.ROUTE_ACK_ACCEPTED_STATUSES:
                self.start_after_ack(mission, normalized)
            elif status in cfg.ROUTE_ACK_REJECTED_STATUSES:
                self.fail_mission(
                    mission,
                    f"route_ack_rejected:{status}:{normalized.get('reason')}",
                )
            else:
                print(
                    f"[ROUTE_ACK] unknown status retained until timeout: ack={ack_id} status={status!r}"
                )

        print(
            f"[ROUTE_ACK] ack={ack_id} robot={robot_id} route_id={route_id} status={status}"
        )

    # ----- Map change / timeout safety -------------------------------------------

    def check_timeouts(self) -> None:
        now = time.monotonic()
        self.process_pending_edge_clears(now_monotonic=now)
        if self.client is None:
            return
        should_retry_waiting_path = False
        should_evaluate_start_delay = False

        with self.lock:
            for mission in self.missions.values():
                if self.robot_is_removed(mission.robot_id):
                    continue
                state = self.robot_state(mission.robot_id)
                if mission.phase == PHASE_STOPPING and mission.stop_requested_monotonic is not None:
                    if now - mission.stop_requested_monotonic > cfg.STOP_CONFIRM_TIMEOUT_SEC:
                        if mission.stop_retries < cfg.MAX_CONTROL_RETRIES:
                            mission.stop_retries += 1
                            payload = self.build_command_payload(
                                mission.robot_id,
                                cfg.LINE_STOP_COMMAND,
                                reason=(
                                    f"retry_stop:{mission.stop_purpose or STOP_PURPOSE_ROUTE_UPDATE}:"
                                    f"{mission.pending_reason}"
                                ),
                                speed=0.0,
                            )
                            mission.stop_command_id = str(payload["command_id"])
                            mission.stop_requested_monotonic = now
                            self.publish_command_payload(mission.robot_id, payload)
                            print(
                                f"[WATCHDOG] line_stop retry robot={mission.robot_id} "
                                f"count={mission.stop_retries}"
                            )
                        else:
                            timeout_reason = (
                                "dependency_stop_confirmation_timeout"
                                if mission.stop_purpose == STOP_PURPOSE_DEPENDENCY_HOLD
                                else "stop_confirmation_timeout"
                            )
                            self.fail_mission(mission, timeout_reason)

                elif mission.phase == PHASE_WAITING_ROUTE_ACK and mission.route_sent_monotonic is not None:
                    if now - mission.route_sent_monotonic > cfg.ROUTE_ACK_TIMEOUT_SEC:
                        if (
                            mission.route_retries < cfg.MAX_CONTROL_RETRIES
                            and mission.pending_route_payload is not None
                        ):
                            mission.route_retries += 1
                            mission.route_sent_monotonic = now
                            self.publish_route_payload(
                                mission.robot_id,
                                copy.deepcopy(mission.pending_route_payload),
                            )
                            print(
                                f"[WATCHDOG] route retry robot={mission.robot_id} "
                                f"route_id={mission.pending_route_id} count={mission.route_retries}"
                            )
                        else:
                            self.fail_mission(mission, "route_ack_timeout")

                elif mission.phase == PHASE_STARTING and mission.start_sent_monotonic is not None:
                    if now - mission.start_sent_monotonic > cfg.START_CONFIRM_TIMEOUT_SEC:
                        if mission.start_retries < cfg.MAX_CONTROL_RETRIES:
                            mission.start_retries += 1
                            payload = self.build_command_payload(
                                mission.robot_id,
                                cfg.LINE_START_COMMAND,
                                reason=f"retry_start:{mission.active_route_id}",
                                speed=cfg.DEFAULT_CONTROL_SPEED,
                                linked_route_id=mission.active_route_id,
                            )
                            mission.start_command_id = str(payload["command_id"])
                            mission.start_sent_monotonic = now
                            self.publish_command_payload(mission.robot_id, payload)
                            print(
                                f"[WATCHDOG] line_start retry robot={mission.robot_id} "
                                f"count={mission.start_retries}"
                            )
                        else:
                            self.fail_mission(mission, "line_tracing_confirmation_timeout")

                elif mission.phase == PHASE_MOVING:
                    age = status_age_sec(state)
                    if age is not None and age > cfg.STATUS_STALE_SEC:
                        self.fail_mission(
                            mission,
                            f"moving_status_stale:{age:.2f}s",
                        )

                elif mission.phase == PHASE_WAITING_PATH:
                    if (
                        mission.last_plan_at_monotonic is None
                        or now - mission.last_plan_at_monotonic >= cfg.WAITING_PATH_RETRY_SEC
                    ):
                        should_retry_waiting_path = True

                elif mission.phase == PHASE_WAITING_START_DELAY:
                    if self.initial_start_delay_status(
                        mission, now_monotonic=now
                    )["ready"]:
                        should_evaluate_start_delay = True

        if should_retry_waiting_path or should_evaluate_start_delay:
            trigger = (
                "watchdog_start_delay_elapsed"
                if should_evaluate_start_delay
                else "watchdog_waiting_path"
            )
            self.evaluate_fleet(trigger=trigger)

    def watchdog_loop(self) -> None:
        print(f"[WATCHDOG] started interval={cfg.WATCHDOG_INTERVAL_SEC}s")
        while not self.watchdog_stop_event.wait(max(0.05, cfg.WATCHDOG_INTERVAL_SEC)):
            try:
                self.check_timeouts()
            except Exception as exc:
                print(f"[WATCHDOG][ERROR] {exc!r}")

    def start_watchdog(self) -> None:
        if not cfg.WATCHDOG_ENABLED or self.watchdog_thread is not None:
            return
        self.watchdog_stop_event.clear()
        self.watchdog_thread = threading.Thread(
            target=self.watchdog_loop,
            name="agv_middleware_watchdog",
            daemon=True,
        )
        self.watchdog_thread.start()

    def stop_watchdog(self) -> None:
        self.watchdog_stop_event.set()
        if self.watchdog_thread and self.watchdog_thread.is_alive():
            self.watchdog_thread.join(timeout=1.0)
        self.watchdog_thread = None

    # ----- MQTT callbacks ---------------------------------------------------------

    def on_connect(self, client: Any, reason_code: Any) -> None:
        if not mqtt_reason_ok(reason_code):
            print(f"[MQTT] connection failed reason={reason_code}")
            return
        print(f"[MQTT] connected {cfg.MQTT_BROKER_HOST}:{cfg.MQTT_BROKER_PORT}")
        for topic in cfg.DEFAULT_SUBSCRIBE_TOPICS:
            qos = (
                cfg.MQTT_OPERATOR_QOS
                if topic == cfg.ROBOT_PRESENCE_REQUEST_TOPIC
                else cfg.MQTT_STATUS_QOS
            )
            client.subscribe(topic, qos=qos)
        print(f"[MQTT] subscribed={cfg.DEFAULT_SUBSCRIBE_TOPICS}")
        if self.publish_map_on_connect:
            self.publish_map_snapshot()
        self.publish_startup_start()

    def on_message(self, msg: Any) -> None:
        topic = str(msg.topic)
        if topic == cfg.REQUEST_MAP_TOPIC:
            print(f"[MQTT RX] map request topic={topic}")
            self.publish_map_snapshot()
            return
        payload = decode_json_payload(msg.payload)
        if payload is None:
            return
        print(f"[MQTT RX] topic={topic} payload={compact_json(payload)}")
        if topic == cfg.ROBOT_PRESENCE_REQUEST_TOPIC:
            self.handle_robot_presence_request(payload)
        elif topic.endswith("/status"):
            self.handle_status(payload, topic)
        elif topic.endswith("/sensing"):
            self.handle_sensing(payload, topic)
        elif topic.endswith("/event"):
            self.handle_event(payload, topic)
        elif topic.endswith("/route_ack"):
            self.handle_route_ack(payload, topic)
        else:
            print(f"[WARN] unsupported topic: {topic}")


# ---------------------------------------------------------------------------
# Paho callback wrappers
# ---------------------------------------------------------------------------


def create_mqtt_client(client_id: str) -> Any:
    if mqtt is None:
        raise RuntimeError("paho-mqtt가 설치되어 있지 않습니다: pip install paho-mqtt")
    if hasattr(mqtt, "CallbackAPIVersion"):
        return mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
    return mqtt.Client(client_id=client_id)


def mqtt_on_connect(
    client: Any,
    userdata: Mapping[str, Any],
    flags: Any,
    reason_code: Any,
    properties: Any = None,
) -> None:
    app: MiddlewareApp = userdata["app"]
    app.on_connect(client, reason_code)


def mqtt_on_disconnect(client: Any, userdata: Mapping[str, Any], *args: Any) -> None:
    print(f"[MQTT] disconnected args={args}")


def mqtt_on_message(client: Any, userdata: Mapping[str, Any], msg: Any) -> None:
    app: MiddlewareApp = userdata["app"]
    try:
        app.on_message(msg)
    except Exception as exc:
        print(f"[ERROR] message handling failed topic={getattr(msg, 'topic', '?')} error={exc!r}")


# ---------------------------------------------------------------------------
# Offline checks and self-tests
# ---------------------------------------------------------------------------


class _DummyPublishResult:
    def __init__(self, rc: int = 0) -> None:
        self.rc = rc


class RecordingMqttClient:
    def __init__(self) -> None:
        self.published: List[Tuple[str, Dict[str, Any], int, bool]] = []

    def publish(self, topic: str, payload: str, qos: int = 0, retain: bool = False) -> _DummyPublishResult:
        parsed = json.loads(payload)
        self.published.append((topic, parsed, qos, retain))
        return _DummyPublishResult(0)

    def subscribe(self, topic: str, qos: int = 0) -> Tuple[int, int]:
        return (0, 1)


def sample_status(
    robot_id: str,
    current_node: str,
    next_node: str,
    *,
    status: str,
    moving: bool,
    route: Optional[Sequence[str]] = None,
    goal: Optional[str] = None,
    previous_node: Optional[str] = None,
) -> Dict[str, Any]:
    selected_route = list(route or cfg.get_reference_route(robot_id))
    route_index = selected_route.index(current_node) if current_node in selected_route else 0
    inferred_previous = previous_node
    if inferred_previous is None and current_node in selected_route and route_index > 0:
        inferred_previous = selected_route[route_index - 1]
    selected_goal = cfg.normalize_node(
        goal or cfg.DEFAULT_TARGET_NODES.get(cfg.normalize_robot_id(robot_id), "")
    )
    return {
        "type": "status",
        "robot_id": cfg.wire_robot_id(robot_id),
        "status": status,
        "mode": "line_tracing" if moving else "idle",
        "robot_run": moving,
        "robot_pause": not moving,
        "current_route": selected_route,
        "route_index": route_index,
        "previous_node": inferred_previous or "",
        "current_node": current_node,
        "next_node": next_node,
        "goal_node": selected_goal,
        "updated_at": now_iso(),
    }


def run_bootstrap_goal_self_test() -> Dict[str, Any]:
    """Verify provisional PURPLE acquisition followed by real-goal replanning."""
    original_require_goal = cfg.REQUIRE_GOAL_IN_STATUS
    original_bootstrap_enabled = cfg.BOOTSTRAP_ROUTE_BEFORE_GOAL
    original_bootstrap_start = cfg.BOOTSTRAP_START_NODE
    original_bootstrap_target = cfg.BOOTSTRAP_TARGET_NODE
    original_bootstrap_route = list(cfg.BOOTSTRAP_ROUTE)
    original_confirm_node = cfg.BOOTSTRAP_GOAL_CONFIRM_NODE
    original_require_stopped = cfg.BOOTSTRAP_REQUIRE_STOPPED_CONFIRMATION
    try:
        cfg.REQUIRE_GOAL_IN_STATUS = False
        cfg.BOOTSTRAP_ROUTE_BEFORE_GOAL = True
        cfg.BOOTSTRAP_START_NODE = "PURPLE"
        cfg.BOOTSTRAP_TARGET_NODE = "ORANGE"
        cfg.BOOTSTRAP_ROUTE = ["PURPLE", "ORANGE", "BLUE", "RED", "GREEN"]
        cfg.BOOTSTRAP_GOAL_CONFIRM_NODE = "PURPLE"
        cfg.BOOTSTRAP_REQUIRE_STOPPED_CONFIRMATION = False

        app = MiddlewareApp(
            FirebaseStore(enabled=False, write_enabled=False),
            auto_start_enabled=True,
            publish_map_on_connect=False,
        )
        client = RecordingMqttClient()
        app.attach_client(client)

        def startup_status(robot_id: str, status: str, *, moving: bool) -> Dict[str, Any]:
            return {
                "type": "status",
                "robot_id": cfg.wire_robot_id(robot_id),
                "status": status,
                "mode": "line_tracing" if moving else "idle",
                "robot_run": moving,
                "robot_pause": not moving,
                "previous_node": "",
                "current_node": "",
                "next_node": "",
                "goal_node": "",
                "current_route": [],
                "route_index": 0,
                "updated_at": now_iso(),
            }

        # Both processes are online even though neither camera has seen PURPLE.
        app.handle_status(
            startup_status("AGV2", "idle", moving=False),
            f"agv/{cfg.wire_robot_id('AGV2')}/status",
        )
        app.handle_status(
            startup_status("AGV1", "idle", moving=False),
            f"agv/{cfg.wire_robot_id('AGV1')}/status",
        )
        agv1_commands = _published_payloads(
            client, topic_suffix="/command", robot_id="AGV1"
        )
        if not agv1_commands or agv1_commands[-1][2].get("command") != "line_stop":
            raise AssertionError("bootstrap dispatch must begin with line_stop")

        # A fresh stopped heartbeat confirms the AGV can accept the provisional route.
        app.handle_status(
            startup_status("AGV1", "stopped", moving=False),
            f"agv/{cfg.wire_robot_id('AGV1')}/status",
        )
        bootstrap_routes = _published_payloads(
            client, topic_suffix="/route", robot_id="AGV1"
        )
        if not bootstrap_routes:
            raise AssertionError("bootstrap route was not published")
        bootstrap_payload = bootstrap_routes[-1][2]
        if bootstrap_payload.get("route") != cfg.BOOTSTRAP_ROUTE:
            raise AssertionError(f"bootstrap route mismatch: {bootstrap_payload}")
        if app.target_reservations_snapshot():
            raise AssertionError("bootstrap route must not reserve a mission target")

        app.handle_route_ack(
            {
                "type": "route_ack",
                "ack_id": "ACK_BOOTSTRAP_AGV1",
                "robot_id": cfg.wire_robot_id("AGV1"),
                "received_route_id": bootstrap_payload["route_id"],
                "status": "accepted",
                "timestamp": now_iso(),
            },
            f"agv/{cfg.wire_robot_id('AGV1')}/route_ack",
        )
        starts = [
            payload
            for _index, _topic, payload in _published_payloads(
                client, topic_suffix="/command", robot_id="AGV1"
            )
            if payload.get("command") == "line_start"
        ]
        if len(starts) != 1:
            raise AssertionError("bootstrap route_ack must produce one line_start")

        app.handle_status(
            startup_status("AGV1", "line_tracing", moving=True),
            f"agv/{cfg.wire_robot_id('AGV1')}/status",
        )
        if app.missions["AGV1"].phase != PHASE_MOVING:
            raise AssertionError("bootstrap line_start was not confirmed as moving")

        # At PURPLE the AGV reads the real goal. It may still report moving;
        # the middleware must then issue line_stop before the final route.
        app.handle_status(
            {
                "type": "status",
                "robot_id": cfg.wire_robot_id("AGV1"),
                "status": "line_tracing",
                "mode": "line_tracing",
                "robot_run": True,
                "robot_pause": False,
                "previous_node": "",
                "current_node": "PURPLE",
                "next_node": "ORANGE",
                "goal_node": "RED",
                "current_route": list(cfg.BOOTSTRAP_ROUTE),
                "route_index": 0,
                "updated_at": now_iso(),
            },
            f"agv/{cfg.wire_robot_id('AGV1')}/status",
        )
        mission = app.missions["AGV1"]
        if mission.bootstrap_active or not mission.goal_confirmed_by_agv:
            raise AssertionError(f"real goal did not complete bootstrap: {mission}")
        if mission.target_node != "RED" or mission.phase != PHASE_STOPPING:
            raise AssertionError(
                f"moving real-goal report must trigger line_stop/replan: "
                f"{mission.phase}, {mission.target_node}"
            )

        # Fresh post-line_stop confirmation produces the final mission route.
        app.handle_status(
            {
                "type": "status",
                "robot_id": cfg.wire_robot_id("AGV1"),
                "status": "stopped",
                "mode": "idle",
                "robot_run": False,
                "robot_pause": True,
                "previous_node": "",
                "current_node": "PURPLE",
                "next_node": "ORANGE",
                "goal_node": "RED",
                "current_route": list(cfg.BOOTSTRAP_ROUTE),
                "route_index": 0,
                "updated_at": now_iso(),
            },
            f"agv/{cfg.wire_robot_id('AGV1')}/status",
        )
        all_routes = _published_payloads(
            client, topic_suffix="/route", robot_id="AGV1"
        )
        final_payload = all_routes[-1][2]
        expected = ["PURPLE", "ORANGE", "BLUE", "GREEN", "RED"]
        if final_payload.get("route") != expected:
            raise AssertionError(f"final mission route mismatch: {final_payload}")

        # Scenario 1 must also survive AGV2 learning its real goal after AGV1
        # already entered BLUE->GREEN.  The progress fact is latched while AGV2
        # is still in bootstrap, but remains inert until its real goals qualify it.
        late_app = MiddlewareApp(
            FirebaseStore(enabled=False, write_enabled=False),
            auto_start_enabled=False,
            publish_map_on_connect=False,
        )
        late_app.handle_status(
            startup_status("AGV2", "idle", moving=False),
            f"agv/{cfg.wire_robot_id('AGV2')}/status",
        )
        late_app.handle_status(
            {
                "type": "status",
                "robot_id": cfg.wire_robot_id("AGV1"),
                "status": "stopped",
                "mode": "idle",
                "robot_run": False,
                "robot_pause": True,
                "previous_node": "",
                "current_node": "PURPLE",
                "next_node": "ORANGE",
                "goal_node": "GREEN",
                "current_route": list(cfg.BOOTSTRAP_ROUTE),
                "route_index": 0,
                "updated_at": now_iso(),
            },
            f"agv/{cfg.wire_robot_id('AGV1')}/status",
        )
        late_app.handle_status(
            sample_status(
                "AGV1",
                "BLUE",
                "GREEN",
                status="line_tracing",
                moving=True,
                route=cfg.REFERENCE_ROUTES[cfg.AGV1_ID],
                goal="GREEN",
            ),
            f"agv/{cfg.wire_robot_id('AGV1')}/status",
        )
        late_latch = "AGV1:BLUE->GREEN"
        if late_latch not in late_app.dependency_latches:
            raise AssertionError(
                "AGV1 edge progress was lost while AGV2 was still bootstrapping"
            )
        late_app.handle_status(
            {
                "type": "status",
                "robot_id": cfg.wire_robot_id("AGV2"),
                "status": "stopped",
                "mode": "idle",
                "robot_run": False,
                "robot_pause": True,
                "previous_node": "",
                "current_node": "PURPLE",
                "next_node": "ORANGE",
                "goal_node": "RED",
                "current_route": list(cfg.BOOTSTRAP_ROUTE),
                "route_index": 0,
                "updated_at": now_iso(),
            },
            f"agv/{cfg.wire_robot_id('AGV2')}/status",
        )
        if not late_app.dependencies_satisfied(late_app.missions["AGV2"]):
            raise AssertionError("late AGV2 goal did not inherit the BLUE->GREEN latch")
        if late_app.reserved_edges_for("AGV2") != {"BLUE-GREEN"}:
            raise AssertionError(
                "late Scenario-1 activation did not expose AGV1 transit reservation"
            )

        return {
            "bootstrap_route": bootstrap_payload["route"],
            "bootstrap_route_id": bootstrap_payload["route_id"],
            "real_goal": mission.target_node,
            "final_route": final_payload["route"],
            "final_phase": mission.phase,
            "bootstrap_target_reserved": False,
            "late_agv2_dependency_latch": late_latch,
        }
    finally:
        cfg.REQUIRE_GOAL_IN_STATUS = original_require_goal
        cfg.BOOTSTRAP_ROUTE_BEFORE_GOAL = original_bootstrap_enabled
        cfg.BOOTSTRAP_START_NODE = original_bootstrap_start
        cfg.BOOTSTRAP_TARGET_NODE = original_bootstrap_target
        cfg.BOOTSTRAP_ROUTE = original_bootstrap_route
        cfg.BOOTSTRAP_GOAL_CONFIRM_NODE = original_confirm_node
        cfg.BOOTSTRAP_REQUIRE_STOPPED_CONFIRMATION = original_require_stopped


def run_graph_self_test() -> Dict[str, Any]:
    edges = cfg.get_default_map_edges()
    normal = GraphRouter.shortest_path(edges, "PURPLE", "RED")
    occupied_green = GraphRouter.shortest_path(
        edges,
        "PURPLE",
        "RED",
        occupied_nodes={"GREEN"},
    )
    blocked_green = GraphRouter.shortest_path(
        edges,
        "PURPLE",
        "RED",
        blocked_edges={"BLUE-GREEN"},
    )
    reserved_green = GraphRouter.shortest_path(
        edges,
        "PURPLE",
        "RED",
        reserved_edges={"BLUE-GREEN"},
    )
    no_path = GraphRouter.shortest_path(
        edges,
        "PURPLE",
        "RED",
        blocked_edges={"BLUE-RED"},
        occupied_nodes={"GREEN"},
    )

    if normal is None or list(normal.route) != cfg.REFERENCE_ROUTES[cfg.AGV2_ID]:
        raise AssertionError(f"normal route mismatch: {normal}")
    if occupied_green is None or list(occupied_green.route) != cfg.EXPECTED_BYPASS_ROUTE:
        raise AssertionError(f"occupied GREEN bypass mismatch: {occupied_green}")
    if blocked_green is None or list(blocked_green.route) != cfg.EXPECTED_BYPASS_ROUTE:
        raise AssertionError(f"blocked BLUE-GREEN bypass mismatch: {blocked_green}")
    if reserved_green is None or list(reserved_green.route) != cfg.EXPECTED_BYPASS_ROUTE:
        raise AssertionError(f"reserved BLUE-GREEN bypass mismatch: {reserved_green}")
    if no_path is not None:
        raise AssertionError(f"expected no path, got: {no_path}")

    return {
        "normal": normal.to_dict(),
        "green_occupied": occupied_green.to_dict(),
        "blue_green_blocked": blocked_green.to_dict(),
        "blue_green_reserved": reserved_green.to_dict(),
        "green_occupied_and_bypass_blocked": None,
    }


def _published_payloads(
    client: RecordingMqttClient,
    *,
    topic_suffix: Optional[str] = None,
    robot_id: Optional[str] = None,
) -> List[Tuple[int, str, Dict[str, Any]]]:
    result: List[Tuple[int, str, Dict[str, Any]]] = []
    wire_id = cfg.wire_robot_id(robot_id) if robot_id else None
    for index, (topic, payload, _qos, _retain) in enumerate(client.published):
        if topic_suffix and not topic.endswith(topic_suffix):
            continue
        if wire_id and f"/{wire_id}/" not in topic:
            continue
        result.append((index, topic, payload))
    return result


def _prepare_operational_self_test(app: MiddlewareApp) -> None:
    """Make non-bootstrap tests independent of REQUIRE_GOAL_IN_STATUS/.env."""
    for mission in app.missions.values():
        mission.bootstrap_active = False
        mission.goal_confirmed = True
        mission.goal_confirmed_by_agv = True
        mission.goal_source = "self_test_operational_goal"
        mission.target_node = mission.default_target_node
        if mission.phase == PHASE_WAITING_GOAL:
            mission.phase = PHASE_WAITING_STATUS
            mission.phase_reason = "self_test_operational_setup"


def run_scenario_self_test() -> Dict[str, Any]:
    firebase = FirebaseStore(enabled=False, write_enabled=False)
    app = MiddlewareApp(
        firebase,
        auto_start_enabled=True,
        publish_map_on_connect=False,
    )
    client = RecordingMqttClient()
    app.attach_client(client)
    _prepare_operational_self_test(app)

    # Both robots are connected and stationary. AGV2 must remain blocked until
    # AGV1 reports that it is moving on BLUE -> GREEN.
    app.handle_status(
        sample_status("AGV2", "PURPLE", "ORANGE", status="idle", moving=False),
        f"agv/{cfg.wire_robot_id('AGV2')}/status",
    )
    app.handle_status(
        sample_status("AGV1", "PURPLE", "ORANGE", status="idle", moving=False),
        f"agv/{cfg.wire_robot_id('AGV1')}/status",
    )

    agv1_commands = _published_payloads(client, topic_suffix="/command", robot_id="AGV1")
    if not agv1_commands or agv1_commands[-1][2].get("command") != "line_stop":
        raise AssertionError("AGV1 must receive line_stop before route")
    if _published_payloads(client, topic_suffix="/route", robot_id="AGV1"):
        raise AssertionError("AGV1 route must wait for a fresh stopped status")
    if _published_payloads(client, topic_suffix="/command", robot_id="AGV2"):
        raise AssertionError("AGV2 must remain blocked before AGV1 enters BLUE-GREEN")

    app.handle_status(
        sample_status("AGV1", "PURPLE", "ORANGE", status="stopped", moving=False),
        f"agv/{cfg.wire_robot_id('AGV1')}/status",
    )
    agv1_routes = _published_payloads(client, topic_suffix="/route", robot_id="AGV1")
    if not agv1_routes:
        raise AssertionError("AGV1 route was not published after stop confirmation")
    agv1_route_payload = agv1_routes[-1][2]
    if agv1_route_payload["route"] != cfg.REFERENCE_ROUTES[cfg.AGV1_ID]:
        raise AssertionError(f"AGV1 route mismatch: {agv1_route_payload['route']}")

    starts_before_ack = [
        item for item in _published_payloads(client, topic_suffix="/command", robot_id="AGV1")
        if item[2].get("command") == "line_start"
    ]
    if starts_before_ack:
        raise AssertionError("line_start was sent before route_ack")

    app.handle_route_ack(
        {
            "type": "route_ack",
            "ack_id": "ACK_AGV1",
            "robot_id": cfg.wire_robot_id("AGV1"),
            "received_route_id": agv1_route_payload["route_id"],
            "status": "accepted",
            "timestamp": now_iso(),
        },
        f"agv/{cfg.wire_robot_id('AGV1')}/route_ack",
    )
    starts_after_ack = [
        item for item in _published_payloads(client, topic_suffix="/command", robot_id="AGV1")
        if item[2].get("command") == "line_start"
    ]
    if len(starts_after_ack) != 1:
        raise AssertionError("accepted AGV1 route_ack must cause exactly one line_start")

    # AGV1 starts moving, but AGV2 must still wait before BLUE-GREEN.
    app.handle_status(
        sample_status("AGV1", "PURPLE", "ORANGE", status="line_tracing", moving=True),
        f"agv/{cfg.wire_robot_id('AGV1')}/status",
    )
    if _published_payloads(client, topic_suffix="/command", robot_id="AGV2"):
        raise AssertionError("AGV2 started before AGV1 reached BLUE-GREEN")

    # This is the release signal: AGV1 has passed BLUE and is heading to GREEN.
    app.handle_status(
        sample_status(
            "AGV1",
            "BLUE",
            "GREEN",
            status="line_tracing",
            moving=True,
            route=cfg.REFERENCE_ROUTES[cfg.AGV1_ID],
        ),
        f"agv/{cfg.wire_robot_id('AGV1')}/status",
    )

    agv1_phase_at_release = app.missions["AGV1"].phase
    if agv1_phase_at_release != PHASE_MOVING:
        raise AssertionError(
            f"AGV1 must still be moving when AGV2 is released: {agv1_phase_at_release}"
        )
    expected_latch = "AGV1:BLUE->GREEN"
    if expected_latch not in app.dependency_latches:
        raise AssertionError(f"BLUE-GREEN dependency was not latched: {app.dependency_latches}")
    if app.reserved_edges_for("AGV2") != {"BLUE-GREEN"}:
        raise AssertionError(
            f"AGV1 transit must reserve BLUE-GREEN: {app.reserved_edges_for('AGV2')}"
        )
    if app.occupied_nodes_for("AGV2") != {"GREEN"}:
        raise AssertionError(
            f"AGV1 transit must reserve GREEN, not BLUE: {app.occupied_nodes_for('AGV2')}"
        )

    configured_delay = float(app.missions["AGV2"].initial_start_delay_sec or 0.0)
    delay_before_elapsed = app.initial_start_delay_status(app.missions["AGV2"])
    agv2_commands = _published_payloads(client, topic_suffix="/command", robot_id="AGV2")
    if configured_delay > 0.0:
        if agv2_commands:
            raise AssertionError("AGV2 began dispatch before its configured start delay")
        anchor = app.missions["AGV1"].first_progress_monotonic
        if anchor is None:
            raise AssertionError("AGV1 progress anchor was not recorded")
        app.missions["AGV1"].first_progress_monotonic = (
            float(anchor) - configured_delay - 0.1
        )
        app.evaluate_fleet(trigger="self_test_start_delay_elapsed")
        agv2_commands = _published_payloads(
            client, topic_suffix="/command", robot_id="AGV2"
        )
    if not agv2_commands or agv2_commands[-1][2].get("command") != "line_stop":
        raise AssertionError(
            "AGV2 must begin dispatch after dependency and start delay are satisfied"
        )

    app.handle_status(
        sample_status("AGV2", "PURPLE", "ORANGE", status="stopped", moving=False),
        f"agv/{cfg.wire_robot_id('AGV2')}/status",
    )
    agv2_routes = _published_payloads(client, topic_suffix="/route", robot_id="AGV2")
    if not agv2_routes:
        raise AssertionError("AGV2 route missing after stop confirmation")
    agv2_route_payload = agv2_routes[-1][2]
    expected_agv2_route = ["PURPLE", "ORANGE", "BLUE", "RED"]
    if agv2_route_payload["route"] != expected_agv2_route:
        raise AssertionError(f"AGV2 bypass route mismatch: {agv2_route_payload['route']}")
    if agv2_route_payload.get("route_scope") == "safe_prefix":
        raise AssertionError(f"AGV2 must prefer complete bypass over safe_prefix: {agv2_route_payload}")

    agv2_starts_before_ack = [
        item for item in _published_payloads(client, topic_suffix="/command", robot_id="AGV2")
        if item[2].get("command") == "line_start"
    ]
    if agv2_starts_before_ack:
        raise AssertionError("AGV2 line_start was sent before route_ack")

    app.handle_route_ack(
        {
            "type": "route_ack",
            "ack_id": "ACK_AGV2",
            "robot_id": cfg.wire_robot_id("AGV2"),
            "received_route_id": agv2_route_payload["route_id"],
            "status": "accepted",
            "timestamp": now_iso(),
        },
        f"agv/{cfg.wire_robot_id('AGV2')}/route_ack",
    )

    # AGV1 may arrive after AGV2 has already been released. Arrival occupancy
    # remains latched at GREEN and AGV2's first route remains a complete bypass.
    app.handle_status(
        sample_status(
            "AGV1",
            "GREEN",
            "DEST",
            status="arrived",
            moving=False,
            route=cfg.REFERENCE_ROUTES[cfg.AGV1_ID],
        ),
        f"agv/{cfg.wire_robot_id('AGV1')}/status",
    )

    for _index, _topic, payload in _published_payloads(client):
        if payload.get("type") == "recovery_reroute":
            raise AssertionError("recovery_reroute must not be emitted")
        if payload.get("command") == "resume":
            raise AssertionError("resume must not be emitted")

    return {
        "release_condition": expected_latch,
        "configured_agv2_initial_start_delay_sec": configured_delay,
        "delay_before_elapsed": delay_before_elapsed,
        "delay_after_elapsed": app.initial_start_delay_status(app.missions["AGV2"]),
        "agv1_phase_at_release": agv1_phase_at_release,
        "agv1_route": agv1_route_payload["route"],
        "agv2_route": agv2_route_payload["route"],
        "agv2_route_scope": agv2_route_payload.get("route_scope"),
        "agv1_final_phase": app.missions["AGV1"].phase,
        "agv2_phase": app.missions["AGV2"].phase,
        "dependency_latches": copy.deepcopy(app.dependency_latches),
        "occupied_nodes": app.occupied_nodes_snapshot(),
        "published_count": len(client.published),
        "commands": [
            payload.get("command")
            for _index, _topic, payload in _published_payloads(client, topic_suffix="/command")
        ],
    }


def run_obstacle_scenario_self_test() -> Dict[str, Any]:
    """Exercise dynamic goals, previous-current blocking, holds, and shared claims."""
    firebase = FirebaseStore(enabled=False, write_enabled=False)
    app = MiddlewareApp(
        firebase,
        auto_start_enabled=False,
        publish_map_on_connect=False,
    )
    client = RecordingMqttClient()
    app.attach_client(client)
    _prepare_operational_self_test(app)

    normal_red_route = ["PURPLE", "ORANGE", "BLUE", "GREEN", "RED"]

    # Confirm both goals from AGV status.  AGV1's configured fallback is GREEN,
    # so this also proves that the runtime goal is truly supplied by the AGV.
    app.handle_status(
        sample_status(
            "AGV1",
            "BLUE",
            "GREEN",
            status="line_tracing",
            moving=True,
            route=normal_red_route,
            goal="RED",
            previous_node="ORANGE",
        ),
        f"agv/{cfg.wire_robot_id('AGV1')}/status",
    )
    app.handle_status(
        sample_status(
            "AGV2",
            "BLUE",
            "GREEN",
            status="line_tracing",
            moving=True,
            route=normal_red_route,
            goal="RED",
            previous_node="ORANGE",
        ),
        f"agv/{cfg.wire_robot_id('AGV2')}/status",
    )
    if app.missions["AGV1"].target_node != "RED":
        raise AssertionError(
            f"AGV1 goal was not updated from status: {app.missions['AGV1'].target_node}"
        )
    # Scenario 1's BLUE->GREEN dependency is goal-qualified.  With both
    # robots targeting RED it must not latch or reserve BLUE-GREEN before the
    # obstacle event; otherwise AGV2 would bypass for the wrong reason.
    if "AGV1:BLUE->GREEN" in app.dependency_latches:
        raise AssertionError(
            "scenario-1 dependency must be inactive when AGV1 goal is RED"
        )
    if not app.dependencies_satisfied(app.missions["AGV2"]):
        raise AssertionError(
            "AGV2 must not wait on the scenario-1 dependency in obstacle mode"
        )
    if app.reserved_edges_for("AGV2"):
        raise AssertionError(
            f"scenario-1 transit reservation leaked into obstacle mode: "
            f"{app.reserved_edges_for('AGV2')}"
        )

    for robot_id in ("AGV1", "AGV2"):
        mission = app.missions[robot_id]
        mission.phase = PHASE_MOVING
        mission.active_route = list(normal_red_route)
        mission.active_route_id = f"ROUTE_PRE_{robot_id}"

    # AGV1 performs its local stop before reporting the event.
    app.handle_status(
        sample_status(
            "AGV1",
            "GREEN",
            "RED",
            status="stopped",
            moving=False,
            route=normal_red_route,
            goal="RED",
            previous_node="BLUE",
        ),
        f"agv/{cfg.wire_robot_id('AGV1')}/status",
    )

    obstacle_event = {
        "type": "obstacle_detected",
        "event_id": "EVT_OBSTACLE_GREEN_RED",
        "robot_id": cfg.wire_robot_id("AGV1"),
        # The AGV protocol defines the blocked segment as previous <-> current.
        "previous_node": "GREEN",
        "current_node": "RED",
        "next_node": "DEST",
        "local_action": "stop",
        "timestamp": now_iso(),
    }
    app.handle_event(
        obstacle_event,
        f"agv/{cfg.wire_robot_id('AGV1')}/event",
    )

    if "GREEN-RED" not in app.blocked_edges():
        raise AssertionError(f"GREEN-RED was not blocked: {app.blocked_edges()}")
    hold = app.obstacle_holds.get("AGV1", {})
    if hold.get("hold_node") != "GREEN":
        raise AssertionError(f"AGV1 safe hold node mismatch: {hold}")
    if app.missions["AGV1"].phase != PHASE_WAITING_OBSTACLE_CLEAR:
        raise AssertionError(
            f"AGV1 must wait for obstacle clear: {app.missions['AGV1'].phase}"
        )
    if app.missions["AGV2"].phase != PHASE_STOPPING:
        raise AssertionError(
            f"AGV2 must stop before reroute: {app.missions['AGV2'].phase}"
        )

    app.handle_status(
        sample_status(
            "AGV2",
            "BLUE",
            "GREEN",
            status="stopped",
            moving=False,
            route=normal_red_route,
            goal="RED",
            previous_node="ORANGE",
        ),
        f"agv/{cfg.wire_robot_id('AGV2')}/status",
    )
    agv2_routes = _published_payloads(
        client,
        topic_suffix="/route",
        robot_id="AGV2",
    )
    if not agv2_routes or agv2_routes[-1][2].get("route") != ["BLUE", "RED"]:
        raise AssertionError(f"AGV2 obstacle bypass mismatch: {agv2_routes}")
    agv2_route = agv2_routes[-1][2]
    app.handle_route_ack(
        {
            "type": "route_ack",
            "ack_id": "ACK_OBSTACLE_AGV2",
            "robot_id": cfg.wire_robot_id("AGV2"),
            "received_route_id": agv2_route["route_id"],
            "status": "accepted",
            "timestamp": now_iso(),
        },
        f"agv/{cfg.wire_robot_id('AGV2')}/route_ack",
    )

    red_claims = app.target_reservations_snapshot().get("RED", {})
    if red_claims.get("owners") != ["AGV2"]:
        raise AssertionError(f"RED must first be claimed by AGV2: {red_claims}")

    clear_event = {
        "type": "obstacle_cleared",
        "event_id": "EVT_CLEAR_GREEN_RED",
        "robot_id": cfg.wire_robot_id("AGV1"),
        "previous_node": "GREEN",
        "current_node": "RED",
        "timestamp": now_iso(),
    }
    app.handle_event(
        clear_event,
        f"agv/{cfg.wire_robot_id('AGV1')}/event",
    )
    if "GREEN-RED" not in app.blocked_edges():
        raise AssertionError("clear event must not open the edge before stabilization")
    if "GREEN-RED" not in app.pending_edge_clears:
        raise AssertionError("clear stabilization timer was not scheduled")

    ready_at = float(app.pending_edge_clears["GREEN-RED"]["ready_monotonic"])
    app.process_pending_edge_clears(now_monotonic=ready_at + 0.001)
    if "GREEN-RED" in app.blocked_edges():
        raise AssertionError("GREEN-RED did not open after the stability window")
    if app.missions["AGV1"].phase != PHASE_STARTING:
        raise AssertionError(
            "AGV1 must receive line_start directly after stable obstacle clear"
        )
    agv1_clear_starts = [
        item for item in _published_payloads(
            client,
            topic_suffix="/command",
            robot_id="AGV1",
        )
        if item[2].get("command") == "line_start"
        and item[2].get("reason") == "obstacle_clear_stable:GREEN-RED"
    ]
    if not agv1_clear_starts:
        raise AssertionError("AGV1 obstacle-clear line_start was not published")
    agv1_routes_after_clear = _published_payloads(
        client,
        topic_suffix="/route",
        robot_id="AGV1",
    )
    if agv1_routes_after_clear:
        raise AssertionError(
            f"AGV1 clear should reuse the active route without republishing: {agv1_routes_after_clear}"
        )

    red_claims = app.target_reservations_snapshot().get("RED", {})
    if red_claims.get("owners") != ["AGV2", "AGV1"]:
        raise AssertionError(
            f"shared reservation mode must retain both RED claims in claim order: {red_claims}"
        )

    app.handle_status(
        sample_status(
            "AGV1",
            "GREEN",
            "RED",
            status="line_tracing",
            moving=True,
            route=["GREEN", "RED"],
            goal="RED",
            previous_node="BLUE",
        ),
        f"agv/{cfg.wire_robot_id('AGV1')}/status",
    )
    if app.missions["AGV1"].phase != PHASE_MOVING:
        raise AssertionError(f"AGV1 did not restart: {app.missions['AGV1'].phase}")
    if "AGV1" in app.obstacle_holds:
        raise AssertionError("AGV1 obstacle hold was not released after moving confirmation")

    return {
        "dynamic_goals": {
            robot_id: app.missions[robot_id].target_node
            for robot_id in ("AGV1", "AGV2")
        },
        "blocked_edge": "GREEN-RED",
        "edge_resolution_source": "previous_current",
        "agv1_hold_phase": PHASE_WAITING_OBSTACLE_CLEAR,
        "agv2_bypass_route": agv2_route["route"],
        "clear_stable_sec": cfg.OBSTACLE_CLEAR_STABLE_SEC,
        "agv1_recovery_route": ["GREEN", "RED"],
        "agv1_final_phase": app.missions["AGV1"].phase,
        "red_reservations": app.target_reservations_snapshot().get("RED"),
        "scenario1_dependency_inactive_for_red_goal": (
            "AGV1:BLUE->GREEN" not in app.dependency_latches
        ),
    }


def _normalized_sample_status(
    robot_id: str,
    current_node: str,
    next_node: str,
    *,
    status: str,
    moving: bool,
    route: Optional[Sequence[str]] = None,
    goal: Optional[str] = None,
    previous_node: Optional[str] = None,
) -> Dict[str, Any]:
    topic = f"agv/{cfg.wire_robot_id(robot_id)}/status"
    return normalize_status_payload(
        topic,
        sample_status(
            robot_id,
            current_node,
            next_node,
            status=status,
            moving=moving,
            route=route,
            goal=goal,
            previous_node=previous_node,
        ),
    )


def run_protocol_self_test() -> Dict[str, Any]:
    """Protect the AGV-facing MQTT schema while internal safety logic evolves."""
    app = MiddlewareApp(
        FirebaseStore(enabled=False, write_enabled=False),
        auto_start_enabled=False,
        publish_map_on_connect=False,
    )
    stop_payload = app.build_command_payload(
        "AGV1",
        cfg.LINE_STOP_COMMAND,
        reason="protocol_self_test_stop",
        speed=0.0,
    )
    start_payload = app.build_command_payload(
        "AGV1",
        cfg.LINE_START_COMMAND,
        reason="protocol_self_test_start",
        speed=cfg.DEFAULT_CONTROL_SPEED,
    )
    route_payload = app.build_route_payload(
        "AGV2",
        cfg.EXPECTED_BYPASS_ROUTE,
        reason="protocol_self_test_route",
        route_id="ROUTE_PROTOCOL_SELF_TEST",
    )
    ack = normalize_route_ack_payload(
        f"agv/{cfg.wire_robot_id('AGV2')}/route_ack",
        {
            "type": "route_ack",
            "ack_id": "ACK_PROTOCOL_SELF_TEST",
            "robot_id": cfg.wire_robot_id("AGV2"),
            "received_route_id": route_payload["route_id"],
            "received_route": route_payload["route"],
            "status": "accepted",
            "reason": "accepted",
            "timestamp": now_iso(),
        },
    )

    expected_topics = {
        "command": f"agv/{cfg.wire_robot_id('AGV1')}/command",
        "route": f"agv/{cfg.wire_robot_id('AGV2')}/route",
        "status_sub": "agv/+/status",
        "route_ack_sub": "agv/+/route_ack",
    }
    actual_topics = {
        "command": cfg.command_topic("AGV1"),
        "route": cfg.route_topic("AGV2"),
        "status_sub": cfg.STATUS_SUB_TOPIC,
        "route_ack_sub": cfg.ROUTE_ACK_SUB_TOPIC,
    }
    if actual_topics != expected_topics:
        raise AssertionError(f"MQTT topic mismatch: {actual_topics} != {expected_topics}")
    if stop_payload.get("command") != "line_stop":
        raise AssertionError(f"stop command mismatch: {stop_payload}")
    if start_payload.get("command") != "line_start":
        raise AssertionError(f"start command mismatch: {start_payload}")
    if route_payload.get("type") != "reroute":
        raise AssertionError(f"route type mismatch: {route_payload}")
    if route_payload.get("route") != route_payload.get("new_route"):
        raise AssertionError("route and new_route must carry the same node array")
    if ack.get("normalized_route_id") != route_payload["route_id"]:
        raise AssertionError(f"received_route_id normalization mismatch: {ack}")
    if ack.get("received_route_normalized") != route_payload["route"]:
        raise AssertionError(f"received_route normalization mismatch: {ack}")

    outbound_payloads = [stop_payload, start_payload, route_payload]
    if any(item.get("command") == "resume" for item in outbound_payloads):
        raise AssertionError("resume is not supported by the current AGV command handler")
    if any(item.get("type") == "recovery_reroute" for item in outbound_payloads):
        raise AssertionError("recovery_reroute must not be emitted")

    return {
        "topics": actual_topics,
        "commands": [stop_payload["command"], start_payload["command"]],
        "route_fields": {
            "type": route_payload["type"],
            "route": route_payload["route"],
            "new_route": route_payload["new_route"],
            "route_id": route_payload["route_id"],
        },
        "ack_route_id": ack["normalized_route_id"],
    }


def run_safety_policy_self_test() -> Dict[str, Any]:
    """Exercise the three revised Windows-only safety decisions offline."""
    original_require_all_status = cfg.REQUIRE_ALL_ROBOT_STATUS_BEFORE_AUTOSTART
    cfg.REQUIRE_ALL_ROBOT_STATUS_BEFORE_AUTOSTART = True
    # 1) DEST or route index alone must not convert a moving/paused AGV to arrived.
    moving_at_target = _normalized_sample_status(
        "AGV1",
        "GREEN",
        "DEST",
        status="line_tracing",
        moving=True,
        route=cfg.REFERENCE_ROUTES[cfg.AGV1_ID],
    )
    paused_at_target = _normalized_sample_status(
        "AGV1",
        "GREEN",
        "DEST",
        status="paused",
        moving=False,
        route=cfg.REFERENCE_ROUTES[cfg.AGV1_ID],
    )
    paused_at_target["mode"] = "paused"
    paused_at_target["robot_pause"] = True
    explicit_arrived = _normalized_sample_status(
        "AGV1",
        "GREEN",
        "DEST",
        status="arrived",
        moving=False,
        route=cfg.REFERENCE_ROUTES[cfg.AGV1_ID],
    )
    idle_fallback = _normalized_sample_status(
        "AGV1",
        "GREEN",
        "DEST",
        status="idle",
        moving=False,
        route=cfg.REFERENCE_ROUTES[cfg.AGV1_ID],
    )
    if status_is_arrived(moving_at_target, "GREEN"):
        raise AssertionError("moving AGV at target/DEST must not be marked arrived")
    if status_is_arrived(paused_at_target, "GREEN"):
        raise AssertionError("paused AGV at target/DEST must not be marked arrived")
    if not status_is_arrived(explicit_arrived, "GREEN"):
        raise AssertionError("explicit arrived status must be accepted")
    if not status_is_arrived(idle_fallback, "GREEN"):
        raise AssertionError("strict terminal idle fallback must remain compatible")

    # 2) A stale observation gates new dispatch, while arrival/last-known occupancy
    # remains represented until a fresh status proves movement.
    stale_app = MiddlewareApp(
        FirebaseStore(enabled=False, write_enabled=False),
        auto_start_enabled=True,
        publish_map_on_connect=False,
    )
    stale_client = RecordingMqttClient()
    stale_app.attach_client(stale_client)
    _prepare_operational_self_test(stale_app)
    stale_app.robot_states["AGV1"] = _normalized_sample_status(
        "AGV1", "PURPLE", "ORANGE", status="idle", moving=False
    )
    stale_app.robot_states["AGV2"] = _normalized_sample_status(
        "AGV2", "PURPLE", "ORANGE", status="idle", moving=False
    )
    stale_app.robot_states["AGV2"]["cache_updated_monotonic"] = (
        time.monotonic() - cfg.STATUS_STALE_SEC - 1.0
    )
    ready, readiness_issues = stale_app.all_required_statuses_available()
    if not str(readiness_issues.get("AGV2", "")).startswith("stale:"):
        raise AssertionError(f"stale AGV2 status must gate dispatch: {readiness_issues}")
    stale_app.evaluate_fleet(trigger="safety_self_test_stale")
    if _published_payloads(stale_client, topic_suffix="/command"):
        raise AssertionError("no new command may be dispatched from stale fleet state")
    if _published_payloads(stale_client, topic_suffix="/route"):
        raise AssertionError("no route may be dispatched from stale fleet state")

    occupancy_app = MiddlewareApp(
        FirebaseStore(enabled=False, write_enabled=False),
        auto_start_enabled=False,
        publish_map_on_connect=False,
    )
    _prepare_operational_self_test(occupancy_app)
    occupancy_app.robot_states["AGV1"] = explicit_arrived
    occupancy_app.mark_arrived(occupancy_app.missions["AGV1"], reason="safety_self_test")
    occupancy_app.robot_states["AGV1"]["cache_updated_monotonic"] = (
        time.monotonic() - max(cfg.STATUS_STALE_SEC, cfg.OCCUPANCY_HOLD_SEC) - 10.0
    )
    if "GREEN" not in occupancy_app.occupied_nodes_for("AGV2"):
        raise AssertionError("arrived GREEN occupancy must survive status/legacy TTL expiry")
    latched_record = occupancy_app.occupancy_records_snapshot().get("AGV1", {})
    if latched_record.get("source") != "mission_arrived_latch":
        raise AssertionError(f"expected arrival occupancy latch: {latched_record}")

    moved_state = _normalized_sample_status(
        "AGV1", "PURPLE", "ORANGE", status="idle", moving=False
    )
    occupancy_app.robot_states["AGV1"] = moved_state
    occupancy_app.update_latched_occupancy_from_status("AGV1", moved_state)
    if "AGV1" in occupancy_app.latched_occupancies:
        raise AssertionError("fresh status at another node must release arrival latch")
    if "PURPLE" not in occupancy_app.occupied_nodes_for("AGV2"):
        raise AssertionError("fresh moved position must replace the released latch")

    # 3) Dependency hold uses line_stop -> fresh stopped status, with watchdog retry.
    dependency_app = MiddlewareApp(
        FirebaseStore(enabled=False, write_enabled=False),
        auto_start_enabled=False,
        publish_map_on_connect=False,
    )
    dependency_client = RecordingMqttClient()
    dependency_app.attach_client(dependency_client)
    _prepare_operational_self_test(dependency_app)
    dependency_app.robot_states["AGV2"] = _normalized_sample_status(
        "AGV2", "PURPLE", "ORANGE", status="line_tracing", moving=True
    )
    dependency_mission = dependency_app.missions["AGV2"]
    if not dependency_app.send_dependency_hold_stop(
        dependency_mission,
        reason="dependency_not_satisfied:safety_self_test",
    ):
        raise AssertionError("dependency line_stop was not published")
    if dependency_mission.phase != PHASE_STOPPING:
        raise AssertionError(f"dependency stop must await confirmation: {dependency_mission.phase}")
    if dependency_mission.stop_purpose != STOP_PURPOSE_DEPENDENCY_HOLD:
        raise AssertionError(f"dependency stop purpose mismatch: {dependency_mission.stop_purpose}")

    dependency_mission.stop_requested_monotonic = (
        time.monotonic() - cfg.STOP_CONFIRM_TIMEOUT_SEC - 0.1
    )
    dependency_app.check_timeouts()
    dep_commands = [
        item[2].get("command")
        for item in _published_payloads(
            dependency_client,
            topic_suffix="/command",
            robot_id="AGV2",
        )
    ]
    if dep_commands != ["line_stop", "line_stop"]:
        raise AssertionError(f"dependency stop must retry with line_stop: {dep_commands}")

    dependency_app.handle_status(
        sample_status(
            "AGV2",
            "PURPLE",
            "ORANGE",
            status="idle",
            moving=False,
        ),
        f"agv/{cfg.wire_robot_id('AGV2')}/status",
    )
    if dependency_mission.phase != PHASE_WAITING_DEPENDENCY:
        raise AssertionError(
            f"fresh stopped status must enter waiting_dependency: {dependency_mission.phase}"
        )
    if dependency_mission.stop_purpose is not None:
        raise AssertionError("confirmed dependency stop must clear internal stop purpose")
    if _published_payloads(dependency_client, topic_suffix="/route", robot_id="AGV2"):
        raise AssertionError("dependency hold must not publish a route")
    if any(command != "line_stop" for command in dep_commands):
        raise AssertionError(f"dependency hold emitted an unsupported command: {dep_commands}")

    result = {
        "arrival": {
            "moving_dest": False,
            "paused_dest": False,
            "explicit_arrived": True,
            "idle_terminal_fallback": True,
        },
        "freshness": {
            "ready": ready,
            "issues": readiness_issues,
        },
        "occupancy": {
            "latched_before_move": latched_record,
            "after_fresh_move": occupancy_app.occupancy_records_snapshot(),
        },
        "dependency_stop": {
            "commands": dep_commands,
            "final_phase": dependency_mission.phase,
            "route_count": len(
                _published_payloads(
                    dependency_client,
                    topic_suffix="/route",
                    robot_id="AGV2",
                )
            ),
        },
    }
    cfg.REQUIRE_ALL_ROBOT_STATUS_BEFORE_AUTOSTART = original_require_all_status
    return result



def run_robot_presence_self_test() -> Dict[str, Any]:
    """Verify explicit physical-removal handling without external services."""
    app = MiddlewareApp(
        FirebaseStore(enabled=False, write_enabled=False),
        auto_start_enabled=False,
        publish_map_on_connect=False,
    )
    client = RecordingMqttClient()
    app.attach_client(client)
    _prepare_operational_self_test(app)

    arrived = _normalized_sample_status(
        "AGV1",
        "GREEN",
        "DEST",
        status="arrived",
        moving=False,
        route=cfg.REFERENCE_ROUTES[cfg.AGV1_ID],
        goal="GREEN",
    )
    app.robot_states["AGV1"] = arrived
    app.missions["AGV1"].goal_confirmed = True
    app.missions["AGV1"].target_node = "GREEN"
    app.mark_arrived(app.missions["AGV1"], reason="presence_self_test")
    if "GREEN" not in app.occupied_nodes_for("AGV2"):
        raise AssertionError("AGV1 must occupy GREEN before operator removal")

    request_id = "PRESENCE_SELF_TEST_ACCEPT"
    app.handle_robot_presence_request({
        "type": "robot_presence_request",
        "request_id": request_id,
        "action": "mark_removed",
        "robot_id": cfg.wire_robot_id("AGV1"),
        "logical_robot_id": "AGV1",
        "operator_confirmed_physical_removal": True,
        "issued_by": "self_test",
        "timestamp": now_iso(),
    })
    if not app.robot_is_removed("AGV1"):
        raise AssertionError("accepted request must mark AGV1 removed")
    if app.missions["AGV1"].phase != PHASE_OUT_OF_SERVICE:
        raise AssertionError("removed AGV mission must be out_of_service")
    if "GREEN" in app.occupied_nodes_for("AGV2"):
        raise AssertionError("removed AGV must no longer occupy GREEN")
    if "AGV1" in app.latched_occupancies:
        raise AssertionError("removed AGV arrival latch must be released")
    if not app.dependencies_satisfied(app.missions["AGV2"]):
        raise AssertionError("dependency on a physically removed AGV must be released")

    accepted_acks = [
        payload
        for topic, payload, _qos, _retain in client.published
        if topic == cfg.ROBOT_PRESENCE_ACK_TOPIC
        and payload.get("request_id") == request_id
    ]
    if not accepted_acks or accepted_acks[-1].get("status") != "accepted":
        raise AssertionError(f"expected accepted presence ACK: {accepted_acks}")

    # A later status from the powered robot must remain observational only and
    # must not recreate occupancy while the operator override is active.
    app.handle_status(
        sample_status(
            "AGV1",
            "GREEN",
            "DEST",
            status="arrived",
            moving=False,
            route=cfg.REFERENCE_ROUTES[cfg.AGV1_ID],
            goal="GREEN",
        ),
        f"agv/{cfg.wire_robot_id('AGV1')}/status",
    )
    if "GREEN" in app.occupied_nodes_for("AGV2"):
        raise AssertionError("removed AGV status must not recreate occupancy")
    if app.missions["AGV1"].phase != PHASE_OUT_OF_SERVICE:
        raise AssertionError("removed AGV status must not reactivate its mission")

    moving_app = MiddlewareApp(
        FirebaseStore(enabled=False, write_enabled=False),
        auto_start_enabled=False,
        publish_map_on_connect=False,
    )
    moving_client = RecordingMqttClient()
    moving_app.attach_client(moving_client)
    _prepare_operational_self_test(moving_app)
    moving_app.robot_states["AGV1"] = _normalized_sample_status(
        "AGV1",
        "BLUE",
        "GREEN",
        status="line_tracing",
        moving=True,
        route=cfg.REFERENCE_ROUTES[cfg.AGV1_ID],
        goal="GREEN",
    )
    reject_id = "PRESENCE_SELF_TEST_REJECT"
    moving_app.handle_robot_presence_request({
        "type": "robot_presence_request",
        "request_id": reject_id,
        "action": "mark_removed",
        "robot_id": cfg.wire_robot_id("AGV1"),
        "logical_robot_id": "AGV1",
        "operator_confirmed_physical_removal": True,
        "issued_by": "self_test",
        "timestamp": now_iso(),
    })
    if moving_app.robot_is_removed("AGV1"):
        raise AssertionError("fresh moving status must reject removal request")
    rejected_acks = [
        payload
        for topic, payload, _qos, _retain in moving_client.published
        if topic == cfg.ROBOT_PRESENCE_ACK_TOPIC
        and payload.get("request_id") == reject_id
    ]
    if not rejected_acks or rejected_acks[-1].get("status") != "rejected":
        raise AssertionError(f"expected rejected presence ACK: {rejected_acks}")

    snapshot = app.map_snapshot()
    return {
        "accepted": accepted_acks[-1],
        "rejected": rejected_acks[-1],
        "track_presence": app.track_presence_snapshot(),
        "map_track_presence": snapshot["meta"]["track_presence"],
        "occupied_nodes_after_removal": app.occupied_nodes_snapshot(),
        "mission_phase": app.missions["AGV1"].phase,
    }

def run_static_check() -> None:
    cfg.validate_settings()
    graph_result = run_graph_self_test()
    print("========== AGV middleware static check ==========")
    print(f"PROJECT_ROOT={cfg.PROJECT_ROOT}")
    print(f"ENV_PATH={cfg.ENV_PATH}")
    print(f"MQTT={cfg.MQTT_BROKER_HOST}:{cfg.MQTT_BROKER_PORT}")
    print(f"ROBOT_IDS={cfg.ROBOT_IDS}")
    print(f"WIRE_IDS={cfg.LOGICAL_TO_WIRE_ID}")
    print(f"MAP_EDGES={list(cfg.DEFAULT_MAP_EDGES.keys())}")
    print(f"MISSIONS={pretty_json(cfg.ROBOT_MISSIONS)}")
    print(f"AGV2_START_DELAY_SEC={cfg.AGV2_START_DELAY_SEC}")
    print(
        "BOOTSTRAP="
        f"enabled={cfg.BOOTSTRAP_ROUTE_BEFORE_GOAL} "
        f"route={cfg.BOOTSTRAP_ROUTE} "
        f"confirm_at={cfg.BOOTSTRAP_GOAL_CONFIRM_NODE}"
    )
    print(f"ROBOT_PRESENCE_REQUEST_TOPIC={cfg.ROBOT_PRESENCE_REQUEST_TOPIC}")
    print("graph scenarios:")
    print(pretty_json(graph_result))
    print(
        "[CHECK] AGV-side requirement: color_thread/update_path must distinguish "
        "BLUE->GREEN from BLUE->RED and select the physical branch."
    )
    print(
        "[CHECK] release signal: fresh moving status current_node=BLUE, "
        "next_node=GREEN. The condition is latched inside Windows only."
    )
    missing = cfg.missing_required_env()
    if missing:
        print(f"[WARN] missing environment values: {missing}")
    print("[OK] static check passed")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_csv_set(value: Optional[str]) -> Set[str]:
    if not value:
        return set()
    return {item.strip().upper() for item in value.split(",") if item.strip()}


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Five-node AGV middleware with BLUE-GREEN release, dynamic rerouting, and route_ack gating"
    )
    parser.add_argument("--check", action="store_true", help="설정과 그래프 경로를 외부 연결 없이 점검")
    parser.add_argument(
        "--bootstrap-self-test",
        action="store_true",
        help="goal 없이 PURPLE 획득용 임시 route/ACK/start 후 실제 goal 재경로를 점검",
    )
    parser.add_argument(
        "--scenario-self-test",
        action="store_true",
        help="AGV1 BLUE-GREEN 진입 후 AGV2 우회 및 ACK 순서를 외부 연결 없이 점검",
    )
    parser.add_argument(
        "--obstacle-self-test",
        action="store_true",
        help="동적 goal, GREEN-RED 장애물 hold, AGV2 BLUE-RED 우회, 안정화 복구를 점검",
    )
    parser.add_argument("--protocol-self-test", action="store_true", help="AGV MQTT topic/payload 호환성을 외부 연결 없이 점검")
    parser.add_argument("--safety-self-test", action="store_true", help="도착·freshness·점유·dependency stop 정책을 외부 연결 없이 점검")
    parser.add_argument("--presence-self-test", action="store_true", help="운영자 물리적 제거 요청과 점유 해제를 외부 연결 없이 점검")
    parser.add_argument("--self-test", action="store_true", help="모든 정적/시나리오/장애물/프로토콜/안전/트랙제거 테스트 실행")
    parser.add_argument("--map-self-test", action="store_true", help="동적 map snapshot을 출력")
    parser.add_argument("--no-firebase", action="store_true", help="Firebase 초기화/읽기/쓰기를 비활성화")
    parser.add_argument("--write-firebase", action="store_true", help="Firebase 쓰기를 활성화")
    parser.add_argument("--init-map", action="store_true", help="기본 5노드 맵을 Firebase에 저장 후 계속 실행")
    parser.add_argument("--init-map-only", action="store_true", help="기본 맵 저장 후 종료")
    parser.add_argument(
        "--publish-map-on-connect",
        action="store_true",
        help="환경 설정과 무관하게 MQTT 연결 직후 map snapshot 발행",
    )
    parser.add_argument("--no-auto-start", action="store_true", help="자동 임무 시작/재경로 판단 비활성화")
    parser.add_argument(
        "--plan",
        nargs=2,
        metavar=("START", "TARGET"),
        help="외부 연결 없이 임의 시작/목적지 경로 계산",
    )
    parser.add_argument("--blocked-edges", default="", help="--plan용 차단 edge CSV")
    parser.add_argument("--occupied-nodes", default="", help="--plan용 점유 node CSV")
    return parser.parse_args(argv)


def run_plan_command(args: argparse.Namespace) -> None:
    plan = GraphRouter.shortest_path(
        cfg.get_default_map_edges(),
        args.plan[0],
        args.plan[1],
        blocked_edges=parse_csv_set(args.blocked_edges),
        occupied_nodes=parse_csv_set(args.occupied_nodes),
    )
    print(pretty_json(plan.to_dict() if plan else {"route": None, "reason": "no_path"}))


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)

    if args.check:
        run_static_check()
        return
    if args.bootstrap_self_test:
        cfg.validate_settings()
        print(pretty_json(run_bootstrap_goal_self_test()))
        print("[OK] bootstrap goal self-test passed")
        return
    if args.scenario_self_test:
        cfg.validate_settings()
        print(pretty_json(run_scenario_self_test()))
        print("[OK] scenario self-test passed")
        return
    if args.obstacle_self_test:
        cfg.validate_settings()
        print(pretty_json(run_obstacle_scenario_self_test()))
        print("[OK] obstacle scenario self-test passed")
        return
    if args.protocol_self_test:
        cfg.validate_settings()
        print(pretty_json(run_protocol_self_test()))
        print("[OK] MQTT protocol self-test passed")
        return
    if args.safety_self_test:
        cfg.validate_settings()
        print(pretty_json(run_safety_policy_self_test()))
        print("[OK] safety policy self-test passed")
        return
    if args.presence_self_test:
        cfg.validate_settings()
        print(pretty_json(run_robot_presence_self_test()))
        print("[OK] robot presence self-test passed")
        return
    if args.self_test:
        run_static_check()
        print(pretty_json(run_bootstrap_goal_self_test()))
        print(pretty_json(run_protocol_self_test()))
        print(pretty_json(run_safety_policy_self_test()))
        print(pretty_json(run_robot_presence_self_test()))
        print(pretty_json(run_scenario_self_test()))
        print(pretty_json(run_obstacle_scenario_self_test()))
        print("[OK] all self-tests passed")
        return
    if args.plan:
        cfg.validate_settings()
        run_plan_command(args)
        return

    cfg.validate_settings()

    if args.map_self_test:
        offline_app = MiddlewareApp(
            FirebaseStore(enabled=False, write_enabled=False),
            auto_start_enabled=False,
            publish_map_on_connect=False,
        )
        print(pretty_json(offline_app.map_snapshot()))
        print("[OK] map snapshot self-test passed")
        return

    firebase = FirebaseStore(
        enabled=not args.no_firebase,
        write_enabled=bool(cfg.FIREBASE_WRITE_ENABLED or args.write_firebase or args.init_map or args.init_map_only),
    )
    firebase.initialize()

    app = MiddlewareApp(
        firebase,
        auto_start_enabled=bool(cfg.AUTO_START_ENABLED and not args.no_auto_start),
        publish_map_on_connect=bool(cfg.PUBLISH_MAP_ON_CONNECT or args.publish_map_on_connect),
    )
    app.load_map_from_firebase()

    if args.init_map or args.init_map_only:
        # Explicit initialization resets the known base edges to configured defaults.
        with app.lock:
            app.map_edges = cfg.get_default_map_edges()
        app.save_map_to_firebase()
        print("[MAP] default five-node map initialization requested")
        if args.init_map_only:
            return

    client_id = cfg.new_client_id()
    client = create_mqtt_client(client_id)
    app.attach_client(client)
    client.user_data_set({"app": app})
    client.on_connect = mqtt_on_connect
    client.on_disconnect = mqtt_on_disconnect
    client.on_message = mqtt_on_message

    print("========== AGV middleware start ==========")
    print(f"MQTT={cfg.MQTT_BROKER_HOST}:{cfg.MQTT_BROKER_PORT}")
    print(f"CLIENT_ID={client_id}")
    print(f"AUTO_START={app.auto_start_enabled}")
    print(f"FIREBASE_WRITE={firebase.write_enabled}")
    print(f"BLOCKED_EDGES_AT_START={sorted(app.blocked_edges())}")

    client.connect(
        cfg.MQTT_BROKER_HOST,
        cfg.MQTT_BROKER_PORT,
        keepalive=cfg.MQTT_KEEPALIVE_SEC,
    )
    app.start_watchdog()
    try:
        client.loop_forever()
    finally:
        app.stop_watchdog()


if __name__ == "__main__":
    main(sys.argv[1:])
