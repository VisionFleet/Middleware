"""
simulator/fake_agv.py - Stage 1 Fake AGV simulator

이 파일은 기존 AGV 노트북(agv2_total_real_final.ipynb)의 MQTT 통신 규격을
흉내 내는 시뮬레이터다. main.py에 맞춘 새 규격을 만들지 않는다.

핵심 호환 규칙
--------------
- topic은 기존 노트북처럼 ROBOT_ID 기준으로 만든다.
  agv/{ROBOT_ID}/command
  agv/{ROBOT_ID}/route
  agv/{ROBOT_ID}/status
  agv/{ROBOT_ID}/sensing
  agv/{ROBOT_ID}/event
  agv/{ROBOT_ID}/route_ack
- status route 필드는 route가 아니라 current_route를 우선 사용한다.
- status/sensing 시간 필드는 updated_at을 사용한다.
- route topic에서는 route 또는 new_route 배열만 읽는다.
- route topic에서 command="go" / command="wait"는 처리하지 않는다.
- 실제 정지/재개는 command topic의 stop / line_stop / line_start / resume 계열로 처리한다.
- route_ack는 received_route_id를 사용한다.

실행 예시
---------
# AGV1, AGV2를 한 프로세스에서 동시에 실행
python simulator/fake_agv.py --both

# 각각 별도 터미널에서 실행
python simulator/fake_agv.py --robot-id AGV1
python simulator/fake_agv.py --robot-id AGV2

# MQTT 없이 payload 형태만 확인
python simulator/fake_agv.py --self-test

# 자동으로 라인트레이싱 시작
python simulator/fake_agv.py --both --auto-start

# Step 10 timeout 테스트: route_ack를 일부러 보내지 않음
python simulator/fake_agv.py --robot-id AGV2 --drop-route-ack

# Step 10 rejected ack 테스트
python simulator/fake_agv.py --robot-id AGV2 --reject-route-ack
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

# setting.py 위치 호환:
# - project-root/simulator/fake_agv.py + project-root/setting.py
# - project-root/simulator/fake_agv.py + project-root/middleware/setting.py
# - main.py와 같은 폴더에서 직접 실행
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
for candidate in (CURRENT_DIR, PROJECT_ROOT, PROJECT_ROOT / "middleware", Path.cwd()):
    if str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

try:
    import setting as cfg  # type: ignore
except Exception:  # pragma: no cover - setting.py 없이 payload self-test를 돕기 위한 fallback
    cfg = None  # type: ignore[assignment]

try:
    import paho.mqtt.client as mqtt
except ImportError:  # --self-test는 paho 없이도 가능하게 둔다.
    mqtt = None  # type: ignore[assignment]

KST = timezone(timedelta(hours=9))

VALID_NODES = set(getattr(cfg, "VALID_NODES", {"RED", "GREEN", "ORANGE", "PURPLE"}))
DEFAULT_ROUTES: Dict[str, List[str]] = {
    "AGV1": list(getattr(cfg, "DEFAULT_ROUTES", {}).get("AGV1", ["PURPLE", "GREEN", "ORANGE"])),
    "AGV2": list(getattr(cfg, "DEFAULT_ROUTES", {}).get("AGV2", ["ORANGE", "GREEN", "RED"])),
}

# 기존 노트북 기본 topic ID와 맞춘다.
LOGICAL_TO_WIRE_ID: Dict[str, str] = {
    "AGV1": os.getenv("AGV1_MQTT_TOPIC_ID") or os.getenv("AGV1_ACTUAL_ID") or "AGV_01",
    "AGV2": os.getenv("AGV2_MQTT_TOPIC_ID") or os.getenv("AGV2_ACTUAL_ID") or "AGV_02",
}
ROBOT_ID_ALIASES: Dict[str, str] = {
    "AGV1": "AGV1",
    "AGV01": "AGV1",
    "AGV_01": "AGV1",
    "1": "AGV1",
    "AGV2": "AGV2",
    "AGV02": "AGV2",
    "AGV_02": "AGV2",
    "2": "AGV2",
}
WIRE_TO_LOGICAL_ID = {wire.upper(): logical for logical, wire in LOGICAL_TO_WIRE_ID.items()}
WIRE_TO_LOGICAL_ID.update(ROBOT_ID_ALIASES)

COMMAND_VALUES = {
    "forward",
    "backward",
    "left",
    "right",
    "stop",
    "emergency_stop",
    "line_start",
    "start_line_tracing",
    "line_stop",
    "stop_line_tracing",
    "resume",
    "resume_line_tracing",
    "clear_obstacle",
}


def now_iso() -> str:
    if cfg is not None and hasattr(cfg, "now_iso"):
        return cfg.now_iso()
    return datetime.now(KST).isoformat(timespec="milliseconds")


def compact_json(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def pretty_json(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)


def normalize_node(value: Any) -> str:
    if value is None:
        return ""
    node = str(value).strip().upper()
    return node if node in VALID_NODES else ""


def normalize_route_for_status(value: Any) -> List[str]:
    """CLI/default route용. 문자열도 편의상 허용한다."""
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip().upper().replace("->", ",").replace(" ", "")
        raw_items: Iterable[Any] = text.split(",") if "," in text else text.split("-")
    elif isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        raw_items = value
    else:
        return []
    route: List[str] = []
    for item in raw_items:
        node = normalize_node(item)
        if node:
            route.append(node)
    return route


def normalize_route_from_mqtt(value: Any) -> List[str]:
    """기존 AGV 노트북의 normalize_route처럼 MQTT route는 list/tuple만 route로 인정한다."""
    if not isinstance(value, (list, tuple)):
        return []
    route: List[str] = []
    for item in value:
        node = normalize_node(item)
        if node:
            route.append(node)
    return route


def normalize_robot_id(value: Any) -> str:
    raw = str(value or "").strip().upper()
    if not raw:
        return "UNKNOWN"
    return WIRE_TO_LOGICAL_ID.get(raw, ROBOT_ID_ALIASES.get(raw, raw))


def wire_robot_id(robot_id: Any) -> str:
    raw = str(robot_id or "").strip()
    logical = normalize_robot_id(raw)
    if logical in LOGICAL_TO_WIRE_ID:
        return LOGICAL_TO_WIRE_ID[logical]
    return raw or logical


def route_for_robot(robot_id: Any, route_override: Optional[str] = None) -> List[str]:
    if route_override:
        route = normalize_route_for_status(route_override)
        if route:
            return route
    logical = normalize_robot_id(robot_id)
    return list(DEFAULT_ROUTES.get(logical, ["ORANGE", "GREEN", "RED"]))


def broker_host() -> str:
    # 기존 노트북과 main.py가 같이 쓰도록 TEMP를 우선한다.
    return (
        os.getenv("MQTT_BROKER_HOST_TEMP")
        or os.getenv("MQTT_BROKER_HOST")
        or str(getattr(cfg, "MQTT_BROKER_HOST", "10.32.240.196"))
    )


def broker_port() -> int:
    return int(os.getenv("MQTT_BROKER_PORT", str(getattr(cfg, "MQTT_BROKER_PORT", 1883))))


def mqtt_reason_ok(reason_code: Any) -> bool:
    try:
        return int(reason_code) == 0
    except Exception:
        return str(reason_code).lower() in {"success", "0"}


def create_mqtt_client(client_id: str) -> Any:
    if mqtt is None:
        raise RuntimeError("paho-mqtt가 설치되어 있지 않습니다. pip install paho-mqtt")
    if hasattr(mqtt, "CallbackAPIVersion"):
        return mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
    return mqtt.Client(client_id=client_id)


def get_ip_address() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


@dataclass
class FakeAgv:
    logical_id: str
    wire_id: str
    route: List[str]
    host: str
    port: int
    status_interval: float = 1.0
    sensing_interval: float = 1.0
    step_interval: float = 2.0
    default_speed: float = 0.30
    auto_start: bool = False
    publish_sensing_enabled: bool = True
    drop_route_ack: bool = False
    reject_route_ack: bool = False

    client: Any = field(default=None, init=False)
    stop_event: threading.Event = field(default_factory=threading.Event, init=False)
    lock: threading.RLock = field(default_factory=threading.RLock, init=False)
    last_status_at: float = field(default=0.0, init=False)
    last_sensing_at: float = field(default=0.0, init=False)
    last_step_at: float = field(default=0.0, init=False)

    def __post_init__(self) -> None:
        if len(self.route) < 1:
            raise ValueError(f"route가 비어 있습니다: {self.route!r}")
        self.command_topic = f"agv/{self.wire_id}/command"
        self.route_topic = f"agv/{self.wire_id}/route"
        self.status_topic = f"agv/{self.wire_id}/status"
        self.sensing_topic = f"agv/{self.wire_id}/sensing"
        self.event_topic = f"agv/{self.wire_id}/event"
        self.route_ack_topic = f"agv/{self.wire_id}/route_ack"

        next_node = self.route[1] if len(self.route) > 1 else "DEST"
        self.runtime: Dict[str, Any] = {
            "mode": "line_tracing" if self.auto_start else "idle",
            "robot_run": bool(self.auto_start),
            "robot_pause": not bool(self.auto_start),
            "mqtt_connected": False,
            "last_manual_command_time": 0.0,
        }
        self.state: Dict[str, Any] = {
            "robot_id": self.wire_id,
            "logical_robot_id": self.logical_id,
            "status": "line_tracing" if self.auto_start else "idle",
            "current_route": self.route[:],
            "route_index": 0,
            "current_node": self.route[0],
            "next_node": next_node,
            "speed": self.default_speed if self.auto_start else 0.0,
            "battery": 100,
            "distance": None,
            "tof_distance_mm": None,
            "obstacle": False,
            "obstacle_source": None,
            "last_command": None,
            "last_route_message": None,
            "heading": "E",
        }

    def update_next_node_locked(self) -> None:
        route = self.state.get("current_route") or []
        idx = int(self.state.get("route_index", 0) or 0)
        if not route:
            self.state["current_node"] = "-"
            self.state["next_node"] = "-"
            self.state["route_index"] = 0
            return
        idx = max(0, min(idx, len(route) - 1))
        self.state["route_index"] = idx
        self.state["current_node"] = route[idx]
        self.state["next_node"] = route[idx + 1] if idx + 1 < len(route) else "DEST"

    def build_status_payload(self) -> Dict[str, Any]:
        with self.lock:
            self.update_next_node_locked()
            snapshot = dict(self.state)
            mode = self.runtime["mode"]
            robot_run = self.runtime["robot_run"]
            robot_pause = self.runtime["robot_pause"]
            mqtt_connected = self.runtime["mqtt_connected"]

        snapshot.update(
            {
                "type": "status",
                "mode": mode,
                "robot_run": robot_run,
                "robot_pause": robot_pause,
                "mqtt_connected": mqtt_connected,
                "det_status": "clear",
                "det_conf": 0.0,
                "det_classes": [],
                "tof_status": "idle",
                "tof_distance_mm": snapshot.get("tof_distance_mm"),
                "ip_address": get_ip_address(),
                "cpu_percent": None,
                "memory_percent": None,
                "temperature_c": None,
                "updated_at": now_iso(),
            }
        )
        return snapshot

    def build_sensing_payload(self) -> Dict[str, Any]:
        with self.lock:
            payload = {
                "type": "sensing",
                "robot_id": self.wire_id,
                "logical_robot_id": self.logical_id,
                "mode": self.runtime["mode"],
                "current_node": self.state["current_node"],
                "next_node": self.state["next_node"],
                "distance": self.state["distance"],
                "tof_distance_mm": self.state.get("tof_distance_mm"),
                "obstacle": self.state["obstacle"],
                "line": {},
                "color": {},
                "detection": {},
                "tof": {},
                "updated_at": now_iso(),
            }
        return payload

    def publish_json(self, topic: str, payload: Dict[str, Any], qos: int = 0) -> bool:
        if self.client is None:
            print(f"[{self.wire_id}] MQTT SKIP topic={topic} payload={compact_json(payload)}")
            return False
        result = self.client.publish(topic, compact_json(payload), qos=qos)
        rc = getattr(result, "rc", result)
        print(f"[{self.wire_id}] MQTT TX topic={topic} rc={rc} type={payload.get('type', '-')}")
        if mqtt is None:
            return False
        return int(rc) == int(mqtt.MQTT_ERR_SUCCESS)

    def publish_status(self) -> None:
        payload = self.build_status_payload()
        self.publish_json(self.status_topic, payload, qos=0)
        print(
            f"[{payload['updated_at']}] STATUS robot={self.wire_id}/{self.logical_id} "
            f"status={payload['status']} mode={payload.get('mode')} "
            f"node={payload['current_node']}->{payload['next_node']} "
            f"idx={payload['route_index']} run={payload['robot_run']} pause={payload['robot_pause']}"
        )

    def publish_sensing(self) -> None:
        payload = self.build_sensing_payload()
        self.publish_json(self.sensing_topic, payload, qos=0)

    def publish_route_ack(self, route_payload: Dict[str, Any], accepted: bool = True, reason: str = "accepted") -> None:
        received_route = route_payload.get("route") or route_payload.get("new_route") or []
        ack = {
            "type": "route_ack",
            "ack_id": f"ACK_{self.wire_id}_{datetime.now(KST).strftime('%Y%m%d_%H%M%S_%f')}",
            "robot_id": self.wire_id,
            "received_type": route_payload.get("type", "-"),
            "received_route_id": route_payload.get("route_id", "-"),
            "received_route": received_route,
            "blocked_edge": route_payload.get("blocked_edge", "-"),
            "status": "accepted" if accepted else "rejected",
            "reason": reason,
            "timestamp": now_iso(),
        }
        self.publish_json(self.route_ack_topic, ack, qos=0)

    def apply_route_to_state(self, new_route: List[str]) -> None:
        with self.lock:
            self.state["current_route"] = new_route[:]
            current_node = self.state.get("current_node")
            if current_node in new_route:
                self.state["route_index"] = new_route.index(current_node)
            else:
                self.state["route_index"] = 0
            self.update_next_node_locked()

    def handle_route(self, data: Dict[str, Any]) -> None:
        # 기존 노트북과 동일하게 route 또는 new_route 배열만 본다.
        new_route = normalize_route_from_mqtt(data.get("route") or data.get("new_route"))
        if not new_route:
            print(f"[{self.wire_id}] ROUTE RX invalid route: {data}")
            self.publish_route_ack(data, accepted=False, reason="invalid_route")
            return

        if self.reject_route_ack:
            print(f"[{self.wire_id}] ROUTE RX forced reject for Step 10 test: {data}")
            if not self.drop_route_ack:
                self.publish_route_ack(data, accepted=False, reason="forced_reject_for_step10_test")
            return

        with self.lock:
            was_line_tracing = self.runtime["mode"] == "line_tracing" and self.runtime["robot_run"]
            self.runtime["robot_pause"] = True
            self.state["last_route_message"] = data
            self.state["status"] = "rerouted"
            self.state["speed"] = 0.0

        self.apply_route_to_state(new_route)
        print(f"[{now_iso()}] ROUTE RX robot={self.wire_id} type={data.get('type')} route={new_route} raw={data}")
        if self.drop_route_ack:
            print(f"[{self.wire_id}] ROUTE_ACK intentionally dropped for Step 10 timeout test")
        else:
            self.publish_route_ack(data, accepted=True)
        self.publish_status()

        # 기존 노트북: line_tracing 중 일반 reroute를 받으면 자동 재개, recovery_reroute는 자동 재개하지 않음.
        with self.lock:
            if was_line_tracing and data.get("type") != "recovery_reroute":
                self.runtime["mode"] = "line_tracing"
                self.runtime["robot_run"] = True
                self.runtime["robot_pause"] = False
                self.state["status"] = "line_tracing"
                self.state["speed"] = self.default_speed
            else:
                self.runtime["robot_pause"] = True
                self.runtime["robot_run"] = False
                self.state["speed"] = 0.0

    def handle_command(self, data: Dict[str, Any]) -> None:
        command = str(data.get("command") or "").strip().lower()
        reason = str(data.get("reason") or data.get("type") or "manual_command")
        try:
            speed = float(data.get("speed", self.default_speed))
        except Exception:
            speed = self.default_speed
        speed = max(0.0, min(1.0, speed))

        if command not in COMMAND_VALUES:
            print(f"[{self.wire_id}] unknown command={command!r} payload={data}")
            return

        with self.lock:
            self.runtime["last_manual_command_time"] = time.time()
            self.state["last_command"] = command

            if command == "emergency_stop":
                self.runtime["mode"] = "emergency"
                self.runtime["robot_run"] = False
                self.runtime["robot_pause"] = True
                self.state["status"] = "emergency"
                self.state["speed"] = 0.0
            elif command in {"stop", "line_stop", "stop_line_tracing"}:
                self.runtime["mode"] = "idle"
                self.runtime["robot_run"] = False
                self.runtime["robot_pause"] = True
                self.state["status"] = "stopped"
                self.state["speed"] = 0.0
            elif command in {"line_start", "start_line_tracing", "resume", "resume_line_tracing"}:
                self.runtime["mode"] = "line_tracing"
                self.runtime["robot_run"] = True
                self.runtime["robot_pause"] = False
                self.state["status"] = "line_tracing"
                self.state["speed"] = speed if speed > 0 else self.default_speed
                self.state["obstacle"] = False
                self.state["obstacle_source"] = None
            elif command == "clear_obstacle":
                self.state["obstacle"] = False
                self.state["obstacle_source"] = None
                self.state["distance"] = None
                self.state["tof_distance_mm"] = None
                self.runtime["mode"] = "idle"
                self.runtime["robot_run"] = False
                self.runtime["robot_pause"] = True
                self.state["status"] = "stopped"
                self.state["speed"] = 0.0
            else:
                # forward/backward/left/right는 기존 노트북처럼 manual 계열로 본다.
                self.runtime["mode"] = "manual"
                self.runtime["robot_run"] = False
                self.runtime["robot_pause"] = True
                self.state["status"] = command
                self.state["speed"] = speed

        print(f"[{self.wire_id}] COMMAND command={command}, speed={speed}, reason={reason}")
        self.publish_status()

    def maybe_step(self) -> None:
        now = time.monotonic()
        with self.lock:
            if now - self.last_step_at < self.step_interval:
                return
            if not (self.runtime["mode"] == "line_tracing" and self.runtime["robot_run"] and not self.runtime["robot_pause"]):
                return
            if self.state.get("obstacle"):
                return

            route = self.state.get("current_route") or []
            idx = int(self.state.get("route_index", 0) or 0)
            if idx >= len(route) - 1:
                self.runtime["mode"] = "idle"
                self.runtime["robot_run"] = False
                self.runtime["robot_pause"] = True
                self.state["status"] = "arrived"
                self.state["speed"] = 0.0
                self.update_next_node_locked()
                self.last_step_at = now
                should_publish = True
            else:
                self.state["route_index"] = idx + 1
                self.update_next_node_locked()
                if self.state["next_node"] == "DEST":
                    self.state["status"] = "arrived"
                    self.state["speed"] = 0.0
                    self.runtime["mode"] = "idle"
                    self.runtime["robot_run"] = False
                    self.runtime["robot_pause"] = True
                else:
                    self.state["status"] = "line_tracing"
                    self.state["speed"] = self.default_speed
                self.last_step_at = now
                should_publish = True

        if should_publish:
            self.publish_status()

    def periodic_publish(self) -> None:
        now = time.monotonic()
        if now - self.last_status_at >= self.status_interval:
            self.last_status_at = now
            self.publish_status()
        if self.publish_sensing_enabled and now - self.last_sensing_at >= self.sensing_interval:
            self.last_sensing_at = now
            self.publish_sensing()

    def on_connect(self, client_obj: Any, userdata: Any, flags: Any, reason_code: Any, properties: Any = None) -> None:
        if mqtt_reason_ok(reason_code):
            with self.lock:
                self.runtime["mqtt_connected"] = True
            print(f"[{self.wire_id}] MQTT connected: {self.host}:{self.port}")
            client_obj.subscribe(self.command_topic)
            client_obj.subscribe(self.route_topic)
            print(f"[{self.wire_id}] subscribe: {self.command_topic}, {self.route_topic}")
            self.publish_status()
        else:
            with self.lock:
                self.runtime["mqtt_connected"] = False
            print(f"[{self.wire_id}] MQTT connect failed: {reason_code}")

    def on_disconnect(self, client_obj: Any, userdata: Any, *args: Any) -> None:
        with self.lock:
            self.runtime["mqtt_connected"] = False
        print(f"[{self.wire_id}] MQTT disconnected args={args}")

    def on_message(self, client_obj: Any, userdata: Any, msg: Any) -> None:
        raw = msg.payload.decode("utf-8", errors="replace")
        print(f"[{now_iso()}] [{self.wire_id}] MQTT RX topic={msg.topic} payload={raw}")
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            print(f"[{self.wire_id}] JSON decode error: {exc}")
            return
        if not isinstance(data, dict):
            print(f"[{self.wire_id}] payload must be object: {data!r}")
            return

        if msg.topic == self.command_topic:
            self.handle_command(data)
        elif msg.topic == self.route_topic:
            self.handle_route(data)
        else:
            print(f"[{self.wire_id}] Unhandled topic: {msg.topic}")

    def connect(self) -> None:
        client_id = f"fake_{self.wire_id.lower()}_{uuid.uuid4().hex[:8]}"
        self.client = create_mqtt_client(client_id)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message
        self.client.connect(self.host, self.port, keepalive=30)
        self.client.loop_start()

    def close(self) -> None:
        self.stop_event.set()
        if self.client is not None:
            try:
                self.client.loop_stop()
                self.client.disconnect()
            except Exception:
                pass

    def loop(self) -> None:
        self.last_step_at = time.monotonic()
        while not self.stop_event.is_set():
            self.maybe_step()
            self.periodic_publish()
            time.sleep(0.05)


def make_fake_agv(
    robot_id: str,
    host: str,
    port: int,
    route_override: Optional[str],
    status_interval: float,
    sensing_interval: float,
    step_interval: float,
    speed: float,
    auto_start: bool,
    no_sensing: bool,
    drop_route_ack: bool = False,
    reject_route_ack: bool = False,
) -> FakeAgv:
    logical_id = normalize_robot_id(robot_id)
    wire_id = wire_robot_id(robot_id)
    route = route_for_robot(logical_id, route_override)
    return FakeAgv(
        logical_id=logical_id,
        wire_id=wire_id,
        route=route,
        host=host,
        port=port,
        status_interval=status_interval,
        sensing_interval=sensing_interval,
        step_interval=step_interval,
        default_speed=speed,
        auto_start=auto_start,
        publish_sensing_enabled=not no_sensing,
        drop_route_ack=drop_route_ack,
        reject_route_ack=reject_route_ack,
    )


def run_self_test() -> None:
    samples = [
        make_fake_agv("AGV1", "localhost", 1883, None, 1.0, 1.0, 2.0, 0.3, False, False),
        make_fake_agv("AGV2", "localhost", 1883, None, 1.0, 1.0, 2.0, 0.3, False, False),
    ]
    for fake in samples:
        print("=" * 80)
        print(f"robot logical={fake.logical_id} wire={fake.wire_id}")
        print(f"topics: command={fake.command_topic}, route={fake.route_topic}, status={fake.status_topic}, route_ack={fake.route_ack_topic}")
        print("status payload:")
        print(pretty_json(fake.build_status_payload()))
        print("sensing payload:")
        print(pretty_json(fake.build_sensing_payload()))
        route_payload = {
            "type": "reroute",
            "route_id": "ROUTE_SELF_TEST",
            "robot_id": fake.wire_id,
            "route": fake.route,
            "new_route": fake.route,
            "reason": "self_test",
            "timestamp": now_iso(),
        }
        print("route_ack payload shape:")
        received_route = route_payload.get("route") or route_payload.get("new_route") or []
        print(pretty_json({
            "type": "route_ack",
            "ack_id": f"ACK_{fake.wire_id}_SELF_TEST",
            "robot_id": fake.wire_id,
            "received_type": route_payload.get("type", "-"),
            "received_route_id": route_payload.get("route_id", "-"),
            "received_route": received_route,
            "blocked_edge": route_payload.get("blocked_edge", "-"),
            "status": "accepted",
            "reason": "accepted",
            "timestamp": now_iso(),
        }))
    print("[OK] fake AGV self-test finished")


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stage 1 notebook-compatible Fake AGV simulator")
    parser.add_argument("--both", action="store_true", help="AGV1/AGV2 두 대를 한 프로세스에서 실행")
    parser.add_argument("--robot-id", default=os.getenv("ROBOT_ID") or os.getenv("AGV_ROBOT_ID") or "AGV1", help="AGV1, AGV2, AGV_01, AGV_02 중 하나. 기본 AGV1")
    parser.add_argument("--route", default=None, help="기본 route override. 예: PURPLE,GREEN,ORANGE")
    parser.add_argument("--broker-host", default=broker_host())
    parser.add_argument("--broker-port", type=int, default=broker_port())
    parser.add_argument("--status-interval", type=float, default=float(os.getenv("FAKE_AGV_STATUS_INTERVAL_SEC", "1.0")))
    parser.add_argument("--sensing-interval", type=float, default=float(os.getenv("FAKE_AGV_SENSING_INTERVAL_SEC", "1.0")))
    parser.add_argument("--step-interval", type=float, default=float(os.getenv("FAKE_AGV_STEP_INTERVAL_SEC", "2.0")))
    parser.add_argument("--speed", type=float, default=float(os.getenv("FAKE_AGV_DEFAULT_SPEED", "0.30")))
    parser.add_argument("--auto-start", action="store_true", help="시작하자마자 line_tracing 상태로 한 step씩 이동")
    parser.add_argument("--no-sensing", action="store_true", help="sensing publish를 끔")
    parser.add_argument("--drop-route-ack", action="store_true", help="Step 10 테스트용: route 수신 후 route_ack를 보내지 않음")
    parser.add_argument("--reject-route-ack", action="store_true", help="Step 10 테스트용: route 수신 후 rejected route_ack를 보냄")
    parser.add_argument("--self-test", action="store_true", help="MQTT 연결 없이 payload 형태만 출력")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)
    if args.self_test:
        run_self_test()
        return

    if mqtt is None:
        raise SystemExit("paho-mqtt가 설치되어 있지 않습니다. pip install paho-mqtt")

    robot_ids = ["AGV1", "AGV2"] if args.both else [args.robot_id]
    fakes = [
        make_fake_agv(
            robot_id=rid,
            host=args.broker_host,
            port=args.broker_port,
            route_override=args.route if not args.both else None,
            status_interval=args.status_interval,
            sensing_interval=args.sensing_interval,
            step_interval=args.step_interval,
            speed=args.speed,
            auto_start=args.auto_start,
            no_sensing=args.no_sensing,
            drop_route_ack=args.drop_route_ack,
            reject_route_ack=args.reject_route_ack,
        )
        for rid in robot_ids
    ]

    print("========== Stage 1 Fake AGV 시작 ==========")
    print(f"MQTT={args.broker_host}:{args.broker_port}")
    for fake in fakes:
        print(f"robot logical={fake.logical_id} wire={fake.wire_id} route={fake.route}")
        print(f"  command={fake.command_topic}")
        print(f"  route={fake.route_topic}")
        print(f"  status={fake.status_topic}")
        fake.connect()

    threads = [threading.Thread(target=fake.loop, name=f"loop-{fake.wire_id}", daemon=True) for fake in fakes]
    for thread in threads:
        thread.start()

    try:
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\n[FakeAGV] stopping...")
    finally:
        for fake in fakes:
            fake.close()


if __name__ == "__main__":
    main(sys.argv[1:])
