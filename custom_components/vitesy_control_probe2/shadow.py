"""Bounded access to the one selected device's classic MQTT shadow."""

from __future__ import annotations

import asyncio
import json
import re
import secrets
import ssl
import threading
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from paho.mqtt import client as mqtt

HOST = "iot.vitesyhub.com"
PORT = 8883
DEVICE_ID = re.compile(r"[A-Za-z0-9:_-]{1,128}\Z")
MAX_PAYLOAD = 64 * 1024
MAX_MESSAGES = 64
REPLY_TIMEOUT = 6
FIELD_NAMES = ("mode", "auto", "standby", "fan", "led_out", "led_in")
WRITABLE_FIELDS = frozenset({"fan", "led_out"})


class ProbeError(Exception):
    """An error with a fixed, nonsecret message."""


class Rejected(ProbeError):
    """The server explicitly rejected the update."""


def _unique_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError
        result[key] = value
    return result


def _non_json_constant(_: str) -> None:
    raise ValueError


def _counter(value: Any) -> int | None:
    return value if type(value) is int and 0 < value < 2**63 else None


def _intensity(value: Any) -> bool:
    return type(value) is int and 0 <= value <= 100


def same_value(left: Any, right: Any) -> bool:
    """Compare JSON values without conflating booleans and numbers."""
    if type(left) is not type(right):
        return False
    if isinstance(left, dict):
        return left.keys() == right.keys() and all(
            same_value(value, right[key]) for key, value in left.items()
        )
    if isinstance(left, list):
        return len(left) == len(right) and all(
            same_value(a, b) for a, b in zip(left, right, strict=True)
        )
    return left == right


def _project(fields: dict[str, Any]) -> dict[str, Any]:
    output = {}
    for key in FIELD_NAMES:
        value = fields.get(key)
        if (
            (key == "mode" and isinstance(value, str) and DEVICE_ID.fullmatch(value))
            or (key in {"auto", "standby"} and type(value) is bool)
            or (key in {"fan", "led_out", "led_in"} and _intensity(value))
        ):
            output[key] = value
    return output


def _field_times(metadata: Any, side: str) -> dict[str, int]:
    values = metadata.get(side) if isinstance(metadata, dict) else None
    if not isinstance(values, dict):
        return {}
    return {
        field: timestamp
        for field in FIELD_NAMES
        if isinstance(item := values.get(field), dict)
        and (timestamp := _counter(item.get("timestamp"))) is not None
    }


@dataclass(frozen=True)
class Snapshot:
    """Cloud version, projected report and internally tracked desired state."""

    version: int
    timestamp: int
    reported: dict[str, Any]
    desired: dict[str, Any]
    reported_at: dict[str, int]
    desired_at: dict[str, int]
    pending: bool

    @classmethod
    def parse(cls, doc: dict[str, Any]) -> Snapshot:
        version, timestamp = (
            _counter(doc.get(key)) for key in ("version", "timestamp")
        )
        state = doc.get("state")
        if version is None or timestamp is None or not isinstance(state, dict):
            raise ProbeError("invalid_shadow")
        reported, desired, delta = (
            state.get("reported"),
            state.get("desired", {}),
            state.get("delta", {}),
        )
        if (
            not isinstance(reported, dict)
            or not isinstance(desired, dict)
            or not isinstance(delta, dict)
        ):
            raise ProbeError("invalid_shadow")
        metadata = doc.get("metadata")
        return cls(
            version,
            timestamp,
            _project(reported),
            desired,
            _field_times(metadata, "reported"),
            _field_times(metadata, "desired"),
            bool(delta)
            or any(
                key not in reported or not same_value(value, reported[key])
                for key, value in desired.items()
            ),
        )

    def public(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "cloud_timestamp": self.timestamp,
            "reported": dict(self.reported),
            "desired": _project(self.desired),
            "reported_at": dict(self.reported_at),
            "desired_at": dict(self.desired_at),
            "pending": self.pending,
        }


@dataclass(frozen=True)
class UpdateAck:
    version: int
    timestamp: int
    desired_at: int | None


@dataclass(frozen=True)
class PatchAck:
    version: int
    timestamp: int
    desired_at: dict[str, int]


def _valid_patch_field(field: str, value: Any) -> bool:
    if field in WRITABLE_FIELDS:
        return value is None or _intensity(value)
    if field == "mode":
        return type(value) is str and value in {"off", "manual"}
    if field == "standby":
        return type(value) is bool
    if field == "auto":
        return value is False or value is None
    return False


class ShadowSession:
    """A separate, short-lived Paho client reusing an existing TLS context."""

    def __init__(
        self, context: ssl.SSLContext, factory: Callable, device_id: str, *, write: bool
    ):
        if not DEVICE_ID.fullmatch(device_id):
            raise ProbeError("invalid_device_identity")
        self.root = f"$aws/things/{device_id}/shadow"
        self.write = write
        self.loop = asyncio.get_running_loop()
        self.opened = self.loop.create_future()
        self.suback: tuple[int, asyncio.Future] | None = None
        self.reply: tuple[str, str, asyncio.Future] | None = None
        self.error: str | None = None
        self.closed = False
        self.startup: asyncio.Future | None = None
        self.slots = threading.BoundedSemaphore(16)
        self.message_count = 0
        self.topics = tuple(
            f"{self.root}/{operation}/{result}"
            for operation in (("get", "update") if write else ("get",))
            for result in ("accepted", "rejected")
        )
        self.client = factory(self._fail_from_thread)
        self.client.connect_timeout = 12
        self.client.tls_set_context(context)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = lambda *_: self._fail_from_thread(
            "mqtt_disconnected"
        )
        self.client.on_connect_fail = lambda *_: self._fail_from_thread(
            "mqtt_connect_failed"
        )
        self.client.on_subscribe = self._on_subscribe
        self.client.on_message = self._on_message

    def _schedule(self, callback: Callable, *args: Any) -> bool:
        try:
            self.loop.call_soon_threadsafe(callback, *args)
        except RuntimeError:
            return False
        return True

    @staticmethod
    def _resolve(future: asyncio.Future, result: Any) -> None:
        if not future.done():
            future.set_result(result)

    def _fail_from_thread(self, error: str) -> None:
        self._schedule(self._fail, error)

    def _fail(self, error: str) -> None:
        if self.closed or self.error:
            return
        # Do not surface errors from shared client callbacks to users.
        self.error = (
            error
            if error
            in {
                "mqtt_disconnected",
                "mqtt_connect_failed",
                "mqtt_connection_rejected",
                "mqtt_start_failed",
                "mqtt_client_certificate_rejected",
                "mqtt_message_limit",
                "mqtt_packet_too_large",
            }
            else "mqtt_failed"
        )
        self._resolve(self.opened, False)
        if self.suback:
            self._resolve(self.suback[1], ())
        if self.reply:
            self._resolve(self.reply[2], None)

    def _on_connect(self, _client, _data, _flags, reason, _properties) -> None:
        if reason.is_failure:
            self._fail_from_thread("mqtt_connection_rejected")
        else:
            self._schedule(self._resolve, self.opened, True)

    def _on_subscribe(self, _client, _data, mid, reasons, _properties) -> None:
        self._schedule(
            self._subscription_result, mid, tuple(reason.value for reason in reasons)
        )

    def _subscription_result(self, mid: int, reasons: tuple[int, ...]) -> None:
        if self.suback and mid == self.suback[0]:
            self._resolve(self.suback[1], reasons)

    def _on_message(self, _client, _data, message) -> None:
        if (
            self.closed
            or self.error
            or message.retain
            or message.dup
            or message.topic not in self.topics
        ):
            return
        self.message_count += 1
        if (
            self.message_count > MAX_MESSAGES
            or len(message.payload) > MAX_PAYLOAD
            or not self.slots.acquire(False)
        ):
            self._fail_from_thread("mqtt_message_limit")
            return
        if not self._schedule(self._receive, message.topic, message.payload):
            self.slots.release()

    def _receive(self, topic: str, payload: bytes) -> None:
        try:
            if self.closed or self.error or self.reply is None:
                return
            operation, token, future = self.reply
            if not topic.startswith(f"{self.root}/{operation}/"):
                return
            try:
                doc = json.loads(
                    payload,
                    object_pairs_hook=_unique_keys,
                    parse_constant=_non_json_constant,
                )
            except (ValueError, UnicodeError, RecursionError):
                return
            if isinstance(doc, dict) and doc.get("clientToken") == token:
                self._resolve(future, (topic.endswith("/accepted"), doc))
        finally:
            self.slots.release()

    def _start(self) -> None:
        try:
            if (
                not self.client.aborted.is_set()
                and self.client.loop_start() != mqtt.MQTT_ERR_SUCCESS
            ):
                self._fail_from_thread("mqtt_start_failed")
        except Exception:  # noqa: BLE001 - Report a fixed failure across the worker-thread boundary.
            self._fail_from_thread("mqtt_start_failed")

    async def connect(self) -> None:
        self.client.connect_async(HOST, PORT, keepalive=30)
        self.startup = self.loop.run_in_executor(None, self._start)
        if not await asyncio.wait_for(self.opened, 16):
            raise ProbeError(self.error or "mqtt_connect_failed")
        result, mid = self.client.subscribe([(topic, 0) for topic in self.topics])
        if result != mqtt.MQTT_ERR_SUCCESS:
            raise ProbeError("mqtt_subscribe_failed")
        self.suback = mid, self.loop.create_future()
        try:
            granted = await asyncio.wait_for(self.suback[1], REPLY_TIMEOUT)
        finally:
            self.suback = None
        if granted != (0,) * len(self.topics):
            raise ProbeError(self.error or "mqtt_subscription_rejected")

    async def _request(self, operation: str, body: dict[str, Any]) -> dict[str, Any]:
        if (
            self.error
            or self.closed
            or self.reply is not None
            or not self.client.is_connected()
        ):
            raise ProbeError(self.error or "mqtt_unavailable")
        if operation != "get" and (operation != "update" or not self.write):
            raise ProbeError("mqtt_publish_scope")
        token = secrets.token_hex(12)
        payload = json.dumps({**body, "clientToken": token}, separators=(",", ":"))
        future = self.loop.create_future()
        self.reply = operation, token, future
        try:
            info = self.client.publish(
                f"{self.root}/{operation}", payload, qos=1, retain=False
            )
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                raise ProbeError("mqtt_publish_failed")
            result = await asyncio.wait_for(future, REPLY_TIMEOUT)
        finally:
            self.reply = None
        if result is None:
            raise ProbeError(self.error or "mqtt_unavailable")
        accepted, doc = result
        if not accepted:
            code = doc.get("code")
            suffix = f"_{code}" if type(code) is int and 100 <= code <= 599 else ""
            raise Rejected(f"shadow_{operation}_rejected{suffix}")
        return doc

    async def get(self) -> Snapshot:
        return Snapshot.parse(await self._request("get", {}))

    async def update(
        self, before: Snapshot, field: str, value: int | None
    ) -> UpdateAck:
        if field not in WRITABLE_FIELDS:
            raise ProbeError("invalid_update")
        ack = await self.update_patch(before, {field: value})
        return UpdateAck(ack.version, ack.timestamp, ack.desired_at.get(field))

    async def update_patch(self, before: Snapshot, patch: dict[str, Any]) -> PatchAck:
        """Conditionally change only supported controls on the pinned shadow."""
        if not patch or any(
            not _valid_patch_field(field, value) for field, value in patch.items()
        ):
            raise ProbeError("invalid_update")
        doc = await self._request(
            "update", {"state": {"desired": patch}, "version": before.version}
        )
        version, timestamp = (
            _counter(doc.get(key)) for key in ("version", "timestamp")
        )
        state = doc.get("state")
        desired = state.get("desired") if isinstance(state, dict) else None
        if (
            version != before.version + 1
            or timestamp is None
            or timestamp < before.timestamp
            or any(
                not same_value(
                    desired.get(field) if isinstance(desired, dict) else None, value
                )
                for field, value in patch.items()
            )
        ):
            raise ProbeError("invalid_update_acknowledgement")
        return PatchAck(
            version, timestamp, _field_times(doc.get("metadata"), "desired")
        )

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self.client.aborted.set()
        try:
            self.client.disconnect()
        finally:
            self.client._sock_close()
        async with asyncio.timeout(3):
            while (
                self.startup is not None and not self.startup.done()
            ) or self.client._thread is not None:
                await asyncio.sleep(0.05)


@asynccontextmanager
async def open_shadow(
    context: ssl.SSLContext,
    factory: Callable,
    device_id: str,
    *,
    write: bool,
) -> AsyncIterator[ShadowSession]:
    """Close only the helper-owned client when the operation ends."""
    session = ShadowSession(context, factory, device_id, write=write)
    try:
        await session.connect()
        yield session
    finally:
        await session.close()
