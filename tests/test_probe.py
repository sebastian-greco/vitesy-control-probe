"""Offline shadow and HA action checks. No live broker or credentials."""

import asyncio
import json
import socket
import ssl
import threading
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from custom_components.vitesy_control_probe import experiment
from custom_components.vitesy_control_probe.shadow import (
    ProbeError,
    Rejected,
    Snapshot,
    UpdateAck,
    open_shadow,
)
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import Unauthorized

from custom_components import vitesy_control_probe as integration


class FakeShadow:
    def __init__(self, *, desired=None):
        self.reported = {
            "mode": "custom",
            "auto": False,
            "standby": False,
            "fan": 40,
            "led_out": 0,
            "led_in": 73,
        }
        self.desired = dict(desired or {})
        self.reported_at = dict.fromkeys(self.reported, 100)
        self.desired_at = dict.fromkeys(self.desired, 100)
        self.version = 20
        self.writes = []
        self.connections = 0
        self.error = None
        self.behavior = None

    async def get(self):
        return Snapshot.parse(
            {
                "state": {
                    "reported": self.reported.copy(),
                    "desired": self.desired.copy(),
                },
                "metadata": {
                    "reported": {
                        key: {"timestamp": stamp}
                        for key, stamp in self.reported_at.items()
                    },
                    "desired": {
                        key: {"timestamp": stamp}
                        for key, stamp in self.desired_at.items()
                    },
                },
                "version": self.version,
                "timestamp": 100 + self.version,
            }
        )

    @asynccontextmanager
    async def open(self, *, write):
        assert write
        self.connections += 1
        self.error = None
        yield self

    async def close(self):
        pass

    async def update(self, before, field, value):
        assert before.version == self.version
        self.writes.append((field, value))
        if len(self.writes) == 1 and self.behavior == "reject":
            raise Rejected("shadow_update_rejected_409")
        if len(self.writes) == 1 and self.behavior == "lost_ack":
            raise TimeoutError
        self.version += 1
        timestamp = 100 + self.version
        ack = UpdateAck(self.version, timestamp, timestamp)
        if value is None:
            self.desired.pop(field)
            self.desired_at.pop(field)
        else:
            self.desired[field] = value
            self.desired_at[field] = timestamp
            if self.behavior != "accepted_only" or len(self.writes) > 1:
                self.reported[field] = value
                self.reported_at[field] = timestamp
                self.version += 1
        if len(self.writes) == 1 and self.behavior == "interference":
            self.desired[field] = 65
            self.desired_at[field] += 1
            self.version += 1
        return ack


@pytest.mark.asyncio
async def test_both_changes_require_device_reports_and_restore_exact_desired():
    shadow = FakeShadow(desired={"fan": 40, "led_in": 73})
    initial = await shadow.get()
    report = {}
    await experiment.exercise(shadow.open, initial, lambda: None, report)
    assert shadow.writes == [
        ("fan", 55),
        ("fan", 40),
        ("led_out", 100),
        ("led_out", 0),
        ("led_out", None),
    ]
    assert (await shadow.get()).reported == initial.reported
    assert (await shadow.get()).desired == initial.desired
    for field in ("fan", "led_out"):
        assert report[field]["restoration"] == "confirmed"
        assert report[field]["device_reported"] == report[field]["requested"]
        assert (
            report[field]["reported_version"] > report[field]["cloud_accepted_version"]
        )


@pytest.mark.asyncio
async def test_cloud_acceptance_without_device_report_restores_and_skips_led(
    monkeypatch,
):
    shadow = FakeShadow(desired={"fan": 40})
    shadow.behavior = "accepted_only"
    monkeypatch.setattr(experiment, "_READBACK_ATTEMPTS", 1)
    report = {}
    with pytest.raises(ProbeError, match="fresh_device_report_not_seen"):
        await experiment.exercise(shadow.open, await shadow.get(), lambda: None, report)
    assert shadow.writes == [("fan", 55), ("fan", 40)]
    assert report["fan"]["restoration"] == "confirmed"
    assert "device_reported" not in report["fan"]


@pytest.mark.parametrize(
    "behavior, error, restoration",
    [
        ("reject", Rejected, "update_rejected"),
        ("lost_ack", TimeoutError, "unconfirmed"),
        ("interference", ProbeError, "unconfirmed"),
    ],
)
@pytest.mark.asyncio
async def test_uncertain_delivery_or_other_writer_never_blindly_restores(
    behavior, error, restoration
):
    shadow = FakeShadow(desired={"fan": 40})
    shadow.behavior = behavior
    report = {}
    with pytest.raises(error):
        await experiment.exercise(shadow.open, await shadow.get(), lambda: None, report)
    assert shadow.writes == [("fan", 55)]
    assert report["fan"]["restoration"] == restoration
    if behavior == "lost_ack":
        assert shadow.connections == 2
        assert (await shadow.get()).version == 20  # May still accept a buffered update.


@pytest.mark.asyncio
async def test_cancel_after_verified_change_restores_before_returning():
    shadow = FakeShadow(desired={"fan": 40})
    initial = await shadow.get()
    report = {}
    task = asyncio.create_task(
        experiment.exercise(shadow.open, initial, lambda: None, report)
    )
    async with asyncio.timeout(1):
        while "device_reported" not in report.get("fan", {}):
            await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert shadow.writes == [("fan", 55), ("fan", 40)]
    assert report["fan"]["restoration"] == "confirmed"


@pytest.mark.asyncio
async def test_pending_or_changed_baseline_blocks_every_write():
    shadow = FakeShadow(desired={"fan": 40})
    initial = await shadow.get()
    shadow.desired["fan"] = 100
    assert experiment.test_readiness(await shadow.get()) == "pending_desired_state"
    with pytest.raises(ProbeError, match="inspected_baseline_changed"):
        await experiment.exercise(shadow.open, initial, lambda: None, {})
    assert shadow.writes == []


class FakeMqttClient:
    """Send a retained response and a wrong-token response before the live one."""

    def __init__(self, _failed):
        self.aborted = threading.Event()
        self._thread = None
        self.writes = []
        self.closed = False

    def tls_set_context(self, context):
        self.context = context

    def connect_async(self, host, port, *, keepalive):
        assert (host, port, keepalive) == ("iot.vitesyhub.com", 8883, 30)

    def loop_start(self):
        self.on_connect(self, None, None, SimpleNamespace(is_failure=False), None)
        return 0

    def subscribe(self, topics):
        self.topics = topics
        self.on_subscribe(self, None, 3, [SimpleNamespace(value=0)] * len(topics), None)
        return 0, 3

    def is_connected(self):
        return not self.closed

    def publish(self, topic, body, *, qos, retain):
        self.writes.append((topic, json.loads(body), qos, retain))
        token = self.writes[-1][1]["clientToken"]
        for expected, retained, fan in (
            (token, True, 4),
            ("unrelated", False, 8),
            (token, False, 40),
        ):
            payload = json.dumps(
                {
                    "clientToken": expected,
                    "version": 20,
                    "timestamp": 100,
                    "state": {"reported": {"fan": fan}},
                }
            )
            self.on_message(
                self,
                None,
                SimpleNamespace(
                    topic=f"{topic}/accepted",
                    payload=payload.encode(),
                    retain=retained,
                    dup=False,
                ),
            )
        return SimpleNamespace(rc=0)

    def disconnect(self):
        self.closed = True

    def _sock_close(self):
        self.closed = True


@pytest.mark.asyncio
async def test_read_only_transport_filters_and_uses_exact_case_preserved_topic():
    context = ssl.create_default_context()
    async with open_shadow(
        context, FakeMqttClient, "TEST:CaSe", write=False
    ) as session:
        client = session.client
        assert client.context is context
        assert (await session.get()).reported == {"fan": 40}
        assert client.topics == [
            ("$aws/things/TEST:CaSe/shadow/get/accepted", 0),
            ("$aws/things/TEST:CaSe/shadow/get/rejected", 0),
        ]
        with pytest.raises(ProbeError, match="mqtt_publish_scope"):
            await session.update(await session.get(), "fan", 20)
        assert all(
            topic == "$aws/things/TEST:CaSe/shadow/get" and qos == 1 and not retained
            for topic, _, qos, retained in client.writes
        )
    assert client.closed


@pytest.mark.asyncio
async def test_setup_and_unload_make_no_connection_and_require_authenticated_service(
    tmp_path, monkeypatch
):
    hass = HomeAssistant(str(tmp_path))
    entry = SimpleNamespace(
        data={
            "source_entry_id": "TEST_ENTRY",
            "device_name": "Fixture",
            "device_id": "TEST_ID",
        }
    )

    def unexpected_source(*_args):
        raise AssertionError(
            "Setup or unauthorized call must not acquire a live source"
        )

    monkeypatch.setattr(integration, "_live_source", unexpected_source)
    assert await integration.async_setup_entry(hass, entry)
    assert all(
        hass.services.has_service(integration.DOMAIN, service)
        for service in integration.SERVICES
    )
    with pytest.raises(Unauthorized):
        await hass.services.async_call(
            integration.DOMAIN, "read_state", {}, blocking=True, return_response=True
        )
    assert await integration.async_unload_entry(hass, entry)
    assert all(
        not hass.services.has_service(integration.DOMAIN, service)
        for service in integration.SERVICES
    )


@pytest.fixture
def live_source_fixture(monkeypatch):
    context = ssl.create_default_context()
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    with context.wrap_socket(
        socket.socket(),
        server_hostname=integration.HOST,
        do_handshake_on_connect=False,
    ) as tls_socket:

        class SourceClient:
            host = integration.HOST
            port = integration.PORT
            _ssl_context = context

            def is_connected(self):
                return True

            def socket(self):
                return tls_socket

        monkeypatch.setattr(integration, "Client", SourceClient)
        client_type = type(
            "_SingleAttemptClient",
            (SourceClient,),
            {"__module__": "custom_components.vitesy_web.mqtt"},
        )
        connection_type = type(
            "_Connection",
            (SimpleNamespace,),
            {"__module__": "custom_components.vitesy_web.mqtt"},
        )
        coordinator_type = type(
            "VitesyCoordinator",
            (SimpleNamespace,),
            {"__module__": "custom_components.vitesy_web.coordinator"},
        )
        client = client_type()
        connection = connection_type(connected=True, _client=client)
        device = SimpleNamespace(
            id="TEST:CaSe",
            name="Fixture",
            model="ETERIA",
            firmware_version="1.1.11",
        )
        coordinator = coordinator_type(
            _devices={device.id.lower(): device},
            _mqtt=SimpleNamespace(
                _connection=connection,
                _devices={
                    f"things/ETERIA/{device.id}/realtime": (device.id.lower(), device)
                },
            ),
        )
        entry = SimpleNamespace(
            state=integration.ConfigEntryState.LOADED,
            domain="vitesy_web",
            runtime_data=coordinator,
        )
        hass = SimpleNamespace(
            config_entries=SimpleNamespace(
                async_get_entry=lambda key: entry if key == "TEST_ENTRY" else None
            )
        )
        yield hass, coordinator, connection, context, device


def test_live_source_reuses_the_same_context_and_case_preserved_identity(
    live_source_fixture,
):
    hass, _, connection, context, device = live_source_fixture
    client = connection._client
    source = integration._live_source(hass, "TEST_ENTRY", "Fixture", "TEST:CaSe")
    assert source.device_id == device.id
    assert source.context is client._ssl_context is context
    assert source.connection is connection
    assert source.client_type is type(client)
    assert context.check_hostname and context.verify_mode == ssl.CERT_REQUIRED


@pytest.mark.parametrize(
    "change, code",
    [
        ("id", "target_device_identity_mismatch"),
        ("duplicate", "target_device_not_unique"),
        ("model", "target_device_identity_mismatch"),
        ("context", "source_mqtt_tls_context_mismatch"),
    ],
)
def test_live_source_rejects_wrong_identity_or_insecure_tls(
    live_source_fixture, change, code
):
    hass, coordinator, _, context, device = live_source_fixture
    pinned_id = "TEST:CaSe"
    if change == "id":
        pinned_id = pinned_id.lower()
    elif change == "duplicate":
        coordinator._devices["another_id"] = SimpleNamespace(name=device.name)
    elif change == "model":
        device.model = "NATEDE-R2"
    else:
        context.check_hostname = False
    with pytest.raises(ProbeError, match=code):
        integration._live_source(hass, "TEST_ENTRY", "Fixture", pinned_id)
