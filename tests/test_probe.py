"""Offline shadow and HA action checks. No live broker or credentials."""

import asyncio
import json
import socket
import ssl
import threading
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import Unauthorized

from custom_components import vitesy_control_probe3 as integration
from custom_components.vitesy_control_probe3 import experiment, off_experiment
from custom_components.vitesy_control_probe3.shadow import (
    PatchAck,
    ProbeError,
    Rejected,
    Snapshot,
    UpdateAck,
    open_shadow,
)


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

    @property
    def cloud_timestamp(self):
        return 100 + self.version

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
                "timestamp": self.cloud_timestamp,
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


class FakeOffShadow(FakeShadow):
    def __init__(
        self, *, latent_auto=False, correction_flag=False, behavior=None, extras=None
    ):
        super().__init__(desired={"mode": "off", "standby": True})
        self.extras = dict(extras or {})
        self.desired.update(self.extras)
        self.reported = {
            "mode": "off",
            "standby": True,
            "auto": False,
            "fan": 0,
            "led_out": 0,
            "led_in": 0,
        }
        self.reported.update(self.extras)
        self.reported_at = dict.fromkeys(self.reported, 100)
        self.latent_auto = latent_auto
        self.correction_flag = correction_flag
        self.behavior = behavior
        self.pending_led_delta = False
        self.pending_version = None

    @property
    def cloud_timestamp(self):
        if self.behavior in {"stale_off_readback", "stale_already_off"}:
            return 100
        return super().cloud_timestamp

    def publish_report(self, **fields):
        self.version += 1
        self.reported.update(fields)
        if self.reported["standby"]:
            self.reported.update(self.extras)
        else:
            if "led_rgb" in self.extras:
                self.reported["led_rgb"] = [245, 250, 255]
            if self.behavior == "unrelated_pending":
                self.reported["goal"] = "allergy"
        self.reported_at = dict.fromkeys(self.reported, self.cloud_timestamp)

    def rewrite_desired(self, **patch):
        for field, value in patch.items():
            if value is None:
                self.desired.pop(field, None)
                self.desired_at.pop(field, None)
            else:
                self.desired[field] = value
                self.desired_at[field] = self.cloud_timestamp

    async def get(self):
        state = await super().get()
        if self.pending_led_delta:
            self.pending_led_delta = False
            self.publish_report(led_out=0)
        return state

    async def update_patch(self, before, patch):
        assert before.version == self.version
        assert "led_in" not in patch
        self.writes.append(patch.copy())
        first = len(self.writes) == 1
        if first and self.behavior == "reject_wake":
            raise Rejected("shadow_update_rejected_409")
        if first and self.behavior == "lost_unapplied":
            self.pending_version = before.version
            raise TimeoutError
        if patch == {"led_out": 100} and self.behavior == "reject_led":
            raise Rejected("shadow_update_rejected_409")
        self.version += 1
        self.rewrite_desired(**patch)
        ack = PatchAck(self.version, self.cloud_timestamp, self.desired_at.copy())
        assert ack.timestamp >= before.timestamp
        if self.behavior in {"stale_off_readback", "stale_already_off"}:
            # Keep all reports and acknowledgements in the same second. An
            # unrelated cloud write can advance version without a device report.
            if self.behavior == "stale_already_off" or patch.get("mode") == "off":
                self.version += 1
                self.rewrite_desired(
                    mode="off", standby=True, fan=None, led_out=None, auto=None
                )
            return ack
        if patch.get("mode") == "manual":
            # Standby masks previous requests. AWS only sends differing values.
            if self.latent_auto:
                self.publish_report(
                    mode="manual",
                    standby=False,
                    auto=True,
                    fan=50,
                    led_out=42,
                    led_in=100,
                )
                self.rewrite_desired(auto=True, fan=None, led_out=None, led_in=None)
            else:
                self.publish_report(
                    mode="manual",
                    standby=False,
                    auto=False,
                    fan=25,
                    led_out=42,
                    led_in=61,
                )
                self.pending_led_delta = True
                if self.correction_flag:
                    self.rewrite_desired(standby=False, auto=False)
            if self.behavior == "interference":
                self.rewrite_desired(mode="external")
            if self.behavior == "extra_interference":
                self.rewrite_desired(goal="allergy")
            if self.behavior == "lost_applied":
                raise TimeoutError
        elif patch.get("mode") == "off":
            if self.behavior != "off_no_report" and not before.reported["standby"]:
                self.publish_report(
                    mode="off", standby=True, auto=False, fan=0, led_out=0, led_in=0
                )
                self.rewrite_desired(
                    standby=True,
                    fan=None,
                    led_out=None,
                    led_in=None,
                    auto=False if self.correction_flag else None,
                )
        elif patch.get("auto") is False:
            self.publish_report(auto=False, fan=25, led_out=0)
        elif patch == {"led_out": 100}:
            self.publish_report(led_out=100)
        return ack


@pytest.fixture
def fast_off(monkeypatch):
    monkeypatch.setattr(off_experiment, "_PAUSE", 0)


@pytest.mark.parametrize(
    "latent_auto, correction_flag", [(False, False), (True, False), (False, True)]
)
@pytest.mark.parametrize(
    "extras",
    [
        None,
        {
            "led_rgb": [0, 0, 0],
            "goal": "general",
            "opaque-private-field": {"token": "test-secret", "array": [1, True]},
            "unknown_null": None,
        },
    ],
)
@pytest.mark.asyncio
async def test_off_test_verifies_reports_and_restores_exact_off_shape(
    latent_auto, correction_flag, extras, fast_off
):
    shadow = FakeOffShadow(
        latent_auto=latent_auto, correction_flag=correction_flag, extras=extras
    )
    before = await shadow.get()
    report = {}
    assert integration._readiness(before) is None
    await off_experiment.exercise_off(shadow.open, before, lambda: None, report)
    assert report["fan"]["reported"]["fan"] == 25
    assert report["fan"]["reported"]["led_in"] == (100 if latent_auto else 61)
    assert report["front_led"]["reported"]["led_out"] == 100
    assert report["restoration"] == "confirmed"
    assert report["restored_state"]["reported"] == before.reported
    assert report["restored_state"]["desired"] == before.public()["desired"]
    assert (await shadow.get()).desired == before.desired
    assert ("firmware_auto_correction" in report) is latent_auto
    assert all("led_in" not in patch for patch in shadow.writes)
    assert len(shadow.writes) == 3 + int(latent_auto) + int(correction_flag)
    if extras:
        assert all(not patch.keys() & extras.keys() for patch in shadow.writes)
        assert report["fan"]["awake_rgb_only_pending"]
        assert not report["restored_state"]["pending"]
        assert report["restored_state"]["unprojected_desired"] == {
            "count": 4,
            "types": {"array": 1, "string": 1, "object": 1, "null": 1},
            "known_fields": {"led_rgb": "array", "goal": "string"},
        }
        serialized = json.dumps(report)
        assert (
            "opaque-private-field" not in serialized and "test-secret" not in serialized
        )


@pytest.mark.parametrize(
    "behavior, error, outcome",
    [
        ("reject_wake", Rejected, None),
        ("reject_led", Rejected, "confirmed"),
        ("lost_unapplied", TimeoutError, "unchanged_off_fenced"),
        ("lost_applied", TimeoutError, "unconfirmed"),
        ("interference", ProbeError, "unconfirmed"),
        ("off_no_report", ProbeError, "unconfirmed"),
        ("stale_off_readback", ProbeError, "unconfirmed"),
        ("stale_already_off", ProbeError, "unconfirmed"),
    ],
)
@pytest.mark.asyncio
async def test_off_recovery_uses_owned_state_and_never_claims_unreported_off(
    behavior, error, outcome, fast_off
):
    shadow = FakeOffShadow(behavior=behavior)
    before = await shadow.get()
    report = {}
    with pytest.raises(error):
        await off_experiment.exercise_off(shadow.open, before, lambda: None, report)
    assert report.get("restoration") == outcome
    if behavior in {"reject_wake", "lost_applied", "interference"}:
        assert len(shadow.writes) == 1
    if behavior == "lost_unapplied":
        assert shadow.connections == 2
        assert shadow.version > shadow.pending_version
        assert report["restoration_evidence"] == "unchanged_off_version_fence"
    if behavior in {"stale_off_readback", "stale_already_off"}:
        assert report["restore_unconfirmed"] == "off_restore_no_fresh_report"
    if outcome == "confirmed":
        assert (await shadow.get()).reported == before.reported


@pytest.mark.asyncio
async def test_off_test_cancellation_after_fan_report_confirms_off(fast_off):
    shadow = FakeOffShadow()
    before = await shadow.get()
    report = {}
    task = asyncio.create_task(
        off_experiment.exercise_off(shadow.open, before, lambda: None, report)
    )
    async with asyncio.timeout(1):
        while "fan" not in report:
            await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert report["restoration"] == "confirmed"
    assert (await shadow.get()).reported == before.reported


@pytest.mark.asyncio
async def test_off_baseline_with_unsettled_desired_field_never_writes(fast_off):
    shadow = FakeOffShadow()
    shadow.desired["other"] = True
    with pytest.raises(ProbeError, match="off_test_baseline_not_supported"):
        await off_experiment.exercise_off(
            shadow.open, await shadow.get(), lambda: None, {}
        )
    assert shadow.writes == []


@pytest.mark.asyncio
async def test_off_baseline_with_extra_controlled_field_never_writes(fast_off):
    shadow = FakeOffShadow()
    shadow.desired["led_in"] = 0
    with pytest.raises(ProbeError, match="off_test_baseline_not_supported"):
        await off_experiment.exercise_off(
            shadow.open, await shadow.get(), lambda: None, {}
        )
    assert shadow.writes == []


@pytest.mark.asyncio
async def test_off_rechecks_unrelated_desired_fields_before_first_write(fast_off):
    shadow = FakeOffShadow(extras={"opaque": {"flag": True}})
    baseline = await shadow.get()
    shadow.rewrite_desired(opaque={"flag": 1})
    shadow.reported["opaque"] = {"flag": 1}
    assert off_experiment.is_off_baseline(await shadow.get())
    with pytest.raises(ProbeError, match="inspected_baseline_changed"):
        await off_experiment.exercise_off(shadow.open, baseline, lambda: None, {})
    assert shadow.writes == []


@pytest.mark.asyncio
async def test_off_never_overwrites_external_change_to_unrelated_desired_field(
    fast_off,
):
    shadow = FakeOffShadow(extras={"goal": "general"}, behavior="extra_interference")
    report = {}
    with pytest.raises(ProbeError, match="off_test_desired_changed"):
        await off_experiment.exercise_off(
            shadow.open, await shadow.get(), lambda: None, report
        )
    assert len(shadow.writes) == 1
    assert shadow.desired["goal"] == "allergy"
    assert report["restoration"] == "unconfirmed"


@pytest.mark.parametrize("latent_auto", [False, True])
@pytest.mark.asyncio
async def test_rgb_exception_does_not_ignore_other_pending_fields(
    latent_auto, fast_off
):
    shadow = FakeOffShadow(
        extras={"goal": "general", "led_rgb": [0, 0, 0]},
        behavior="unrelated_pending",
        latent_auto=latent_auto,
    )
    report = {}
    with pytest.raises(ProbeError, match="off_test_no_fresh_report"):
        await off_experiment.exercise_off(
            shadow.open, await shadow.get(), lambda: None, report
        )
    assert "fan" not in report and "front_led" not in report
    assert len(shadow.writes) == 2
    assert report["restoration"] == "confirmed"


@pytest.mark.parametrize(
    "reported, desired, delta, expected",
    [
        (
            {"standby": False, "led_rgb": [245, 250, 255]},
            {"led_rgb": [0, 0, 0]},
            {"led_rgb": [0, 0, 0]},
            True,
        ),
        (
            {"standby": True, "led_rgb": [245, 250, 255]},
            {"led_rgb": [0, 0, 0]},
            {"led_rgb": [0, 0, 0]},
            False,
        ),
        (
            {"standby": False, "led_rgb": [245, 250, True]},
            {"led_rgb": [0, 0, 0]},
            {},
            False,
        ),
        (
            {"standby": False, "led_rgb": [245, 250, 255]},
            {"led_rgb": [0, 0, 0]},
            {"led_rgb": [0, 0, 1]},
            False,
        ),
        (
            {"standby": False, "led_rgb": [245, 250, 255]},
            {"led_rgb": [0, 0, 0]},
            {"other": True},
            False,
        ),
    ],
)
def test_rgb_pending_exception_requires_exact_documented_shape(
    reported, desired, delta, expected
):
    state = Snapshot.parse(
        {
            "version": 1,
            "timestamp": 100,
            "state": {
                "reported": reported,
                "desired": desired,
                "delta": delta,
            },
        }
    )
    assert state.pending and state.awake_rgb_only_pending is expected


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


class FakePatchClient(FakeMqttClient):
    def publish(self, topic, body, *, qos, retain):
        if not topic.endswith("/update"):
            return super().publish(topic, body, qos=qos, retain=retain)
        request = json.loads(body)
        self.writes.append((topic, request, qos, retain))
        payload = json.dumps(
            {
                "clientToken": request["clientToken"],
                "version": request["version"] + 1,
                "timestamp": 101,
                "state": {
                    "desired": {
                        field: value
                        for field, value in request["state"]["desired"].items()
                        if value is not None
                    }
                },
            }
        )
        self.on_message(
            self,
            None,
            SimpleNamespace(
                topic=f"{topic}/accepted",
                payload=payload.encode(),
                retain=False,
                dup=False,
            ),
        )
        return SimpleNamespace(rc=0)


@pytest.mark.asyncio
async def test_off_transport_patch_is_exact_and_does_not_allow_inner_led_or_auto_enable():
    async with open_shadow(
        ssl.create_default_context(), FakePatchClient, "TEST:CaSe", write=True
    ) as session:
        before = await session.get()
        for invalid in (
            {"led_in": 0},
            {"auto": True},
            {"mode": "schedule"},
            {"standby": 1},
        ):
            with pytest.raises(ProbeError, match="invalid_update"):
                await session.update_patch(before, invalid)
        patch = {
            "mode": "off",
            "standby": True,
            "auto": None,
            "fan": None,
            "led_out": None,
        }
        ack = await session.update_patch(before, patch)
        assert ack.version == before.version + 1
        _, (topic, update, qos, retain) = session.client.writes
        assert (
            topic == "$aws/things/TEST:CaSe/shadow/update" and qos == 1 and not retain
        )
        assert update["version"] == before.version
        assert update["state"] == {"desired": patch}


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
