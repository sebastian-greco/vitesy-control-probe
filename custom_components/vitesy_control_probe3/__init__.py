"""Temporary admin-only MQTT control probe for one Eteria."""

from __future__ import annotations

import asyncio
import logging
import ssl
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from homeassistant.config_entries import ConfigEntry, ConfigEntryState
from homeassistant.core import HomeAssistant, ServiceCall, SupportsResponse
from homeassistant.exceptions import HomeAssistantError, Unauthorized
from homeassistant.helpers.service import async_register_admin_service
from paho.mqtt import __version__ as PAHO_VERSION
from paho.mqtt.client import Client

from .experiment import exercise, test_readiness
from .off_experiment import exercise_off, is_off_baseline
from .shadow import DEVICE_ID, HOST, PORT, ProbeError, Snapshot, open_shadow

DOMAIN = "vitesy_control_probe3"
SOURCE_DOMAIN = "vitesy_web"
TARGET_MODEL = "ETERIA"
TARGET_FIRMWARE = "1.1.11"
SERVICES = ("read_state", "start_test", "status")
BASELINE_LIFETIME = 300
_LOGGER = logging.getLogger(__name__)


def _readiness(state: Snapshot) -> str | None:
    if is_off_baseline(state):
        return None
    return test_readiness(state)


@dataclass(frozen=True)
class LiveSource:
    """The exact running source connection and its TLS context."""

    context: ssl.SSLContext
    client_type: Callable
    connection: Any
    device_id: str

    def open(self, *, write: bool):
        return open_shadow(self.context, self.client_type, self.device_id, write=write)


def _live_source(
    hass: HomeAssistant,
    entry_id: str,
    name: str,
    device_id: str | None = None,
) -> LiveSource:
    """Resolve only the pinned entry, target and running TLS connection."""
    if PAHO_VERSION != "2.1.0":
        raise ProbeError("unsupported_paho_version")
    if (
        not isinstance(entry_id, str)
        or not DEVICE_ID.fullmatch(entry_id)
        or not isinstance(name, str)
        or not 0 < len(name) <= 100
        or not name.isprintable()
    ):
        raise ProbeError("invalid_probe_target")
    entry = hass.config_entries.async_get_entry(entry_id)
    if (
        entry is None
        or entry.domain != SOURCE_DOMAIN
        or entry.state is not ConfigEntryState.LOADED
    ):
        raise ProbeError("source_entry_not_loaded")
    coordinator = getattr(entry, "runtime_data", None)
    if type(coordinator).__module__ != "custom_components.vitesy_web.coordinator":
        raise ProbeError("unsupported_source_coordinator")
    inventory = getattr(coordinator, "_devices", None)
    if not isinstance(inventory, dict):
        raise ProbeError("source_inventory_unavailable")
    matches = [
        (key, device)
        for key, device in inventory.items()
        if getattr(device, "name", None) == name
    ]
    if len(matches) != 1:
        raise ProbeError("target_device_not_unique")
    key, device = matches[0]
    if (
        device.model != TARGET_MODEL
        or device.firmware_version != TARGET_FIRMWARE
        or not isinstance(device.id, str)
        or not DEVICE_ID.fullmatch(device.id)
        or key != device.id.lower()
        or (device_id is not None and device.id != device_id)
    ):
        raise ProbeError("target_device_identity_mismatch")

    source = getattr(coordinator, "_mqtt", None)
    topics = getattr(source, "_devices", None)
    if not isinstance(topics, dict) or topics.get(
        f"things/{TARGET_MODEL}/{device.id}/realtime"
    ) != (key, device):
        raise ProbeError("target_not_in_running_mqtt_session")
    connection = getattr(source, "_connection", None)
    if (
        type(connection).__module__ != "custom_components.vitesy_web.mqtt"
        or not connection.connected
    ):
        raise ProbeError("source_mqtt_not_connected")
    client = getattr(connection, "_client", None)
    if (
        not isinstance(client, Client)
        or type(client).__module__ != "custom_components.vitesy_web.mqtt"
        or type(client).__name__ != "_SingleAttemptClient"
        or not client.is_connected()
        or client.host != HOST
        or client.port != PORT
    ):
        raise ProbeError("source_mqtt_client_mismatch")
    context = getattr(client, "_ssl_context", None)
    socket = client.socket()
    if (
        not isinstance(context, ssl.SSLContext)
        or context.verify_mode != ssl.CERT_REQUIRED
        or not context.check_hostname
        or context.minimum_version < ssl.TLSVersion.TLSv1_2
        or not isinstance(socket, ssl.SSLSocket)
        or socket.context is not context
        or socket.server_hostname != HOST
    ):
        raise ProbeError("source_mqtt_tls_context_mismatch")
    return LiveSource(context, type(client), connection, device.id)


class Probe:
    """Run one bounded operation at a time and retain only sanitized results."""

    def __init__(
        self,
        hass: HomeAssistant,
        source_entry_id: str,
        device_name: str,
        device_id: str,
    ) -> None:
        self.hass = hass
        self.source_entry_id = source_entry_id
        self.device_name = device_name
        self.device_id = device_id
        self.task: asyncio.Task | None = None
        self.baseline: tuple[Snapshot, str, float] | None = None
        self.report: dict[str, Any] = {"status": "idle", "device": device_name}
        self.closing = False

    def live_source(self) -> LiveSource:
        return _live_source(
            self.hass, self.source_entry_id, self.device_name, self.device_id
        )

    async def handle(self, call: ServiceCall) -> dict[str, Any]:
        # HA's admin-service wrapper accepts internal calls without a user ID.
        # Require a signed-in admin for these one-off test actions.
        if call.context.user_id is None:
            raise Unauthorized(context=call.context)
        if call.service == "status":
            return deepcopy(self.report)
        if self.closing or (self.task is not None and not self.task.done()):
            raise HomeAssistantError("A Vitesy probe operation is already running")
        try:
            source = self.live_source()
        except ProbeError as error:
            raise HomeAssistantError(str(error)) from None
        prior = None
        if call.service == "start_test":
            prior = self.baseline
            if (
                prior is None
                or prior[1] != source.device_id
                or asyncio.get_running_loop().time() >= prior[2]
            ):
                raise HomeAssistantError(
                    "Read and inspect the selected device's state before starting the test"
                )
            if (reason := _readiness(prior[0])) is not None:
                raise HomeAssistantError(reason)
        self.baseline = None
        self.report = {
            "status": "running",
            "device": self.device_name,
            "operation": call.service,
        }
        self.task = self.hass.async_create_background_task(
            self.run(call.service, source, prior[0] if prior else None),
            "Vitesy control probe",
        )
        return deepcopy(self.report)

    async def run(
        self, operation: str, source: LiveSource, prior: Snapshot | None
    ) -> None:
        try:
            if operation == "read_state":
                async with source.open(write=False) as shadow:
                    snapshot = await shadow.get()
                self.baseline = (
                    snapshot,
                    source.device_id,
                    asyncio.get_running_loop().time() + BASELINE_LIFETIME,
                )
                self.report.update(
                    state=snapshot.public(),
                    test_blocked_by=_readiness(snapshot),
                    test_strategy="wake_from_off"
                    if is_off_baseline(snapshot)
                    else "active_manual",
                )
            else:
                if prior is None:
                    raise ProbeError("missing_inspected_baseline")

                def still_same_source() -> None:
                    current = self.live_source()
                    if (
                        current.connection is not source.connection
                        or current.device_id != source.device_id
                    ):
                        raise ProbeError("source_session_changed")

                test_operation = exercise_off if is_off_baseline(prior) else exercise
                await test_operation(source.open, prior, still_same_source, self.report)
        except asyncio.CancelledError:
            self.report.update(status="cancelled")
            raise
        except ProbeError as error:
            self.report.update(status="failed", error=str(error))
        except TimeoutError:
            self.report.update(status="failed", error="mqtt_timeout")
        except Exception:  # noqa: BLE001 - Service responses never expose underlying credentials or payloads.
            self.report.update(status="failed", error="probe_failed")
        else:
            self.report.update(status="complete")

    async def stop(self) -> None:
        self.closing = True
        if self.task is not None and not self.task.done():
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry[Probe]) -> bool:
    """Register services without opening a connection or changing state."""
    source, name, target = (
        entry.data.get(key) for key in ("source_entry_id", "device_name", "device_id")
    )
    if (
        not isinstance(source, str)
        or not DEVICE_ID.fullmatch(source)
        or not isinstance(name, str)
        or not 0 < len(name) <= 100
        or not name.isprintable()
        or not isinstance(target, str)
        or not DEVICE_ID.fullmatch(target)
        or any(hass.services.has_service(DOMAIN, service) for service in SERVICES)
    ):
        return False
    probe = entry.runtime_data = Probe(hass, source, name, target)
    for service in SERVICES:
        async_register_admin_service(
            hass,
            DOMAIN,
            service,
            probe.handle,
            supports_response=SupportsResponse.ONLY,
        )
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry[Probe]) -> bool:
    """Close helper-owned connections and allow an outstanding restore to finish."""
    for service in SERVICES:
        hass.services.async_remove(DOMAIN, service)
    probe = entry.runtime_data
    await probe.stop()
    if probe.report.get("restore_unconfirmed"):
        _LOGGER.warning(
            "The Eteria control probe could not confirm restoration: %s",
            probe.report["restore_unconfirmed"],
        )
    return True
