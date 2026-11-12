"""Wake one settled Off Eteria, test its outputs, and confirm operational Off."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Any, Literal

from .shadow import (
    FIELD_NAMES,
    PatchAck,
    ProbeError,
    Rejected,
    ShadowSession,
    Snapshot,
    same_value,
)

_OFF_REPORTED = {
    "mode": "off",
    "standby": True,
    "auto": False,
    "fan": 0,
    "led_out": 0,
    "led_in": 0,
}
_OFF_DESIRED = {"mode": "off", "standby": True}
_MANUAL = {"mode": "manual", "standby": False, "auto": False, "fan": 25, "led_out": 0}
_LED = {"led_out": 100}
_ATTEMPTS = 6
_PAUSE = 1.6


def is_off_baseline(state: Snapshot) -> bool:
    """Recognize precisely the supported observable Off configuration."""
    return (
        not state.pending
        and same_value(state.reported, _OFF_REPORTED)
        and same_value(state.desired, _OFF_DESIRED)
        and set(FIELD_NAMES) <= state.reported_at.keys()
        and _OFF_DESIRED.keys() <= state.desired_at.keys()
    )


@dataclass
class _Write:
    before: Snapshot
    patch: dict[str, Any]
    stage: Literal["wake", "disable_auto", "led", "off"]
    ack: PatchAck | None = None
    reasserted: dict[str, int] = dataclass_field(default_factory=dict)

    def expected(self) -> dict[str, Any]:
        return {
            key: value
            for key, value in (self.before.desired | self.patch).items()
            if value is not None
        }


def _fresh(state: Snapshot, write: _Write, fields: set[str]) -> bool:
    ack = write.ack
    return (
        ack is not None
        and state.version > ack.version
        and all(
            state.reported_at.get(field, 0)
            >= max(ack.timestamp, write.before.reported_at.get(field, 0))
            for field in fields
        )
    )


def _fresh_off(state: Snapshot, write: _Write) -> bool:
    if not _fresh(state, write, set(FIELD_NAMES)):
        return False
    if not same_value(write.before.reported, _OFF_REPORTED):
        return True
    # A report already showing Off can survive an unrelated same-second
    # shadow update. Demand fresh mode and standby metadata in that case.
    return all(
        field in write.before.reported_at
        and state.reported_at[field] > write.before.reported_at[field]
        for field in ("mode", "standby")
    )


def _desired_owned(state: Snapshot, write: _Write, expected: dict[str, Any]) -> bool:
    ack = write.ack
    if (
        ack is None
        or state.version < ack.version
        or not same_value(state.desired, expected)
    ):
        return False
    reasserted = {}
    for field, value in expected.items():
        actual = state.desired_at.get(field)
        if field in write.patch:
            original = ack.desired_at.get(field, ack.timestamp)
        else:
            original = write.before.desired_at.get(field)
        if actual is not None and actual in (original, write.reasserted.get(field)):
            continue
        # The firmware may reassert standby/auto in the same packet as a
        # report. Other desired values must retain our known update metadata.
        if not (
            field in {"standby", "auto"}
            and _fresh(state, write, {field})
            and actual is not None
            and actual == state.reported_at.get(field)
            and same_value(state.reported.get(field), value)
        ):
            return False
        reasserted[field] = actual
    write.reasserted.update(reasserted)
    return True


def _frame(state: Snapshot, write: _Write) -> str:
    """Recognize owned pending, manual, LED and firmware Auto frames."""
    if not set(FIELD_NAMES) <= state.reported.keys():
        raise ProbeError("off_test_incomplete_report")
    reported = state.reported
    awake_auto = (
        reported["mode"] == "manual"
        and reported["standby"] is False
        and reported["auto"] is True
        and reported["led_in"] == 100
        and (
            reported["fan"] == 50
            or (reported["fan"] == 25 and reported["led_out"] == 0)
        )
    )
    if (
        write.stage in {"wake", "disable_auto"}
        and awake_auto
        and (
            not state.pending
            and _fresh(state, write, set(FIELD_NAMES))
            and _desired_owned(
                state, write, {"mode": "manual", "standby": False, "auto": True}
            )
            and state.desired_at.get("auto") == state.reported_at.get("auto")
        )
    ):
        # AWS suppresses delta fields equal to the masked Off report. A unit
        # with retained Auto can therefore wake before receiving auto:false.
        return "auto"

    if not _desired_owned(state, write, write.expected()):
        raise ProbeError("off_test_desired_changed")
    if same_value(reported, write.before.reported):
        return "pending"
    if write.stage == "disable_auto" and awake_auto:
        return "pending"
    if write.stage == "led":
        expected = write.before.reported | _LED
        if not same_value(reported, expected):
            raise ProbeError("off_test_other_reported_fields_changed")
        return (
            "led"
            if not state.pending and _fresh(state, write, {"led_out"})
            else "pending"
        )

    if all(
        same_value(reported.get(field), value)
        for field, value in _MANUAL.items()
        if field != "led_out"
    ) and (
        write.stage == "wake" or reported["led_in"] == write.before.reported["led_in"]
    ):
        # A previously masked front LED value can remain pending until AWS
        # sends the ordinary led_out:0 delta after the initial wake report.
        return (
            "manual"
            if (
                reported["led_out"] == 0
                and not state.pending
                and _fresh(state, write, set(FIELD_NAMES))
            )
            else "pending"
        )
    raise ProbeError("off_test_unexpected_report")


class _OffTest:
    def __init__(self, open_shadow: Callable, report: dict[str, Any]) -> None:
        self.open = open_shadow
        self.report = report
        self.current: _Write | None = None
        self.previous: _Write | None = None

    async def send(self, shadow: ShadowSession, write: _Write) -> None:
        self.previous, self.current = self.current, write
        try:
            write.ack = await shadow.update_patch(write.before, write.patch)
        except Rejected:
            self.current, self.previous = self.previous, None
            raise

    async def observe(self, shadow: ShadowSession) -> tuple[Snapshot, str]:
        assert self.current is not None
        for attempt in range(_ATTEMPTS):
            state = await shadow.get()
            frame = _frame(state, self.current)
            if frame != "pending":
                return state, frame
            if attempt + 1 < _ATTEMPTS:
                await asyncio.sleep(_PAUSE)
        raise ProbeError("off_test_no_fresh_report")

    async def apply(
        self, shadow: ShadowSession, start: Snapshot, check_source: Callable[[], None]
    ) -> None:
        check_source()
        wake = _Write(start, _MANUAL, "wake")
        await self.send(shadow, wake)
        assert wake.ack is not None
        self.report["wake_accepted_version"] = wake.ack.version
        state, frame = await self.observe(shadow)
        if frame == "auto":
            self.report["firmware_auto_correction"] = state.public()
            check_source()
            correction = _Write(
                state, {"auto": False, "fan": 25, "led_out": 0}, "disable_auto"
            )
            await self.send(shadow, correction)
            state, frame = await self.observe(shadow)
        if frame != "manual":
            raise ProbeError("off_test_manual_not_confirmed")
        self.report["fan"] = state.public()

        # Refresh before each versioned write and require the same manual frame.
        await asyncio.sleep(_PAUSE)
        state, frame = await self.observe(shadow)
        if frame != "manual":
            raise ProbeError("off_test_manual_changed")
        check_source()
        led = _Write(state, _LED, "led")
        await self.send(shadow, led)
        state, frame = await self.observe(shadow)
        if frame != "led":
            raise ProbeError("off_test_led_not_confirmed")
        self.report["front_led"] = state.public()
        await asyncio.sleep(_PAUSE)

    async def restore(self, shadow: ShadowSession) -> tuple[Snapshot, str]:
        assert self.current is not None
        latest = self.current
        state = await shadow.get()
        fence_unaccepted_wake = False
        if is_off_baseline(state):
            if state.version > latest.before.version:
                if latest.ack is not None and _fresh_off(state, latest):
                    return await self.confirm_stable(shadow, state), "fresh_off_report"
                raise ProbeError("off_restore_no_fresh_report")
            if (
                state.version < latest.before.version
                or latest.ack is not None
                or self.previous is not None
            ):
                raise ProbeError("off_restore_stale_shadow")
            # Same-version Off and no earlier accepted activation. A no-op
            # Off CAS can fence a still-buffered activation at this version.
            fence_unaccepted_wake = True
        else:
            # If a subsequent ACK was lost, the last positively owned frame
            # can still be fenced with an Off write at its current version.
            owner = latest if latest.ack is not None else self.previous
            if owner is None:
                raise ProbeError("off_restore_ownership_unconfirmed")
            _frame(state, owner)

        patch = _OFF_DESIRED | {
            key: None for key in ("auto", "fan", "led_out") if key in state.desired
        }
        off = _Write(state, patch, "off")
        off.ack = await shadow.update_patch(state, patch)
        for attempt in range(_ATTEMPTS):
            current = await shadow.get()
            if (
                is_off_baseline(current)
                and _desired_owned(current, off, _OFF_DESIRED)
                and (fence_unaccepted_wake or _fresh_off(current, off))
            ):
                evidence = (
                    "unchanged_off_version_fence"
                    if fence_unaccepted_wake
                    else "fresh_off_report"
                )
                return await self.confirm_stable(shadow, current), evidence
            # A pending device correction flag may reassert auto:false in its
            # first Off report. Remove only that firmware-owned extra field.
            if (
                same_value(current.reported, _OFF_REPORTED)
                and not current.pending
                and _fresh_off(current, off)
                and _desired_owned(current, off, _OFF_DESIRED | {"auto": False})
                and current.desired_at.get("auto") == current.reported_at.get("auto")
            ):
                cleared = await shadow.update_patch(current, {"auto": None})
                clean = await shadow.get()
                if clean.version >= cleared.version and is_off_baseline(clean):
                    return await self.confirm_stable(shadow, clean), "fresh_off_report"
                raise ProbeError("off_restore_auto_cleanup_unconfirmed")
            if not _desired_owned(current, off, _OFF_DESIRED):
                raise ProbeError("off_restore_desired_changed")
            if attempt + 1 < _ATTEMPTS:
                await asyncio.sleep(_PAUSE)
        raise ProbeError("off_restore_no_fresh_report")

    @staticmethod
    async def confirm_stable(shadow: ShadowSession, first: Snapshot) -> Snapshot:
        await asyncio.sleep(_PAUSE)
        current = await shadow.get()
        if (
            current.version < first.version
            or current.timestamp < first.timestamp
            or not is_off_baseline(current)
        ):
            raise ProbeError("off_restore_not_stable")
        return current

    async def recover(self, shadow: ShadowSession) -> None:
        assert self.current is not None
        try:
            async with asyncio.timeout(50):
                if self.current.ack is None or shadow.error is not None:
                    try:
                        await shadow.close()
                    except (OSError, TimeoutError):
                        pass
                    async with self.open(write=True) as fresh:
                        state, evidence = await self.restore(fresh)
                else:
                    state, evidence = await self.restore(shadow)
        except ProbeError as error:
            self.report.update(
                restoration="unconfirmed", restore_unconfirmed=str(error)
            )
        except TimeoutError:
            self.report.update(
                restoration="unconfirmed", restore_unconfirmed="off_restore_timeout"
            )
        except Exception:  # noqa: BLE001 - Do not expose underlying MQTT/TLS details.
            self.report.update(
                restoration="unconfirmed", restore_unconfirmed="off_restore_failed"
            )
        else:
            self.report.update(
                restoration="confirmed"
                if evidence == "fresh_off_report"
                else "unchanged_off_fenced",
                restoration_evidence=evidence,
                restored_state=state.public(),
            )


async def exercise_off(
    open_shadow: Callable,
    baseline: Snapshot,
    check_source: Callable[[], None],
    report: dict[str, Any],
) -> None:
    """Run only from the verified Off profile and shield its bounded restoration."""
    if not is_off_baseline(baseline):
        raise ProbeError("off_test_baseline_not_supported")
    run = _OffTest(open_shadow, report)
    report.update(test_strategy="wake_from_off", original_state=baseline.public())
    async with open_shadow(write=True) as shadow:
        start = await shadow.get()
        if (
            start.version < baseline.version
            or start.timestamp < baseline.timestamp
            or not is_off_baseline(start)
        ):
            raise ProbeError("inspected_baseline_changed")
        try:
            async with asyncio.timeout(38):
                await run.apply(shadow, start, check_source)
        finally:
            if run.current is not None:
                task = asyncio.create_task(run.recover(shadow))
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    await task
                    raise
    if report.get("restoration") != "confirmed":
        raise ProbeError("off_restore_unconfirmed")
