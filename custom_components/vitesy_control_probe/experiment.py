"""Change and restore one control at a time, using fresh device reports."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from .shadow import ProbeError, Rejected, ShadowSession, Snapshot, UpdateAck, same_value

_BASELINE_FIELDS = frozenset({"mode", "auto", "standby", "fan", "led_out"})
_MANUAL_MODES = frozenset({"manual", "custom"})
_READBACK_ATTEMPTS = 5


def test_readiness(state: Snapshot) -> str | None:
    """Describe why a live shadow is unsuitable for a reversible manual test."""
    reported = state.reported
    if (
        not _BASELINE_FIELDS <= reported.keys()
        or not {"fan", "led_out"} <= state.reported_at.keys()
    ):
        return "incomplete_reported_baseline"
    if (
        reported["mode"].lower() not in _MANUAL_MODES
        or reported["auto"] is not False
        or reported["standby"] is not False
        or reported["fan"] < 1
    ):
        return "device_not_in_active_manual_mode"
    if state.pending:
        return "pending_desired_state"
    return None


def _same_baseline(current: Snapshot, before: Snapshot) -> bool:
    return (
        current.version >= before.version
        and current.timestamp >= before.timestamp
        and not current.pending
        and same_value(current.reported, before.reported)
        and same_value(current.desired, before.desired)
    )


def _other_fields_unchanged(current: Snapshot, before: Snapshot, field: str) -> None:
    for side in ("reported", "desired"):
        left = {
            key: value for key, value in getattr(current, side).items() if key != field
        }
        right = {
            key: value for key, value in getattr(before, side).items() if key != field
        }
        if not same_value(left, right):
            raise ProbeError("other_shadow_settings_changed")


def _belongs_to_update(state: Snapshot, field: str, value: int, ack: UpdateAck) -> bool:
    return (
        state.version >= ack.version
        and same_value(state.desired.get(field), value)
        and state.desired_at.get(field) == (ack.desired_at or ack.timestamp)
    )


async def _wait_reported(
    shadow: ShadowSession,
    before: Snapshot,
    ack: UpdateAck,
    field: str,
    value: int,
) -> Snapshot:
    prior_value = before.reported[field]
    changed = not same_value(prior_value, value)
    for attempt in range(_READBACK_ATTEMPTS):
        current = await shadow.get()
        _other_fields_unchanged(current, before, field)
        if not _belongs_to_update(current, field, value, ack):
            raise ProbeError("test_desired_state_changed")
        reported = current.reported.get(field)
        if not same_value(reported, prior_value) and not same_value(reported, value):
            raise ProbeError("device_reported_unexpected_value")
        if (
            same_value(reported, value)
            and not current.pending
            and (
                not changed
                or (
                    current.version > ack.version
                    and current.reported_at.get(field, 0)
                    > before.reported_at.get(field, 0)
                    and current.reported_at[field] >= ack.timestamp
                )
            )
        ):
            return current
        if attempt + 1 < _READBACK_ATTEMPTS:
            await asyncio.sleep(1)
    raise ProbeError("fresh_device_report_not_seen")


async def _restore(
    shadow: ShadowSession,
    before: Snapshot,
    field: str,
    target: int,
    ack: UpdateAck | None,
    original: Snapshot,
) -> Snapshot:
    current = await shadow.get()
    # A read at the original version cannot fence a buffered conditional update
    # whose acknowledgement was lost. A newer baseline version can.
    if current.version > before.version and _same_baseline(current, before):
        return current
    _other_fields_unchanged(current, before, field)
    if ack is None or not _belongs_to_update(current, field, target, ack):
        raise ProbeError("restore_ownership_unconfirmed")
    prior_value = before.reported[field]
    if current.reported.get(field) not in (prior_value, target):
        raise ProbeError("restore_external_change")
    rollback = await shadow.update(current, field, prior_value)
    current = await _wait_reported(shadow, current, rollback, field, prior_value)
    if field not in before.desired:
        # Restore a formerly absent desired field as well as the physical value.
        cleared = await shadow.update(current, field, None)
        current = await shadow.get()
        if current.version < cleared.version:
            raise ProbeError("restore_clear_unconfirmed")
    if not _same_baseline(current, original):
        raise ProbeError("original_settings_not_confirmed")
    return current


async def _recover(
    shadow: ShadowSession,
    open_shadow: Callable,
    before: Snapshot,
    field: str,
    target: int,
    ack: UpdateAck | None,
    original: Snapshot,
    result: dict[str, Any],
) -> Snapshot | None:
    try:
        # An absent acceptance can leave a QoS 1 message in flight. Retire this
        # client before inspecting the shadow from one fresh connection.
        if ack is None or shadow.error is not None:
            try:
                await shadow.close()
            except (OSError, TimeoutError):
                pass  # close() has already fenced the old helper's socket.
            async with open_shadow(write=True) as recovery:
                restored = await _restore(
                    recovery, before, field, target, ack, original
                )
        else:
            restored = await _restore(shadow, before, field, target, ack, original)
    except ProbeError as error:
        result.update(restoration="unconfirmed", restoration_error=str(error))
    except TimeoutError:
        result.update(restoration="unconfirmed", restoration_error="restore_timeout")
    except Exception:  # noqa: BLE001 - Do not leak errors from the underlying TLS client.
        result.update(restoration="unconfirmed", restoration_error="restore_failed")
    else:
        result.update(restoration="confirmed", restored_state=restored.public())
        return restored
    return None


async def _exercise_one(
    shadow: ShadowSession,
    open_shadow: Callable,
    baseline: Snapshot,
    field: str,
    value: int,
    check_source: Callable[[], None],
    report: dict[str, Any],
) -> None:
    # Read again just before each conditional update. Any intervening writer
    # also causes the broker's version precondition to reject the update.
    before = await shadow.get()
    if not _same_baseline(before, baseline) or test_readiness(before):
        raise ProbeError("inspected_baseline_changed")
    check_source()
    result = report[field] = {"original": before.reported[field], "requested": value}
    ack = restored = None
    rejected = False
    try:
        try:
            ack = await shadow.update(before, field, value)
        except Rejected:
            rejected = True
            result["restoration"] = "update_rejected"
            raise
        result["cloud_accepted_version"] = ack.version
        observed = await _wait_reported(shadow, before, ack, field, value)
        result.update(
            device_reported=observed.reported[field],
            reported_version=observed.version,
            reported_at=observed.reported_at[field],
        )
        # AWS metadata timestamps are integer seconds. Leave enough time for
        # the restore to be distinguishable by a fresh per-field report.
        await asyncio.sleep(1.1)
    finally:
        if not rejected:
            task = asyncio.create_task(
                _recover(
                    shadow, open_shadow, before, field, value, ack, baseline, result
                )
            )
            try:
                restored = await asyncio.shield(task)
            except asyncio.CancelledError:
                restored = await task
                raise
            finally:
                if restored is None:
                    report["restore_unconfirmed"] = field
    if restored is None:
        raise ProbeError("restore_unconfirmed")


async def exercise(
    open_shadow: Callable,
    baseline: Snapshot,
    check_source: Callable[[], None],
    report: dict[str, Any],
) -> None:
    """Test fan and front LED separately, restoring the fan before touching the LED."""
    if (reason := test_readiness(baseline)) is not None:
        raise ProbeError(reason)
    fan, led = baseline.reported["fan"], baseline.reported["led_out"]
    new_fan = fan + 15 if fan <= 84 else fan - 15
    new_led = 100 if led == 0 else 0
    async with open_shadow(write=True) as shadow:
        await _exercise_one(
            shadow, open_shadow, baseline, "fan", new_fan, check_source, report
        )
        await _exercise_one(
            shadow, open_shadow, baseline, "led_out", new_led, check_source, report
        )
