# Vitesy Control Probe

This temporary Home Assistant integration tests fan and front LED control on one Eteria running firmware 1.1.11. It requires a loaded Vitesy Web integration with a working MQTT connection and its existing Paho MQTT 2.1.0 dependency. It shares the existing in-process TLS context with a separate MQTT client for each operation. It makes no Vitesy HTTP or sign-in requests and does not export or save the private key.

Add **Vitesy Control Probe** using the Vitesy Web config entry ID and exact Eteria device name. Setup pins the case-sensitive device ID and registers three actions for signed-in HA administrators. Setup does not connect to MQTT or change any device state.

1. Run `vitesy_control_probe.read_state` and poll `vitesy_control_probe.status` for the result. The read uses a single correlated MQTT shadow request. Confirm that the state belongs to the intended device and has `test_blocked_by: null`.
2. Run `vitesy_control_probe.start_test` within five minutes of that read. The probe rechecks the baseline and requires active manual operation with no pending desired state. It changes the fan by 15 percentage points, verifies a fresh device-reported change and restores the fan before testing the front LED in the same way.
3. Check `vitesy_control_probe.status` for each reported value and restoration result. A broker acknowledgement alone is insufficient. MQTT readback confirms what the device reports, so verify physical behavior separately if needed.

The probe uses conditional shadow versions and avoids overwriting settings after detecting an external change. A lost acknowledgement, interrupted network or concurrent change can leave restoration unconfirmed. In that case, inspect the device in the official app and restore its original value as needed. Removing the probe unregisters its actions and waits for any in-flight restoration attempt. Remove the helper after the experiment.

From the helper project's root, run the offline tests in a Home Assistant 2026.11 and Paho 2.1.0 environment:

```sh
PYTHONPATH=. python -m pytest tests
```
