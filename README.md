# Vitesy Control Probe 2

This temporary Home Assistant integration tests fan and front LED control on one Eteria running firmware 1.1.11. It requires a loaded Vitesy Web integration with a working MQTT connection and its existing Paho MQTT 2.1.0 dependency. It shares the existing in-process TLS context with a separate MQTT client for each operation. It makes no Vitesy HTTP or sign-in requests and does not export or save the private key.

Add **Vitesy Control Probe 2** using the Vitesy Web config entry ID and exact Eteria device name. Setup pins the case-sensitive device ID and registers three actions for signed-in HA administrators. Setup does not connect to MQTT or change any device state.

1. Run `vitesy_control_probe2.read_state` and poll `vitesy_control_probe2.status` for the result. Confirm the intended device, `test_blocked_by: null` and the displayed test strategy.
2. Run `vitesy_control_probe2.start_test` within five minutes. In active manual mode, the probe changes the fan by 15 percentage points and restores it before testing the front LED. From a supported settled Off state, the probe wakes in manual mode, confirms fan level 25, confirms front LED level 100, then returns to Off and checks the settled report and desired state.
3. Check `vitesy_control_probe2.status` for each device-reported value and restoration result. MQTT readback confirms what the device reports. Check physical behavior separately if needed.

The Off test can replace settings the device remembers from its previous active program. Standby masks those settings in the shadow, so the Off test restores the observable Off state. It sends no inner LED command. A retained inner LED value can become active while the device is awake and returns to zero output in Off.

The probe uses conditional shadow versions and avoids overwriting settings after detecting an external change. A lost acknowledgement, interrupted network or concurrent change can leave restoration unconfirmed. In that case, inspect the device in the official app and restore its original value as needed. `unchanged_off_fenced` means an unacknowledged wake could no longer apply and the previously reported Off state remained unchanged. Removing the probe unregisters its actions and waits for any in-flight restoration attempt. Remove the helper after the experiment.

From the helper project's root, run the offline tests in a Home Assistant 2026.11 and Paho 2.1.0 environment:

```sh
PYTHONPATH=. python -m pytest tests
```
