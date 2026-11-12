"""Add the temporary probe without contacting the broker or changing a device."""

import voluptuous as vol
from homeassistant.config_entries import ConfigFlow, ConfigFlowResult

from . import DOMAIN, _live_source
from .shadow import ProbeError

_SCHEMA = vol.Schema(
    {vol.Required("source_entry_id"): str, vol.Required("device_name"): str}
)


class VitesyControlProbeFlow(ConfigFlow, domain=DOMAIN):
    """Pin a single Eteria to a Vitesy Web account already loaded in HA."""

    VERSION = 1

    async def async_step_user(
        self,
        user_input: dict | None = None,
    ) -> ConfigFlowResult:
        self._async_abort_entries_match({})
        errors = {}
        if user_input is not None:
            entry_id = user_input["source_entry_id"].strip()
            name = user_input["device_name"].strip()
            try:
                source = _live_source(self.hass, entry_id, name)
            except ProbeError as error:
                errors["base"] = str(error)
            else:
                return self.async_create_entry(
                    title="Vitesy Control Probe 2",
                    data={
                        "source_entry_id": entry_id,
                        "device_name": name,
                        "device_id": source.device_id,
                    },
                )
        return self.async_show_form(step_id="user", data_schema=_SCHEMA, errors=errors)
