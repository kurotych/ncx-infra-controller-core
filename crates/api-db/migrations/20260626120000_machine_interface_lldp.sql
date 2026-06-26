-- Per-interface LLDP neighbor data reported periodically by running agents
-- (host scout + DPU agent). Keyed by the machine's own (local) NIC MAC.
-- Columns map directly from the LldpSwitchData message (+ parent NetworkInterface MAC).
-- Independent of network_devices / port_to_network_device_map (those stay discovery-only).
CREATE TABLE machine_interface_lldp (
  machine_id          VARCHAR(64) NOT NULL REFERENCES machines(id) ON DELETE CASCADE,
  local_mac_address   VARCHAR(64) NOT NULL,            -- local NIC MAC (NetworkInterface.mac_address)
  local_port          TEXT NOT NULL DEFAULT '',        -- LldpSwitchData.local_port (e.g. eth0, p0)
  remote_port         TEXT NOT NULL DEFAULT '',        -- LldpSwitchData.remote_port
  switch_name         TEXT NOT NULL DEFAULT '',        -- LldpSwitchData.name
  switch_id           TEXT NOT NULL DEFAULT '',        -- LldpSwitchData.id ("type=value")
  switch_description  TEXT NOT NULL DEFAULT '',        -- LldpSwitchData.description
  switch_mgmt_ips     TEXT[] NOT NULL DEFAULT '{}',    -- LldpSwitchData.ip_address (mgmt-ip can be non-IP)
  updated             TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY (machine_id, local_mac_address)
);
