-- LLDP switch neighbor data reported per machine, mirroring the
-- (non-deprecated) fields of the LldpSwitchData proto message.
CREATE TABLE machine_lldp_neighbors (
    machine_id varchar(64) NOT NULL REFERENCES machines(id) ON DELETE CASCADE,
    name text NOT NULL,
    description text NOT NULL,
    local_port text NOT NULL,
    ip_addresses inet[] DEFAULT '{}' NOT NULL,
    -- Chassis id split into its LLDP subtype (e.g. "mac", "local") and value.
    id_type text NOT NULL,
    id_value text NOT NULL,
    -- Remote port id split into its LLDP subtype (e.g. "ifname") and value.
    remote_port_type text NOT NULL,
    remote_port_value text NOT NULL,
    -- LLDP-MED inventory fields, when the neighbor advertises them.
    serial text,
    manufacturer text,
    model text,
    created timestamp with time zone DEFAULT now() NOT NULL,
    -- A machine can see several neighbors.
    -- A local_port can see several neighbors.
    PRIMARY KEY (machine_id, local_port, id_type, id_value, remote_port_type, remote_port_value)
);
