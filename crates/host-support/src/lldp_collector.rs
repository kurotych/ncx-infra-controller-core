//! Collects the LLDP neighbors this host/DPU sees by shelling out to `lldpcli`,
//! parsing its JSON, and dropping self-loopback entries.

use std::fs;
use std::io::Read;
use std::process::{Command as StdCommand, Stdio};
use std::time::Duration;

use ::rpc::machine_discovery as rpc_discovery;
use serde::{Deserialize, Serialize};
use tokio::process::Command as TokioCommand;
use tracing::{debug, warn};
use wait_timeout::ChildExt;

/// Per-invocation cap for each `lldpcli` call (sync and async), so a wedged
/// `lldpd` can't stall a caller. On timeout the child is killed.
const LLDPCLI_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(thiserror::Error, Debug)]
pub enum LldpCollectorError {
    #[error("LLDP error: {0}")]
    Lldp(String),
}

pub type LldpCollectorResult<T> = Result<T, LldpCollectorError>;

/// One LLDP neighbor plus the MAC of the local interface that sees it.
#[derive(Debug, Clone)]
pub struct LldpNeighbor {
    pub local_mac: String,
    pub switch: rpc_discovery::LldpSwitchData,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct LldpValue {
    pub value: String,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct LldpId {
    #[serde(rename = "type")]
    pub id_type: String,
    pub value: String,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct LldpChassis {
    #[serde(default)]
    pub id: Vec<LldpId>,
    #[serde(default)]
    pub name: Vec<LldpValue>,
    #[serde(default)]
    pub descr: Vec<LldpValue>,
    #[serde(rename = "mgmt-ip", default)]
    pub mgmt_ip: Vec<LldpValue>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct LldpPort {
    #[serde(default)]
    pub id: Vec<LldpId>,
    #[serde(default)]
    pub descr: Vec<LldpValue>,
    #[serde(default)]
    pub ttl: Vec<LldpValue>,
}

/// LLDP-MED inventory (`lldp-med[].inventory[]`), advertised by some neighbors
/// (e.g. BlueField DPUs). Every field is optional.
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct LldpInventory {
    #[serde(default)]
    pub serial: Vec<LldpValue>,
    #[serde(default)]
    pub manufacturer: Vec<LldpValue>,
    #[serde(default)]
    pub model: Vec<LldpValue>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct LldpMed {
    #[serde(default)]
    pub inventory: Vec<LldpInventory>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct LldpInterfaceEntry {
    pub name: String, // local interface name (host side)
    #[serde(default)]
    pub chassis: Vec<LldpChassis>,
    #[serde(default)]
    pub port: Vec<LldpPort>,
    #[serde(rename = "lldp-med", default)]
    pub lldp_med: Vec<LldpMed>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct LldpRoot {
    #[serde(default)]
    pub interface: Vec<LldpInterfaceEntry>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct LldpResponse {
    #[serde(default)]
    pub lldp: Vec<LldpRoot>,
}

/// Returns one `LldpNeighbor` per LLDP neighbor, filtering out self-loopback ones.
///
/// Synchronous, for callers not on a Tokio runtime (hardware enumeration,
/// one-shot CLI). Each `lldpcli` call is still timeout-bounded and killed on
/// timeout. On a Tokio runtime, prefer [`collect_lldp_neighbors_async`].
pub fn collect_lldp_neighbors_sync() -> LldpCollectorResult<Vec<LldpNeighbor>> {
    let local_chassis_id = get_local_chassis_id_sync();
    Ok(into_neighbors(
        local_chassis_id,
        get_all_lldp_neighbors_sync()?,
    ))
}

/// Async counterpart of [`collect_lldp_neighbors_sync`] for callers on a Tokio
/// runtime (the periodic reporter). Behaves identically; the only difference is
/// that the timeout is driven by the runtime rather than a blocking wait.
pub async fn collect_lldp_neighbors_async() -> LldpCollectorResult<Vec<LldpNeighbor>> {
    let local_chassis_id = get_local_chassis_id_async().await;
    let all = get_all_lldp_neighbors_async().await?;
    Ok(into_neighbors(local_chassis_id, all))
}

/// Drop self-loopback neighbors and pair each remaining one with its local NIC
/// MAC. Shared by the sync and async collectors.
fn into_neighbors(
    local_chassis_id: Option<LldpId>,
    all: Vec<rpc_discovery::LldpSwitchData>,
) -> Vec<LldpNeighbor> {
    all.into_iter()
        // Unknown local chassis id -> filter nothing: a spurious self-entry
        // beats a lost fabric link.
        .filter(|lldp| {
            local_chassis_id
                .as_ref()
                .is_none_or(|own| !is_self_loopback(lldp, own))
        })
        .filter_map(|lldp| {
            Some(LldpNeighbor {
                local_mac: read_interface_mac(&lldp.local_port)?,
                switch: lldp,
            })
        })
        .collect()
}

/// Run `lldpcli` synchronously, killing the child if it exceeds
/// [`LLDPCLI_TIMEOUT`]. stdout is drained on a helper thread so a full pipe
/// buffer can't deadlock the blocking wait. Returns stdout as a `String`.
fn run_lldpcli_sync(args: &[&str]) -> LldpCollectorResult<String> {
    let mut child = StdCommand::new("lldpcli")
        .args(args)
        .stdout(Stdio::piped())
        .spawn()
        .map_err(|e| LldpCollectorError::Lldp(format!("couldn't spawn lldpcli: {e}")))?;

    // Drain stdout concurrently: if lldpcli filled the pipe buffer while we
    // blocked in `wait_timeout`, it could deadlock waiting for us to read.
    let mut stdout = child.stdout.take().expect("stdout is piped");
    let reader = std::thread::spawn(move || {
        let mut buf = String::new();
        stdout.read_to_string(&mut buf).map(|_| buf)
    });

    let status = match child
        .wait_timeout(LLDPCLI_TIMEOUT)
        .map_err(|e| LldpCollectorError::Lldp(format!("waiting for lldpcli: {e}")))?
    {
        Some(status) => status,
        None => {
            // Timed out: killing the child closes the pipe, unblocking the reader.
            let _ = child.kill();
            let _ = child.wait();
            let _ = reader.join();
            return Err(LldpCollectorError::Lldp(format!(
                "lldpcli timed out after {}s",
                LLDPCLI_TIMEOUT.as_secs()
            )));
        }
    };

    let stdout = reader
        .join()
        .map_err(|_| LldpCollectorError::Lldp("lldpcli stdout reader thread panicked".into()))?
        .map_err(|e| LldpCollectorError::Lldp(format!("reading lldpcli stdout: {e}")))?;

    if !status.success() {
        return Err(LldpCollectorError::Lldp(format!(
            "lldpcli exited unsuccessfully: {status}"
        )));
    }
    Ok(stdout)
}

/// Run `lldpcli` on the Tokio runtime, killing the child if it exceeds
/// [`LLDPCLI_TIMEOUT`]. `kill_on_drop` guarantees a timed-out invocation is
/// terminated rather than orphaned. Returns stdout as a `String`.
async fn run_lldpcli_async(args: &[&str]) -> LldpCollectorResult<String> {
    let mut command = TokioCommand::new("lldpcli");
    command.args(args).kill_on_drop(true);

    let output = tokio::time::timeout(LLDPCLI_TIMEOUT, command.output())
        .await
        .map_err(|_| {
            LldpCollectorError::Lldp(format!(
                "lldpcli timed out after {}s",
                LLDPCLI_TIMEOUT.as_secs()
            ))
        })?
        .map_err(|e| LldpCollectorError::Lldp(format!("couldn't run lldpcli: {e}")))?;

    if !output.status.success() {
        return Err(LldpCollectorError::Lldp(format!(
            "lldpcli exited unsuccessfully: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    String::from_utf8(output.stdout).map_err(|e| LldpCollectorError::Lldp(e.to_string()))
}

/// Every LLDP neighbor across all interfaces lldpd monitors.
///
/// Returns an empty vec when no neighbor is advertised.
fn get_all_lldp_neighbors_sync() -> LldpCollectorResult<Vec<rpc_discovery::LldpSwitchData>> {
    let out = run_lldpcli_sync(&["-f", "json0", "show", "neighbors", "details"]).map_err(|e| {
        warn!(error = %e, "Could not discover LLDP neighbors");
        e
    })?;
    parse_lldp_neighbors(&out)
}

/// Async counterpart of [`get_all_lldp_neighbors_sync`].
async fn get_all_lldp_neighbors_async() -> LldpCollectorResult<Vec<rpc_discovery::LldpSwitchData>> {
    let out = run_lldpcli_async(&["-f", "json0", "show", "neighbors", "details"])
        .await
        .map_err(|e| {
            warn!(error = %e, "Could not discover LLDP neighbors");
            e
        })?;
    parse_lldp_neighbors(&out)
}

/// True when a neighbor's chassis id (type + value) matches our own local
/// chassis id, i.e. the "neighbor" is this box itself via an internal loopback
/// (e.g. a DPU e-switch representor pair).
fn is_self_loopback(neighbor: &rpc_discovery::LldpSwitchData, own: &LldpId) -> bool {
    let is_self = neighbor.id_type == own.id_type && neighbor.id_value == own.value;
    if is_self {
        debug!(
            local_port = neighbor.local_port,
            "dropping self-loopback LLDP neighbor"
        );
    }
    is_self
}

/// Chassis id this host's lldpd advertises (`lldpcli -f json0 show chassis`).
///
/// `None` when lldpd is unavailable or the output is malformed.
fn get_local_chassis_id_sync() -> Option<LldpId> {
    let out = run_lldpcli_sync(&["-f", "json0", "show", "chassis"])
        .map_err(|e| warn!(error = %e, "Could not read local LLDP chassis"))
        .ok()?;
    parse_local_chassis_id(&out)
}

/// Async counterpart of [`get_local_chassis_id_sync`].
async fn get_local_chassis_id_async() -> Option<LldpId> {
    let out = run_lldpcli_async(&["-f", "json0", "show", "chassis"])
        .await
        .map_err(|e| warn!(error = %e, "Could not read local LLDP chassis"))
        .ok()?;
    parse_local_chassis_id(&out)
}

/// Parse `lldpcli -f json0 show chassis` output down to the first chassis id.
fn parse_local_chassis_id(json: &str) -> Option<LldpId> {
    #[derive(Deserialize, Default)]
    struct LocalChassisEntry {
        #[serde(default)]
        chassis: Vec<LldpChassis>,
    }
    #[derive(Deserialize, Default)]
    struct LocalChassisRoot {
        #[serde(rename = "local-chassis", default)]
        local_chassis: Vec<LocalChassisEntry>,
    }

    let root: LocalChassisRoot = serde_json::from_str(json)
        .map_err(|e| warn!(json, error = %e, "Could not deserialize local LLDP chassis"))
        .ok()?;
    root.local_chassis
        .into_iter()
        .flat_map(|entry| entry.chassis)
        .find_map(|chassis| chassis.id.into_iter().next())
}

fn read_interface_mac(ifname: &str) -> Option<String> {
    let mac = fs::read_to_string(format!("/sys/class/net/{ifname}/address")).ok()?;
    let mac = mac.trim();
    if mac.is_empty() {
        return None;
    }
    Some(mac.to_string())
}

/// Parse `lldpcli -f json0` output into one `LldpSwitchData` per neighbor.
///
/// Each `lldp[].interface[]` entry is a distinct neighbor. Entries advertising no
/// chassis are skipped; every other field is optional — missing chassis/port
/// fields degrade to empty values, absent LLDP-MED inventory to `None`.
fn parse_lldp_neighbors(
    lldp_json: &str,
) -> LldpCollectorResult<Vec<rpc_discovery::LldpSwitchData>> {
    let lldp_resp: LldpResponse = serde_json::from_str(lldp_json).map_err(|e| {
        warn!(lldp_json, error = %e, "Could not deserialize LLDP response");
        LldpCollectorError::Lldp(e.to_string())
    })?;

    let mut neighbors = Vec::new();
    for entry in lldp_resp.lldp.iter().flat_map(|root| root.interface.iter()) {
        let Some(chassis) = entry.chassis.first() else {
            debug!(port = entry.name, "No LLDP chassis data");
            continue;
        };

        let (id_type, id_value) = chassis
            .id
            .first()
            .map(|id| (id.id_type.clone(), id.value.clone()))
            .unwrap_or_default();
        let (remote_port_type, remote_port_value) = entry
            .port
            .first()
            .and_then(|port| port.id.first())
            .map(|id| (id.id_type.clone(), id.value.clone()))
            .unwrap_or_default();

        let med_inventory = entry
            .lldp_med
            .first()
            .and_then(|med| med.inventory.first())
            .map(|inv| {
                let field = |vals: &[LldpValue]| vals.first().map(|v| v.value.clone());
                rpc_discovery::LldpMedInventory {
                    serial: field(&inv.serial),
                    manufacturer: field(&inv.manufacturer),
                    model: field(&inv.model),
                }
            });

        // The `deprecated` allow keeps the legacy combined `id`/`remote_port`
        // strings populated for backward compatibility until consumers migrate
        // to the split *_type/*_value fields.
        #[allow(deprecated)]
        neighbors.push(rpc_discovery::LldpSwitchData {
            name: chassis
                .name
                .first()
                .map(|n| n.value.clone())
                .unwrap_or_default(),
            id: format!("{id_type}={id_value}"),
            description: chassis
                .descr
                .first()
                .map(|d| d.value.clone())
                .unwrap_or_default(),
            local_port: entry.name.clone(),
            ip_address: chassis.mgmt_ip.iter().map(|ip| ip.value.clone()).collect(),
            remote_port: format!("{remote_port_type}={remote_port_value}"),
            id_type,
            id_value,
            remote_port_type,
            remote_port_value,
            med_inventory,
        });
    }

    Ok(neighbors)
}

#[cfg(test)]
mod tests {
    use super::{
        LldpId, is_self_loopback, parse_lldp_neighbors, parse_local_chassis_id, rpc_discovery,
    };

    // Three LLDP neighbors on a single physical port (`vlldp`). `-f json0` emits
    // each as its own `interface` array entry, so `parse_lldp_neighbors` must yield one
    // `LldpSwitchData` per entry, in order, with no mgmt-ip or description.
    const MULTI_NEIGHBOR: &str = r#"{
      "lldp": [
        { "interface": [
          { "name": "vlldp", "chassis": [
              { "id": [{"type":"local","value":"host-00"}], "name": [{"value":"neighbor-00"}] }],
            "port": [{ "id": [{"type":"ifname","value":"port-00"}], "ttl": [{"value":"120"}] }] },
          { "name": "vlldp", "chassis": [
              { "id": [{"type":"local","value":"host-01"}], "name": [{"value":"neighbor-01"}] }],
            "port": [{ "id": [{"type":"ifname","value":"port-01"}], "ttl": [{"value":"120"}] }] },
          { "name": "vlldp", "chassis": [
              { "id": [{"type":"local","value":"host-02"}], "name": [{"value":"neighbor-02"}] }],
            "port": [{ "id": [{"type":"ifname","value":"port-02"}], "ttl": [{"value":"120"}] }] }
        ] }
      ]
    }"#;

    // Single neighbor carrying two mgmt-ips (v4 + v6) and a description — the shape
    // of a real `lldpcli -f json0 show neighbors ports p0`.
    const SINGLE_NEIGHBOR: &str = r#"{
      "lldp": [
        { "interface": [
          { "name": "p0", "chassis": [
              { "id": [{"type":"mac","value":"00:11:22:33:44:55"}],
                "name": [{"value":"example-switch-01"}],
                "descr": [{"value":"Cumulus Linux version 5.11.1 running on Mellanox switch"}],
                "mgmt-ip": [{"value":"192.0.2.10"},{"value":"2001:db8::10"}] }],
            "port": [{ "id": [{"type":"ifname","value":"swp2"}], "ttl": [{"value":"120"}] }] }
        ] }
      ]
    }"#;

    #[test]
    #[allow(deprecated)]
    fn parses_multiple_neighbors_on_one_port() {
        let neighbors = parse_lldp_neighbors(MULTI_NEIGHBOR).expect("parse");
        assert_eq!(neighbors.len(), 3);
        for (i, n) in neighbors.iter().enumerate() {
            assert_eq!(n.name, format!("neighbor-0{i}"));
            // split fields plus the legacy combined strings
            assert_eq!(n.id_type, "local");
            assert_eq!(n.id_value, format!("host-0{i}"));
            assert_eq!(n.id, format!("local=host-0{i}"));
            assert_eq!(n.remote_port_type, "ifname");
            assert_eq!(n.remote_port_value, format!("port-0{i}"));
            assert_eq!(n.remote_port, format!("ifname=port-0{i}"));
            assert_eq!(n.local_port, "vlldp");
            assert!(n.ip_address.is_empty());
            assert!(n.description.is_empty());
        }
    }

    #[test]
    #[allow(deprecated)]
    fn parses_single_neighbor_with_mgmt_ips() {
        let neighbors = parse_lldp_neighbors(SINGLE_NEIGHBOR).expect("parse");
        assert_eq!(neighbors.len(), 1);
        let n = &neighbors[0];
        assert_eq!(n.name, "example-switch-01");
        assert_eq!(n.id_type, "mac");
        assert_eq!(n.id_value, "00:11:22:33:44:55");
        assert_eq!(n.id, "mac=00:11:22:33:44:55");
        assert_eq!(n.local_port, "p0");
        assert_eq!(n.remote_port_type, "ifname");
        assert_eq!(n.remote_port_value, "swp2");
        assert_eq!(n.remote_port, "ifname=swp2");
        assert_eq!(n.ip_address, vec!["192.0.2.10", "2001:db8::10"]);
        assert!(n.description.contains("Cumulus Linux"));
    }

    // Neighbor advertising LLDP-MED inventory (a BlueField DPU). serial /
    // manufacturer / model live under `lldp-med[].inventory[]`.
    const INVENTORY_NEIGHBOR: &str = r#"{
      "lldp": [
        { "interface": [
          { "name": "enp1s0np0", "chassis": [
              { "id": [{"type":"mac","value":"00:11:22:33:44:55"}],
                "name": [{"value":"example-dpu-01"}] }],
            "port": [{ "id": [{"type":"mac","value":"00:11:22:33:44:66"}], "ttl": [{"value":"120"}] }],
            "lldp-med": [{ "inventory": [{
                "serial": [{"value":"SN0123456789"}],
                "manufacturer": [{"value":"https://example.com"}],
                "model": [{"value":"BlueField-3 DPU"}] }] }] }
        ] }
      ]
    }"#;

    #[test]
    fn parses_lldp_med_inventory() {
        let neighbors = parse_lldp_neighbors(INVENTORY_NEIGHBOR).expect("parse");
        assert_eq!(neighbors.len(), 1);
        let inv = neighbors[0].med_inventory.as_ref().expect("inventory");
        assert_eq!(inv.serial.as_deref(), Some("SN0123456789"));
        assert_eq!(inv.manufacturer.as_deref(), Some("https://example.com"));
        assert_eq!(inv.model.as_deref(), Some("BlueField-3 DPU"));
    }

    #[test]
    fn parses_missing_inventory_as_none() {
        let neighbors = parse_lldp_neighbors(SINGLE_NEIGHBOR).expect("parse");
        assert_eq!(neighbors.len(), 1);
        assert!(neighbors[0].med_inventory.is_none());
    }

    #[test]
    fn parses_no_neighbors_as_empty() {
        let neighbors = parse_lldp_neighbors(r#"{"lldp":[{"interface":[]}]}"#).expect("parse");
        assert!(neighbors.is_empty());
    }

    // Shape of `lldpcli -f json0 show chassis` on a DPU.
    const LOCAL_CHASSIS: &str = r#"{
      "local-chassis": [
        { "chassis": [
            { "id": [{"type":"mac","value":"58:a2:e1:54:6f:ae"}],
              "name": [{"value":"10-213-2-193.forge.local"}],
              "descr": [{"value":"Forge-SRE"}] }] }
      ]
    }"#;

    #[test]
    fn parses_local_chassis_id() {
        let id = parse_local_chassis_id(LOCAL_CHASSIS).expect("chassis id");
        assert_eq!(id.id_type, "mac");
        assert_eq!(id.value, "58:a2:e1:54:6f:ae");
    }

    #[test]
    fn parses_local_chassis_id_absent() {
        assert!(parse_local_chassis_id(r#"{"local-chassis":[{"chassis":[{}]}]}"#).is_none());
        assert!(parse_local_chassis_id("not json").is_none());
    }

    // Representor pairs on a DPU report the DPU's own chassis as the neighbor;
    // only a matching (type, value) pair marks the loopback — a genuinely
    // different switch must be kept.
    #[test]
    fn self_loopback_detected_by_chassis_id() {
        let own = LldpId {
            id_type: "mac".into(),
            value: "58:a2:e1:54:6f:ae".into(),
        };
        let neighbor = |id_type: &str, id_value: &str| rpc_discovery::LldpSwitchData {
            id_type: id_type.into(),
            id_value: id_value.into(),
            ..Default::default()
        };

        assert!(is_self_loopback(
            &neighbor("mac", "58:a2:e1:54:6f:ae"),
            &own
        ));
        // different chassis value -> genuine external link
        assert!(!is_self_loopback(
            &neighbor("mac", "24:8a:07:b4:41:aa"),
            &own
        ));
        // same string value but different id type -> not self
        assert!(!is_self_loopback(
            &neighbor("local", "58:a2:e1:54:6f:ae"),
            &own
        ));
    }
}
