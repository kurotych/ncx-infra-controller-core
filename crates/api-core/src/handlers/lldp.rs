use std::collections::HashMap;

use ::rpc::forge as rpc;
use db::machine_interface_lldp::MachineInterfaceLldp;
use model::hardware_info::LldpSwitchData;
use tonic::{Request, Response, Status};

use crate::CarbideError;
use crate::api::{Api, log_request_data};
use crate::handlers::utils::convert_and_log_machine_id;

/// Persist a periodic LLDP neighbor report from a running agent (host scout or
/// DPU agent). The report is a full snapshot of the machine's current
/// per-interface neighbors.
///
/// The stored set is diffed against the report first; only a real change
/// triggers a write (delete-all + insert), so the common unchanged report costs
/// a single SELECT and no row churn. A neighbor that disappears (or all of them)
/// is dropped by the replace.
///
/// Writes only `machine_interface_lldp` table.
pub(crate) async fn report_lldp_neighbors(
    api: &Api,
    request: Request<rpc::LldpNeighborReport>,
) -> Result<Response<()>, Status> {
    log_request_data(&request);

    let req = request.into_inner();
    let machine_id = convert_and_log_machine_id(req.machine_id.as_ref())?;

    // Desired set from the report; interfaces without a neighbor are omitted.
    let mut desired: Vec<(String, LldpSwitchData)> = Vec::with_capacity(req.interfaces.len());
    for iface in req.interfaces {
        let Some(lldp) = iface.lldp else {
            continue;
        };
        let lldp = LldpSwitchData::try_from(lldp).map_err(CarbideError::from)?;
        desired.push((iface.mac_address, lldp));
    }

    let mut txn = api.txn_begin().await?;
    let existing =
        db::machine_interface_lldp::find_by_machine_id(txn.as_pgconn(), &machine_id).await?;

    if !neighbor_set_changed(&existing, &desired) {
        tracing::trace!(%machine_id, "LLDP neighbors unchanged; skipping write");
        return Ok(Response::new(()));
    }

    db::machine_interface_lldp::replace_all(&mut txn, &machine_id, &desired).await?;
    txn.commit().await?;

    tracing::debug!(
        %machine_id,
        interfaces = desired.len(),
        "updated LLDP neighbors",
    );

    Ok(Response::new(()))
}

/// Compare the stored neighbor set with the reported one, keyed by local MAC and
/// ignoring the `updated` timestamp. Order-independent.
fn neighbor_set_changed(
    existing: &[MachineInterfaceLldp],
    desired: &[(String, LldpSwitchData)],
) -> bool {
    if existing.len() != desired.len() {
        return true;
    }

    // (local_port, remote_port, switch_name, switch_id, switch_description, mgmt_ips)
    type Fields<'a> = (&'a str, &'a str, &'a str, &'a str, &'a str, &'a [String]);

    let existing_map: HashMap<&str, Fields> = existing
        .iter()
        .map(|r| {
            (
                r.local_mac_address.as_str(),
                (
                    r.local_port.as_str(),
                    r.remote_port.as_str(),
                    r.switch_name.as_str(),
                    r.switch_id.as_str(),
                    r.switch_description.as_str(),
                    r.switch_mgmt_ips.as_slice(),
                ),
            )
        })
        .collect();

    let desired_map: HashMap<&str, Fields> = desired
        .iter()
        .map(|(mac, l)| {
            (
                mac.as_str(),
                (
                    l.local_port.as_str(),
                    l.remote_port.as_str(),
                    l.name.as_str(),
                    l.id.as_str(),
                    l.description.as_str(),
                    l.ip_address.as_slice(),
                ),
            )
        })
        .collect();

    existing_map != desired_map
}
