/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use ::rpc::forge as rpc;
use model::hardware_info::LldpSwitchData;
use tonic::{Request, Response, Status};

use crate::CarbideError;
use crate::api::{Api, log_request_data};
use crate::handlers::utils::convert_and_log_machine_id;

/// Persist a periodic LLDP neighbor report from a running agent (host scout or
/// DPU agent). The report is a full snapshot of the machine's current
/// per-interface neighbors: present interfaces are upserted and interfaces no
/// longer reported are removed (reconcile), so stale neighbors don't linger.
///
/// Writes only `machine_interface_lldp`; the discovery-time
/// `network_devices`/`port_to_network_device_map` tables are untouched.
pub(crate) async fn report_lldp_neighbors(
    api: &Api,
    request: Request<rpc::LldpNeighborReport>,
) -> Result<Response<()>, Status> {
    log_request_data(&request);

    let req = request.into_inner();
    let machine_id = convert_and_log_machine_id(req.machine_id.as_ref())?;

    let mut txn = api.txn_begin().await?;

    let mut reported_macs: Vec<String> = Vec::with_capacity(req.interfaces.len());
    for iface in req.interfaces {
        let Some(lldp) = iface.lldp else {
            // No neighbor for this interface; nothing to store (it will be
            // reconciled away below if a row existed).
            continue;
        };
        let lldp = LldpSwitchData::try_from(lldp).map_err(CarbideError::from)?;
        db::machine_interface_lldp::upsert(&mut txn, &machine_id, &iface.mac_address, &lldp)
            .await?;
        reported_macs.push(iface.mac_address);
    }

    db::machine_interface_lldp::delete_missing(&mut txn, &machine_id, &reported_macs).await?;

    txn.commit().await?;

    tracing::debug!(
        %machine_id,
        interfaces = reported_macs.len(),
        "reported LLDP neighbors",
    );

    Ok(Response::new(()))
}
