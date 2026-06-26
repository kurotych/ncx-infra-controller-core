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

//! Per-interface LLDP neighbor data, reported periodically by running agents
//! (host scout + DPU agent). Keyed by the machine's own (local) NIC MAC.
//!
//! This is independent of `network_devices` / `port_to_network_device_map`,
//! which remain discovery-only and unchanged.

use carbide_uuid::machine::MachineId;
use chrono::{DateTime, Utc};
use model::hardware_info::LldpSwitchData;
use sqlx::PgConnection;
use sqlx::prelude::FromRow;

use crate::db_read::DbReader;
use crate::{DatabaseError, DatabaseResult};

/// A stored LLDP neighbor observed on one local interface of a machine.
#[derive(Debug, Clone, FromRow)]
pub struct MachineInterfaceLldp {
    pub machine_id: MachineId,
    pub local_mac_address: String,
    pub local_port: String,
    pub remote_port: String,
    pub switch_name: String,
    pub switch_id: String,
    pub switch_description: String,
    pub switch_mgmt_ips: Vec<String>,
    pub updated: DateTime<Utc>,
}

/// Insert or update the neighbor for one (machine, local MAC) pair.
pub async fn upsert(
    txn: &mut PgConnection,
    machine_id: &MachineId,
    local_mac_address: &str,
    lldp: &LldpSwitchData,
) -> DatabaseResult<()> {
    let query = r#"INSERT INTO machine_interface_lldp
            (machine_id, local_mac_address, local_port, remote_port,
             switch_name, switch_id, switch_description, switch_mgmt_ips, updated)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now())
        ON CONFLICT (machine_id, local_mac_address) DO UPDATE SET
            local_port=EXCLUDED.local_port,
            remote_port=EXCLUDED.remote_port,
            switch_name=EXCLUDED.switch_name,
            switch_id=EXCLUDED.switch_id,
            switch_description=EXCLUDED.switch_description,
            switch_mgmt_ips=EXCLUDED.switch_mgmt_ips,
            updated=now()"#;

    sqlx::query(query)
        .bind(machine_id)
        .bind(local_mac_address)
        .bind(&lldp.local_port)
        .bind(&lldp.remote_port)
        .bind(&lldp.name)
        .bind(&lldp.id)
        .bind(&lldp.description)
        .bind(&lldp.ip_address)
        .execute(txn)
        .await
        .map_err(|e| DatabaseError::query(query, e))?;

    Ok(())
}

/// Drop rows for `machine_id` whose local MAC is not in `keep_macs`.
///
/// Reconciles a full-snapshot report: interfaces whose neighbor disappeared
/// (or whole machine going neighbor-less, i.e. empty `keep_macs`) are removed.
pub async fn delete_missing(
    txn: &mut PgConnection,
    machine_id: &MachineId,
    keep_macs: &[String],
) -> DatabaseResult<()> {
    // `x <> ALL('{}')` is true for every row, so an empty keep-set clears the machine.
    let query =
        "DELETE FROM machine_interface_lldp WHERE machine_id=$1 AND local_mac_address <> ALL($2)";

    sqlx::query(query)
        .bind(machine_id)
        .bind(keep_macs)
        .execute(txn)
        .await
        .map_err(|e| DatabaseError::query(query, e))?;

    Ok(())
}

/// All stored LLDP neighbors for a machine.
pub async fn find_by_machine_id(
    txn: impl DbReader<'_>,
    machine_id: &MachineId,
) -> DatabaseResult<Vec<MachineInterfaceLldp>> {
    let query = "SELECT * FROM machine_interface_lldp WHERE machine_id=$1";

    sqlx::query_as(query)
        .bind(machine_id)
        .fetch_all(txn)
        .await
        .map_err(|e| DatabaseError::query(query, e))
}
