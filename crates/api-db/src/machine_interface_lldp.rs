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

/// Replace the full LLDP neighbor set for a machine: delete all existing rows,
/// then insert `neighbors`. Atomic within `txn`.
///
/// Intended to be called only when the set actually changed (the handler diffs
/// against the stored set first), so unchanged periodic reports cost no writes.
/// `neighbors` is `(local_mac_address, neighbor)`.
pub async fn replace_all(
    txn: &mut PgConnection,
    machine_id: &MachineId,
    neighbors: &[(String, LldpSwitchData)],
) -> DatabaseResult<()> {
    let delete = "DELETE FROM machine_interface_lldp WHERE machine_id=$1";
    sqlx::query(delete)
        .bind(machine_id)
        .execute(&mut *txn)
        .await
        .map_err(|e| DatabaseError::query(delete, e))?;

    // Nothing to insert: an empty VALUES list is invalid SQL, and the delete
    // above already cleared the machine's neighbors.
    if neighbors.is_empty() {
        return Ok(());
    }

    // Single multi-row INSERT (one round-trip) via QueryBuilder::push_values.
    let mut builder = sqlx::QueryBuilder::<sqlx::Postgres>::new(
        "INSERT INTO machine_interface_lldp \
         (machine_id, local_mac_address, local_port, remote_port, \
          switch_name, switch_id, switch_description, switch_mgmt_ips, updated) ",
    );
    builder.push_values(neighbors, |mut row, (local_mac_address, lldp)| {
        row.push_bind(machine_id)
            .push_bind(local_mac_address)
            .push_bind(&lldp.local_port)
            .push_bind(&lldp.remote_port)
            .push_bind(&lldp.name)
            .push_bind(&lldp.id)
            .push_bind(&lldp.description)
            .push_bind(&lldp.ip_address)
            .push("now()");
    });

    builder
        .build()
        .execute(&mut *txn)
        .await
        .map_err(|e| DatabaseError::query(builder.sql(), e))?;

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
