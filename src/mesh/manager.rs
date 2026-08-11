use std::collections::{BTreeMap, BTreeSet};

use thiserror::Error;

use crate::mesh::{DeviceId, KeyMaterial, MeshGraph, NetworkId, NodeState, ReplayWindow, Schedule};

/// Field-device join request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JoinRequest {
    /// Unique device identifier.
    pub device: DeviceId,
    /// Claimed network identifier.
    pub network: NetworkId,
    /// Request security counter.
    pub security_counter: u32,
}

/// Network-manager decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinDecision {
    /// The device was admitted and assigned a short address.
    Accepted {
        /// Assigned short address.
        nickname: u16,
    },
    /// The device is absent from the allow list.
    NotAllowed,
    /// The device requested a different network.
    WrongNetwork,
    /// The caller-supplied cryptographic verifier rejected the request.
    AuthenticationFailed,
}

/// Network-manager state summary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MeshSnapshot {
    /// Number of active nodes.
    pub nodes: usize,
    /// Number of measured links.
    pub links: usize,
    /// Number of schedule cells.
    pub cells: usize,
}

/// Host-side network-manager state.
pub struct MeshManager {
    network: NetworkId,
    graph: MeshGraph,
    schedule: Schedule,
    allowlist: BTreeSet<DeviceId>,
    join_keys: BTreeMap<DeviceId, KeyMaterial>,
    replay: BTreeMap<DeviceId, ReplayWindow>,
    next_nickname: u16,
}

impl core::fmt::Debug for MeshManager {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("MeshManager")
            .field("network", &self.network)
            .field("graph", &self.graph)
            .field("schedule", &self.schedule)
            .field("allowlist", &self.allowlist)
            .field("join_keys", &"[redacted]")
            .field("replay", &self.replay)
            .field("next_nickname", &self.next_nickname)
            .finish()
    }
}

impl MeshManager {
    /// Creates a manager for the specified network and superframe.
    pub fn new(network: NetworkId, superframe_size: u16) -> Result<Self, MeshManagerError> {
        Ok(Self {
            network,
            graph: MeshGraph::default(),
            schedule: Schedule::new(superframe_size)?,
            allowlist: BTreeSet::new(),
            join_keys: BTreeMap::new(),
            replay: BTreeMap::new(),
            next_nickname: 1,
        })
    }

    /// Allows a device to join and stores its individual key.
    ///
    /// Replacing an existing key invalidates replay, graph, and schedule state so the device
    /// must authenticate a fresh join with the new material.
    pub fn allow(&mut self, device: DeviceId, join_key: KeyMaterial) {
        self.allowlist.insert(device);
        if self.join_keys.insert(device, join_key).is_some() {
            self.replay.remove(&device);
            self.graph.remove_node(device);
            self.schedule.remove_device(device);
        }
    }

    /// Permanently removes device admission, key material, and active state.
    pub fn revoke(&mut self, device: DeviceId) {
        self.allowlist.remove(&device);
        self.join_keys.remove(&device);
        self.replay.remove(&device);
        self.graph.remove_node(device);
        self.schedule.remove_device(device);
    }

    fn apply_join(&mut self, request: JoinRequest) -> Result<JoinDecision, MeshManagerError> {
        if request.network != self.network {
            return Ok(JoinDecision::WrongNetwork);
        }
        if !self.allowlist.contains(&request.device)
            || !self.join_keys.contains_key(&request.device)
        {
            return Ok(JoinDecision::NotAllowed);
        }
        if let Some(node) = self.graph.node(request.device)
            && node.active
        {
            self.replay
                .entry(request.device)
                .or_default()
                .accept(request.security_counter)?;
            return Ok(JoinDecision::Accepted {
                nickname: node.nickname,
            });
        }
        if self.next_nickname == 0 {
            return Err(MeshManagerError::AddressSpace);
        }
        self.replay
            .entry(request.device)
            .or_default()
            .accept(request.security_counter)?;
        let nickname = self.next_nickname;
        self.next_nickname = self.next_nickname.checked_add(1).unwrap_or(0);
        self.graph.upsert_node(NodeState {
            id: request.device,
            nickname,
            active: true,
        });
        Ok(JoinDecision::Accepted { nickname })
    }

    /// Authenticates a join with the provisioned per-device key before changing network state.
    ///
    /// The verifier owns the exact WirelessHART cryptographic implementation and receives the
    /// request plus temporary read-only key bytes. Returning `false` leaves replay, graph, and
    /// nickname state unchanged.
    pub fn join_authenticated(
        &mut self,
        request: JoinRequest,
        authenticate: impl FnOnce(&JoinRequest, &[u8; 16]) -> bool,
    ) -> Result<JoinDecision, MeshManagerError> {
        if request.network != self.network {
            return Ok(JoinDecision::WrongNetwork);
        }
        let Some(join_key) = self.join_keys.get(&request.device) else {
            return Ok(JoinDecision::NotAllowed);
        };
        if !self.allowlist.contains(&request.device) {
            return Ok(JoinDecision::NotAllowed);
        }
        if !authenticate(&request, join_key.expose()) {
            return Ok(JoinDecision::AuthenticationFailed);
        }
        self.apply_join(request)
    }

    /// Provides mutable graph access for loading neighbor reports.
    pub const fn graph_mut(&mut self) -> &mut MeshGraph {
        &mut self.graph
    }

    /// Provides mutable schedule access.
    pub const fn schedule_mut(&mut self) -> &mut Schedule {
        &mut self.schedule
    }

    /// Returns a safe summary without key material.
    pub fn snapshot(&self) -> MeshSnapshot {
        MeshSnapshot {
            nodes: self.graph.node_count(),
            links: self.graph.link_count(),
            cells: self.schedule.cells().len(),
        }
    }
}

/// Network-manager error.
#[derive(Debug, Error)]
pub enum MeshManagerError {
    /// Schedule error.
    #[error(transparent)]
    Schedule(#[from] crate::mesh::ScheduleError),
    /// Replay protection was violated.
    #[error(transparent)]
    Security(#[from] crate::mesh::SecurityError),
    /// The short-address space was exhausted.
    #[error("short network-address space exhausted")]
    AddressSpace,
}
