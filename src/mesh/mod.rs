//! Host-side WirelessHART model: admission, network graph, routes, and schedule.

mod manager;
mod security;
mod topology;

pub use manager::{JoinDecision, JoinRequest, MeshManager, MeshManagerError, MeshSnapshot};
pub use security::{KeyMaterial, ReplayWindow, SecurityError};
pub use topology::{
    Cell, CellDirection, DeviceId, LinkHealth, MeshGraph, NodeState, Route, Schedule, ScheduleError,
};

/// WirelessHART network identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NetworkId(pub u16);
