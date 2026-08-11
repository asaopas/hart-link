//! Host-side WirelessHART model: admission, network graph, routes, and schedule.
//!
//! # Hardware validation status
//!
//! The state machines, resource limits, key erasure, replay window, topology,
//! routes, and schedule validation are covered by software tests. This crate is
//! not currently exercised against real WirelessHART radios and Network
//! Managers. The feature therefore does not claim complete over-the-air
//! interoperability or FieldComm conformance. This limitation does not apply
//! to ordinary wired HART, which has been exercised through a transparent Moxa
//! TCP gateway and directly through a USB HART modem.
//!
//! If a real installation behaves differently, please open a
//! [WirelessHART hardware report](https://github.com/asaopas/hart-link/issues/new?template=wirelesshart-hardware.yml).
//! Include the crate version, enabled features, gateway and device firmware,
//! the expected and actual event sequence, timestamps, sanitized logs, and the
//! smallest raw capture or byte dump that reproduces the problem.
//!
//! Never attach join keys, network or session keys, passwords, private
//! certificates, licensed specification text, proprietary DeviceInfo content,
//! or unrelated production traffic. Replace secrets with fixed placeholders
//! without changing message lengths or ordering. A useful report can be turned
//! into a regression test before the implementation is adjusted.

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
