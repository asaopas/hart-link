//! Software-tested WirelessHART topology, route, and schedule state.
//!
//! Real gateway, radio, and Network Manager validation is still pending; see
//! the parent [`crate::mesh`] module for scope and reporting guidance.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use thiserror::Error;

/// Eight-byte unique network-device identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DeviceId(pub [u8; 8]);

/// Measured quality of a neighbor link.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LinkHealth {
    /// Received signal level in decibels relative to one milliwatt.
    pub rssi_dbm: i16,
    /// Successful-packet ratio in `0.0..=1.0`.
    pub reliability: f32,
    /// Measurement age in seconds.
    pub age_seconds: u32,
}

/// Current node state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeState {
    /// Unique identifier.
    pub id: DeviceId,
    /// Short network address.
    pub nickname: u16,
    /// Whether the device may transmit application data.
    pub active: bool,
}

/// Undirected neighbor graph.
#[derive(Debug, Clone, Default)]
pub struct MeshGraph {
    nodes: BTreeMap<DeviceId, NodeState>,
    links: BTreeMap<(DeviceId, DeviceId), LinkHealth>,
    adjacency: BTreeMap<DeviceId, BTreeSet<DeviceId>>,
}

impl MeshGraph {
    /// Adds or updates a node.
    pub fn upsert_node(&mut self, node: NodeState) -> Option<NodeState> {
        self.adjacency.entry(node.id).or_default();
        self.nodes.insert(node.id, node)
    }

    /// Removes a node and all of its links.
    pub fn remove_node(&mut self, id: DeviceId) -> Option<NodeState> {
        if let Some(neighbors) = self.adjacency.remove(&id) {
            for neighbor in neighbors {
                if let Some(entries) = self.adjacency.get_mut(&neighbor) {
                    entries.remove(&id);
                }
            }
        }
        self.links
            .retain(|(left, right), _| *left != id && *right != id);
        self.nodes.remove(&id)
    }

    /// Updates the symmetric link between two existing nodes.
    pub fn update_link(&mut self, left: DeviceId, right: DeviceId, health: LinkHealth) -> bool {
        if left == right
            || !health.reliability.is_finite()
            || !(0.0..=1.0).contains(&health.reliability)
            || !self.nodes.contains_key(&left)
            || !self.nodes.contains_key(&right)
        {
            return false;
        }
        self.links.insert(ordered_pair(left, right), health);
        self.adjacency.entry(left).or_default().insert(right);
        self.adjacency.entry(right).or_default().insert(left);
        true
    }

    /// Builds a minimum-hop route subject to a reliability threshold.
    pub fn route(
        &self,
        source: DeviceId,
        destination: DeviceId,
        minimum_reliability: f32,
    ) -> Option<Route> {
        if !minimum_reliability.is_finite() || !(0.0..=1.0).contains(&minimum_reliability) {
            return None;
        }
        if !self.nodes.get(&source).is_some_and(|node| node.active)
            || !self.nodes.get(&destination).is_some_and(|node| node.active)
        {
            return None;
        }
        if source == destination {
            return Some(Route { hops: vec![source] });
        }
        let mut queue = VecDeque::from([source]);
        let mut previous = BTreeMap::new();
        let mut visited = BTreeSet::from([source]);
        while let Some(current) = queue.pop_front() {
            for neighbor in self.neighbors(current, minimum_reliability) {
                if !visited.insert(neighbor) {
                    continue;
                }
                previous.insert(neighbor, current);
                if neighbor == destination {
                    return reconstruct_route(source, destination, &previous);
                }
                queue.push_back(neighbor);
            }
        }
        None
    }

    /// Returns the node count.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Returns the link count.
    pub fn link_count(&self) -> usize {
        self.links.len()
    }

    /// Returns a node by its unique identifier.
    pub fn node(&self, id: DeviceId) -> Option<&NodeState> {
        self.nodes.get(&id)
    }

    fn neighbors(
        &self,
        id: DeviceId,
        minimum_reliability: f32,
    ) -> impl Iterator<Item = DeviceId> + '_ {
        self.adjacency
            .get(&id)
            .into_iter()
            .flatten()
            .copied()
            .filter(move |candidate| {
                self.links
                    .get(&ordered_pair(id, *candidate))
                    .is_some_and(|health| health.reliability >= minimum_reliability)
                    && self.nodes.get(candidate).is_some_and(|node| node.active)
            })
    }
}

/// Node sequence from source through destination, inclusive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Route {
    /// Path in transmission order.
    pub hops: Vec<DeviceId>,
}

/// Direction assigned to a cell.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CellDirection {
    /// Transmission from source to peer.
    Transmit,
    /// Reception by source from peer.
    Receive,
    /// Shared cell for management traffic.
    Shared,
}

/// One cell in a time-frequency schedule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cell {
    /// Superframe timeslot number.
    pub timeslot: u16,
    /// Offset within the channel table.
    pub channel_offset: u8,
    /// First link participant.
    pub source: DeviceId,
    /// Second link participant.
    pub destination: DeviceId,
    /// Cell purpose.
    pub direction: CellDirection,
}

/// Validated schedule without per-device conflicts.
#[derive(Debug, Clone, Default)]
pub struct Schedule {
    superframe_size: u16,
    cells: Vec<Cell>,
}

impl Schedule {
    /// Creates an empty superframe.
    pub fn new(superframe_size: u16) -> Result<Self, ScheduleError> {
        if superframe_size == 0 {
            return Err(ScheduleError::EmptySuperframe);
        }
        Ok(Self {
            superframe_size,
            cells: Vec::new(),
        })
    }

    /// Adds a cell after validating bounds and conflicts.
    pub fn insert(&mut self, cell: Cell) -> Result<(), ScheduleError> {
        if cell.timeslot >= self.superframe_size {
            return Err(ScheduleError::Timeslot(cell.timeslot));
        }
        if cell.channel_offset > 15 {
            return Err(ScheduleError::Channel(cell.channel_offset));
        }
        if cell.source == cell.destination {
            return Err(ScheduleError::Loopback);
        }
        if self.cells.iter().any(|existing| {
            existing.timeslot == cell.timeslot
                && (existing.source == cell.source
                    || existing.source == cell.destination
                    || existing.destination == cell.source
                    || existing.destination == cell.destination)
        }) {
            return Err(ScheduleError::Conflict(cell.timeslot));
        }
        if self.cells.iter().any(|existing| {
            existing.timeslot == cell.timeslot && existing.channel_offset == cell.channel_offset
        }) {
            return Err(ScheduleError::RadioConflict {
                timeslot: cell.timeslot,
                channel: cell.channel_offset,
            });
        }
        let key = (cell.timeslot, cell.channel_offset);
        let index = self
            .cells
            .binary_search_by_key(&key, |value| (value.timeslot, value.channel_offset))
            .unwrap_or_else(|index| index);
        self.cells.insert(index, cell);
        Ok(())
    }

    /// Removes every cell involving a device and returns the number removed.
    pub fn remove_device(&mut self, device: DeviceId) -> usize {
        let previous = self.cells.len();
        self.cells
            .retain(|cell| cell.source != device && cell.destination != device);
        previous.saturating_sub(self.cells.len())
    }

    /// Returns cells in superframe order.
    pub fn cells(&self) -> &[Cell] {
        &self.cells
    }

    /// Returns the superframe length.
    pub const fn superframe_size(&self) -> u16 {
        self.superframe_size
    }
}

/// Schedule-construction error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ScheduleError {
    /// The superframe length is zero.
    #[error("superframe length cannot be zero")]
    EmptySuperframe,
    /// A timeslot lies outside the superframe.
    #[error("timeslot {0} lies outside the superframe")]
    Timeslot(u16),
    /// A channel offset lies outside the table.
    #[error("channel offset {0} is outside 0..=15")]
    Channel(u8),
    /// The same device appears on both sides of a link.
    #[error("a cell cannot connect a device to itself")]
    Loopback,
    /// A device is already occupied in the same timeslot.
    #[error("device conflict in timeslot {0}")]
    Conflict(u16),
    /// Two independent links attempt to use the same time-frequency cell.
    #[error("radio conflict in timeslot {timeslot}, channel offset {channel}")]
    RadioConflict {
        /// Conflicting timeslot.
        timeslot: u16,
        /// Conflicting channel offset.
        channel: u8,
    },
}

fn ordered_pair(left: DeviceId, right: DeviceId) -> (DeviceId, DeviceId) {
    if left <= right {
        (left, right)
    } else {
        (right, left)
    }
}

fn reconstruct_route(
    source: DeviceId,
    destination: DeviceId,
    previous: &BTreeMap<DeviceId, DeviceId>,
) -> Option<Route> {
    let mut hops = vec![destination];
    let mut current = destination;
    while current != source {
        current = *previous.get(&current)?;
        hops.push(current);
    }
    hops.reverse();
    Some(Route { hops })
}
