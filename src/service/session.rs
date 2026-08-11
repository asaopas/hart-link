use thiserror::Error;

use crate::{
    Address,
    operation::{CommandOutcome, DeviceIdentity, Operation, OperationError, ReadDeviceIdentity},
    service::{ExchangeError, LinkClient, Priority, RetryPolicy},
    wire::WireError,
};

/// Learned parameters of a specific device.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionProfile {
    /// Identification data returned by Command 0.
    pub identity: DeviceIdentity,
    /// Address used after identification.
    pub address: Address,
    /// Preamble count for subsequent requests.
    pub request_preambles: u8,
}

/// Error opening or using a session.
#[derive(Debug, Error)]
pub enum SessionError {
    /// Exchange error.
    #[error(transparent)]
    Exchange(#[from] ExchangeError),
    /// Application-format error.
    #[error(transparent)]
    Operation(#[from] OperationError),
    /// The identity cannot be represented as a unique HART address.
    #[error(transparent)]
    Address(#[from] WireError),
    /// Initial identification preambles are outside the practical line range.
    #[error("initial preamble count {0} is outside 2..=40")]
    Preambles(u8),
}

/// Identified device using a shared link queue.
#[derive(Debug, Clone)]
pub struct DeviceSession {
    link: LinkClient,
    profile: SessionProfile,
}

impl DeviceSession {
    /// Runs Command 0 using the link client's configured defaults.
    pub async fn identify_default(
        link: LinkClient,
        address: Address,
    ) -> Result<Self, SessionError> {
        let identity = link.execute_default(address, &ReadDeviceIdentity).await?;
        Self::from_identity(link, address, identity)
    }

    /// Runs Command 0 and creates a profile with an adaptive preamble count.
    pub async fn identify(
        link: LinkClient,
        address: Address,
        policy: RetryPolicy,
    ) -> Result<Self, SessionError> {
        let identity = link
            .execute(address, &ReadDeviceIdentity, Priority::Service, policy)
            .await?;
        Self::from_identity(link, address, identity)
    }

    /// Runs Command 0 with a conservative caller-selected initial preamble count.
    pub async fn identify_with_preambles(
        link: LinkClient,
        address: Address,
        preambles: u8,
        policy: RetryPolicy,
    ) -> Result<Self, SessionError> {
        if !(2..=40).contains(&preambles) {
            return Err(SessionError::Preambles(preambles));
        }
        let operation = ReadDeviceIdentity;
        let request = operation.request(address)?.with_preambles(preambles);
        let reply = link.request(request, Priority::Service, policy).await?;
        let identity = operation.decode_reply(&reply)?;
        Self::from_identity(link, address, identity)
    }

    /// Creates a session from an identity already returned by discovery or recovery.
    pub fn from_identity(
        link: LinkClient,
        address: Address,
        identity: DeviceIdentity,
    ) -> Result<Self, SessionError> {
        let request_preambles = identity.request_preambles.max(2);
        let address = match address {
            Address::Polling { master, .. } => identity.unique_address(master)?,
            Address::Unique { .. } => address,
        };
        Ok(Self {
            link,
            profile: SessionProfile {
                identity,
                address,
                request_preambles,
            },
        })
    }

    /// Returns the learned profile.
    pub const fn profile(&self) -> &SessionProfile {
        &self.profile
    }

    /// Returns the shared queue client.
    pub const fn link(&self) -> &LinkClient {
        &self.link
    }

    /// Wraps this identified session with validated health and adaptive timing settings.
    pub fn managed(
        self,
        options: crate::service::DeviceHealthOptions,
    ) -> Result<crate::service::ManagedDeviceSession, crate::service::DeviceHealthConfigError> {
        crate::service::ManagedDeviceSession::new(self, options)
    }

    /// Wraps this identified session with conservative health defaults.
    pub fn managed_with_defaults(self) -> crate::service::ManagedDeviceSession {
        crate::service::ManagedDeviceSession::with_defaults(self)
    }

    /// Creates a request using the learned preamble count.
    pub fn request<O: crate::Operation>(
        &self,
        operation: &O,
    ) -> Result<crate::Request, OperationError> {
        Ok(operation
            .request(self.profile.address)?
            .with_preambles(self.profile.request_preambles))
    }

    /// Executes an operation with the learned unique address and preamble count.
    pub async fn execute<O: crate::Operation>(
        &self,
        operation: &O,
        priority: Priority,
        policy: RetryPolicy,
    ) -> Result<O::Output, SessionError> {
        let request = self.request(operation)?;
        let reply = self.link.request(request, priority, policy).await?;
        operation.decode_reply(&reply).map_err(SessionError::from)
    }

    /// Executes an operation using the link client's configured defaults.
    pub async fn execute_default<O: crate::Operation>(
        &self,
        operation: &O,
    ) -> Result<O::Output, SessionError> {
        let request = self.request(operation)?;
        let reply = self.link.request_default(request).await?;
        operation.decode_reply(&reply).map_err(SessionError::from)
    }

    /// Executes an operation while accepting command-specific warning codes explicitly.
    pub async fn execute_accepting<O: crate::Operation>(
        &self,
        operation: &O,
        accepted_warnings: &[u8],
        priority: Priority,
        policy: RetryPolicy,
    ) -> Result<CommandOutcome<O::Output>, SessionError> {
        let request = self.request(operation)?;
        let reply = self.link.request(request, priority, policy).await?;
        operation
            .decode_reply_accepting(&reply, accepted_warnings)
            .map_err(SessionError::from)
    }

    /// Executes with configured defaults while accepting explicit warning codes.
    pub async fn execute_accepting_default<O: crate::Operation>(
        &self,
        operation: &O,
        accepted_warnings: &[u8],
    ) -> Result<CommandOutcome<O::Output>, SessionError> {
        let request = self.request(operation)?;
        let reply = self.link.request_default(request).await?;
        operation
            .decode_reply_accepting(&reply, accepted_warnings)
            .map_err(SessionError::from)
    }

    /// Collects a partial snapshot without discarding successful fields when another command
    /// is unsupported, rejected, or times out.
    pub async fn snapshot(
        &self,
        options: crate::service::SnapshotOptions,
    ) -> crate::service::DeviceSnapshot {
        crate::service::snapshot::capture_snapshot(self, options).await
    }
}
