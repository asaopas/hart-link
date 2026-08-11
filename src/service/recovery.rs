use crate::{
    Address,
    operation::{DeviceIdentity, ReadDeviceIdentity},
    service::{ExchangeError, LinkClient, Priority, RetryPolicy},
};

/// Device whose state should be recovered after reconnecting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnownDevice {
    /// Last known working address.
    pub address: Address,
    /// Identity before the connection was lost.
    pub identity: DeviceIdentity,
}

/// Result of repeated identification.
#[derive(Debug)]
pub enum RecoveryStatus {
    /// The same device returned.
    Restored(DeviceIdentity),
    /// A different device responds at the address.
    Replaced(DeviceIdentity),
    /// No device confirmed its presence.
    Unavailable(ExchangeError),
}

/// Complete link-recovery report.
#[derive(Debug, Default)]
pub struct RecoveryReport {
    /// Status of every known address.
    pub devices: Vec<(Address, RecoveryStatus)>,
}

/// Re-identifies known addresses without blindly restoring application subscriptions.
pub async fn reconcile_devices(
    link: &LinkClient,
    known: &[KnownDevice],
    retry: RetryPolicy,
) -> RecoveryReport {
    let mut report = RecoveryReport::default();
    for previous in known {
        let status = match link
            .execute(
                previous.address,
                &ReadDeviceIdentity,
                Priority::Service,
                retry,
            )
            .await
        {
            Ok(identity) if same_device(&identity, &previous.identity) => {
                RecoveryStatus::Restored(identity)
            }
            Ok(identity) => RecoveryStatus::Replaced(identity),
            Err(error) => RecoveryStatus::Unavailable(error),
        };
        report.devices.push((previous.address, status));
    }
    report
}

fn same_device(left: &DeviceIdentity, right: &DeviceIdentity) -> bool {
    left.manufacturer_id == right.manufacturer_id
        && left.address_device_type() == right.address_device_type()
        && left.device_id == right.device_id
}
