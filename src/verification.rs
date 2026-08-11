//! Repeatable functional campaigns without claims of official certification.

use std::time::{Duration, Instant};

use crate::{
    Request,
    service::{ExchangeError, LinkClient, Priority, RetryPolicy},
};

/// Expected result of one functional case.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaseExpectation {
    /// Accepted response codes.
    pub response_codes: Vec<u8>,
    /// Optional payload prefix.
    pub data_prefix: Option<Vec<u8>>,
}

/// One bounded verification case.
#[derive(Debug, Clone)]
pub struct VerificationCase {
    /// Unique name in the report.
    pub name: String,
    /// Complete request.
    pub request: Request,
    /// Expected result.
    pub expectation: CaseExpectation,
    /// Timing and retry policy.
    pub retry: RetryPolicy,
}

/// Result of one case.
#[derive(Debug)]
pub struct CaseResult {
    /// Case name.
    pub name: String,
    /// Duration from enqueue to result.
    pub elapsed: Duration,
    /// Whether the expectation was met.
    pub passed: bool,
    /// Exchange error when no response was received.
    pub error: Option<ExchangeError>,
}

/// Result of a local campaign.
#[derive(Debug, Default)]
pub struct VerificationReport {
    /// Results in execution order.
    pub cases: Vec<CaseResult>,
}

impl VerificationReport {
    /// Reports whether at least one case ran and every case passed.
    pub fn passed(&self) -> bool {
        !self.cases.is_empty() && self.cases.iter().all(|case| case.passed)
    }
}

/// Runs cases sequentially through one physical queue.
pub async fn run_campaign(link: &LinkClient, cases: &[VerificationCase]) -> VerificationReport {
    let mut report = VerificationReport::default();
    for case in cases {
        let started = Instant::now();
        let result = link
            .request(case.request.clone(), Priority::Normal, case.retry)
            .await;
        let elapsed = started.elapsed();
        let (passed, error) = match result {
            Ok(reply) => {
                let code_ok = case
                    .expectation
                    .response_codes
                    .contains(&reply.response_code)
                    && !reply.has_communication_error();
                let data_ok = case
                    .expectation
                    .data_prefix
                    .as_ref()
                    .is_none_or(|prefix| reply.data.starts_with(prefix));
                (code_ok && data_ok, None)
            }
            Err(error) => (false, Some(error)),
        };
        report.cases.push(CaseResult {
            name: case.name.clone(),
            elapsed,
            passed,
            error,
        });
    }
    report
}
