use crate::{
    DeviceReply, Request,
    service::{ExchangeError, LinkClient, QueueId, RetryPolicy},
};

/// Result check for one step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanExpectation {
    /// A zero response code is required.
    Success,
    /// One of the listed codes is accepted.
    ResponseCodes(Vec<u8>),
    /// The payload must begin with the specified bytes.
    DataPrefix(Vec<u8>),
}

/// One sequential device-operation step.
#[derive(Debug, Clone)]
pub struct PlanStep {
    /// Name used in reports.
    pub name: String,
    /// Complete request.
    pub request: Request,
    /// Queue used for this step.
    pub queue: QueueId,
    /// Retry and timing limits.
    pub retry: RetryPolicy,
    /// Expected result.
    pub expectation: PlanExpectation,
}

impl PlanStep {
    /// Creates a success step in the default queue with default retry settings.
    pub fn new(name: impl Into<String>, request: Request) -> Self {
        Self {
            name: name.into(),
            request,
            queue: QueueId::DEFAULT,
            retry: RetryPolicy::default(),
            expectation: PlanExpectation::Success,
        }
    }

    /// Sets the queue used by this step.
    pub const fn with_queue(mut self, queue: QueueId) -> Self {
        self.queue = queue;
        self
    }

    /// Sets timing and retry behavior.
    pub const fn with_retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// Replaces the result expectation.
    pub fn expecting(mut self, expectation: PlanExpectation) -> Self {
        self.expectation = expectation;
        self
    }
}

/// Sequential plan with an explicit stopping policy.
#[derive(Debug, Clone, Default)]
pub struct Plan {
    /// Steps in execution order.
    pub steps: Vec<PlanStep>,
    /// Whether to continue after an expectation is not met.
    pub continue_after_failure: bool,
}

/// Report for one step.
#[derive(Debug)]
pub struct StepReport {
    /// Step name.
    pub name: String,
    /// Received response or exchange error.
    pub result: Result<DeviceReply, ExchangeError>,
    /// Whether a successful response meets the expectation.
    pub expectation_met: bool,
}

/// Result of sequential execution.
#[derive(Debug, Default)]
pub struct PlanReport {
    /// Reports for executed steps.
    pub steps: Vec<StepReport>,
}

impl Plan {
    /// Creates an empty fail-fast plan.
    pub const fn new() -> Self {
        Self {
            steps: Vec::new(),
            continue_after_failure: false,
        }
    }

    /// Appends a step.
    pub fn with_step(mut self, step: PlanStep) -> Self {
        self.steps.push(step);
        self
    }

    /// Selects whether later steps run after an exchange or expectation failure.
    pub const fn continuing_after_failure(mut self, enabled: bool) -> Self {
        self.continue_after_failure = enabled;
        self
    }

    /// Executes steps through the shared link queue.
    pub async fn execute(&self, link: &LinkClient) -> PlanReport {
        let mut report = PlanReport::default();
        for step in &self.steps {
            let result = link
                .request(step.request.clone(), step.queue, step.retry)
                .await;
            let expectation_met = result
                .as_ref()
                .is_ok_and(|reply| expectation_matches(&step.expectation, reply));
            let exchange_failed = result.is_err();
            report.steps.push(StepReport {
                name: step.name.clone(),
                result,
                expectation_met,
            });
            if (exchange_failed || !expectation_met) && !self.continue_after_failure {
                break;
            }
        }
        report
    }
}

fn expectation_matches(expectation: &PlanExpectation, reply: &DeviceReply) -> bool {
    match expectation {
        PlanExpectation::Success => reply.response_code == 0,
        PlanExpectation::ResponseCodes(codes) => {
            !reply.has_communication_error() && codes.contains(&reply.response_code)
        }
        PlanExpectation::DataPrefix(prefix) => {
            reply.response_code == 0
                && !reply.has_communication_error()
                && reply.data.starts_with(prefix)
        }
    }
}
