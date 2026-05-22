use crate::budget::RetryBudget;

#[derive(Debug, Clone)]
pub struct FailureContext {
    pub task_id: String,
    pub action_id: String,
    pub reason: String,
    pub retry_count: u32,
    pub replan_count: u32,
}

#[derive(Debug, Clone)]
pub enum RecoveryDecision {
    Retry { backoff_ms: u64 },
    Replan { failure_context: String },
    Rollback { checkpoint_id: String },
    Escalate { reason: String },
}

pub struct RecoveryEngine;

impl RecoveryEngine {
    pub fn decide(ctx: &FailureContext, budget: &RetryBudget) -> RecoveryDecision {
        if ctx.retry_count < budget.max_retries {
            RecoveryDecision::Retry { backoff_ms: Self::exponential_backoff(ctx.retry_count) }
        } else if ctx.replan_count < budget.max_replans {
            RecoveryDecision::Replan {
                failure_context: format!("Action {} failed after {} retries: {}", ctx.action_id, ctx.retry_count, ctx.reason),
            }
        } else {
            RecoveryDecision::Escalate {
                reason: format!("Exhausted {} retries and {} replans for task {}. Last error: {}", budget.max_retries, budget.max_replans, ctx.task_id, ctx.reason),
            }
        }
    }

    fn exponential_backoff(attempt: u32) -> u64 {
        500 * 2u64.pow(attempt)
    }
}
