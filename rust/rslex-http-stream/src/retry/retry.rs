use crate::retry::{backoff::BackoffStrategy, RetryCondition};
use std::future::Future;
use tokio::time::sleep;

pub struct RetryStrategy<B: BackoffStrategy, C: RetryCondition> {
    backoff: B,
    condition: C,
}

impl<B: BackoffStrategy, C: RetryCondition> RetryStrategy<B, C> {
    pub fn new(backoff: B, condition: C) -> Self {
        RetryStrategy { backoff, condition }
    }

    #[tracing::instrument(level = "debug", skip(self, request, action))]
    pub async fn run<A, F>(&self, request: &C::Request, mut action: A) -> C::Response
    where
        A: Unpin + Send + FnMut() -> F,
        F: Future<Output = C::Response>,
    {
        let mut backoff_delays = self.backoff.to_iter();
        let mut tries = 0;

        loop {
            tries += 1;
            let result_try = (action()).await;
            let (should_retry, result_try) = self.condition.should_retry(request, result_try, tries).await;

            if should_retry {
                let delay = backoff_delays.next();
                match delay {
                    Some(interval) => {
                        tracing::debug!(tries, should_retry, delay = ?interval, "[RetryStrategy::run()] retrying after delay: {:?}", interval);
                        if interval.as_millis() > 0 {
                            sleep(interval).await;
                        }
                    },
                    None => {
                        tracing::info!(
                            tries,
                            should_retry,
                            delay = "None",
                            "[RetryStrategy::run()] ran out of backoff_delays, stopping retries"
                        );
                        return result_try;
                    },
                }
            } else {
                tracing::debug!(tries, should_retry, "[RetryStrategy::run()] should_retry={}", should_retry);
                return result_try;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::retry::ExponentialBackoffWithJitter;
    use async_trait::async_trait;
    use fluent_assertions::*;
    use std::{sync::Mutex, time::Duration};
    use tokio_test::block_on;

    #[async_trait]
    impl<F> RetryCondition for F
    where F: Send + Sync + 'static + Fn(&u32) -> bool
    {
        type Request = u32;
        type Response = u32;

        async fn should_retry(&self, _: &Self::Request, response_result: Self::Response, _: u32) -> (bool, Self::Response) {
            ((self)(&response_result), response_result)
        }
    }

    #[test]
    fn should_retry_on_specific_condition() {
        let number_of_retries = 3u32;
        let backoff = ExponentialBackoffWithJitter::new(Duration::from_millis(10), number_of_retries);

        let retry_strategy = RetryStrategy::new(backoff, |_: &u32| true);
        let action_calls = Mutex::new(0u32);
        let res = retry_strategy.run(&10u32, async || {
            let mut val = action_calls.lock().unwrap();
            *val += 1;
            *val
        });

        block_on(res).should().be(number_of_retries + 1);
    }

    #[test]
    fn should_not_retry_when_condition_not_satisfied() {
        let number_of_retries = 3u32;
        let backoff = ExponentialBackoffWithJitter::new(Duration::from_millis(10), number_of_retries);

        let retry_strategy = RetryStrategy::new(backoff, |_: &u32| false);
        let action_calls = Mutex::new(0u32);
        let res = retry_strategy.run(&0, async || {
            let mut val = action_calls.lock().unwrap();
            *val += 1;
            *val
        });

        block_on(res).should().be(1);
    }

    #[test]
    fn should_stop_retry_on_first_success() {
        let number_of_retries = 3u32;
        let backoff = ExponentialBackoffWithJitter::new(Duration::from_millis(10), number_of_retries);

        let retry_strategy = RetryStrategy::new(backoff, |response: &u32| *response <= 2);
        let action_calls = Mutex::new(0u32);
        let res = retry_strategy.run(&0, async || {
            let mut val = action_calls.lock().unwrap();
            *val += 1;
            *val
        });

        block_on(res).should().be(3);
    }

    #[test]
    fn should_call_action_on_empty_backoff() {
        let backoff = ExponentialBackoffWithJitter::new(Duration::from_millis(10), 0);

        let retry_strategy = RetryStrategy::new(backoff, |_: &u32| true);
        let action_calls = Mutex::new(0u32);
        let res = retry_strategy.run(&0, async || {
            let mut val = action_calls.lock().unwrap();
            *val += 1;
            *val
        });

        block_on(res).should().be(1);
    }
}
