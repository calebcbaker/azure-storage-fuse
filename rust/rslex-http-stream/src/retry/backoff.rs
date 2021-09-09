/// Implements exponential back off strategy with jitter.
/// Ported from [C# Polly library](https://github.com/Polly-Contrib/Polly.Contrib.WaitAndRetry/blob/master/src/Polly.Contrib.WaitAndRetry/Backoff.DecorrelatedJitterV2.cs).
/// [Algorithm description](https://github.com/Polly-Contrib/Polly.Contrib.WaitAndRetry#wait-and-retry-with-jittered-back-off).
use rand::Rng;
use std::time::Duration;

const P_FACTOR: f64 = 4.0;
const RP_SCALING_FACTOR: f64 = 1.0 / 1.4;
static ZERO_DURATION: Duration = Duration::from_millis(0);
static MAX_DURATION: Duration = Duration::from_secs(60 * 5);
static MAX_DURATION_MS: u64 = MAX_DURATION.as_millis() as u64;

pub trait BackoffStrategy: Sync + Send + 'static {
    type Iter: Iterator<Item = Duration> + Sync + Send + 'static;

    fn to_iter(&self) -> Self::Iter;
}

pub struct ExponentialBackoffWithJitter {
    initial_backoff: Duration,
    number_of_retries: u32,
}

pub struct ExponentialBackoffWithJitterIterator {
    initial_backoff: Duration,
    number_of_retries: u32,
    current_attempt: u32,
    previous_value: f64,
}

impl ExponentialBackoffWithJitter {
    pub fn new(initial_backoff: Duration, number_of_retries: u32) -> ExponentialBackoffWithJitter {
        if initial_backoff < ZERO_DURATION {
            panic!("Initial backoff delay can't be negative");
        }
        ExponentialBackoffWithJitter {
            initial_backoff,
            number_of_retries,
        }
    }
}

impl ExponentialBackoffWithJitterIterator {
    pub(self) fn new(backoff: &ExponentialBackoffWithJitter) -> Self {
        ExponentialBackoffWithJitterIterator {
            initial_backoff: backoff.initial_backoff,
            number_of_retries: backoff.number_of_retries,
            current_attempt: 0,
            previous_value: 0.0,
        }
    }
}

impl Iterator for ExponentialBackoffWithJitterIterator {
    type Item = Duration;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_attempt >= self.number_of_retries {
            return None;
        }

        if self.current_attempt == 0 {
            self.current_attempt += 1;
            return Some(ZERO_DURATION);
        }

        let mut rng = rand::thread_rng();
        let t: f64 = self.current_attempt as f64 + rng.gen::<f64>() - 1.0; // first delay will always be 0 to retry fast on the first failure
        let next: f64 = 4f64.powf(t) * (P_FACTOR * t).sqrt().tanh();

        let formula_intrinsic_value: f64 = next - self.previous_value;
        self.previous_value = next;
        let next_delay_ms = (self.initial_backoff.as_millis() as f64 * formula_intrinsic_value * RP_SCALING_FACTOR) as u64;

        self.current_attempt += 1;

        if next_delay_ms > MAX_DURATION_MS {
            return Some(MAX_DURATION);
        }

        return Some(Duration::from_millis(next_delay_ms));
    }
}

impl BackoffStrategy for ExponentialBackoffWithJitter {
    type Iter = ExponentialBackoffWithJitterIterator;

    fn to_iter(&self) -> Self::Iter {
        ExponentialBackoffWithJitterIterator::new(&self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluent_assertions::*;
    use std::iter;

    #[test]
    fn zero_backoff_should_return_zero_retry_delays() {
        let number_of_retries = 5;

        ExponentialBackoffWithJitter::new(ZERO_DURATION, number_of_retries)
            .to_iter()
            .should()
            .equal_iterator(iter::repeat(ZERO_DURATION).take(number_of_retries as usize));
    }

    #[test]
    fn zero_number_of_retries_should_result_in_zero_number_fo_delays() {
        let number_of_retries = 0;

        ExponentialBackoffWithJitter::new(Duration::from_millis(100), number_of_retries)
            .to_iter()
            .should()
            .be_empty();
    }

    #[test]
    fn should_produce_delay_intervals() {
        let number_of_retries = 5;
        let initial_backoff = Duration::from_millis(100);

        let backoff = ExponentialBackoffWithJitter::new(initial_backoff, number_of_retries);

        let mut delays_count = 0;

        // skipping the first delay cause it is always 0
        for delay in backoff.to_iter().skip(1) {
            delays_count += 1;
            delay.should().be_greater_than(ZERO_DURATION);
        }

        delays_count.should().be(number_of_retries - 1);
    }

    #[test]
    fn should_not_overflow() {
        let number_of_retries = 100;
        let initial_backoff = MAX_DURATION;

        let backoff = ExponentialBackoffWithJitter::new(initial_backoff, number_of_retries);

        let mut delays_count = 0;
        // skipping the first delay cause it is always 0
        for delay in backoff.to_iter().skip(1) {
            delays_count += 1;
            delay
                .should()
                .be_greater_than(ZERO_DURATION)
                .and()
                .should()
                .be_less_than_or_equal_to(MAX_DURATION);
        }

        delays_count.should().be(number_of_retries - 1);
    }

    #[test]
    fn should_add_randomness_to_delays() {
        let number_of_retries = 5;
        let initial_backoff = Duration::from_millis(100);

        let backoff = ExponentialBackoffWithJitter::new(initial_backoff, number_of_retries);

        let intervals1 = backoff.to_iter().collect::<Vec<Duration>>();
        let intervals2 = backoff.to_iter().collect::<Vec<Duration>>();

        intervals1.should().have_length(intervals2.len()).and().should().not_be(intervals2);
    }

    #[test]
    fn first_delay_should_always_be_zero() {
        let number_of_retries = 5;
        let initial_backoff = Duration::from_millis(100);

        let backoff = ExponentialBackoffWithJitter::new(initial_backoff, number_of_retries);

        // first run
        backoff.to_iter().next().should().be_some().with_value(ZERO_DURATION);

        // second run
        backoff.to_iter().next().should().be_some().with_value(ZERO_DURATION);
    }
}
