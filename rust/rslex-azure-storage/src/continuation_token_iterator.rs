use crossbeam_channel::{bounded, Receiver};
use std::thread;

pub(crate) struct ContinuationTokenIterator<I, E> {
    current_iterator: I,
    batch_receiver: Option<Receiver<Result<I, E>>>,
}

impl<I, E> ContinuationTokenIterator<I, E>
where
    I: Iterator + Send + 'static,
    E: Send + 'static,
{
    pub fn new<IntoI, F>(batch_preload_limit: u8, factory: F) -> Result<ContinuationTokenIterator<I, E>, E>
    where
        IntoI: IntoIterator<Item = I::Item, IntoIter = I>,
        F: Send + 'static + Fn(Option<&str>) -> Result<(IntoI, Option<String>), E>,
    {
        debug_assert!(batch_preload_limit > 0);

        let (first_batch, token) = (factory)(None)?;

        let receiver = if token.is_some() {
            let (sender, receiver) = bounded(batch_preload_limit as usize);
            thread::spawn(move || {
                let mut token_opt = token;
                while let Some(token) = token_opt {
                    match (factory)(Some(&token)) {
                        Ok((next_iter, next_token)) => {
                            // This may return error after receiver is dropped.
                            // This can only happen once iterator itself is dropped and we don't care about the error anymore.
                            let _ = sender.send(Ok(next_iter.into_iter()));
                            token_opt = next_token;
                        },
                        Err(e) => {
                            let _ = sender.send(Err(e));
                            token_opt = None;
                        },
                    }
                }
            });
            Some(receiver)
        } else {
            None
        };

        Ok(ContinuationTokenIterator {
            current_iterator: first_batch.into_iter(),
            batch_receiver: receiver,
        })
    }
}

impl<I, E> Iterator for ContinuationTokenIterator<I, E>
where I: Iterator
{
    type Item = Result<I::Item, E>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(next) = self.current_iterator.next() {
                return Some(Ok(next));
            } else {
                if let Some(receiver) = &self.batch_receiver {
                    match receiver.recv() {
                        Ok(Ok(next_batch)) => {
                            self.current_iterator = next_batch;
                        },
                        Ok(Err(e)) => {
                            return Some(Err(e));
                        },
                        Err(_e) => {
                            return None;
                        },
                    }
                } else {
                    return None;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluent_assertions::*;
    use itertools::Itertools;

    #[test]
    fn continuation_token_iterator_single_request_iterator() {
        let iterators = vec![vec![1, 2, 3, 4].into_iter()];

        let iter_clone = iterators.clone();

        let f = move |_: Option<&str>| Ok::<_, u32>((iter_clone[0].clone().into_iter(), None));

        let result = ContinuationTokenIterator::new(5, f)
            .should()
            .be_ok()
            .which_value()
            .map(|r| r.unwrap())
            .collect_vec();

        result.should().equal_iterator(iterators.into_iter().flat_map(|s| s));
    }

    #[test]
    fn continuation_token_iterator_should_iterate_over_all_results() {
        let iterators = vec![
            vec![1, 2, 3, 4].into_iter(),
            vec![5, 6, 7].into_iter(),
            vec![8, 9].into_iter(),
            vec![10, 11, 12, 13].into_iter(),
        ];

        let iter_clone = iterators.clone();

        let f = move |token: Option<&str>| {
            let index = token.unwrap_or("0").parse::<usize>().unwrap();
            let next_token = if index + 1 < iter_clone.len() {
                Some((index + 1).to_string())
            } else {
                None
            };
            Ok::<_, u32>((iter_clone[index].clone().into_iter(), next_token))
        };

        let result = ContinuationTokenIterator::new(5, f)
            .should()
            .be_ok()
            .which_value()
            .map(|r| r.unwrap())
            .collect_vec();

        result.should().equal_iterator(iterators.into_iter().flat_map(|s| s));
    }

    #[test]
    fn continuation_token_iterator_should_skip_empty_result() {
        let iterators = vec![
            vec![1, 2, 3, 4].into_iter(),
            vec![].into_iter(),
            vec![8, 9].into_iter(),
            vec![10, 11, 12, 13].into_iter(),
        ];

        let iter_clone = iterators.clone();

        let f = move |token: Option<&str>| {
            let index = token.unwrap_or("0").parse::<usize>().unwrap();
            let next_token = if index + 1 < iter_clone.len() {
                Some((index + 1).to_string())
            } else {
                None
            };
            Ok::<_, u32>((iter_clone[index].clone().into_iter(), next_token))
        };

        let result = ContinuationTokenIterator::new(5, f)
            .should()
            .be_ok()
            .which_value()
            .map(|r| r.unwrap())
            .collect_vec();

        result.should().equal_iterator(iterators.into_iter().flat_map(|s| s));
    }

    #[test]
    fn continuation_token_iterator_should_stop_on_first_error() {
        let iterators = vec![
            vec![1, 2, 3, 4].into_iter(),
            vec![5, 6, 7].into_iter(),
            vec![].into_iter(),
            vec![10, 11, 12, 13].into_iter(),
        ];

        let iter_clone = iterators.clone();

        let f = move |token: Option<&str>| {
            let index = token.unwrap_or("0").parse::<usize>().unwrap();
            let next_token = if index + 1 < iter_clone.len() {
                Some((index + 1).to_string())
            } else {
                None
            };
            if iterators[index].len() == 0 {
                Err("empty data")
            } else {
                Ok((iter_clone[index].clone().into_iter(), next_token))
            }
        };

        let result = ContinuationTokenIterator::new(5, f).should().be_ok().which_value().collect_vec();

        result[0..7].should().all_be_ok();
        result[7].should().be_err();
        result.should().have_length(8);
    }

    #[test]
    fn continuation_token_iterator_should_return_error_on_first_batch_error() {
        ContinuationTokenIterator::new(5, |_| Err::<(Vec<i32>, Option<String>), _>("Error"))
            .should()
            .be_err();
    }
}
