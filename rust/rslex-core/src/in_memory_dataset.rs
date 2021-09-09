use crate::{
    iterator_extensions::SharedVecIter, Dataset, ExecutionResult, Record, RecordIterator, RecordIteratorRef, RecordSchema, RowsPartition,
    SyncRecord, SyncRecordSchema, SyncValue, Value,
};
use itertools::Itertools;
use std::sync::{Arc, Mutex};

struct InMemoryRowsIterator {
    iterator: SharedVecIter<ExecutionResult<Arc<SyncRecord>>>,
    last_processed_schema: SyncRecordSchema,
    last_output_schema: RecordSchema,
}

impl RecordIterator for InMemoryRowsIterator {}

impl Iterator for InMemoryRowsIterator {
    type Item = ExecutionResult<Record>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iterator.next().map(|result| {
            result.map(|record| {
                let values = record.iter().map(|v| Value::from(v.clone())).collect_vec();
                if &self.last_processed_schema != record.schema() {
                    self.last_processed_schema = record.schema().clone();
                    self.last_output_schema = RecordSchema::from(&self.last_processed_schema);
                }

                Record::new(values, self.last_output_schema.clone())
            })
        })
    }
}

#[derive(Debug)]
struct InMemorySyncPartition {
    records: Arc<Vec<ExecutionResult<Arc<SyncRecord>>>>,
}

impl RowsPartition for InMemorySyncPartition {
    fn iter(&self) -> RecordIteratorRef {
        Box::new(InMemoryRowsIterator {
            iterator: SharedVecIter::new(self.records.clone()),
            last_processed_schema: SyncRecordSchema::empty(),
            last_output_schema: RecordSchema::empty(),
        })
    }
}

pub fn empty() -> Dataset {
    from_single_partition_results(vec![])
}

pub fn from_multiple_sources_results(sources_results: Vec<Vec<Vec<ExecutionResult<Record>>>>) -> Dataset {
    let mut last_processed_schema = RecordSchema::empty();
    let mut last_output_schema = SyncRecordSchema::empty();
    let partitions_per_source = sources_results
        .into_iter()
        .map(|source_results| {
            source_results
                .into_iter()
                .map(|partition_results| {
                    let partition_data = partition_results
                        .into_iter()
                        .map(|result| {
                            result.map(|r| {
                                let (schema, values) = r.deconstruct();
                                if schema != last_processed_schema {
                                    last_output_schema = SyncRecordSchema::from(&schema);
                                    last_processed_schema = schema;
                                }

                                Arc::new(SyncRecord::new(
                                    values.iter().map(|v| SyncValue::from(v.clone())).collect_vec(),
                                    last_output_schema.clone(),
                                ))
                            })
                        })
                        .collect_vec();

                    Arc::new(InMemorySyncPartition {
                        records: Arc::new(partition_data),
                    }) as Arc<dyn RowsPartition>
                })
                .collect_vec()
        })
        .collect_vec();

    Dataset::from_multiple_sources(partitions_per_source)
}

pub fn from_multiple_sources_records(sources_records: Vec<Vec<Vec<Record>>>) -> Dataset {
    let results = sources_records
        .into_iter()
        .map(|s| s.into_iter().map(|p| p.into_iter().map(Ok).collect_vec()).collect_vec())
        .collect_vec();
    from_multiple_sources_results(results)
}

pub fn from_multiple_partition_results(partition_results: Vec<Vec<ExecutionResult<Record>>>) -> Dataset {
    from_multiple_sources_results(vec![partition_results])
}

pub fn from_single_partition_results(records: Vec<ExecutionResult<Record>>) -> Dataset {
    from_multiple_partition_results(vec![records])
}

pub fn from_multiple_partition_records(partitions: Vec<Vec<Record>>) -> Dataset {
    from_multiple_sources_records(vec![partitions])
}

pub fn from_single_partition_records(records: Vec<Record>) -> Dataset {
    from_multiple_partition_records(vec![records])
}

pub fn from_sync_records(records: Vec<SyncRecord>, target_num_partitions: usize) -> Dataset {
    let items_per_partition = records.len() / target_num_partitions;
    let leftover_item_count = records.len() % target_num_partitions;
    let mut partitions = Vec::with_capacity(target_num_partitions);
    let mut remaining_items_in_current_partition = 0;
    let mut partition_idx = None;
    for record in records.into_iter() {
        if remaining_items_in_current_partition == 0 {
            partition_idx = partition_idx.map(|i| i + 1).or(Some(0));
            remaining_items_in_current_partition = items_per_partition + if partition_idx.unwrap() < leftover_item_count { 1 } else { 0 };
            partitions.push(Vec::with_capacity(remaining_items_in_current_partition));
        }

        partitions[partition_idx.unwrap_or_default()].push(Ok(Arc::new(record)));
        remaining_items_in_current_partition -= 1;
    }

    Dataset::from_single_source(
        partitions
            .into_iter()
            .map(|p| Arc::new(InMemorySyncPartition { records: Arc::new(p) }) as Arc<dyn RowsPartition>)
            .collect(),
    )
}

#[derive(Clone, Debug, PartialEq)]
struct ForceThreadSafeRecord {
    record: Record,
}

unsafe impl Sync for ForceThreadSafeRecord {}
unsafe impl Send for ForceThreadSafeRecord {}

#[derive(Debug)]
struct InMemoryPartition {
    records: Mutex<std::vec::IntoIter<Vec<ForceThreadSafeRecord>>>,
}

impl RowsPartition for InMemoryPartition {
    fn iter(&self) -> RecordIteratorRef {
        let mut iter = self.records.lock().unwrap();
        let records = iter.next().expect("More pulls requested than allocated.");
        Box::new(InMemoryPartitionIter {
            records: records.into_iter(),
        })
    }
}

struct InMemoryPartitionIter {
    records: std::vec::IntoIter<ForceThreadSafeRecord>,
}

impl RecordIterator for InMemoryPartitionIter {}

impl Iterator for InMemoryPartitionIter {
    type Item = ExecutionResult<Record>;

    fn next(&mut self) -> Option<Self::Item> {
        self.records.next().map(|r| Ok(r.record))
    }
}

pub fn from_existing_dataset(dataset: &Dataset, pulls_allowed: usize) -> ExecutionResult<Dataset> {
    dataset
        .reduce(
            |mut iter, _| {
                iter.fold_ok(vec![], |mut vec, record| {
                    vec.push(ForceThreadSafeRecord { record });
                    vec
                })
            },
            num_cpus::get(),
        )
        .map(|partition_records| {
            let partitions = partition_records
                .into_iter()
                .map::<Arc<dyn RowsPartition>, _>(|records| {
                    let records_per_pull = (0..pulls_allowed).map(|_| records.clone()).collect_vec();
                    Arc::new(InMemoryPartition {
                        records: Mutex::new(records_per_pull.into_iter()),
                    })
                })
                .collect_vec();
            Dataset::from_single_source(partitions)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync_record;
    use fluent_assertions::*;

    fn test_from_sync_records(records: Vec<SyncRecord>, expected_partitions: Vec<Vec<SyncRecord>>) {
        let dataset = from_sync_records(records.clone(), expected_partitions.len());
        let new_dataset = from_existing_dataset(&dataset, 1);
        new_dataset
            .unwrap()
            .partitions()
            .iter()
            .map(|p| p.iter().map(|r| SyncRecord::from(r.unwrap())).collect_vec())
            .collect_vec()
            .should()
            .have_length(expected_partitions.len())
            .and()
            .should()
            .equal(&expected_partitions);
    }

    #[test]
    fn from_sync_records_with_fewer_records_than_target_partitions() {
        let records = vec![sync_record!("A" => 1)];
        test_from_sync_records(records.clone(), vec![records]);
    }

    #[test]
    fn from_sync_records_with_records_multiple_of_partitions() {
        let records = vec![
            sync_record!("A" => 1),
            sync_record!("A" => 2),
            sync_record!("A" => 3),
            sync_record!("A" => 4),
        ];
        test_from_sync_records(
            records.clone(),
            records.chunks(2).map(|c| c.iter().cloned().collect_vec()).collect_vec(),
        )
    }

    #[test]
    fn from_sync_records_with_one_more_record_than_partition_count() {
        let records = vec![
            sync_record!("A" => 1),
            sync_record!("A" => 2),
            sync_record!("A" => 3),
            sync_record!("A" => 4),
            sync_record!("A" => 5),
            sync_record!("A" => 5),
            sync_record!("A" => 4),
            sync_record!("A" => 5),
        ];
        test_from_sync_records(
            records.clone(),
            vec![
                records.iter().cloned().take(3).collect(),
                records.iter().skip(3).take(3).cloned().collect(),
                records.iter().skip(6).take(2).cloned().collect(),
            ],
        )
    }

    #[test]
    fn from_sync_records_with_one_fewer_record_than_multiple_of_partition_count() {
        let records = vec![sync_record!("A" => 1), sync_record!("A" => 2), sync_record!("A" => 3)];
        test_from_sync_records(
            records.clone(),
            vec![
                records.iter().cloned().take(2).collect(),
                records.iter().skip(2).take(1).cloned().collect(),
            ],
        )
    }
}
