// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A deterministic range source: `id` in [start, end), and `label = "row-{id}"`.
//! All C ABI and Arrow C Stream plumbing is supplied by the SDK's export macro.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::StringArray;
use arrow_array::{ArrayRef, Int64Array, RecordBatch, RecordBatchOptions};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use serde::{Deserialize, Serialize};
use spark_datasource::{
    CreateInfo, DataSource, LatestOffsetInfo, LimitSupport, MicroBatchInfo, MicroBatchStream,
    MicroBatchStreamInfo, Options, PartitionReader, PartitionReaderFactory, ReadLimit,
    ReaderContext, Result, ScanBuilder, ScanInfo, ScanPlan, Unsupported,
};

// The largest values of numPartitions and batchSize.
const MAX_PARTITIONS: usize = 100_000;
const MAX_BATCH_SIZE: usize = 1_000_000;

pub struct RangeSource {
    start: i64,
    end: i64,
    num_partitions: usize,
    batch_size: usize,
    max_rows_per_trigger: Option<u64>,
}

pub struct RangeScan {
    start: i64,
    end: i64,
    num_partitions: usize,
    limit: Option<usize>,
    factory: RangeReaderFactory,
}

/// Reader configuration travels to each executor; it contains no live objects.
/// Spark passes the columns to read to the reader, so the factory omits them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeReaderFactory {
    batch_size: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangePartition {
    start: i64,
    end: i64,
}

/// A streaming read of the range. The range is fixed, so all its rows are available when
/// the query starts: the first micro-batch reads them, unless `maxRowsPerTrigger` limits each
/// micro-batch, and later ones find no new data.
pub struct RangeStream {
    start: i64,
    end: i64,
    num_partitions: usize,
    max_rows_per_trigger: Option<u64>,
    factory: RangeReaderFactory,
}

/// The offset of a streaming read: the next row to read. Spark stores it in the checkpoint
/// as `{"next":13}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeOffset {
    next: i64,
}

impl DataSource for RangeSource {
    type ScanBuilder = RangeScan;
    type MicroBatchStream = RangeStream;
    // A read-only source.
    type BatchWrite = Unsupported;
    type StreamingWrite = Unsupported;

    fn new(info: CreateInfo) -> Result<Self> {
        let options = info.options;
        for key in options.keys() {
            if !matches!(
                key.as_str(),
                "start" | "end" | "numpartitions" | "batchsize" | "maxrowspertrigger"
            ) {
                return Err(format!("Unknown range option: {key}").into());
            }
        }
        let start = option(&options, "start", 0_i64)?;
        let end = option(&options, "end", 100_i64)?;
        let num_partitions = option(&options, "numpartitions", 2_usize)?;
        let batch_size = option(&options, "batchsize", 1024_usize)?;
        let max_rows_per_trigger = options
            .get("maxrowspertrigger")
            .map(|_| option(&options, "maxrowspertrigger", 0_u64))
            .transpose()?;
        if end < start {
            return Err("end must be greater than or equal to start".into());
        }
        if num_partitions == 0 || batch_size == 0 || max_rows_per_trigger == Some(0) {
            return Err("numPartitions, batchSize and maxRowsPerTrigger must be positive".into());
        }
        // A failed allocation aborts the process, so bound the sizes that the reads allocate.
        if num_partitions > MAX_PARTITIONS || batch_size > MAX_BATCH_SIZE {
            return Err(format!(
                "numPartitions must be at most {MAX_PARTITIONS}, and batchSize at most \
                 {MAX_BATCH_SIZE}"
            )
            .into());
        }
        Ok(Self {
            start,
            end,
            num_partitions,
            batch_size,
            max_rows_per_trigger,
        })
    }

    fn infer_schema(&self) -> Result<SchemaRef> {
        output_schema(&["id".into(), "label".into()])
    }

    fn new_scan(&self, info: ScanInfo) -> Result<Self::ScanBuilder> {
        read_columns(&info.schema)?;
        Ok(RangeScan {
            start: self.start,
            end: self.end,
            num_partitions: self.num_partitions,
            limit: None,
            factory: RangeReaderFactory {
                batch_size: self.batch_size,
            },
        })
    }

    fn new_micro_batch_stream(&self, info: MicroBatchStreamInfo) -> Result<Self::MicroBatchStream> {
        read_columns(&info.schema)?;
        Ok(RangeStream {
            start: self.start,
            end: self.end,
            num_partitions: self.num_partitions,
            max_rows_per_trigger: self.max_rows_per_trigger,
            factory: RangeReaderFactory {
                batch_size: self.batch_size,
            },
        })
    }
}

impl PartitionReaderFactory for RangeReaderFactory {
    type Partition = RangePartition;

    fn open<'task>(
        &self,
        partition: RangePartition,
        context: ReaderContext<'task>,
    ) -> Result<PartitionReader<'task>> {
        let batch_size = self.batch_size;
        if batch_size == 0 || partition.start > partition.end {
            return Err("Invalid serialized range plan".into());
        }
        // The schema to read has the columns that the query needs, in order, with the
        // types that the scan validated.
        let schema = context.schema;
        let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        output_schema(&columns)?;
        let task = context.task;
        let mut current = partition.start;
        let batches = std::iter::from_fn(move || {
            if current == partition.end {
                return None;
            }
            if task.is_interrupted() {
                current = partition.end;
                return Some(Err(ArrowError::ComputeError(
                    "Range read was cancelled".into(),
                )));
            }
            let next =
                (i128::from(current) + batch_size as i128).min(i128::from(partition.end)) as i64;
            let num_rows = (i128::from(next) - i128::from(current)) as usize;
            let arrays: Vec<ArrayRef> = columns
                .iter()
                .map(|column| match column.as_str() {
                    "id" => Arc::new(Int64Array::from_iter_values(current..next)) as ArrayRef,
                    "label" => Arc::new(StringArray::from_iter_values(
                        (current..next).map(|id| format!("row-{id}")),
                    )) as ArrayRef,
                    _ => unreachable!("column names were validated before reading"),
                })
                .collect();
            current = next;
            // Explicit row counts also preserve COUNT(*) with a zero-column projection.
            Some(RecordBatch::try_new_with_options(
                Arc::clone(&schema),
                arrays,
                &RecordBatchOptions::new().with_row_count(Some(num_rows)),
            ))
        });
        Ok(Box::new(batches))
    }
}

// The readers get the columns to read in their context, so the scan does not prune columns.
impl ScanBuilder for RangeScan {
    type Factory = RangeReaderFactory;

    fn push_limit(&mut self, limit: usize) -> Result<LimitSupport> {
        self.limit = Some(limit);
        Ok(LimitSupport::Global)
    }

    fn plan(self) -> Result<ScanPlan<RangeReaderFactory>> {
        // Apply a global limit by shortening the range, then split it.
        let lower = i128::from(self.start);
        let upper = i128::from(self.end);
        let upper = self
            .limit
            .map_or(upper, |limit| upper.min(lower + limit as i128));
        let partitions = split(lower, upper, self.num_partitions);
        Ok(ScanPlan::new(self.factory, partitions))
    }
}

impl MicroBatchStream for RangeStream {
    type Offset = RangeOffset;
    // Micro-batches are read as batch scans are.
    type Factory = RangeReaderFactory;

    fn initial_offset(&mut self) -> Result<RangeOffset> {
        Ok(RangeOffset { next: self.start })
    }

    fn default_read_limit(&self) -> Result<ReadLimit> {
        Ok(self
            .max_rows_per_trigger
            .map_or(ReadLimit::AllAvailable, ReadLimit::MaxRows))
    }

    fn latest_offset(&mut self, info: LatestOffsetInfo<RangeOffset>) -> Result<RangeOffset> {
        // The offsets of a checkpoint may come from a query with other options. Reject them
        // here, before Spark logs the latest offset, which it would plan again on restart.
        if info.start.next < self.start || info.start.next > self.end {
            return Err(format!(
                "The offset {} is not in the range [{}, {}]",
                info.start.next, self.start, self.end
            )
            .into());
        }
        // All the rows of the range are available, within the limit and the end of the run.
        let mut next = self.end;
        if let ReadLimit::MaxRows(rows) = info.limit {
            let rows = i64::try_from(rows).unwrap_or(i64::MAX);
            next = next.min(info.start.next.saturating_add(rows));
        }
        if let Some(run_end) = info.run_end {
            next = next.min(run_end.next);
        }
        Ok(RangeOffset { next })
    }

    fn plan(&mut self, info: MicroBatchInfo<RangeOffset>) -> Result<ScanPlan<RangeReaderFactory>> {
        let (start, end) = (info.start, info.end);
        // The offsets of a checkpoint may come from a query with other options.
        if start.next < self.start || end.next > self.end || start.next > end.next {
            return Err(format!(
                "The offsets {} and {} do not delimit rows of the range [{}, {})",
                start.next, end.next, self.start, self.end
            )
            .into());
        }
        let partitions = split(
            i128::from(start.next),
            i128::from(end.next),
            self.num_partitions,
        );
        Ok(ScanPlan::new(self.factory.clone(), partitions))
    }
}

/// Splits the rows in [lower, upper) into at most `num_partitions` disjoint partitions.
fn split(lower: i128, upper: i128, num_partitions: usize) -> Vec<RangePartition> {
    let rows = (upper - lower).max(0);
    let count = (num_partitions as i128).min(rows) as usize;
    let mut partitions = Vec::with_capacity(count);
    if count > 0 {
        let width = rows / count as i128;
        let extra = rows % count as i128;
        let mut start = lower;
        for index in 0..count {
            let end = start + width + i128::from((index as i128) < extra);
            partitions.push(RangePartition {
                start: start as i64,
                end: end as i64,
            });
            start = end;
        }
    }
    partitions
}

/// The columns of a schema to read, which must have the types of the range.
fn read_columns(schema: &SchemaRef) -> Result<Vec<String>> {
    let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    let expected = output_schema(&columns)?;
    for (field, expected) in schema.fields().iter().zip(expected.fields()) {
        if field.data_type() != expected.data_type() {
            return Err(format!(
                "Unsupported type for {}: {}",
                field.name(),
                field.data_type()
            )
            .into());
        }
    }
    Ok(columns)
}

fn option<T>(options: &Options, name: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    options.get(name).map_or(Ok(default), |value| {
        value
            .parse()
            .map_err(|error| format!("Invalid {name}: {error}").into())
    })
}

fn output_schema(columns: &[String]) -> Result<SchemaRef> {
    let mut seen = BTreeSet::new();
    let fields: Result<Vec<Field>> = columns
        .iter()
        .map(|name| {
            if !seen.insert(name) {
                return Err(format!("Duplicate column: {name}").into());
            }
            let data_type = match name.as_str() {
                "id" => DataType::Int64,
                "label" => DataType::Utf8,
                _ => return Err(format!("Unknown column: {name}").into()),
            };
            Ok(Field::new(name, data_type, false))
        })
        .collect();
    Ok(Arc::new(Schema::new(fields?)))
}

spark_datasource::export_data_source!(RangeSource);

#[cfg(test)]
mod tests {
    use super::*;
    use spark_datasource::TaskContext;

    fn info(values: &[(&str, &str)]) -> CreateInfo {
        let options: Options = values
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        CreateInfo::new("rust_sdk_range", options)
    }

    fn source(values: &[(&str, &str)]) -> RangeSource {
        RangeSource::new(info(values)).unwrap()
    }

    fn scan(source: &RangeSource) -> RangeScan {
        source
            .new_scan(ScanInfo::new(source.infer_schema().unwrap()))
            .unwrap()
    }

    fn stream(source: &RangeSource) -> RangeStream {
        let info =
            MicroBatchStreamInfo::new(source.infer_schema().unwrap(), "file:/tmp/checkpoint");
        source.new_micro_batch_stream(info).unwrap()
    }

    /// Reads every partition with `columns` as the schema to read, as Spark passes it.
    fn read(plan: ScanPlan<RangeReaderFactory>, columns: &[&str]) -> Vec<RecordBatch> {
        let columns: Vec<String> = columns.iter().map(|column| column.to_string()).collect();
        let schema = output_schema(&columns).unwrap();
        plan.partitions
            .into_iter()
            .enumerate()
            .flat_map(|(index, partition)| {
                let task = TaskContext::for_testing().with_partition_id(index as i32);
                plan.factory
                    .open(partition, ReaderContext::new(schema.clone(), task))
                    .unwrap()
                    .map(|batch| batch.unwrap())
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    const ALL: &[&str] = &["id", "label"];

    fn ids(batches: &[RecordBatch]) -> Vec<i64> {
        batches
            .iter()
            .flat_map(|batch| {
                let index = batch.schema().index_of("id").unwrap();
                batch
                    .column(index)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    #[test]
    fn applies_a_global_limit_and_reads_the_columns_in_their_order() {
        let source = source(&[
            ("start", "10"),
            ("end", "25"),
            ("numpartitions", "3"),
            ("batchsize", "2"),
        ]);
        let mut scan = scan(&source);
        assert_eq!(scan.push_limit(5).unwrap(), LimitSupport::Global);
        let plan = scan.plan().unwrap();
        assert_eq!(plan.partitions.len(), 3);
        let batches = read(plan, &["label", "id"]);
        assert_eq!(ids(&batches), (10..15).collect::<Vec<_>>());
        for batch in &batches {
            assert!(batch.num_rows() <= 2);
            assert_eq!(batch.schema().field(0).name(), "label");
            let labels = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for (row, id) in ids(std::slice::from_ref(batch)).iter().enumerate() {
                assert_eq!(labels.value(row), format!("row-{id}"));
            }
        }
    }

    #[test]
    fn limits_near_the_end_of_the_int64_range_do_not_overflow() {
        let source = source(&[
            ("start", &(i64::MAX - 3).to_string()),
            ("end", &i64::MAX.to_string()),
        ]);
        for (limit, expected) in [(2, 2), (usize::MAX, 3)] {
            let mut scan = scan(&source);
            scan.push_limit(limit).unwrap();
            assert_eq!(ids(&read(scan.plan().unwrap(), ALL)).len(), expected);
        }
    }

    #[test]
    fn serialized_plans_preserve_zero_column_row_counts_and_batch_sizes() {
        let source = source(&[("end", "7"), ("numpartitions", "1"), ("batchsize", "3")]);
        let plan = scan(&source).plan().unwrap();
        let factory = serde_json::from_slice(&serde_json::to_vec(&plan.factory).unwrap()).unwrap();
        let partitions = plan
            .partitions
            .iter()
            .map(|partition| {
                serde_json::from_slice(&serde_json::to_vec(partition).unwrap()).unwrap()
            })
            .collect();
        // SELECT count(*) reads no columns.
        let batches = read(ScanPlan::new(factory, partitions), &[]);
        assert_eq!(
            batches
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>(),
            vec![3, 3, 1]
        );
        assert!(batches.iter().all(|batch| batch.num_columns() == 0));
    }

    #[test]
    fn zero_limit_and_empty_range_do_not_add_rows() {
        let provider = source(&[("end", "10"), ("numpartitions", "20")]);
        let mut zero = scan(&provider);
        zero.push_limit(0).unwrap();
        assert!(zero.plan().unwrap().partitions.is_empty());
        let mut limited = scan(&provider);
        limited.push_limit(2).unwrap();
        let plan = limited.plan().unwrap();
        assert_eq!(plan.partitions.len(), 2);
        assert_eq!(ids(&read(plan, ALL)), vec![0, 1]);
        let empty = source(&[("start", "5"), ("end", "5")]);
        assert!(scan(&empty).plan().unwrap().partitions.is_empty());
    }

    #[test]
    fn rejects_invalid_options_and_incompatible_user_schemas() {
        for (key, value) in [
            ("batchsize", "0"),
            ("batchsize", "-1"),
            ("batchsize", "1000001"),
            ("numpartitions", "0"),
            ("numpartitions", "100001"),
            ("maxrowspertrigger", "0"),
            ("end", "-1"),
            ("start", "not-an-integer"),
            ("unknown", "1"),
        ] {
            assert!(RangeSource::new(info(&[(key, value)])).is_err());
        }
        let source = source(&[]);
        let invalid = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        assert!(source.new_scan(ScanInfo::new(invalid.clone())).is_err());
        let info = MicroBatchStreamInfo::new(invalid, "file:/tmp/checkpoint");
        assert!(source.new_micro_batch_stream(info).is_err());
        assert!(output_schema(&["id".into(), "id".into()]).is_err());
    }

    #[test]
    fn a_stream_plans_micro_batches_between_json_offsets() {
        let source = source(&[
            ("start", "10"),
            ("end", "25"),
            ("numpartitions", "2"),
            ("batchsize", "4"),
        ]);
        let mut stream = stream(&source);
        assert_eq!(
            stream.default_read_limit().unwrap(),
            ReadLimit::AllAvailable
        );
        let initial = stream.initial_offset().unwrap();
        let latest = stream
            .latest_offset(LatestOffsetInfo::new(initial))
            .unwrap();
        assert_eq!(serde_json::to_string(&initial).unwrap(), r#"{"next":10}"#);
        assert_eq!(serde_json::to_string(&latest).unwrap(), r#"{"next":25}"#);
        // As if an earlier query had planned a micro-batch up to an offset of its checkpoint.
        let middle: RangeOffset = serde_json::from_str(r#"{"next":13}"#).unwrap();
        let first = stream.plan(MicroBatchInfo::new(initial, middle)).unwrap();
        assert_eq!(first.partitions.len(), 2);
        assert_eq!(ids(&read(first, ALL)), (10..13).collect::<Vec<_>>());
        let second = stream.plan(MicroBatchInfo::new(middle, latest)).unwrap();
        assert_eq!(ids(&read(second, ALL)), (13..25).collect::<Vec<_>>());
        assert!(stream
            .plan(MicroBatchInfo::new(latest, latest))
            .unwrap()
            .partitions
            .is_empty());
        let outside: RangeOffset = serde_json::from_str(r#"{"next":30}"#).unwrap();
        assert!(stream.plan(MicroBatchInfo::new(initial, outside)).is_err());
        assert!(stream.plan(MicroBatchInfo::new(latest, initial)).is_err());
        assert!(stream
            .latest_offset(LatestOffsetInfo::new(outside))
            .is_err());
    }

    #[test]
    fn a_stream_limits_its_micro_batches_to_max_rows_per_trigger() {
        let source = source(&[("start", "10"), ("end", "25"), ("maxrowspertrigger", "4")]);
        let mut stream = stream(&source);
        let limit = stream.default_read_limit().unwrap();
        assert_eq!(limit, ReadLimit::MaxRows(4));
        let request = |start: i64, limit: ReadLimit, run_end: Option<i64>| {
            let mut info = LatestOffsetInfo::new(RangeOffset { next: start });
            info.limit = limit;
            info.run_end = run_end.map(|next| RangeOffset { next });
            info
        };
        let mut ends = vec![];
        let mut start = 10;
        loop {
            let end = stream
                .latest_offset(request(start, limit, None))
                .unwrap()
                .next;
            if end == start {
                break;
            }
            ends.push(end);
            start = end;
        }
        assert_eq!(ends, vec![14, 18, 22, 25]);
        // A run of Trigger.AvailableNow does not read beyond its end.
        let end = stream.latest_offset(request(18, limit, Some(20))).unwrap();
        assert_eq!(end.next, 20);
        // Spark asks without a limit, for the progress of the query or with Trigger.Once.
        let end = stream.latest_offset(request(10, ReadLimit::AllAvailable, None));
        assert_eq!(end.unwrap().next, 25);
    }

    #[test]
    fn opens_a_huge_partition_lazily_without_materializing_it() {
        let source = source(&[
            ("start", "-9223372036854775808"),
            ("end", "9223372036854775807"),
            ("numpartitions", "1"),
            ("batchsize", "2"),
        ]);
        let plan = scan(&source).plan().unwrap();
        let context =
            ReaderContext::new(source.infer_schema().unwrap(), TaskContext::for_testing());
        let mut reader = plan
            .factory
            .open(plan.partitions[0].clone(), context)
            .unwrap();
        let first = reader.next().unwrap().unwrap();
        let second = reader.next().unwrap().unwrap();
        assert_eq!(ids(&[first]), vec![i64::MIN, i64::MIN + 1]);
        assert_eq!(ids(&[second]), vec![i64::MIN + 2, i64::MIN + 3]);
    }
}
