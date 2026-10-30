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

//! A batch-read Parquet source on the arrow-rs Parquet reader.
//!
//! The driver splits the files of a path into byte ranges, as Spark splits the files of its own
//! sources, and each task reads the row groups whose middle falls in its range. Before it decodes,
//! the reader skips the row groups and pages in which, by their statistics, no row passes the
//! pushed predicates. It evaluates the predicates on the batches that it decodes, and returns only
//! the rows that pass, in batches of the batch size.

mod filter;
mod prune;
mod schema;

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};
use arrow_select::coalesce::BatchCoalescer;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder,
};
use parquet::arrow::ProjectionMask;
use serde::{Deserialize, Serialize};
use spark_native_source::{
    CreateInfo, DataSource, FilterSupport, LimitSupport, Options, PartitionReader,
    PartitionReaderFactory, Predicate, ReaderContext, Result, ScanBuilder, ScanInfo, ScanPlan,
    TaskContext, Unsupported,
};

use crate::filter::Filter;

// The reader allocates buffers of hundreds of KiB for each page and batch. When the mmap threshold
// of glibc is fixed, as MALLOC_TRIM_THRESHOLD_ and the other malloc settings of the environment fix
// it, glibc maps each of them, and unmaps it when it is freed, so that every batch faults its
// memory in again, and the threads of the tasks contend for the arenas that MALLOC_ARENA_MAX
// limits. mimalloc reuses the memory of each thread, whatever the settings of glibc.
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

pub struct ParquetSource {
    path: PathBuf,
    batch_size: usize,
    max_partition_bytes: u64,
    open_cost_in_bytes: u64,
    row_filter: bool,
}

pub struct ParquetScan {
    path: PathBuf,
    // The schema to read, from which the pushed predicates take their column types.
    schema: SchemaRef,
    max_partition_bytes: u64,
    open_cost_in_bytes: u64,
    row_filter: bool,
    factory: ParquetReaderFactory,
}

/// The configuration of the readers, which travels to the executors. The SDK passes the
/// columns to read to each reader, so the factory omits them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ParquetReaderFactory {
    // The predicates that the readers evaluate exactly, in the order that Spark pushed them.
    predicates: Vec<Predicate>,
    batch_size: usize,
    // A limit on the rows of each task, which the scan as a whole may exceed.
    limit: Option<usize>,
}

/// The byte ranges of files that a task reads, one after the other.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParquetPartition {
    ranges: Vec<FileRange>,
}

/// A byte range of a file: its task reads the row groups whose middle falls in the range.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileRange {
    path: PathBuf,
    start: u64,
    end: u64,
}

impl DataSource for ParquetSource {
    type ScanBuilder = ParquetScan;
    // A batch-read-only source.
    type MicroBatchStream = Unsupported;
    type BatchWrite = Unsupported;
    type StreamingWrite = Unsupported;

    fn new(info: CreateInfo) -> Result<Self> {
        let options = info.options;
        for key in options.keys() {
            if !matches!(
                key.as_str(),
                "path" | "batchsize" | "maxpartitionbytes" | "opencostinbytes" | "rowfilter"
            ) {
                return Err(format!("Unknown Parquet option: {key}").into());
            }
        }
        let path = PathBuf::from(options.get("path").ok_or("The path option is required")?);
        if !path.is_absolute() {
            return Err("The path must be absolute and shared by the driver and executors".into());
        }
        let batch_size = option(&options, "batchsize", 8192_usize)?;
        let max_partition_bytes = option(&options, "maxpartitionbytes", 128_u64 << 20)?;
        if batch_size == 0 || max_partition_bytes == 0 {
            return Err("batchSize and maxPartitionBytes must be positive".into());
        }
        Ok(Self {
            path,
            batch_size,
            max_partition_bytes,
            open_cost_in_bytes: option(&options, "opencostinbytes", 4_u64 << 20)?,
            row_filter: option(&options, "rowfilter", true)?,
        })
    }

    fn schema(&self) -> Result<SchemaRef> {
        // As Spark does without mergeSchema, take the schema of one file for all of them.
        let (first, _) = &data_files(&self.path)?[0];
        let metadata = load(first)?;
        schema::spark_schema(metadata.schema())
            .map_err(|error| format!("{}: {error}", first.display()).into())
    }

    fn new_scan(&self, info: ScanInfo) -> Result<Self::ScanBuilder> {
        Ok(ParquetScan {
            path: self.path.clone(),
            schema: info.schema,
            max_partition_bytes: self.max_partition_bytes,
            open_cost_in_bytes: self.open_cost_in_bytes,
            row_filter: self.row_filter,
            factory: ParquetReaderFactory {
                predicates: Vec::new(),
                batch_size: self.batch_size,
                limit: None,
            },
        })
    }
}

impl ScanBuilder for ParquetScan {
    type Factory = ParquetReaderFactory;

    fn push_filters(&mut self, filters: &[Predicate]) -> Result<Vec<FilterSupport>> {
        Ok(filters
            .iter()
            .map(|predicate| {
                if self.row_filter && Filter::new(predicate, &self.schema).is_some() {
                    self.factory.predicates.push(predicate.clone());
                    FilterSupport::Exact
                } else {
                    FilterSupport::Unsupported
                }
            })
            .collect())
    }

    fn push_limit(&mut self, limit: usize) -> Result<LimitSupport> {
        // The SDK asks only when every predicate is exact, so a task can stop after the limit,
        // which the reader counts after its filter. The tasks together may return more rows.
        self.factory.limit = Some(self.factory.limit.map_or(limit, |first| first.min(limit)));
        Ok(LimitSupport::Unsupported)
    }

    fn prune_columns(&mut self, _columns: &[String]) -> Result<bool> {
        // Each reader returns the columns of its ReaderContext::schema, in that order.
        Ok(true)
    }

    fn plan(self) -> Result<ScanPlan<ParquetReaderFactory>> {
        let mut ranges = Vec::new();
        for (path, size) in data_files(&self.path)? {
            let mut start = 0;
            while start < size {
                let end = size.min(start.saturating_add(self.max_partition_bytes));
                ranges.push(FileRange {
                    path: path.clone(),
                    start,
                    end,
                });
                start = end;
            }
        }
        // As Spark packs the files of its own sources into tasks: by decreasing size, each task
        // takes ranges while their bytes, with the cost of opening each, fit in maxPartitionBytes.
        ranges.sort_by_key(|range| std::cmp::Reverse(range.end - range.start));
        let mut partitions = Vec::new();
        let mut task = Vec::new();
        let mut bytes = 0_u64;
        for range in ranges {
            let length = range.end - range.start;
            if !task.is_empty() && bytes.saturating_add(length) > self.max_partition_bytes {
                partitions.push(ParquetPartition {
                    ranges: std::mem::take(&mut task),
                });
                bytes = 0;
            }
            bytes = bytes
                .saturating_add(length)
                .saturating_add(self.open_cost_in_bytes);
            task.push(range);
        }
        if !task.is_empty() {
            partitions.push(ParquetPartition { ranges: task });
        }
        Ok(ScanPlan::new(self.factory, partitions))
    }
}

impl PartitionReaderFactory for ParquetReaderFactory {
    type Partition = ParquetPartition;

    fn open<'task>(
        &self,
        partition: ParquetPartition,
        context: ReaderContext<'task>,
    ) -> Result<PartitionReader<'task>> {
        // Opens each file only once the reader of the previous one ends.
        let factory = self.clone();
        let ReaderContext { schema, task, .. } = context;
        let batches =
            partition
                .ranges
                .into_iter()
                .flat_map(move |range| -> PartitionReader<'task> {
                    match factory.open_range(&range, &schema, task) {
                        Ok(Some(batches)) => Box::new(batches),
                        Ok(None) => Box::new(std::iter::empty()),
                        Err(error) => {
                            Box::new(std::iter::once(Err(ArrowError::ExternalError(error))))
                        }
                    }
                });
        // The task stops after the limit, over its files, which then decode no more batches.
        let mut remaining = self.limit;
        Ok(Box::new(batches.map_while(move |batch| {
            let Some(remaining) = &mut remaining else {
                return Some(batch);
            };
            if *remaining == 0 {
                return None;
            }
            Some(batch.map(|batch| {
                let rows = batch.num_rows().min(*remaining);
                *remaining -= rows;
                batch.slice(0, rows)
            }))
        })))
    }
}

impl ParquetReaderFactory {
    /// The batches of a range of a file, or `None` if no row group of the range may have rows that
    /// pass the predicates.
    fn open_range<'task>(
        &self,
        range: &FileRange,
        schema: &SchemaRef,
        task: TaskContext<'task>,
    ) -> Result<Option<Batches<'task>>> {
        let path = range.path.display().to_string();
        let in_file = |error: Box<dyn std::error::Error + Send + Sync>| format!("{path}: {error}");
        let file = File::open(&range.path).map_err(|error| in_file(error.into()))?;
        let metadata = schema::load(&file, !self.predicates.is_empty()).map_err(in_file)?;
        let file_schema = metadata.schema().clone();
        let parquet_schema = metadata.parquet_schema().clone();
        let filters = self
            .predicates
            .iter()
            .map(|predicate| {
                // The scan checked the predicates on the schema to read, which the file has.
                Filter::new(predicate, &file_schema).ok_or_else(|| {
                    format!("{path}: The predicate {predicate:?} does not apply to the file")
                })
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let row_groups = row_groups_in(&metadata, range.start, range.end);
        let (row_groups, selection) =
            prune::prune(&filters, &metadata, row_groups).map_err(|error| in_file(error.into()))?;
        if row_groups.is_empty() {
            return Ok(None);
        }

        // The reader decodes the columns that Spark reads and those of the filters, in the order
        // of the file.
        let index_of = |name: &str| {
            file_schema
                .index_of(name)
                .map_err(|_| format!("{path}: The file has no column {name}"))
        };
        let columns = schema
            .fields()
            .iter()
            .map(|field| index_of(field.name()))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let mut filter_columns = Vec::new();
        for filter in &filters {
            filter.columns(&mut filter_columns);
        }
        let mut read = columns.clone();
        for column in filter_columns {
            read.push(index_of(column)?);
        }
        read.sort_unstable();
        read.dedup();
        let output: Vec<usize> = columns
            .iter()
            .map(|index| read.binary_search(index).expect("read"))
            .collect();

        let mut builder = ParquetRecordBatchReaderBuilder::new_with_metadata(file, metadata)
            .with_row_groups(row_groups)
            .with_projection(ProjectionMask::roots(&parquet_schema, read))
            .with_batch_size(self.batch_size);
        if let Some(selection) = selection {
            builder = builder.with_row_selection(selection);
        }
        // With filters, the limit counts the rows that pass them, after the reader.
        if let Some(limit) = self.limit.filter(|_| filters.is_empty()) {
            builder = builder.with_limit(limit);
        }
        let reader = builder.build().map_err(|error| in_file(error.into()))?;
        let schema = Arc::new(reader.schema().project(&output)?);
        // Batches of more than half the batch size pass through the coalescer, without a copy.
        let coalescer = BatchCoalescer::new(schema, self.batch_size)
            .with_biggest_coalesce_batch_size(Some(self.batch_size / 2));
        let batches = Batches {
            reader,
            filter: filters
                .into_iter()
                .reduce(|left, right| Filter::And(Box::new(left), Box::new(right))),
            output,
            coalescer,
            finished: false,
            task,
        };
        Ok(Some(batches))
    }
}

/// The batches of a range of a file: those that the reader decodes, with the rows that pass the
/// filter, and the columns that Spark reads, in its order. The filter evaluates the pushed
/// predicates on each batch, rather than in an arrow-rs `RowFilter`, which decodes the columns of
/// the predicates first, and the other columns only for the rows that pass. Spark checks the rows
/// that a source returns against the pushed predicates again, so it also reads their columns,
/// which a `RowFilter` decodes twice; and when the rows that pass are scattered over the pages,
/// the selections of rows that it builds, with millions of ranges, cost more than decoding every
/// row. The coalescer gathers the rows that pass into batches of the batch size.
struct Batches<'task> {
    reader: ParquetRecordBatchReader,
    // The conjunction of the pushed predicates.
    filter: Option<Filter>,
    // The positions of the columns that Spark reads in the batches of the reader.
    output: Vec<usize>,
    coalescer: BatchCoalescer,
    finished: bool,
    // Checked before the reader decodes each batch: when few rows pass the filter, the reader may
    // decode many batches for each batch that it returns.
    task: TaskContext<'task>,
}

impl Batches<'_> {
    fn push(&mut self, batch: RecordBatch) -> std::result::Result<(), ArrowError> {
        let Some(filter) = &self.filter else {
            return self.coalescer.push_batch(batch.project(&self.output)?);
        };
        let passes = filter.evaluate(&batch)?;
        let batch = batch.project(&self.output)?;
        self.coalescer.push_batch_with_filter(batch, &passes)
    }

    fn next_batch(&mut self) -> std::result::Result<Option<RecordBatch>, ArrowError> {
        loop {
            if let Some(batch) = self.coalescer.next_completed_batch() {
                return Ok(Some(batch));
            }
            if self.finished {
                return Ok(None);
            }
            if self.task.is_interrupted() {
                return Err(ArrowError::ExternalError(
                    "The Parquet read was cancelled".into(),
                ));
            }
            match self.reader.next().transpose()? {
                Some(batch) => self.push(batch)?,
                None => {
                    self.finished = true;
                    self.coalescer.finish_buffered_batch()?;
                }
            }
        }
    }
}

impl Iterator for Batches<'_> {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_batch().transpose()
    }
}

spark_native_source::export_data_source!(ParquetSource);

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

/// The files of a path, by path, with their sizes: the path itself, or the files in it. As
/// Spark does, skips the files whose names start with `_` or `.`, such as `_SUCCESS` and the
/// checksums of Hadoop, and the empty files.
fn data_files(path: &Path) -> Result<Vec<(PathBuf, u64)>> {
    let metadata = fs::metadata(path).map_err(|error| format!("{}: {error}", path.display()))?;
    let mut files = Vec::new();
    if metadata.is_file() {
        files.push((path.to_path_buf(), metadata.len()));
    } else {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let name = entry.file_name();
            if name.to_string_lossy().starts_with(['_', '.']) {
                continue;
            }
            let metadata = fs::metadata(entry.path())?;
            if metadata.is_dir() {
                return Err(format!(
                    "{}: This source does not read subdirectories, such as the directories of \
                     partitions",
                    entry.path().display()
                )
                .into());
            }
            files.push((entry.path(), metadata.len()));
        }
        files.sort();
    }
    files.retain(|(_, size)| *size > 0);
    if files.is_empty() {
        return Err(format!("{}: There is no Parquet file to read", path.display()).into());
    }
    Ok(files)
}

fn load(path: &Path) -> Result<ArrowReaderMetadata> {
    let file = File::open(path).map_err(|error| format!("{}: {error}", path.display()))?;
    schema::load(&file, false)
}

/// The row groups whose middle falls in `[start, end)`, as Spark assigns row groups to the
/// splits of a file, so that each row group belongs to exactly one split.
fn row_groups_in(metadata: &ArrowReaderMetadata, start: u64, end: u64) -> Vec<usize> {
    let row_groups = metadata.metadata().row_groups().iter().enumerate();
    row_groups
        .filter_map(|(index, row_group)| {
            let first = row_group
                .columns()
                .iter()
                .map(|column| column.byte_range().0)
                .min()?;
            let middle = first + u64::try_from(row_group.compressed_size()).ok()? / 2;
            (start <= middle && middle < end).then_some(index)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    use arrow_array::builder::{Int64Builder, ListBuilder, MapBuilder, StringBuilder};
    use arrow_array::cast::AsArray;
    use arrow_array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array,
        Int16Array, Int32Array, Int64Array, StringArray, StructArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, UInt32Array,
    };
    use arrow_schema::{DataType, Field, TimeUnit};
    use parquet::arrow::ArrowWriter;
    use parquet::data_type::{Int96, Int96Type};
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::parser::parse_message_type;
    use serde_json::{json, Value};

    use super::*;

    struct TempDirectory(PathBuf);

    impl TempDirectory {
        fn new() -> Self {
            static NEXT: AtomicUsize = AtomicUsize::new(0);
            let name = format!(
                "rust-sdk-parquet-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::SeqCst)
            );
            let path = std::env::temp_dir().join(name);
            let _ = fs::remove_dir_all(&path);
            fs::create_dir(&path).unwrap();
            Self(path)
        }

        fn join(&self, name: &str) -> PathBuf {
            self.0.join(name)
        }
    }

    impl Drop for TempDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn write(path: &Path, batches: &[RecordBatch], rows_per_group: usize) {
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(rows_per_group))
            .build();
        write_with(path, batches, properties);
    }

    fn write_with(path: &Path, batches: &[RecordBatch], properties: WriterProperties) {
        let file = File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, batches[0].schema(), Some(properties)).unwrap();
        for batch in batches {
            writer.write(batch).unwrap();
        }
        writer.close().unwrap();
    }

    /// `id` in [start, end), `k` = id % 10, and `label` = "row-{id}", null for multiples of 7.
    fn rows(start: i64, end: i64) -> RecordBatch {
        let ids: Vec<i64> = (start..end).collect();
        let k = ids.iter().map(|id| (id % 10) as i32);
        let label = ids
            .iter()
            .map(|id| (id % 7 != 0).then(|| format!("row-{id}")));
        RecordBatch::try_from_iter([
            ("id", Arc::new(Int64Array::from(ids.clone())) as ArrayRef),
            ("k", Arc::new(Int32Array::from_iter_values(k)) as ArrayRef),
            ("label", Arc::new(StringArray::from_iter(label)) as ArrayRef),
        ])
        .unwrap()
    }

    fn source(path: &Path, options: &[(&str, &str)]) -> ParquetSource {
        let mut options: Options = options
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();
        options.insert("path".into(), path.to_str().unwrap().into());
        ParquetSource::new(CreateInfo::new("rust_sdk_parquet", options)).unwrap()
    }

    struct Read {
        support: Vec<FilterSupport>,
        // The batches of each partition.
        partitions: Vec<Vec<RecordBatch>>,
    }

    impl Read {
        fn batches(&self) -> impl Iterator<Item = &RecordBatch> {
            self.partitions.iter().flatten()
        }

        fn ids(&self) -> Vec<i64> {
            let mut ids: Vec<i64> = self.batches().flat_map(ids).collect();
            ids.sort_unstable();
            ids
        }
    }

    fn ids(batch: &RecordBatch) -> Vec<i64> {
        let ids = batch.column_by_name("id").unwrap();
        let ids = ids.as_any().downcast_ref::<Int64Array>().unwrap();
        ids.values().to_vec()
    }

    /// Scans as the SDK does: pushes the predicates and the limit, prunes the columns, plans,
    /// and opens each serialized partition with the serialized factory.
    fn read(
        source: &ParquetSource,
        columns: Option<&[&str]>,
        predicates: &[Value],
        limit: Option<usize>,
    ) -> Result<Read> {
        let schema = source.schema()?;
        let mut scan = source.new_scan(ScanInfo::new(schema.clone()))?;
        let predicates: Vec<Predicate> = predicates
            .iter()
            .map(|predicate| serde_json::from_value(predicate.clone()).unwrap())
            .collect();
        let support = scan.push_filters(&predicates)?;
        if let Some(limit) = limit {
            assert_eq!(scan.push_limit(limit)?, LimitSupport::Unsupported);
        }
        let read_schema = match columns {
            Some(columns) => {
                let columns: Vec<String> =
                    columns.iter().map(|column| column.to_string()).collect();
                assert!(scan.prune_columns(&columns)?);
                let indices: Vec<usize> = columns
                    .iter()
                    .map(|column| schema.index_of(column).unwrap())
                    .collect();
                Arc::new(schema.project(&indices)?)
            }
            None => schema,
        };
        let plan = scan.plan()?;
        let factory: ParquetReaderFactory =
            serde_json::from_str(&serde_json::to_string(&plan.factory)?)?;
        let mut partitions = Vec::new();
        for partition in plan.partitions {
            let partition = serde_json::from_str(&serde_json::to_string(&partition)?)?;
            let context = ReaderContext::new(read_schema.clone(), TaskContext::for_testing());
            let batches = factory.open(partition, context)?;
            partitions.push(batches.collect::<std::result::Result<_, _>>()?);
        }
        Ok(Read {
            support,
            partitions,
        })
    }

    fn column(name: &str) -> Value {
        json!({"type": "column", "name": [name]})
    }

    fn literal(data_type: &str, value: Value) -> Value {
        json!({"type": "literal", "dataType": data_type, "value": value})
    }

    fn function(name: &str, children: Vec<Value>) -> Value {
        json!({"type": "function", "name": name, "children": children})
    }

    #[test]
    fn reads_the_types_that_spark_writes_as_nullable_columns() {
        let directory = TempDirectory::new();
        let mut tags = ListBuilder::new(StringBuilder::new());
        tags.append_value([Some("a"), None]);
        tags.append_null();
        let mut attrs = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
        attrs.keys().append_value("x");
        attrs.values().append_value(1);
        attrs.append(true).unwrap();
        attrs.append(false).unwrap();
        let point = StructArray::from(vec![
            (
                Arc::new(Field::new("x", DataType::Int32, false)),
                Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("name", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec![Some("p"), None])) as ArrayRef,
            ),
        ]);
        let utc = TimestampMicrosecondArray::from(vec![Some(-1), None]).with_timezone("UTC");
        let amount = Decimal128Array::from(vec![Some(12345), None])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let batch = RecordBatch::try_from_iter([
            ("id", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
            (
                "flag",
                Arc::new(BooleanArray::from(vec![Some(true), None])) as ArrayRef,
            ),
            (
                "small",
                Arc::new(Int16Array::from(vec![Some(-3), None])) as ArrayRef,
            ),
            (
                "score",
                Arc::new(Float64Array::from(vec![Some(-0.0), Some(f64::NAN)])) as ArrayRef,
            ),
            (
                "day",
                Arc::new(Date32Array::from(vec![Some(19000), None])) as ArrayRef,
            ),
            ("ts", Arc::new(utc) as ArrayRef),
            (
                "ntz",
                Arc::new(TimestampMicrosecondArray::from(vec![Some(7), None])) as ArrayRef,
            ),
            ("amount", Arc::new(amount) as ArrayRef),
            (
                "bytes",
                Arc::new(BinaryArray::from_opt_vec(vec![Some(b"\xff"), None])) as ArrayRef,
            ),
            ("tags", Arc::new(tags.finish()) as ArrayRef),
            ("point", Arc::new(point) as ArrayRef),
            ("attrs", Arc::new(attrs.finish()) as ArrayRef),
        ])
        .unwrap();
        write(
            &directory.join("types.parquet"),
            std::slice::from_ref(&batch),
            1024,
        );

        let source = source(&directory.0, &[]);
        let schema = source.schema().unwrap();
        assert_eq!(schema.fields().len(), batch.num_columns());
        for (field, written) in schema.fields().iter().zip(batch.schema().fields()) {
            assert_eq!(field.name(), written.name());
            assert!(field.is_nullable(), "{field:?}");
            // Nested types differ in the nullability of their fields, which the asserts below
            // check.
            let nested = matches!(
                field.data_type(),
                DataType::Struct(_) | DataType::List(_) | DataType::Map(_, _)
            );
            assert!(
                nested || field.data_type() == written.data_type(),
                "{field:?} {written:?}"
            );
        }
        let DataType::Struct(point) = schema.field_with_name("point").unwrap().data_type() else {
            panic!("point is a struct");
        };
        assert!(point.iter().all(|field| field.is_nullable()));
        let DataType::Map(entries, _) = schema.field_with_name("attrs").unwrap().data_type() else {
            panic!("attrs is a map");
        };
        let DataType::Struct(key_value) = entries.data_type() else {
            panic!("the entries are a struct");
        };
        assert!(
            !entries.is_nullable() && !key_value[0].is_nullable() && key_value[1].is_nullable()
        );

        let read = read(&source, None, &[], None).unwrap();
        let batches: Vec<_> = read.batches().collect();
        assert_eq!(batches.len(), 1);
        // The reader returns strings and binary values as views, at every level.
        let read_schema = batches[0].schema();
        assert_eq!(read_schema.field(8).data_type(), &DataType::BinaryView);
        let DataType::List(tag) = read_schema.field(9).data_type() else {
            panic!("tags is a list");
        };
        assert_eq!(tag.data_type(), &DataType::Utf8View);
        for (index, column) in batch.columns().iter().enumerate() {
            let read = batches[0].column(index);
            assert_eq!(read.len(), column.len());
            if let Some(bytes) = column.as_binary_opt::<i32>() {
                assert!(
                    read.as_binary_view().iter().eq(bytes.iter()),
                    "column {index}"
                );
            } else {
                assert_eq!(
                    read.to_data().buffers(),
                    column.to_data().buffers(),
                    "column {index}"
                );
            }
            assert_eq!(
                read.logical_nulls(),
                column.logical_nulls(),
                "column {index}"
            );
        }
    }

    #[test]
    fn returns_the_columns_in_the_order_of_the_schema_to_read() {
        let directory = TempDirectory::new();
        write(&directory.join("rows.parquet"), &[rows(0, 100)], 1024);
        let source = source(&directory.0, &[("batchsize", "16")]);
        let read = read(&source, Some(&["label", "id"]), &[], None).unwrap();
        assert!(read.batches().count() > 1);
        for batch in read.batches() {
            assert!(batch.num_rows() <= 16);
            let names: Vec<_> = batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            assert_eq!(names, ["label", "id"]);
        }
        assert_eq!(read.ids(), (0..100).collect::<Vec<_>>());
    }

    #[test]
    fn splits_read_each_row_group_once() {
        let directory = TempDirectory::new();
        write(&directory.join("a.parquet"), &[rows(0, 1000)], 100);
        write(&directory.join("b.parquet"), &[rows(1000, 1500)], 100);
        let source = source(&directory.0, &[("maxpartitionbytes", "512")]);
        let read = read(&source, Some(&["id"]), &[], None).unwrap();
        let non_empty = read
            .partitions
            .iter()
            .filter(|batches| !batches.is_empty())
            .count();
        assert!(non_empty > 2, "{non_empty}");
        assert!(read.partitions.len() > non_empty);
        assert_eq!(read.ids(), (0..1500).collect::<Vec<_>>());
    }

    #[test]
    fn returns_the_rows_that_pass_the_pushed_predicates() {
        let directory = TempDirectory::new();
        write(&directory.join("rows.parquet"), &[rows(0, 1000)], 100);
        let predicates = [
            function("=", vec![column("k"), literal("int", json!(3))]),
            function(
                "STARTS_WITH",
                vec![column("label"), literal("string", json!("row-1"))],
            ),
            function("FUTURE", vec![column("id")]),
        ];
        let expected: Vec<i64> = (0..1000)
            .filter(|id| id % 10 == 3 && id % 7 != 0 && format!("row-{id}").starts_with("row-1"))
            .collect();

        let columns = Some(&["id", "k", "label"][..]);
        let filtered = read(&source(&directory.0, &[]), columns, &predicates, None).unwrap();
        let exact = FilterSupport::Exact;
        assert_eq!(filtered.support, [exact, exact, FilterSupport::Unsupported]);
        assert_eq!(filtered.ids(), expected);

        // Without the row filter, Spark evaluates all the predicates.
        let unfiltered = source(&directory.0, &[("rowfilter", "false")]);
        let unfiltered = read(&unfiltered, columns, &predicates, None).unwrap();
        assert_eq!(unfiltered.support, [FilterSupport::Unsupported; 3]);
        assert_eq!(unfiltered.ids(), (0..1000).collect::<Vec<_>>());
    }

    #[test]
    fn returns_the_rows_that_pass_when_statistics_skip_row_groups_and_pages() {
        let directory = TempDirectory::new();
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(100))
            .set_data_page_row_count_limit(10)
            .set_write_batch_size(10)
            .build();
        write_with(
            &directory.join("rows.parquet"),
            &[rows(0, 1000)],
            properties,
        );
        let id = |op, value: i64| function(op, vec![column("id"), literal("bigint", json!(value))]);
        let not = |child| function("NOT", vec![child]);
        let label_is_not_null = function("IS_NOT_NULL", vec![column("label")]);
        let k = function("=", vec![column("k"), literal("int", json!(3))]);
        let in_list = function(
            "IN",
            vec![
                column("id"),
                literal("bigint", json!(5)),
                literal("bigint", json!(512)),
            ],
        );
        let label_starts = function(
            "STARTS_WITH",
            vec![column("label"), literal("string", json!("row-1"))],
        );
        // The predicates, and the ids that pass them.
        type Case = (Vec<Value>, fn(i64) -> bool);
        let cases: [Case; 6] = [
            (vec![id("<", 55)], |id| id < 55),
            (vec![not(id(">=", 55)), label_is_not_null.clone()], |id| {
                id < 55 && id % 7 != 0
            }),
            (vec![function("OR", vec![id(">", 990), k])], |id| {
                id > 990 || id % 10 == 3
            }),
            (vec![in_list], |id| id == 5 || id == 512),
            (vec![id(">", 300), id("<", 305)], |id| id > 300 && id < 305),
            (vec![label_is_not_null.clone(), label_starts], |id| {
                id % 7 != 0 && format!("row-{id}").starts_with("row-1")
            }),
        ];
        for batch_size in ["8192", "7"] {
            let source = source(&directory.0, &[("batchsize", batch_size)]);
            for (predicates, passes) in &cases {
                let columns = Some(&["label", "id", "k"][..]);
                let read = read(&source, columns, predicates, None).unwrap();
                let expected: Vec<i64> = (0..1000).filter(|&id| passes(id)).collect();
                assert_eq!(read.ids(), expected, "{predicates:?}");
                for batch in read.batches() {
                    let labels = batch.column(0).as_string_view();
                    for (row, id) in ids(batch).into_iter().enumerate() {
                        let label = (id % 7 != 0).then(|| format!("row-{id}"));
                        assert_eq!(
                            labels.is_valid(row).then(|| labels.value(row)),
                            label.as_deref()
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn an_empty_projection_counts_the_rows_that_pass() {
        let directory = TempDirectory::new();
        write(&directory.join("rows.parquet"), &[rows(0, 1000)], 100);
        let source = source(&directory.0, &[]);
        let count = |predicates: &[Value]| {
            let read = read(&source, Some(&[]), predicates, None).unwrap();
            assert!(read.batches().all(|batch| batch.num_columns() == 0));
            read.batches().map(RecordBatch::num_rows).sum::<usize>()
        };
        assert_eq!(count(&[]), 1000);
        assert_eq!(
            count(&[function("<", vec![column("k"), literal("int", json!(2))])]),
            200
        );
    }

    #[test]
    fn a_limit_caps_the_rows_of_each_task() {
        let directory = TempDirectory::new();
        write(&directory.join("a.parquet"), &[rows(0, 1000)], 100);
        write(&directory.join("b.parquet"), &[rows(1000, 2000)], 100);
        // Each file is a task of its own when opening it costs maxPartitionBytes.
        let source = source(
            &directory.0,
            &[("batchsize", "8"), ("opencostinbytes", "134217728")],
        );
        let limited = read(&source, Some(&["id"]), &[], Some(5)).unwrap();
        assert_eq!(limited.partitions.len(), 2);
        for batches in &limited.partitions {
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 5);
        }
        // With predicates, the limit counts the rows that pass them, over batches.
        let k = function("=", vec![column("k"), literal("int", json!(3))]);
        let filtered = read(&source, Some(&["id"]), &[k], Some(12)).unwrap();
        let mut tasks: Vec<Vec<i64>> = filtered
            .partitions
            .iter()
            .map(|batches| batches.iter().flat_map(ids).collect())
            .collect();
        tasks.sort_unstable();
        for (ids, first) in tasks.iter().zip([0, 1000]) {
            let expected: Vec<i64> = (first..).step_by(10).map(|id| id + 3).take(12).collect();
            assert_eq!(ids, &expected);
        }
        // The limit counts the rows of all the files of a task.
        let packed = self::source(&directory.0, &[]);
        let limited = read(&packed, Some(&["id"]), &[], Some(1500)).unwrap();
        assert_eq!(limited.partitions.len(), 1);
        assert_eq!(
            limited.batches().map(RecordBatch::num_rows).sum::<usize>(),
            1500
        );
    }

    #[test]
    fn packs_the_ranges_of_files_into_tasks_as_spark_does() {
        let directory = TempDirectory::new();
        for (index, rows_of_file) in [(0, 3000), (1, 1000), (2, 2000), (3, 500)] {
            let first = index * 10_000;
            let path = directory.join(&format!("{index}.parquet"));
            write(&path, &[rows(first, first + rows_of_file)], 100);
        }
        let sizes: Vec<u64> = (0..4)
            .map(|index| {
                fs::metadata(directory.join(&format!("{index}.parquet")))
                    .unwrap()
                    .len()
            })
            .collect();
        // Tasks of the two largest files, and then of the two smallest, with their costs to open.
        let max = sizes[0] + sizes[2] + 2;
        let options = [
            ("maxpartitionbytes", max.to_string()),
            ("opencostinbytes", "1".to_string()),
        ];
        let options: Vec<(&str, &str)> = options.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let source = source(&directory.0, &options);
        let plan = source
            .new_scan(ScanInfo::new(source.schema().unwrap()))
            .unwrap()
            .plan()
            .unwrap();
        let files: Vec<Vec<String>> = plan
            .partitions
            .iter()
            .map(|partition| {
                let names = partition.ranges.iter().map(|range| range.path.file_name());
                names
                    .map(|name| name.unwrap().to_string_lossy().into_owned())
                    .collect()
            })
            .collect();
        assert_eq!(
            files,
            [["0.parquet", "2.parquet"], ["1.parquet", "3.parquet"]]
        );
        let read = read(&source, Some(&["id"]), &[], None).unwrap();
        let mut expected: Vec<i64> = [(0, 3000), (10_000, 1000), (20_000, 2000), (30_000, 500)]
            .iter()
            .flat_map(|&(first, rows)| first..first + rows)
            .collect();
        expected.sort_unstable();
        assert_eq!(read.ids(), expected);
    }

    /// The INT96 of a timestamp: the nanoseconds of its day and its Julian day.
    fn int96(micros: i64) -> Int96 {
        const MICROS_PER_DAY: i64 = 86_400_000_000;
        let day = micros.div_euclid(MICROS_PER_DAY) + 2_440_588;
        let nanos = micros.rem_euclid(MICROS_PER_DAY) * 1000;
        let mut value = Int96::new();
        value.set_data(nanos as u32, (nanos >> 32) as u32, day as u32);
        value
    }

    #[test]
    fn reads_int96_timestamps_as_microseconds_in_utc() {
        let directory = TempDirectory::new();
        let path = directory.join("int96.parquet");
        let schema = "message spark_schema {
            optional int96 ts;
            optional group event { optional int96 at; }
        }";
        // Around the years 1019 and 9892, outside the range of nanoseconds, and the epoch.
        let micros = [-30_000_000_000_000_000, 0, 250_000_000_000_000_123];
        let values: Vec<Int96> = micros.iter().map(|&value| int96(value)).collect();
        let schema = Arc::new(parse_message_type(schema).unwrap());
        let mut writer =
            SerializedFileWriter::new(File::create(&path).unwrap(), schema, Default::default())
                .unwrap();
        let mut row_group = writer.next_row_group().unwrap();
        // The values of `ts`, then of `event.at`, whose structs are all present.
        for definition_level in [1, 2] {
            let mut column = row_group.next_column().unwrap().unwrap();
            let levels = [definition_level; 3];
            column
                .typed::<Int96Type>()
                .write_batch(&values, Some(&levels), None)
                .unwrap();
            column.close().unwrap();
        }
        row_group.close().unwrap();
        writer.close().unwrap();

        let source = source(&path, &[]);
        let utc = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let schema = source.schema().unwrap();
        assert_eq!(schema.field(0).data_type(), &utc);
        let DataType::Struct(event) = schema.field(1).data_type() else {
            panic!("event is a struct");
        };
        assert_eq!(event[0].data_type(), &utc);
        let read = read(&source, None, &[], None).unwrap();
        let batch = read.batches().next().unwrap();
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(ts.values().as_ref(), micros);
        let event = batch
            .column(1)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let at = event
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(at.values().as_ref(), micros);
    }

    #[test]
    fn rejects_types_that_spark_reads_differently_or_not_at_all() {
        let directory = TempDirectory::new();
        let columns: [(&str, ArrayRef); 3] = [
            (
                "millis",
                Arc::new(TimestampMillisecondArray::from(vec![1]).with_timezone("+00:00")),
            ),
            // Not an INT96, so it must not be read as microseconds.
            ("nanos", Arc::new(TimestampNanosecondArray::from(vec![1]))),
            ("unsigned", Arc::new(UInt32Array::from(vec![1]))),
        ];
        for (name, array) in columns {
            let path = directory.join(&format!("{name}.parquet"));
            write(
                &path,
                &[RecordBatch::try_from_iter([(name, array)]).unwrap()],
                1024,
            );
            let error = source(&path, &[]).schema().unwrap_err().to_string();
            assert!(
                error.contains(&format!("The column {name} contains")),
                "{error}"
            );
            assert!(error.contains("which this source does not read"), "{error}");
        }
    }

    #[test]
    fn reads_the_data_files_of_a_directory() {
        let directory = TempDirectory::new();
        write(&directory.join("b.parquet"), &[rows(5, 10)], 1024);
        write(&directory.join("a.parquet"), &[rows(0, 5)], 1024);
        fs::write(directory.join("_SUCCESS"), "").unwrap();
        fs::write(directory.join(".a.parquet.crc"), "not parquet").unwrap();
        fs::write(directory.join("empty.parquet"), "").unwrap();
        let files: Vec<_> = data_files(&directory.0)
            .unwrap()
            .into_iter()
            .map(|(path, _)| path)
            .collect();
        assert_eq!(
            files,
            [directory.join("a.parquet"), directory.join("b.parquet")]
        );
        let read = read(&source(&directory.0, &[]), None, &[], None).unwrap();
        assert_eq!(read.ids(), (0..10).collect::<Vec<_>>());
        let file = source(&directory.join("b.parquet"), &[]);
        assert_eq!(read_ids(&file), (5..10).collect::<Vec<_>>());

        fs::create_dir(directory.join("k=1")).unwrap();
        let error = data_files(&directory.0).unwrap_err().to_string();
        assert!(error.contains("does not read subdirectories"), "{error}");
        let empty = TempDirectory::new();
        let error = data_files(&empty.0).unwrap_err().to_string();
        assert!(
            error.contains("There is no Parquet file to read"),
            "{error}"
        );
    }

    fn read_ids(source: &ParquetSource) -> Vec<i64> {
        read(source, None, &[], None).unwrap().ids()
    }

    #[test]
    fn fails_for_a_column_that_a_file_lacks() {
        let directory = TempDirectory::new();
        write(&directory.join("a.parquet"), &[rows(0, 5)], 1024);
        let other = RecordBatch::try_from_iter([(
            "other",
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
        )])
        .unwrap();
        write(&directory.join("b.parquet"), &[other], 1024);
        let error = read(&source(&directory.0, &[]), Some(&["id"]), &[], None)
            .err()
            .unwrap()
            .to_string();
        assert!(
            error.contains("b.parquet: The file has no column id"),
            "{error}"
        );
    }

    #[test]
    fn stops_reading_when_the_task_is_cancelled() {
        let directory = TempDirectory::new();
        write(&directory.join("rows.parquet"), &[rows(0, 100)], 1024);
        let source = source(&directory.0, &[]);
        let schema = source.schema().unwrap();
        let plan = source
            .new_scan(ScanInfo::new(schema.clone()))
            .unwrap()
            .plan()
            .unwrap();
        let interrupted = AtomicBool::new(true);
        let task = TaskContext::for_testing().interrupted_by(&interrupted);
        let partition = plan.partitions.into_iter().next().unwrap();
        let mut reader = plan
            .factory
            .open(partition, ReaderContext::new(schema, task))
            .unwrap();
        let error = reader.next().unwrap().unwrap_err().to_string();
        assert!(error.contains("The Parquet read was cancelled"), "{error}");
    }

    #[test]
    fn rejects_invalid_options() {
        let info = |options: &[(&str, &str)]| {
            let options = options
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect();
            ParquetSource::new(CreateInfo::new("rust_sdk_parquet", options))
        };
        assert!(info(&[("path", "/tmp")]).is_ok());
        for (options, message) in [
            (&[][..], "The path option is required"),
            (&[("path", "relative")][..], "The path must be absolute"),
            (
                &[("path", "/tmp"), ("mergeschema", "true")][..],
                "Unknown Parquet option",
            ),
            (
                &[("path", "/tmp"), ("batchsize", "0")][..],
                "must be positive",
            ),
            (
                &[("path", "/tmp"), ("rowfilter", "maybe")][..],
                "Invalid rowfilter",
            ),
        ] {
            let error = info(options).err().unwrap().to_string();
            assert!(error.contains(message), "{error}");
        }
    }
}
