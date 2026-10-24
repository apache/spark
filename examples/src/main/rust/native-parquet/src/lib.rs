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
//! sources, and each task reads the row groups whose middle falls in its range. Pushed predicates
//! become an arrow-rs `RowFilter`: the reader decodes the columns of the predicates first, and
//! the other columns only for the rows that pass.

mod filter;
mod schema;

use std::fs::{self, File};
use std::path::{Path, PathBuf};

use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef};
use parquet::arrow::arrow_reader::{
    ArrowPredicate, ArrowPredicateFn, ArrowReaderMetadata, ParquetRecordBatchReaderBuilder,
    RowFilter,
};
use parquet::arrow::ProjectionMask;
use serde::{Deserialize, Serialize};
use spark_native_source::{
    CreateInfo, DataSource, FilterSupport, LimitSupport, Options, PartitionReader,
    PartitionReaderFactory, Predicate, ReaderContext, Result, ScanBuilder, ScanInfo, ScanPlan,
    Unsupported,
};

use crate::filter::Filter;

pub struct ParquetSource {
    path: PathBuf,
    batch_size: usize,
    max_partition_bytes: u64,
    row_filter: bool,
}

pub struct ParquetScan {
    path: PathBuf,
    // The schema to read, from which the pushed predicates take their column types.
    schema: SchemaRef,
    max_partition_bytes: u64,
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

/// A byte range of a file: its task reads the row groups whose middle falls in the range.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParquetPartition {
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
                "path" | "batchsize" | "maxpartitionbytes" | "rowfilter"
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
        let mut partitions = Vec::new();
        for (path, size) in data_files(&self.path)? {
            let mut start = 0;
            while start < size {
                let end = size.min(start.saturating_add(self.max_partition_bytes));
                partitions.push(ParquetPartition {
                    path: path.clone(),
                    start,
                    end,
                });
                start = end;
            }
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
        let path = partition.path.display().to_string();
        let in_file = |error: Box<dyn std::error::Error + Send + Sync>| format!("{path}: {error}");
        let file = File::open(&partition.path).map_err(|error| in_file(error.into()))?;
        let metadata = schema::load(&file).map_err(in_file)?;
        let row_groups = row_groups_in(&metadata, partition.start, partition.end);
        if row_groups.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }
        let file_schema = metadata.schema().clone();
        let parquet_schema = metadata.parquet_schema().clone();

        // The reader returns the projected columns in the order of the file; `order` restores
        // the order of the schema to read.
        let indices = context
            .schema
            .fields()
            .iter()
            .map(|field| {
                file_schema
                    .index_of(field.name())
                    .map_err(|_| format!("{path}: The file has no column {}", field.name()))
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let mut sorted = indices.clone();
        sorted.sort_unstable();
        let order: Vec<usize> = indices
            .iter()
            .map(|index| sorted.binary_search(index).expect("projected"))
            .collect();

        let mut builder = ParquetRecordBatchReaderBuilder::new_with_metadata(file, metadata)
            .with_row_groups(row_groups)
            .with_projection(ProjectionMask::roots(&parquet_schema, indices))
            .with_batch_size(self.batch_size);
        if !self.predicates.is_empty() {
            let mut predicates: Vec<Box<dyn ArrowPredicate>> = Vec::new();
            for predicate in &self.predicates {
                // The scan checked the predicates on the schema to read, which the file has.
                let filter = Filter::new(predicate, &file_schema).ok_or_else(|| {
                    format!("{path}: The predicate {predicate:?} does not apply to the file")
                })?;
                let mut columns = Vec::new();
                filter.columns(&mut columns);
                let columns = columns
                    .iter()
                    .map(|column| file_schema.index_of(column))
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                let mask = ProjectionMask::roots(&parquet_schema, columns);
                predicates.push(Box::new(ArrowPredicateFn::new(mask, move |batch| {
                    filter.evaluate(&batch)
                })));
            }
            builder = builder.with_row_filter(RowFilter::new(predicates));
        }
        if let Some(limit) = self.limit {
            builder = builder.with_limit(limit);
        }
        // Evaluates the row filter on the row groups of the task.
        let reader = builder.build().map_err(|error| in_file(error.into()))?;

        let task = context.task;
        Ok(Box::new(reader.map(move |batch| {
            if task.is_interrupted() {
                return Err(ArrowError::ExternalError(
                    "The Parquet read was cancelled".into(),
                ));
            }
            batch.and_then(|batch: RecordBatch| batch.project(&order))
        })))
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
    schema::load(&file)
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
    use spark_native_source::TaskContext;

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
        for (index, column) in batch.columns().iter().enumerate() {
            let read = batches[0].column(index);
            assert_eq!(read.len(), column.len());
            assert_eq!(
                read.to_data().buffers(),
                column.to_data().buffers(),
                "column {index}"
            );
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
    fn evaluates_the_pushed_predicates_in_a_row_filter() {
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
        let source = source(&directory.0, &[]);
        let read = read(&source, Some(&["id"]), &[], Some(5)).unwrap();
        assert_eq!(read.partitions.len(), 2);
        for batches in &read.partitions {
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 5);
        }
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
