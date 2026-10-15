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
use arrow_array::{ArrayRef, Int64Array, RecordBatch, RecordBatchIterator, RecordBatchOptions};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use serde::{Deserialize, Serialize};
use spark_native_source::{
    BatchReader, BatchReaderFactory, BatchSource, FilterSupport, LimitSupport, Options, Predicate,
    Result, ScanBuilder, ScanPlan, TaskContext,
};

pub struct RangeSource {
    start: i64,
    end: i64,
    num_partitions: usize,
    batch_size: usize,
}

pub struct RangeScan {
    lower: i128,
    upper: i128,
    num_partitions: usize,
    limit: Option<usize>,
    factory: RangeReaderFactory,
}

/// Reader configuration travels to each executor; it contains no live objects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeReaderFactory {
    columns: Vec<String>,
    batch_size: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangePartition {
    start: i64,
    end: i64,
}

impl BatchSource for RangeSource {
    type Scan = RangeScan;

    fn new(options: Options) -> Result<Self> {
        for key in options.keys() {
            if !matches!(
                key.as_str(),
                "start" | "end" | "numpartitions" | "batchsize"
            ) {
                return Err(format!("Unknown range option: {key}").into());
            }
        }
        let start = option(&options, "start", 0_i64)?;
        let end = option(&options, "end", 100_i64)?;
        let num_partitions = option(&options, "numpartitions", 2_usize)?;
        let batch_size = option(&options, "batchsize", 1024_usize)?;
        if end < start {
            return Err("end must be greater than or equal to start".into());
        }
        if num_partitions == 0 || batch_size == 0 {
            return Err("numPartitions and batchSize must be positive".into());
        }
        Ok(Self {
            start,
            end,
            num_partitions,
            batch_size,
        })
    }

    fn schema(&self) -> Result<SchemaRef> {
        output_schema(&["id".into(), "label".into()])
    }

    fn scan(&self, schema: SchemaRef) -> Result<Self::Scan> {
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
        Ok(RangeScan {
            lower: i128::from(self.start),
            upper: i128::from(self.end),
            num_partitions: self.num_partitions,
            limit: None,
            factory: RangeReaderFactory {
                columns,
                batch_size: self.batch_size,
            },
        })
    }
}

impl BatchReaderFactory for RangeReaderFactory {
    type Partition = RangePartition;

    fn open<'task>(
        &self,
        partition: RangePartition,
        task: TaskContext<'task>,
    ) -> Result<BatchReader<'task>> {
        let factory = self.clone();
        if factory.batch_size == 0 || partition.start > partition.end {
            return Err("Invalid serialized range plan".into());
        }
        let schema = output_schema(&factory.columns)?;
        let batch_schema = Arc::clone(&schema);
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
            let next = (i128::from(current) + factory.batch_size as i128)
                .min(i128::from(partition.end)) as i64;
            let num_rows = (i128::from(next) - i128::from(current)) as usize;
            let columns: Vec<ArrayRef> = factory
                .columns
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
                Arc::clone(&batch_schema),
                columns,
                &RecordBatchOptions::new().with_row_count(Some(num_rows)),
            ))
        });
        Ok(Box::new(RecordBatchIterator::new(batches, schema)))
    }
}

impl ScanBuilder for RangeScan {
    type Factory = RangeReaderFactory;

    fn push_filters(&mut self, filters: &[Predicate]) -> Result<Vec<FilterSupport>> {
        Ok(filters
            .iter()
            .map(|filter| {
                if let Some((lower, upper)) = predicate_bounds(filter) {
                    self.lower = self.lower.max(lower);
                    self.upper = self.upper.min(upper);
                    FilterSupport::Exact
                } else {
                    FilterSupport::Unsupported
                }
            })
            .collect())
    }

    fn push_limit(&mut self, limit: usize) -> Result<LimitSupport> {
        self.limit = Some(self.limit.map_or(limit, |previous| previous.min(limit)));
        Ok(LimitSupport::Global)
    }

    fn prune_columns(&mut self, columns: &[String]) -> Result<bool> {
        if columns
            .iter()
            .any(|column| !self.factory.columns.contains(column))
        {
            return Err("Projection contains a column outside the read schema".into());
        }
        output_schema(columns)?;
        self.factory.columns = columns.to_vec();
        Ok(true)
    }

    fn plan(self) -> Result<ScanPlan<RangeReaderFactory>> {
        // Filter before applying a global limit, then split the selected interval.
        let upper = self.limit.map_or(self.upper, |limit| {
            self.upper.min(self.lower + limit as i128)
        });
        let rows = (upper - self.lower).max(0);
        let count = (self.num_partitions as i128).min(rows) as usize;
        let mut partitions = Vec::with_capacity(count);
        if count > 0 {
            let width = rows / count as i128;
            let extra = rows % count as i128;
            let mut start = self.lower;
            for index in 0..count {
                let end = start + width + i128::from((index as i128) < extra);
                partitions.push(RangePartition {
                    start: start as i64,
                    end: end as i64,
                });
                start = end;
            }
        }
        Ok(ScanPlan {
            factory: self.factory,
            partitions,
        })
    }
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

/// Closed-open bounds use i128 so strict comparisons never overflow at i64 extrema.
fn predicate_bounds(predicate: &Predicate) -> Option<(i128, i128)> {
    let Predicate::Function { name, children } = predicate else {
        return None;
    };
    if children.len() != 2 {
        return None;
    }
    if name == "AND" {
        // Do not mutate the scan when only one side of an AND is supported.
        let left = predicate_bounds(&children[0])?;
        let right = predicate_bounds(&children[1])?;
        return Some((left.0.max(right.0), left.1.min(right.1)));
    }
    let (value, reversed) = if is_id(&children[0]) {
        (integer_literal(&children[1])?, false)
    } else if is_id(&children[1]) {
        (integer_literal(&children[0])?, true)
    } else {
        return None;
    };
    let name = match (name.as_str(), reversed) {
        (">", true) => "<",
        (">=", true) => "<=",
        ("<", true) => ">",
        ("<=", true) => ">=",
        (name, _) => name,
    };
    let min = i128::from(i64::MIN);
    let max = i128::from(i64::MAX) + 1;
    let value = i128::from(value);
    match name {
        "=" | "<=>" => Some((value, value + 1)),
        ">" => Some((value + 1, max)),
        ">=" => Some((value, max)),
        "<" => Some((min, value)),
        "<=" => Some((min, value + 1)),
        _ => None,
    }
}

fn is_id(predicate: &Predicate) -> bool {
    matches!(predicate, Predicate::Column { name } if name.len() == 1 && name[0] == "id")
}

fn integer_literal(predicate: &Predicate) -> Option<i64> {
    let Predicate::Literal { data_type, value } = predicate else {
        return None;
    };
    let value = value.as_i64()?;
    match data_type.as_str() {
        "tinyint" if i8::try_from(value).is_ok() => Some(value),
        "smallint" if i16::try_from(value).is_ok() => Some(value),
        "int" if i32::try_from(value).is_ok() => Some(value),
        "bigint" => Some(value),
        _ => None,
    }
}

spark_native_source::export_data_source!(RangeSource);

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn source(values: &[(&str, &str)]) -> RangeSource {
        RangeSource::new(
            values
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
        .unwrap()
    }

    fn scan(source: &RangeSource) -> RangeScan {
        source.scan(source.schema().unwrap()).unwrap()
    }

    fn comparison(name: &str, value: i64, reversed: bool) -> Predicate {
        let mut children = vec![
            Predicate::Column {
                name: vec!["id".into()],
            },
            Predicate::Literal {
                data_type: "bigint".into(),
                value: json!(value),
            },
        ];
        if reversed {
            children.reverse();
        }
        Predicate::Function {
            name: name.into(),
            children,
        }
    }

    fn read(plan: ScanPlan<RangeReaderFactory>) -> Vec<RecordBatch> {
        plan.partitions
            .into_iter()
            .flat_map(|partition| {
                plan.factory
                    .open(partition, TaskContext::detached())
                    .unwrap()
                    .map(|batch| batch.unwrap())
                    .collect::<Vec<_>>()
            })
            .collect()
    }

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
    fn filters_before_global_limit_and_preserves_projected_order() {
        let source = source(&[
            ("start", "10"),
            ("end", "25"),
            ("numpartitions", "3"),
            ("batchsize", "2"),
        ]);
        let mut scan = scan(&source);
        assert_eq!(
            scan.push_filters(&[comparison(">=", 13, false), comparison("<", 22, false),])
                .unwrap(),
            vec![FilterSupport::Exact; 2]
        );
        assert_eq!(scan.push_limit(5).unwrap(), LimitSupport::Global);
        assert!(scan.prune_columns(&["label".into(), "id".into()]).unwrap());
        let plan = scan.plan().unwrap();
        assert_eq!(plan.partitions.len(), 3);
        let batches = read(plan);
        assert_eq!(ids(&batches), (13..18).collect::<Vec<_>>());
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
    fn comparisons_handle_reversed_operands_and_int64_boundaries() {
        for (start, end) in [(-2, 3), (i64::MIN, i64::MIN + 3), (i64::MAX - 3, i64::MAX)] {
            let source = source(&[("start", &start.to_string()), ("end", &end.to_string())]);
            for literal in [i64::MIN, -1, 0, 1, i64::MAX] {
                for op in ["=", "<=>", ">", ">=", "<", "<="] {
                    for reversed in [false, true] {
                        let mut scan = scan(&source);
                        assert_eq!(
                            scan.push_filters(&[comparison(op, literal, reversed)])
                                .unwrap(),
                            vec![FilterSupport::Exact]
                        );
                        let expected: Vec<_> = (start..end)
                            .filter(|id| {
                                let (left, right) = if reversed {
                                    (literal, *id)
                                } else {
                                    (*id, literal)
                                };
                                match op {
                                    "=" | "<=>" => left == right,
                                    ">" => left > right,
                                    ">=" => left >= right,
                                    "<" => left < right,
                                    "<=" => left <= right,
                                    _ => unreachable!(),
                                }
                            })
                            .collect();
                        assert_eq!(
                            ids(&read(scan.plan().unwrap())),
                            expected,
                            "range={start}..{end}, op={op}, value={literal}, reverse={reversed}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn unsupported_filters_remain_residual_without_mutating_the_scan() {
        let source = source(&[("end", "10")]);
        let mut scan = scan(&source);
        let unsupported_and = Predicate::Function {
            name: "AND".into(),
            children: vec![comparison(">", 5, false), Predicate::Unknown],
        };
        let null: Predicate = serde_json::from_value(json!({
            "type": "function", "name": "<=>", "children": [
                {"type": "column", "name": ["id"]},
                {"type": "literal", "dataType": "bigint", "value": null}
            ]
        }))
        .unwrap();
        let double: Predicate = serde_json::from_value(json!({
            "type": "function", "name": "=", "children": [
                {"type": "column", "name": ["id"]},
                {"type": "literal", "dataType": "double", "value": 1.5}
            ]
        }))
        .unwrap();
        assert_eq!(
            scan.push_filters(&[
                unsupported_and,
                null,
                double,
                comparison("FUTURE_COMPARISON", 0, false),
            ])
            .unwrap(),
            vec![FilterSupport::Unsupported; 4]
        );
        assert_eq!(
            ids(&read(scan.plan().unwrap())),
            (0..10).collect::<Vec<_>>()
        );
    }

    #[test]
    fn conjunctions_and_contradictory_predicates_form_exact_intersections() {
        let source = source(&[("end", "10")]);
        let mut first = scan(&source);
        let and = Predicate::Function {
            name: "AND".into(),
            children: vec![comparison(">=", 3, false), comparison("<=", 6, false)],
        };
        assert_eq!(
            first.push_filters(&[and]).unwrap(),
            vec![FilterSupport::Exact]
        );
        assert_eq!(ids(&read(first.plan().unwrap())), vec![3, 4, 5, 6]);
        let mut empty = scan(&source);
        empty
            .push_filters(&[comparison(">", 5, false), comparison("<", 3, false)])
            .unwrap();
        assert!(empty.plan().unwrap().partitions.is_empty());
    }

    #[test]
    fn serialized_plans_preserve_zero_column_row_counts_and_batch_sizes() {
        let source = source(&[("end", "7"), ("numpartitions", "1"), ("batchsize", "3")]);
        let mut scan = scan(&source);
        scan.prune_columns(&[]).unwrap();
        let plan = scan.plan().unwrap();
        let factory = serde_json::from_slice(&serde_json::to_vec(&plan.factory).unwrap()).unwrap();
        let partitions = plan
            .partitions
            .iter()
            .map(|partition| {
                serde_json::from_slice(&serde_json::to_vec(partition).unwrap()).unwrap()
            })
            .collect();
        let batches = read(ScanPlan {
            factory,
            partitions,
        });
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
    fn zero_limit_empty_range_and_repeated_limits_do_not_add_rows() {
        let provider = source(&[("end", "10"), ("numpartitions", "20")]);
        let mut zero = scan(&provider);
        zero.push_limit(0).unwrap();
        assert!(zero.plan().unwrap().partitions.is_empty());
        let mut limited = scan(&provider);
        limited.push_limit(2).unwrap();
        limited.push_limit(5).unwrap();
        let plan = limited.plan().unwrap();
        assert_eq!(plan.partitions.len(), 2);
        assert_eq!(ids(&read(plan)), vec![0, 1]);
        let empty = source(&[("start", "5"), ("end", "5")]);
        assert!(scan(&empty).plan().unwrap().partitions.is_empty());
    }

    #[test]
    fn rejects_invalid_options_and_incompatible_user_schemas() {
        for (key, value) in [
            ("batchsize", "0"),
            ("batchsize", "-1"),
            ("numpartitions", "0"),
            ("end", "-1"),
            ("start", "not-an-integer"),
            ("unknown", "1"),
        ] {
            assert!(RangeSource::new([(key.into(), value.into())].into()).is_err());
        }
        let source = source(&[]);
        let invalid = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        assert!(source.scan(invalid).is_err());
        let labels = output_schema(&["label".into()]).unwrap();
        let mut scan = source.scan(labels).unwrap();
        assert!(scan.prune_columns(&["id".into()]).is_err());
        assert!(output_schema(&["id".into(), "id".into()]).is_err());
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
        let mut reader = plan
            .factory
            .open(plan.partitions[0].clone(), TaskContext::detached())
            .unwrap();
        let first = reader.next().unwrap().unwrap();
        let second = reader.next().unwrap().unwrap();
        assert_eq!(ids(&[first]), vec![i64::MIN, i64::MIN + 1]);
        assert_eq!(ids(&[second]), vec![i64::MIN + 2, i64::MIN + 3]);
    }
}
