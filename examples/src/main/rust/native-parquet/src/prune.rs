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

//! Skips the row groups and pages in which, by their statistics, no row passes the filters, as
//! Spark's reader does with the statistics of row groups and the column indexes of pages.
//!
//! The statistics of a row group or page bound its values and count its nulls. For each filter
//! and each range of rows, pruning finds whether the filter may be true for some row of the range,
//! and whether it may be false, so that it can also prune under NOT; a row for which a filter is
//! null is neither. Unknown statistics may be anything. The reader still evaluates the filters on
//! every row that it reads, so pruning changes what the reader decodes, but not what it returns.

use std::collections::HashMap;
use std::ops::Range;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, BooleanArray, Scalar, UInt64Array};
use arrow_ord::cmp;
use arrow_schema::{ArrowError, DataType};
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, RowSelection};
use parquet::basic::{ColumnOrder, Type as PhysicalType};

use crate::filter::{Comparison, Filter, TextMatch};

/// The row groups that may have rows that pass all the filters, and the rows of these row groups
/// to read, if the column indexes of their pages exclude some.
pub(crate) fn prune(
    filters: &[Filter],
    metadata: &ArrowReaderMetadata,
    row_groups: Vec<usize>,
) -> Result<(Vec<usize>, Option<RowSelection>), ArrowError> {
    let mut columns = Vec::new();
    for filter in filters {
        filter.columns(&mut columns);
    }
    let converters: Vec<(&str, StatisticsConverter)> = columns
        .into_iter()
        .filter_map(|column| Some((column, converter(column, metadata)?)))
        .collect();
    if converters.is_empty() {
        return Ok((row_groups, None));
    }

    let all = metadata.metadata().row_groups();
    let groups: Vec<_> = row_groups.iter().map(|&index| &all[index]).collect();
    let mut stats = HashMap::new();
    for (column, converter) in &converters {
        let nulls = converter.row_group_null_counts(groups.iter().copied())?;
        let rows = groups.iter().map(|group| group.num_rows() as u64);
        stats.insert(
            *column,
            Stats::new(
                converter.row_group_mins(groups.iter().copied())?,
                converter.row_group_maxes(groups.iter().copied())?,
                &nulls,
                rows,
                (0..groups.len()).collect(),
            ),
        );
    }
    let row_groups: Vec<usize> = row_groups
        .into_iter()
        .zip(passes(filters, groups.len(), &stats)?)
        .filter_map(|(index, passes)| passes.then_some(index))
        .collect();

    let (Some(column_index), Some(offset_index)) = (
        metadata.metadata().column_index(),
        metadata.metadata().offset_index(),
    ) else {
        return Ok((row_groups, None));
    };
    let mut kept = Vec::new();
    let mut selectors = Vec::new();
    let mut skips = false;
    for index in row_groups {
        let rows = all[index].num_rows() as usize;
        // The ranges between the first rows of the pages of any column.
        let mut starts = vec![0, rows];
        let mut pages = Vec::new();
        for (column, converter) in &converters {
            let parquet = converter
                .parquet_column_index()
                .expect("a column of the file");
            let firsts: Vec<usize> = offset_index[index][parquet]
                .page_locations()
                .iter()
                .map(|location| location.first_row_index as usize)
                .collect();
            // A column without an offset index, or with an invalid one, has unknown pages.
            let valid = firsts.first() == Some(&0) && firsts.windows(2).all(|w| w[0] < w[1]);
            if !valid || firsts[firsts.len() - 1] >= rows {
                continue;
            }
            starts.extend(&firsts);
            pages.push((*column, converter, firsts));
        }
        starts.sort_unstable();
        starts.dedup();
        let ranges: Vec<Range<usize>> = starts.windows(2).map(|w| w[0]..w[1]).collect();

        let mut stats = HashMap::new();
        for (column, converter, firsts) in pages {
            let ends = firsts.iter().skip(1).copied().chain([rows]);
            let page_rows = firsts
                .iter()
                .zip(ends)
                .map(|(first, end)| (end - first) as u64);
            // The page of each range.
            let entries = ranges
                .iter()
                .map(|range| firsts.partition_point(|&first| first <= range.start) - 1)
                .collect();
            let group = [index];
            stats.insert(
                column,
                Stats::new(
                    converter.data_page_mins(column_index, offset_index, &group)?,
                    converter.data_page_maxes(column_index, offset_index, &group)?,
                    &converter.data_page_null_counts(column_index, offset_index, &group)?,
                    page_rows,
                    entries,
                ),
            );
        }
        let read: Vec<Range<usize>> = ranges
            .iter()
            .zip(passes(filters, ranges.len(), &stats)?)
            .filter_map(|(range, passes)| passes.then_some(range.clone()))
            .collect();
        let read_rows: usize = read.iter().map(|range| range.len()).sum();
        skips |= read_rows < rows;
        if read_rows > 0 {
            kept.push(index);
            selectors.extend(RowSelection::from_consecutive_ranges(read.into_iter(), rows).iter());
        }
    }
    let selection = skips.then(|| RowSelection::from(selectors));
    Ok((kept, selection))
}

/// The converter of the statistics of a top-level column, or `None` if they do not bound its
/// values as Spark orders them. INT96 values have no defined order, writers leave NaN, which Spark
/// orders above all numbers, out of the bounds of floats, and arrow-rs reads the deprecated bounds
/// of old files, which order bytes as signed, when a file defines no order.
fn converter<'a>(
    column: &str,
    metadata: &'a ArrowReaderMetadata,
) -> Option<StatisticsConverter<'a>> {
    let converter =
        StatisticsConverter::try_new(column, metadata.schema(), metadata.parquet_schema())
            .ok()?
            .with_missing_null_counts_as_zero(false);
    let index = converter.parquet_column_index()?;
    let order = metadata.metadata().file_metadata().column_order(index);
    match metadata.parquet_schema().column(index).physical_type() {
        PhysicalType::INT96 | PhysicalType::FLOAT | PhysicalType::DOUBLE => None,
        PhysicalType::BYTE_ARRAY | PhysicalType::FIXED_LEN_BYTE_ARRAY
            if !matches!(order, ColumnOrder::TYPE_DEFINED_ORDER(_)) =>
        {
            None
        }
        _ => Some(converter),
    }
}

/// The statistics of a column for a sequence of ranges of rows, each in one row group or page.
struct Stats {
    // The bounds of each row group or page, null if unknown.
    min: ArrayRef,
    max: ArrayRef,
    // Whether each row group or page may have nulls, and whether all its rows are.
    some_null: Vec<bool>,
    all_null: Vec<bool>,
    // The row group or page of each range.
    entries: Vec<usize>,
}

impl Stats {
    fn new(
        min: ArrayRef,
        max: ArrayRef,
        nulls: &UInt64Array,
        rows: impl Iterator<Item = u64>,
        entries: Vec<usize>,
    ) -> Self {
        let nulls = nulls.iter().zip(rows);
        let (some_null, all_null) = nulls
            .map(|(nulls, rows)| (nulls != Some(0), nulls == Some(rows)))
            .unzip();
        Self {
            min,
            max,
            some_null,
            all_null,
            entries,
        }
    }

    /// Whether the bound of each range may compare so with a value: an unknown bound may.
    fn may(&self, compare: Result<BooleanArray, ArrowError>) -> Result<Vec<bool>, ArrowError> {
        let compare = compare?;
        let may = |entry: usize| compare.is_null(entry) || compare.value(entry);
        Ok(self.entries.iter().map(|&entry| may(entry)).collect())
    }
}

/// Whether each range of rows may have rows for which a filter is true, and rows for which it
/// is false.
struct Outcomes {
    can_be_true: Vec<bool>,
    can_be_false: Vec<bool>,
}

impl Outcomes {
    fn all(ranges: usize, can_be_true: bool, can_be_false: bool) -> Self {
        Self {
            can_be_true: vec![can_be_true; ranges],
            can_be_false: vec![can_be_false; ranges],
        }
    }

    fn and(self, other: Self) -> Self {
        Self {
            can_be_true: zip(&self.can_be_true, &other.can_be_true, |a, b| a && b),
            can_be_false: zip(&self.can_be_false, &other.can_be_false, |a, b| a || b),
        }
    }

    fn or(self, other: Self) -> Self {
        Self {
            can_be_true: zip(&self.can_be_true, &other.can_be_true, |a, b| a || b),
            can_be_false: zip(&self.can_be_false, &other.can_be_false, |a, b| a && b),
        }
    }

    fn not(self) -> Self {
        Self {
            can_be_true: self.can_be_false,
            can_be_false: self.can_be_true,
        }
    }
}

fn zip(left: &[bool], right: &[bool], f: impl Fn(bool, bool) -> bool) -> Vec<bool> {
    left.iter().zip(right).map(|(&a, &b)| f(a, b)).collect()
}

/// Whether each of `ranges` ranges of rows may have rows that pass all the filters.
fn passes(
    filters: &[Filter],
    ranges: usize,
    stats: &HashMap<&str, Stats>,
) -> Result<Vec<bool>, ArrowError> {
    let mut passes = vec![true; ranges];
    for filter in filters {
        passes = zip(
            &passes,
            &outcomes(filter, ranges, stats)?.can_be_true,
            |a, b| a && b,
        );
    }
    Ok(passes)
}

fn outcomes(
    filter: &Filter,
    ranges: usize,
    stats: &HashMap<&str, Stats>,
) -> Result<Outcomes, ArrowError> {
    let unknown = || Outcomes::all(ranges, true, true);
    Ok(match filter {
        Filter::Compare { column, op, value } => match stats.get(column.as_str()) {
            Some(stats) => compare(stats, *op, value.as_ref())?,
            None => unknown(),
        },
        Filter::IsNull { column, negated } => match stats.get(column.as_str()) {
            Some(stats) => {
                let is_null = is_null(stats);
                if *negated {
                    is_null.not()
                } else {
                    is_null
                }
            }
            None => unknown(),
        },
        Filter::In { column, values } => match stats.get(column.as_str()) {
            Some(stats) => {
                let mut result = Outcomes::all(ranges, false, true);
                for value in values {
                    result = result.or(compare(stats, Comparison::Equal, value.as_ref())?);
                }
                result
            }
            None => unknown(),
        },
        Filter::Text {
            column,
            op: TextMatch::StartsWith,
            pattern,
        } => match (stats.get(column.as_str()), bytes(pattern, 0)) {
            (Some(stats), Some(prefix)) => starts_with(stats, prefix),
            _ => unknown(),
        },
        Filter::Text { .. } => unknown(),
        Filter::And(left, right) => {
            outcomes(left, ranges, stats)?.and(outcomes(right, ranges, stats)?)
        }
        Filter::Or(left, right) => {
            outcomes(left, ranges, stats)?.or(outcomes(right, ranges, stats)?)
        }
        Filter::Not(child) => outcomes(child, ranges, stats)?.not(),
    })
}

fn is_null(stats: &Stats) -> Outcomes {
    let all_null = stats.entries.iter().map(|&entry| stats.all_null[entry]);
    Outcomes {
        can_be_true: stats
            .entries
            .iter()
            .map(|&entry| stats.some_null[entry])
            .collect(),
        can_be_false: all_null.map(|all_null| !all_null).collect(),
    }
}

/// The outcomes of STARTS_WITH. The strings that start with a prefix are those from the prefix to
/// its successor, the prefix without its trailing 0xFF bytes and with its last byte incremented:
/// a range of rows may have such strings if its bounds meet these, and others unless its bounds
/// are within them. Without a successor, every string from the prefix on starts with it.
fn starts_with(stats: &Stats, prefix: &[u8]) -> Outcomes {
    let mut successor = prefix.to_vec();
    while successor.last() == Some(&0xff) {
        successor.pop();
    }
    let successor = match successor.last_mut() {
        Some(last) => {
            *last += 1;
            Some(successor)
        }
        None => None,
    };
    let below_successor = |value: &[u8]| successor.as_deref().is_none_or(|end| value < end);
    let (can_be_true, can_be_false) = stats
        .entries
        .iter()
        .map(|&entry| {
            if stats.all_null[entry] {
                return (false, false);
            }
            let (min, max) = (bytes(&stats.min, entry), bytes(&stats.max, entry));
            let can_be_true =
                max.is_none_or(|max| max >= prefix) && min.is_none_or(below_successor);
            let within = min.is_some_and(|min| min >= prefix) && max.is_some_and(below_successor);
            (can_be_true, !within)
        })
        .unzip();
    Outcomes {
        can_be_true,
        can_be_false,
    }
}

/// The bytes of a string, or `None` if it is null.
fn bytes(array: &ArrayRef, index: usize) -> Option<&[u8]> {
    if array.is_null(index) {
        return None;
    }
    match array.data_type() {
        DataType::Utf8 => Some(array.as_string::<i32>().value(index).as_bytes()),
        DataType::Utf8View => Some(array.as_string_view().value(index).as_bytes()),
        _ => None,
    }
}

/// The outcomes of comparing a column with a value, or with null.
fn compare(
    stats: &Stats,
    op: Comparison,
    value: Option<&ArrayRef>,
) -> Result<Outcomes, ArrowError> {
    let Some(value) = value else {
        // Null compares as null, except with <=>, which is IS NULL.
        return Ok(match op {
            Comparison::NullSafeEqual => is_null(stats),
            _ => Outcomes::all(stats.entries.len(), false, false),
        });
    };
    let value = Scalar::new(value);
    let (min, max) = (&stats.min, &stats.max);
    // Some value of the range may be less than, or greater than, the value.
    let below = || stats.may(cmp::lt(min, &value));
    let above = || stats.may(cmp::gt(max, &value));
    // Some value may be at most, or at least, the value.
    let at_most = || stats.may(cmp::lt_eq(min, &value));
    let at_least = || stats.may(cmp::gt_eq(max, &value));
    let both = |left: Vec<bool>, right: Vec<bool>| zip(&left, &right, |a, b| a && b);
    let either = |left: Vec<bool>, right: Vec<bool>| zip(&left, &right, |a, b| a || b);
    // Some value may equal the value, or differ from it.
    let equal = || Ok::<_, ArrowError>(both(at_most()?, at_least()?));
    let differ = || Ok::<_, ArrowError>(either(below()?, above()?));
    let (can_be_true, can_be_false) = match op {
        Comparison::Equal => (equal()?, differ()?),
        Comparison::NotEqual => (differ()?, equal()?),
        // Null rows are false, so the comparison may be false for any range.
        Comparison::NullSafeEqual => (equal()?, vec![true; stats.entries.len()]),
        Comparison::Less => (below()?, at_least()?),
        Comparison::LessOrEqual => (at_most()?, above()?),
        Comparison::Greater => (above()?, at_most()?),
        Comparison::GreaterOrEqual => (at_least()?, below()?),
    };
    let mut outcomes = Outcomes {
        can_be_true,
        can_be_false,
    };
    // A range of nulls is neither, or false for <=>.
    for (i, &entry) in stats.entries.iter().enumerate() {
        if stats.all_null[entry] {
            outcomes.can_be_true[i] = false;
            outcomes.can_be_false[i] = op == Comparison::NullSafeEqual;
        }
    }
    Ok(outcomes)
}

#[cfg(test)]
mod tests {
    use std::fs::{self, File};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use arrow_array::{Int32Array, Int64Array, RecordBatch, StringArray};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use serde_json::{json, Value};
    use spark_native_source::Predicate;

    use super::*;

    fn column(name: &str) -> Value {
        json!({"type": "column", "name": [name]})
    }

    fn literal(data_type: &str, value: Value) -> Value {
        json!({"type": "literal", "dataType": data_type, "value": value})
    }

    fn function(name: &str, children: Vec<Value>) -> Value {
        json!({"type": "function", "name": name, "children": children})
    }

    fn id(op: &str, value: i64) -> Value {
        function(op, vec![column("id"), literal("bigint", json!(value))])
    }

    fn label(op: &str, value: &str) -> Value {
        function(op, vec![column("label"), literal("string", json!(value))])
    }

    /// 1000 rows in row groups of 200 and pages of 20: `id` from 0, `k` = id % 10, and `label`
    /// = "row-0042" for 42, null in the third row group.
    fn metadata() -> ArrowReaderMetadata {
        let ids: Vec<i64> = (0..1000).collect();
        let label = ids
            .iter()
            .map(|id| (!(400..600).contains(id)).then(|| format!("row-{id:04}")));
        let batch = RecordBatch::try_from_iter([
            ("id", Arc::new(Int64Array::from(ids.clone())) as ArrayRef),
            (
                "k",
                Arc::new(Int32Array::from_iter_values(
                    ids.iter().map(|id| (id % 10) as i32),
                )) as _,
            ),
            ("label", Arc::new(StringArray::from_iter(label)) as _),
        ])
        .unwrap();
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(200))
            .set_data_page_row_count_limit(20)
            .set_write_batch_size(20)
            .build();
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let name = format!(
            "rust-sdk-prune-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        );
        let path = std::env::temp_dir().join(name);
        let mut writer = ArrowWriter::try_new(
            File::create(&path).unwrap(),
            batch.schema(),
            Some(properties),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let metadata = crate::schema::load(&File::open(&path).unwrap(), true).unwrap();
        fs::remove_file(&path).unwrap();
        metadata
    }

    /// The rows of the file that the reader reads with the predicates.
    fn read(metadata: &ArrowReaderMetadata, predicates: &[Value]) -> Vec<usize> {
        let filters: Vec<Filter> = predicates
            .iter()
            .map(|predicate| {
                let predicate: Predicate = serde_json::from_value(predicate.clone()).unwrap();
                Filter::new(&predicate, metadata.schema()).unwrap()
            })
            .collect();
        let row_groups = (0..metadata.metadata().num_row_groups()).collect();
        let (row_groups, selection) = prune(&filters, metadata, row_groups).unwrap();
        // The rows of the row groups to read, in order, which the selection selects from.
        let rows: Vec<usize> = row_groups
            .iter()
            .flat_map(|&index| index * 200..(index + 1) * 200)
            .collect();
        let Some(selection) = selection else {
            return rows;
        };
        assert_eq!(
            selection.row_count() + selection.skipped_row_count(),
            rows.len()
        );
        let mut read = Vec::new();
        let mut next = 0;
        for selector in selection.iter() {
            if !selector.skip {
                read.extend(&rows[next..next + selector.row_count]);
            }
            next += selector.row_count;
        }
        read
    }

    /// The rows in the ranges `[start, end)`.
    fn rows(ranges: &[(usize, usize)]) -> Vec<usize> {
        ranges.iter().flat_map(|&(start, end)| start..end).collect()
    }

    #[test]
    fn skips_the_row_groups_and_pages_that_no_row_passes() {
        let metadata = metadata();
        // The pages from 0 to 59 have values below 50.
        assert_eq!(read(&metadata, &[id("<", 50)]), rows(&[(0, 60)]));
        assert_eq!(read(&metadata, &[id(">=", 50)]), rows(&[(40, 1000)]));
        assert_eq!(read(&metadata, &[id("=", 50)]), rows(&[(40, 60)]));
        assert_eq!(read(&metadata, &[id("<>", 50)]), rows(&[(0, 1000)]));
        assert_eq!(
            read(&metadata, &[id("<=", 19), id(">", 0)]),
            rows(&[(0, 20)])
        );
        let ids = |values: &[i64]| {
            let mut children = vec![column("id")];
            children.extend(values.iter().map(|&value| literal("bigint", json!(value))));
            function("IN", children)
        };
        assert_eq!(
            read(&metadata, &[ids(&[5, 950])]),
            rows(&[(0, 20), (940, 960)])
        );
        assert!(read(&metadata, &[id("<", 0)]).is_empty());
        assert!(read(&metadata, &[id("<", 50), id(">", 950)]).is_empty());
        // `k` takes every value in every page.
        let k = function("=", vec![column("k"), literal("int", json!(3))]);
        assert_eq!(read(&metadata, &[k]), rows(&[(0, 1000)]));
    }

    #[test]
    fn prunes_by_the_outcomes_of_not_and_or() {
        let metadata = metadata();
        let not = |child| function("NOT", vec![child]);
        let or = |left, right| function("OR", vec![left, right]);
        assert_eq!(read(&metadata, &[not(id(">=", 50))]), rows(&[(0, 60)]));
        assert_eq!(read(&metadata, &[not(not(id("<", 50)))]), rows(&[(0, 60)]));
        assert_eq!(
            read(&metadata, &[or(id("<", 50), id(">", 950))]),
            rows(&[(0, 60), (940, 1000)])
        );
        // NOT of a comparison with a null is null, as is the comparison.
        let null = function("<", vec![column("id"), literal("bigint", Value::Null)]);
        assert!(read(&metadata, std::slice::from_ref(&null)).is_empty());
        assert!(read(&metadata, &[not(null)]).is_empty());
        // The reader evaluates ENDS_WITH and CONTAINS on each row, but pruning does not bound them.
        let ends = label("ENDS_WITH", "9");
        assert_eq!(
            read(&metadata, std::slice::from_ref(&ends)),
            rows(&[(0, 1000)])
        );
        assert_eq!(read(&metadata, &[ends, id("<", 50)]), rows(&[(0, 60)]));
    }

    #[test]
    fn skips_the_pages_whose_bounds_exclude_a_prefix() {
        let metadata = metadata();
        let not = |child| function("NOT", vec![child]);
        // The pages of 20 rows from 940 to 999.
        assert_eq!(
            read(&metadata, &[label("STARTS_WITH", "row-09")]),
            rows(&[(900, 1000)])
        );
        assert_eq!(
            read(&metadata, &[label("STARTS_WITH", "row-095")]),
            rows(&[(940, 960)])
        );
        assert_eq!(
            read(&metadata, &[label("STARTS_WITH", "row-00")]),
            rows(&[(0, 100)])
        );
        assert!(read(&metadata, &[label("STARTS_WITH", "row-1")]).is_empty());
        assert!(read(&metadata, &[label("STARTS_WITH", "rox")]).is_empty());
        // Every label starts with "row-", so that no page passes the negation, except none: the
        // row group of nulls passes neither.
        assert!(read(&metadata, &[not(label("STARTS_WITH", "row-"))]).is_empty());
        assert_eq!(
            read(&metadata, &[label("STARTS_WITH", "")]),
            rows(&[(0, 400), (600, 1000)])
        );
        assert_eq!(
            read(&metadata, &[not(label("STARTS_WITH", "row-09"))]),
            rows(&[(0, 400), (600, 900)])
        );
    }

    #[test]
    fn a_comparison_is_neither_true_nor_false_for_nulls() {
        let metadata = metadata();
        let not = |child| function("NOT", vec![child]);
        let is_null = |name| function(name, vec![column("label")]);
        assert_eq!(read(&metadata, &[label("<", "row-0050")]), rows(&[(0, 60)]));
        // The row group of nulls passes neither the comparison nor its negation.
        assert_eq!(
            read(&metadata, &[not(label("<", "row-0900"))]),
            rows(&[(900, 1000)])
        );
        assert_eq!(read(&metadata, &[is_null("IS_NULL")]), rows(&[(400, 600)]));
        assert_eq!(
            read(&metadata, &[is_null("IS_NOT_NULL")]),
            rows(&[(0, 400), (600, 1000)])
        );
        let null_safe = |value| function("<=>", vec![column("label"), literal("string", value)]);
        assert_eq!(
            read(&metadata, &[null_safe(Value::Null)]),
            rows(&[(400, 600)])
        );
        assert_eq!(
            read(&metadata, &[null_safe(json!("row-0042"))]),
            rows(&[(40, 60)])
        );
        // `id` has no nulls.
        assert!(read(&metadata, &[function("IS_NULL", vec![column("id")])]).is_empty());
    }
}
