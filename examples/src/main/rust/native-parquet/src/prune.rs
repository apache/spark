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

//! Skips the row groups and pages that have no value in the domain of a column, as Spark's reader
//! does with the statistics of row groups and the column indexes of pages.
//!
//! The statistics of a row group or page bound its values and count its nulls. A range of rows
//! may have a value in the domain of a column if it may have nulls and the domain has null, or if
//! it has other values and its bounds meet one of the ranges of the domain. Unknown statistics may
//! be anything. The reader still evaluates the domains on every row that it reads, so pruning
//! changes what the reader decodes, but not what it returns.

use std::collections::HashMap;
use std::ops::{Bound, Range};

use arrow_array::{Array, ArrayRef, BooleanArray, Scalar, UInt64Array};
use arrow_ord::cmp;
use arrow_schema::ArrowError;
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, RowSelection};
use parquet::basic::{ColumnOrder, Type as PhysicalType};

use crate::filter::Filter;

/// The row groups that may have rows whose values are all in their domains, and the rows of these
/// row groups to read, if the column indexes of their pages exclude some.
pub(crate) fn prune(
    filters: &[Filter],
    metadata: &ArrowReaderMetadata,
    row_groups: Vec<usize>,
) -> Result<(Vec<usize>, Option<RowSelection>), ArrowError> {
    // Spark passes at most one domain for each column.
    let converters: Vec<(&str, StatisticsConverter)> = filters
        .iter()
        .filter_map(|filter| {
            let column = filter.column.as_str();
            Some((column, converter(column, metadata)?))
        })
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
}

/// Whether the bound of each row group or page may compare so with a value: an unknown bound may.
fn may(compare: Result<BooleanArray, ArrowError>) -> Result<Vec<bool>, ArrowError> {
    let compare = compare?;
    Ok((0..compare.len())
        .map(|entry| compare.is_null(entry) || compare.value(entry))
        .collect())
}

fn zip(left: &[bool], right: &[bool], f: impl Fn(bool, bool) -> bool) -> Vec<bool> {
    left.iter().zip(right).map(|(&a, &b)| f(a, b)).collect()
}

/// Whether each of `ranges` ranges of rows may have rows whose values are all in their domains.
fn passes(
    filters: &[Filter],
    ranges: usize,
    stats: &HashMap<&str, Stats>,
) -> Result<Vec<bool>, ArrowError> {
    let mut passes = vec![true; ranges];
    for filter in filters {
        if let Some(stats) = stats.get(filter.column.as_str()) {
            passes = zip(&passes, &may_have(filter, stats)?, |a, b| a && b);
        }
    }
    Ok(passes)
}

/// Whether each range of rows may have a value in the domain of a filter.
fn may_have(filter: &Filter, stats: &Stats) -> Result<Vec<bool>, ArrowError> {
    // Whether each row group or page may have a value of the domain other than null and NaN: one
    // in a range whose bounds its bounds meet.
    let entries = stats.min.len();
    let values = match &filter.ranges {
        None => vec![true; entries],
        Some(ranges) => {
            let mut values = vec![false; entries];
            for range in ranges {
                let low = match &range.low {
                    Bound::Included(value) => may(cmp::gt_eq(&stats.max, &Scalar::new(value)))?,
                    Bound::Excluded(value) => may(cmp::gt(&stats.max, &Scalar::new(value)))?,
                    Bound::Unbounded => vec![true; entries],
                };
                let high = match &range.high {
                    Bound::Included(value) => may(cmp::lt_eq(&stats.min, &Scalar::new(value)))?,
                    Bound::Excluded(value) => may(cmp::lt(&stats.min, &Scalar::new(value)))?,
                    Bound::Unbounded => vec![true; entries],
                };
                values = zip(&values, &zip(&low, &high, |a, b| a && b), |a, b| a || b);
            }
            values
        }
    };
    // The statistics of floats are not used, since they leave NaN out.
    let may_have = |entry: usize| {
        (filter.null_allowed && stats.some_null[entry])
            || filter.nan_allowed
            || (!stats.all_null[entry] && values[entry])
    };
    Ok(stats.entries.iter().map(|&entry| may_have(entry)).collect())
}

#[cfg(test)]
mod tests {
    use std::fs::{self, File};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use arrow_array::{Int32Array, Int64Array, RecordBatch, StringArray};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use spark_datasource::{Domain, Value, ValueRange};

    use super::*;

    fn domain(column: &str, ranges: Option<Vec<ValueRange>>) -> Domain {
        Domain::new(vec![column.into()], ranges)
    }

    fn range(low: Bound<Value>, high: Bound<Value>) -> ValueRange {
        ValueRange { low, high }
    }

    fn id(ranges: Vec<ValueRange>) -> Domain {
        domain("id", Some(ranges))
    }

    fn int(value: i64) -> Value {
        Value::Integer(value)
    }

    /// The domain of the labels that start with a prefix: from the prefix to its successor.
    fn starts_with(prefix: &str, successor: &str) -> Domain {
        let string = |value: &str| Value::String(value.into());
        let low = Bound::Included(string(prefix));
        domain(
            "label",
            Some(vec![range(low, Bound::Excluded(string(successor)))]),
        )
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

    /// The rows of the file that the reader reads with the domains.
    fn read(metadata: &ArrowReaderMetadata, domains: &[Domain]) -> Vec<usize> {
        let filters: Vec<Filter> = domains
            .iter()
            .map(|domain| Filter::new(domain, metadata.schema()).unwrap())
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
    fn skips_the_row_groups_and_pages_without_values_in_the_domains() {
        let metadata = metadata();
        let below = |value| range(Bound::Unbounded, Bound::Excluded(int(value)));
        let above = |value| range(Bound::Excluded(int(value)), Bound::Unbounded);
        // The pages from 0 to 59 have values below 50.
        assert_eq!(read(&metadata, &[id(vec![below(50)])]), rows(&[(0, 60)]));
        let at_least = range(Bound::Included(int(50)), Bound::Unbounded);
        assert_eq!(read(&metadata, &[id(vec![at_least])]), rows(&[(40, 1000)]));
        let fifty = ValueRange::single(int(50));
        assert_eq!(read(&metadata, &[id(vec![fifty])]), rows(&[(40, 60)]));
        // id <> 50.
        let other = id(vec![below(50), above(50)]);
        assert_eq!(read(&metadata, &[other]), rows(&[(0, 1000)]));
        // id > 0 AND id <= 19.
        let small = range(Bound::Excluded(int(0)), Bound::Included(int(19)));
        assert_eq!(read(&metadata, &[id(vec![small])]), rows(&[(0, 20)]));
        // id IN (5, 950), and id < 50 OR id > 950.
        let ids = id(vec![
            ValueRange::single(int(5)),
            ValueRange::single(int(950)),
        ]);
        assert_eq!(read(&metadata, &[ids]), rows(&[(0, 20), (940, 960)]));
        let ends = id(vec![below(50), above(950)]);
        assert_eq!(read(&metadata, &[ends]), rows(&[(0, 60), (940, 1000)]));
        assert!(read(&metadata, &[id(vec![below(0)])]).is_empty());
        assert!(read(&metadata, &[id(vec![])]).is_empty());
        // `k` takes every value in every page, so only the domain of `id` skips pages.
        let k = domain("k", Some(vec![ValueRange::single(int(3))]));
        assert_eq!(
            read(&metadata, std::slice::from_ref(&k)),
            rows(&[(0, 1000)])
        );
        assert_eq!(read(&metadata, &[k, id(vec![below(50)])]), rows(&[(0, 60)]));
    }

    #[test]
    fn skips_the_pages_whose_bounds_exclude_a_prefix() {
        let metadata = metadata();
        // The pages of 20 rows from 940 to 999.
        let read = |domain| read(&metadata, &[domain]);
        assert_eq!(read(starts_with("row-09", "row-0:")), rows(&[(900, 1000)]));
        assert_eq!(read(starts_with("row-095", "row-096")), rows(&[(940, 960)]));
        assert_eq!(read(starts_with("row-00", "row-01")), rows(&[(0, 100)]));
        assert!(read(starts_with("row-1", "row-2")).is_empty());
        assert!(read(starts_with("rox", "roy")).is_empty());
    }

    #[test]
    fn skips_by_the_null_counts_whether_or_not_the_domain_has_null() {
        let metadata = metadata();
        let label = |ranges, null_allowed| {
            let mut domain = domain("label", ranges);
            domain.null_allowed = null_allowed;
            read(&metadata, &[domain])
        };
        let string = |value: &str| Value::String(value.into());
        // IS NULL, and IS NOT NULL.
        assert_eq!(label(Some(vec![]), true), rows(&[(400, 600)]));
        assert_eq!(label(None, false), rows(&[(0, 400), (600, 1000)]));
        // The row group of nulls has no value below a bound.
        let below = vec![range(Bound::Unbounded, Bound::Excluded(string("row-0050")))];
        assert_eq!(label(Some(below), false), rows(&[(0, 60)]));
        // label <=> 'row-0042', and label IS NULL OR label = 'row-0042'.
        let value = || Some(vec![ValueRange::single(string("row-0042"))]);
        assert_eq!(label(value(), false), rows(&[(40, 60)]));
        assert_eq!(label(value(), true), rows(&[(40, 60), (400, 600)]));
        // `id` has no nulls.
        let mut null = domain("id", Some(vec![]));
        null.null_allowed = true;
        assert!(read(&metadata, &[null]).is_empty());
    }
}
