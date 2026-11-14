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

//! The domains that Spark pushes, which the reader evaluates on the batches that it decodes.

use std::ops::Bound;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float32Type, Float64Type};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Decimal128Array,
    Decimal64Array, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    RecordBatch, Scalar, StringArray, StringViewArray, TimestampMicrosecondArray,
};
use arrow_buffer::{BooleanBuffer, Buffer};
use arrow_ord::cmp;
use arrow_schema::{ArrowError, DataType, Schema, TimeUnit};
use spark_datasource::{Domain, Value};

/// The domain of a top-level column, with bounds of the Arrow type of the column. A row whose
/// value is not in the domain matches none of the rows that the filters of the query keep, so
/// the reader drops it, and skips the row groups and pages that have no value in the domain.
#[derive(Debug)]
pub(crate) struct Filter {
    pub(crate) column: String,
    pub(crate) null_allowed: bool,
    pub(crate) nan_allowed: bool,
    // The ranges of the other values in the domain, or None if they are all in it.
    pub(crate) ranges: Option<Vec<Range>>,
}

/// A range of the values of a domain, whose bounds are one-element arrays of the type of the
/// column, with floats that Arrow compares as Spark does.
#[derive(Debug)]
pub(crate) struct Range {
    pub(crate) low: Bound<ArrayRef>,
    pub(crate) high: Bound<ArrayRef>,
    // The value of a range of a single value, such as one of an IN list.
    single: Option<ArrayRef>,
}

impl Filter {
    /// The filter of a domain on a top-level column of `schema`, the schema that Spark reads or
    /// that of a file, or `None` if the reader does not evaluate it: for a field of a struct, a
    /// column that the schema does not have, or values of another type than the column.
    pub(crate) fn new(domain: &Domain, schema: &Schema) -> Option<Self> {
        let [column] = domain.path.as_slice() else {
            return None;
        };
        let data_type = comparable(schema.field_with_name(column).ok()?.data_type())?;
        let ranges = match &domain.ranges {
            None => None,
            Some(ranges) => Some(
                ranges
                    .iter()
                    .map(|range| {
                        let single = match (&range.low, &range.high) {
                            (Bound::Included(low), Bound::Included(high)) if low == high => {
                                Some(literal(low, data_type)?)
                            }
                            _ => None,
                        };
                        Some(Range {
                            low: bound(&range.low, data_type)?,
                            high: bound(&range.high, data_type)?,
                            single,
                        })
                    })
                    .collect::<Option<Vec<_>>>()?,
            ),
        };
        Some(Self {
            column: column.clone(),
            null_allowed: domain.null_allowed,
            nan_allowed: domain.nan_allowed,
            ranges,
        })
    }

    /// Whether the value of each row of a batch that has the column is in the domain. A row that
    /// is false matches no filter of the query.
    pub(crate) fn evaluate(&self, batch: &RecordBatch) -> Result<BooleanBuffer, ArrowError> {
        let array = column_of(batch, &self.column)?;
        let mut matches = match &self.ranges {
            Some(ranges) => in_ranges(&spark_ordered(array), ranges)?,
            None => BooleanBuffer::new_set(array.len()),
        };
        if let Some(nans) = nans(array) {
            matches = if self.nan_allowed {
                &matches | &nans
            } else {
                &matches & &!&nans
            };
        }
        if let Some(nulls) = array.nulls() {
            let valid = nulls.inner();
            matches = if self.null_allowed {
                &(&matches & valid) | &!valid
            } else {
                &matches & valid
            };
        }
        Ok(matches)
    }
}

/// Whether each value of a column is in one of the ranges. The bits of nulls and NaNs are
/// arbitrary.
fn in_ranges(array: &ArrayRef, ranges: &[Range]) -> Result<BooleanBuffer, ArrowError> {
    let singles: Option<Vec<&ArrayRef>> =
        ranges.iter().map(|range| range.single.as_ref()).collect();
    if let Some(matches) = singles.and_then(|values| equal_views(array, &values)) {
        return Ok(matches);
    }
    let mut matches = BooleanBuffer::new_unset(array.len());
    for range in ranges {
        let in_range = match &range.single {
            Some(value) => cmp::eq(array, &Scalar::new(value))?.values().clone(),
            None => {
                let low = match &range.low {
                    Bound::Included(value) => Some(cmp::gt_eq(array, &Scalar::new(value))?),
                    Bound::Excluded(value) => Some(cmp::gt(array, &Scalar::new(value))?),
                    Bound::Unbounded => None,
                };
                let high = match &range.high {
                    Bound::Included(value) => Some(cmp::lt_eq(array, &Scalar::new(value))?),
                    Bound::Excluded(value) => Some(cmp::lt(array, &Scalar::new(value))?),
                    Bound::Unbounded => None,
                };
                match (low, high) {
                    (Some(low), Some(high)) => low.values() & high.values(),
                    (Some(bound), None) | (None, Some(bound)) => bound.values().clone(),
                    (None, None) => BooleanBuffer::new_set(array.len()),
                }
            }
        };
        matches = &matches | &in_range;
    }
    Ok(matches)
}

/// Whether each value of a float or double column is NaN, or `None` for another column.
fn nans(array: &ArrayRef) -> Option<BooleanBuffer> {
    match array.data_type() {
        DataType::Float32 => {
            let values = array.as_primitive::<Float32Type>().values();
            Some(BooleanBuffer::collect_bool(values.len(), |row| {
                values[row].is_nan()
            }))
        }
        DataType::Float64 => {
            let values = array.as_primitive::<Float64Type>().values();
            Some(BooleanBuffer::collect_bool(values.len(), |row| {
                values[row].is_nan()
            }))
        }
        _ => None,
    }
}

/// The type of a column whose values Arrow compares as Spark does, in the schema that Spark
/// reads or in the one that the reader reads, which has views of strings and binary values, and
/// 64-bit decimals. Floats compare as Spark compares them once [`spark_ordered`] changes their
/// zeros.
fn comparable(data_type: &DataType) -> Option<&DataType> {
    match data_type {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::BinaryView
        | DataType::Date32
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Timestamp(TimeUnit::Microsecond, _) => Some(data_type),
        _ => None,
    }
}

fn bound(bound: &Bound<Value>, data_type: &DataType) -> Option<Bound<ArrayRef>> {
    Some(match bound {
        Bound::Included(value) => Bound::Included(literal(value, data_type)?),
        Bound::Excluded(value) => Bound::Excluded(literal(value, data_type)?),
        Bound::Unbounded => Bound::Unbounded,
    })
}

/// A value as a one-element array of `data_type`, or `None` if it is not a value of that type.
/// Spark compares strings and binary values by their bytes, as Arrow does.
fn literal(value: &Value, data_type: &DataType) -> Option<ArrayRef> {
    Some(match (data_type, value) {
        (DataType::Boolean, Value::Boolean(value)) => Arc::new(BooleanArray::from(vec![*value])),
        (DataType::Int8, Value::Integer(value)) => {
            Arc::new(Int8Array::from(vec![i8::try_from(*value).ok()?]))
        }
        (DataType::Int16, Value::Integer(value)) => {
            Arc::new(Int16Array::from(vec![i16::try_from(*value).ok()?]))
        }
        (DataType::Int32, Value::Integer(value)) => {
            Arc::new(Int32Array::from(vec![i32::try_from(*value).ok()?]))
        }
        (DataType::Int64, Value::Integer(value)) => Arc::new(Int64Array::from(vec![*value])),
        // The SDK holds the bound of a float column as the double of the same value.
        (DataType::Float32, Value::Float(value)) if f64::from(*value as f32) == *value => {
            Arc::new(Float32Array::from(vec![spark_f32(*value as f32)]))
        }
        (DataType::Float64, Value::Float(value)) => {
            Arc::new(Float64Array::from(vec![spark_f64(*value)]))
        }
        (DataType::Utf8, Value::String(value)) => Arc::new(StringArray::from(vec![value.as_str()])),
        (DataType::Utf8View, Value::String(value)) => {
            Arc::new(StringViewArray::from_iter_values([value]))
        }
        (DataType::Binary, Value::Binary(value)) => {
            Arc::new(BinaryArray::from_vec(vec![value.as_slice()]))
        }
        (DataType::BinaryView, Value::Binary(value)) => {
            Arc::new(BinaryViewArray::from_iter_values([value.as_slice()]))
        }
        (DataType::Date32, Value::Date(value)) => Arc::new(Date32Array::from(vec![*value])),
        (DataType::Timestamp(TimeUnit::Microsecond, Some(zone)), Value::Timestamp(value)) => {
            Arc::new(TimestampMicrosecondArray::from(vec![*value]).with_timezone(zone.clone()))
        }
        (DataType::Timestamp(TimeUnit::Microsecond, None), Value::TimestampNtz(value)) => {
            Arc::new(TimestampMicrosecondArray::from(vec![*value]))
        }
        // Equal scales compare as their unscaled values. A file may store the decimals of up to
        // 18 digits as integers, which the reader reads as 64-bit decimals.
        (
            DataType::Decimal128(precision, scale),
            Value::Decimal {
                unscaled,
                scale: value_scale,
                ..
            },
        ) if value_scale == scale => Arc::new(
            Decimal128Array::from(vec![*unscaled])
                .with_precision_and_scale(*precision, *scale)
                .ok()?,
        ),
        (
            DataType::Decimal64(precision, scale),
            Value::Decimal {
                unscaled,
                scale: value_scale,
                ..
            },
        ) if value_scale == scale => Arc::new(
            Decimal64Array::from(vec![i64::try_from(*unscaled).ok()?])
                .with_precision_and_scale(*precision, *scale)
                .ok()?,
        ),
        _ => return None,
    })
}

/// The values of a column, with floats that Arrow compares as Spark does. Arrow orders floats by
/// the IEEE 754 total order, in which -0.0 is less than 0.0, which Spark equates, as the bounds of
/// the ranges do once [`literal`] changes -0.0 to 0.0. The reader handles NaN by itself.
fn spark_ordered(array: &ArrayRef) -> ArrayRef {
    match array.data_type() {
        DataType::Float32 => Arc::new(
            array
                .as_primitive::<Float32Type>()
                .unary::<_, Float32Type>(spark_f32),
        ),
        DataType::Float64 => Arc::new(
            array
                .as_primitive::<Float64Type>()
                .unary::<_, Float64Type>(spark_f64),
        ),
        _ => Arc::clone(array),
    }
}

fn spark_f32(value: f32) -> f32 {
    if value == 0.0 {
        0.0
    } else {
        value
    }
}

fn spark_f64(value: f64) -> f64 {
    if value == 0.0 {
        0.0
    } else {
        value
    }
}

/// Whether each value of a column of string or binary views equals one of `values`, or `None`
/// unless each of these has at most 12 bytes. Such a value is in its view, after its length, and
/// Arrow pads the rest of the view with zeros, so a view of the column is that of the value
/// exactly when its value is, and a longer value has another length: the views compare without
/// the bytes of the column, or a branch on their lengths. The bits of the null rows are arbitrary.
fn equal_views(array: &ArrayRef, values: &[&ArrayRef]) -> Option<BooleanBuffer> {
    let views = |array: &ArrayRef| match array.data_type() {
        DataType::Utf8View => Some(array.as_string_view().views().clone()),
        DataType::BinaryView => Some(array.as_binary_view().views().clone()),
        _ => None,
    };
    let column = views(array)?;
    let values = values
        .iter()
        .map(|value| views(value)?.first().copied())
        .collect::<Option<Vec<u128>>>()?;
    if values.iter().any(|&view| view as u32 > 12) {
        return None;
    }
    Some(match values.as_slice() {
        [value] => pack(&column, |view| view == *value),
        values => pack(&column, |view| values.contains(&view)),
    })
}

/// The bits of `f` for each view, 64 at a time, which the compiler can vectorize.
fn pack(views: &[u128], f: impl Fn(u128) -> bool) -> BooleanBuffer {
    let words: Vec<u64> = views
        .chunks(64)
        .map(|chunk| {
            let bits = chunk.iter().enumerate();
            bits.fold(0, |word, (bit, &view)| word | (u64::from(f(view)) << bit))
        })
        .collect();
    BooleanBuffer::new(Buffer::from_vec(words), 0, views.len())
}

fn column_of<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef, ArrowError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| ArrowError::SchemaError(format!("The filter reads a missing column {name}")))
}

#[cfg(test)]
mod tests {
    use arrow_schema::{Field, Fields};
    use spark_datasource::ValueRange;

    use super::*;

    /// The domain of a column with the ranges, without null or NaN.
    fn domain(column: &str, ranges: Option<Vec<ValueRange>>) -> Domain {
        Domain::new(vec![column.into()], ranges)
    }

    fn range(low: Bound<Value>, high: Bound<Value>) -> ValueRange {
        ValueRange { low, high }
    }

    fn singles(values: impl IntoIterator<Item = Value>) -> Option<Vec<ValueRange>> {
        Some(values.into_iter().map(ValueRange::single).collect())
    }

    /// Whether each row of the batch is in the domain.
    fn evaluate(domain: &Domain, batch: &RecordBatch) -> Vec<bool> {
        let filter = Filter::new(domain, &batch.schema()).unwrap();
        let matches = filter.evaluate(batch).unwrap();
        matches.iter().collect()
    }

    /// Rows with `i` 1, 2, null, 4.
    fn ints() -> RecordBatch {
        let i = Int64Array::from(vec![Some(1), Some(2), None, Some(4)]);
        RecordBatch::try_from_iter([("i", Arc::new(i) as ArrayRef)]).unwrap()
    }

    #[test]
    fn rows_match_when_their_value_is_in_a_range_or_null_is_allowed() {
        let batch = ints();
        let int = Value::Integer;
        let (t, f) = (true, false);
        // i < 2 OR i > 3, then with null.
        let mut outside = domain(
            "i",
            Some(vec![
                range(Bound::Unbounded, Bound::Excluded(int(2))),
                range(Bound::Excluded(int(3)), Bound::Unbounded),
            ]),
        );
        assert_eq!(evaluate(&outside, &batch), [t, f, f, t]);
        outside.null_allowed = true;
        assert_eq!(evaluate(&outside, &batch), [t, f, t, t]);
        let between = domain(
            "i",
            Some(vec![range(
                Bound::Included(int(2)),
                Bound::Included(int(4)),
            )]),
        );
        assert_eq!(evaluate(&between, &batch), [f, t, f, t]);
        assert_eq!(
            evaluate(&domain("i", singles([int(4), int(1)])), &batch),
            [t, f, f, t]
        );
        // IS NOT NULL, and IS NULL, which allows no other value.
        assert_eq!(evaluate(&domain("i", None), &batch), [t, t, f, t]);
        let mut null = domain("i", Some(vec![]));
        null.null_allowed = true;
        assert_eq!(evaluate(&null, &batch), [f, f, t, f]);
    }

    #[test]
    fn strings_and_binary_values_compare_by_their_bytes() {
        let values = [
            Some("apple"),
            Some("banana"),
            None,
            Some("\u{e9}clair"),
            Some("B"),
        ];
        let bytes: Vec<Option<&[u8]>> = values.iter().map(|v| v.map(str::as_bytes)).collect();
        let columns: [ArrayRef; 4] = [
            Arc::new(StringArray::from(values.to_vec())),
            Arc::new(StringViewArray::from(values.to_vec())),
            Arc::new(BinaryArray::from(bytes.clone())),
            Arc::new(BinaryViewArray::from(bytes)),
        ];
        let (t, f) = (true, false);
        for column in columns {
            let binary = matches!(column.data_type(), DataType::Binary | DataType::BinaryView);
            let value = |text: &str| match binary {
                true => Value::Binary(text.as_bytes().to_vec()),
                false => Value::String(text.into()),
            };
            let batch = RecordBatch::try_from_iter([("s", column)]).unwrap();
            // STARTS_WITH 'ban', from the prefix to its successor.
            let prefix = domain(
                "s",
                Some(vec![range(
                    Bound::Included(value("ban")),
                    Bound::Excluded(value("bao")),
                )]),
            );
            assert_eq!(evaluate(&prefix, &batch), [f, t, f, f, f]);
            // Uppercase letters sort before lowercase ones, and the bytes of other letters after.
            let above = domain(
                "s",
                Some(vec![range(Bound::Excluded(value("b")), Bound::Unbounded)]),
            );
            assert_eq!(evaluate(&above, &batch), [f, t, f, t, f]);
        }
    }

    #[test]
    fn views_equal_values_as_strings_and_binary_values_do() {
        // Values in their views and longer ones, of 12 bytes, empty, ending with NUL, and null.
        let values = [
            Some("AIR"),
            Some("REG AIR"),
            None,
            Some(""),
            Some("AIR FREIGHT, LONG"),
            Some("AIRMAIL12345"),
            Some("AIR\0"),
        ];
        let bytes: Vec<Option<&[u8]>> = values.iter().map(|v| v.map(str::as_bytes)).collect();
        let columns: [ArrayRef; 3] = [
            Arc::new(StringArray::from(values.to_vec())),
            Arc::new(StringViewArray::from(values.to_vec())),
            Arc::new(BinaryViewArray::from(bytes)),
        ];
        let rows = |column: &ArrayRef, texts: &[&str]| {
            let binary = column.data_type() == &DataType::BinaryView;
            let value = |text: &&str| match binary {
                true => Value::Binary(text.as_bytes().to_vec()),
                false => Value::String((*text).into()),
            };
            let batch = RecordBatch::try_from_iter([("s", column.clone())]).unwrap();
            evaluate(&domain("s", singles(texts.iter().map(value))), &batch)
        };
        // The last value is not in its view.
        let texts = [
            "AIR",
            "",
            "REG AIR",
            "AIRMAIL12345",
            "AIR\0",
            "AIR FREIGHT, LONG",
        ];
        for text in texts {
            // Each value is in one row.
            let expected = rows(&columns[0], &[text]);
            assert_eq!(expected.iter().filter(|&&row| row).count(), 1, "{text:?}");
            for column in &columns[1..] {
                assert_eq!(rows(column, &[text]), expected, "{text:?}");
            }
        }
        let (t, f) = (true, false);
        for column in &columns {
            assert_eq!(
                rows(column, &texts[..4]),
                [t, t, f, t, f, t, f],
                "{column:?}"
            );
            let long = [texts[0], texts[5]];
            assert_eq!(rows(column, &long), [t, f, f, f, t, f, f], "{column:?}");
        }
    }

    #[test]
    fn values_take_the_type_of_their_column() {
        let schema = Schema::new(vec![
            Field::new("flag", DataType::Boolean, true),
            Field::new("tiny", DataType::Int8, true),
            Field::new("bytes", DataType::Binary, true),
            Field::new("day", DataType::Date32, true),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
                true,
            ),
            Field::new(
                "ntz",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new("amount", DataType::Decimal128(10, 2), true),
            Field::new("cents", DataType::Decimal64(10, 2), true),
            Field::new("ratio", DataType::Float32, true),
        ]);
        let supported = |name: &str, value: Value| {
            Filter::new(&domain(name, singles([value])), &schema).is_some()
        };
        let decimal = |unscaled, scale| Value::Decimal {
            unscaled,
            precision: 10,
            scale,
        };
        assert!(supported("flag", Value::Boolean(true)));
        assert!(supported("tiny", Value::Integer(-128)));
        // A file may have a narrower column than the schema that Spark reads.
        assert!(!supported("tiny", Value::Integer(128)));
        assert!(supported("bytes", Value::Binary(vec![0, 0xff])));
        assert!(!supported("bytes", Value::String("a".into())));
        assert!(supported("day", Value::Date(19000)));
        assert!(supported("ts", Value::Timestamp(-1)));
        assert!(!supported("ts", Value::TimestampNtz(-1)));
        assert!(supported("ntz", Value::TimestampNtz(-1)));
        assert!(!supported("ntz", Value::Timestamp(-1)));
        assert!(supported("amount", decimal(1234, 2)));
        assert!(!supported("amount", decimal(12345, 3)));
        assert!(supported("cents", decimal(i64::MAX.into(), 2)));
        assert!(!supported("cents", decimal(i128::from(i64::MAX) + 1, 2)));
        // A double that no float equals.
        assert!(supported("ratio", Value::Float(0.5)));
        assert!(!supported("ratio", Value::Float(0.1)));
        assert!(!supported("day", Value::String("2022-01-08".into())));
    }

    #[test]
    fn values_compare_as_values_of_their_column() {
        let day = Date32Array::from(vec![Some(18999), Some(19000), None]);
        let ts =
            TimestampMicrosecondArray::from(vec![Some(-1), Some(0), Some(1)]).with_timezone("UTC");
        let amount = Decimal128Array::from(vec![Some(1234), Some(-5), None])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let cents = Decimal64Array::from(vec![Some(1234), Some(-5), None])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let flags = BooleanArray::from(vec![Some(true), Some(false), None]);
        let batch = RecordBatch::try_from_iter([
            ("day", Arc::new(day) as ArrayRef),
            ("ts", Arc::new(ts) as ArrayRef),
            ("amount", Arc::new(amount) as ArrayRef),
            ("cents", Arc::new(cents) as ArrayRef),
            ("flag", Arc::new(flags) as ArrayRef),
        ])
        .unwrap();
        let (t, f) = (true, false);
        let below = |column: &str, value| {
            domain(
                column,
                Some(vec![range(Bound::Unbounded, Bound::Excluded(value))]),
            )
        };
        assert_eq!(
            evaluate(&below("day", Value::Date(19000)), &batch),
            [t, f, f]
        );
        let since_epoch = domain(
            "ts",
            Some(vec![range(
                Bound::Included(Value::Timestamp(0)),
                Bound::Unbounded,
            )]),
        );
        assert_eq!(evaluate(&since_epoch, &batch), [f, t, t]);
        let decimal = Value::Decimal {
            unscaled: -5,
            precision: 10,
            scale: 2,
        };
        for column in ["amount", "cents"] {
            assert_eq!(
                evaluate(&domain(column, singles([decimal.clone()])), &batch),
                [f, t, f]
            );
        }
        // False is before true.
        assert_eq!(
            evaluate(&below("flag", Value::Boolean(true)), &batch),
            [f, t, f]
        );
    }

    #[test]
    fn floats_compare_as_spark_compares_them() {
        // NaN with and without the sign bit, the zeros, and the infinities.
        let negative_nan = f64::from_bits(0xfff8_0000_0000_0001);
        let values = [
            0.0,
            -0.0,
            f64::NAN,
            negative_nan,
            1.0,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ];
        let doubles = Float64Array::from(values.to_vec());
        let floats = Float32Array::from_iter_values(values.iter().map(|&v| v as f32));
        let batch = RecordBatch::try_from_iter([
            ("d", Arc::new(doubles) as ArrayRef),
            ("r", Arc::new(floats) as ArrayRef),
        ])
        .unwrap();
        let (t, f) = (true, false);
        let float = Value::Float;
        for name in ["d", "r"] {
            let rows = |ranges: Vec<ValueRange>, nan_allowed: bool| {
                let mut domain = domain(name, Some(ranges));
                domain.nan_allowed = nan_allowed;
                evaluate(&domain, &batch)
            };
            // Spark equates the zeros, and orders NaN above all numbers, which its domains hold
            // apart from the ranges.
            let zero = |value| vec![ValueRange::single(float(value))];
            assert_eq!(rows(zero(0.0), false), [t, t, f, f, f, f, f]);
            assert_eq!(rows(zero(-0.0), false), [t, t, f, f, f, f, f]);
            let below_zero = vec![range(Bound::Unbounded, Bound::Excluded(float(0.0)))];
            assert_eq!(rows(below_zero, false), [f, f, f, f, f, f, t]);
            let up_to_zero = vec![range(Bound::Unbounded, Bound::Included(float(-0.0)))];
            assert_eq!(rows(up_to_zero, false), [t, t, f, f, f, f, t]);
            // x > 1, which is true for NaN.
            let above_one = vec![range(Bound::Excluded(float(1.0)), Bound::Unbounded)];
            assert_eq!(rows(above_one.clone(), true), [f, f, t, t, f, t, f]);
            assert_eq!(rows(above_one, false), [f, f, f, f, f, t, f]);
            let ones = vec![
                ValueRange::single(float(-0.0)),
                ValueRange::single(float(1.0)),
            ];
            assert_eq!(rows(ones, false), [t, t, f, f, t, f, f]);
        }
    }

    #[test]
    fn domains_of_fields_and_of_other_columns_are_not_filters() {
        let point = Fields::from(vec![Field::new("x", DataType::Int64, true)]);
        let schema = Schema::new(vec![
            Field::new("i", DataType::Int64, true),
            Field::new_struct("point", point, true),
            Field::new(
                "millis",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
        ]);
        let unsupported = |domain: Domain| Filter::new(&domain, &schema).is_none();
        let x = Domain::new(vec!["point".into(), "x".into()], None);
        assert!(unsupported(x));
        assert!(unsupported(domain("missing", None)));
        assert!(unsupported(domain("point", None)));
        assert!(unsupported(domain(
            "millis",
            singles([Value::TimestampNtz(1)])
        )));
        assert!(!unsupported(domain("i", None)));
    }
}
