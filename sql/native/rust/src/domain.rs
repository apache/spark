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

//! The domains of the columns that the filters of a query constrain, which a scan gets in
//! [`ScanBuilder::push_domains`](crate::ScanBuilder::push_domains).

use std::ops::Bound;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Decimal128Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
    Int8Type, TimestampMicrosecondType,
};
use arrow_array::{Array, ArrayRef, BooleanArray, StructArray};
use arrow_schema::{DataType, Fields, TimeUnit};
use serde::{Deserialize, Serialize};

use crate::Result;

/// The domain of a column: the values that the column can have in the rows that match the
/// filters of a query. A value is in the domain when it is:
///
/// - null, if [`null_allowed`](Self::null_allowed);
/// - NaN, if [`nan_allowed`](Self::nan_allowed), for a `float` or `double` column;
/// - another value, if [`ranges`](Self::ranges) is `None`, or if it is in one of the ranges.
///
/// A scan can skip the data that has no value in the domain of a column, such as a file or a
/// row group whose statistics show that its values are all below the ranges, or a row whose
/// value is outside them. It must never skip a row whose values are all in their domains, since
/// Spark cannot detect it. Spark evaluates the filters again after reading, so the rows that a
/// scan returns need not match them.
///
/// Values compare in the order of Spark SQL:
///
/// - numbers, dates and timestamps by their values. The bounds of the ranges of a `float` or
///   `double` column are never NaN, and -0.0 is equal to 0.0: compare them with `<` and `==`,
///   not with a total order, such as [`f64::total_cmp`], that puts -0.0 before 0.0. Data that
///   has NaN values, or may have, can only be skipped if `nan_allowed` is false;
/// - strings by their UTF-8 bytes, as Rust compares `str`;
/// - binary values as unsigned bytes, in lexicographic order, as Rust compares `[u8]`;
/// - booleans with false before true.
///
/// A domain is serializable, so that the factory of a scan can carry it to the executors.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Domain {
    /// The column, by its name in [`ScanInfo::schema`](crate::ScanInfo::schema), and then by the
    /// name of its field in each struct: `["point", "x"]` is the field `x` of the column
    /// `point`.
    pub path: Vec<String>,
    /// Whether null is in the domain. A field of a struct is null in the rows where the struct,
    /// or a struct that contains it, is null, whatever the child array of the field holds there:
    /// Spark never allows null for a field of a struct, since statistics computed from that
    /// child array may not count these rows as nulls.
    pub null_allowed: bool,
    /// Whether NaN is in the domain. Always false for the columns of other types than `float`
    /// and `double`.
    pub nan_allowed: bool,
    /// The ranges of the other values in the domain, sorted, and without overlaps, or `None` if
    /// they are all in it. Without ranges, no other value than null or NaN is in the domain.
    pub ranges: Option<Vec<ValueRange>>,
}

impl Domain {
    /// A domain without null or NaN, created outside Spark, for example in a test. Set
    /// [`null_allowed`](Self::null_allowed) and [`nan_allowed`](Self::nan_allowed) for others.
    pub fn new(path: Vec<String>, ranges: Option<Vec<ValueRange>>) -> Self {
        Self {
            path,
            null_allowed: false,
            nan_allowed: false,
            ranges,
        }
    }

    /// The domain that Spark passes for the column at `path`, by the indices of the column in
    /// `fields`, the schema that `create_reader` got, and then of its field in each struct, or
    /// `None` if the SDK has no [`Value`] for the type of the column, which a later version of
    /// Spark may pass.
    pub(crate) fn from_abi(
        fields: &Fields,
        path: &[usize],
        null_allowed: bool,
        nan_allowed: bool,
        ranges: Option<&StructArray>,
    ) -> Result<Option<Self>> {
        let invalid = || format!("the path {path:?} of a domain is not a column of the scan");
        let mut names = Vec::with_capacity(path.len());
        let mut fields = fields;
        for (depth, &index) in path.iter().enumerate() {
            let field = fields.get(index).ok_or_else(invalid)?;
            names.push(field.name().clone());
            if depth + 1 < path.len() {
                let DataType::Struct(children) = field.data_type() else {
                    return Err(invalid().into());
                };
                fields = children;
            }
        }
        if names.is_empty() {
            return Err(invalid().into());
        }
        let ranges = match ranges.map(value_ranges).transpose()? {
            Some(None) => return Ok(None),
            ranges => ranges.flatten(),
        };
        Ok(Some(Self {
            path: names,
            null_allowed,
            nan_allowed,
            ranges,
        }))
    }
}

/// A range of the values of a [`Domain`]. A single value is a range whose bounds are equal and
/// included.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ValueRange {
    /// The lower bound, or [`Bound::Unbounded`] if the range has none.
    pub low: Bound<Value>,
    /// The upper bound, or [`Bound::Unbounded`] if the range has none.
    pub high: Bound<Value>,
}

impl ValueRange {
    /// The range of a single value.
    pub fn single(value: Value) -> Self {
        Self {
            low: Bound::Included(value.clone()),
            high: Bound::Included(value),
        }
    }
}

/// A value of a column, in the bounds of the ranges of a [`Domain`]. It has the type of its
/// column in [`ScanInfo::schema`](crate::ScanInfo::schema). New variants may be added for the
/// types that a later version of Spark passes domains for: a connector ignores the domains of
/// values that it does not handle.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Value {
    Boolean(bool),
    /// A `tinyint`, `smallint`, `int`, or `bigint`.
    Integer(i64),
    /// A `float` or `double`, which is never NaN: a `float` is the exact value of the float.
    Float(f64),
    /// A `decimal(precision, scale)` of the precision and scale of its column, whose value is
    /// `unscaled * 10^-scale`.
    Decimal {
        unscaled: i128,
        precision: u8,
        scale: i8,
    },
    String(String),
    Binary(Vec<u8>),
    /// A `date`, as days since 1970-01-01.
    Date(i32),
    /// A `timestamp`, as microseconds since the epoch.
    Timestamp(i64),
    /// A `timestamp_ntz`, as microseconds since 1970-01-01 00:00:00, without a time zone.
    TimestampNtz(i64),
}

/// The ranges of a domain, from the struct array that Spark passes, with the fields `low`,
/// `high`, `low_inclusive` and `high_inclusive`, or `None` if its values have a type that the SDK
/// has no [`Value`] for.
fn value_ranges(ranges: &StructArray) -> Result<Option<Vec<ValueRange>>> {
    let (Some(lows), Some(highs)) = (
        values(field(ranges, "low")?),
        values(field(ranges, "high")?),
    ) else {
        return Ok(None);
    };
    let low_inclusive = flags(ranges, "low_inclusive")?;
    let high_inclusive = flags(ranges, "high_inclusive")?;
    let bound = |value: Option<Value>, inclusive: bool| match value {
        None => Bound::Unbounded,
        Some(value) if inclusive => Bound::Included(value),
        Some(value) => Bound::Excluded(value),
    };
    let ranges = lows
        .into_iter()
        .zip(highs)
        .enumerate()
        .map(|(row, (low, high))| ValueRange {
            low: bound(low, low_inclusive.value(row)),
            high: bound(high, high_inclusive.value(row)),
        })
        .collect();
    Ok(Some(ranges))
}

fn field<'a>(ranges: &'a StructArray, name: &str) -> Result<&'a ArrayRef> {
    ranges
        .column_by_name(name)
        .ok_or_else(|| format!("the ranges of a domain have no field {name}").into())
}

fn flags<'a>(ranges: &'a StructArray, name: &str) -> Result<&'a BooleanArray> {
    field(ranges, name)?
        .as_boolean_opt()
        .ok_or_else(|| format!("the field {name} of the ranges of a domain is not boolean").into())
}

/// The values of an array of one of the types of version 1 of the ABI, with `None` for its
/// nulls, or `None` for another type.
fn values(array: &ArrayRef) -> Option<Vec<Option<Value>>> {
    fn convert<T>(
        values: impl IntoIterator<Item = Option<T>>,
        value: impl Fn(T) -> Value,
    ) -> Option<Vec<Option<Value>>> {
        Some(values.into_iter().map(|v| v.map(&value)).collect())
    }
    match array.data_type() {
        DataType::Boolean => convert(array.as_boolean(), Value::Boolean),
        DataType::Int8 => convert(array.as_primitive::<Int8Type>(), |v| {
            Value::Integer(v.into())
        }),
        DataType::Int16 => convert(array.as_primitive::<Int16Type>(), |v| {
            Value::Integer(v.into())
        }),
        DataType::Int32 => convert(array.as_primitive::<Int32Type>(), |v| {
            Value::Integer(v.into())
        }),
        DataType::Int64 => convert(array.as_primitive::<Int64Type>(), Value::Integer),
        DataType::Float32 => convert(array.as_primitive::<Float32Type>(), |v| {
            Value::Float(v.into())
        }),
        DataType::Float64 => convert(array.as_primitive::<Float64Type>(), Value::Float),
        &DataType::Decimal128(precision, scale) => {
            convert(array.as_primitive::<Decimal128Type>(), |unscaled| {
                Value::Decimal {
                    unscaled,
                    precision,
                    scale,
                }
            })
        }
        DataType::Utf8 => convert(array.as_string::<i32>(), |v| Value::String(v.to_owned())),
        DataType::Binary => convert(array.as_binary::<i32>(), |v| Value::Binary(v.to_vec())),
        DataType::Date32 => convert(array.as_primitive::<Date32Type>(), Value::Date),
        DataType::Timestamp(TimeUnit::Microsecond, zone) => {
            let values = array.as_primitive::<TimestampMicrosecondType>();
            match zone {
                Some(_) => convert(values, Value::Timestamp),
                None => convert(values, Value::TimestampNtz),
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{
        ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
        Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, StringArray,
        Time32SecondArray, TimestampMicrosecondArray,
    };
    use arrow_schema::{Field, Schema};

    use super::*;

    /// The ranges that Spark passes, of the given bounds, with each bound included.
    fn ranges(low: ArrayRef, high: ArrayRef, inclusive: (bool, bool)) -> StructArray {
        let rows = low.len();
        let flag = |value| Arc::new(BooleanArray::from(vec![value; rows])) as ArrayRef;
        StructArray::try_from(vec![
            ("low", low),
            ("high", high),
            ("low_inclusive", flag(inclusive.0)),
            ("high_inclusive", flag(inclusive.1)),
        ])
        .unwrap()
    }

    fn convert(ranges: &StructArray) -> Option<Vec<ValueRange>> {
        value_ranges(ranges).unwrap()
    }

    #[test]
    fn converts_the_values_of_each_type_of_version_1() {
        let utc = TimestampMicrosecondArray::from(vec![-1]).with_timezone("UTC");
        let decimals = Decimal128Array::from(vec![-12345])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let cases: Vec<(ArrayRef, Value)> = vec![
            (
                Arc::new(BooleanArray::from(vec![true])),
                Value::Boolean(true),
            ),
            (Arc::new(Int8Array::from(vec![-128])), Value::Integer(-128)),
            (Arc::new(Int16Array::from(vec![300])), Value::Integer(300)),
            (
                Arc::new(Int32Array::from(vec![i32::MIN])),
                Value::Integer(i32::MIN.into()),
            ),
            (
                Arc::new(Int64Array::from(vec![i64::MAX])),
                Value::Integer(i64::MAX),
            ),
            (
                Arc::new(Float32Array::from(vec![0.1])),
                Value::Float(0.1_f32.into()),
            ),
            (Arc::new(Float64Array::from(vec![-0.0])), Value::Float(-0.0)),
            (
                Arc::new(decimals),
                Value::Decimal {
                    unscaled: -12345,
                    precision: 10,
                    scale: 2,
                },
            ),
            (
                Arc::new(StringArray::from(vec!["row-1"])),
                Value::String("row-1".into()),
            ),
            (
                Arc::new(BinaryArray::from_vec(vec![b"\xff\x00"])),
                Value::Binary(vec![0xff, 0]),
            ),
            (Arc::new(Date32Array::from(vec![19000])), Value::Date(19000)),
            (Arc::new(utc), Value::Timestamp(-1)),
            (
                Arc::new(TimestampMicrosecondArray::from(vec![7])),
                Value::TimestampNtz(7),
            ),
        ];
        for (array, value) in cases {
            let ranges = ranges(array.clone(), array, (true, true));
            assert_eq!(convert(&ranges), Some(vec![ValueRange::single(value)]));
        }
    }

    #[test]
    fn null_bounds_are_unbounded_and_flags_include_bounds() {
        let low: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(5), Some(10)]));
        let high: ArrayRef = Arc::new(Int64Array::from(vec![Some(0), Some(7), None]));
        let flags = |values: Vec<bool>| Arc::new(BooleanArray::from(values)) as ArrayRef;
        let ranges = StructArray::try_from(vec![
            ("low", low),
            ("high", high),
            ("low_inclusive", flags(vec![false, true, false])),
            ("high_inclusive", flags(vec![false, false, false])),
        ])
        .unwrap();
        assert_eq!(
            convert(&ranges),
            Some(vec![
                ValueRange {
                    low: Bound::Unbounded,
                    high: Bound::Excluded(Value::Integer(0)),
                },
                ValueRange {
                    low: Bound::Included(Value::Integer(5)),
                    high: Bound::Excluded(Value::Integer(7)),
                },
                ValueRange {
                    low: Bound::Excluded(Value::Integer(10)),
                    high: Bound::Unbounded,
                },
            ])
        );
        // No range: only null or NaN may be in the domain.
        let empty = ranges.slice(0, 0);
        assert_eq!(convert(&empty), Some(vec![]));
    }

    #[test]
    fn resolves_paths_to_names_and_skips_types_without_values() {
        let point = Fields::from(vec![
            Field::new("x", DataType::Int32, true),
            Field::new("at", DataType::Time32(arrow_schema::TimeUnit::Second), true),
        ]);
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("point", DataType::Struct(point), true),
        ]);
        let fields = schema.fields();
        let ints: ArrayRef = Arc::new(Int32Array::from(vec![3]));
        let x = ranges(ints.clone(), ints, (true, true));
        let domain = Domain::from_abi(fields, &[1, 0], false, false, Some(&x))
            .unwrap()
            .unwrap();
        assert_eq!(domain.path, ["point", "x"]);
        assert_eq!(
            domain.ranges,
            Some(vec![ValueRange::single(Value::Integer(3))])
        );
        let all = Domain::from_abi(fields, &[0], true, false, None)
            .unwrap()
            .unwrap();
        assert_eq!(
            (all.path, all.null_allowed, all.ranges),
            (vec!["id".to_owned()], true, None)
        );
        // A type that the SDK has no value for, which a later version of Spark may pass.
        let times: ArrayRef = Arc::new(Time32SecondArray::from(vec![1]));
        let at = ranges(times.clone(), times, (true, true));
        assert_eq!(
            Domain::from_abi(fields, &[1, 1], false, false, Some(&at)).unwrap(),
            None
        );
        for path in [&[][..], &[2], &[0, 0], &[1, 2]] {
            let error = Domain::from_abi(fields, path, false, false, None).unwrap_err();
            assert!(
                error.to_string().contains("is not a column of the scan"),
                "{path:?}: {error}"
            );
        }
    }

    #[test]
    fn domains_round_trip_in_the_state_of_a_reader() {
        // A reader factory carries its domains to the executors, which the SDK encodes.
        let decimal = |unscaled| Value::Decimal {
            unscaled,
            precision: 38,
            scale: 2,
        };
        let mut domain = Domain::new(
            vec!["point".into(), "x".into()],
            Some(vec![
                ValueRange {
                    low: Bound::Unbounded,
                    high: Bound::Excluded(decimal(-(10_i128.pow(37)))),
                },
                ValueRange::single(decimal(i128::from(i64::MAX) + 1)),
            ]),
        );
        domain.null_allowed = true;
        let values = Domain::new(
            vec!["values".into()],
            Some(
                [
                    Value::Boolean(false),
                    Value::Integer(i64::MIN),
                    Value::Float(-0.0),
                    Value::String("\u{1F600}".into()),
                    Value::Binary(vec![0xff, 0]),
                    Value::Date(-1),
                    Value::Timestamp(-1),
                    Value::TimestampNtz(i64::MAX),
                ]
                .into_iter()
                .map(ValueRange::single)
                .collect(),
            ),
        );
        let domains = vec![domain, values, Domain::new(vec!["all".into()], None)];
        let encoded = crate::bridge::encode(&domains).unwrap();
        assert_eq!(
            crate::bridge::decode::<Vec<Domain>>(&encoded).unwrap(),
            domains
        );
    }
}
