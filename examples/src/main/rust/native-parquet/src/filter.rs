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

//! The pushed predicates that the reader evaluates on the batches that it decodes.

use std::sync::Arc;

use arrow_arith::boolean::{and_kleene, is_not_null, is_null, not, or_kleene};
use arrow_array::cast::AsArray;
use arrow_array::types::{Float32Type, Float64Type};
use arrow_array::{
    ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Decimal128Array,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch, Scalar,
    StringArray, StringViewArray, TimestampMicrosecondArray,
};
use arrow_ord::cmp;
use arrow_schema::{ArrowError, DataType, Schema, TimeUnit};
use arrow_string::like;
use spark_native_source::{LiteralValue, Predicate};

/// A predicate on the top-level columns of a file, which evaluates to what Spark SQL computes,
/// including nulls, so that the reader drops exactly the rows that Spark drops.
#[derive(Debug)]
pub(crate) enum Filter {
    /// A column compared with a literal of its type, or with null.
    Compare {
        column: String,
        op: Comparison,
        value: Option<ArrayRef>,
    },
    IsNull {
        column: String,
        negated: bool,
    },
    /// A column in a list of literals, which may contain null.
    In {
        column: String,
        values: Vec<Option<ArrayRef>>,
    },
    /// A string column matched with a string literal.
    Text {
        column: String,
        op: TextMatch,
        pattern: ArrayRef,
    },
    And(Box<Filter>, Box<Filter>),
    Or(Box<Filter>, Box<Filter>),
    Not(Box<Filter>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Comparison {
    Equal,
    NotEqual,
    NullSafeEqual,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TextMatch {
    StartsWith,
    EndsWith,
    Contains,
}

impl Filter {
    /// The filter of a predicate on the columns of `schema`, or `None` if the reader does not
    /// evaluate the predicate exactly as Spark does.
    pub(crate) fn new(predicate: &Predicate, schema: &Schema) -> Option<Self> {
        let Predicate::Function { name, children } = predicate else {
            return None;
        };
        let filter = |child| Self::new(child, schema).map(Box::new);
        Some(match (name.as_str(), children.as_slice()) {
            ("AND", [left, right]) => Self::And(filter(left)?, filter(right)?),
            ("OR", [left, right]) => Self::Or(filter(left)?, filter(right)?),
            ("NOT", [child]) => Self::Not(filter(child)?),
            ("IS_NULL" | "IS_NOT_NULL", [child]) => Self::IsNull {
                column: column(child, schema)?.0,
                negated: name == "IS_NOT_NULL",
            },
            ("IN", [child, values @ ..]) if !values.is_empty() => {
                let (column, data_type) = comparable_column(child, schema)?;
                let values = values
                    .iter()
                    .map(|value| literal(value, data_type))
                    .collect::<Option<_>>()?;
                Self::In { column, values }
            }
            ("STARTS_WITH" | "ENDS_WITH" | "CONTAINS", [child, pattern]) => {
                let (column, data_type) = column(child, schema)?;
                if !matches!(data_type, DataType::Utf8 | DataType::Utf8View) {
                    return None;
                }
                let op = match name.as_str() {
                    "STARTS_WITH" => TextMatch::StartsWith,
                    "ENDS_WITH" => TextMatch::EndsWith,
                    _ => TextMatch::Contains,
                };
                Self::Text {
                    column,
                    op,
                    pattern: literal(pattern, data_type)??,
                }
            }
            (name, [left, right]) => {
                let op = comparison(name)?;
                // Spark may put the literal on either side.
                let (column, data_type, value, op) = match comparable_column(left, schema) {
                    Some((column, data_type)) => (column, data_type, right, op),
                    None => {
                        let (column, data_type) = comparable_column(right, schema)?;
                        (column, data_type, left, op.reversed())
                    }
                };
                Self::Compare {
                    column,
                    op,
                    value: literal(value, data_type)?,
                }
            }
            _ => return None,
        })
    }

    /// Adds the names of the columns that the filter reads to `columns`.
    pub(crate) fn columns<'a>(&'a self, columns: &mut Vec<&'a str>) {
        match self {
            Self::Compare { column, .. }
            | Self::IsNull { column, .. }
            | Self::In { column, .. }
            | Self::Text { column, .. } => {
                if !columns.contains(&column.as_str()) {
                    columns.push(column);
                }
            }
            Self::And(left, right) | Self::Or(left, right) => {
                left.columns(columns);
                right.columns(columns);
            }
            Self::Not(child) => child.columns(columns),
        }
    }

    /// Evaluates the filter on a batch that has the columns that it reads. A row is null when
    /// Spark computes null, which the reader drops, as Spark drops it, unless a NOT changes it.
    pub(crate) fn evaluate(&self, batch: &RecordBatch) -> Result<BooleanArray, ArrowError> {
        match self {
            Self::Compare { column, op, value } => {
                let array = &spark_ordered(column_of(batch, column)?);
                let Some(value) = value else {
                    return Ok(match op {
                        Comparison::NullSafeEqual => is_null(array)?,
                        _ => BooleanArray::new_null(array.len()),
                    });
                };
                let value = Scalar::new(value);
                match op {
                    Comparison::Equal => cmp::eq(array, &value),
                    Comparison::NotEqual => cmp::neq(array, &value),
                    Comparison::NullSafeEqual => cmp::not_distinct(array, &value),
                    Comparison::Less => cmp::lt(array, &value),
                    Comparison::LessOrEqual => cmp::lt_eq(array, &value),
                    Comparison::Greater => cmp::gt(array, &value),
                    Comparison::GreaterOrEqual => cmp::gt_eq(array, &value),
                }
            }
            Self::IsNull { column, negated } => {
                let array = column_of(batch, column)?;
                if *negated {
                    is_not_null(array)
                } else {
                    is_null(array)
                }
            }
            Self::In { column, values } => {
                // The disjunction of the equalities: null when no value matches, and the column
                // or a value is null.
                let array = &spark_ordered(column_of(batch, column)?);
                let mut result: Option<BooleanArray> = None;
                for value in values {
                    let matches = match value {
                        Some(value) => cmp::eq(array, &Scalar::new(value))?,
                        None => BooleanArray::new_null(array.len()),
                    };
                    result = Some(match result {
                        Some(result) => or_kleene(&result, &matches)?,
                        None => matches,
                    });
                }
                Ok(result.expect("an IN filter has values"))
            }
            Self::Text {
                column,
                op,
                pattern,
            } => {
                let array = column_of(batch, column)?;
                let pattern = Scalar::new(pattern);
                match op {
                    TextMatch::StartsWith => like::starts_with(array, &pattern),
                    TextMatch::EndsWith => like::ends_with(array, &pattern),
                    TextMatch::Contains => like::contains(array, &pattern),
                }
            }
            Self::And(left, right) => and_kleene(&left.evaluate(batch)?, &right.evaluate(batch)?),
            Self::Or(left, right) => or_kleene(&left.evaluate(batch)?, &right.evaluate(batch)?),
            Self::Not(child) => not(&child.evaluate(batch)?),
        }
    }
}

impl Comparison {
    /// The comparison with its operands swapped: `5 < x` is `x > 5`.
    fn reversed(self) -> Self {
        match self {
            Self::Less => Self::Greater,
            Self::LessOrEqual => Self::GreaterOrEqual,
            Self::Greater => Self::Less,
            Self::GreaterOrEqual => Self::LessOrEqual,
            symmetric => symmetric,
        }
    }
}

fn comparison(name: &str) -> Option<Comparison> {
    Some(match name {
        "=" => Comparison::Equal,
        "<>" => Comparison::NotEqual,
        "<=>" => Comparison::NullSafeEqual,
        "<" => Comparison::Less,
        "<=" => Comparison::LessOrEqual,
        ">" => Comparison::Greater,
        ">=" => Comparison::GreaterOrEqual,
        _ => return None,
    })
}

/// A top-level column of the schema.
fn column<'a>(predicate: &Predicate, schema: &'a Schema) -> Option<(String, &'a DataType)> {
    let Predicate::Column { name } = predicate else {
        return None;
    };
    let [name] = name.as_slice() else {
        return None;
    };
    let field = schema.field_with_name(name).ok()?;
    Some((name.clone(), field.data_type()))
}

/// A top-level column of a type that Arrow compares as Spark does, in the schema that Spark
/// reads or in the one that the reader reads, which has views of strings and binary values.
/// Floats compare as Spark compares them once [`spark_ordered`] changes their zeros and NaNs.
fn comparable_column<'a>(
    predicate: &Predicate,
    schema: &'a Schema,
) -> Option<(String, &'a DataType)> {
    let (name, data_type) = column(predicate, schema)?;
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
        | DataType::Decimal128(_, _)
        | DataType::Timestamp(TimeUnit::Microsecond, _) => Some((name, data_type)),
        _ => None,
    }
}

/// A literal as a one-element array of `data_type`, `Some(None)` for null, or `None` if the
/// literal is not a value of that type. Spark compares strings and binary by their bytes, as
/// Arrow does.
fn literal(predicate: &Predicate, data_type: &DataType) -> Option<Option<ArrayRef>> {
    let array: ArrayRef = match (data_type, predicate.literal_value()?) {
        (_, LiteralValue::Null) => return Some(None),
        (DataType::Boolean, LiteralValue::Boolean(value)) => {
            Arc::new(BooleanArray::from(vec![value]))
        }
        (DataType::Int8, LiteralValue::Integer(value)) => {
            Arc::new(Int8Array::from(vec![i8::try_from(value).ok()?]))
        }
        (DataType::Int16, LiteralValue::Integer(value)) => {
            Arc::new(Int16Array::from(vec![i16::try_from(value).ok()?]))
        }
        (DataType::Int32, LiteralValue::Integer(value)) => {
            Arc::new(Int32Array::from(vec![i32::try_from(value).ok()?]))
        }
        (DataType::Int64, LiteralValue::Integer(value)) => Arc::new(Int64Array::from(vec![value])),
        // The SDK reads a float literal as the double of the same value.
        (DataType::Float32, LiteralValue::Float(value)) if f64::from(value as f32) == value => {
            Arc::new(Float32Array::from(vec![spark_f32(value as f32)]))
        }
        (DataType::Float64, LiteralValue::Float(value)) => {
            Arc::new(Float64Array::from(vec![spark_f64(value)]))
        }
        (DataType::Utf8, LiteralValue::String(value)) => Arc::new(StringArray::from(vec![value])),
        (DataType::Utf8View, LiteralValue::String(value)) => {
            Arc::new(StringViewArray::from_iter_values([value]))
        }
        (DataType::Binary, LiteralValue::Binary(value)) => {
            Arc::new(BinaryArray::from_vec(vec![value.as_slice()]))
        }
        (DataType::BinaryView, LiteralValue::Binary(value)) => {
            Arc::new(BinaryViewArray::from_iter_values([value.as_slice()]))
        }
        (DataType::Date32, LiteralValue::Date(value)) => Arc::new(Date32Array::from(vec![value])),
        (
            DataType::Timestamp(TimeUnit::Microsecond, Some(zone)),
            LiteralValue::Timestamp(value),
        ) => Arc::new(TimestampMicrosecondArray::from(vec![value]).with_timezone(zone.clone())),
        (DataType::Timestamp(TimeUnit::Microsecond, None), LiteralValue::TimestampNtz(value)) => {
            Arc::new(TimestampMicrosecondArray::from(vec![value]))
        }
        // Equal scales compare as their unscaled values.
        (
            DataType::Decimal128(precision, scale),
            LiteralValue::Decimal {
                unscaled,
                scale: literal_scale,
                ..
            },
        ) if literal_scale == *scale => Arc::new(
            Decimal128Array::from(vec![unscaled])
                .with_precision_and_scale(*precision, *scale)
                .ok()?,
        ),
        _ => return None,
    };
    Some(Some(array))
}

/// The values of a column, with floats that Arrow compares as Spark does. Arrow orders floats by
/// the IEEE 754 total order, in which -0.0 is less than 0.0, and NaNs differ by their bits, those
/// with the sign bit being less than all numbers. Spark equates the zeros and the NaNs, and orders
/// NaN above all numbers, as the total order does once -0.0 is 0.0 and every NaN is the same
/// positive NaN, as in the literals.
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
    if value.is_nan() {
        // The quiet NaN without the sign bit, whose bits f32::NAN does not guarantee.
        f32::from_bits(0x7fc0_0000)
    } else if value == 0.0 {
        0.0
    } else {
        value
    }
}

fn spark_f64(value: f64) -> f64 {
    if value.is_nan() {
        f64::from_bits(0x7ff8_0000_0000_0000)
    } else if value == 0.0 {
        0.0
    } else {
        value
    }
}

fn column_of<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef, ArrowError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| ArrowError::SchemaError(format!("The filter reads a missing column {name}")))
}

#[cfg(test)]
mod tests {
    use arrow_array::Array;
    use arrow_schema::{Field, Fields};
    use serde_json::{json, Value};

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

    fn predicate(value: Value) -> Predicate {
        serde_json::from_value(value).unwrap()
    }

    /// Rows with `i` 1, 2, null, 4 and `s` "apple", "banana", null, "cherry".
    fn batch() -> RecordBatch {
        RecordBatch::try_from_iter([
            (
                "i",
                Arc::new(Int64Array::from(vec![Some(1), Some(2), None, Some(4)])) as ArrayRef,
            ),
            (
                "s",
                Arc::new(StringArray::from(vec![
                    Some("apple"),
                    Some("banana"),
                    None,
                    Some("cherry"),
                ])) as ArrayRef,
            ),
            (
                "f",
                Arc::new(Float64Array::from(vec![0.0, -0.0, f64::NAN, 1.0])) as ArrayRef,
            ),
        ])
        .unwrap()
    }

    /// What Spark computes for each row: true, false, or null.
    fn evaluate(predicate: Value) -> Vec<Option<bool>> {
        let batch = batch();
        let filter = Filter::new(&self::predicate(predicate), &batch.schema()).unwrap();
        let result = filter.evaluate(&batch).unwrap();
        (0..result.len())
            .map(|row| result.is_valid(row).then(|| result.value(row)))
            .collect()
    }

    fn bigint(value: i64) -> Value {
        literal("bigint", json!(value))
    }

    #[test]
    fn comparisons_are_null_for_null_operands() {
        let i = || column("i");
        let (t, f, n) = (Some(true), Some(false), None);
        assert_eq!(evaluate(function("=", vec![i(), bigint(2)])), [f, t, n, f]);
        assert_eq!(evaluate(function("<>", vec![i(), bigint(2)])), [t, f, n, t]);
        assert_eq!(evaluate(function("<", vec![i(), bigint(2)])), [t, f, n, f]);
        assert_eq!(evaluate(function("<=", vec![i(), bigint(2)])), [t, t, n, f]);
        assert_eq!(evaluate(function(">", vec![i(), bigint(2)])), [f, f, n, t]);
        assert_eq!(evaluate(function(">=", vec![i(), bigint(2)])), [f, t, n, t]);
        // Null-safe equality is never null.
        assert_eq!(
            evaluate(function("<=>", vec![i(), bigint(2)])),
            [f, t, f, f]
        );
        let null = || literal("bigint", Value::Null);
        assert_eq!(evaluate(function("<=>", vec![i(), null()])), [f, f, t, f]);
        assert_eq!(evaluate(function("=", vec![i(), null()])), [n, n, n, n]);
        assert_eq!(evaluate(function("IS_NULL", vec![i()])), [f, f, t, f]);
        assert_eq!(evaluate(function("IS_NOT_NULL", vec![i()])), [t, t, f, t]);
    }

    #[test]
    fn a_literal_on_the_left_reverses_the_comparison() {
        let (t, f, n) = (Some(true), Some(false), None);
        // 2 < i is i > 2.
        assert_eq!(
            evaluate(function("<", vec![bigint(2), column("i")])),
            [f, f, n, t]
        );
        assert_eq!(
            evaluate(function(">=", vec![bigint(2), column("i")])),
            [t, t, n, f]
        );
        assert_eq!(
            evaluate(function("=", vec![bigint(4), column("i")])),
            [f, f, n, t]
        );
    }

    #[test]
    fn in_lists_and_logical_operators_use_three_valued_logic() {
        let (t, f, n) = (Some(true), Some(false), None);
        let in_list = |values: Vec<Value>| {
            let mut children = vec![column("i")];
            children.extend(values);
            function("IN", children)
        };
        assert_eq!(evaluate(in_list(vec![bigint(1), bigint(4)])), [t, f, n, t]);
        // A null value makes the rows that match no value null, which NOT keeps null.
        let with_null = || in_list(vec![bigint(1), literal("bigint", Value::Null)]);
        assert_eq!(evaluate(with_null()), [t, n, n, n]);
        assert_eq!(evaluate(function("NOT", vec![with_null()])), [f, n, n, n]);

        let less = || function("<", vec![column("i"), bigint(3)]);
        let named = || function("=", vec![column("s"), literal("string", json!("cherry"))]);
        // A null operand of AND and OR decides nothing when the other operand does.
        assert_eq!(
            evaluate(function("AND", vec![less(), named()])),
            [f, f, n, f]
        );
        assert_eq!(
            evaluate(function("OR", vec![less(), named()])),
            [t, t, n, t]
        );
        assert_eq!(evaluate(function("NOT", vec![less()])), [f, f, n, t]);
    }

    #[test]
    fn strings_match_by_their_bytes() {
        let (t, f, n) = (Some(true), Some(false), None);
        let text = |name: &str, pattern: &str| {
            function(name, vec![column("s"), literal("string", json!(pattern))])
        };
        assert_eq!(evaluate(text("STARTS_WITH", "ban")), [f, t, n, f]);
        assert_eq!(evaluate(text("ENDS_WITH", "rry")), [f, f, n, t]);
        assert_eq!(evaluate(text("CONTAINS", "an")), [f, t, n, f]);
        assert_eq!(evaluate(text(">", "b")), [f, t, n, t]);
        // Uppercase letters sort before lowercase ones.
        assert_eq!(evaluate(text("<", "B")), [f, f, n, f]);
    }

    #[test]
    fn literals_take_the_type_of_their_column() {
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
        ]);
        let supported = |name: &str, data_type: &str, value: Value| {
            let compare = function("=", vec![column(name), literal(data_type, value)]);
            Filter::new(&predicate(compare), &schema).is_some()
        };
        assert!(supported("flag", "boolean", json!(true)));
        assert!(supported("tiny", "tinyint", json!(-128)));
        // Spark unwraps casts into literals of the column type, but a value in range compares
        // the same in any integer type.
        assert!(supported("tiny", "bigint", json!(127)));
        assert!(!supported("tiny", "bigint", json!(128)));
        assert!(supported("bytes", "binary", json!("AP8=")));
        assert!(supported("day", "date", json!(19000)));
        assert!(supported("ts", "timestamp", json!(-1)));
        assert!(!supported("ts", "timestamp_ntz", json!(-1)));
        assert!(supported("ntz", "timestamp_ntz", json!(-1)));
        assert!(!supported("ntz", "timestamp", json!(-1)));
        assert!(supported("amount", "decimal(10,2)", json!("12.34")));
        assert!(supported("amount", "decimal(12,2)", json!("12.34")));
        assert!(!supported("amount", "decimal(10,3)", json!("12.345")));
        assert!(!supported("day", "string", json!("2022-01-08")));
    }

    #[test]
    fn literals_compare_as_values_of_their_column() {
        let day = Arc::new(Date32Array::from(vec![Some(18999), Some(19000), None])) as ArrayRef;
        let ts = Arc::new(
            TimestampMicrosecondArray::from(vec![Some(-1), Some(0), Some(1)]).with_timezone("UTC"),
        ) as ArrayRef;
        let amount = Arc::new(
            Decimal128Array::from(vec![Some(1234), Some(-5), None])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        ) as ArrayRef;
        let bytes = Arc::new(BinaryArray::from_vec(vec![b"\x00", b"\xff", b"a"])) as ArrayRef;
        let batch = RecordBatch::try_from_iter([
            ("day", day),
            ("ts", ts),
            ("amount", amount),
            ("bytes", bytes),
        ])
        .unwrap();
        let rows = |value: Value| {
            let filter = Filter::new(&predicate(value), &batch.schema()).unwrap();
            let result = filter.evaluate(&batch).unwrap();
            (0..result.len())
                .map(|row| result.is_valid(row).then(|| result.value(row)))
                .collect::<Vec<_>>()
        };
        let (t, f, n) = (Some(true), Some(false), None);
        let date = literal("date", json!(19000));
        assert_eq!(rows(function("<", vec![column("day"), date])), [t, f, n]);
        let epoch = literal("timestamp", json!(0));
        assert_eq!(rows(function(">=", vec![column("ts"), epoch])), [f, t, t]);
        let cents = literal("decimal(10,2)", json!("-0.05"));
        assert_eq!(
            rows(function("=", vec![column("amount"), cents])),
            [f, t, n]
        );
        // Spark compares the bytes of binary values as unsigned.
        let byte = literal("binary", json!("gA=="));
        assert_eq!(rows(function(">", vec![column("bytes"), byte])), [f, t, f]);
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
        let batch = RecordBatch::try_from_iter([
            (
                "d",
                Arc::new(Float64Array::from(values.to_vec())) as ArrayRef,
            ),
            (
                "r",
                Arc::new(Float32Array::from_iter_values(
                    values.iter().map(|&v| v as f32),
                )) as _,
            ),
        ])
        .unwrap();
        let rows = |value: Value| {
            let filter = Filter::new(&predicate(value), &batch.schema()).unwrap();
            let result = filter.evaluate(&batch).unwrap();
            (0..result.len())
                .map(|row| result.value(row))
                .collect::<Vec<_>>()
        };
        let (t, f) = (true, false);
        for (name, data_type) in [("d", "double"), ("r", "float")] {
            let compare = |op: &str, value: f64| {
                rows(function(
                    op,
                    vec![column(name), literal(data_type, json!(value))],
                ))
            };
            // Spark equates the zeros and the NaNs, and orders NaN above all numbers.
            assert_eq!(compare("=", 0.0), [t, t, f, f, f, f, f]);
            assert_eq!(compare("=", -0.0), [t, t, f, f, f, f, f]);
            assert_eq!(compare("<>", 0.0), [f, f, t, t, t, t, t]);
            assert_eq!(compare("<", 0.0), [f, f, f, f, f, f, t]);
            assert_eq!(compare("<=", -0.0), [t, t, f, f, f, f, t]);
            assert_eq!(compare(">", 1.0), [f, f, t, t, f, t, f]);
            assert_eq!(compare(">=", f64::from(f32::MAX)), [f, f, t, t, f, t, f]);
            assert_eq!(compare("<=>", -0.0), [t, t, f, f, f, f, f]);
            let in_list = function(
                "IN",
                vec![
                    column(name),
                    literal(data_type, json!(-0.0)),
                    literal(data_type, json!(1.0)),
                ],
            );
            assert_eq!(rows(in_list), [t, t, f, f, t, f, f]);
        }
    }

    #[test]
    fn other_predicates_remain_for_spark() {
        let nested = Field::new_struct(
            "point",
            Fields::from(vec![Field::new("x", DataType::Int64, true)]),
            true,
        );
        let schema = Schema::new(vec![
            Field::new("i", DataType::Int64, true),
            Field::new("f", DataType::Float64, true),
            Field::new("g", DataType::Float32, true),
            Field::new("s", DataType::Utf8, true),
            nested,
        ]);
        let unsupported = |value: Value| Filter::new(&predicate(value), &schema).is_none();
        // A double that no float equals.
        assert!(unsupported(function(
            "=",
            vec![column("g"), literal("double", json!(0.1))]
        )));
        let nested_x = json!({"type": "column", "name": ["point", "x"]});
        let nested = function("=", vec![nested_x, bigint(1)]);
        assert!(unsupported(nested.clone()));
        assert!(unsupported(function(
            "=",
            vec![column("missing"), bigint(1)]
        )));
        assert!(unsupported(function("=", vec![column("i"), column("i")])));
        assert!(unsupported(function(
            "FUTURE",
            vec![column("i"), bigint(1)]
        )));
        assert!(unsupported(function("IN", vec![column("i")])));
        assert!(unsupported(function(
            "STARTS_WITH",
            vec![column("i"), bigint(1)]
        )));
        let null_pattern = literal("string", Value::Null);
        assert!(unsupported(function(
            "STARTS_WITH",
            vec![column("s"), null_pattern]
        )));
        assert!(unsupported(json!({"type": "future"})));
        // A conjunct that is not exact makes the whole predicate residual.
        let int = function("<", vec![column("i"), bigint(1)]);
        assert!(unsupported(function(
            "OR",
            vec![int.clone(), nested.clone()]
        )));
        assert!(unsupported(function("AND", vec![int, nested])));
        // IS_NULL compares nothing, so it applies to any column.
        assert!(!unsupported(function("IS_NULL", vec![column("f")])));
        assert!(!unsupported(function("IS_NOT_NULL", vec![column("point")])));
    }

    #[test]
    fn lists_the_columns_that_it_reads_once() {
        let batch = batch();
        let both = function(
            "OR",
            vec![
                function("=", vec![column("s"), literal("string", json!("a"))]),
                function(
                    "AND",
                    vec![
                        function("IS_NULL", vec![column("i")]),
                        function(
                            "STARTS_WITH",
                            vec![column("s"), literal("string", json!("b"))],
                        ),
                    ],
                ),
            ],
        );
        let filter = Filter::new(&predicate(both), &batch.schema()).unwrap();
        let mut columns = Vec::new();
        filter.columns(&mut columns);
        assert_eq!(columns, ["s", "i"]);
    }
}
