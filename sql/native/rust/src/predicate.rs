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

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The expression tree documented by `NativeBridge.pushPredicates`.
///
/// Literal values keep their JSON representation so a connector can reject
/// unsupported types without lossy numeric conversions. In particular,
/// decimal values are strings and timestamps are integer microseconds.
/// [`literal_value`](Self::literal_value) decodes them by their data type.
/// Function names are extensible: unknown names must remain residual.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
#[non_exhaustive]
pub enum Predicate {
    Column {
        name: Vec<String>,
    },
    Literal {
        #[serde(rename = "dataType")]
        data_type: String,
        value: Value,
    },
    Function {
        name: String,
        children: Vec<Predicate>,
    },
    #[serde(other)]
    Unknown,
}

/// The value of a literal, decoded by its Spark SQL data type.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum LiteralValue {
    /// A null of any data type.
    Null,
    Boolean(bool),
    /// A `tinyint`, `smallint`, `int`, or `bigint`, within the range of its type.
    Integer(i64),
    /// A `float` or `double`. A `float` is the exact value of the float.
    Float(f64),
    /// A `string`, which Spark compares by its UTF-8 bytes.
    String(String),
    Binary(Vec<u8>),
    /// A `decimal(precision, scale)`, whose value is `unscaled * 10^-scale`.
    Decimal {
        unscaled: i128,
        precision: u8,
        scale: i8,
    },
    /// A `date`, as days since 1970-01-01.
    Date(i32),
    /// A `timestamp`, as microseconds since the epoch.
    Timestamp(i64),
    /// A `timestamp_ntz`, as microseconds since 1970-01-01 00:00:00 without a time zone.
    TimestampNtz(i64),
}

impl Predicate {
    /// Decodes a literal by its data type. Returns `None` for other expressions, and
    /// for data types or values that this SDK does not decode: keep such predicates
    /// unsupported.
    pub fn literal_value(&self) -> Option<LiteralValue> {
        let Predicate::Literal { data_type, value } = self else {
            return None;
        };
        if value.is_null() {
            return Some(LiteralValue::Null);
        }
        Some(match data_type.as_str() {
            "boolean" => LiteralValue::Boolean(value.as_bool()?),
            "tinyint" => LiteralValue::Integer(i8::try_from(value.as_i64()?).ok()?.into()),
            "smallint" => LiteralValue::Integer(i16::try_from(value.as_i64()?).ok()?.into()),
            "int" => LiteralValue::Integer(i32::try_from(value.as_i64()?).ok()?.into()),
            "bigint" => LiteralValue::Integer(value.as_i64()?),
            "float" => {
                // Spark writes a float as the double of the same value.
                let value = value.as_f64()?;
                if f64::from(value as f32) != value {
                    return None;
                }
                LiteralValue::Float(value)
            }
            "double" => LiteralValue::Float(value.as_f64()?),
            "string" => LiteralValue::String(value.as_str()?.to_owned()),
            "binary" => LiteralValue::Binary(decode_base64(value.as_str()?)?),
            "date" => LiteralValue::Date(i32::try_from(value.as_i64()?).ok()?),
            "timestamp" => LiteralValue::Timestamp(value.as_i64()?),
            "timestamp_ntz" => LiteralValue::TimestampNtz(value.as_i64()?),
            other => decode_decimal(other, value.as_str()?)?,
        })
    }
}

/// Decodes a `decimal(p,s)` literal, which Spark writes as a plain decimal string.
fn decode_decimal(data_type: &str, text: &str) -> Option<LiteralValue> {
    let arguments = data_type.strip_prefix("decimal(")?.strip_suffix(')')?;
    let (precision, scale) = arguments.split_once(',')?;
    let precision: u8 = precision.trim().parse().ok()?;
    let scale: i8 = scale.trim().parse().ok()?;
    if !(1..=38).contains(&precision) || i16::from(scale) > i16::from(precision) {
        return None;
    }
    let (negative, digits) = match text.strip_prefix('-') {
        Some(digits) => (true, digits),
        None => (false, text),
    };
    let (integer, fraction) = match digits.split_once('.') {
        Some((integer, fraction)) if !fraction.is_empty() => (integer, fraction),
        Some(_) => return None,
        None => (digits, ""),
    };
    if integer.is_empty()
        || !integer
            .bytes()
            .chain(fraction.bytes())
            .all(|digit| digit.is_ascii_digit())
    {
        return None;
    }
    let mut unscaled: i128 = 0;
    for digit in integer.bytes().chain(fraction.bytes()) {
        unscaled = unscaled
            .checked_mul(10)?
            .checked_add(i128::from(digit - b'0'))?;
    }
    // The digits have `fraction.len()` decimal places; rescale them to `scale` exactly.
    let shift = i32::from(scale) - i32::try_from(fraction.len()).ok()?;
    let factor = 10_i128.checked_pow(shift.unsigned_abs())?;
    if shift >= 0 {
        unscaled = unscaled.checked_mul(factor)?;
    } else if unscaled % factor == 0 {
        unscaled /= factor;
    } else {
        return None;
    }
    if unscaled >= 10_i128.pow(u32::from(precision)) {
        return None;
    }
    Some(LiteralValue::Decimal {
        unscaled: if negative { -unscaled } else { unscaled },
        precision,
        scale,
    })
}

/// Decodes standard base64 with padding, as Spark writes binary literals.
fn decode_base64(text: &str) -> Option<Vec<u8>> {
    let text = text.as_bytes();
    if text.len() % 4 != 0 {
        return None;
    }
    let groups = text.len() / 4;
    let mut bytes = Vec::with_capacity(groups * 3);
    for (index, group) in text.chunks_exact(4).enumerate() {
        let padding = group.iter().rev().take_while(|&&byte| byte == b'=').count();
        if padding > 2 || (padding > 0 && index + 1 != groups) {
            return None;
        }
        let mut bits = 0_u32;
        for &byte in &group[..4 - padding] {
            bits = (bits << 6) | u32::from(base64_value(byte)?);
        }
        bits <<= 6 * padding;
        bytes.extend_from_slice(&bits.to_be_bytes()[1..4 - padding]);
    }
    Some(bytes)
}

fn base64_value(byte: u8) -> Option<u8> {
    match byte {
        b'A'..=b'Z' => Some(byte - b'A'),
        b'a'..=b'z' => Some(byte - b'a' + 26),
        b'0'..=b'9' => Some(byte - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{LiteralValue, Predicate};

    fn literal(data_type: &str, value: serde_json::Value) -> Option<LiteralValue> {
        Predicate::Literal {
            data_type: data_type.into(),
            value,
        }
        .literal_value()
    }

    #[test]
    fn preserves_int64_literals_and_nested_column_paths() {
        let json = r#"{"type":"function","name":">=","children":[
            {"type":"column","name":["outer","id"]},
            {"type":"literal","dataType":"bigint","value":9223372036854775807}
        ]}"#;
        let predicate: Predicate = serde_json::from_str(json).unwrap();
        let Predicate::Function { children, .. } = predicate else {
            panic!("expected a function");
        };
        assert_eq!(
            children[0],
            Predicate::Column {
                name: vec!["outer".into(), "id".into()]
            }
        );
        let Predicate::Literal { value, .. } = &children[1] else {
            panic!("expected a literal");
        };
        assert_eq!(value.as_i64(), Some(i64::MAX));
        assert_eq!(
            children[1].literal_value(),
            Some(LiteralValue::Integer(i64::MAX))
        );
        assert_eq!(children[0].literal_value(), None);
    }

    #[test]
    fn preserves_unknown_expressions_for_conservative_fallback() {
        let json = r#"{"type":"future_expression","value":1}"#;
        assert_eq!(
            serde_json::from_str::<Predicate>(json).unwrap(),
            Predicate::Unknown
        );
        let json = r#"{"type":"function","name":"FUTURE_FUNCTION","children":[]}"#;
        let Predicate::Function { name, .. } = serde_json::from_str(json).unwrap() else {
            panic!("expected a function");
        };
        assert_eq!(name, "FUTURE_FUNCTION");
    }

    #[test]
    fn decodes_literals_by_their_data_type_and_range() {
        assert_eq!(literal("int", json!(null)), Some(LiteralValue::Null));
        assert_eq!(literal("void", json!(null)), Some(LiteralValue::Null));
        assert_eq!(
            literal("boolean", json!(true)),
            Some(LiteralValue::Boolean(true))
        );
        assert_eq!(
            literal("tinyint", json!(-128)),
            Some(LiteralValue::Integer(-128))
        );
        assert_eq!(literal("tinyint", json!(128)), None);
        assert_eq!(
            literal("smallint", json!(32767)),
            Some(LiteralValue::Integer(32767))
        );
        assert_eq!(literal("smallint", json!(32768)), None);
        assert_eq!(
            literal("int", json!(i32::MIN)),
            Some(LiteralValue::Integer(-1 << 31))
        );
        assert_eq!(literal("int", json!(1_i64 << 31)), None);
        assert_eq!(literal("bigint", json!(1.5)), None);
        assert_eq!(
            literal("double", json!(1.5)),
            Some(LiteralValue::Float(1.5))
        );
        assert_eq!(
            literal("float", json!(f64::from(0.1_f32))),
            Some(LiteralValue::Float(f64::from(0.1_f32)))
        );
        assert_eq!(
            literal("float", json!(0.1)),
            None,
            "0.1 is not a float value"
        );
        assert_eq!(
            literal("string", json!("row-1")),
            Some(LiteralValue::String("row-1".into()))
        );
        assert_eq!(literal("string", json!(1)), None);
        assert_eq!(
            literal("date", json!(19000)),
            Some(LiteralValue::Date(19000))
        );
        assert_eq!(
            literal("timestamp", json!(-1)),
            Some(LiteralValue::Timestamp(-1))
        );
        assert_eq!(
            literal("timestamp_ntz", json!(i64::MAX)),
            Some(LiteralValue::TimestampNtz(i64::MAX))
        );
        assert_eq!(literal("interval day", json!(1)), None);
    }

    #[test]
    fn decodes_binary_literals_as_padded_base64() {
        for (text, bytes) in [
            ("", &b""[..]),
            ("Zg==", b"f"),
            ("Zm8=", b"fo"),
            ("Zm9v", b"foo"),
            ("Zm9vYg==", b"foob"),
            ("AP8+/w==", &[0x00, 0xff, 0x3e, 0xff]),
        ] {
            assert_eq!(
                literal("binary", json!(text)),
                Some(LiteralValue::Binary(bytes.to_vec())),
                "{text}"
            );
        }
        for invalid in ["Zg", "Zg=", "Z===", "Zg==Zg==", "Zm9-", "Zm 9"] {
            assert_eq!(literal("binary", json!(invalid)), None, "{invalid}");
        }
    }

    #[test]
    fn decodes_plain_decimal_strings_at_their_scale() {
        let decimal = |unscaled, precision, scale| {
            Some(LiteralValue::Decimal {
                unscaled,
                precision,
                scale,
            })
        };
        assert_eq!(
            literal("decimal(10,2)", json!("123.45")),
            decimal(12345, 10, 2)
        );
        assert_eq!(literal("decimal(10,2)", json!("-0.05")), decimal(-5, 10, 2));
        assert_eq!(literal("decimal(10,2)", json!("1.00")), decimal(100, 10, 2));
        assert_eq!(
            literal("decimal(5,0)", json!("12345")),
            decimal(12345, 5, 0)
        );
        assert_eq!(
            literal("decimal(3,-2)", json!("12300")),
            decimal(123, 3, -2)
        );
        let max = "9".repeat(38);
        assert_eq!(
            literal("decimal(38,0)", json!(max)),
            decimal(10_i128.pow(38) - 1, 38, 0)
        );
        for (data_type, text) in [
            ("decimal(4,2)", "123.45"),
            ("decimal(10,2)", "1.234"),
            ("decimal(3,-2)", "12345"),
            ("decimal(10,2)", "1."),
            ("decimal(10,2)", ".5"),
            ("decimal(10,2)", "1e3"),
            ("decimal(10,2)", "+1.00"),
            ("decimal(39,0)", "1"),
            ("decimal(2,3)", "0.001"),
        ] {
            assert_eq!(literal(data_type, json!(text)), None, "{data_type} {text}");
        }
        assert_eq!(literal("decimal(10,2)", json!(1.5)), None);
    }
}
