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

#[cfg(test)]
mod tests {
    use super::Predicate;

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
}
