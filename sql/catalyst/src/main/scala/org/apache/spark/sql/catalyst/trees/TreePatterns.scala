/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.trees

// Enums for commonly encountered tree patterns in rewrite rules.
object TreePattern extends Enumeration  {
  type TreePattern = Value

  // Enum Ids start from 0.
  // Expression patterns (alphabetically ordered)
  val AGGREGATE_EXPRESSION = Value(0)
  val ALIAS: Value = Value
  val AND: Value = Value
  val ARRAYS_ZIP: Value = Value
  val ATTRIBUTE_REFERENCE: Value = Value
  val APPEND_COLUMNS: Value = Value
  val AVERAGE: Value = Value
  val GROUPING_ANALYTICS: Value = Value
  val BINARY_ARITHMETIC: Value = Value
  val BINARY_COMPARISON: Value = Value
  val CASE_WHEN: Value = Value
  val CAST: Value = Value
  val COALESCE: Value = Value
  val COMMON_EXPR_REF: Value = Value
  val CONCAT: Value = Value
  val COUNT: Value = Value
  val CREATE_NAMED_STRUCT: Value = Value
  val CURRENT_LIKE: Value = Value
  val DESERIALIZE_TO_OBJECT: Value = Value
  val DYNAMIC_PRUNING_EXPRESSION: Value = Value
  val DYNAMIC_PRUNING_SUBQUERY: Value = Value
  val EXISTS_SUBQUERY = Value
  val EXPRESSION_WITH_RANDOM_SEED: Value = Value
  val EXTRACT_VALUE: Value = Value
  val FUNCTION_TABLE_RELATION_ARGUMENT_EXPRESSION: Value = Value
  val GENERATE: Value = Value
  val GENERATOR: Value = Value
  val HIGH_ORDER_FUNCTION: Value = Value
  val IF: Value = Value
  val IN: Value = Value
  val IN_SUBQUERY: Value = Value
  val INLINE_TABLE_EVAL: Value = Value
  val INSET: Value = Value
  val INTERSECT: Value = Value
  val INVOKE: Value = Value
  val JSON_TO_STRUCT: Value = Value
  val LAMBDA_FUNCTION: Value = Value
  val LAMBDA_VARIABLE: Value = Value
  val LATERAL_COLUMN_ALIAS_REFERENCE: Value = Value
  val LATERAL_SUBQUERY: Value = Value
  val LAZY_EXPRESSION: Value = Value
  val LIKE_FAMLIY: Value = Value
  val LIST_SUBQUERY: Value = Value
  val LITERAL: Value = Value
  val MAP_OBJECTS: Value = Value
  val MULTI_ALIAS: Value = Value
  val NEW_INSTANCE: Value = Value
  val NOT: Value = Value
  val NULL_CHECK: Value = Value
  val NULL_LITERAL: Value = Value
  val PIPE_OPERATOR_SELECT: Value = Value
  val SERIALIZE_FROM_OBJECT: Value = Value
  val OR: Value = Value
  val OUTER_REFERENCE: Value = Value
  val PARAMETER: Value = Value
  val PARAMETERIZED_QUERY: Value = Value
  val PIVOT: Value = Value
  val PLAN_EXPRESSION: Value = Value
  val PYTHON_UDF: Value = Value
  val REGEXP_EXTRACT_FAMILY: Value = Value
  val REGEXP_REPLACE: Value = Value
  val RUNTIME_REPLACEABLE: Value = Value
  val SCALAR_SUBQUERY: Value = Value
  val SCALAR_SUBQUERY_REFERENCE: Value = Value
  val SCALA_UDF: Value = Value
  val SESSION_WINDOW: Value = Value
  val SORT: Value = Value
  val SQL_FUNCTION_EXPRESSION: Value = Value
  val SQL_TABLE_FUNCTION: Value = Value
  val SUBQUERY_ALIAS: Value = Value
  val SUM: Value = Value
  val TIME_WINDOW: Value = Value
  val TIME_ZONE_AWARE_EXPRESSION: Value = Value
  val TRANSPOSE: Value = Value
  val TRUE_OR_FALSE_LITERAL: Value = Value
  val VARIANT_GET: Value = Value
  val WINDOW_EXPRESSION: Value = Value
  val WINDOW_TIME: Value = Value
  val UNPIVOT: Value = Value
  val UPDATE_FIELDS: Value = Value
  val UPPER_OR_LOWER: Value = Value
  val UP_CAST: Value = Value
  val DISTRIBUTED_SEQUENCE_ID: Value = Value

  // Logical plan patterns (alphabetically ordered)
  val AGGREGATE: Value = Value
  val AS_OF_JOIN: Value = Value
  val COMMAND: Value = Value
  val CTE: Value = Value
  val DF_DROP_COLUMNS: Value = Value
  val DISTINCT_LIKE: Value = Value
  val EVAL_PYTHON_UDF: Value = Value
  val EVAL_PYTHON_UDTF: Value = Value
  val EVENT_TIME_WATERMARK: Value = Value
  val EXCEPT: Value = Value
  val EXECUTE_IMMEDIATE: Value = Value
  val FILTER: Value = Value
  val INNER_LIKE_JOIN: Value = Value
  val JOIN: Value = Value
  val LATERAL_JOIN: Value = Value
  val LEFT_SEMI_OR_ANTI_JOIN: Value = Value
  val LIMIT: Value = Value
  val LOCAL_RELATION: Value = Value
  val LOGICAL_QUERY_STAGE: Value = Value
  val NATURAL_LIKE_JOIN: Value = Value
  val OUTER_JOIN: Value = Value
  val PROJECT: Value = Value
  val PYTHON_DATA_SOURCE: Value = Value
  val RELATION_TIME_TRAVEL: Value = Value
  val REPARTITION_OPERATION: Value = Value
  val REBALANCE_PARTITIONS: Value = Value
  val UNION: Value = Value
  val UNRESOLVED_RELATION: Value = Value
  val UNRESOLVED_WITH: Value = Value
  val UPDATE_EVENT_TIME_WATERMARK_COLUMN: Value = Value
  val TEMP_RESOLVED_COLUMN: Value = Value
  val TYPED_FILTER: Value = Value
  val WINDOW: Value = Value
  val WINDOW_GROUP_LIMIT: Value = Value
  val WITH_EXPRESSION: Value = Value
  val WITH_WINDOW_DEFINITION: Value = Value

  // Unresolved expression patterns (Alphabetically ordered)
  val UNRESOLVED_ALIAS: Value = Value
  val UNRESOLVED_ATTRIBUTE: Value = Value
  val UNRESOLVED_COLLATION: Value = Value
  val UNRESOLVED_DESERIALIZER: Value = Value
  val UNRESOLVED_DF_STAR: Value = Value
  val UNRESOLVED_HAVING: Value = Value
  val UNRESOLVED_IDENTIFIER: Value = Value
  val UNRESOLVED_ORDINAL: Value = Value
  val UNRESOLVED_FUNCTION: Value = Value
  val UNRESOLVED_HINT: Value = Value
  val UNRESOLVED_WINDOW_EXPRESSION: Value = Value
  val UNRESOLVED_PLAN_ID: Value = Value

  // Unresolved Plan patterns (Alphabetically ordered)
  val UNRESOLVED_FUNC: Value = Value
  val UNRESOLVED_PROCEDURE: Value = Value
  val UNRESOLVED_SUBQUERY_COLUMN_ALIAS: Value = Value
  val UNRESOLVED_TABLE_VALUED_FUNCTION: Value = Value
  val UNRESOLVED_TRANSPOSE: Value = Value
  val UNRESOLVED_TVF_ALIASES: Value = Value

  // Execution expression patterns (alphabetically ordered)
  val IN_SUBQUERY_EXEC: Value = Value

  // Execution Plan patterns (alphabetically ordered)
  val EXCHANGE: Value = Value
}
