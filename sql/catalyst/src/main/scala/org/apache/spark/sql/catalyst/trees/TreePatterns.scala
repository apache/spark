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
  val AND_OR: Value = Value(0)
  val ATTRIBUTE_REFERENCE: Value = Value
  val APPEND_COLUMNS: Value = Value
  val BINARY_ARITHMETIC: Value = Value
  val BINARY_COMPARISON: Value = Value
  val CASE_WHEN: Value = Value
  val CAST: Value = Value
  val CONCAT: Value = Value
  val COUNT: Value = Value
  val CREATE_NAMED_STRUCT: Value = Value
  val DESERIALIZE_TO_OBJECT: Value = Value
  val DYNAMIC_PRUNING_SUBQUERY: Value = Value
  val EXISTS_SUBQUERY = Value
  val EXPRESSION_WITH_RANDOM_SEED: Value = Value
  val EXTRACT_VALUE: Value = Value
  val IF: Value = Value
  val IN: Value = Value
  val IN_SUBQUERY: Value = Value
  val INSET: Value = Value
  val JSON_TO_STRUCT: Value = Value
  val LAMBDA_VARIABLE: Value = Value
  val LIKE_FAMLIY: Value = Value
  val LIST_SUBQUERY: Value = Value
  val LITERAL: Value = Value
  val MAP_OBJECTS: Value = Value
  val NOT: Value = Value
  val NULL_CHECK: Value = Value
  val NULL_LITERAL: Value = Value
  val SERIALIZE_FROM_OBJECT: Value = Value
  val OUTER_REFERENCE: Value = Value
  val PLAN_EXPRESSION: Value = Value
  val SCALAR_SUBQUERY: Value = Value
  val TRUE_OR_FALSE_LITERAL: Value = Value
  val WINDOW_EXPRESSION: Value = Value
  val UNARY_POSITIVE: Value = Value
  val UPPER_OR_LOWER: Value = Value

  // Logical plan patterns (alphabetically ordered)
  val AGGREGATE: Value = Value
  val EXCEPT: Value = Value
  val FILTER: Value = Value
  val INNER_LIKE_JOIN: Value = Value
  val JOIN: Value = Value
  val LEFT_SEMI_OR_ANTI_JOIN: Value = Value
  val LIMIT: Value = Value
  val LOCAL_RELATION: Value = Value
  val NATURAL_LIKE_JOIN: Value = Value
  val OUTER_JOIN: Value = Value
  val PROJECT: Value = Value
  val TYPED_FILTER: Value = Value
  val WINDOW: Value = Value
}
