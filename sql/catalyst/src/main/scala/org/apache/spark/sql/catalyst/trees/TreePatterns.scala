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

// Enums for tree patterns.
object TreePattern extends Enumeration  {
  type TreePattern = Value

  // Expression patterns (alphabetically ordered)
  val IN: Value = Value(0)

  // Logical plan patterns (alphabetically ordered)
  val LATERAL_JOIN: Value = Value
  val LEFT_SEMI_OR_ANTI_JOIN: Value = Value
  val CROSS_JOIN: Value = Value
  val INNER_LIKE_JOIN: Value = Value
  val JOIN: Value = Value
  val OUTER_JOIN: Value = Value
  val NATURAL_LIKE_JOIN: Value = Value
}
