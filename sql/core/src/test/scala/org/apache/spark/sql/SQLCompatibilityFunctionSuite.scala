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

package org.apache.spark.sql

import org.apache.spark.sql.test.SharedSQLContext

/**
 * A test suite for functions added for compatibility with other databases such as Oracle, MSSQL.
 * These functions are typically implemented using the trait
 * [[org.apache.spark.sql.catalyst.expressions.RuntimeReplaceable]].
 */
class SQLCompatibilityFunctionSuite extends QueryTest with SharedSQLContext {

  test("ifnull") {
    checkAnswer(
      sql("SELECT ifnull(null, 'x'), ifnull('y', 'x'), ifnull(null, null)"),
      Row("x", "y", null))

    // Type coercion
    checkAnswer(
      sql("SELECT ifnull(1, 2.1d), ifnull(null, 2.1d)"),
      Row(1.0, 2.1))
  }

  test("nullif") {
    checkAnswer(
      sql("SELECT nullif('x', 'x'), nullif('x', 'y')"),
      Row(null, "x"))

    // Type coercion
    checkAnswer(
      sql("SELECT nullif(1, 2.1d), nullif(1, 1.0d)"),
      Row(1.0, null))
  }

  test("nvl") {
    checkAnswer(
      sql("SELECT nvl(null, 'x'), nvl('y', 'x'), nvl(null, null)"),
      Row("x", "y", null))

    // Type coercion
    checkAnswer(
      sql("SELECT nvl(1, 2.1d), nvl(null, 2.1d)"),
      Row(1.0, 2.1))
  }

  test("nvl2") {
    checkAnswer(
      sql("SELECT nvl2(null, 'x', 'y'), nvl2('n', 'x', 'y'), nvl2(null, null, null)"),
      Row("y", "x", null))

    // Type coercion
    checkAnswer(
      sql("SELECT nvl2(null, 1, 2.1d), nvl2('n', 1, 2.1d)"),
      Row(2.1, 1.0))
  }
}
