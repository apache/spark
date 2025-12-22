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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for function qualification support (system.builtin.func, builtin.func, etc.)
 */
class FunctionQualificationSuite extends SharedSparkSession {

  test("unqualified builtin function works") {
    val result = sql("SELECT abs(-5)").collect()
    assert(result.length == 1)
    assert(result(0).getInt(0) == 5)
  }

  test("builtin qualified function works") {
    val result = sql("SELECT builtin.abs(-5)").collect()
    assert(result.length == 1)
    assert(result(0).getInt(0) == 5)
  }

  test("system.builtin qualified function works") {
    val result = sql("SELECT system.builtin.abs(-5)").collect()
    assert(result.length == 1)
    assert(result(0).getInt(0) == 5)
  }

  test("case insensitive qualification works") {
    val result = sql("SELECT BUILTIN.ABS(-5)").collect()
    assert(result.length == 1)
    assert(result(0).getInt(0) == 5)
  }

  test("multiple qualified functions in single query") {
    val result = sql("SELECT builtin.abs(-5), system.builtin.upper('test')").collect()
    assert(result.length == 1)
    assert(result(0).getInt(0) == 5)
    assert(result(0).getString(1) == "TEST")
  }

  test("qualified function in WHERE clause") {
    val result = sql("SELECT 1 WHERE builtin.abs(-5) = 5").collect()
    assert(result.length == 1)
  }

  test("qualified aggregate function") {
    val result = sql("SELECT builtin.sum(value) FROM VALUES (1), (2), (3) AS t(value)").collect()
    assert(result.length == 1)
    assert(result(0).getLong(0) == 6)
  }
}
