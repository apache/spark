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

package org.apache.spark.sql.execution.datasources.csv

import org.apache.spark.SparkFunSuite

class CSVUtilsSuite extends SparkFunSuite {

  test("generate default names when header is not used") {
    val csvOptions = CSVOptions("header", "false")
    val row = Array("a", "b", "c")
    val expected = Array("_c0", "_c1", "_c2")
    assert(CSVUtils.makeSafeHeader(row, csvOptions, false).deep == expected.deep)
  }

  test("duplicated empty strings as field names") {
    val csvOptions = CSVOptions("header", "true")
    val row = Array("", "", "")
    val expected = Array("_c0", "_c1", "_c2")
    assert(CSVUtils.makeSafeHeader(row, csvOptions, false).deep == expected.deep)
  }

  test("duplicated nullValue as field names") {
    val csvOptions = new CSVOptions(Map("header" -> "true", "nullValue" -> "abc"))
    val row = Array("abc", "abc", "abc")
    val expected = Array("_c0", "_c1", "_c2")
    assert(CSVUtils.makeSafeHeader(row, csvOptions, false).deep == expected.deep)
  }

  test("duplicated field names - case-sensitive") {
    val csvOptions = CSVOptions("header", "true")
    val row = Array("a", "A", "a")
    val expected = Array("a0", "A", "a2")
    val caseSensitive = true
    assert(CSVUtils.makeSafeHeader(row, csvOptions, caseSensitive).deep == expected.deep)
  }

  test("duplicated field names - case-insensitive") {
    val csvOptions = CSVOptions("header", "true")
    val row = Array("a", "A", "a")
    val expected = Array("a0", "A1", "a2")
    val caseSensitive = false
    assert(CSVUtils.makeSafeHeader(row, csvOptions, caseSensitive).deep == expected.deep)
  }
}
