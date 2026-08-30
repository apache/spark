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

package org.apache.spark.sql.catalyst.catalog

import org.json4s.JsonAST.{JArray, JObject, JString}
import org.json4s.jackson.JsonMethods.{compact, render}

import org.apache.spark.SparkFunSuite

/**
 * Unit tests for [[SqlPathFormat]] -- the helper that converts the raw JSON-array-of-arrays
 * path stored on view / SQL function metadata into the JSON-object form used by DESCRIBE
 * AS JSON and the human-readable form used by DESCRIBE EXTENDED.
 */
class SqlPathFormatSuite extends SparkFunSuite {

  private def compactJson(v: JArray): String = compact(render(v))

  test("toDescribeJson: maps each [catalog, ns...] entry to a JSON object") {
    val stored =
      """[["spark_catalog","default"],["system","builtin"]]"""
    val result = SqlPathFormat.toDescribeJson(stored)
      .getOrElse(fail(s"Expected a JSON value, got None for: $stored"))
    val expected = JArray(List(
      JObject("catalog_name" -> JString("spark_catalog"),
        "namespace" -> JArray(List(JString("default")))),
      JObject("catalog_name" -> JString("system"),
        "namespace" -> JArray(List(JString("builtin"))))))
    assert(compactJson(result.asInstanceOf[JArray]) == compactJson(expected))
  }

  test("toDescribeJson: multi-level namespace becomes [head, tail...]") {
    val stored = """[["cat1","db","sub"]]"""
    val result = SqlPathFormat.toDescribeJson(stored)
      .getOrElse(fail("Expected a JSON value"))
    val expected = JArray(List(
      JObject("catalog_name" -> JString("cat1"),
        "namespace" -> JArray(List(JString("db"), JString("sub"))))))
    assert(compactJson(result.asInstanceOf[JArray]) == compactJson(expected))
  }

  test("toDescribeJson: empty array returns None") {
    assert(SqlPathFormat.toDescribeJson("[]").isEmpty)
  }

  test("toDescribeJson: malformed payloads return None") {
    Seq(
      "",
      "not_json",
      "{}",
      """{"foo":1}""",
      """[1, 2, 3]"""
    ).foreach { payload =>
      assert(SqlPathFormat.toDescribeJson(payload).isEmpty, s"payload=$payload")
    }
  }

  test("formatForDisplay: renders plain identifiers without backticks") {
    val json = SqlPathFormat.toDescribeJson(
      """[["spark_catalog","default"],["system","builtin"]]""")
      .getOrElse(fail("Expected a JSON value"))
    val rendered = SqlPathFormat.formatForDisplay(json)
      .getOrElse(fail("Expected a display string"))
    assert(rendered == "spark_catalog.default, system.builtin")
  }

  test("formatForDisplay: backticks identifiers that need quoting") {
    val json = SqlPathFormat.toDescribeJson(
      """[["spark_catalog","weird.schema"]]""")
      .getOrElse(fail("Expected a JSON value"))
    val rendered = SqlPathFormat.formatForDisplay(json)
      .getOrElse(fail("Expected a display string"))
    assert(rendered == "spark_catalog.`weird.schema`")
  }

  test("formatForDisplay: round-trips multi-level namespaces") {
    val json = SqlPathFormat.toDescribeJson("""[["cat","db","ns"]]""")
      .getOrElse(fail("Expected a JSON value"))
    val rendered = SqlPathFormat.formatForDisplay(json)
      .getOrElse(fail("Expected a display string"))
    assert(rendered == "cat.db.ns")
  }
}
