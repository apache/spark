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

package org.apache.spark.sql.catalyst.json

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.{EqualTo, Filter, StringStartsWith}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

class JacksonParserSuite extends SparkFunSuite {
  test("skipping rows using pushdown filters") {
    def check(
      input: String = """{"i":1, "s": "a"}""",
      schema: StructType = StructType.fromDDL("i INTEGER"),
      filters: Seq[Filter],
      expected: Seq[InternalRow]): Unit = {
      val options = new JSONOptions(Map.empty[String, String], "GMT", "")
      val parser = new JacksonParser(schema, options, false, filters)
      val createParser = CreateJacksonParser.string _
      val actual = parser.parse(input, createParser, UTF8String.fromString)
      assert(actual === expected)
    }

    check(filters = Seq(), expected = Seq(InternalRow(1)))
    check(filters = Seq(EqualTo("i", 1)), expected = Seq(InternalRow(1)))
    check(filters = Seq(EqualTo("i", 2)), expected = Seq.empty)
    check(
      schema = StructType.fromDDL("s STRING"),
      filters = Seq(StringStartsWith("s", "b")),
      expected = Seq.empty)
    check(
      schema = StructType.fromDDL("i INTEGER, s STRING"),
      filters = Seq(StringStartsWith("s", "a")),
      expected = Seq(InternalRow(1, UTF8String.fromString("a"))))
    check(
      input = """{"i":1,"s": "a", "d": 3.14}""",
      schema = StructType.fromDDL("i INTEGER, d DOUBLE"),
      filters = Seq(EqualTo("d", 3.14)),
      expected = Seq(InternalRow(1, 3.14)))
  }
}
