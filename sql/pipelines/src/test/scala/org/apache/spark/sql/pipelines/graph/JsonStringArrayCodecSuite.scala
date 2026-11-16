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

package org.apache.spark.sql.pipelines.graph

import org.apache.spark.SparkFunSuite

/**
 * Tests for the JSON string-array codec shared by the declared-keys record
 * ([[PipelinesTableProperties.declaredPropertyKeys]]) and the AutoCDC auxiliary-table properties.
 * The record decides which table properties a later run removes, so a value must either parse to
 * exactly the serialized list or be rejected as malformed.
 */
class JsonStringArrayCodecSuite extends SparkFunSuite {

  private def assertRoundTrip(values: Seq[String]): Unit = {
    val json = JsonStringArrayCodec.serialize(values)
    assert(
      JsonStringArrayCodec.parse(json).contains(values),
      s"round-trip failed: input=$values, serialized=$json")
  }

  test("round-trips plain values and the empty list") {
    assertRoundTrip(Seq.empty)
    assertRoundTrip(Seq("p.a"))
    assertRoundTrip(Seq("p.a", "p.b", "comment"))
  }

  test("round-trips values containing characters that JSON must escape") {
    assertRoundTrip(Seq("a\"b"))
    assertRoundTrip(Seq("a\\b"))
    assertRoundTrip(Seq("a\nb", "a\tb"))
    assertRoundTrip(Seq("a\"b\\c\nd"))
  }

  test("round-trips values containing separator characters that JSON does not escape") {
    assertRoundTrip(Seq("a,b", "[a]", "{a}", "a:b", "it's", "a.b", "name with spaces", "a`b"))
  }

  test("serializes a compact JSON string array") {
    assert(JsonStringArrayCodec.serialize(Seq.empty) == "[]")
    assert(JsonStringArrayCodec.serialize(Seq("p.a", "p.b")) == """["p.a","p.b"]""")
    assert(JsonStringArrayCodec.serialize(Seq("a\"b", "a\\b")) == """["a\"b","a\\b"]""")
  }

  test("parse accepts whitespace inside the array") {
    assert(JsonStringArrayCodec.parse("""[ "p.a" , "p.b" ]""").contains(Seq("p.a", "p.b")))
  }

  test("parse returns None for inputs that are not a JSON array of strings") {
    Seq(
      "not-json",
      "",
      "\"id\"", // bare string
      "null",
      "{\"id\": 1}", // object
      "[1, 2, 3]", // numbers
      "[\"id\", 1]", // mixed types
      "[\"id\", null]",
      "[[\"id\"]]", // nested array
      "[\"id\"" // unterminated
    ).foreach { raw =>
      assert(JsonStringArrayCodec.parse(raw).isEmpty, s"expected None for: $raw")
    }
  }
}
