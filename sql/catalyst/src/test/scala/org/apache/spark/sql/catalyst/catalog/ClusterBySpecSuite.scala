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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.connector.expressions.{ApplyTransform, FieldReference, IdentityTransform, LiteralValue}
import org.apache.spark.sql.types.{IntegerType, StringType, TimestampNTZType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

class ClusterBySpecSuite extends SparkFunSuite with SQLHelper {

  // -- plain column roundtrip tests
  test("plain columns roundtrip through toJson and fromProperty") {
    val spec = ClusterBySpec.ofColumns(Seq(FieldReference(Seq("a")), FieldReference(Seq("b", "c"))))
    val json = spec.toJson
    val roundtripped = ClusterBySpec.fromProperty(json)
    assert(roundtripped.columnNames === spec.columnNames)
    assert(roundtripped.entries.forall(_.isInstanceOf[IdentityTransform]))
  }

  // -- ApplyTransform roundtrip tests
  test("ApplyTransform entry roundtrips through toJson and fromProperty") {
    val spec = new ClusterBySpec(Seq(
      ApplyTransform("variant_get", Seq(
        FieldReference(Seq("col1")),
        LiteralValue(UTF8String.fromString("$.foo"), StringType),
        LiteralValue(UTF8String.fromString("STRING"), StringType)))))

    val json = spec.toJson
    val roundtripped = ClusterBySpec.fromProperty(json)

    assert(roundtripped.columnNames === Seq(FieldReference(Seq("col1"))))
    assert(roundtripped.entries.length === 1)
    val rt = roundtripped.entries.head.asInstanceOf[ApplyTransform]
    assert(rt.name === "variant_get")
    assert(rt.args.length === 3)
    assert(rt.args(0).asInstanceOf[FieldReference].fieldNames().toSeq === Seq("col1"))
  }

  test("mixed plain and expression entries roundtrip") {
    val spec = new ClusterBySpec(Seq(
      IdentityTransform(FieldReference(Seq("id"))),
      ApplyTransform("upper", Seq(FieldReference(Seq("name"))))))

    val roundtripped = ClusterBySpec.fromProperty(spec.toJson)

    assert(roundtripped.columnNames ===
      Seq(FieldReference(Seq("id")), FieldReference(Seq("name"))))
    assert(roundtripped.entries(0).isInstanceOf[IdentityTransform])
    val rt = roundtripped.entries(1).asInstanceOf[ApplyTransform]
    assert(rt.name === "upper")
  }

  test("fromProperty is backward compatible with the old plain Seq[Seq[String]] format") {
    // Old format: just column name arrays
    val oldFormat = """[["id"],["data"]]"""
    val spec = ClusterBySpec.fromProperty(oldFormat)
    assert(spec.columnNames === Seq(FieldReference(Seq("id")), FieldReference(Seq("data"))))
    assert(spec.entries.forall(_.isInstanceOf[IdentityTransform]))
  }

  test("integer literal in expression roundtrips correctly") {
    val spec = new ClusterBySpec(Seq(
      ApplyTransform("some_func", Seq(
        FieldReference(Seq("col")),
        LiteralValue(42, IntegerType)))))

    val roundtripped = ClusterBySpec.fromProperty(spec.toJson)
    assert(roundtripped.columnNames === Seq(FieldReference(Seq("col"))))
    val rt = roundtripped.entries.head.asInstanceOf[ApplyTransform]
    assert(rt.name === "some_func")
    assert(rt.args.length === 2)
  }

  // -- string literal round-trip tests
  test("string literal values are preserved through toJson/fromProperty round-trip") {
    val spec = new ClusterBySpec(Seq(
      ApplyTransform("variant_get", Seq(
        FieldReference(Seq("col")),
        LiteralValue(UTF8String.fromString("$.foo"), StringType),
        LiteralValue(UTF8String.fromString("STRING"), StringType)))))

    val json = spec.toJson
    val roundtripped = ClusterBySpec.fromProperty(json)

    val rt = roundtripped.entries.head.asInstanceOf[ApplyTransform]
    assert(rt.name === "variant_get")
    assert(rt.args.length === 3)

    val lit1 = rt.args(1).asInstanceOf[LiteralValue[_]]
    assert(lit1.dataType === StringType)
    assert(lit1.value.toString === "$.foo")

    val lit2 = rt.args(2).asInstanceOf[LiteralValue[_]]
    assert(lit2.dataType === StringType)
    assert(lit2.value.toString === "STRING")
  }

  test("string literal with embedded single quotes round-trips correctly") {
    val spec = new ClusterBySpec(Seq(
      ApplyTransform("some_func", Seq(
        FieldReference(Seq("col")),
        LiteralValue(UTF8String.fromString("it's a test"), StringType)))))

    val json = spec.toJson
    val roundtripped = ClusterBySpec.fromProperty(json)

    val rt = roundtripped.entries.head.asInstanceOf[ApplyTransform]
    val lit = rt.args(1).asInstanceOf[LiteralValue[_]]
    assert(lit.dataType === StringType)
    assert(lit.value.toString === "it's a test")
  }

  test("string literal that looks like a column name round-trips as string, not column ref") {
    val spec = new ClusterBySpec(Seq(
      ApplyTransform("some_func", Seq(
        FieldReference(Seq("col")),
        LiteralValue(UTF8String.fromString("my_column"), StringType)))))

    val json = spec.toJson
    val roundtripped = ClusterBySpec.fromProperty(json)

    val rt = roundtripped.entries.head.asInstanceOf[ApplyTransform]
    assert(rt.args(1).isInstanceOf[LiteralValue[_]])
    val lit = rt.args(1).asInstanceOf[LiteralValue[_]]
    assert(lit.dataType === StringType)
    assert(lit.value.toString === "my_column")
  }

  // -- timestamp literal round-trip tests
  private def testTimestampRoundTrip(microseconds: Long): Unit = {
    val spec = new ClusterBySpec(Seq(
      ApplyTransform("date_trunc", Seq(
        FieldReference(Seq("ts_col")),
        LiteralValue(microseconds, TimestampType)))))

    val json = spec.toJson
    val roundtripped = ClusterBySpec.fromProperty(json)

    val rt = roundtripped.entries.head.asInstanceOf[ApplyTransform]
    assert(rt.name === "date_trunc")
    assert(rt.args.length === 2)
    val lit = rt.args(1).asInstanceOf[LiteralValue[_]]
    assert(lit.dataType === TimestampType)
    assert(lit.value == microseconds)
  }

  test("timestamp literal round-trips correctly") {
    withSQLConf("spark.sql.session.timeZone" -> "UTC") {
      testTimestampRoundTrip(1672531200000000L)
    }
  }

  test("timestamp literal round-trip with non-UTC timezone") {
    withSQLConf("spark.sql.session.timeZone" -> "America/Toronto") {
      testTimestampRoundTrip(1672531200000000L)
    }
  }

  test("timestamp_ntz literal round-trips correctly") {
    val microseconds = 1672531200000000L
    val spec = new ClusterBySpec(Seq(
      ApplyTransform("date_trunc", Seq(
        FieldReference(Seq("ts_col")),
        LiteralValue(microseconds, TimestampNTZType)))))

    val json = spec.toJson
    val roundtripped = ClusterBySpec.fromProperty(json)

    val rt = roundtripped.entries.head.asInstanceOf[ApplyTransform]
    assert(rt.args.length === 2)
    val lit = rt.args(1).asInstanceOf[LiteralValue[_]]
    assert(lit.dataType === TimestampNTZType)
    assert(lit.value == microseconds)
  }

  // -- backward-compatible factory tests
  test("apply(Seq[NamedReference]) creates IdentityTransform entries") {
    val spec = ClusterBySpec.ofColumns(Seq(FieldReference(Seq("a")), FieldReference(Seq("b"))))
    assert(spec.entries.length === 2)
    assert(spec.entries.forall(_.isInstanceOf[IdentityTransform]))
    assert(spec.columnNames === Seq(FieldReference(Seq("a")), FieldReference(Seq("b"))))
  }

  test("plain-columns-only toJson produces old Seq[Seq[String]] format") {
    val spec = ClusterBySpec.ofColumns(Seq(FieldReference(Seq("id")), FieldReference(Seq("data"))))
    val json = spec.toJson
    // Should be flat array of arrays, no objects
    assert(!json.contains("{"), s"Expected flat array format but got: $json")
    assert(json === """[["id"],["data"]]""")
  }

  test("expression entries toJson produces structured JSON format") {
    val spec = new ClusterBySpec(Seq(
      ApplyTransform("upper", Seq(FieldReference(Seq("name"))))))
    val json = spec.toJson
    assert(json.contains("transform"), s"Expected structured format but got: $json")
    assert(json.contains("\"name\":\"upper\""))
  }
}
