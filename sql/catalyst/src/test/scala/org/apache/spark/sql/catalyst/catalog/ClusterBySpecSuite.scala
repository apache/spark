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
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.connector.expressions.{ClusteringColumnTransform, FieldReference, LiteralValue}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType, TimestampNTZType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

class ClusterBySpecSuite extends SparkFunSuite with SQLHelper {

  // -- column name roundtrip tests
  test("fromColumnEntries with plain columns roundtrips through toColumnNames") {
    val spec = ClusterBySpec(Seq(FieldReference(Seq("a")), FieldReference(Seq("b", "c"))))
    val json = spec.toColumnNames
    val roundtripped = ClusterBySpec.fromProperty(json)
    assert(roundtripped.columnNames === spec.columnNames)
    assert(roundtripped.clusteringColumnTransforms.isEmpty)
  }

  // -- from expressions tests
  test("fromExpressions with a plain column reference produces no transform") {
    val spec = ClusterBySpec.fromExpressions(
      Seq(scala.util.Right(Seq("col1"))))
    assert(spec.columnNames === Seq(FieldReference(Seq("col1"))))
    assert(spec.clusteringColumnTransforms.isEmpty)
  }

  test("fromExpressions with a function call produces a ClusteringColumnTransform") {
    val funcExpr = UnresolvedFunction(
      "variant_get",
      Seq(
        UnresolvedAttribute(Seq("col1")),
        Literal(UTF8String.fromString("$.foo"), StringType),
        Literal(UTF8String.fromString("STRING"), StringType)),
      isDistinct = false)
    val spec = ClusterBySpec.fromExpressions(Seq(scala.util.Left(funcExpr)))
    assert(spec.columnNames === Seq(FieldReference(Seq("col1"))))
    assert(spec.clusteringColumnTransforms.length === 1)
    val transformOpt = spec.clusteringColumnTransforms.head
    assert(transformOpt.isDefined)
    val transform = transformOpt.get.asInstanceOf[ClusteringColumnTransform]
    assert(transform.name === "variant_get")
  }

  // -- toColumnNames / fromProperty roundtrip with expressions tests
  test("expression column roundtrips through toColumnNames and fromProperty") {
    val transform = new ClusteringColumnTransform(
      "variant_get",
      Array(
        FieldReference(Seq("col1")),
        LiteralValue(UTF8String.fromString("$.foo"), StringType),
        LiteralValue(UTF8String.fromString("STRING"), StringType)))
    val spec = ClusterBySpec(
      columnNames = Seq(FieldReference(Seq("col1"))),
      clusteringColumnTransforms = Seq(Some(transform)))

    val json = spec.toColumnNames

    // The serialized form should be a plain JSON array of arrays (no nested objects)
    assert(!json.contains("{"), s"Expected flat array format but got: $json")

    val roundtripped = ClusterBySpec.fromProperty(json)
    assert(roundtripped.columnNames === spec.columnNames)
    assert(roundtripped.clusteringColumnTransforms.length === 1)
    val rt = roundtripped.clusteringColumnTransforms.head.get
      .asInstanceOf[ClusteringColumnTransform]
    assert(rt.name === "variant_get")
    assert(rt.arguments().length === 3)
    assert(rt.arguments()(0).asInstanceOf[FieldReference].fieldNames().toSeq === Seq("col1"))
  }

  test("mixed plain and expression columns roundtrip through toColumnNames and fromProperty") {
    val transform = new ClusteringColumnTransform(
      "upper",
      Array(FieldReference(Seq("name"))))
    val spec = ClusterBySpec(
      columnNames = Seq(FieldReference(Seq("id")), FieldReference(Seq("name"))),
      clusteringColumnTransforms = Seq(None, Some(transform)))

    val roundtripped = ClusterBySpec.fromProperty(spec.toColumnNames)

    assert(roundtripped.columnNames === spec.columnNames)
    assert(roundtripped.clusteringColumnTransforms.length === 2)
    assert(roundtripped.clusteringColumnTransforms(0).isEmpty) // plain column
    val rt = roundtripped.clusteringColumnTransforms(1).get.asInstanceOf[ClusteringColumnTransform]
    assert(rt.name === "upper")
  }

  test("fromProperty is backward compatible with the old plain Seq[Seq[String]] format") {
    // Old format: just column name arrays
    val oldFormat = """[["id"],["data"]]"""
    val spec = ClusterBySpec.fromProperty(oldFormat)
    assert(spec.columnNames === Seq(FieldReference(Seq("id")), FieldReference(Seq("data"))))
    assert(spec.clusteringColumnTransforms.isEmpty)
  }

  test("integer literal in expression roundtrips correctly") {
    val transform = new ClusteringColumnTransform(
      "some_func",
      Array(
        FieldReference(Seq("col")),
        LiteralValue(42, IntegerType)))
    val spec = ClusterBySpec(
      columnNames = Seq(FieldReference(Seq("col"))),
      clusteringColumnTransforms = Seq(Some(transform)))

    val roundtripped = ClusterBySpec.fromProperty(spec.toColumnNames)
    assert(roundtripped.columnNames === spec.columnNames)
    val rt = roundtripped.clusteringColumnTransforms.head.get
      .asInstanceOf[ClusteringColumnTransform]
    assert(rt.name === "some_func")
    assert(rt.arguments().length === 2)
  }

  // -- string literal round-trip tests
  test("string literal values are preserved through toColumnNames/fromProperty round-trip") {
    val transform = new ClusteringColumnTransform(
      "variant_get",
      Array(
        FieldReference(Seq("col")),
        LiteralValue(UTF8String.fromString("$.foo"), StringType),
        LiteralValue(UTF8String.fromString("STRING"), StringType)))
    val spec = ClusterBySpec(
      columnNames = Seq(FieldReference(Seq("col"))),
      clusteringColumnTransforms = Seq(Some(transform)))

    val json = spec.toColumnNames
    val roundtripped = ClusterBySpec.fromProperty(json)

    val rt = roundtripped.clusteringColumnTransforms.head.get
      .asInstanceOf[ClusteringColumnTransform]
    assert(rt.name === "variant_get")
    assert(rt.arguments().length === 3)

    // Verify the string literal values and types survive the round-trip
    val lit1 = rt.arguments()(1).asInstanceOf[LiteralValue[_]]
    assert(lit1.dataType === StringType)
    assert(lit1.value.toString === "$.foo")

    val lit2 = rt.arguments()(2).asInstanceOf[LiteralValue[_]]
    assert(lit2.dataType === StringType)
    assert(lit2.value.toString === "STRING")
  }

  test("string literal with embedded single quotes round-trips correctly") {
    val transform = new ClusteringColumnTransform(
      "some_func",
      Array(
        FieldReference(Seq("col")),
        LiteralValue(UTF8String.fromString("it's a test"), StringType)))
    val spec = ClusterBySpec(
      columnNames = Seq(FieldReference(Seq("col"))),
      clusteringColumnTransforms = Seq(Some(transform)))

    val json = spec.toColumnNames
    val roundtripped = ClusterBySpec.fromProperty(json)

    val rt = roundtripped.clusteringColumnTransforms.head.get
      .asInstanceOf[ClusteringColumnTransform]
    val lit = rt.arguments()(1).asInstanceOf[LiteralValue[_]]
    assert(lit.dataType === StringType)
    assert(lit.value.toString === "it's a test")
  }

  test("string literal that looks like a column name round-trips as string, not column ref") {
    // A string value like "col_name" should survive as a StringType literal,
    // not be re-parsed as an UnresolvedAttribute (column reference).
    val transform = new ClusteringColumnTransform(
      "some_func",
      Array(
        FieldReference(Seq("col")),
        LiteralValue(UTF8String.fromString("my_column"), StringType)))
    val spec = ClusterBySpec(
      columnNames = Seq(FieldReference(Seq("col"))),
      clusteringColumnTransforms = Seq(Some(transform)))

    val json = spec.toColumnNames
    val roundtripped = ClusterBySpec.fromProperty(json)

    val rt = roundtripped.clusteringColumnTransforms.head.get
      .asInstanceOf[ClusteringColumnTransform]
    // The second argument should still be a LiteralValue with StringType,
    // not a FieldReference (which would indicate it was parsed as a column name)
    assert(rt.arguments()(1).isInstanceOf[LiteralValue[_]])
    val lit = rt.arguments()(1).asInstanceOf[LiteralValue[_]]
    assert(lit.dataType === StringType)
    assert(lit.value.toString === "my_column")
  }

  // -- timestamp literal round-trip tests
  private def testTimestampRoundTrip(microseconds: Long): Unit = {
    val transform = new ClusteringColumnTransform(
      "date_trunc",
      Array(
        FieldReference(Seq("ts_col")),
        LiteralValue(microseconds, TimestampType)))
    val spec = ClusterBySpec(
      columnNames = Seq(FieldReference(Seq("ts_col"))),
      clusteringColumnTransforms = Seq(Some(transform)))

    val json = spec.toColumnNames
    val roundtripped = ClusterBySpec.fromProperty(json)

    val rt = roundtripped.clusteringColumnTransforms.head.get
      .asInstanceOf[ClusteringColumnTransform]
    assert(rt.name === "date_trunc")
    assert(rt.arguments().length === 2)
    val lit = rt.arguments()(1).asInstanceOf[LiteralValue[_]]
    assert(lit.dataType === TimestampType)
    assert(lit.value == microseconds)
  }

  test("timestamp literal round-trips correctly") {
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      // 2023-01-01T00:00:00Z in microseconds
      testTimestampRoundTrip(1672531200000000L)
    }
  }

  test("timestamp literal round-trip is consistent with Java8 Date API enabled") {
    withSQLConf(
      SQLConf.DATETIME_JAVA8API_ENABLED.key -> "true",
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      testTimestampRoundTrip(1672531200000000L)
    }
  }

  test("timestamp literal round-trip is consistent with Java8 Date API disabled") {
    withSQLConf(
      SQLConf.DATETIME_JAVA8API_ENABLED.key -> "false",
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      testTimestampRoundTrip(1672531200000000L)
    }
  }

  test("timestamp literal round-trip with non-UTC timezone") {
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Toronto") {
      testTimestampRoundTrip(1672531200000000L)
    }
  }

  test("timestamp_ntz literal round-trips correctly") {
    val microseconds = 1672531200000000L
    val transform = new ClusteringColumnTransform(
      "date_trunc",
      Array(
        FieldReference(Seq("ts_col")),
        LiteralValue(microseconds, TimestampNTZType)))
    val spec = ClusterBySpec(
      columnNames = Seq(FieldReference(Seq("ts_col"))),
      clusteringColumnTransforms = Seq(Some(transform)))

    val json = spec.toColumnNames
    val roundtripped = ClusterBySpec.fromProperty(json)

    val rt = roundtripped.clusteringColumnTransforms.head.get
      .asInstanceOf[ClusteringColumnTransform]
    assert(rt.arguments().length === 2)
    val lit = rt.arguments()(1).asInstanceOf[LiteralValue[_]]
    assert(lit.dataType === TimestampNTZType)
    assert(lit.value == microseconds)
  }
}
