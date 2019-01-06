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

import java.util.Locale

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Cast, UpCast}
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LeafNode, LogicalPlan, Project}
import org.apache.spark.sql.types.{DoubleType, FloatType, StructField, StructType}

case class TestRelation(output: Seq[AttributeReference]) extends LeafNode with NamedRelation {
  override def name: String = "table-name"
}

class DataSourceV2AnalysisSuite extends AnalysisTest {
  val table = TestRelation(StructType(Seq(
    StructField("x", FloatType),
    StructField("y", FloatType))).toAttributes)

  val requiredTable = TestRelation(StructType(Seq(
    StructField("x", FloatType, nullable = false),
    StructField("y", FloatType, nullable = false))).toAttributes)

  val widerTable = TestRelation(StructType(Seq(
    StructField("x", DoubleType),
    StructField("y", DoubleType))).toAttributes)

  test("Append.byName: basic behavior") {
    val query = TestRelation(table.schema.toAttributes)

    val parsedPlan = AppendData.byName(table, query)

    checkAnalysis(parsedPlan, parsedPlan)
    assertResolved(parsedPlan)
  }

  test("Append.byName: does not match by position") {
    val query = TestRelation(StructType(Seq(
      StructField("a", FloatType),
      StructField("b", FloatType))).toAttributes)

    val parsedPlan = AppendData.byName(table, query)

    assertNotResolved(parsedPlan)
    assertAnalysisError(parsedPlan, Seq(
      "Cannot write incompatible data to table", "'table-name'",
      "Cannot find data for output column", "'x'", "'y'"))
  }

  test("Append.byName: case sensitive column resolution") {
    val query = TestRelation(StructType(Seq(
      StructField("X", FloatType), // doesn't match case!
      StructField("y", FloatType))).toAttributes)

    val parsedPlan = AppendData.byName(table, query)

    assertNotResolved(parsedPlan)
    assertAnalysisError(parsedPlan, Seq(
      "Cannot write incompatible data to table", "'table-name'",
      "Cannot find data for output column", "'x'"),
      caseSensitive = true)
  }

  test("Append.byName: case insensitive column resolution") {
    val query = TestRelation(StructType(Seq(
      StructField("X", FloatType), // doesn't match case!
      StructField("y", FloatType))).toAttributes)

    val X = query.output.head
    val y = query.output.last

    val parsedPlan = AppendData.byName(table, query)
    val expectedPlan = AppendData.byName(table,
      Project(Seq(
        Alias(Cast(toLower(X), FloatType, Some(conf.sessionLocalTimeZone)), "x")(),
        Alias(Cast(y, FloatType, Some(conf.sessionLocalTimeZone)), "y")()),
        query))

    assertNotResolved(parsedPlan)
    checkAnalysis(parsedPlan, expectedPlan, caseSensitive = false)
    assertResolved(expectedPlan)
  }

  test("Append.byName: data columns are reordered by name") {
    // out of order
    val query = TestRelation(StructType(Seq(
      StructField("y", FloatType),
      StructField("x", FloatType))).toAttributes)

    val y = query.output.head
    val x = query.output.last

    val parsedPlan = AppendData.byName(table, query)
    val expectedPlan = AppendData.byName(table,
      Project(Seq(
        Alias(Cast(x, FloatType, Some(conf.sessionLocalTimeZone)), "x")(),
        Alias(Cast(y, FloatType, Some(conf.sessionLocalTimeZone)), "y")()),
        query))

    assertNotResolved(parsedPlan)
    checkAnalysis(parsedPlan, expectedPlan)
    assertResolved(expectedPlan)
  }

  test("Append.byName: fail nullable data written to required columns") {
    val parsedPlan = AppendData.byName(requiredTable, table)
    assertNotResolved(parsedPlan)
    assertAnalysisError(parsedPlan, Seq(
      "Cannot write incompatible data to table", "'table-name'",
      "Cannot write nullable values to non-null column", "'x'", "'y'"))
  }

  test("Append.byName: allow required data written to nullable columns") {
    val parsedPlan = AppendData.byName(table, requiredTable)
    assertResolved(parsedPlan)
    checkAnalysis(parsedPlan, parsedPlan)
  }

  test("Append.byName: missing required columns cause failure and are identified by name") {
    // missing required field x
    val query = TestRelation(StructType(Seq(
      StructField("y", FloatType, nullable = false))).toAttributes)

    val parsedPlan = AppendData.byName(requiredTable, query)

    assertNotResolved(parsedPlan)
    assertAnalysisError(parsedPlan, Seq(
      "Cannot write incompatible data to table", "'table-name'",
      "Cannot find data for output column", "'x'"))
  }

  test("Append.byName: missing optional columns cause failure and are identified by name") {
    // missing optional field x
    val query = TestRelation(StructType(Seq(
      StructField("y", FloatType))).toAttributes)

    val parsedPlan = AppendData.byName(table, query)

    assertNotResolved(parsedPlan)
    assertAnalysisError(parsedPlan, Seq(
      "Cannot write incompatible data to table", "'table-name'",
      "Cannot find data for output column", "'x'"))
  }

  test("Append.byName: fail canWrite check") {
    val parsedPlan = AppendData.byName(table, widerTable)

    assertNotResolved(parsedPlan)
    assertAnalysisError(parsedPlan, Seq(
      "Cannot write", "'table-name'",
      "Cannot safely cast", "'x'", "'y'", "DoubleType to FloatType"))
  }

  test("Append.byName: insert safe cast") {
    val x = table.output.head
    val y = table.output.last

    val parsedPlan = AppendData.byName(widerTable, table)
    val expectedPlan = AppendData.byName(widerTable,
      Project(Seq(
        Alias(Cast(x, DoubleType, Some(conf.sessionLocalTimeZone)), "x")(),
        Alias(Cast(y, DoubleType, Some(conf.sessionLocalTimeZone)), "y")()),
        table))

    assertNotResolved(parsedPlan)
    checkAnalysis(parsedPlan, expectedPlan)
    assertResolved(expectedPlan)
  }

  test("Append.byName: fail extra data fields") {
    val query = TestRelation(StructType(Seq(
      StructField("x", FloatType),
      StructField("y", FloatType),
      StructField("z", FloatType))).toAttributes)

    val parsedPlan = AppendData.byName(table, query)

    assertNotResolved(parsedPlan)
    assertAnalysisError(parsedPlan, Seq(
      "Cannot write", "'table-name'", "too many data columns",
      "Table columns: 'x', 'y'",
      "Data columns: 'x', 'y', 'z'"))
  }

  test("Append.byName: multiple field errors are reported") {
    val xRequiredTable = TestRelation(StructType(Seq(
      StructField("x", FloatType, nullable = false),
      StructField("y", DoubleType))).toAttributes)

    val query = TestRelation(StructType(Seq(
      StructField("x", DoubleType),
      StructField("b", FloatType))).toAttributes)

    val parsedPlan = AppendData.byName(xRequiredTable, query)

    assertNotResolved(parsedPlan)
    assertAnalysisError(parsedPlan, Seq(
      "Cannot write incompatible data to table", "'table-name'",
      "Cannot safely cast", "'x'", "DoubleType to FloatType",
      "Cannot write nullable values to non-null column", "'x'",
      "Cannot find data for output column", "'y'"))
  }

  test("Append.byPosition: basic behavior") {
    val query = TestRelation(StructType(Seq(
      StructField("a", FloatType),
      StructField("b", FloatType))).toAttributes)

    val a = query.output.head
    val b = query.output.last

    val parsedPlan = AppendData.byPosition(table, query)
    val expectedPlan = AppendData.byPosition(table,
      Project(Seq(
        Alias(Cast(a, FloatType, Some(conf.sessionLocalTimeZone)), "x")(),
        Alias(Cast(b, FloatType, Some(conf.sessionLocalTimeZone)), "y")()),
        query))

    assertNotResolved(parsedPlan)
    checkAnalysis(parsedPlan, expectedPlan, caseSensitive = false)
    assertResolved(expectedPlan)
  }

  test("Append.byPosition: data columns are not reordered") {
    // out of order
    val query = TestRelation(StructType(Seq(
      StructField("y", FloatType),
      StructField("x", FloatType))).toAttributes)

    val y = query.output.head
    val x = query.output.last

    val parsedPlan = AppendData.byPosition(table, query)
    val expectedPlan = AppendData.byPosition(table,
      Project(Seq(
        Alias(Cast(y, FloatType, Some(conf.sessionLocalTimeZone)), "x")(),
        Alias(Cast(x, FloatType, Some(conf.sessionLocalTimeZone)), "y")()),
        query))

    assertNotResolved(parsedPlan)
    checkAnalysis(parsedPlan, expectedPlan)
    assertResolved(expectedPlan)
  }

  test("Append.byPosition: fail nullable data written to required columns") {
    val parsedPlan = AppendData.byPosition(requiredTable, table)
    assertNotResolved(parsedPlan)
    assertAnalysisError(parsedPlan, Seq(
      "Cannot write incompatible data to table", "'table-name'",
      "Cannot write nullable values to non-null column", "'x'", "'y'"))
  }

  test("Append.byPosition: allow required data written to nullable columns") {
    val parsedPlan = AppendData.byPosition(table, requiredTable)
    assertResolved(parsedPlan)
    checkAnalysis(parsedPlan, parsedPlan)
  }

  test("Append.byPosition: missing required columns cause failure") {
    // missing optional field x
    val query = TestRelation(StructType(Seq(
      StructField("y", FloatType, nullable = false))).toAttributes)

    val parsedPlan = AppendData.byPosition(requiredTable, query)

    assertNotResolved(parsedPlan)
    assertAnalysisError(parsedPlan, Seq(
      "Cannot write", "'table-name'", "not enough data columns",
      "Table columns: 'x', 'y'",
      "Data columns: 'y'"))
  }

  test("Append.byPosition: missing optional columns cause failure") {
    // missing optional field x
    val query = TestRelation(StructType(Seq(
      StructField("y", FloatType))).toAttributes)

    val parsedPlan = AppendData.byPosition(table, query)

    assertNotResolved(parsedPlan)
    assertAnalysisError(parsedPlan, Seq(
      "Cannot write", "'table-name'", "not enough data columns",
      "Table columns: 'x', 'y'",
      "Data columns: 'y'"))
  }

  test("Append.byPosition: fail canWrite check") {
    val widerTable = TestRelation(StructType(Seq(
      StructField("a", DoubleType),
      StructField("b", DoubleType))).toAttributes)

    val parsedPlan = AppendData.byPosition(table, widerTable)

    assertNotResolved(parsedPlan)
    assertAnalysisError(parsedPlan, Seq(
      "Cannot write", "'table-name'",
      "Cannot safely cast", "'x'", "'y'", "DoubleType to FloatType"))
  }

  test("Append.byPosition: insert safe cast") {
    val widerTable = TestRelation(StructType(Seq(
      StructField("a", DoubleType),
      StructField("b", DoubleType))).toAttributes)

    val x = table.output.head
    val y = table.output.last

    val parsedPlan = AppendData.byPosition(widerTable, table)
    val expectedPlan = AppendData.byPosition(widerTable,
      Project(Seq(
        Alias(Cast(x, DoubleType, Some(conf.sessionLocalTimeZone)), "a")(),
        Alias(Cast(y, DoubleType, Some(conf.sessionLocalTimeZone)), "b")()),
        table))

    assertNotResolved(parsedPlan)
    checkAnalysis(parsedPlan, expectedPlan)
    assertResolved(expectedPlan)
  }

  test("Append.byPosition: fail extra data fields") {
    val query = TestRelation(StructType(Seq(
      StructField("a", FloatType),
      StructField("b", FloatType),
      StructField("c", FloatType))).toAttributes)

    val parsedPlan = AppendData.byName(table, query)

    assertNotResolved(parsedPlan)
    assertAnalysisError(parsedPlan, Seq(
      "Cannot write", "'table-name'", "too many data columns",
      "Table columns: 'x', 'y'",
      "Data columns: 'a', 'b', 'c'"))
  }

  test("Append.byPosition: multiple field errors are reported") {
    val xRequiredTable = TestRelation(StructType(Seq(
      StructField("x", FloatType, nullable = false),
      StructField("y", DoubleType))).toAttributes)

    val query = TestRelation(StructType(Seq(
      StructField("x", DoubleType),
      StructField("b", FloatType))).toAttributes)

    val parsedPlan = AppendData.byPosition(xRequiredTable, query)

    assertNotResolved(parsedPlan)
    assertAnalysisError(parsedPlan, Seq(
      "Cannot write incompatible data to table", "'table-name'",
      "Cannot write nullable values to non-null column", "'x'",
      "Cannot safely cast", "'x'", "DoubleType to FloatType"))
  }

  def assertNotResolved(logicalPlan: LogicalPlan): Unit = {
    assert(!logicalPlan.resolved, s"Plan should not be resolved: $logicalPlan")
  }

  def assertResolved(logicalPlan: LogicalPlan): Unit = {
    assert(logicalPlan.resolved, s"Plan should be resolved: $logicalPlan")
  }

  def toLower(attr: AttributeReference): AttributeReference = {
    AttributeReference(attr.name.toLowerCase(Locale.ROOT), attr.dataType)(attr.exprId)
  }
}
