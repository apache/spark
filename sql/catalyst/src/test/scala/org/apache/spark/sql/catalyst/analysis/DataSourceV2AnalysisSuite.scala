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

import java.net.URI
import java.util.Locale

import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.{Alias, AnsiCast, AttributeReference, Cast, LessThanOrEqual, Literal}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.StoreAssignmentPolicy
import org.apache.spark.sql.types._

class V2AppendDataANSIAnalysisSuite extends DataSourceV2ANSIAnalysisSuite {
  override def byName(table: NamedRelation, query: LogicalPlan): LogicalPlan = {
    AppendData.byName(table, query)
  }

  override def byPosition(table: NamedRelation, query: LogicalPlan): LogicalPlan = {
    AppendData.byPosition(table, query)
  }
}

class V2AppendDataStrictAnalysisSuite extends DataSourceV2StrictAnalysisSuite {
  override def byName(table: NamedRelation, query: LogicalPlan): LogicalPlan = {
    AppendData.byName(table, query)
  }

  override def byPosition(table: NamedRelation, query: LogicalPlan): LogicalPlan = {
    AppendData.byPosition(table, query)
  }
}

class V2OverwritePartitionsDynamicANSIAnalysisSuite extends DataSourceV2ANSIAnalysisSuite {
  override def byName(table: NamedRelation, query: LogicalPlan): LogicalPlan = {
    OverwritePartitionsDynamic.byName(table, query)
  }

  override def byPosition(table: NamedRelation, query: LogicalPlan): LogicalPlan = {
    OverwritePartitionsDynamic.byPosition(table, query)
  }
}

class V2OverwritePartitionsDynamicStrictAnalysisSuite extends DataSourceV2StrictAnalysisSuite {
  override def byName(table: NamedRelation, query: LogicalPlan): LogicalPlan = {
    OverwritePartitionsDynamic.byName(table, query)
  }

  override def byPosition(table: NamedRelation, query: LogicalPlan): LogicalPlan = {
    OverwritePartitionsDynamic.byPosition(table, query)
  }
}

class V2OverwriteByExpressionANSIAnalysisSuite extends DataSourceV2ANSIAnalysisSuite {
  override def byName(table: NamedRelation, query: LogicalPlan): LogicalPlan = {
    OverwriteByExpression.byName(table, query, Literal(true))
  }

  override def byPosition(table: NamedRelation, query: LogicalPlan): LogicalPlan = {
    OverwriteByExpression.byPosition(table, query, Literal(true))
  }

  test("delete expression is resolved using table fields") {
    testResolvedOverwriteByExpression()
  }

  test("delete expression is not resolved using query fields") {
    testNotResolvedOverwriteByExpression()
  }
}

class V2OverwriteByExpressionStrictAnalysisSuite extends DataSourceV2StrictAnalysisSuite {
  override def byName(table: NamedRelation, query: LogicalPlan): LogicalPlan = {
    OverwriteByExpression.byName(table, query, Literal(true))
  }

  override def byPosition(table: NamedRelation, query: LogicalPlan): LogicalPlan = {
    OverwriteByExpression.byPosition(table, query, Literal(true))
  }

  test("delete expression is resolved using table fields") {
    testResolvedOverwriteByExpression()
  }

  test("delete expression is not resolved using query fields") {
    testNotResolvedOverwriteByExpression()
  }
}

case class TestRelation(output: Seq[AttributeReference]) extends LeafNode with NamedRelation {
  override def name: String = "table-name"
}

case class TestRelationAcceptAnySchema(output: Seq[AttributeReference])
  extends LeafNode with NamedRelation {
  override def name: String = "test-name"
  override def skipSchemaResolution: Boolean = true
}

abstract class DataSourceV2ANSIAnalysisSuite extends DataSourceV2AnalysisBaseSuite {

  // For Ansi store assignment policy, expression `AnsiCast` is used instead of `Cast`.
  override def checkAnalysis(
      inputPlan: LogicalPlan,
      expectedPlan: LogicalPlan,
      caseSensitive: Boolean = true): Unit = {
    val expectedPlanWithAnsiCast = expectedPlan transformAllExpressions {
      case c: Cast => AnsiCast(c.child, c.dataType, c.timeZoneId)
      case other => other
    }

    withSQLConf(SQLConf.STORE_ASSIGNMENT_POLICY.key -> StoreAssignmentPolicy.ANSI.toString) {
      super.checkAnalysis(inputPlan, expectedPlanWithAnsiCast, caseSensitive)
    }
  }

  override def assertAnalysisError(
      inputPlan: LogicalPlan,
      expectedErrors: Seq[String],
      caseSensitive: Boolean = true): Unit = {
    withSQLConf(SQLConf.STORE_ASSIGNMENT_POLICY.key -> StoreAssignmentPolicy.ANSI.toString) {
      super.assertAnalysisError(inputPlan, expectedErrors, caseSensitive)
    }
  }
}

abstract class DataSourceV2StrictAnalysisSuite extends DataSourceV2AnalysisBaseSuite {
  override def checkAnalysis(
      inputPlan: LogicalPlan,
      expectedPlan: LogicalPlan,
      caseSensitive: Boolean = true): Unit = {
    withSQLConf(SQLConf.STORE_ASSIGNMENT_POLICY.key -> StoreAssignmentPolicy.STRICT.toString) {
      super.checkAnalysis(inputPlan, expectedPlan, caseSensitive)
    }
  }

  override def assertAnalysisError(
      inputPlan: LogicalPlan,
      expectedErrors: Seq[String],
      caseSensitive: Boolean = true): Unit = {
    withSQLConf(SQLConf.STORE_ASSIGNMENT_POLICY.key -> StoreAssignmentPolicy.STRICT.toString) {
      super.assertAnalysisError(inputPlan, expectedErrors, caseSensitive)
    }
  }

  test("byName: fail canWrite check") {
    val parsedPlan = byName(table, widerTable)

    assertNotResolved(parsedPlan)
    assertAnalysisError(parsedPlan, Seq(
      "Cannot write", "'table-name'",
      "Cannot safely cast", "'x'", "'y'", "double to float"))
  }

  test("byName: multiple field errors are reported") {
    val xRequiredTable = TestRelation(StructType(Seq(
      StructField("x", FloatType, nullable = false),
      StructField("y", DoubleType))).toAttributes)

    val query = TestRelation(StructType(Seq(
      StructField("x", DoubleType),
      StructField("b", FloatType))).toAttributes)

    val parsedPlan = byName(xRequiredTable, query)

    assertNotResolved(parsedPlan)
    assertAnalysisError(parsedPlan, Seq(
      "Cannot write incompatible data to table", "'table-name'",
      "Cannot safely cast", "'x'", "double to float",
      "Cannot write nullable values to non-null column", "'x'",
      "Cannot find data for output column", "'y'"))
  }


  test("byPosition: fail canWrite check") {
    val widerTable = TestRelation(StructType(Seq(
      StructField("a", DoubleType),
      StructField("b", DoubleType))).toAttributes)

    val parsedPlan = byPosition(table, widerTable)

    assertNotResolved(parsedPlan)
    assertAnalysisError(parsedPlan, Seq(
      "Cannot write", "'table-name'",
      "Cannot safely cast", "'x'", "'y'", "double to float"))
  }

  test("byPosition: multiple field errors are reported") {
    val xRequiredTable = TestRelation(StructType(Seq(
      StructField("x", FloatType, nullable = false),
      StructField("y", DoubleType))).toAttributes)

    val query = TestRelation(StructType(Seq(
      StructField("x", DoubleType),
      StructField("b", FloatType))).toAttributes)

    val parsedPlan = byPosition(xRequiredTable, query)

    assertNotResolved(parsedPlan)
    assertAnalysisError(parsedPlan, Seq(
      "Cannot write incompatible data to table", "'table-name'",
      "Cannot write nullable values to non-null column", "'x'",
      "Cannot safely cast", "'x'", "double to float"))
  }
}

abstract class DataSourceV2AnalysisBaseSuite extends AnalysisTest {

  override def getAnalyzer: Analyzer = {
    val catalog = new SessionCatalog(new InMemoryCatalog, FunctionRegistry.builtin)
    catalog.createDatabase(
      CatalogDatabase("default", "", new URI("loc"), Map.empty),
      ignoreIfExists = false)
    new Analyzer(catalog) {
      override val extendedResolutionRules = EliminateSubqueryAliases :: Nil
    }
  }

  val table = TestRelation(StructType(Seq(
    StructField("x", FloatType),
    StructField("y", FloatType))).toAttributes)

  val requiredTable = TestRelation(StructType(Seq(
    StructField("x", FloatType, nullable = false),
    StructField("y", FloatType, nullable = false))).toAttributes)

  val widerTable = TestRelation(StructType(Seq(
    StructField("x", DoubleType),
    StructField("y", DoubleType))).toAttributes)

  def byName(table: NamedRelation, query: LogicalPlan): LogicalPlan

  def byPosition(table: NamedRelation, query: LogicalPlan): LogicalPlan

  test("SPARK-33136: output resolved on complex types for V2 write commands") {
    def assertTypeCompatibility(name: String, fromType: DataType, toType: DataType): Unit = {
      val table = TestRelation(StructType(Seq(StructField("a", toType))).toAttributes)
      val query = TestRelation(StructType(Seq(StructField("a", fromType))).toAttributes)
      val parsedPlan = byName(table, query)
      assertResolved(parsedPlan)
      checkAnalysis(parsedPlan, parsedPlan)
    }

    // The major difference between `from` and `to` is that `from` is a complex type
    // with non-nullable, whereas `to` is same data type with flipping nullable.

    // nested struct type
    val fromStructType = StructType(Array(
      StructField("s", StringType),
      StructField("i_nonnull", IntegerType, nullable = false),
      StructField("st", StructType(Array(
        StructField("l", LongType),
        StructField("s_nonnull", StringType, nullable = false))))))

    val toStructType = StructType(Array(
      StructField("s", StringType),
      StructField("i_nonnull", IntegerType),
      StructField("st", StructType(Array(
        StructField("l", LongType),
        StructField("s_nonnull", StringType))))))

    assertTypeCompatibility("struct", fromStructType, toStructType)

    // array type
    assertTypeCompatibility("array", ArrayType(LongType, containsNull = false),
      ArrayType(LongType, containsNull = true))

    // array type with struct type
    val fromArrayWithStructType = ArrayType(
      StructType(Array(StructField("s", StringType, nullable = false))),
      containsNull = false)

    val toArrayWithStructType = ArrayType(
      StructType(Array(StructField("s", StringType))),
      containsNull = true)

    assertTypeCompatibility("array_struct", fromArrayWithStructType, toArrayWithStructType)

    // map type
    assertTypeCompatibility("map", MapType(IntegerType, StringType, valueContainsNull = false),
      MapType(IntegerType, StringType, valueContainsNull = true))

    // map type with struct type
    val fromMapWithStructType = MapType(
      IntegerType,
      StructType(Array(StructField("s", StringType, nullable = false))),
      valueContainsNull = false)

    val toMapWithStructType = MapType(
      IntegerType,
      StructType(Array(StructField("s", StringType))),
      valueContainsNull = true)

    assertTypeCompatibility("map_struct", fromMapWithStructType, toMapWithStructType)
  }

  test("skipSchemaResolution should still require query to be resolved") {
    val table = TestRelationAcceptAnySchema(StructType(Seq(
      StructField("a", FloatType),
      StructField("b", DoubleType))).toAttributes)
    val query = UnresolvedRelation(Seq("t"))
    val parsedPlan = byName(table, query)
    assertNotResolved(parsedPlan)
  }

  test("byName: basic behavior") {
    val query = TestRelation(table.schema.toAttributes)

    val parsedPlan = byName(table, query)

    checkAnalysis(parsedPlan, parsedPlan)
    assertResolved(parsedPlan)
  }

  test("byName: does not match by position") {
    val query = TestRelation(StructType(Seq(
      StructField("a", FloatType),
      StructField("b", FloatType))).toAttributes)

    val parsedPlan = byName(table, query)

    assertNotResolved(parsedPlan)
    assertAnalysisError(parsedPlan, Seq(
      "Cannot write incompatible data to table", "'table-name'",
      "Cannot find data for output column", "'x'", "'y'"))
  }

  test("byName: case sensitive column resolution") {
    val query = TestRelation(StructType(Seq(
      StructField("X", FloatType), // doesn't match case!
      StructField("y", FloatType))).toAttributes)

    val parsedPlan = byName(table, query)

    assertNotResolved(parsedPlan)
    assertAnalysisError(parsedPlan, Seq(
      "Cannot write incompatible data to table", "'table-name'",
      "Cannot find data for output column", "'x'"),
      caseSensitive = true)
  }

  test("byName: case insensitive column resolution") {
    val query = TestRelation(StructType(Seq(
      StructField("X", FloatType), // doesn't match case!
      StructField("y", FloatType))).toAttributes)

    val X = query.output.head
    val y = query.output.last

    val parsedPlan = byName(table, query)
    val expectedPlan = byName(table, Project(Seq(X.withName("x"), y), query))

    assertNotResolved(parsedPlan)
    checkAnalysis(parsedPlan, expectedPlan, caseSensitive = false)
    assertResolved(expectedPlan)
  }

  test("byName: data columns are reordered by name") {
    // out of order
    val query = TestRelation(StructType(Seq(
      StructField("y", FloatType),
      StructField("x", FloatType))).toAttributes)

    val y = query.output.head
    val x = query.output.last

    val parsedPlan = byName(table, query)
    val expectedPlan = byName(table, Project(Seq(x, y), query))

    assertNotResolved(parsedPlan)
    checkAnalysis(parsedPlan, expectedPlan)
    assertResolved(expectedPlan)
  }

  test("byName: fail nullable data written to required columns") {
    val parsedPlan = byName(requiredTable, table)
    assertNotResolved(parsedPlan)
    assertAnalysisError(parsedPlan, Seq(
      "Cannot write incompatible data to table", "'table-name'",
      "Cannot write nullable values to non-null column", "'x'", "'y'"))
  }

  test("byName: allow required data written to nullable columns") {
    val parsedPlan = byName(table, requiredTable)
    assertResolved(parsedPlan)
    checkAnalysis(parsedPlan, parsedPlan)
  }

  test("byName: missing required columns cause failure and are identified by name") {
    // missing required field x
    val query = TestRelation(StructType(Seq(
      StructField("y", FloatType, nullable = false))).toAttributes)

    val parsedPlan = byName(requiredTable, query)

    assertNotResolved(parsedPlan)
    assertAnalysisError(parsedPlan, Seq(
      "Cannot write incompatible data to table", "'table-name'",
      "Cannot find data for output column", "'x'"))
  }

  test("byName: missing optional columns cause failure and are identified by name") {
    // missing optional field x
    val query = TestRelation(StructType(Seq(
      StructField("y", FloatType))).toAttributes)

    val parsedPlan = byName(table, query)

    assertNotResolved(parsedPlan)
    assertAnalysisError(parsedPlan, Seq(
      "Cannot write incompatible data to table", "'table-name'",
      "Cannot find data for output column", "'x'"))
  }

  test("byName: insert safe cast") {
    val x = table.output.head
    val y = table.output.last

    val parsedPlan = byName(widerTable, table)
    val expectedPlan = byName(widerTable,
      Project(Seq(
        Alias(Cast(x, DoubleType, Some(conf.sessionLocalTimeZone)), "x")(),
        Alias(Cast(y, DoubleType, Some(conf.sessionLocalTimeZone)), "y")()),
        table))

    assertNotResolved(parsedPlan)
    checkAnalysis(parsedPlan, expectedPlan)
    assertResolved(expectedPlan)
  }

  test("byName: fail extra data fields") {
    val query = TestRelation(StructType(Seq(
      StructField("x", FloatType),
      StructField("y", FloatType),
      StructField("z", FloatType))).toAttributes)

    val parsedPlan = byName(table, query)

    assertNotResolved(parsedPlan)
    assertAnalysisError(parsedPlan, Seq(
      "Cannot write", "'table-name'", "too many data columns",
      "Table columns: 'x', 'y'",
      "Data columns: 'x', 'y', 'z'"))
  }

  test("byPosition: basic behavior") {
    val query = TestRelation(StructType(Seq(
      StructField("a", FloatType),
      StructField("b", FloatType))).toAttributes)

    val a = query.output.head
    val b = query.output.last

    val parsedPlan = byPosition(table, query)
    val expectedPlan = byPosition(table,
      Project(Seq(
        Alias(Cast(a, FloatType, Some(conf.sessionLocalTimeZone)), "x")(),
        Alias(Cast(b, FloatType, Some(conf.sessionLocalTimeZone)), "y")()),
        query))

    assertNotResolved(parsedPlan)
    checkAnalysis(parsedPlan, expectedPlan, caseSensitive = false)
    assertResolved(expectedPlan)
  }

  test("byPosition: data columns are not reordered") {
    // out of order
    val query = TestRelation(StructType(Seq(
      StructField("y", FloatType),
      StructField("x", FloatType))).toAttributes)

    val y = query.output.head
    val x = query.output.last

    val parsedPlan = byPosition(table, query)
    val expectedPlan = byPosition(table,
      Project(Seq(
        Alias(Cast(y, FloatType, Some(conf.sessionLocalTimeZone)), "x")(),
        Alias(Cast(x, FloatType, Some(conf.sessionLocalTimeZone)), "y")()),
        query))

    assertNotResolved(parsedPlan)
    checkAnalysis(parsedPlan, expectedPlan)
    assertResolved(expectedPlan)
  }

  test("byPosition: fail nullable data written to required columns") {
    val parsedPlan = byPosition(requiredTable, table)
    assertNotResolved(parsedPlan)
    assertAnalysisError(parsedPlan, Seq(
      "Cannot write incompatible data to table", "'table-name'",
      "Cannot write nullable values to non-null column", "'x'", "'y'"))
  }

  test("byPosition: allow required data written to nullable columns") {
    val parsedPlan = byPosition(table, requiredTable)
    assertResolved(parsedPlan)
    checkAnalysis(parsedPlan, parsedPlan)
  }

  test("byPosition: missing required columns cause failure") {
    // missing optional field x
    val query = TestRelation(StructType(Seq(
      StructField("y", FloatType, nullable = false))).toAttributes)

    val parsedPlan = byPosition(requiredTable, query)

    assertNotResolved(parsedPlan)
    assertAnalysisError(parsedPlan, Seq(
      "Cannot write", "'table-name'", "not enough data columns",
      "Table columns: 'x', 'y'",
      "Data columns: 'y'"))
  }

  test("byPosition: missing optional columns cause failure") {
    // missing optional field x
    val query = TestRelation(StructType(Seq(
      StructField("y", FloatType))).toAttributes)

    val parsedPlan = byPosition(table, query)

    assertNotResolved(parsedPlan)
    assertAnalysisError(parsedPlan, Seq(
      "Cannot write", "'table-name'", "not enough data columns",
      "Table columns: 'x', 'y'",
      "Data columns: 'y'"))
  }

  test("byPosition: insert safe cast") {
    val widerTable = TestRelation(StructType(Seq(
      StructField("a", DoubleType),
      StructField("b", DoubleType))).toAttributes)

    val x = table.output.head
    val y = table.output.last

    val parsedPlan = byPosition(widerTable, table)
    val expectedPlan = byPosition(widerTable,
      Project(Seq(
        Alias(Cast(x, DoubleType, Some(conf.sessionLocalTimeZone)), "a")(),
        Alias(Cast(y, DoubleType, Some(conf.sessionLocalTimeZone)), "b")()),
        table))

    assertNotResolved(parsedPlan)
    checkAnalysis(parsedPlan, expectedPlan)
    assertResolved(expectedPlan)
  }

  test("byPosition: fail extra data fields") {
    val query = TestRelation(StructType(Seq(
      StructField("a", FloatType),
      StructField("b", FloatType),
      StructField("c", FloatType))).toAttributes)

    val parsedPlan = byName(table, query)

    assertNotResolved(parsedPlan)
    assertAnalysisError(parsedPlan, Seq(
      "Cannot write", "'table-name'", "too many data columns",
      "Table columns: 'x', 'y'",
      "Data columns: 'a', 'b', 'c'"))
  }

  test("bypass output column resolution") {
    val table = TestRelationAcceptAnySchema(StructType(Seq(
      StructField("a", FloatType, nullable = false),
      StructField("b", DoubleType))).toAttributes)

    val query = TestRelation(StructType(Seq(
      StructField("s", StringType))).toAttributes)

    withClue("byName") {
      val parsedPlan = byName(table, query)
      assertResolved(parsedPlan)
      checkAnalysis(parsedPlan, parsedPlan)
    }

    withClue("byPosition") {
      val parsedPlan = byPosition(table, query)
      assertResolved(parsedPlan)
      checkAnalysis(parsedPlan, parsedPlan)
    }
  }

  test("check fields of struct type column") {
    val tableWithStructCol = TestRelation(
      new StructType().add(
        "col", new StructType().add("a", IntegerType).add("b", IntegerType)
      ).toAttributes
    )

    val query = TestRelation(
      new StructType().add(
        "col", new StructType().add("x", IntegerType).add("y", IntegerType)
      ).toAttributes
    )

    withClue("byName") {
      val parsedPlan = byName(tableWithStructCol, query)
      assertNotResolved(parsedPlan)
      assertAnalysisError(parsedPlan, Seq(
        "Cannot write incompatible data to table", "'table-name'",
        "Struct 'col' 0-th field name does not match", "expected 'a', found 'x'",
        "Struct 'col' 1-th field name does not match", "expected 'b', found 'y'"))
    }

    withClue("byPosition") {
      val parsedPlan = byPosition(tableWithStructCol, query)
      assertNotResolved(parsedPlan)

      val expectedQuery = Project(Seq(Alias(
        Cast(
          query.output.head,
          new StructType().add("a", IntegerType).add("b", IntegerType),
          Some(conf.sessionLocalTimeZone)),
        "col")()),
        query)
      checkAnalysis(parsedPlan, byPosition(tableWithStructCol, expectedQuery))
    }
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

  protected def testResolvedOverwriteByExpression(): Unit = {
    val table = TestRelation(StructType(Seq(
      StructField("x", DoubleType, nullable = false),
      StructField("y", DoubleType))).toAttributes)

    val query = TestRelation(StructType(Seq(
      StructField("a", DoubleType, nullable = false),
      StructField("b", DoubleType))).toAttributes)

    val a = query.output.head
    val b = query.output.last
    val x = table.output.head

    val parsedPlan = OverwriteByExpression.byPosition(table, query,
      LessThanOrEqual(UnresolvedAttribute(Seq("x")), Literal(15.0d)))

    val expectedPlan = OverwriteByExpression.byPosition(table,
      Project(Seq(
        Alias(Cast(a, DoubleType, Some(conf.sessionLocalTimeZone)), "x")(),
        Alias(Cast(b, DoubleType, Some(conf.sessionLocalTimeZone)), "y")()),
        query),
      LessThanOrEqual(x, Literal(15.0d)))

    assertNotResolved(parsedPlan)
    checkAnalysis(parsedPlan, expectedPlan)
    assertResolved(expectedPlan)
  }

  protected def testNotResolvedOverwriteByExpression(): Unit = {
    val table = TestRelation(StructType(Seq(
      StructField("x", DoubleType, nullable = false),
      StructField("y", DoubleType))).toAttributes)

    val query = TestRelation(StructType(Seq(
      StructField("a", DoubleType, nullable = false),
      StructField("b", DoubleType))).toAttributes)

    // the write is resolved (checked above). this test plan is not because of the expression.
    val parsedPlan = OverwriteByExpression.byPosition(table, query,
      LessThanOrEqual(UnresolvedAttribute(Seq("a")), Literal(15.0d)))

    assertNotResolved(parsedPlan)
    assertAnalysisError(parsedPlan, Seq("cannot resolve", "`a`", "given input columns", "x, y"))

    val tableAcceptAnySchema = TestRelationAcceptAnySchema(StructType(Seq(
      StructField("x", DoubleType, nullable = false),
      StructField("y", DoubleType))).toAttributes)

    val parsedPlan2 = OverwriteByExpression.byPosition(tableAcceptAnySchema, query,
      LessThanOrEqual(UnresolvedAttribute(Seq("a")), Literal(15.0d)))
    assertNotResolved(parsedPlan2)
    assertAnalysisError(parsedPlan2, Seq("cannot resolve", "`a`", "given input columns", "x, y"))
  }
}
