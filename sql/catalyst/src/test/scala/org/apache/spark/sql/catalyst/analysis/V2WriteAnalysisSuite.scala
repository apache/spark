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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Alias, ArrayTransform, AttributeReference, Cast, CreateNamedStruct, GetStructField, If, IsNull, LessThanOrEqual, Literal}
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.StoreAssignmentPolicy
import org.apache.spark.sql.types._

class V2AppendDataANSIAnalysisSuite extends V2ANSIWriteAnalysisSuiteBase {
  override def byName(table: NamedRelation, query: LogicalPlan): LogicalPlan = {
    AppendData.byName(table, query)
  }

  override def byPosition(table: NamedRelation, query: LogicalPlan): LogicalPlan = {
    AppendData.byPosition(table, query)
  }
}

class V2AppendDataStrictAnalysisSuite extends V2StrictWriteAnalysisSuiteBase {
  override def byName(table: NamedRelation, query: LogicalPlan): LogicalPlan = {
    AppendData.byName(table, query)
  }

  override def byPosition(table: NamedRelation, query: LogicalPlan): LogicalPlan = {
    AppendData.byPosition(table, query)
  }
}

class V2OverwritePartitionsDynamicANSIAnalysisSuite extends V2ANSIWriteAnalysisSuiteBase {
  override def byName(table: NamedRelation, query: LogicalPlan): LogicalPlan = {
    OverwritePartitionsDynamic.byName(table, query)
  }

  override def byPosition(table: NamedRelation, query: LogicalPlan): LogicalPlan = {
    OverwritePartitionsDynamic.byPosition(table, query)
  }
}

class V2OverwritePartitionsDynamicStrictAnalysisSuite extends V2StrictWriteAnalysisSuiteBase {
  override def byName(table: NamedRelation, query: LogicalPlan): LogicalPlan = {
    OverwritePartitionsDynamic.byName(table, query)
  }

  override def byPosition(table: NamedRelation, query: LogicalPlan): LogicalPlan = {
    OverwritePartitionsDynamic.byPosition(table, query)
  }
}

class V2OverwriteByExpressionANSIAnalysisSuite extends V2ANSIWriteAnalysisSuiteBase {
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

class V2OverwriteByExpressionStrictAnalysisSuite extends V2StrictWriteAnalysisSuiteBase {
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

object TestRelation {
  def apply(schema: StructType): TestRelation = apply(DataTypeUtils.toAttributes(schema))
}

case class TestRelationAcceptAnySchema(output: Seq[AttributeReference])
  extends LeafNode with NamedRelation {
  override def name: String = "test-name"
  override def skipSchemaResolution: Boolean = true
}

object TestRelationAcceptAnySchema {
  def apply(schema: StructType): TestRelationAcceptAnySchema =
    apply(DataTypeUtils.toAttributes(schema))
}

abstract class V2ANSIWriteAnalysisSuiteBase extends V2WriteAnalysisSuiteBase {

  // For Ansi store assignment policy, expression `AnsiCast` is used instead of `Cast`.
  override def checkAnalysis(
      inputPlan: LogicalPlan,
      expectedPlan: LogicalPlan,
      caseSensitive: Boolean = true): Unit = {
    val expectedPlanWithAnsiCast = expectedPlan transformAllExpressions {
      case c: Cast =>
        val cast = Cast(c.child, c.dataType, c.timeZoneId, ansiEnabled = true)
        cast.setTagValue(Cast.BY_TABLE_INSERTION, ())
        cast
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

  override def assertAnalysisErrorCondition(
      inputPlan: LogicalPlan,
      expectedErrorCondition: String,
      expectedMessageParameters: Map[String, String],
      queryContext: Array[ExpectedContext] = Array.empty,
      caseSensitive: Boolean = true): Unit = {
    withSQLConf(SQLConf.STORE_ASSIGNMENT_POLICY.key -> StoreAssignmentPolicy.ANSI.toString) {
      super.assertAnalysisErrorCondition(
        inputPlan,
        expectedErrorCondition,
        expectedMessageParameters,
        queryContext,
        caseSensitive
      )
    }
  }
}

abstract class V2StrictWriteAnalysisSuiteBase extends V2WriteAnalysisSuiteBase {
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

  override def assertAnalysisErrorCondition(
      inputPlan: LogicalPlan,
      expectedErrorCondition: String,
      expectedMessageParameters: Map[String, String],
      queryContext: Array[ExpectedContext] = Array.empty,
      caseSensitive: Boolean = true): Unit = {
    withSQLConf(SQLConf.STORE_ASSIGNMENT_POLICY.key -> StoreAssignmentPolicy.STRICT.toString) {
      super.assertAnalysisErrorCondition(
        inputPlan,
        expectedErrorCondition,
        expectedMessageParameters,
        queryContext,
        caseSensitive
      )
    }
  }

  test("byName: fail canWrite check") {
    val parsedPlan = byName(table, widerTable)

    assertNotResolved(parsedPlan)
    assertAnalysisErrorCondition(
      parsedPlan,
      expectedErrorCondition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST",
      expectedMessageParameters = Map(
        "tableName" -> "`table-name`",
        "colName" -> "`x`",
        "srcType" -> "\"DOUBLE\"",
        "targetType" -> "\"FLOAT\"")
    )
  }

  test("byName: multiple field errors are reported") {
    val xRequiredTable = TestRelation(StructType(Seq(
      StructField("x", FloatType, nullable = false),
      StructField("y", DoubleType))))

    val query = TestRelation(StructType(Seq(
      StructField("x", DoubleType),
      StructField("b", FloatType))))

    val parsedPlan = byName(xRequiredTable, query)

    assertNotResolved(parsedPlan)
    assertAnalysisErrorCondition(
      parsedPlan,
      expectedErrorCondition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST",
      expectedMessageParameters = Map(
        "tableName" -> "`table-name`",
        "colName" -> "`x`",
        "srcType" -> "\"DOUBLE\"",
        "targetType" -> "\"FLOAT\"")
    )
  }

  test("byPosition: fail canWrite check") {
    val widerTable = TestRelation(StructType(Seq(
      StructField("a", DoubleType),
      StructField("b", DoubleType))))

    val parsedPlan = byPosition(table, widerTable)

    assertNotResolved(parsedPlan)
    assertAnalysisErrorCondition(
      parsedPlan,
      expectedErrorCondition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST",
      expectedMessageParameters = Map(
        "tableName" -> "`table-name`",
        "colName" -> "`x`",
        "srcType" -> "\"DOUBLE\"",
        "targetType" -> "\"FLOAT\"")
    )
  }

  test("byPosition: multiple field errors are reported") {
    val xRequiredTable = TestRelation(StructType(Seq(
      StructField("x", FloatType, nullable = false),
      StructField("y", FloatType))))

    val query = TestRelation(StructType(Seq(
      StructField("x", DoubleType),
      StructField("b", DoubleType))))

    val parsedPlan = byPosition(xRequiredTable, query)

    assertNotResolved(parsedPlan)
    assertAnalysisErrorCondition(
      parsedPlan,
      expectedErrorCondition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST",
      expectedMessageParameters = Map(
        "tableName" -> "`table-name`",
        "colName" -> "`x`",
        "srcType" -> "\"DOUBLE\"",
        "targetType" -> "\"FLOAT\"")
    )
  }
}

abstract class V2WriteAnalysisSuiteBase extends AnalysisTest {

  override def extendedAnalysisRules: Seq[Rule[LogicalPlan]] = Seq(EliminateSubqueryAliases)

  val table = TestRelation(Seq($"x".float, $"y".float))

  val requiredTable = TestRelation(Seq($"x".float.notNull, $"y".float.notNull))

  val widerTable = TestRelation(Seq($"x".double, $"y".double))

  def byName(table: NamedRelation, query: LogicalPlan): LogicalPlan

  def byPosition(table: NamedRelation, query: LogicalPlan): LogicalPlan

  test("SPARK-49352: Avoid redundant array transform for identical expression") {
    def assertArrayField(fromType: ArrayType, toType: ArrayType, hasTransform: Boolean): Unit = {
      val table = TestRelation(Seq($"a".int, $"arr".array(toType)))
      val query = TestRelation(Seq($"arr".array(fromType), $"a".int))

      val writePlan = byName(table, query).analyze

      assertResolved(writePlan)
      checkAnalysis(writePlan, writePlan)

      val transform = writePlan.children.head.expressions.exists { e =>
        e.find {
          case _: ArrayTransform => true
          case _ => false
        }.isDefined
      }
      if (hasTransform) {
        assert(transform)
      } else {
        assert(!transform)
      }
    }

    assertArrayField(ArrayType(LongType), ArrayType(LongType), hasTransform = false)
    assertArrayField(
      ArrayType(new StructType().add("x", "int").add("y", "int")),
      ArrayType(new StructType().add("y", "int").add("x", "byte")),
      hasTransform = true)
  }

  test("SPARK-48922: Avoid redundant array transform of identical expression for map type") {
    def assertMapField(fromType: MapType, toType: MapType, transformNum: Int): Unit = {
      val table = TestRelation(Seq($"a".int, Symbol("map").map(toType)))
      val query = TestRelation(Seq(Symbol("map").map(fromType), $"a".int))

      val writePlan = byName(table, query).analyze

      assertResolved(writePlan)
      checkAnalysis(writePlan, writePlan)

      val transforms = writePlan.children.head.expressions.flatMap { e =>
        e.flatMap {
          case t: ArrayTransform => Some(t)
          case _ => None
        }
      }
      assert(transforms.size == transformNum)
    }

    assertMapField(MapType(LongType, StringType), MapType(LongType, StringType), 0)
    assertMapField(
      MapType(LongType, new StructType().add("x", "int").add("y", "int")),
      MapType(LongType, new StructType().add("y", "int").add("x", "byte")),
      1)
    assertMapField(MapType(LongType, LongType), MapType(IntegerType, LongType), 1)
    assertMapField(
      MapType(LongType, new StructType().add("x", "int").add("y", "int")),
      MapType(IntegerType, new StructType().add("y", "int").add("x", "byte")),
      2)
  }

  test("SPARK-33136: output resolved on complex types for V2 write commands") {
    def assertTypeCompatibility(name: String, fromType: DataType, toType: DataType): Unit = {
      val table = TestRelation(StructType(Seq(StructField("a", toType))))
      val query = TestRelation(StructType(Seq(StructField("a", fromType))))
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
      StructField("b", DoubleType))))
    val query = UnresolvedRelation(Seq("t"))
    val parsedPlan = byName(table, query)
    assertNotResolved(parsedPlan)
  }

  test("byName: basic behavior") {
    val query = TestRelation(table.schema)

    val parsedPlan = byName(table, query)

    checkAnalysis(parsedPlan, parsedPlan)
    assertResolved(parsedPlan)
  }

  test("byName: does not match by position") {
    val query = TestRelation(StructType(Seq(
      StructField("a", FloatType),
      StructField("b", FloatType))))

    val parsedPlan = byName(table, query)

    withSQLConf(SQLConf.USE_NULLS_FOR_MISSING_DEFAULT_COLUMN_VALUES.key -> "false") {
      assertNotResolved(parsedPlan)
      assertAnalysisErrorCondition(
        parsedPlan,
        expectedErrorCondition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA",
        expectedMessageParameters = Map("tableName" -> "`table-name`", "colName" -> "`x`")
      )
    }
  }

  test("byName: case sensitive column resolution") {
    val query = TestRelation(StructType(Seq(
      StructField("X", FloatType), // doesn't match case!
      StructField("y", FloatType))))

    val parsedPlan = byName(table, query)

    withSQLConf(SQLConf.USE_NULLS_FOR_MISSING_DEFAULT_COLUMN_VALUES.key -> "false") {
      assertNotResolved(parsedPlan)
      assertAnalysisErrorCondition(
        parsedPlan,
        expectedErrorCondition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA",
        expectedMessageParameters = Map("tableName" -> "`table-name`", "colName" -> "`x`")
      )
    }
  }

  test("byName: case insensitive column resolution") {
    val query = TestRelation(StructType(Seq(
      StructField("X", FloatType), // doesn't match case!
      StructField("y", FloatType))))

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
      StructField("x", FloatType))))

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

    val analyzed = parsedPlan.analyze
    assertNullCheckExists(analyzed, Seq("x"))
    assertNullCheckExists(analyzed, Seq("y"))
  }

  test("byName: allow required data written to nullable columns") {
    val parsedPlan = byName(table, requiredTable)
    assertResolved(parsedPlan)
    checkAnalysis(parsedPlan, parsedPlan)
  }

  test("byName: missing required columns cause failure and are identified by name") {
    // missing required field x
    val query = TestRelation(StructType(Seq(
      StructField("y", FloatType, nullable = false))))

    val parsedPlan = byName(requiredTable, query)

    assertNotResolved(parsedPlan)
    assertAnalysisErrorCondition(
      parsedPlan,
      expectedErrorCondition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA",
      expectedMessageParameters = Map("tableName" -> "`table-name`", "colName" -> "`x`")
    )
  }

  test("byName: missing optional columns cause failure and are identified by name") {
    // missing optional field x
    val query = TestRelation(StructType(Seq(
      StructField("y", FloatType))))

    val parsedPlan = byName(table, query)

    withSQLConf(SQLConf.USE_NULLS_FOR_MISSING_DEFAULT_COLUMN_VALUES.key -> "false") {
      assertNotResolved(parsedPlan)
      assertAnalysisErrorCondition(
        parsedPlan,
        expectedErrorCondition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA",
        expectedMessageParameters = Map("tableName" -> "`table-name`", "colName" -> "`x`")
      )
    }
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
      StructField("z", FloatType))))

    val parsedPlan = byName(table, query)

    assertNotResolved(parsedPlan)
    assertAnalysisErrorCondition(
      inputPlan = parsedPlan,
      expectedErrorCondition = "INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS",
      expectedMessageParameters = Map(
        "tableName" -> "`table-name`",
        "tableColumns" -> "`x`, `y`",
        "dataColumns" -> "`x`, `y`, `z`")
    )
  }

  test("byName: fail extra data fields in struct") {
    val table = TestRelation(Seq($"a".int, $"b".struct($"x".int, $"y".int)))
    val query = TestRelation(Seq($"b".struct($"y".int, $"x".int, $"z".int), $"a".int))

    val writePlan = byName(table, query)
    assertAnalysisErrorCondition(
      writePlan,
      expectedErrorCondition = "INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_STRUCT_FIELDS",
      expectedMessageParameters = Map(
        "tableName" -> "`table-name`",
        "colName" -> "`b`",
        "extraFields" -> "`z`"
      )
    )
  }

  test("byPosition: basic behavior") {
    val query = TestRelation(StructType(Seq(
      StructField("a", FloatType),
      StructField("b", FloatType))))

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
      StructField("x", FloatType))))

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

    val analyzed = parsedPlan.analyze
    assertNullCheckExists(analyzed, Seq("x"))
    assertNullCheckExists(analyzed, Seq("y"))
  }

  test("byPosition: allow required data written to nullable columns") {
    val parsedPlan = byPosition(table, requiredTable)
    assertResolved(parsedPlan)
    checkAnalysis(parsedPlan, parsedPlan)
  }

  test("byPosition: missing required columns cause failure") {
    // missing optional field x
    val query = TestRelation(StructType(Seq(
      StructField("y", FloatType, nullable = false))))

    val parsedPlan = byPosition(requiredTable, query)

    assertNotResolved(parsedPlan)
    assertAnalysisErrorCondition(
      inputPlan = parsedPlan,
      expectedErrorCondition = "INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS",
      expectedMessageParameters = Map(
        "tableName" -> "`table-name`",
        "tableColumns" -> "`x`, `y`",
        "dataColumns" -> "`y`")
    )
  }

  test("byPosition: missing optional columns cause failure") {
    // missing optional field x
    val query = TestRelation(StructType(Seq(
      StructField("y", FloatType))))

    val parsedPlan = byPosition(table, query)

    assertNotResolved(parsedPlan)
    assertAnalysisErrorCondition(
      inputPlan = parsedPlan,
      expectedErrorCondition = "INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS",
      expectedMessageParameters = Map(
        "tableName" -> "`table-name`",
        "tableColumns" -> "`x`, `y`",
        "dataColumns" -> "`y`")
    )
  }

  test("byPosition: insert safe cast") {
    val widerTable = TestRelation(StructType(Seq(
      StructField("a", DoubleType),
      StructField("b", DoubleType))))

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
      StructField("c", FloatType))))

    val parsedPlan = byName(table, query)

    assertNotResolved(parsedPlan)
    assertAnalysisErrorCondition(
      inputPlan = parsedPlan,
      expectedErrorCondition = "INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS",
      expectedMessageParameters = Map(
        "tableName" -> "`table-name`",
        "tableColumns" -> "`x`, `y`",
        "dataColumns" -> "`a`, `b`, `c`")
    )
  }

  test("bypass output column resolution") {
    val table = TestRelationAcceptAnySchema(StructType(Seq(
      StructField("a", FloatType, nullable = false),
      StructField("b", DoubleType))))

    val query = TestRelation(StructType(Seq(
      StructField("s", StringType))))

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
      )
    )

    val query = TestRelation(
      new StructType().add(
        "col", new StructType().add("x", IntegerType).add("y", IntegerType)
      )
    )

    withClue("byName") {
      val parsedPlan = byName(tableWithStructCol, query)
      assertNotResolved(parsedPlan)
      assertAnalysisErrorCondition(
        parsedPlan,
        expectedErrorCondition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA",
        expectedMessageParameters = Map("tableName" -> "`table-name`", "colName" -> "`col`.`a`")
      )
    }

    withClue("byPosition") {
      val parsedPlan = byPosition(tableWithStructCol, query)
      assertNotResolved(parsedPlan)

      val queryCol = query.output.head
      val expectedColType = tableWithStructCol.schema("col").dataType

      val expectedQuery = Project(Seq(Alias(
        If(
          IsNull(queryCol),
          Literal(null, expectedColType),
          CreateNamedStruct(Seq(
            Literal("a"), Cast(
              GetStructField(queryCol, 0, name = Some("x")),
              IntegerType,
              Some(conf.sessionLocalTimeZone)),
            Literal("b"), Cast(
              GetStructField(queryCol, 1, name = Some("y")),
              IntegerType,
              Some(conf.sessionLocalTimeZone))))),
        "col")()),
        query)
      checkAnalysis(parsedPlan, byPosition(tableWithStructCol, expectedQuery))
    }
  }

  test("SPARK-42997: extra fields in nested struct (byName)") {
    checkExtraFieldsInNestedStruct(byNameResolution = true)
  }

  test("SPARK-42997: extra fields in nested struct (byPosition)") {
    checkExtraFieldsInNestedStruct(byNameResolution = false)
  }

  private def checkExtraFieldsInNestedStruct(byNameResolution: Boolean): Unit = {
    val table = TestRelation(Seq(
      $"a".int,
      $"b".struct($"n1".int, $"n2".struct($"dn1".int, $"dn2".int))))
    val query = TestRelation(Seq(
      $"a".int,
      $"b".struct($"n1".int, $"n2".struct($"dn1".int, $"dn2".int, $"dn3".int))))

    val parsedPlan = if (byNameResolution) byName(table, query) else byPosition(table, query)

    assertNotResolved(parsedPlan)
    assertAnalysisErrorCondition(
      parsedPlan,
      expectedErrorCondition = "INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_STRUCT_FIELDS",
      expectedMessageParameters = Map(
        "tableName" -> "`table-name`",
        "colName" -> "`b`.`n2`",
        "extraFields" -> "`dn3`")
    )
  }

  test("SPARK-42997: extra fields in struct inside array (byName)") {
    checkExtraFieldsInStructInsideArray(byNameResolution = true)
  }

  test("SPARK-42997: extra fields in struct inside array (byPosition)") {
    checkExtraFieldsInStructInsideArray(byNameResolution = false)
  }

  private def checkExtraFieldsInStructInsideArray(byNameResolution: Boolean): Unit = {
    val table = TestRelation(Seq(
      $"a".int,
      $"arr".array(new StructType().add("x", "int").add("y", "int"))))
    val query = TestRelation(Seq(
      $"a".int,
      $"arr".array(new StructType().add("x", "int").add("y", "int").add("z", "int"))))

    val parsedPlan = if (byNameResolution) byName(table, query) else byPosition(table, query)

    assertNotResolved(parsedPlan)
    assertAnalysisErrorCondition(
      parsedPlan,
      expectedErrorCondition = "INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_STRUCT_FIELDS",
      expectedMessageParameters = Map(
        "tableName" -> "`table-name`",
        "colName" -> "`arr`.`element`",
        "extraFields" -> "`z`")
    )
  }

  test("SPARK-42997: extra fields in struct inside map key (byName)") {
    checkExtraFieldsInStructInsideMapKey(byNameResolution = true)
  }

  test("SPARK-42997: extra fields in struct inside map key (byPosition)") {
    checkExtraFieldsInStructInsideMapKey(byNameResolution = false)
  }

  private def checkExtraFieldsInStructInsideMapKey(byNameResolution: Boolean): Unit = {
    val table = TestRelation(Seq(
      $"a".int,
      Symbol("m").map(
        new StructType().add("x", "int").add("y", "int"),
        new StructType().add("x", "int").add("y", "int"))))
    val query = TestRelation(Seq(
      $"a".int,
      Symbol("m").map(
        new StructType().add("x", "int").add("y", "int").add("z", "int"),
        new StructType().add("x", "int").add("y", "int"))))

    val parsedPlan = if (byNameResolution) byName(table, query) else byPosition(table, query)

    assertNotResolved(parsedPlan)
    assertAnalysisErrorCondition(
      parsedPlan,
      expectedErrorCondition = "INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_STRUCT_FIELDS",
      expectedMessageParameters = Map(
        "tableName" -> "`table-name`",
        "colName" -> "`m`.`key`",
        "extraFields" -> "`z`")
    )
  }

  test("SPARK-42997: extra fields in struct inside map value (byName)") {
    checkExtraFieldsInStructInsideMapValue(byNameResolution = true)
  }

  test("SPARK-42997: extra fields in struct inside map value (byPosition)") {
    checkExtraFieldsInStructInsideMapValue(byNameResolution = false)
  }

  private def checkExtraFieldsInStructInsideMapValue(byNameResolution: Boolean): Unit = {
    val table = TestRelation(Seq(
      $"a".int,
      Symbol("m").map(
        new StructType().add("x", "int").add("y", "int"),
        new StructType().add("x", "int").add("y", "int"))))
    val query = TestRelation(Seq(
      $"a".int,
      Symbol("m").map(
        new StructType().add("x", "int").add("y", "int"),
        new StructType().add("x", "int").add("y", "int").add("z", "int"))))

    val parsedPlan = if (byNameResolution) byName(table, query) else byPosition(table, query)

    assertNotResolved(parsedPlan)
    assertAnalysisErrorCondition(
      parsedPlan,
      expectedErrorCondition = "INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_STRUCT_FIELDS",
      expectedMessageParameters = Map(
        "tableName" -> "`table-name`",
        "colName" -> "`m`.`value`",
        "extraFields" -> "`z`")
    )
  }

  test("SPARK-42997: missing fields in nested struct (byName)") {
    checkMissingFieldsInNestedStruct(byNameResolution = true)
  }

  test("SPARK-42997: missing fields in nested struct (byPosition)") {
    checkMissingFieldsInNestedStruct(byNameResolution = false)
  }

  private def checkMissingFieldsInNestedStruct(byNameResolution: Boolean): Unit = {
    val table = TestRelation(Seq(
      $"a".int,
      $"b".struct($"n1".int, $"n2".struct($"dn1".int, $"dn2".int, $"dn3".int))))
    val query = TestRelation(Seq(
      $"a".int,
      $"b".struct($"n1".int, $"n2".struct($"dn1".int, $"dn2".int))))

    val (parsedPlan, expectedErrMsg) = if (byNameResolution) {
      byName(table, query) -> "Cannot find data for output column 'b.n2.dn3'"
    } else {
      byPosition(table, query) -> "Struct 'b.n2' missing fields: 'dn3'"
    }

    assertNotResolved(parsedPlan)
    if (byNameResolution) {
      assertAnalysisErrorCondition(
        parsedPlan,
        expectedErrorCondition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA",
        expectedMessageParameters = Map(
          "tableName" -> "`table-name`",
          "colName" -> "`b`.`n2`.`dn3`")
      )
    } else {
      assertAnalysisErrorCondition(
        parsedPlan,
        expectedErrorCondition = "INCOMPATIBLE_DATA_FOR_TABLE.STRUCT_MISSING_FIELDS",
        expectedMessageParameters = Map(
          "tableName" -> "`table-name`",
          "colName" -> "`b`.`n2`",
          "missingFields" -> "`dn3`")
      )
    }
  }

  test("SPARK-42997: missing fields in struct inside array (byName)") {
    checkMissingFieldsInStructInsideArray(byNameResolution = true)
  }

  test("SPARK-42997: missing fields in struct inside array (byPosition)") {
    checkMissingFieldsInStructInsideArray(byNameResolution = false)
  }

  private def checkMissingFieldsInStructInsideArray(byNameResolution: Boolean): Unit = {
    val table = TestRelation(Seq(
      $"a".int,
      $"arr".array(new StructType().add("x", "int").add("y", "int"))))
    val query = TestRelation(Seq(
      $"a".int,
      $"arr".array(new StructType().add("x", "int"))))

    val (parsedPlan, expectedErrMsg) = if (byNameResolution) {
      byName(table, query) -> "Cannot find data for output column 'arr.element.y'"
    } else {
      byPosition(table, query) -> "Struct 'arr.element' missing fields: 'y'"
    }

    assertNotResolved(parsedPlan)
    if (byNameResolution) {
      assertAnalysisErrorCondition(
        parsedPlan,
        expectedErrorCondition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA",
        expectedMessageParameters = Map(
          "tableName" -> "`table-name`",
          "colName" -> "`arr`.`element`.`y`")
      )
    } else {
      assertAnalysisErrorCondition(
        parsedPlan,
        expectedErrorCondition = "INCOMPATIBLE_DATA_FOR_TABLE.STRUCT_MISSING_FIELDS",
        expectedMessageParameters = Map(
          "tableName" -> "`table-name`",
          "colName" -> "`arr`.`element`",
          "missingFields" -> "`y`")
      )
    }
  }

  test("SPARK-42997: missing fields in struct inside map key (byName)") {
    checkMissingFieldsInStructInsideMapKey(byNameResolution = true)
  }

  test("SPARK-42997: missing fields in struct inside map key (byPosition)") {
    checkMissingFieldsInStructInsideMapKey(byNameResolution = false)
  }

  private def checkMissingFieldsInStructInsideMapKey(byNameResolution: Boolean): Unit = {
    val table = TestRelation(Seq(
      $"a".int,
      Symbol("m").map(
        new StructType().add("x", "int").add("y", "int"),
        new StructType().add("x", "int").add("y", "int"))))
    val query = TestRelation(Seq(
      $"a".int,
      Symbol("m").map(
        new StructType().add("x", "int"),
        new StructType().add("x", "int").add("y", "int"))))

    val (parsedPlan, expectedErrMsg) = if (byNameResolution) {
      byName(table, query) -> "Cannot find data for output column 'm.key.y'"
    } else {
      byPosition(table, query) -> "Struct 'm.key' missing fields: 'y'"
    }

    assertNotResolved(parsedPlan)
    if (byNameResolution) {
      assertAnalysisErrorCondition(
        parsedPlan,
        expectedErrorCondition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA",
        expectedMessageParameters = Map(
          "tableName" -> "`table-name`",
          "colName" -> "`m`.`key`.`y`")
      )
    } else {
      assertAnalysisErrorCondition(
        parsedPlan,
        expectedErrorCondition = "INCOMPATIBLE_DATA_FOR_TABLE.STRUCT_MISSING_FIELDS",
        expectedMessageParameters = Map(
          "tableName" -> "`table-name`",
          "colName" -> "`m`.`key`",
          "missingFields" -> "`y`")
      )
    }
  }

  test("SPARK-42997: missing fields in struct inside map value (byName)") {
    checkMissingFieldsInStructInsideMapValue(byNameResolution = true)
  }

  test("SPARK-42997: missing fields in struct inside map value (byPosition)") {
    checkMissingFieldsInStructInsideMapValue(byNameResolution = false)
  }

  private def checkMissingFieldsInStructInsideMapValue(byNameResolution: Boolean): Unit = {
    val table = TestRelation(Seq(
      $"a".int,
      Symbol("m").map(
        new StructType().add("x", "int").add("y", "int"),
        new StructType().add("x", "int").add("y", "int"))))
    val query = TestRelation(Seq(
      $"a".int,
      Symbol("m").map(
        new StructType().add("x", "int").add("y", "int"),
        new StructType().add("x", "int"))))

    val (parsedPlan, expectedErrMsg) = if (byNameResolution) {
      byName(table, query) -> "Cannot find data for output column 'm.value.y'"
    } else {
      byPosition(table, query) -> "Struct 'm.value' missing fields: 'y'"
    }

    assertNotResolved(parsedPlan)
    if (byNameResolution) {
      assertAnalysisErrorCondition(
        parsedPlan,
        expectedErrorCondition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA",
        expectedMessageParameters = Map(
          "tableName" -> "`table-name`",
          "colName" -> "`m`.`value`.`y`")
      )
    } else {
      assertAnalysisErrorCondition(
        parsedPlan,
        expectedErrorCondition = "INCOMPATIBLE_DATA_FOR_TABLE.STRUCT_MISSING_FIELDS",
        expectedMessageParameters = Map(
          "tableName" -> "`table-name`",
          "colName" -> "`m`.`value`",
          "missingFields" -> "`y`")
      )
    }
  }

  test("SPARK-42855: NOT NULL checks for nested structs, arrays, maps (byName)") {
    checkNotNullStructArrayMap(byNameResolution = true)
  }

  test("SPARK-42855: NOT NULL checks for nested structs, arrays, maps (byPosition)") {
    checkNotNullStructArrayMap(byNameResolution = false)
  }

  private def checkNotNullStructArrayMap(byNameResolution: Boolean): Unit = {
    val structType = new StructType().add("x", "int").add("y", "int")
    val table = TestRelation(Seq(
      $"a".int,
      $"b".struct(
        $"s".struct(structType).notNull,
        $"arr".array(structType).notNull,
        Symbol("m").map(structType, structType).notNull)))
    val query = TestRelation(Seq(
      $"a".int,
      $"b".struct(
        $"s".struct(structType),
        $"arr".array(structType),
        Symbol("m").map(structType, structType))))

    val parsedPlan = if (byNameResolution) byName(table, query) else byPosition(table, query)
    val analyzedPlan = parsedPlan.analyze

    assertNoNullCheck(analyzedPlan, Seq("b"))
    assertNullCheckExists(analyzedPlan, Seq("b", "s"))
    assertNullCheckExists(analyzedPlan, Seq("b", "arr"))
    assertNullCheckExists(analyzedPlan, Seq("b", "m"))
  }

  test("SPARK-42855: NOT NULL checks for nested struct fields (required input) (byName)") {
    checkNestedStructWithNotNullFields(byNameResolution = true)
  }

  test("SPARK-42855: NOT NULL checks for nested struct fields (required input) (byPosition)") {
    checkNestedStructWithNotNullFields(byNameResolution = false)
  }

  private def checkNestedStructWithNotNullFields(byNameResolution: Boolean): Unit = {
    val table = TestRelation(Seq(
      $"a".int,
      $"b".struct($"n1".int, $"n2".struct($"dn1".int.notNull, $"dn2".int.notNull))))
    val query = TestRelation(Seq(
      $"a".int,
      $"b".struct($"n1".int, $"n2".struct($"dn1".int.notNull, $"dn2".int).notNull).notNull))

    val parsedPlan = if (byNameResolution) byName(table, query) else byPosition(table, query)
    val analyzedPlan = parsedPlan.analyze

    assertNullCheckExists(analyzedPlan, Seq("b", "n2", "dn2"))
    // the entire 'b.n2.dn1' path in the query is required, no need for checks
    assertNoNullCheck(analyzedPlan, Seq("b", "n2", "dn1"))
  }

  test("SPARK-42855: NOT NULL checks for nested struct fields (byName)") {
    checkNullableNestedStructWithNotNullFields(byNameResolution = true)
  }

  test("SPARK-42855: NOT NULL checks for nested struct fields (byPosition)") {
    checkNullableNestedStructWithNotNullFields(byNameResolution = false)
  }

  private def checkNullableNestedStructWithNotNullFields(byNameResolution: Boolean): Unit = {
    val table = TestRelation(Seq(
      $"a".int,
      $"b".struct($"n1".int, $"n2".struct($"dn1".int.notNull, $"dn2".int.notNull))))
    val query = TestRelation(Seq(
      $"a".int,
      $"b".struct($"n1".int, $"n2".struct($"dn1".int.notNull, $"dn2".int))))

    val parsedPlan = if (byNameResolution) byName(table, query) else byPosition(table, query)
    val analyzedPlan = parsedPlan.analyze

    assertNoNullCheck(analyzedPlan, Seq("b", "n2"))
    // the 'b.n2.dn1' path in the query is nullable as 'b' and 'b.n2' are nullable
    // that's why there is AssertNotNull for 'b.n2.dn1' as the query is being transformed
    assertNullCheckExists(analyzedPlan, Seq("b", "n2", "dn1"))
    assertNullCheckExists(analyzedPlan, Seq("b", "n2", "dn2"))
  }

  test("SPARK-42855: NOT NULL checks for nullable array with required element (byName)") {
    checkNullableArrayWithNotNullElement(byNameResolution = true)
  }

  test("SPARK-42855: NOT NULL checks for nullable array with required element (byPosition)") {
    checkNullableArrayWithNotNullElement(byNameResolution = false)
  }

  private def checkNullableArrayWithNotNullElement(byNameResolution: Boolean): Unit = {
    val structType = new StructType().add("x", "int").add("y", "int")
    val table = TestRelation(Seq(
      $"a".int,
      $"b".struct(
        $"i".int,
        $"arr".array(ArrayType(structType, containsNull = false)))))
    val query = TestRelation(Seq(
      $"a".int,
      $"b".struct(
        $"i".int,
        $"arr".array(ArrayType(structType, containsNull = true)))))

    val parsedPlan = if (byNameResolution) byName(table, query) else byPosition(table, query)
    val analyzedPlan = parsedPlan.analyze

    assertNoNullCheck(analyzedPlan, Seq("b", "arr"))
    assertNullCheckExists(analyzedPlan, Seq("b", "arr", "element"))
  }

  test("SPARK-42855: NOT NULL checks for fields inside nullable array (byName)") {
    checkNotNullFieldsInsideNullableArray(byNameResolution = true)
  }

  test("SPARK-42855: NOT NULL checks for fields inside nullable array (byPosition)") {
    checkNotNullFieldsInsideNullableArray(byNameResolution = false)
  }

  private def checkNotNullFieldsInsideNullableArray(byNameResolution: Boolean): Unit = {
    val tableStructType = new StructType().add("x", "int", nullable = false).add("y", "int")
    val table = TestRelation(Seq(
      $"a".int,
      $"b".struct(
        $"i".int,
        $"arr".array(ArrayType(tableStructType, containsNull = true)))))
    val queryStructType = new StructType().add("x", "int").add("y", "int")
    val query = TestRelation(Seq(
      $"a".int,
      $"b".struct(
        $"i".int,
        $"arr".array(ArrayType(queryStructType, containsNull = true)))))

    val parsedPlan = if (byNameResolution) byName(table, query) else byPosition(table, query)
    val analyzedPlan = parsedPlan.analyze

    assertNoNullCheck(analyzedPlan, Seq("b", "arr"))
    assertNoNullCheck(analyzedPlan, Seq("b", "arr", "element"))
    assertNullCheckExists(analyzedPlan, Seq("b", "arr", "element", "x"))
    assertNoNullCheck(analyzedPlan, Seq("b", "arr", "element", "y"))
  }

  test("SPARK-42855: NOT NULL checks for nullable map with required values (byName)") {
    checkNullableMapWithNotNullValues(byNameResolution = true)
  }

  test("SPARK-42855: NOT NULL checks for nullable map with required values (byPosition)") {
    checkNullableMapWithNotNullValues(byNameResolution = false)
  }

  private def checkNullableMapWithNotNullValues(byNameResolution: Boolean): Unit = {
    val structType = new StructType().add("x", "int").add("y", "int")
    val table = TestRelation(Seq(
      $"a".int,
      $"b".struct(
        $"i".int,
        Symbol("m").map(MapType(structType, structType, valueContainsNull = false)))))
    val query = TestRelation(Seq(
      $"a".int,
      $"b".struct(
        $"i".int,
        Symbol("m").map(MapType(structType, structType, valueContainsNull = true)))))

    val parsedPlan = if (byNameResolution) byName(table, query) else byPosition(table, query)
    val analyzedPlan = parsedPlan.analyze

    assertNoNullCheck(analyzedPlan, Seq("b", "m"))
    assertNullCheckExists(analyzedPlan, Seq("b", "m", "value"))
  }

  test("SPARK-42855: NOT NULL checks for fields inside nullable maps (byName)") {
    checkNotNullFieldsInsideNullableMap(byNameResolution = true)
  }

  test("SPARK-42855: NOT NULL checks for fields inside nullable maps (byPosition)") {
    checkNotNullFieldsInsideNullableMap(byNameResolution = false)
  }

  private def checkNotNullFieldsInsideNullableMap(byNameResolution: Boolean): Unit = {
    val tableStructType = new StructType().add("x", "int", nullable = false).add("y", "int")
    val table = TestRelation(Seq(
      $"a".int,
      $"b".struct(
        $"i".int,
        Symbol("m").map(MapType(tableStructType, tableStructType, valueContainsNull = true)))))
    val queryStructType = new StructType().add("x", "int", nullable = true).add("y", "int")
    val query = TestRelation(Seq(
      $"a".int,
      $"b".struct(
        $"i".int,
        Symbol("m").map(MapType(queryStructType, queryStructType, valueContainsNull = true)))))

    val parsedPlan = if (byNameResolution) byName(table, query) else byPosition(table, query)
    val analyzedPlan = parsedPlan.analyze

    assertNoNullCheck(analyzedPlan, Seq("b", "m"))

    assertNoNullCheck(analyzedPlan, Seq("b", "m", "key"))
    assertNullCheckExists(analyzedPlan, Seq("b", "m", "key", "x"))
    assertNoNullCheck(analyzedPlan, Seq("b", "m", "key", "y"))

    assertNoNullCheck(analyzedPlan, Seq("b", "m", "value"))
    assertNullCheckExists(analyzedPlan, Seq("b", "m", "value", "x"))
    assertNoNullCheck(analyzedPlan, Seq("b", "m", "value", "y"))
  }

  test("SPARK-42855: no null checks when nullability is compatible (byName)") {
    checkCompatibleWritesWithNestedStructs(byNameResolution = true)
  }

  test("SPARK-42855: no null checks when nullability is compatible (byPosition)") {
    checkCompatibleWritesWithNestedStructs(byNameResolution = false)
  }

  private def checkCompatibleWritesWithNestedStructs(byNameResolution: Boolean): Unit = {
    val structType = new StructType().add("x", "int", nullable = false).add("y", "int")
    val table = TestRelation(Seq(
      $"a".int,
      $"b".struct(structType),
      $"arr".array(structType),
      Symbol("m").map(structType, structType)))
    val query = TestRelation(Seq(
      $"a".int,
      $"b".struct(structType),
      $"arr".array(structType),
      Symbol("m").map(structType, structType)))

    val parsedPlan = if (byNameResolution) byName(table, query) else byPosition(table, query)

    assertResolved(parsedPlan)
    checkAnalysis(parsedPlan, parsedPlan)
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
      StructField("y", DoubleType))))

    val query = TestRelation(StructType(Seq(
      StructField("a", DoubleType, nullable = false),
      StructField("b", DoubleType))))

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
      StructField("y", DoubleType))))

    val query = TestRelation(StructType(Seq(
      StructField("a", DoubleType, nullable = false),
      StructField("b", DoubleType))))

    // the write is resolved (checked above). this test plan is not because of the expression.
    val parsedPlan = OverwriteByExpression.byPosition(table, query,
      LessThanOrEqual(UnresolvedAttribute(Seq("a")), Literal(15.0d)))

    assertNotResolved(parsedPlan)
    assertAnalysisErrorCondition(
      parsedPlan,
      "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      Map("objectName" -> "`a`", "proposal" -> "`x`, `y`")
    )

    val tableAcceptAnySchema = TestRelationAcceptAnySchema(StructType(Seq(
      StructField("x", DoubleType, nullable = false),
      StructField("y", DoubleType))))

    val parsedPlan2 = OverwriteByExpression.byPosition(tableAcceptAnySchema, query,
      LessThanOrEqual(UnresolvedAttribute(Seq("a")), Literal(15.0d)))
    assertNotResolved(parsedPlan2)
    assertAnalysisErrorCondition(
      parsedPlan2,
      "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      Map("objectName" -> "`a`", "proposal" -> "`x`, `y`")
    )
  }

  test("SPARK-36498: reorder inner fields with byName mode") {
    val table = TestRelation(Seq($"a".int, $"b".struct($"x".int, $"y".int)))
    val query = TestRelation(Seq($"b".struct($"y".int, $"x".byte), $"a".int))

    val writePlan = byName(table, query).analyze
    assert(writePlan.children.head.schema == table.schema)
  }

  test("SPARK-36498: reorder inner fields in array of struct with byName mode") {
    val table = TestRelation(Seq(
      $"a".int,
      $"arr".array(new StructType().add("x", "int").add("y", "int"))))
    val query = TestRelation(Seq(
      $"arr".array(new StructType().add("y", "int").add("x", "byte")),
      $"a".int))

    val writePlan = byName(table, query).analyze
    assert(writePlan.children.head.schema == table.schema)
  }

  test("SPARK-36498: reorder inner fields in map of struct with byName mode") {
    val table = TestRelation(Seq(
      $"a".int,
      Symbol("m").map(
        new StructType().add("x", "int").add("y", "int"),
        new StructType().add("x", "int").add("y", "int"))))
    val query = TestRelation(Seq(
      Symbol("m").map(
        new StructType().add("y", "int").add("x", "byte"),
        new StructType().add("y", "int").add("x", "byte")),
      $"a".int))

    val writePlan = byName(table, query).analyze
    assert(writePlan.children.head.schema == table.schema)
  }

  test("SPARK-42608: use full column names for inner fields in resolution errors") {
    val table = TestRelation(Seq(
      $"a".int,
      $"b".struct($"x".int.notNull, $"y".int),
      $"c".struct($"x".int, $"y".int)))
    val query = TestRelation(Seq(
      $"b".struct($"y".int, $"x".byte),
      $"c".struct($"y".int, $"x".byte),
      $"a".int))

    val parsedPlan = byName(table, query)
    val analyzedPlan = parsedPlan.analyze

    assertNullCheckExists(analyzedPlan, Seq("b", "x"))
  }

  protected def assertNullCheckExists(plan: LogicalPlan, colPath: Seq[String]): Unit = {
    val asserts = findAsserts(plan, colPath)
    assert(asserts.nonEmpty, s"Must have NOT NULL checks for col $colPath")
  }

  protected def assertNoNullCheck(plan: LogicalPlan, colPath: Seq[String]): Unit = {
    val asserts = findAsserts(plan, colPath)
    assert(asserts.isEmpty, s"Must have no NOT NULL checks for col $colPath")
  }

  private def findAsserts(plan: LogicalPlan, colPath: Seq[String]): Seq[AssertNotNull] = {
    val query = plan match {
      case command: V2WriteCommand => command.query
      case other => fail(s"Expected V2WriteCommand: $other")
    }

    query.expressions.flatMap(e => e.collect {
      case assert: AssertNotNull if assert.walkedTypePath == colPath => assert
    })
  }
}
