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

import org.apache.spark.{SPARK_DOC_ROOT, SparkException}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{Count, Max}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.{AsOfJoinDirection, Cross, Inner, LeftOuter, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.errors.DataTypeErrorsBase
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

private[sql] case class GroupableData(data: Int) {
  def getData: Int = data
}

private[sql] class GroupableUDT extends UserDefinedType[GroupableData] {

  override def sqlType: DataType = IntegerType

  override def serialize(groupableData: GroupableData): Int = groupableData.data

  override def deserialize(datum: Any): GroupableData = {
    datum match {
      case data: Int => GroupableData(data)
    }
  }

  override def userClass: Class[GroupableData] = classOf[GroupableData]

  private[spark] override def asNullable: GroupableUDT = this
}

private[sql] case class UngroupableData(data: Map[Int, Int]) {
  def getData: Map[Int, Int] = data
}

case class TestFunction(
    children: Seq[Expression],
    inputTypes: Seq[AbstractDataType])
  extends Expression with ImplicitCastInputTypes with Unevaluable {
  override def nullable: Boolean = true
  override def dataType: DataType = StringType
  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)
}

case class TestFunctionWithTypeCheckFailure(
    children: Seq[Expression],
    inputTypes: Seq[AbstractDataType])
  extends Expression with Unevaluable {

  override def checkInputDataTypes(): TypeCheckResult = {
    for ((child, idx) <- children.zipWithIndex) {
      val expectedDataType = inputTypes(idx)
      if (child.dataType != expectedDataType) {
        return TypeCheckResult.TypeCheckFailure(
          s"Expression must be a ${expectedDataType.simpleString}")
      }
    }
    TypeCheckResult.TypeCheckSuccess
  }

  override def nullable: Boolean = true
  override def dataType: DataType = StringType
  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)
}

case class UnresolvedTestPlan() extends UnresolvedLeafNode

case class SupportsNonDeterministicExpressionTestOperator(
    actions: Seq[Expression],
    allowNonDeterministicExpression: Boolean)
  extends LeafNode with SupportsNonDeterministicExpression {
  override def output: Seq[Attribute] = Seq()
}

class AnalysisErrorSuite extends AnalysisTest with DataTypeErrorsBase {
  import TestRelations._

  def errorTest(
      name: String,
      plan: LogicalPlan,
      errorMessages: Seq[String],
      caseSensitive: Boolean = true): Unit = {
    test(name) {
      assertAnalysisError(plan, errorMessages, caseSensitive)
    }
  }

  def errorConditionTest(
      name: String,
      plan: LogicalPlan,
      condition: String,
      messageParameters: Map[String, String],
      caseSensitive: Boolean = true): Unit = {
    test(name) {
      assertAnalysisErrorCondition(
        plan, condition, messageParameters, caseSensitive = caseSensitive)
    }
  }

  val dateLit = Literal.create(null, DateType)

  errorTest(
    "scalar subquery with 2 columns",
     testRelation.select(
       (ScalarSubquery(testRelation.select($"a", dateLit.as("b"))) + Literal(1)).as("a")),
       "Scalar subquery must return only one column, but got 2" :: Nil)

  errorTest(
    "scalar subquery with no column",
    testRelation.select(ScalarSubquery(LocalRelation()).as("a")),
    "Scalar subquery must return only one column, but got 0" :: Nil)

  errorConditionTest(
    "single invalid type, single arg",
    testRelation.select(TestFunction(dateLit :: Nil, IntegerType :: Nil).as("a")),
    condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
    messageParameters = Map(
      "sqlExpr" -> "\"testfunction(NULL)\"",
      "paramIndex" -> "first",
      "inputSql" -> "\"NULL\"",
      "inputType" -> "\"DATE\"",
      "requiredType" -> "\"INT\""))

  errorConditionTest(
    "single invalid type, second arg",
    testRelation.select(
      TestFunction(dateLit :: dateLit :: Nil, DateType :: IntegerType :: Nil).as("a")),
    condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
    messageParameters = Map(
      "sqlExpr" -> "\"testfunction(NULL, NULL)\"",
      "paramIndex" -> "second",
      "inputSql" -> "\"NULL\"",
      "inputType" -> "\"DATE\"",
      "requiredType" -> "\"INT\""))

  errorConditionTest(
    "multiple invalid type",
    testRelation.select(
      TestFunction(dateLit :: dateLit :: Nil, IntegerType :: IntegerType :: Nil).as("a")),
    condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
    messageParameters = Map(
      "sqlExpr" -> "\"testfunction(NULL, NULL)\"",
      "paramIndex" -> "first",
      "inputSql" -> "\"NULL\"",
      "inputType" -> "\"DATE\"",
      "requiredType" -> "\"INT\""))

  errorConditionTest(
    "SPARK-44477: type check failure",
    testRelation.select(
      TestFunctionWithTypeCheckFailure(dateLit :: Nil, BinaryType :: Nil).as("a")),
    condition = "DATATYPE_MISMATCH.TYPE_CHECK_FAILURE_WITH_HINT",
    messageParameters = Map(
      "sqlExpr" -> "\"testfunctionwithtypecheckfailure(NULL)\"",
      "msg" -> "Expression must be a binary",
      "hint" -> ""))

  errorConditionTest(
    "invalid window function",
    testRelation2.select(
      WindowExpression(
        Literal(0),
        WindowSpecDefinition(
          UnresolvedAttribute("a") :: Nil,
          SortOrder(UnresolvedAttribute("b"), Ascending) :: Nil,
          UnspecifiedFrame)).as("window")),
    condition = "UNSUPPORTED_EXPR_FOR_WINDOW",
    messageParameters = Map("sqlExpr" -> "\"0\""))

  errorConditionTest(
    "distinct aggregate function in window",
    testRelation2.select(
      WindowExpression(
        Count(UnresolvedAttribute("b")).toAggregateExpression(isDistinct = true),
        WindowSpecDefinition(
          UnresolvedAttribute("a") :: Nil,
          SortOrder(UnresolvedAttribute("b"), Ascending) :: Nil,
          UnspecifiedFrame)).as("window")),
    condition = "DISTINCT_WINDOW_FUNCTION_UNSUPPORTED",
    messageParameters = Map("windowExpr" ->
      s"""
         |"count(DISTINCT b) OVER (PARTITION BY a ORDER BY b ASC NULLS FIRST
         | RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
         |""".stripMargin.replaceAll("\n", "")))

  errorTest(
    "window aggregate function with filter predicate",
    testRelation2.select(
      WindowExpression(
        Count(UnresolvedAttribute("b"))
          .toAggregateExpression(isDistinct = false, filter = Some(UnresolvedAttribute("b") > 1)),
        WindowSpecDefinition(
          UnresolvedAttribute("a") :: Nil,
          SortOrder(UnresolvedAttribute("b"), Ascending) :: Nil,
          UnspecifiedFrame)).as("window")),
    "window aggregate function with filter predicate is not supported" :: Nil
  )

  test("distinct function") {
    assertAnalysisErrorCondition(
      CatalystSqlParser.parsePlan("SELECT hex(DISTINCT a) FROM TaBlE"),
      expectedErrorCondition = "INVALID_SQL_SYNTAX.FUNCTION_WITH_UNSUPPORTED_SYNTAX",
      expectedMessageParameters = Map(
        "prettyName" -> toSQLId("hex"),
        "syntax" -> toSQLStmt("DISTINCT")),
      Array(ExpectedContext("hex(DISTINCT a)", 7, 21)))
  }

  test("non aggregate function with filter predicate") {
    assertAnalysisErrorCondition(
      CatalystSqlParser.parsePlan("SELECT hex(a) FILTER (WHERE c = 1) FROM TaBlE2"),
      expectedErrorCondition = "INVALID_SQL_SYNTAX.FUNCTION_WITH_UNSUPPORTED_SYNTAX",
      expectedMessageParameters = Map(
        "prettyName" -> toSQLId("hex"),
        "syntax" -> toSQLStmt("FILTER CLAUSE")),
      Array(ExpectedContext("hex(a) FILTER (WHERE c = 1)", 7, 33)))
  }

  test("distinct window function") {
    assertAnalysisErrorCondition(
      CatalystSqlParser.parsePlan("SELECT percent_rank(DISTINCT a) OVER () FROM TaBlE"),
      expectedErrorCondition = "INVALID_SQL_SYNTAX.FUNCTION_WITH_UNSUPPORTED_SYNTAX",
      expectedMessageParameters = Map(
        "prettyName" -> toSQLId("percent_rank"),
        "syntax" -> toSQLStmt("DISTINCT")),
      Array(ExpectedContext("percent_rank(DISTINCT a) OVER ()", 7, 38)))
  }

  test("window function with filter predicate") {
    assertAnalysisErrorCondition(
      CatalystSqlParser.parsePlan(
        "SELECT percent_rank(a) FILTER (WHERE c > 1) OVER () FROM TaBlE2"),
      expectedErrorCondition = "INVALID_SQL_SYNTAX.FUNCTION_WITH_UNSUPPORTED_SYNTAX",
      expectedMessageParameters = Map(
        "prettyName" -> toSQLId("percent_rank"),
        "syntax" -> toSQLStmt("FILTER CLAUSE")),
      Array(ExpectedContext("percent_rank(a) FILTER (WHERE c > 1) OVER ()", 7, 50)))
  }

  test("window specification error") {
    assertAnalysisErrorCondition(
      inputPlan = CatalystSqlParser.parsePlan(
        """
          |WITH sample_data AS (
          |    SELECT 1 AS a, 10 AS b UNION ALL
          |    SELECT 2 AS a, 20 AS b
          |)
          |SELECT
          |    AVG(a) OVER (b) AS avg_a
          |FROM sample_data
          |GROUP BY a, b;
          |""".stripMargin),
      expectedErrorCondition = "MISSING_WINDOW_SPECIFICATION",
      expectedMessageParameters = Map(
        "windowName" -> "b",
        "docroot" -> SPARK_DOC_ROOT))
  }

  test("higher order function with filter predicate") {
    assertAnalysisErrorCondition(
      CatalystSqlParser.parsePlan("SELECT aggregate(array(1, 2, 3), 0, (acc, x) -> acc + x) " +
        "FILTER (WHERE c > 1)"),
      expectedErrorCondition = "INVALID_SQL_SYNTAX.FUNCTION_WITH_UNSUPPORTED_SYNTAX",
      expectedMessageParameters = Map(
        "prettyName" -> toSQLId("aggregate"),
        "syntax" -> toSQLStmt("FILTER CLAUSE")),
      Array(ExpectedContext(
        "aggregate(array(1, 2, 3), 0, (acc, x) -> acc + x) FILTER (WHERE c > 1)", 7, 76)))
  }

  test("function don't support ignore nulls") {
    assertAnalysisErrorCondition(
      CatalystSqlParser.parsePlan("SELECT hex(a) IGNORE NULLS FROM TaBlE2"),
      expectedErrorCondition = "INVALID_SQL_SYNTAX.FUNCTION_WITH_UNSUPPORTED_SYNTAX",
      expectedMessageParameters = Map(
        "prettyName" -> toSQLId("hex"),
        "syntax" -> toSQLStmt("IGNORE NULLS")),
      Array(ExpectedContext("hex(a) IGNORE NULLS", 7, 25)))
  }

  test("some window function don't support ignore nulls") {
    assertAnalysisErrorCondition(
      CatalystSqlParser.parsePlan("SELECT percent_rank(a) IGNORE NULLS FROM TaBlE2"),
      expectedErrorCondition = "INVALID_SQL_SYNTAX.FUNCTION_WITH_UNSUPPORTED_SYNTAX",
      expectedMessageParameters = Map(
        "prettyName" -> toSQLId("percent_rank"),
        "syntax" -> toSQLStmt("IGNORE NULLS")),
      Array(ExpectedContext("percent_rank(a) IGNORE NULLS", 7, 34)))
  }

  test("aggregate function don't support ignore nulls") {
    assertAnalysisErrorCondition(
      CatalystSqlParser.parsePlan("SELECT count(a) IGNORE NULLS FROM TaBlE2"),
      expectedErrorCondition = "INVALID_SQL_SYNTAX.FUNCTION_WITH_UNSUPPORTED_SYNTAX",
      expectedMessageParameters = Map(
        "prettyName" -> toSQLId("count"),
        "syntax" -> toSQLStmt("IGNORE NULLS")),
      Array(ExpectedContext("count(a) IGNORE NULLS", 7, 27)))
  }

  test("higher order function don't support ignore nulls") {
    assertAnalysisErrorCondition(
      CatalystSqlParser.parsePlan(
        "SELECT aggregate(array(1, 2, 3), 0, (acc, x) -> acc + x) IGNORE NULLS"),
      expectedErrorCondition = "INVALID_SQL_SYNTAX.FUNCTION_WITH_UNSUPPORTED_SYNTAX",
      expectedMessageParameters = Map(
        "prettyName" -> toSQLId("aggregate"),
        "syntax" -> toSQLStmt("IGNORE NULLS")),
      Array(ExpectedContext(
        "aggregate(array(1, 2, 3), 0, (acc, x) -> acc + x) IGNORE NULLS", 7, 68)))
  }

  errorConditionTest(
    name = "nested aggregate functions",
    testRelation.groupBy($"a")(
      Max(Count(Literal(1)).toAggregateExpression()).toAggregateExpression()),
    condition = "NESTED_AGGREGATE_FUNCTION",
    messageParameters = Map.empty
  )

  errorTest(
    "offset window function",
    testRelation2.select(
      WindowExpression(
        new Lead(UnresolvedAttribute("b")),
        WindowSpecDefinition(
          UnresolvedAttribute("a") :: Nil,
          SortOrder(UnresolvedAttribute("b"), Ascending) :: Nil,
          SpecifiedWindowFrame(RangeFrame, Literal(1), Literal(2)))).as("window")),
    "Cannot specify window frame for lead function" :: Nil)

  errorConditionTest(
    "the offset of nth_value window function is negative or zero",
    testRelation2.select(
      WindowExpression(
        new NthValue(AttributeReference("b", IntegerType)(), Literal(0)),
        WindowSpecDefinition(
          UnresolvedAttribute("a") :: Nil,
          SortOrder(UnresolvedAttribute("b"), Ascending) :: Nil,
          SpecifiedWindowFrame(RowFrame, Literal(0), Literal(0)))).as("window")),
    condition = "DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE",
    messageParameters = Map(
      "sqlExpr" -> "\"nth_value(b, 0)\"",
      "exprName" -> "offset",
      "valueRange" -> "(0, 9223372036854775807]",
      "currentValue" -> "0L"))

  errorConditionTest(
    "the offset of nth_value window function is not int literal",
    testRelation2.select(
      WindowExpression(
        new NthValue(AttributeReference("b", IntegerType)(), Literal(true)),
        WindowSpecDefinition(
          UnresolvedAttribute("a") :: Nil,
          SortOrder(UnresolvedAttribute("b"), Ascending) :: Nil,
          SpecifiedWindowFrame(RowFrame, Literal(0), Literal(0)))).as("window")),
    condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
    messageParameters = Map(
      "sqlExpr" -> "\"nth_value(b, true)\"",
      "paramIndex" -> "second",
      "inputSql" -> "\"true\"",
      "inputType" -> "\"BOOLEAN\"",
      "requiredType" -> "\"INT\""))

  errorConditionTest(
    "the buckets of ntile window function is not foldable",
    testRelation2.select(
      WindowExpression(
        NTile(Literal(99.9f)),
        WindowSpecDefinition(
          UnresolvedAttribute("a") :: Nil,
          SortOrder(UnresolvedAttribute("b"), Ascending) :: Nil,
          UnspecifiedFrame)).as("window")),
    condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
    messageParameters = Map(
      "sqlExpr" -> "\"ntile(99.9)\"",
      "paramIndex" -> "first",
      "inputSql" -> "\"99.9\"",
      "inputType" -> "\"FLOAT\"",
      "requiredType" -> "\"INT\""))


  errorConditionTest(
    "the buckets of ntile window function is not int literal",
    testRelation2.select(
      WindowExpression(
        NTile(AttributeReference("b", IntegerType)()),
        WindowSpecDefinition(
          UnresolvedAttribute("a") :: Nil,
          SortOrder(UnresolvedAttribute("b"), Ascending) :: Nil,
          UnspecifiedFrame)).as("window")),
    condition = "DATATYPE_MISMATCH.NON_FOLDABLE_INPUT",
    messageParameters = Map(
      "sqlExpr" -> "\"ntile(b)\"",
      "inputName" -> "`buckets`",
      "inputExpr" -> "\"b\"",
      "inputType" -> "\"INT\""))

  errorConditionTest(
    "unresolved attributes",
    testRelation.select($"abcd"),
    "UNRESOLVED_COLUMN.WITH_SUGGESTION",
    Map("objectName" -> "`abcd`", "proposal" -> "`a`"))

  errorConditionTest(
    "unresolved attributes with a generated name",
    testRelation2.groupBy($"a")(max($"b"))
      .where(sum($"b") > 0)
      .orderBy($"havingCondition".asc),
    "UNRESOLVED_COLUMN.WITH_SUGGESTION",
    Map("objectName" -> "`havingCondition`", "proposal" -> "`max(b)`"))

  errorConditionTest(
    "unresolved star expansion in max",
    testRelation2.groupBy($"a")(sum(UnresolvedStar(None))),
    condition = "INVALID_USAGE_OF_STAR_OR_REGEX",
    messageParameters = Map("elem" -> "'*'", "prettyName" -> "expression `sum`")
  )

  errorConditionTest(
    "sorting by unsupported column types",
    mapRelation.orderBy($"map".asc),
    condition = "DATATYPE_MISMATCH.INVALID_ORDERING_TYPE",
    messageParameters = Map(
      "sqlExpr" -> "\"map ASC NULLS FIRST\"",
      "functionName" -> "`sortorder`",
      "dataType" -> "\"MAP<INT, INT>\""))

  errorConditionTest(
    "sorting by attributes are not from grouping expressions",
    testRelation2.groupBy($"a", $"c")($"a", $"c", count($"a").as("a3")).orderBy($"b".asc),
    "UNRESOLVED_COLUMN.WITH_SUGGESTION",
    Map("objectName" -> "`b`", "proposal" -> "`a`, `c`, `a3`"))

  errorConditionTest(
    "non-boolean filters",
    testRelation.where(Literal(1)),
    condition = "DATATYPE_MISMATCH.FILTER_NOT_BOOLEAN",
    messageParameters = Map("sqlExpr" -> "\"1\"", "filter" -> "\"1\"", "type" -> "\"INT\""))

  errorConditionTest(
    "non-boolean join conditions",
    testRelation.join(testRelation, condition = Some(Literal(1))),
    condition = "JOIN_CONDITION_IS_NOT_BOOLEAN_TYPE",
    messageParameters = Map("joinCondition" -> "\"1\"", "conditionType" -> "\"INT\""))

  errorConditionTest(
    "missing group by",
    testRelation2.groupBy($"a")($"b"),
    "MISSING_AGGREGATION",
    messageParameters = Map(
      "expression" -> "\"b\"",
      "expressionAnyValue" -> "\"any_value(b)\"")
  )

  errorConditionTest(
    "ambiguous field",
    nestedRelation.select($"top.duplicateField"),
    condition = "AMBIGUOUS_REFERENCE_TO_FIELDS",
    messageParameters = Map(
      "field" -> "`duplicateField`",
      "count" -> "2"),
    caseSensitive = false
  )

  errorConditionTest(
    "ambiguous field due to case insensitivity",
    nestedRelation.select($"top.differentCase"),
    condition = "AMBIGUOUS_REFERENCE_TO_FIELDS",
    messageParameters = Map(
      "field" -> "`differentCase`",
      "count" -> "2"),
    caseSensitive = false
  )

  errorConditionTest(
    "missing field",
    nestedRelation2.select($"top.c"),
    "FIELD_NOT_FOUND",
    Map("fieldName" -> "`c`", "fields" -> "`aField`, `bField`, `cField`"),
    caseSensitive = false)

  checkError(
    exception = intercept[SparkException] {
      val analyzer = getAnalyzer
      analyzer.checkAnalysis(analyzer.execute(UnresolvedTestPlan()))
    },
    condition = "INTERNAL_ERROR",
    parameters = Map("message" -> "Found the unresolved operator: 'UnresolvedTestPlan"))

  errorTest(
    "union with unequal number of columns",
    testRelation.union(testRelation2),
    "union" :: "number of columns" :: testRelation2.output.length.toString ::
      testRelation.output.length.toString :: Nil)

  errorTest(
    "intersect with unequal number of columns",
    testRelation.intersect(testRelation2, isAll = false),
    "intersect" :: "number of columns" :: testRelation2.output.length.toString ::
      testRelation.output.length.toString :: Nil)

  errorTest(
    "except with unequal number of columns",
    testRelation.except(testRelation2, isAll = false),
    "except" :: "number of columns" :: testRelation2.output.length.toString ::
      testRelation.output.length.toString :: Nil)

  errorTest(
    "union with incompatible column types",
    testRelation.union(nestedRelation),
    "union" :: "compatible column types" :: Nil)

  errorTest(
    "union with a incompatible column type and compatible column types",
    testRelation3.union(testRelation4),
    "union"  :: "compatible column types" :: "map" :: "decimal" :: Nil)

  errorTest(
    "intersect with incompatible column types",
    testRelation.intersect(nestedRelation, isAll = false),
    "intersect" :: "compatible column types" :: Nil)

  errorTest(
    "intersect with a incompatible column type and compatible column types",
    testRelation3.intersect(testRelation4, isAll = false),
    "intersect" :: "compatible column types" :: "map" :: "decimal" :: Nil)

  errorTest(
    "except with incompatible column types",
    testRelation.except(nestedRelation, isAll = false),
    "except" :: "compatible column types" :: Nil)

  errorTest(
    "except with a incompatible column type and compatible column types",
    testRelation3.except(testRelation4, isAll = false),
    "except" :: "compatible column types" :: "map" :: "decimal" :: Nil)

  errorConditionTest(
    "SPARK-9955: correct error message for aggregate",
    // When parse SQL string, we will wrap aggregate expressions with UnresolvedAlias.
    testRelation2.where($"bad_column" > 1).groupBy($"a")(UnresolvedAlias(max($"b"))),
    "UNRESOLVED_COLUMN.WITH_SUGGESTION",
    Map("objectName" -> "`bad_column`", "proposal" -> "`a`, `c`, `d`, `b`, `e`"))

  errorConditionTest(
    "slide duration greater than window in time window",
    testRelation2.select(
      TimeWindow(Literal("2016-01-01 01:01:01"), "1 second", "2 second", "0 second").as("window")),
    "DATATYPE_MISMATCH.PARAMETER_CONSTRAINT_VIOLATION",
    Map(
      "sqlExpr" -> "\"window(2016-01-01 01:01:01, 1000000, 2000000, 0)\"",
      "leftExprName" -> "`slide_duration`",
      "leftExprValue" -> "2000000L",
      "constraint" -> "<=",
      "rightExprName" -> "`window_duration`",
      "rightExprValue" -> "1000000L"
    )
  )

  errorConditionTest(
    "start time greater than slide duration in time window",
    testRelation.select(
      TimeWindow(Literal("2016-01-01 01:01:01"), "1 second", "1 second", "1 minute").as("window")),
    "DATATYPE_MISMATCH.PARAMETER_CONSTRAINT_VIOLATION",
    Map(
      "sqlExpr" -> "\"window(2016-01-01 01:01:01, 1000000, 1000000, 60000000)\"",
      "leftExprName" -> "`abs(start_time)`",
      "leftExprValue" -> "60000000L",
      "constraint" -> "<",
      "rightExprName" -> "`slide_duration`",
      "rightExprValue" -> "1000000L"
    )
  )

  errorConditionTest(
    "start time equal to slide duration in time window",
    testRelation.select(
      TimeWindow(Literal("2016-01-01 01:01:01"), "1 second", "1 second", "1 second").as("window")),
    "DATATYPE_MISMATCH.PARAMETER_CONSTRAINT_VIOLATION",
    Map(
      "sqlExpr" -> "\"window(2016-01-01 01:01:01, 1000000, 1000000, 1000000)\"",
      "leftExprName" -> "`abs(start_time)`",
      "leftExprValue" -> "1000000L",
      "constraint" -> "<",
      "rightExprName" -> "`slide_duration`",
      "rightExprValue" -> "1000000L"
    )
  )

  errorConditionTest(
    "SPARK-21590: absolute value of start time greater than slide duration in time window",
    testRelation.select(
      TimeWindow(Literal("2016-01-01 01:01:01"), "1 second", "1 second", "-1 minute").as("window")),
    "DATATYPE_MISMATCH.PARAMETER_CONSTRAINT_VIOLATION",
    Map(
      "sqlExpr" -> "\"window(2016-01-01 01:01:01, 1000000, 1000000, -60000000)\"",
      "leftExprName" -> "`abs(start_time)`",
      "leftExprValue" -> "60000000L",
      "constraint" -> "<",
      "rightExprName" -> "`slide_duration`",
      "rightExprValue" -> "1000000L"
    )
  )

  errorConditionTest(
    "SPARK-21590: absolute value of start time equal to slide duration in time window",
    testRelation.select(
      TimeWindow(Literal("2016-01-01 01:01:01"), "1 second", "1 second", "-1 second").as("window")),
    "DATATYPE_MISMATCH.PARAMETER_CONSTRAINT_VIOLATION",
    Map(
      "sqlExpr" -> "\"window(2016-01-01 01:01:01, 1000000, 1000000, -1000000)\"",
      "leftExprName" -> "`abs(start_time)`",
      "leftExprValue" -> "1000000L",
      "constraint" -> "<",
      "rightExprName" -> "`slide_duration`",
      "rightExprValue" -> "1000000L"
    )
  )

  errorConditionTest(
    "negative window duration in time window",
    testRelation.select(
      TimeWindow(Literal("2016-01-01 01:01:01"), "-1 second", "1 second", "0 second").as("window")),
      "DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE",
    Map(
      "sqlExpr" -> "\"window(2016-01-01 01:01:01, -1000000, 1000000, 0)\"",
      "exprName" -> "`window_duration`",
      "valueRange" -> s"(0, 9223372036854775807]",
      "currentValue" -> "-1000000L"
    )
  )

  errorConditionTest(
    "zero window duration in time window",
    testRelation.select(
      TimeWindow(Literal("2016-01-01 01:01:01"), "0 second", "1 second", "0 second").as("window")),
    "DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE",
    Map(
      "sqlExpr" -> "\"window(2016-01-01 01:01:01, 0, 1000000, 0)\"",
      "exprName" -> "`window_duration`",
      "valueRange" -> "(0, 9223372036854775807]",
      "currentValue" -> "0L"
    )
  )

  errorConditionTest(
    "negative slide duration in time window",
    testRelation.select(
      TimeWindow(Literal("2016-01-01 01:01:01"), "1 second", "-1 second", "0 second").as("window")),
    "DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE",
    Map(
      "sqlExpr" -> "\"window(2016-01-01 01:01:01, 1000000, -1000000, 0)\"",
      "exprName" -> "`slide_duration`",
      "valueRange" -> "(0, 9223372036854775807]",
      "currentValue" -> "-1000000L"
    )
  )

  errorConditionTest(
    "zero slide duration in time window",
    testRelation.select(
      TimeWindow(Literal("2016-01-01 01:01:01"), "1 second", "0 second", "0 second").as("window")),
    "DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE",
    Map(
      "sqlExpr" -> "\"window(2016-01-01 01:01:01, 1000000, 0, 0)\"",
      "exprName" -> "`slide_duration`",
      "valueRange" -> "(0, 9223372036854775807]",
      "currentValue" -> "0L"
    )
  )

  errorTest(
    "generator nested in expressions",
    listRelation.select(Explode($"list") + 1),
    """The generator is not supported: nested in expressions "(explode(list) + 1)""""
      :: Nil
  )

  errorTest(
    "SPARK-30998: unsupported nested inner generators",
    {
      val nestedListRelation = LocalRelation(
        AttributeReference("nestedList", ArrayType(ArrayType(IntegerType)))())
      nestedListRelation.select(Explode(Explode($"nestedList")))
    },
    "The generator is not supported: nested in expressions " +
      """"explode(explode(nestedList))"""" :: Nil
  )

  errorTest(
    "SPARK-30998: unsupported nested inner generators for aggregates",
    testRelation.select(Explode(Explode(
      CreateArray(CreateArray(min($"a") :: max($"a") :: Nil) :: Nil)))),
    "The generator is not supported: nested in expressions " +
      """"explode(explode(array(array(min(a), max(a)))))"""" :: Nil
  )

  errorTest(
    "generator nested in expressions for aggregates",
    testRelation.select(Explode(CreateArray(min($"a") :: max($"a") :: Nil)) + 1),
    "The generator is not supported: nested in expressions " +
      """"(explode(array(min(a), max(a))) + 1)"""" :: Nil
  )

  errorTest(
    "generator appears in operator which is not Project",
    listRelation.sortBy(Explode($"list").asc),
    "The generator is not supported: outside the SELECT clause, found: Sort" :: Nil
  )

  errorConditionTest(
    "an evaluated limit class must not be string",
    testRelation.limit(Literal(UTF8String.fromString("abc"), StringType)),
    "INVALID_LIMIT_LIKE_EXPRESSION.DATA_TYPE",
    Map(
      "name" -> "limit",
      "expr" -> "\"abc\"",
      "dataType" -> "\"STRING\""
    )
  )

  errorConditionTest(
    "an evaluated limit class must not be long",
    testRelation.limit(Literal(10L, LongType)),
    "INVALID_LIMIT_LIKE_EXPRESSION.DATA_TYPE",
    Map(
      "name" -> "limit",
      "expr" -> "\"10\"",
      "dataType" -> "\"BIGINT\""
    )
  )

  errorConditionTest(
    "an evaluated limit class must not be null",
    testRelation.limit(Literal(null, IntegerType)),
    "INVALID_LIMIT_LIKE_EXPRESSION.IS_NULL",
    Map(
      "name" -> "limit",
      "expr" -> "\"NULL\""
    )
  )

  errorConditionTest(
    "num_rows in limit clause must be equal to or greater than 0",
    listRelation.limit(-1),
    "INVALID_LIMIT_LIKE_EXPRESSION.IS_NEGATIVE",
    Map(
      "name" -> "limit",
      "expr" -> "\"-1\"",
      "v" -> "-1"
    )
  )

  errorConditionTest(
    "an evaluated offset class must not be string",
    testRelation.offset(Literal(UTF8String.fromString("abc"), StringType)),
    "INVALID_LIMIT_LIKE_EXPRESSION.DATA_TYPE",
    Map(
      "name" -> "offset",
      "expr" -> "\"abc\"",
      "dataType" -> "\"STRING\""
    )
  )

  errorConditionTest(
    "an evaluated offset class must not be long",
    testRelation.offset(Literal(10L, LongType)),
    "INVALID_LIMIT_LIKE_EXPRESSION.DATA_TYPE",
    Map(
      "name" -> "offset",
      "expr" -> "\"10\"",
      "dataType" -> "\"BIGINT\""
    )
  )

  errorConditionTest(
    "an evaluated offset class must not be null",
    testRelation.offset(Literal(null, IntegerType)),
    "INVALID_LIMIT_LIKE_EXPRESSION.IS_NULL",
    Map(
      "name" -> "offset",
      "expr" -> "\"NULL\""
    )
  )

  errorConditionTest(
    "num_rows in offset clause must be equal to or greater than 0",
    testRelation.offset(-1),
    "INVALID_LIMIT_LIKE_EXPRESSION.IS_NEGATIVE",
    Map(
      "name" -> "offset",
      "expr" -> "\"-1\"",
      "v" -> "-1"
    )
  )

  errorConditionTest(
    "the sum of num_rows in limit clause and num_rows in offset clause less than Int.MaxValue",
    testRelation.offset(Literal(2000000000, IntegerType)).limit(Literal(1000000000, IntegerType)),
    "SUM_OF_LIMIT_AND_OFFSET_EXCEEDS_MAX_INT",
    Map("limit" -> "1000000000", "offset" -> "2000000000"))

  errorTest(
    "more than one generators for aggregates in SELECT",
    testRelation.select(Explode(CreateArray(min($"a") :: Nil)),
      Explode(CreateArray(max($"a") :: Nil))),
    "The generator is not supported: only one generator allowed per SELECT clause but found 2: " +
      """"explode(array(min(a)))", "explode(array(max(a)))"""" :: Nil
  )

  errorConditionTest(
    "EXEC IMMEDIATE - nested execute immediate not allowed",
    CatalystSqlParser.parsePlan("EXECUTE IMMEDIATE 'EXECUTE IMMEDIATE \\\'SELECT 42\\\''"),
    "NESTED_EXECUTE_IMMEDIATE",
    Map(
      "sqlString" -> "EXECUTE IMMEDIATE 'SELECT 42'"))

  errorConditionTest(
    "EXEC IMMEDIATE - both positional and named used",
    CatalystSqlParser.parsePlan("EXECUTE IMMEDIATE 'SELECT 42 where ? = :first'" +
      " USING 1, 2 as first"),
    "INVALID_QUERY_MIXED_QUERY_PARAMETERS",
    Map.empty)

  test("EXEC IMMEDIATE - non string variable as sqlString parameter") {
    val execImmediatePlan = ExecuteImmediateQuery(
      Seq.empty,
      scala.util.Right(UnresolvedAttribute("testVarA")),
      Seq(UnresolvedAttribute("testVarA")))

    assertAnalysisErrorCondition(
      inputPlan = execImmediatePlan,
      expectedErrorCondition = "INVALID_VARIABLE_TYPE_FOR_QUERY_EXECUTE_IMMEDIATE",
      expectedMessageParameters = Map(
        "varType" -> "\"INT\""
      ))
  }

  test("EXEC IMMEDIATE - Null string as sqlString parameter") {
    val execImmediatePlan = ExecuteImmediateQuery(
      Seq.empty,
      scala.util.Right(UnresolvedAttribute("testVarNull")),
      Seq(UnresolvedAttribute("testVarNull")))

    assertAnalysisErrorCondition(
      inputPlan = execImmediatePlan,
      expectedErrorCondition = "NULL_QUERY_STRING_EXECUTE_IMMEDIATE",
      expectedMessageParameters = Map("varName" -> "`testVarNull`"))
  }


  test("EXEC IMMEDIATE - Unsupported expr for parameter") {
    val execImmediatePlan: LogicalPlan = ExecuteImmediateQuery(
      Seq(UnresolvedAttribute("testVarA"), NaNvl(Literal(1), Literal(1))),
      scala.util.Left("SELECT ?"),
      Seq.empty)

    assertAnalysisErrorCondition(
      inputPlan = execImmediatePlan,
      expectedErrorCondition = "UNSUPPORTED_EXPR_FOR_PARAMETER",
      expectedMessageParameters = Map(
        "invalidExprSql" -> "\"nanvl(1, 1)\""
      ))
  }

  test("EXEC IMMEDIATE - Name Parametrize query with non named parameters") {
    val execImmediateSetVariablePlan = ExecuteImmediateQuery(
      Seq(Literal(2), new Alias(UnresolvedAttribute("testVarA"), "first")(), Literal(3)),
      scala.util.Left("SELECT :first"),
      Seq.empty)

    assertAnalysisErrorCondition(
      inputPlan = execImmediateSetVariablePlan,
      expectedErrorCondition = "ALL_PARAMETERS_MUST_BE_NAMED",
      expectedMessageParameters = Map(
        "exprs" -> "\"2\", \"3\""
      ))
  }

  test("EXEC IMMEDIATE - INTO specified for COMMAND query") {
    val execImmediateSetVariablePlan = ExecuteImmediateQuery(
      Seq.empty,
      scala.util.Left("SET VAR testVarA = 1"),
      Seq(UnresolvedAttribute("testVarA")))

    assertAnalysisErrorCondition(
      inputPlan = execImmediateSetVariablePlan,
      expectedErrorCondition = "INVALID_STATEMENT_FOR_EXECUTE_INTO",
      expectedMessageParameters = Map(
        "sqlString" -> "SET VAR TESTVARA = 1"
      ))
  }

  test("SPARK-6452 regression test") {
    // CheckAnalysis should throw AnalysisException when Aggregate contains missing attribute(s)
    // Since we manually construct the logical plan at here and Sum only accept
    // LongType, DoubleType, and DecimalType. We use LongType as the type of a.
    val attrA = AttributeReference("a", LongType)(exprId = ExprId(1))
    val otherA = AttributeReference("a", LongType)(exprId = ExprId(2))
    val attrC = AttributeReference("c", LongType)(exprId = ExprId(3))
    val aliases = Alias(sum(attrA), "b")() :: Alias(sum(attrC), "d")() :: Nil
    val plan = Aggregate(
      Nil,
      aliases,
      LocalRelation(otherA))

    assert(plan.resolved)

    assertAnalysisErrorCondition(
      inputPlan = plan,
      expectedErrorCondition = "MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION",
      expectedMessageParameters = Map(
        "missingAttributes" -> "\"a\", \"c\"",
        "input" -> "\"a\"",
        "operator" -> s"!Aggregate [${aliases.mkString(", ")}]",
        "operation" -> "\"a\""
      )
    )
  }

  test("error test for self-join") {
    val join = Join(testRelation, testRelation, Cross, None, JoinHint.NONE)
    checkError(
      exception = intercept[SparkException] {
        SimpleAnalyzer.checkAnalysis(join)
      },
      condition = "INTERNAL_ERROR",
      parameters = Map("message" ->
        """
          |Failure when resolving conflicting references in Join:
          |'Join Cross
          |:- LocalRelation <empty>, [a#x]
          |+- LocalRelation <empty>, [a#x]
          |
          |Conflicting attributes: "a".""".stripMargin))
  }

  test("error test for self-intersect") {
    val intersect = Intersect(testRelation, testRelation, true)
    checkError(
      exception = intercept[SparkException] {
        SimpleAnalyzer.checkAnalysis(intersect)
      },
      condition = "INTERNAL_ERROR",
      parameters = Map("message" ->
        """
          |Failure when resolving conflicting references in Intersect All:
          |'Intersect All true
          |:- LocalRelation <empty>, [a#x]
          |+- LocalRelation <empty>, [a#x]
          |
          |Conflicting attributes: "a".""".stripMargin))
  }

  test("error test for self-except") {
    val except = Except(testRelation, testRelation, true)
    checkError(
      exception = intercept[SparkException] {
        SimpleAnalyzer.checkAnalysis(except)
      },
      condition = "INTERNAL_ERROR",
      parameters = Map("message" ->
        """
          |Failure when resolving conflicting references in Except All:
          |'Except All true
          |:- LocalRelation <empty>, [a#x]
          |+- LocalRelation <empty>, [a#x]
          |
          |Conflicting attributes: "a".""".stripMargin))
  }

  test("error test for self-asOfJoin") {
    val asOfJoin =
      AsOfJoin(testRelation, testRelation, testRelation.output(0), testRelation.output(0),
      None, Inner, tolerance = None, allowExactMatches = true,
      direction = AsOfJoinDirection("backward"))
    checkError(
      exception = intercept[SparkException] {
        SimpleAnalyzer.checkAnalysis(asOfJoin)
      },
      condition = "INTERNAL_ERROR",
      parameters = Map("message" ->
        """
          |Failure when resolving conflicting references in AsOfJoin:
          |'AsOfJoin (a#x >= a#x), Inner
          |:- LocalRelation <empty>, [a#x]
          |+- LocalRelation <empty>, [a#x]
          |
          |Conflicting attributes: "a".""".stripMargin))
  }

  test("check grouping expression data types") {
    def checkDataType(dataType: DataType): Unit = {
      val plan =
        Aggregate(
          AttributeReference("a", dataType)(exprId = ExprId(2)) :: Nil,
          Alias(sum(AttributeReference("b", IntegerType)(exprId = ExprId(1))), "c")() :: Nil,
          LocalRelation(
            AttributeReference("a", dataType)(exprId = ExprId(2)),
            AttributeReference("b", IntegerType)(exprId = ExprId(1))))

      assertAnalysisSuccess(plan, true)
    }

    val supportedDataTypes = Seq(
      StringType, BinaryType,
      NullType, BooleanType,
      ByteType, ShortType, IntegerType, LongType,
      FloatType, DoubleType, DecimalType(25, 5), DecimalType(6, 5),
      DateType, TimestampType,
      ArrayType(IntegerType),
      MapType(StringType, LongType),
      new StructType()
        .add("f1", FloatType, nullable = true)
        .add("f2", MapType(StringType, LongType), nullable = true),
      new StructType()
        .add("f1", FloatType, nullable = true)
        .add("f2", StringType, nullable = true),
      new StructType()
        .add("f1", FloatType, nullable = true)
        .add("f2", ArrayType(BooleanType, containsNull = true), nullable = true),
      new GroupableUDT())
    supportedDataTypes.foreach { dataType =>
      checkDataType(dataType)
    }
  }

  test("we should fail analysis when we find nested aggregate functions") {
    val plan =
      Aggregate(
        AttributeReference("a", IntegerType)(exprId = ExprId(2)) :: Nil,
        Alias(sum(sum(AttributeReference("b", IntegerType)(exprId = ExprId(1)))), "c")() :: Nil,
        LocalRelation(
          AttributeReference("a", IntegerType)(exprId = ExprId(2)),
          AttributeReference("b", IntegerType)(exprId = ExprId(1))))

    assertAnalysisErrorCondition(
      inputPlan = plan,
      expectedErrorCondition = "NESTED_AGGREGATE_FUNCTION",
      expectedMessageParameters = Map.empty
    )
  }

  test("Join can work on binary types but can't work on map types") {
    val left = LocalRelation($"a".binary, Symbol("b").map(StringType, StringType))
    val right = LocalRelation($"c".binary, Symbol("d").map(StringType, StringType))

    val plan1 = left.join(
      right,
      joinType = Cross,
      condition = Some($"a" === $"c"))

    assertAnalysisSuccess(plan1)

    val plan2 = left.join(
      right,
      joinType = Cross,
      condition = Some($"b" === $"d"))

    assertAnalysisErrorCondition(
      inputPlan = plan2,
      expectedErrorCondition = "DATATYPE_MISMATCH.INVALID_ORDERING_TYPE",
      expectedMessageParameters = Map(
        "functionName" -> "`=`",
        "dataType" -> "\"MAP<STRING, STRING>\"",
        "sqlExpr" -> "\"(b = d)\""
      ),
      caseSensitive = true
    )
  }

  test("PredicateSubQuery is used outside of a allowed nodes") {
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    val plan = Sort(
      Seq(SortOrder(InSubquery(Seq(a), ListQuery(LocalRelation(b))), Ascending)),
      global = true,
      LocalRelation(a))
    assertAnalysisError(plan, "Predicate subqueries can only be used in " :: Nil)
  }

  test("PredicateSubQuery correlated predicate is nested in an illegal plan") {
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    val c = AttributeReference("c", IntegerType)()

    val plan1 = Filter(
      Exists(
        Join(
          LocalRelation(b),
          Filter(EqualTo(UnresolvedAttribute("a"), c), LocalRelation(c)),
          LeftOuter,
          Option(EqualTo(b, c)),
          JoinHint.NONE)),
      LocalRelation(a))
    assertAnalysisError(plan1, "Accessing outer query column is not allowed in" :: Nil)

    val plan2 = Filter(
      Exists(
        Join(
          Filter(EqualTo(UnresolvedAttribute("a"), c), LocalRelation(c)),
          LocalRelation(b),
          RightOuter,
          Option(EqualTo(b, c)),
          JoinHint.NONE)),
      LocalRelation(a))
    assertAnalysisError(plan2, "Accessing outer query column is not allowed in" :: Nil)

    val plan3 = Filter(
      Exists(
        Sample(0.0, 0.5, false, 1L,
          Filter(EqualTo(UnresolvedAttribute("a"), b), LocalRelation(b))).select("b")
      ),
      LocalRelation(a))
    assertAnalysisError(plan3,
                        "Accessing outer query column is not allowed in" :: Nil)
  }

  test("Error on filter condition containing aggregate expressions") {
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    val plan = Filter($"a" === UnresolvedFunction("max", Seq(b), true), LocalRelation(a, b))
    assertAnalysisErrorCondition(plan,
      expectedErrorCondition = "INVALID_WHERE_CONDITION",
      expectedMessageParameters = Map(
        "condition" -> "\"(a = max(DISTINCT b))\"",
        "expressionList" -> "max(DISTINCT b)"))
  }

  test("SPARK-30811: CTE should not cause stack overflow when " +
    "it refers to non-existent table with same name") {
    val plan = UnresolvedWith(
      UnresolvedRelation(TableIdentifier("t")),
      Seq("t" -> SubqueryAlias("t",
        Project(
          Alias(Literal(1), "x")() :: Nil,
          UnresolvedRelation(TableIdentifier("t", Option("nonexist")))))))
    assertAnalysisErrorCondition(plan,
      expectedErrorCondition = "TABLE_OR_VIEW_NOT_FOUND",
      Map("relationName" -> "`nonexist`.`t`"))
  }

  test("SPARK-33909: Check rand functions seed is legal at analyzer side") {
    Seq((Rand("a".attr), "\"rand(a)\""),
      (Randn("a".attr), "\"randn(a)\"")).foreach {
      case (r, expectedArg) =>
        val plan = Project(Seq(r.as("r")), testRelation)
        assertAnalysisErrorCondition(plan,
          expectedErrorCondition = "SEED_EXPRESSION_IS_UNFOLDABLE",
          expectedMessageParameters = Map(
            "seedExpr" -> "\"a\"",
            "exprWithSeed" -> expectedArg),
          caseSensitive = false
        )
    }
    Seq(
      Rand(1.0) -> ("\"rand(1.0)\"", "\"1.0\"", "\"DOUBLE\""),
      Rand("1") -> ("\"rand(1)\"", "\"1\"", "\"STRING\""),
      Randn("a") -> ("\"randn(a)\"", "\"a\"", "\"STRING\"")
    ).foreach { case (r, (sqlExpr, inputSql, inputType)) =>
      val plan = Project(Seq(r.as("r")), testRelation)
      assertAnalysisErrorCondition(plan,
        expectedErrorCondition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
        expectedMessageParameters = Map(
          "sqlExpr" -> sqlExpr,
          "paramIndex" -> "first",
          "inputSql" -> inputSql,
          "inputType" -> inputType,
          "requiredType" -> "(\"INT\" or \"BIGINT\")"),
        caseSensitive = false
      )
    }
  }

  test("SPARK-34946: correlated scalar subquery in grouping expressions only") {
    val c1 = AttributeReference("c1", IntegerType)()
    val c2 = AttributeReference("c2", IntegerType)()
    val t = LocalRelation(c1, c2)
    val plan = Aggregate(
      ScalarSubquery(
        Aggregate(Nil, sum($"c2").as("sum") :: Nil,
          Filter($"t1.c1" === $"t2.c1",
            t.as("t2")))
      ) :: Nil,
      sum($"c2").as("sum") :: Nil, t.as("t1"))
    assertAnalysisErrorCondition(
      plan,
      expectedErrorCondition =
        "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.MUST_AGGREGATE_CORRELATED_SCALAR_SUBQUERY",
      expectedMessageParameters = Map.empty)
  }

  test("SPARK-34946: correlated scalar subquery in aggregate expressions only") {
    val c1 = AttributeReference("c1", IntegerType)()
    val c2 = AttributeReference("c2", IntegerType)()
    val t = LocalRelation(c1, c2)
    val plan = Aggregate(
      $"c1" :: Nil,
      ScalarSubquery(
        Aggregate(Nil, sum($"c2").as("sum") :: Nil,
          Filter($"t1.c1" === $"t2.c1",
            t.as("t2")))
      ).as("sub") :: Nil, t.as("t1"))
    assertAnalysisErrorCondition(
      plan,
      expectedErrorCondition =
        "SCALAR_SUBQUERY_IS_IN_GROUP_BY_OR_AGGREGATE_FUNCTION",
      expectedMessageParameters = Map("sqlExpr" -> "\"scalarsubquery(c1)\""))
  }

  errorConditionTest(
    "SPARK-34920: error code to error message",
    testRelation2.where($"bad_column" > 1).groupBy($"a")(UnresolvedAlias(max($"b"))),
    condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
    messageParameters = Map(
      "objectName" -> "`bad_column`",
      "proposal" -> "`a`, `c`, `d`, `b`, `e`"))

  errorConditionTest(
    "SPARK-39783: backticks in error message for candidate column with dots",
    // This selects a column that does not exist,
    // the error message suggest the existing column with correct backticks
    testRelation6.select($"`the`.`id`"),
    condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
    messageParameters = Map(
      "objectName" -> "`the`.`id`",
      "proposal" -> "`the.id`"))

  errorConditionTest(
    "SPARK-39783: backticks in error message for candidate struct column",
    // This selects a column that does not exist,
    // the error message suggest the existing column with correct backticks
    nestedRelation2.select($"`top.aField`"),
    condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
    messageParameters = Map(
      "objectName" -> "`top.aField`",
      "proposal" -> "`top`"))

  test("SPARK-35673: fail if the plan still contains UnresolvedHint after analysis") {
    val hintName = "some_random_hint_that_does_not_exist"
    val plan = UnresolvedHint(hintName, Seq.empty,
      Project(Alias(Literal(1), "x")() :: Nil, OneRowRelation())
    )
    assert(plan.resolved)

    checkError(
      exception = intercept[SparkException] {
        SimpleAnalyzer.checkAnalysis(plan)
      },
      condition = "INTERNAL_ERROR",
      parameters = Map("message" -> "Hint not found: `some_random_hint_that_does_not_exist`"))

    // UnresolvedHint be removed by batch `Remove Unresolved Hints`
    assertAnalysisSuccess(plan, true)
  }

  test("SPARK-35618: Resolve star expressions in subqueries") {
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    val t0 = OneRowRelation()
    val t1 = LocalRelation(a, b).as("t1")

    // t1.* in the subquery should be resolved into outer(t1.a) and outer(t1.b).
    assertAnalysisError(
      Project(ScalarSubquery(t0.select(star("t1"))).as("sub") :: Nil, t1),
      "Scalar subquery must return only one column, but got 2" :: Nil)

    // t2.* cannot be resolved and the error should be the initial analysis exception.
    assertAnalysisErrorCondition(
      Project(ScalarSubquery(t0.select(star("t2"))).as("sub") :: Nil, t1),
      expectedErrorCondition = "CANNOT_RESOLVE_STAR_EXPAND",
      expectedMessageParameters = Map("targetString" -> "`t2`", "columns" -> "")
    )
  }

  test("SPARK-35618: Invalid star usage in subqueries") {
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    val c = AttributeReference("c", IntegerType)()
    val t1 = LocalRelation(a, b).as("t1")
    val t2 = LocalRelation(b, c).as("t2")

    // SELECT * FROM t1 WHERE a = (SELECT sum(c) FROM t2 WHERE t1.* = t2.b)
    assertAnalysisErrorCondition(
      Filter(EqualTo(a, ScalarSubquery(t2.select(sum(c)).where(star("t1") === b))), t1),
      expectedErrorCondition = "INVALID_USAGE_OF_STAR_OR_REGEX",
      expectedMessageParameters = Map("elem" -> "'*'", "prettyName" -> "expression `equalto`")
    )

    // SELECT * FROM t1 JOIN t2 ON (EXISTS (SELECT 1 FROM t2 WHERE t1.* = b))
    assertAnalysisErrorCondition(
      t1.join(t2, condition = Some(Exists(t2.select(1).where(star("t1") === b)))),
      expectedErrorCondition = "INVALID_USAGE_OF_STAR_OR_REGEX",
      expectedMessageParameters = Map("elem" -> "'*'", "prettyName" -> "expression `equalto`")
    )
  }

  test("SPARK-36488: Regular expression expansion should fail with a meaningful message") {
    withSQLConf(SQLConf.SUPPORT_QUOTED_REGEX_COLUMN_NAME.key -> "true") {
      assertAnalysisErrorCondition(
        testRelation.select(Divide(UnresolvedRegex(".?", None, false), "a")),
        expectedErrorCondition = "INVALID_USAGE_OF_STAR_OR_REGEX",
        expectedMessageParameters = Map(
          "elem" -> "regular expression '.?'",
          "prettyName" -> "expression `divide`")
      )
      assertAnalysisErrorCondition(
        testRelation.select(
          Divide(UnresolvedRegex(".?", None, false), UnresolvedRegex(".*", None, false))),
        expectedErrorCondition = "INVALID_USAGE_OF_STAR_OR_REGEX",
        expectedMessageParameters = Map(
          "elem" -> "regular expressions '.?', '.*'",
          "prettyName" -> "expression `divide`")
      )
      assertAnalysisErrorCondition(
        testRelation.select(
          Divide(UnresolvedRegex(".?", None, false), UnresolvedRegex(".?", None, false))),
        expectedErrorCondition = "INVALID_USAGE_OF_STAR_OR_REGEX",
        expectedMessageParameters = Map(
          "elem" -> "regular expression '.?'",
          "prettyName" -> "expression `divide`")
      )
      assertAnalysisErrorCondition(
        testRelation.select(Divide(UnresolvedStar(None), "a")),
        expectedErrorCondition = "INVALID_USAGE_OF_STAR_OR_REGEX",
        expectedMessageParameters = Map(
          "elem" -> "'*'",
          "prettyName" -> "expression `divide`")
      )
      assertAnalysisErrorCondition(
        testRelation.select(Divide(UnresolvedStar(None), UnresolvedStar(None))),
        expectedErrorCondition = "INVALID_USAGE_OF_STAR_OR_REGEX",
        expectedMessageParameters = Map(
          "elem" -> "'*'",
          "prettyName" -> "expression `divide`")
      )
      assertAnalysisErrorCondition(
        testRelation.select(Divide(UnresolvedStar(None), UnresolvedRegex(".?", None, false))),
        expectedErrorCondition = "INVALID_USAGE_OF_STAR_OR_REGEX",
        expectedMessageParameters = Map(
          "elem" -> "'*' and regular expression '.?'",
          "prettyName" -> "expression `divide`")
      )
      assertAnalysisErrorCondition(
        testRelation.select(Least(Seq(UnresolvedStar(None),
          UnresolvedRegex(".*", None, false), UnresolvedRegex(".?", None, false)))),
        expectedErrorCondition = "INVALID_USAGE_OF_STAR_OR_REGEX",
        expectedMessageParameters = Map(
          "elem" -> "'*' and regular expressions '.*', '.?'",
          "prettyName" -> "expression `least`")
      )
    }
  }

  errorConditionTest(
    "SPARK-47572: Enforce Window partitionSpec is orderable",
    testRelation2.select(
      WindowExpression(
        new Rank(),
        WindowSpecDefinition(
          CreateMap(Literal("key") :: UnresolvedAttribute("a") :: Nil) :: Nil,
          SortOrder(UnresolvedAttribute("b"), Ascending) :: Nil,
          UnspecifiedFrame)).as("window")),
    condition = "EXPRESSION_TYPE_IS_NOT_ORDERABLE",
    messageParameters = Map(
      "expr" -> "\"_w0\"",
      "exprType" -> "\"MAP<STRING, STRING>\""))

  test("SPARK-48871: SupportsNonDeterministicExpression allows non-deterministic expressions") {
    val nonDeterministicExpressions = Seq(new Rand())
    val tolerantPlan =
      SupportsNonDeterministicExpressionTestOperator(
        nonDeterministicExpressions, allowNonDeterministicExpression = true)
    assertAnalysisSuccess(tolerantPlan)

    val intolerantPlan =
      SupportsNonDeterministicExpressionTestOperator(
        nonDeterministicExpressions, allowNonDeterministicExpression = false)
    assertAnalysisError(
      intolerantPlan,
      "INVALID_NON_DETERMINISTIC_EXPRESSIONS" :: Nil
    )
  }
}
