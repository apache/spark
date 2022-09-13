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

import org.scalatest.Assertions._

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{Count, Max}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.{Cross, LeftOuter, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData, MapData}
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

private[sql] class UngroupableUDT extends UserDefinedType[UngroupableData] {

  override def sqlType: DataType = MapType(IntegerType, IntegerType)

  override def serialize(ungroupableData: UngroupableData): MapData = {
    val keyArray = new GenericArrayData(ungroupableData.data.keys.toSeq)
    val valueArray = new GenericArrayData(ungroupableData.data.values.toSeq)
    new ArrayBasedMapData(keyArray, valueArray)
  }

  override def deserialize(datum: Any): UngroupableData = {
    datum match {
      case data: MapData =>
        val keyArray = data.keyArray().array
        val valueArray = data.valueArray().array
        assert(keyArray.length == valueArray.length)
        val mapData = keyArray.zip(valueArray).toMap.asInstanceOf[Map[Int, Int]]
        UngroupableData(mapData)
    }
  }

  override def userClass: Class[UngroupableData] = classOf[UngroupableData]

  private[spark] override def asNullable: UngroupableUDT = this
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

case class UnresolvedTestPlan() extends LeafNode {
  override lazy val resolved = false
  override def output: Seq[Attribute] = Nil
}

class AnalysisErrorSuite extends AnalysisTest {
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

  def errorClassTest(
      name: String,
      plan: LogicalPlan,
      errorClass: String,
      messageParameters: Map[String, String]): Unit = {
    errorClassTest(name, plan, errorClass, null, messageParameters)
  }

  def errorClassTest(
      name: String,
      plan: LogicalPlan,
      errorClass: String,
      errorSubClass: String,
      messageParameters: Map[String, String]): Unit = {
    test(name) {
      assertAnalysisErrorClass(plan, errorClass, errorSubClass, messageParameters,
        caseSensitive = true)
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

  errorTest(
    "single invalid type, single arg",
    testRelation.select(TestFunction(dateLit :: Nil, IntegerType :: Nil).as("a")),
    "cannot resolve" :: "testfunction(CAST(NULL AS DATE))" :: "argument 1" :: "requires int type" ::
    "'CAST(NULL AS DATE)' is of date type" :: Nil)

  errorTest(
    "single invalid type, second arg",
    testRelation.select(
      TestFunction(dateLit :: dateLit :: Nil, DateType :: IntegerType :: Nil).as("a")),
    "cannot resolve" :: "testfunction(CAST(NULL AS DATE), CAST(NULL AS DATE))" ::
      "argument 2" :: "requires int type" ::
      "'CAST(NULL AS DATE)' is of date type" :: Nil)

  errorTest(
    "multiple invalid type",
    testRelation.select(
      TestFunction(dateLit :: dateLit :: Nil, IntegerType :: IntegerType :: Nil).as("a")),
    "cannot resolve" :: "testfunction(CAST(NULL AS DATE), CAST(NULL AS DATE))" ::
      "argument 1" :: "argument 2" :: "requires int type" ::
      "'CAST(NULL AS DATE)' is of date type" :: Nil)

  errorTest(
    "invalid window function",
    testRelation2.select(
      WindowExpression(
        Literal(0),
        WindowSpecDefinition(
          UnresolvedAttribute("a") :: Nil,
          SortOrder(UnresolvedAttribute("b"), Ascending) :: Nil,
          UnspecifiedFrame)).as("window")),
    "not supported within a window function" :: Nil)

  errorTest(
    "distinct aggregate function in window",
    testRelation2.select(
      WindowExpression(
        Count(UnresolvedAttribute("b")).toAggregateExpression(isDistinct = true),
        WindowSpecDefinition(
          UnresolvedAttribute("a") :: Nil,
          SortOrder(UnresolvedAttribute("b"), Ascending) :: Nil,
          UnspecifiedFrame)).as("window")),
    "Distinct window functions are not supported" :: Nil)

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

  errorTest(
    "distinct function",
    CatalystSqlParser.parsePlan("SELECT hex(DISTINCT a) FROM TaBlE"),
    "Function hex does not support DISTINCT" :: Nil)

  errorTest(
    "non aggregate function with filter predicate",
    CatalystSqlParser.parsePlan("SELECT hex(a) FILTER (WHERE c = 1) FROM TaBlE2"),
    "Function hex does not support FILTER clause" :: Nil)

  errorTest(
    "distinct window function",
    CatalystSqlParser.parsePlan("SELECT percent_rank(DISTINCT a) OVER () FROM TaBlE"),
    "Function percent_rank does not support DISTINCT" :: Nil)

  errorTest(
    "window function with filter predicate",
    CatalystSqlParser.parsePlan("SELECT percent_rank(a) FILTER (WHERE c > 1) OVER () FROM TaBlE2"),
    "Function percent_rank does not support FILTER clause" :: Nil)

  errorTest(
    "higher order function with filter predicate",
    CatalystSqlParser.parsePlan("SELECT aggregate(array(1, 2, 3), 0, (acc, x) -> acc + x) " +
      "FILTER (WHERE c > 1)"),
    "Function aggregate does not support FILTER clause" :: Nil)

  errorTest(
    "non-deterministic filter predicate in aggregate functions",
    CatalystSqlParser.parsePlan("SELECT count(a) FILTER (WHERE rand(int(c)) > 1) FROM TaBlE2"),
    "FILTER expression is non-deterministic, it cannot be used in aggregate functions" :: Nil)

  errorTest(
    "function don't support ignore nulls",
    CatalystSqlParser.parsePlan("SELECT hex(a) IGNORE NULLS FROM TaBlE2"),
    "Function hex does not support IGNORE NULLS" :: Nil)

  errorTest(
    "some window function don't support ignore nulls",
    CatalystSqlParser.parsePlan("SELECT percent_rank(a) IGNORE NULLS FROM TaBlE2"),
    "Function percent_rank does not support IGNORE NULLS" :: Nil)

  errorTest(
    "aggregate function don't support ignore nulls",
    CatalystSqlParser.parsePlan("SELECT count(a) IGNORE NULLS FROM TaBlE2"),
    "Function count does not support IGNORE NULLS" :: Nil)

  errorTest(
    "higher order function don't support ignore nulls",
    CatalystSqlParser.parsePlan("SELECT aggregate(array(1, 2, 3), 0, (acc, x) -> acc + x) " +
      "IGNORE NULLS"), "Function aggregate does not support IGNORE NULLS" :: Nil)

  errorTest(
    "nested aggregate functions",
    testRelation.groupBy($"a")(
      Max(Count(Literal(1)).toAggregateExpression()).toAggregateExpression()),
    "not allowed to use an aggregate function in the argument of another aggregate function." :: Nil
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

  errorTest(
    "the offset of nth_value window function is negative or zero",
    testRelation2.select(
      WindowExpression(
        new NthValue(AttributeReference("b", IntegerType)(), Literal(0)),
        WindowSpecDefinition(
          UnresolvedAttribute("a") :: Nil,
          SortOrder(UnresolvedAttribute("b"), Ascending) :: Nil,
          SpecifiedWindowFrame(RowFrame, Literal(0), Literal(0)))).as("window")),
    "The 'offset' argument of nth_value must be greater than zero but it is 0." :: Nil)

  errorTest(
    "the offset of nth_value window function is not int literal",
    testRelation2.select(
      WindowExpression(
        new NthValue(AttributeReference("b", IntegerType)(), Literal(true)),
        WindowSpecDefinition(
          UnresolvedAttribute("a") :: Nil,
          SortOrder(UnresolvedAttribute("b"), Ascending) :: Nil,
          SpecifiedWindowFrame(RowFrame, Literal(0), Literal(0)))).as("window")),
    "argument 2 requires int type, however, 'true' is of boolean type." :: Nil)

  errorTest(
    "too many generators",
    listRelation.select(Explode($"list").as("a"), Explode($"list").as("b")),
    "only one generator" :: "explode" :: Nil)

  errorClassTest(
    "unresolved attributes",
    testRelation.select($"abcd"),
    "UNRESOLVED_COLUMN",
    "WITH_SUGGESTION",
    Map("objectName" -> "`abcd`", "proposal" -> "`a`"))

  errorClassTest(
    "unresolved attributes with a generated name",
    testRelation2.groupBy($"a")(max($"b"))
      .where(sum($"b") > 0)
      .orderBy($"havingCondition".asc),
    "UNRESOLVED_COLUMN",
    "WITH_SUGGESTION",
    Map("objectName" -> "`havingCondition`", "proposal" -> "`max(b)`"))

  errorTest(
    "unresolved star expansion in max",
    testRelation2.groupBy($"a")(sum(UnresolvedStar(None))),
    "Invalid usage of '*'" :: "in expression 'sum'" :: Nil)

  errorTest(
    "sorting by unsupported column types",
    mapRelation.orderBy($"map".asc),
    "sort" :: "type" :: "map<int,int>" :: Nil)

  errorClassTest(
    "sorting by attributes are not from grouping expressions",
    testRelation2.groupBy($"a", $"c")($"a", $"c", count($"a").as("a3")).orderBy($"b".asc),
    "UNRESOLVED_COLUMN",
    "WITH_SUGGESTION",
    Map("objectName" -> "`b`", "proposal" -> "`a`, `c`, `a3`"))

  errorTest(
    "non-boolean filters",
    testRelation.where(Literal(1)),
    "filter" :: "'1'" :: "not a boolean" :: Literal(1).dataType.simpleString :: Nil)

  errorTest(
    "non-boolean join conditions",
    testRelation.join(testRelation, condition = Some(Literal(1))),
    "condition" :: "'1'" :: "not a boolean" :: Literal(1).dataType.simpleString :: Nil)

  errorTest(
    "missing group by",
    testRelation2.groupBy($"a")($"b"),
    "\"b\"" :: "COLUMN_NOT_IN_GROUP_BY_CLAUSE" :: Nil
  )

  errorTest(
    "ambiguous field",
    nestedRelation.select($"top.duplicateField"),
    "Ambiguous reference to fields" :: "duplicateField" :: Nil,
    caseSensitive = false)

  errorTest(
    "ambiguous field due to case insensitivity",
    nestedRelation.select($"top.differentCase"),
    "Ambiguous reference to fields" :: "differentCase" :: "differentcase" :: Nil,
    caseSensitive = false)

  errorTest(
    "missing field",
    nestedRelation2.select($"top.c"),
    "No such struct field" :: "aField" :: "bField" :: "cField" :: Nil,
    caseSensitive = false)

  errorTest(
    "catch all unresolved plan",
    UnresolvedTestPlan(),
    "unresolved" :: Nil)

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
    "union" :: "the compatible column types" :: Nil)

  errorTest(
    "union with a incompatible column type and compatible column types",
    testRelation3.union(testRelation4),
    "union"  :: "the compatible column types" :: "map" :: "decimal" :: Nil)

  errorTest(
    "intersect with incompatible column types",
    testRelation.intersect(nestedRelation, isAll = false),
    "intersect" :: "the compatible column types" :: Nil)

  errorTest(
    "intersect with a incompatible column type and compatible column types",
    testRelation3.intersect(testRelation4, isAll = false),
    "intersect" :: "the compatible column types" :: "map" :: "decimal" :: Nil)

  errorTest(
    "except with incompatible column types",
    testRelation.except(nestedRelation, isAll = false),
    "except" :: "the compatible column types" :: Nil)

  errorTest(
    "except with a incompatible column type and compatible column types",
    testRelation3.except(testRelation4, isAll = false),
    "except" :: "the compatible column types" :: "map" :: "decimal" :: Nil)

  errorClassTest(
    "SPARK-9955: correct error message for aggregate",
    // When parse SQL string, we will wrap aggregate expressions with UnresolvedAlias.
    testRelation2.where($"bad_column" > 1).groupBy($"a")(UnresolvedAlias(max($"b"))),
    "UNRESOLVED_COLUMN",
    "WITH_SUGGESTION",
    Map("objectName" -> "`bad_column`", "proposal" -> "`a`, `b`, `c`, `d`, `e`"))

  errorTest(
    "slide duration greater than window in time window",
    testRelation2.select(
      TimeWindow(Literal("2016-01-01 01:01:01"), "1 second", "2 second", "0 second").as("window")),
      s"The slide duration " :: " must be less than or equal to the windowDuration " :: Nil
  )

  errorTest(
    "start time greater than slide duration in time window",
    testRelation.select(
      TimeWindow(Literal("2016-01-01 01:01:01"), "1 second", "1 second", "1 minute").as("window")),
      "The absolute value of start time " :: " must be less than the slideDuration " :: Nil
  )

  errorTest(
    "start time equal to slide duration in time window",
    testRelation.select(
      TimeWindow(Literal("2016-01-01 01:01:01"), "1 second", "1 second", "1 second").as("window")),
      "The absolute value of start time " :: " must be less than the slideDuration " :: Nil
  )

  errorTest(
    "SPARK-21590: absolute value of start time greater than slide duration in time window",
    testRelation.select(
      TimeWindow(Literal("2016-01-01 01:01:01"), "1 second", "1 second", "-1 minute").as("window")),
    "The absolute value of start time " :: " must be less than the slideDuration " :: Nil
  )

  errorTest(
    "SPARK-21590: absolute value of start time equal to slide duration in time window",
    testRelation.select(
      TimeWindow(Literal("2016-01-01 01:01:01"), "1 second", "1 second", "-1 second").as("window")),
    "The absolute value of start time " :: " must be less than the slideDuration " :: Nil
  )

  errorTest(
    "negative window duration in time window",
    testRelation.select(
      TimeWindow(Literal("2016-01-01 01:01:01"), "-1 second", "1 second", "0 second").as("window")),
      "The window duration " :: " must be greater than 0." :: Nil
  )

  errorTest(
    "zero window duration in time window",
    testRelation.select(
      TimeWindow(Literal("2016-01-01 01:01:01"), "0 second", "1 second", "0 second").as("window")),
      "The window duration " :: " must be greater than 0." :: Nil
  )

  errorTest(
    "negative slide duration in time window",
    testRelation.select(
      TimeWindow(Literal("2016-01-01 01:01:01"), "1 second", "-1 second", "0 second").as("window")),
      "The slide duration " :: " must be greater than 0." :: Nil
  )

  errorTest(
    "zero slide duration in time window",
    testRelation.select(
      TimeWindow(Literal("2016-01-01 01:01:01"), "1 second", "0 second", "0 second").as("window")),
      "The slide duration" :: " must be greater than 0." :: Nil
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

  errorTest(
    "an evaluated limit class must not be null",
    testRelation.limit(Literal(null, IntegerType)),
    "The evaluated limit expression must not be null, but got " :: Nil
  )

  errorTest(
    "num_rows in limit clause must be equal to or greater than 0",
    listRelation.limit(-1),
    "The limit expression must be equal to or greater than 0, but got -1" :: Nil
  )

  errorTest(
    "an evaluated offset class must not be string",
    testRelation.offset(Literal(UTF8String.fromString("abc"), StringType)),
    "The offset expression must be integer type, but got string" :: Nil
  )

  errorTest(
    "an evaluated offset class must not be long",
    testRelation.offset(Literal(10L, LongType)),
    "The offset expression must be integer type, but got bigint" :: Nil
  )

  errorTest(
    "an evaluated offset class must not be null",
    testRelation.offset(Literal(null, IntegerType)),
    "The evaluated offset expression must not be null, but got " :: Nil
  )

  errorTest(
    "num_rows in offset clause must be equal to or greater than 0",
    testRelation.offset(-1),
    "The offset expression must be equal to or greater than 0, but got -1" :: Nil
  )

  errorTest(
    "the sum of num_rows in limit clause and num_rows in offset clause less than Int.MaxValue",
    testRelation.offset(Literal(2000000000, IntegerType)).limit(Literal(1000000000, IntegerType)),
    "The sum of the LIMIT clause and the OFFSET clause must not be greater than" +
      " the maximum 32-bit integer value (2,147,483,647)," +
      " but found limit = 1000000000, offset = 2000000000." :: Nil
  )

  errorTest(
    "more than one generators in SELECT",
    listRelation.select(Explode($"list"), Explode($"list")),
    "The generator is not supported: only one generator allowed per select clause but found 2: " +
      """"explode(list)", "explode(list)"""" :: Nil
  )

  errorTest(
    "more than one generators for aggregates in SELECT",
    testRelation.select(Explode(CreateArray(min($"a") :: Nil)),
      Explode(CreateArray(max($"a") :: Nil))),
    "The generator is not supported: only one generator allowed per select clause but found 2: " +
      """"explode(array(min(a)))", "explode(array(max(a)))"""" :: Nil
  )

  errorTest(
    "SPARK-38666: non-boolean aggregate filter",
    CatalystSqlParser.parsePlan("SELECT sum(c) filter (where e) FROM TaBlE2"),
    "FILTER expression is not of type boolean" :: Nil)

  errorTest(
    "SPARK-38666: aggregate in aggregate filter",
    CatalystSqlParser.parsePlan("SELECT sum(c) filter (where max(e) > 1) FROM TaBlE2"),
    "FILTER expression contains aggregate" :: Nil)

  errorTest(
    "SPARK-38666: window function in aggregate filter",
    CatalystSqlParser.parsePlan("SELECT sum(c) " +
       "filter (where nth_value(e, 2) over(order by b) > 1) FROM TaBlE2"),
    "FILTER expression contains window function" :: Nil)

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

    val resolved = s"${attrA.toString},${attrC.toString}"

    val errorMsg = s"Resolved attribute(s) $resolved missing from ${otherA.toString} " +
                     s"in operator !Aggregate [${aliases.mkString(", ")}]. " +
                     s"Attribute(s) with the same name appear in the operation: a. " +
                     "Please check if the right attribute(s) are used."

    assertAnalysisError(plan, errorMsg :: Nil)
  }

  test("error test for self-join") {
    val join = Join(testRelation, testRelation, Cross, None, JoinHint.NONE)
    val error = intercept[AnalysisException] {
      SimpleAnalyzer.checkAnalysis(join)
    }
    assert(error.message.contains("Failure when resolving conflicting references in Join"))
    assert(error.message.contains("Conflicting attributes"))
  }

  test("check grouping expression data types") {
    def checkDataType(dataType: DataType, shouldSuccess: Boolean): Unit = {
      val plan =
        Aggregate(
          AttributeReference("a", dataType)(exprId = ExprId(2)) :: Nil,
          Alias(sum(AttributeReference("b", IntegerType)(exprId = ExprId(1))), "c")() :: Nil,
          LocalRelation(
            AttributeReference("a", dataType)(exprId = ExprId(2)),
            AttributeReference("b", IntegerType)(exprId = ExprId(1))))

      if (shouldSuccess) {
        assertAnalysisSuccess(plan, true)
      } else {
        assertAnalysisError(plan, "expression a cannot be used as a grouping expression" :: Nil)
      }
    }

    val supportedDataTypes = Seq(
      StringType, BinaryType,
      NullType, BooleanType,
      ByteType, ShortType, IntegerType, LongType,
      FloatType, DoubleType, DecimalType(25, 5), DecimalType(6, 5),
      DateType, TimestampType,
      ArrayType(IntegerType),
      new StructType()
        .add("f1", FloatType, nullable = true)
        .add("f2", StringType, nullable = true),
      new StructType()
        .add("f1", FloatType, nullable = true)
        .add("f2", ArrayType(BooleanType, containsNull = true), nullable = true),
      new GroupableUDT())
    supportedDataTypes.foreach { dataType =>
      checkDataType(dataType, shouldSuccess = true)
    }

    val unsupportedDataTypes = Seq(
      MapType(StringType, LongType),
      new StructType()
        .add("f1", FloatType, nullable = true)
        .add("f2", MapType(StringType, LongType), nullable = true),
      new UngroupableUDT())
    unsupportedDataTypes.foreach { dataType =>
      checkDataType(dataType, shouldSuccess = false)
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

    assertAnalysisError(
      plan,
      "It is not allowed to use an aggregate function in the argument of " +
        "another aggregate function." :: Nil)
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
    assertAnalysisError(plan2, "EqualTo does not support ordering on type map" :: Nil)
  }

  test("PredicateSubQuery is used outside of a allowed nodes") {
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    val plan = Sort(
      Seq(SortOrder(InSubquery(Seq(a), ListQuery(LocalRelation(b))), Ascending)),
      global = true,
      LocalRelation(a))
    assertAnalysisError(plan, "Predicate sub-queries can only be used in " :: Nil)
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
      Exists(Union(LocalRelation(b),
        Filter(EqualTo(UnresolvedAttribute("a"), c), LocalRelation(c)))),
      LocalRelation(a))
    assertAnalysisError(plan3, "Accessing outer query column is not allowed in" :: Nil)

    val plan4 = Filter(
      Exists(
        Limit(1,
          Filter(EqualTo(UnresolvedAttribute("a"), b), LocalRelation(b)))
      ),
      LocalRelation(a))
    assertAnalysisError(plan4, "Accessing outer query column is not allowed in" :: Nil)

    val plan5 = Filter(
      Exists(
        Sample(0.0, 0.5, false, 1L,
          Filter(EqualTo(UnresolvedAttribute("a"), b), LocalRelation(b))).select("b")
      ),
      LocalRelation(a))
    assertAnalysisError(plan5,
                        "Accessing outer query column is not allowed in" :: Nil)
  }

  test("Error on filter condition containing aggregate expressions") {
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    val plan = Filter($"a" === UnresolvedFunction("max", Seq(b), true), LocalRelation(a, b))
    assertAnalysisError(plan,
      "Aggregate/Window/Generate expressions are not valid in where clause of the query" :: Nil)
  }

  test("SPARK-30811: CTE should not cause stack overflow when " +
    "it refers to non-existent table with same name") {
    val plan = UnresolvedWith(
      UnresolvedRelation(TableIdentifier("t")),
      Seq("t" -> SubqueryAlias("t",
        Project(
          Alias(Literal(1), "x")() :: Nil,
          UnresolvedRelation(TableIdentifier("t", Option("nonexist")))))))
    assertAnalysisError(plan, "Table or view not found:" :: Nil)
  }

  test("SPARK-33909: Check rand functions seed is legal at analyer side") {
    Seq(Rand("a".attr), Randn("a".attr)).foreach { r =>
      val plan = Project(Seq(r.as("r")), testRelation)
      assertAnalysisError(plan,
        s"Input argument to ${r.prettyName} must be a constant." :: Nil)
    }
    Seq(Rand(1.0), Rand("1"), Randn("a")).foreach { r =>
      val plan = Project(Seq(r.as("r")), testRelation)
      assertAnalysisError(plan,
        s"data type mismatch: argument 1 requires (int or bigint) type" :: Nil)
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
    assertAnalysisError(plan, "Correlated scalar subqueries in the group by clause must also be " +
      "in the aggregate expressions" :: Nil)
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
    assertAnalysisError(plan, "Correlated scalar subquery 'scalarsubquery(t1.c1)' is " +
      "neither present in the group by, nor in an aggregate function. Add it to group by " +
      "using ordinal position or wrap it in first() (or first_value) if you don't care " +
      "which value you get." :: Nil)
  }

  errorTest(
    "SPARK-34920: error code to error message",
    testRelation2.where($"bad_column" > 1).groupBy($"a")(UnresolvedAlias(max($"b"))),
    "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name " +
      "`bad_column` cannot be resolved. Did you mean one of the following? " +
      "[`a`, `b`, `c`, `d`, `e`]"
      :: Nil)

  test("SPARK-35080: Unsupported correlated equality predicates in subquery") {
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    val c = AttributeReference("c", IntegerType)()
    val d = AttributeReference("d", DoubleType)()
    val t1 = LocalRelation(a, b, d)
    val t2 = LocalRelation(c)
    val conditions = Seq(
      (abs($"a") === $"c", "abs(a) = outer(c)"),
      (abs($"a") <=> $"c", "abs(a) <=> outer(c)"),
      ($"a" + 1 === $"c", "(a + 1) = outer(c)"),
      ($"a" + $"b" === $"c", "(a + b) = outer(c)"),
      ($"a" + $"c" === $"b", "(a + outer(c)) = b"),
      (And($"a" === $"c", Cast($"d", IntegerType) === $"c"), "CAST(d AS INT) = outer(c)"))
    conditions.foreach { case (cond, msg) =>
      val plan = Project(
        ScalarSubquery(
          Aggregate(Nil, count(Literal(1)).as("cnt") :: Nil,
            Filter(cond, t1))
        ).as("sub") :: Nil,
        t2)
      assertAnalysisError(plan, s"Correlated column is not allowed in predicate ($msg)" :: Nil)
    }
  }

  test("SPARK-35673: fail if the plan still contains UnresolvedHint after analysis") {
    val hintName = "some_random_hint_that_does_not_exist"
    val plan = UnresolvedHint(hintName, Seq.empty,
      Project(Alias(Literal(1), "x")() :: Nil, OneRowRelation())
    )
    assert(plan.resolved)

    val error = intercept[AnalysisException] {
      SimpleAnalyzer.checkAnalysis(plan)
    }
    assert(error.message.contains(s"Hint not found: ${hintName}"))

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
    assertAnalysisError(
      Project(ScalarSubquery(t0.select(star("t2"))).as("sub") :: Nil, t1),
      "cannot resolve 't2.*' given input columns ''" :: Nil
    )
  }

  test("SPARK-35618: Invalid star usage in subqueries") {
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    val c = AttributeReference("c", IntegerType)()
    val t1 = LocalRelation(a, b).as("t1")
    val t2 = LocalRelation(b, c).as("t2")

    // SELECT * FROM t1 WHERE a = (SELECT sum(c) FROM t2 WHERE t1.* = t2.b)
    assertAnalysisError(
      Filter(EqualTo(a, ScalarSubquery(t2.select(sum(c)).where(star("t1") === b))), t1),
      "Invalid usage of '*' in Filter" :: Nil
    )

    // SELECT * FROM t1 JOIN t2 ON (EXISTS (SELECT 1 FROM t2 WHERE t1.* = b))
    assertAnalysisError(
      t1.join(t2, condition = Some(Exists(t2.select(1).where(star("t1") === b)))),
      "Invalid usage of '*' in Filter" :: Nil
    )
  }

  test("SPARK-36488: Regular expression expansion should fail with a meaningful message") {
    withSQLConf(SQLConf.SUPPORT_QUOTED_REGEX_COLUMN_NAME.key -> "true") {
      assertAnalysisError(testRelation.select(Divide(UnresolvedRegex(".?", None, false), "a")),
        s"Invalid usage of regular expression '.?' in" :: Nil)
      assertAnalysisError(testRelation.select(
        Divide(UnresolvedRegex(".?", None, false), UnresolvedRegex(".*", None, false))),
        s"Invalid usage of regular expressions '.?', '.*' in" :: Nil)
      assertAnalysisError(testRelation.select(
        Divide(UnresolvedRegex(".?", None, false), UnresolvedRegex(".?", None, false))),
        s"Invalid usage of regular expression '.?' in" :: Nil)
      assertAnalysisError(testRelation.select(Divide(UnresolvedStar(None), "a")),
        "Invalid usage of '*' in" :: Nil)
      assertAnalysisError(testRelation.select(Divide(UnresolvedStar(None), UnresolvedStar(None))),
        "Invalid usage of '*' in" :: Nil)
      assertAnalysisError(testRelation.select(Divide(UnresolvedStar(None),
        UnresolvedRegex(".?", None, false))),
        "Invalid usage of '*' and regular expression '.?' in" :: Nil)
      assertAnalysisError(testRelation.select(Least(Seq(UnresolvedStar(None),
        UnresolvedRegex(".*", None, false), UnresolvedRegex(".?", None, false)))),
        "Invalid usage of '*' and regular expressions '.*', '.?' in" :: Nil)
    }
  }
}
