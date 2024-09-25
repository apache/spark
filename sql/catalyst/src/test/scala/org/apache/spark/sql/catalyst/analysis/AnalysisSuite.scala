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

import java.util.TimeZone

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import org.apache.logging.log4j.Level
import org.scalatest.matchers.must.Matchers

import org.apache.spark.SparkException
import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{AliasIdentifier, QueryPlanningTracker, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.{InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count, Sum}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser.parsePlan
import org.apache.spark.sql.catalyst.plans.{Cross, FullOuter, Inner, UsingJoin}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, RangePartitioning, RoundRobinPartitioning}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.connector.catalog.InMemoryTable
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class AnalysisSuite extends AnalysisTest with Matchers {
  import org.apache.spark.sql.catalyst.analysis.TestRelations._

  test("fail for unresolved plan") {
    intercept[AnalysisException] {
      // `testRelation` does not have column `b`.
      testRelation.select($"b").analyze
    }
  }

  test("fail if a leaf node has char/varchar type output") {
    val schema1 = new StructType().add("c", CharType(5))
    val schema2 = new StructType().add("c", VarcharType(5))
    val schema3 = new StructType().add("c", ArrayType(CharType(5)))
    Seq(schema1, schema2, schema3).foreach { schema =>
      val table = new InMemoryTable("t", schema, Array.empty, Map.empty[String, String].asJava)
      checkError(
        exception = intercept[SparkException] {
          DataSourceV2Relation(
            table,
            DataTypeUtils.toAttributes(schema),
            None,
            None,
            CaseInsensitiveStringMap.empty()).analyze
        },
        condition = "INTERNAL_ERROR",
        parameters = Map("message" ->
          "Logical plan should not have output of char/varchar type.*\n"),
        matchPVals = true)
    }
  }

  test("union project *") {
    val plan = (1 to 120)
      .map(_ => testRelation)
      .fold[LogicalPlan](testRelation) { (a, b) =>
        a.select(UnresolvedStar(None)).select($"a").union(b.select(UnresolvedStar(None)))
      }

    assertAnalysisSuccess(plan)
  }

  test("check project's resolved") {
    assert(Project(testRelation.output, testRelation).resolved)

    assert(!Project(Seq(UnresolvedAttribute("a")), testRelation).resolved)

    val explode = Explode(AttributeReference("a", IntegerType, nullable = true)())
    assert(!Project(Seq(Alias(explode, "explode")()), testRelation).resolved)

    assert(!Project(Seq(Alias(count(Literal(1)), "count")()), testRelation).resolved)
  }

  test("analyze project") {
    checkAnalysis(
      Project(Seq(UnresolvedAttribute("a")), testRelation),
      Project(testRelation.output, testRelation))

    checkAnalysisWithoutViewWrapper(
      Project(Seq(UnresolvedAttribute("TbL.a")),
        SubqueryAlias("TbL", UnresolvedRelation(TableIdentifier("TaBlE")))),
      Project(testRelation.output, testRelation))

    assertAnalysisErrorCondition(
      Project(Seq(UnresolvedAttribute("tBl.a")),
        SubqueryAlias("TbL", UnresolvedRelation(TableIdentifier("TaBlE")))),
      "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      Map("objectName" -> "`tBl`.`a`", "proposal" -> "`TbL`.`a`")
    )

    checkAnalysisWithoutViewWrapper(
      Project(Seq(UnresolvedAttribute("TbL.a")),
        SubqueryAlias("TbL", UnresolvedRelation(TableIdentifier("TaBlE")))),
      Project(testRelation.output, testRelation),
      caseSensitive = false)

    checkAnalysisWithoutViewWrapper(
      Project(Seq(UnresolvedAttribute("tBl.a")),
        SubqueryAlias("TbL", UnresolvedRelation(TableIdentifier("TaBlE")))),
      Project(testRelation.output, testRelation),
      caseSensitive = false)
  }

  test("SPARK-42108: transform count(*) to count(1)") {
    val a = testRelation.output(0)

    checkAnalysis(
      Project(
        Alias(UnresolvedFunction("count" :: Nil,
          UnresolvedStar(None) :: Nil, isDistinct = false), "x")() :: Nil, testRelation),
      Aggregate(Nil, count(Literal(1)).as("x") :: Nil, testRelation))

    checkAnalysis(
      Project(
        Alias(UnresolvedFunction("count" :: Nil,
          UnresolvedStar(None) :: Nil, isDistinct = false), "x")() ::
          Alias(UnresolvedFunction("count" :: Nil,
            UnresolvedAttribute("a") :: Nil, isDistinct = false), "y")() :: Nil, testRelation),
      Aggregate(Nil, count(Literal(1)).as("x") :: count(a).as("y") :: Nil, testRelation))
  }

  test("resolve sort references - filter/limit") {
    val a = testRelation2.output(0)
    val b = testRelation2.output(1)
    val c = testRelation2.output(2)

    // Case 1: one missing attribute is in the leaf node and another is in the unary node
    val plan1 = testRelation2
      .where($"a" > "str").select($"a", $"b")
      .where($"b" > "str").select($"a")
      .sortBy($"b".asc, $"c".desc)
    val expected1 = testRelation2
      .where(a > "str").select(a, b, c)
      .where(b > "str").select(a, b, c)
      .sortBy(b.asc, c.desc)
      .select(a)
    checkAnalysis(plan1, expected1)

    // Case 2: all the missing attributes are in the leaf node
    val plan2 = testRelation2
      .where($"a" > "str").select($"a")
      .where($"a" > "str").select($"a")
      .sortBy($"b".asc, $"c".desc)
    val expected2 = testRelation2
      .where(a > "str").select(a, b, c)
      .where(a > "str").select(a, b, c)
      .sortBy(b.asc, c.desc)
      .select(a)
    checkAnalysis(plan2, expected2)
  }

  test("resolve sort references - join") {
    val a = testRelation2.output(0)
    val b = testRelation2.output(1)
    val c = testRelation2.output(2)
    val h = testRelation3.output(3)

    // Case: join itself can resolve all the missing attributes
    val plan = testRelation2.join(testRelation3)
      .where($"a" > "str").select($"a", $"b")
      .sortBy($"c".desc, $"h".asc)
    val expected = testRelation2.join(testRelation3)
      .where(a > "str").select(a, b, c, h)
      .sortBy(c.desc, h.asc)
      .select(a, b)
    checkAnalysis(plan, expected)
  }

  test("resolve sort references - aggregate") {
    val a = testRelation2.output(0)
    val b = testRelation2.output(1)
    val c = testRelation2.output(2)
    val alias_a3 = count(a).as("a3")

    // Case 1: when the child of Sort is not Aggregate,
    //   the sort reference is handled by the rule ResolveMissingReferences
    val plan1 = testRelation2
      .groupBy($"a", $"c", $"b")($"a", $"c", count($"a").as("a3"))
      .select($"a", $"c", $"a3")
      .orderBy($"b".asc)

    val expected1 = testRelation2
      .groupBy(a, c, b)(a, c, alias_a3, b)
      .select(a, c, alias_a3.toAttribute, b)
      .orderBy(b.asc)
      .select(a, c, alias_a3.toAttribute)

    checkAnalysis(plan1, expected1)

    // Case 2: when the child of Sort is Aggregate,
    //   the sort reference is handled by the rule ResolveAggregateFunctions
    val plan2 = testRelation2
      .groupBy($"a", $"c", $"b")($"a", $"c", count($"a").as("a3"))
      .orderBy($"b".asc)

    val expected2 = testRelation2
      .groupBy(a, c, b)(a, c, alias_a3, b)
      .orderBy(b.asc)
      .select(a, c, alias_a3.toAttribute)

    checkAnalysis(plan2, expected2)
  }

  test("resolve relations") {
    assertAnalysisError(UnresolvedRelation(TableIdentifier("tAbLe")), Seq())
    checkAnalysisWithoutViewWrapper(UnresolvedRelation(TableIdentifier("TaBlE")), testRelation)
    checkAnalysisWithoutViewWrapper(
      UnresolvedRelation(TableIdentifier("tAbLe")), testRelation, caseSensitive = false)
    checkAnalysisWithoutViewWrapper(
      UnresolvedRelation(TableIdentifier("TaBlE")), testRelation, caseSensitive = false)
  }

  test("divide should be casted into fractional types") {
    val plan = getAnalyzer.execute(
      testRelation2.select(
        $"a" / Literal(2) as "div1",
        $"a" / $"b" as "div2",
        $"a" / $"c" as "div3",
        $"a" / $"d" as "div4",
        $"e" / $"e" as "div5"))
    val pl = plan.asInstanceOf[Project].projectList

    assert(pl(0).dataType == DoubleType)
    if (!SQLConf.get.ansiEnabled) {
      assert(pl(1).dataType == DoubleType)
    }
    assert(pl(2).dataType == DoubleType)
    assert(pl(3).dataType == DoubleType)
    assert(pl(4).dataType == DoubleType)
  }

  test("pull out nondeterministic expressions from RepartitionByExpression") {
    val plan = RepartitionByExpression(Seq(Rand(33)), testRelation, numPartitions = 10)
    val projected = Alias(Rand(33), "_nondeterministic")()
    val expected =
      Project(testRelation.output,
        RepartitionByExpression(Seq(projected.toAttribute),
          Project(testRelation.output :+ projected, testRelation),
          numPartitions = 10))
    checkAnalysis(plan, expected)
  }

  test("pull out nondeterministic expressions from Sort") {
    val plan = Sort(Seq(SortOrder(Rand(33), Ascending)), false, testRelation)
    val projected = Alias(Rand(33), "_nondeterministic")()
    val expected =
      Project(testRelation.output,
        Sort(Seq(SortOrder(projected.toAttribute, Ascending)), false,
          Project(testRelation.output :+ projected, testRelation)))
    checkAnalysis(plan, expected)
  }

  test("SPARK-9634: cleanup unnecessary Aliases in LogicalPlan") {
    val a = testRelation.output.head
    var plan = testRelation.select(((a + 1).as("a+1") + 2).as("col"))
    var expected = testRelation.select((a + 1 + 2).as("col"))
    checkAnalysis(plan, expected)

    plan = testRelation.groupBy(a.as("a1").as("a2"))((min(a).as("min_a") + 1).as("col"))
    expected = testRelation.groupBy(a)((min(a) + 1).as("col"))
    checkAnalysis(plan, expected)

    // CreateStruct is a special case that we should not trim Alias for it.
    plan = testRelation.select(CreateStruct(Seq(a, (a + 1).as("a+1"))).as("col"))
    expected = testRelation.select(CreateNamedStruct(Seq(
      Literal(a.name), a,
      Literal("a+1"), (a + 1))).as("col"))
    checkAnalysis(plan, expected)
  }

  test("Analysis may leave unnecessary aliases") {
    val att1 = testRelation.output.head
    var plan = testRelation.select(
      CreateStruct(Seq(att1, ((att1.as("aa")) + 1).as("a_plus_1"))).as("col"),
      att1
    )
    val prevPlan = getAnalyzer.execute(plan)
    plan = prevPlan.select(CreateArray(Seq(
      CreateStruct(Seq(att1, (att1 + 1).as("a_plus_1"))).as("col1"),
      /** alias should be eliminated by [[CleanupAliases]] */
      "col".attr.as("col2")
    )).as("arr"))
    plan = getAnalyzer.execute(plan)

    val expectedPlan = prevPlan.select(
      CreateArray(Seq(
        CreateNamedStruct(Seq(
          Literal(att1.name), att1,
          Literal("a_plus_1"), (att1 + 1))),
          $"col".struct(prevPlan.output(0).dataType.asInstanceOf[StructType]).notNull
      )).as("arr")
    )

    checkAnalysis(plan, expectedPlan)
  }

  test("SPARK-10534: resolve attribute references in order by clause") {
    val a = testRelation2.output(0)
    val c = testRelation2.output(2)

    val plan = testRelation2.select($"c").orderBy(Floor($"a").asc)
    val expected = testRelation2.select(c, a)
      .orderBy(Floor(Cast(a, DoubleType, Option(TimeZone.getDefault().getID))).asc).select(c)

    checkAnalysis(plan, expected)
  }

  test("self intersect should resolve duplicate expression IDs") {
    val plan = testRelation.intersect(testRelation, isAll = false)
    assertAnalysisSuccess(plan)
  }

  test("SPARK-8654: invalid CAST in NULL IN(...) expression") {
    val plan = Project(Alias(In(Literal(null), Seq(Literal(1), Literal(2))), "a")() :: Nil,
      LocalRelation()
    )
    assertAnalysisSuccess(plan)
  }

  test("SPARK-8654: different types in inlist but can be converted to a common type") {
    val plan = Project(Alias(In(Literal(null), Seq(Literal(1), Literal(1.2345))), "a")() :: Nil,
      LocalRelation()
    )
    assertAnalysisSuccess(plan)
  }

  test("SPARK-8654: check type compatibility error") {
    val plan = Project(Alias(In(Literal(null), Seq(Literal(true), Literal(1))), "a")() :: Nil,
      LocalRelation()
    )
    assertAnalysisErrorCondition(
      plan,
      "DATATYPE_MISMATCH.DATA_DIFF_TYPES",
      Map(
        "functionName" -> "`in`",
        "dataType" -> "[\"VOID\", \"BOOLEAN\", \"INT\"]",
        "sqlExpr" -> "\"(NULL IN (true, 1))\""))
  }

  test("SPARK-11725: correctly handle null inputs for ScalaUDF") {
    def resolvedEncoder[T : TypeTag](): ExpressionEncoder[T] = {
      ExpressionEncoder[T]().resolveAndBind()
    }

    val testRelation = LocalRelation(
      AttributeReference("a", StringType)(),
      AttributeReference("b", DoubleType)(),
      AttributeReference("c", ShortType)(),
      AttributeReference("d", DoubleType, nullable = false)())

    val string = testRelation.output(0)
    val double = testRelation.output(1)
    val short = testRelation.output(2)
    val nonNullableDouble = testRelation.output(3)
    val nullResult = Literal.create(null, StringType)

    def checkUDF(udf: Expression, transformed: Expression): Unit = {
      checkAnalysis(
        Project(Alias(udf, "")() :: Nil, testRelation),
        Project(Alias(transformed, "")() :: Nil, testRelation)
      )
    }

    // non-primitive parameters do not need special null handling
    val udf1 = ScalaUDF((s: String) => "x", StringType, string :: Nil,
      Option(resolvedEncoder[String]()) :: Nil)
    val expected1 = udf1
    checkUDF(udf1, expected1)

    // only primitive parameter needs special null handling
    val udf2 = ScalaUDF((s: String, d: Double) => "x", StringType, string :: double :: Nil,
      Option(resolvedEncoder[String]()) :: Option(resolvedEncoder[Double]()) :: Nil)
    val expected2 =
      If(IsNull(double), nullResult, udf2.copy(children = string :: KnownNotNull(double) :: Nil))
    checkUDF(udf2, expected2)

    // special null handling should apply to all primitive parameters
    val udf3 = ScalaUDF((s: Short, d: Double) => "x", StringType, short :: double :: Nil,
      Option(resolvedEncoder[Short]()) :: Option(resolvedEncoder[Double]()) :: Nil)
    val expected3 = If(
      IsNull(short) || IsNull(double),
      nullResult,
      udf3.copy(children = KnownNotNull(short) :: KnownNotNull(double) :: Nil))
    checkUDF(udf3, expected3)

    // we can skip special null handling for primitive parameters that are not nullable
    val udf4 = ScalaUDF(
      (s: Short, d: Double) => "x",
      StringType,
      short :: nonNullableDouble :: Nil,
      Option(resolvedEncoder[Short]()) :: Option(resolvedEncoder[Double]()) :: Nil)
    val expected4 = If(
      IsNull(short),
      nullResult,
      udf4.copy(children = KnownNotNull(short) :: nonNullableDouble :: Nil))
    checkUDF(udf4, expected4)
  }

  test("SPARK-24891 Fix HandleNullInputsForUDF rule") {
    val a = testRelation.output(0)
    val func = (x: Int, y: Int) => x + y
    val udf1 = ScalaUDF(func, IntegerType, a :: a :: Nil,
      Option(ExpressionEncoder[java.lang.Integer]()) ::
        Option(ExpressionEncoder[java.lang.Integer]()) :: Nil)
    val udf2 = ScalaUDF(func, IntegerType, a :: udf1 :: Nil,
      Option(ExpressionEncoder[java.lang.Integer]()) ::
        Option(ExpressionEncoder[java.lang.Integer]()) :: Nil)
    val plan = Project(Alias(udf2, "")() :: Nil, testRelation)
    comparePlans(plan.analyze, plan.analyze.analyze)
  }

  test("SPARK-11863 mixture of aliases and real columns in order by clause - tpcds 19,55,71") {
    val a = testRelation2.output(0)
    val c = testRelation2.output(2)
    val alias1 = a.as("a1")
    val alias2 = c.as("a2")
    val alias3 = count(a).as("a3")

    val plan = testRelation2
      .groupBy($"a", $"c")($"a".as("a1"), $"c".as("a2"), count($"a").as("a3"))
      .orderBy($"a1".asc, $"c".asc)

    val expected = testRelation2
      .groupBy(a, c)(alias1, alias2, alias3)
      .orderBy(alias1.toAttribute.asc, alias2.toAttribute.asc)
    checkAnalysis(plan, expected)
  }

  test("Eliminate the unnecessary union") {
    val plan = Union(testRelation :: Nil)
    val expected = testRelation
    checkAnalysis(plan, expected)
  }

  test("SPARK-12102: Ignore nullability when comparing two sides of case") {
    val relation = LocalRelation($"a".struct($"x".int),
      $"b".struct($"x".int.withNullability(false)))
    val plan = relation.select(
      CaseWhen(Seq((Literal(true), $"a".attr)), $"b").as("val"))
    assertAnalysisSuccess(plan)
  }

  test("Keep attribute qualifiers after dedup") {
    val input = LocalRelation($"key".int, $"value".string)

    val query =
      Project(Seq($"x.key", $"y.key"),
        Join(
          Project(Seq($"x.key"), SubqueryAlias("x", input)),
          Project(Seq($"y.key"), SubqueryAlias("y", input)),
          Cross, None, JoinHint.NONE))

    assertAnalysisSuccess(query)
  }

  private def assertExpressionType(
      expression: Expression,
      expectedDataType: DataType): Unit = {
    val afterAnalyze =
      Project(Seq(Alias(expression, "a")()), OneRowRelation()).analyze.expressions.head
    if (!afterAnalyze.dataType.equals(expectedDataType)) {
      fail(
        s"""
           |data type of expression $expression doesn't match expected:
           |Actual data type:
           |${afterAnalyze.dataType}
           |
           |Expected data type:
           |${expectedDataType}
         """.stripMargin)
    }
  }

  test("SPARK-15776: test whether Divide expression's data type can be deduced correctly by " +
    "analyzer") {
    assertExpressionType(sum(Divide(1, 2)), DoubleType)
    assertExpressionType(sum(Divide(1.0, 2)), DoubleType)
    assertExpressionType(sum(Divide(1, 2.0)), DoubleType)
    assertExpressionType(sum(Divide(1.0, 2.0)), DoubleType)
    assertExpressionType(sum(Divide(1, 2.0f)), DoubleType)
    assertExpressionType(sum(Divide(1.0f, 2)), DoubleType)
    assertExpressionType(sum(Divide(1, Decimal(2))), DecimalType(22, 11))
    assertExpressionType(sum(Divide(Decimal(1), 2)), DecimalType(26, 6))
    assertExpressionType(sum(Divide(Decimal(1), 2.0)), DoubleType)
    assertExpressionType(sum(Divide(1.0, Decimal(2.0))), DoubleType)
  }

  test("SPARK-18058: union and set operations shall not care about the nullability" +
    " when comparing column types") {
    val firstTable = LocalRelation(
      AttributeReference("a",
        StructType(Seq(StructField("a", IntegerType, nullable = true))), nullable = false)())
    val secondTable = LocalRelation(
      AttributeReference("a",
        StructType(Seq(StructField("a", IntegerType, nullable = false))), nullable = false)())

    val unionPlan = Union(firstTable, secondTable)
    assertAnalysisSuccess(unionPlan)

    val r1 = Except(firstTable, secondTable, isAll = false)
    val r2 = Intersect(firstTable, secondTable, isAll = false)

    assertAnalysisSuccess(r1)
    assertAnalysisSuccess(r2)
  }

  test("resolve as with an already existed alias") {
    checkAnalysis(
      Project(Seq(UnresolvedAttribute("tbl2.a")),
        SubqueryAlias("tbl", testRelation).as("tbl2")),
      Project(testRelation.output, testRelation),
      caseSensitive = false)

    checkAnalysis(SubqueryAlias("tbl", testRelation).as("tbl2"), testRelation)
  }

  test("SPARK-20311 range(N) as alias") {
    def rangeWithAliases(args: Seq[Int], outputNames: Seq[String]): LogicalPlan = {
      SubqueryAlias("t",
        UnresolvedTVFAliases("range",
          UnresolvedTableValuedFunction("range", args.map(Literal(_))), outputNames)
        .select(star()))
    }
    assertAnalysisSuccess(rangeWithAliases(3 :: Nil, "a" :: Nil))
    assertAnalysisSuccess(rangeWithAliases(1 :: 4 :: Nil, "b" :: Nil))
    assertAnalysisSuccess(rangeWithAliases(2 :: 6 :: 2 :: Nil, "c" :: Nil))
    assertAnalysisErrorCondition(
      rangeWithAliases(3 :: Nil, "a" :: "b" :: Nil),
      "NUM_TABLE_VALUE_ALIASES_MISMATCH",
      Map("funcName" -> "`range`", "aliasesNum" -> "2", "outColsNum" -> "1"))
  }

  test("SPARK-20841 Support table column aliases in FROM clause") {
    def tableColumnsWithAliases(outputNames: Seq[String]): LogicalPlan = {
      UnresolvedSubqueryColumnAliases(
        outputNames,
        SubqueryAlias("t", UnresolvedRelation(TableIdentifier("TaBlE3")))
      ).select(star())
    }
    assertAnalysisSuccess(tableColumnsWithAliases("col1" :: "col2" :: "col3" :: "col4" :: Nil))
    assertAnalysisErrorCondition(
      tableColumnsWithAliases("col1" :: Nil),
      "ASSIGNMENT_ARITY_MISMATCH",
      Map("numExpr" -> "1", "numTarget" -> "4")
    )
    assertAnalysisErrorCondition(
      tableColumnsWithAliases("col1" :: "col2" :: "col3" :: "col4" :: "col5" :: Nil),
      "ASSIGNMENT_ARITY_MISMATCH",
      Map("numExpr" -> "5", "numTarget" -> "4")
    )
  }

  test("SPARK-20962 Support subquery column aliases in FROM clause") {
    def tableColumnsWithAliases(outputNames: Seq[String]): LogicalPlan = {
      UnresolvedSubqueryColumnAliases(
        outputNames,
        SubqueryAlias(
          "t",
          UnresolvedRelation(TableIdentifier("TaBlE3")))
      ).select(star())
    }
    assertAnalysisSuccess(tableColumnsWithAliases("col1" :: "col2" :: "col3" :: "col4" :: Nil))
    assertAnalysisErrorCondition(
      tableColumnsWithAliases("col1" :: Nil),
      "ASSIGNMENT_ARITY_MISMATCH",
      Map("numExpr" -> "1", "numTarget" -> "4")
    )
    assertAnalysisErrorCondition(
      tableColumnsWithAliases("col1" :: "col2" :: "col3" :: "col4" :: "col5" :: Nil),
      "ASSIGNMENT_ARITY_MISMATCH",
      Map("numExpr" -> "5", "numTarget" -> "4")
    )
  }

  test("SPARK-20963 Support aliases for join relations in FROM clause") {
    def joinRelationWithAliases(outputNames: Seq[String]): LogicalPlan = {
      val src1 = LocalRelation($"id".int, $"v1".string).as("s1")
      val src2 = LocalRelation($"id".int, $"v2".string).as("s2")
      UnresolvedSubqueryColumnAliases(
        outputNames,
        SubqueryAlias(
          "dst",
          src1.join(src2, Inner, Option($"s1.id" === $"s2.id")))
      ).select(star())
    }
    assertAnalysisSuccess(joinRelationWithAliases("col1" :: "col2" :: "col3" :: "col4" :: Nil))
    assertAnalysisErrorCondition(
      joinRelationWithAliases("col1" :: Nil),
      "ASSIGNMENT_ARITY_MISMATCH",
      Map("numExpr" -> "1", "numTarget" -> "4")
    )
    assertAnalysisErrorCondition(
      joinRelationWithAliases("col1" :: "col2" :: "col3" :: "col4" :: "col5" :: Nil),
        "ASSIGNMENT_ARITY_MISMATCH",
        Map("numExpr" -> "5", "numTarget" -> "4")
      )
  }

  test("SPARK-22614 RepartitionByExpression partitioning") {
    def checkPartitioning[T <: Partitioning: ClassTag](
        numPartitions: Int, exprs: Expression*): Unit = {
      val partitioning = RepartitionByExpression(exprs, testRelation2, numPartitions).partitioning
      val clazz = implicitly[ClassTag[T]].runtimeClass
      assert(clazz.isInstance(partitioning))
    }

    checkPartitioning[HashPartitioning](numPartitions = 10, exprs = Literal(20))
    checkPartitioning[HashPartitioning](numPartitions = 10,
      exprs = $"a".attr, $"b".attr)

    checkPartitioning[RangePartitioning](numPartitions = 10,
      exprs = SortOrder(Literal(10), Ascending))
    checkPartitioning[RangePartitioning](numPartitions = 10,
      exprs = SortOrder($"a".attr, Ascending), SortOrder($"b".attr, Descending))

    checkPartitioning[RoundRobinPartitioning](numPartitions = 10, exprs = Seq.empty: _*)

    intercept[IllegalArgumentException] {
      checkPartitioning(numPartitions = 0, exprs = Literal(20))
    }
    intercept[IllegalArgumentException] {
      checkPartitioning(numPartitions = -1, exprs = Literal(20))
    }
    intercept[IllegalArgumentException] {
      checkPartitioning(numPartitions = 10, exprs =
        SortOrder($"a".attr, Ascending), $"b".attr)
    }
  }

  test("SPARK-24208: analysis fails on self-join with FlatMapGroupsInPandas") {
    val pythonUdf = PythonUDF("pyUDF", null,
      StructType(Seq(StructField("a", LongType))),
      Seq.empty,
      PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF,
      true)
    val output = DataTypeUtils.toAttributes(pythonUdf.dataType.asInstanceOf[StructType])
    val project = Project(Seq(UnresolvedAttribute("a")), testRelation)
    val flatMapGroupsInPandas = FlatMapGroupsInPandas(
      Seq(UnresolvedAttribute("a")), pythonUdf, output, project)
    val left = SubqueryAlias("temp0", flatMapGroupsInPandas)
    val right = SubqueryAlias("temp1", flatMapGroupsInPandas)
    val join = Join(left, right, Inner, None, JoinHint.NONE)
    assertAnalysisSuccess(
      Project(Seq(UnresolvedAttribute("temp0.a"), UnresolvedAttribute("temp1.a")), join))
  }

  test("SPARK-34319: analysis fails on self-join with FlatMapCoGroupsInPandas") {
    val pythonUdf = PythonUDF("pyUDF", null,
      StructType(Seq(StructField("a", LongType))),
      Seq.empty,
      PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF,
      true)
    val output = DataTypeUtils.toAttributes(pythonUdf.dataType.asInstanceOf[StructType])
    val project1 = Project(Seq(UnresolvedAttribute("a")), testRelation)
    val project2 = Project(Seq(UnresolvedAttribute("a")), testRelation2)
    val flatMapGroupsInPandas = FlatMapCoGroupsInPandas(
      1,
      1,
      pythonUdf,
      output,
      project1,
      project2)
    val left = SubqueryAlias("temp0", flatMapGroupsInPandas)
    val right = SubqueryAlias("temp1", flatMapGroupsInPandas)
    val join = Join(left, right, Inner, None, JoinHint.NONE)
    assertAnalysisSuccess(
      Project(Seq(UnresolvedAttribute("temp0.a"), UnresolvedAttribute("temp1.a")), join))
  }

  test("SPARK-34319: analysis fails on self-join with MapInPandas") {
    val pythonUdf = PythonUDF("pyUDF", null,
      StructType(Seq(StructField("a", LongType))),
      Seq.empty,
      PythonEvalType.SQL_MAP_PANDAS_ITER_UDF,
      true)
    val output = DataTypeUtils.toAttributes(pythonUdf.dataType.asInstanceOf[StructType])
    val project = Project(Seq(UnresolvedAttribute("a")), testRelation)
    val mapInPandas = MapInPandas(
      pythonUdf,
      output,
      project,
      false,
      None)
    val left = SubqueryAlias("temp0", mapInPandas)
    val right = SubqueryAlias("temp1", mapInPandas)
    val join = Join(left, right, Inner, None, JoinHint.NONE)
    assertAnalysisSuccess(
      Project(Seq(UnresolvedAttribute("temp0.a"), UnresolvedAttribute("temp1.a")), join))
  }

  test("SPARK-45930: MapInPandas with non-deterministic UDF") {
    val pythonUdf = PythonUDF("pyUDF", null,
      StructType(Seq(StructField("a", LongType))),
      Seq.empty,
      PythonEvalType.SQL_MAP_PANDAS_ITER_UDF,
      false)
    val output = DataTypeUtils.toAttributes(pythonUdf.dataType.asInstanceOf[StructType])
    val project = Project(Seq(UnresolvedAttribute("a")), testRelation)
    val mapInPandas = MapInPandas(
      pythonUdf,
      output,
      project,
      false,
      None)
    assertAnalysisSuccess(mapInPandas)
  }

  test("SPARK-45930: MapInArrow with non-deterministic UDF") {
    val pythonUdf = PythonUDF("pyUDF", null,
      StructType(Seq(StructField("a", LongType))),
      Seq.empty,
      PythonEvalType.SQL_MAP_ARROW_ITER_UDF,
      false)
    val output = DataTypeUtils.toAttributes(pythonUdf.dataType.asInstanceOf[StructType])
    val project = Project(Seq(UnresolvedAttribute("a")), testRelation)
    val mapInArrow = MapInArrow(
      pythonUdf,
      output,
      project,
      false,
      None)
    assertAnalysisSuccess(mapInArrow)
  }

  test("SPARK-34741: Avoid ambiguous reference in MergeIntoTable") {
    val cond = $"a" > 1
    assertAnalysisErrorCondition(
      MergeIntoTable(
        testRelation,
        testRelation,
        cond,
        matchedActions = UpdateAction(Some(cond), Assignment($"a", $"a") :: Nil) :: Nil,
        notMatchedActions = Nil,
        notMatchedBySourceActions = Nil,
        withSchemaEvolution = false
      ),
      "AMBIGUOUS_REFERENCE",
      Map("name" -> "`a`", "referenceNames" -> "[`a`, `a`]"))
  }

  test("SPARK-24488 Generator with multiple aliases") {
    assertAnalysisSuccess(
      listRelation.select(Explode($"list").as("first_alias").as("second_alias")))
    assertAnalysisSuccess(
      listRelation.select(MultiAlias(MultiAlias(
        PosExplode($"list"), Seq("first_pos", "first_val")), Seq("second_pos", "second_val"))))
  }

  test("SPARK-24151: CURRENT_DATE, CURRENT_TIMESTAMP should be case insensitive") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val input = Project(Seq(
        UnresolvedAttribute("current_date"),
        UnresolvedAttribute("CURRENT_DATE"),
        UnresolvedAttribute("CURRENT_TIMESTAMP"),
        UnresolvedAttribute("current_timestamp")), testRelation)
      val expected = Project(Seq(
        Alias(CurrentDate(), toPrettySQL(CurrentDate()))(),
        Alias(CurrentDate(), toPrettySQL(CurrentDate()))(),
        Alias(CurrentTimestamp(), toPrettySQL(CurrentTimestamp()))(),
        Alias(CurrentTimestamp(), toPrettySQL(CurrentTimestamp()))()), testRelation).analyze
      checkAnalysis(input, expected)
    }
  }

  test("CTE with non-existing column alias") {
    assertAnalysisErrorCondition(parsePlan("WITH t(x) AS (SELECT 1) SELECT * FROM t WHERE y = 1"),
      "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      Map("objectName" -> "`y`", "proposal" -> "`x`"),
      Array(ExpectedContext("y", 46, 46))
    )
  }

  test("CTE with non-matching column alias") {
    assertAnalysisErrorCondition(
      parsePlan("WITH t(x, y) AS (SELECT 1) SELECT * FROM t WHERE x = 1"),
      "ASSIGNMENT_ARITY_MISMATCH",
      Map("numExpr" -> "2", "numTarget" -> "1"),
      Array(ExpectedContext("t(x, y) AS (SELECT 1)", 5, 25))
    )
  }

  test("SPARK-28251: Insert into non-existing table error message is user friendly") {
    assertAnalysisErrorCondition(parsePlan("INSERT INTO test VALUES (1)"),
      "TABLE_OR_VIEW_NOT_FOUND", Map("relationName" -> "`test`"),
      Array(ExpectedContext("test", 12, 15)))
  }

  test("check CollectMetrics resolved") {
    val a = testRelation.output.head
    val sum = Sum(a).toAggregateExpression().as("sum")
    val random_sum = Sum(Rand(1L)).toAggregateExpression().as("rand_sum")
    val literal = Literal(1).as("lit")

    // Ok
    assert(CollectMetrics("event", literal :: sum :: random_sum :: Nil, testRelation, 0).resolved)

    // Bad name
    assert(!CollectMetrics("", sum :: Nil, testRelation, 0).resolved)
    assertAnalysisErrorCondition(
      CollectMetrics("", sum :: Nil, testRelation, 0),
      expectedErrorCondition = "INVALID_OBSERVED_METRICS.MISSING_NAME",
      expectedMessageParameters = Map(
        "operator" ->
          "'CollectMetrics , [sum(a#x) AS sum#xL], 0\n+- LocalRelation <empty>, [a#x]\n")
    )

    // No columns
    assert(!CollectMetrics("evt", Nil, testRelation, 0).resolved)

    // non-deterministic expression inside an aggregate function is valid
    val tsLiteral = Literal.create(java.sql.Timestamp.valueOf("2023-11-30 21:05:00.000000"),
      TimestampType)

    assertAnalysisSuccess(
      CollectMetrics(
        "invalid",
        Count(
          GreaterThan(tsLiteral, CurrentBatchTimestamp(1699485296000L, TimestampType))
        ).as("count") :: Nil,
        testRelation,
        0
      )
    )

    // Unwrapped attribute
    assertAnalysisErrorCondition(
      CollectMetrics("event", a :: Nil, testRelation, 0),
      expectedErrorCondition = "INVALID_OBSERVED_METRICS.NON_AGGREGATE_FUNC_ARG_IS_ATTRIBUTE",
      expectedMessageParameters = Map("expr" -> "\"a\"")
    )

    // Unwrapped non-deterministic expression
    assertAnalysisErrorCondition(
      CollectMetrics("event", Rand(10).as("rnd") :: Nil, testRelation, 0),
      expectedErrorCondition =
        "INVALID_OBSERVED_METRICS.NON_AGGREGATE_FUNC_ARG_IS_NON_DETERMINISTIC",
      expectedMessageParameters = Map("expr" -> "\"rand(10) AS rnd\"")
    )

    // Distinct aggregate
    assertAnalysisErrorCondition(
      CollectMetrics(
        "event",
        Sum(a).toAggregateExpression(isDistinct = true).as("sum") :: Nil,
        testRelation, 0),
      expectedErrorCondition =
        "INVALID_OBSERVED_METRICS.AGGREGATE_EXPRESSION_WITH_DISTINCT_UNSUPPORTED",
      expectedMessageParameters = Map("expr" -> "\"sum(DISTINCT a) AS sum\"")
    )

    // Nested aggregate
    assertAnalysisErrorCondition(
      CollectMetrics(
        "event",
        Sum(Sum(a).toAggregateExpression()).toAggregateExpression().as("sum") :: Nil,
        testRelation, 0),
      expectedErrorCondition = "INVALID_OBSERVED_METRICS.NESTED_AGGREGATES_UNSUPPORTED",
      expectedMessageParameters = Map("expr" -> "\"sum(sum(a)) AS sum\"")
    )

    // Windowed aggregate
    val windowExpr = WindowExpression(
      RowNumber(),
      WindowSpecDefinition(Nil, a.asc :: Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow)))
    assertAnalysisErrorCondition(
      CollectMetrics("event", windowExpr.as("rn") :: Nil, testRelation, 0),
      expectedErrorCondition = "INVALID_OBSERVED_METRICS.WINDOW_EXPRESSIONS_UNSUPPORTED",
      expectedMessageParameters = Map(
        "expr" ->
          """
            |"row_number() OVER (ORDER BY a ASC NULLS FIRST ROWS
            | BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rn"
            |""".stripMargin.replace("\n", ""))
    )
  }

  test("check CollectMetrics duplicates") {
    val a = testRelation.output.head
    val sum = Sum(a).toAggregateExpression().as("sum")
    val count = Count(Literal(1)).toAggregateExpression().as("cnt")

    // Same result - duplicate names are allowed
    assertAnalysisSuccess(Union(
      CollectMetrics("evt1", count :: Nil, testRelation, 0) ::
      CollectMetrics("evt1", count :: Nil, testRelation, 0) :: Nil))

    // Same children, structurally different metrics - fail
    assertAnalysisErrorCondition(
      Union(
        CollectMetrics("evt1", count :: Nil, testRelation, 0) ::
          CollectMetrics("evt1", sum :: Nil, testRelation, 1) :: Nil),
      expectedErrorCondition = "DUPLICATED_METRICS_NAME",
      expectedMessageParameters = Map("metricName" -> "evt1")
    )

    // Different children, same metrics - fail
    val b = $"b".string
    val tblB = LocalRelation(b)
    assertAnalysisErrorCondition(
      Union(
        CollectMetrics("evt1", count :: Nil, testRelation, 0) ::
          CollectMetrics("evt1", count :: Nil, tblB, 1) :: Nil),
      expectedErrorCondition = "DUPLICATED_METRICS_NAME",
      expectedMessageParameters = Map("metricName" -> "evt1")
    )

    // Subquery different tree - fail
    val subquery = Aggregate(Nil, sum :: Nil, CollectMetrics("evt1", count :: Nil, testRelation, 0))
    val query = Project(
      b :: ScalarSubquery(subquery, Nil).as("sum") :: Nil,
      CollectMetrics("evt1", count :: Nil, tblB, 1))
    assertAnalysisErrorCondition(
      query,
      expectedErrorCondition = "DUPLICATED_METRICS_NAME",
      expectedMessageParameters = Map("metricName" -> "evt1")
    )

    // Aggregate with filter predicate - fail
    val sumWithFilter = sum.transform {
      case a: AggregateExpression => a.copy(filter = Some(true))
    }.asInstanceOf[NamedExpression]
    assertAnalysisErrorCondition(
      CollectMetrics("evt1", sumWithFilter :: Nil, testRelation, 0),
      expectedErrorCondition =
        "INVALID_OBSERVED_METRICS.AGGREGATE_EXPRESSION_WITH_FILTER_UNSUPPORTED",
      expectedMessageParameters = Map("expr" -> "\"sum(a) FILTER (WHERE true) AS sum\"")
    )
  }

  test("Canonicalize CollectMetrics") {
    assert(CollectMetrics("", Seq(Rand(10).as("rnd")), testRelation, 1).canonicalized ==
      CollectMetrics("", Seq(Rand(10).as("rnd")), testRelation, 2).canonicalized)
  }

  test("Analysis exceed max iterations") {
    // RuleExecutor only throw exception or log warning when the rule is supposed to run
    // more than once.
    val maxIterations = 2
    withSQLConf(SQLConf.ANALYZER_MAX_ITERATIONS.key -> maxIterations.toString) {
      val testAnalyzer = new Analyzer(
        new SessionCatalog(new InMemoryCatalog, FunctionRegistry.builtin))

      val plan = testRelation2.select(
        $"a" / Literal(2) as "div1",
        $"a" / $"b" as "div2",
        $"a" / $"c" as "div3",
        $"a" / $"d" as "div4",
        $"e" / $"e" as "div5")

      val message = intercept[RuntimeException] {
        testAnalyzer.execute(plan)
      }.getMessage
      assert(message.startsWith(s"Max iterations ($maxIterations) reached for batch Resolution, " +
        s"please set '${SQLConf.ANALYZER_MAX_ITERATIONS.key}' to a larger value."))
    }
  }

  test("SPARK-30886 Deprecate two-parameter TRIM/LTRIM/RTRIM") {
    Seq("trim", "ltrim", "rtrim").foreach { f =>
      val logAppender = new LogAppender("deprecated two-parameter TRIM/LTRIM/RTRIM functions")
      def check(count: Int): Unit = {
        val message = "Two-parameter TRIM/LTRIM/RTRIM function signatures are deprecated."
        assert(logAppender.loggingEvents.size == count)
        assert(logAppender.loggingEvents.exists(
          e => e.getLevel == Level.WARN &&
            e.getMessage.getFormattedMessage.contains(message)))
      }

      withLogAppender(logAppender) {
        val testAnalyzer1 = new Analyzer(
          new SessionCatalog(new InMemoryCatalog, FunctionRegistry.builtin))

        val plan1 = testRelation2.select(
          UnresolvedFunction(f, $"a" :: Nil, isDistinct = false))
        testAnalyzer1.execute(plan1)
        // One-parameter is not deprecated.
        assert(logAppender.loggingEvents.isEmpty)

        val plan2 = testRelation2.select(
          UnresolvedFunction(f, $"a" :: $"b" :: Nil, isDistinct = false))
        testAnalyzer1.execute(plan2)
        // Deprecation warning is printed out once.
        check(1)

        val plan3 = testRelation2.select(
          UnresolvedFunction(f, $"b" :: $"a" :: Nil, isDistinct = false))
        testAnalyzer1.execute(plan3)
        // There is no change in the log.
        check(1)

        // New analyzer from new SessionState
        val testAnalyzer2 = new Analyzer(
          new SessionCatalog(new InMemoryCatalog, FunctionRegistry.builtin))
        val plan4 = testRelation2.select(
          UnresolvedFunction(f, $"c" :: $"d" :: Nil, isDistinct = false))
        testAnalyzer2.execute(plan4)
        // Additional deprecation warning from new analyzer
        check(2)

        val plan5 = testRelation2.select(
          UnresolvedFunction(f, $"c" :: $"d" :: Nil, isDistinct = false))
        testAnalyzer2.execute(plan5)
        // There is no change in the log.
        check(2)
      }
    }
  }

  test("SPARK-32131: Fix wrong column index when we have more than two columns" +
    " during union and set operations" ) {
    val firstTable = LocalRelation(
      AttributeReference("a", StringType)(),
      AttributeReference("b", DoubleType)(),
      AttributeReference("c", IntegerType)(),
      AttributeReference("d", FloatType)())

    val secondTable = LocalRelation(
      AttributeReference("a", StringType)(),
      AttributeReference("b", TimestampType)(),
      AttributeReference("c", IntegerType)(),
      AttributeReference("d", FloatType)())

    val thirdTable = LocalRelation(
      AttributeReference("a", StringType)(),
      AttributeReference("b", DoubleType)(),
      AttributeReference("c", TimestampType)(),
      AttributeReference("d", FloatType)())

    val fourthTable = LocalRelation(
      AttributeReference("a", StringType)(),
      AttributeReference("b", DoubleType)(),
      AttributeReference("c", IntegerType)(),
      AttributeReference("d", TimestampType)())

    assertAnalysisErrorCondition(
      Union(firstTable, secondTable),
      expectedErrorCondition = "INCOMPATIBLE_COLUMN_TYPE",
      expectedMessageParameters = Map(
        "tableOrdinalNumber" -> "second",
        "columnOrdinalNumber" -> "second",
        "dataType2" -> "\"DOUBLE\"",
        "operator" -> "UNION",
        "hint" -> "",
        "dataType1" -> "\"TIMESTAMP\"")
    )

    assertAnalysisErrorCondition(
      Union(firstTable, thirdTable),
      expectedErrorCondition = "INCOMPATIBLE_COLUMN_TYPE",
      expectedMessageParameters = Map(
        "tableOrdinalNumber" -> "second",
        "columnOrdinalNumber" -> "third",
        "dataType2" -> "\"INT\"",
        "operator" -> "UNION",
        "hint" -> "",
        "dataType1" -> "\"TIMESTAMP\"")
    )

    assertAnalysisErrorCondition(
      Union(firstTable, fourthTable),
      expectedErrorCondition = "INCOMPATIBLE_COLUMN_TYPE",
      expectedMessageParameters = Map(
        "tableOrdinalNumber" -> "second",
        "columnOrdinalNumber" -> "4th",
        "dataType2" -> "\"FLOAT\"",
        "operator" -> "UNION",
        "hint" -> "",
        "dataType1" -> "\"TIMESTAMP\"")
    )

    assertAnalysisErrorCondition(
      Except(firstTable, secondTable, isAll = false),
      expectedErrorCondition = "INCOMPATIBLE_COLUMN_TYPE",
      expectedMessageParameters = Map(
        "tableOrdinalNumber" -> "second",
        "columnOrdinalNumber" -> "second",
        "dataType2" -> "\"DOUBLE\"",
        "operator" -> "EXCEPT",
        "hint" -> "",
        "dataType1" -> "\"TIMESTAMP\"")
    )

    assertAnalysisErrorCondition(
      Intersect(firstTable, secondTable, isAll = false),
      expectedErrorCondition = "INCOMPATIBLE_COLUMN_TYPE",
      expectedMessageParameters = Map(
        "tableOrdinalNumber" -> "second",
        "columnOrdinalNumber" -> "second",
        "dataType2" -> "\"DOUBLE\"",
        "operator" -> "INTERSECT",
        "hint" -> "",
        "dataType1" -> "\"TIMESTAMP\"")
    )
  }

  test("SPARK-31975: Throw user facing error when use WindowFunction directly") {
    assertAnalysisErrorCondition(
      inputPlan = testRelation2.select(RowNumber()),
      expectedErrorCondition = "WINDOW_FUNCTION_WITHOUT_OVER_CLAUSE",
      expectedMessageParameters = Map("funcName" -> "\"row_number()\"")
    )

    assertAnalysisErrorCondition(
      inputPlan = testRelation2.select(Sum(RowNumber())),
      expectedErrorCondition = "WINDOW_FUNCTION_WITHOUT_OVER_CLAUSE",
      expectedMessageParameters = Map("funcName" -> "\"row_number()\"")
    )

    assertAnalysisErrorCondition(
      inputPlan = testRelation2.select(RowNumber() + 1),
      expectedErrorCondition = "WINDOW_FUNCTION_WITHOUT_OVER_CLAUSE",
      expectedMessageParameters = Map("funcName" -> "\"row_number()\"")
    )
  }

  test("SPARK-32237: Hint in CTE") {
    val plan = UnresolvedWith(
      Project(
        Seq(UnresolvedAttribute("cte.a")),
        UnresolvedRelation(TableIdentifier("cte"))
      ),
      Seq(
        (
          "cte",
          SubqueryAlias(
            AliasIdentifier("cte"),
            UnresolvedHint(
              "REPARTITION",
              Seq(Literal(3)),
              Project(testRelation.output, testRelation)
            )
          )
        )
      )
    )
    assertAnalysisSuccess(plan)
  }

  test("SPARK-33197: Make sure changes to ANALYZER_MAX_ITERATIONS take effect at runtime") {
    // RuleExecutor only throw exception or log warning when the rule is supposed to run
    // more than once.
    val maxIterations = 2
    val maxIterationsEnough = 5
    withSQLConf(SQLConf.ANALYZER_MAX_ITERATIONS.key -> maxIterations.toString) {
      val testAnalyzer = new Analyzer(
        new SessionCatalog(new InMemoryCatalog, FunctionRegistry.builtin))

      val plan = testRelation2.select(
        $"a" / Literal(2) as "div1",
        $"a" / $"b" as "div2",
        $"a" / $"c" as "div3",
        $"a" / $"d" as "div4",
        $"e" / $"e" as "div5")

      val message1 = intercept[RuntimeException] {
        testAnalyzer.execute(plan)
      }.getMessage
      assert(message1.startsWith(s"Max iterations ($maxIterations) reached for batch Resolution, " +
        s"please set '${SQLConf.ANALYZER_MAX_ITERATIONS.key}' to a larger value."))

      withSQLConf(SQLConf.ANALYZER_MAX_ITERATIONS.key -> maxIterationsEnough.toString) {
        try {
          testAnalyzer.execute(plan)
        } catch {
          case ex: AnalysisException
            if ex.getMessage.contains(SQLConf.ANALYZER_MAX_ITERATIONS.key) =>
              fail("analyzer.execute should not reach max iterations.")
        }
      }

      val message2 = intercept[RuntimeException] {
        testAnalyzer.execute(plan)
      }.getMessage
      assert(message2.startsWith(s"Max iterations ($maxIterations) reached for batch Resolution, " +
        s"please set '${SQLConf.ANALYZER_MAX_ITERATIONS.key}' to a larger value."))
    }
  }

  test("SPARK-33733: PullOutNondeterministic should check and collect deterministic field") {
    val reflect =
      CallMethodViaReflection(Seq("java.lang.Math", "abs", testRelation.output.head))
    val udf = ScalaUDF(
      (s: String) => s,
      StringType,
      Literal.create(null, StringType) :: Nil,
      Option(ExpressionEncoder[String]().resolveAndBind()) :: Nil,
      udfDeterministic = false)

    Seq(reflect, udf).foreach { e: Expression =>
      val plan = Sort(Seq(e.asc), false, testRelation)
      val projected = Alias(e, "_nondeterministic")()
      val expect =
        Project(testRelation.output,
          Sort(Seq(projected.toAttribute.asc), false,
            Project(testRelation.output :+ projected,
              testRelation)))
      checkAnalysis(plan, expect)
    }
  }

  test("SPARK-33857: Unify the default seed of random functions") {
    Seq(new Rand(), new Randn(), Shuffle(Literal(Array(1))), Uuid()).foreach { r =>
      assert(r.seedExpression == UnresolvedSeed)
      val p = getAnalyzer.execute(Project(Seq(r.as("r")), testRelation))
      assert(
        p.asInstanceOf[Project].projectList.head.asInstanceOf[Alias]
          .child.asInstanceOf[ExpressionWithRandomSeed]
          .seedExpression.isInstanceOf[Literal]
      )
    }
  }

  test("SPARK-22748: Analyze __grouping__id as a literal function") {
    assertAnalysisSuccess(parsePlan(
      """
        |SELECT grouping__id FROM (
        |  SELECT grouping__id FROM (
        |    SELECT a, b, count(1), grouping__id FROM TaBlE2
        |      GROUP BY a, b WITH ROLLUP
        |  )
        |)
      """.stripMargin), false)


    assertAnalysisSuccess(parsePlan(
      """
        |SELECT grouping__id FROM (
        |  SELECT a, b, count(1), grouping__id FROM TaBlE2
        |   GROUP BY a, b WITH CUBE
        |)
      """.stripMargin), false)

    assertAnalysisSuccess(parsePlan(
      """
        |SELECT grouping__id FROM (
        |  SELECT a, b, count(1), grouping__id FROM TaBlE2
        |    GROUP BY a, b GROUPING SETS ((a, b), ())
        |)
      """.stripMargin), false)

    assertAnalysisSuccess(parsePlan(
      """
        |SELECT a, b, count(1) FROM TaBlE2
        |  GROUP BY CUBE(a, b) HAVING grouping__id > 0
      """.stripMargin), false)

    assertAnalysisSuccess(parsePlan(
      """
        |SELECT * FROM (
        |  SELECT a, b, count(1), grouping__id FROM TaBlE2
        |    GROUP BY a, b GROUPING SETS ((a, b), ())
        |) WHERE grouping__id > 0
      """.stripMargin), false)

    assertAnalysisSuccess(parsePlan(
      """
        |SELECT * FROM (
        |  SELECT a, b, count(1), grouping__id FROM TaBlE2
        |    GROUP BY a, b GROUPING SETS ((a, b), ())
        |) ORDER BY grouping__id > 0
      """.stripMargin), false)

    assertAnalysisSuccess(parsePlan(
      """
        |SELECT a, b, count(1) FROM TaBlE2
        |  GROUP BY a, b GROUPING SETS ((a, b), ())
        |    ORDER BY grouping__id > 0
      """.stripMargin), false)

    assertAnalysisErrorCondition(
      parsePlan(
        """
          |SELECT grouping__id FROM (
          |  SELECT a, b, count(1), grouping__id FROM TaBlE2
          |    GROUP BY a, b
          |)
        """.stripMargin),
      "UNSUPPORTED_GROUPING_EXPRESSION",
      Map.empty,
      Array(ExpectedContext("grouping__id", 53, 64))
    )
  }

  test("SPARK-36275: Resolve aggregate functions should work with nested fields") {
    assertAnalysisSuccess(parsePlan(
      """
        |SELECT c.x, SUM(c.y)
        |FROM VALUES NAMED_STRUCT('x', 'A', 'y', 1), NAMED_STRUCT('x', 'A', 'y', 2) AS t(c)
        |GROUP BY c.x
        |HAVING c.x > 1
        |""".stripMargin))

    assertAnalysisSuccess(parsePlan(
      """
        |SELECT c.x, SUM(c.y)
        |FROM VALUES NAMED_STRUCT('x', 'A', 'y', 1), NAMED_STRUCT('x', 'A', 'y', 2) AS t(c)
        |GROUP BY c.x
        |ORDER BY c.x
        |""".stripMargin))

    assertAnalysisErrorCondition(parsePlan(
     """
        |SELECT c.x
        |FROM VALUES NAMED_STRUCT('x', 'A', 'y', 1), NAMED_STRUCT('x', 'A', 'y', 2) AS t(c)
        |GROUP BY c.x
        |ORDER BY c.x + c.y
        |""".stripMargin),
      "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      Map("objectName" -> "`c`.`y`", "proposal" -> "`x`"),
      Array(ExpectedContext("c.y", 123, 125))
    )
  }

  test("SPARK-38118: Func(wrong_type) in the HAVING clause should throw data mismatch error") {
    assertAnalysisErrorCondition(
      inputPlan = parsePlan(
        s"""
           |WITH t as (SELECT true c)
           |SELECT t.c
           |FROM t
           |GROUP BY t.c
           |HAVING mean(t.c) > 0d""".stripMargin),
      expectedErrorCondition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
      expectedMessageParameters = Map(
        "sqlExpr" -> "\"mean(c)\"",
        "paramIndex" -> "first",
        "inputSql" -> "\"c\"",
        "inputType" -> "\"BOOLEAN\"",
        "requiredType" -> "\"NUMERIC\" or \"ANSI INTERVAL\""),
      queryContext = Array(ExpectedContext("mean(t.c)", 65, 73)),
      caseSensitive = false
    )

    assertAnalysisErrorCondition(
      inputPlan = parsePlan(
        s"""
           |WITH t as (SELECT true c, false d)
           |SELECT (t.c AND t.d) c
           |FROM t
           |GROUP BY t.c, t.d
           |HAVING mean(c) > 0d""".stripMargin),
      expectedErrorCondition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
      expectedMessageParameters = Map(
        "sqlExpr" -> "\"mean(c)\"",
        "paramIndex" -> "first",
        "inputSql" -> "\"c\"",
        "inputType" -> "\"BOOLEAN\"",
        "requiredType" -> "\"NUMERIC\" or \"ANSI INTERVAL\""),
      queryContext = Array(ExpectedContext("mean(c)", 91, 97)),
      caseSensitive = false)

    assertAnalysisErrorCondition(
      inputPlan = parsePlan(
        s"""
           |WITH t as (SELECT true c)
           |SELECT t.c
           |FROM t
           |GROUP BY t.c
           |HAVING abs(t.c) > 0d""".stripMargin),
      expectedErrorCondition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
      expectedMessageParameters = Map(
        "sqlExpr" -> "\"abs(c)\"",
        "paramIndex" -> "first",
        "inputSql" -> "\"c\"",
        "inputType" -> "\"BOOLEAN\"",
        "requiredType" ->
          "(\"NUMERIC\" or \"INTERVAL DAY TO SECOND\" or \"INTERVAL YEAR TO MONTH\")"),
      queryContext = Array(ExpectedContext("abs(t.c)", 65, 72)),
      caseSensitive = false
    )

    assertAnalysisErrorCondition(
      inputPlan = parsePlan(
        s"""
         |WITH t as (SELECT true c, false d)
         |SELECT (t.c AND t.d) c
         |FROM t
         |GROUP BY t.c, t.d
         |HAVING abs(c) > 0d""".stripMargin),
      expectedErrorCondition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
      expectedMessageParameters = Map(
        "sqlExpr" -> "\"abs(c)\"",
        "paramIndex" -> "first",
        "inputSql" -> "\"c\"",
        "inputType" -> "\"BOOLEAN\"",
        "requiredType" ->
          "(\"NUMERIC\" or \"INTERVAL DAY TO SECOND\" or \"INTERVAL YEAR TO MONTH\")"),
      queryContext = Array(ExpectedContext("abs(c)", 91, 96)),
      caseSensitive = false
    )
  }

  test("SPARK-39354: should be [TABLE_OR_VIEW_NOT_FOUND]") {
    assertAnalysisErrorCondition(parsePlan(
      s"""
         |WITH t1 as (SELECT 1 user_id, CAST("2022-06-02" AS DATE) dt)
         |SELECT *
         |FROM t1
         |JOIN t2 ON t1.user_id = t2.user_id
         |WHERE t1.dt >= DATE_SUB('2020-12-27', 90)""".stripMargin),
      "TABLE_OR_VIEW_NOT_FOUND", Map("relationName" -> "`t2`"),
      Array(ExpectedContext("t2", 84, 85)))
  }

  test("SPARK-39144: nested subquery expressions deduplicate relations should be done bottom up") {
    val innerRelation = SubqueryAlias("src1", testRelation)
    val outerRelation = SubqueryAlias("src2", testRelation)
    val ref1 = testRelation.output.head

    val subPlan = getAnalyzer.execute(
      Project(
        Seq(UnresolvedStar(None)),
        Filter.apply(
          Exists(
            Filter.apply(
              EqualTo(
                OuterReference(ref1),
                ref1),
              innerRelation
            )
          ),
          outerRelation
        )))

    val finalPlan = {
      Union.apply(
        Project(
          Seq(UnresolvedStar(None)),
          subPlan
        ),
        Filter.apply(
          Exists(
            subPlan
          ),
          subPlan
        )
      )
    }

    assertAnalysisSuccess(finalPlan)
  }

  test("Execute Immediate plan transformation") {
    try {
    SimpleAnalyzer.catalogManager.tempVariableManager.create(
      "res", "1", Literal(1), overrideIfExists = true)
    SimpleAnalyzer.catalogManager.tempVariableManager.create(
      "res2", "1", Literal(1), overrideIfExists = true)
    val actual1 = parsePlan("EXECUTE IMMEDIATE 'SELECT 42 WHERE ? = 1' USING 2").analyze
    val expected1 = parsePlan("SELECT 42 where 2 = 1").analyze
    comparePlans(actual1, expected1)
    val actual2 = parsePlan(
      "EXECUTE IMMEDIATE 'SELECT 42 WHERE :first = 1' USING 2 as first").analyze
    val expected2 = parsePlan("SELECT 42 where 2 = 1").analyze
    comparePlans(actual2, expected2)
    // Test that plan is transformed to SET operation
    val actual3 = parsePlan(
      "EXECUTE IMMEDIATE 'SELECT 17, 7 WHERE ? = 1' INTO res, res2 USING 2").analyze
    val expected3 = parsePlan("SET var (res, res2) = (SELECT 17, 7 where 2 = 1)").analyze
      comparePlans(actual3, expected3)
    } finally {
      SimpleAnalyzer.catalogManager.tempVariableManager.remove("res")
      SimpleAnalyzer.catalogManager.tempVariableManager.remove("res2")
    }
  }

  test("SPARK-41271: bind named parameters to literals") {
    CTERelationDef.curId.set(0)
    val actual1 = NameParameterizedQuery(
      child = parsePlan("WITH a AS (SELECT 1 c) SELECT * FROM a LIMIT :limitA"),
      args = Map("limitA" -> Literal(10))).analyze
    CTERelationDef.curId.set(0)
    val expected1 = parsePlan("WITH a AS (SELECT 1 c) SELECT * FROM a LIMIT 10").analyze
    comparePlans(actual1, expected1)
    // Ignore unused arguments
    CTERelationDef.curId.set(0)
    val actual2 = NameParameterizedQuery(
      child = parsePlan("WITH a AS (SELECT 1 c) SELECT c FROM a WHERE c < :param2"),
      args = Map("param1" -> Literal(10), "param2" -> Literal(20))).analyze
    CTERelationDef.curId.set(0)
    val expected2 = parsePlan("WITH a AS (SELECT 1 c) SELECT c FROM a WHERE c < 20").analyze
    comparePlans(actual2, expected2)
  }

  test("SPARK-44066: bind positional parameters to literals") {
    CTERelationDef.curId.set(0)
    val actual1 = PosParameterizedQuery(
      child = parsePlan("WITH a AS (SELECT 1 c) SELECT * FROM a LIMIT ?"),
      args = Seq(Literal(10))).analyze
    CTERelationDef.curId.set(0)
    val expected1 = parsePlan("WITH a AS (SELECT 1 c) SELECT * FROM a LIMIT 10").analyze
    comparePlans(actual1, expected1)
    // Ignore unused arguments
    CTERelationDef.curId.set(0)
    val actual2 = PosParameterizedQuery(
      child = parsePlan("WITH a AS (SELECT 1 c) SELECT c FROM a WHERE c < ?"),
      args = Seq(Literal(20), Literal(10))).analyze
    CTERelationDef.curId.set(0)
    val expected2 = parsePlan("WITH a AS (SELECT 1 c) SELECT c FROM a WHERE c < 20").analyze
    comparePlans(actual2, expected2)
  }

  test("SPARK-41489: type of filter expression should be a bool") {
    assertAnalysisErrorCondition(parsePlan(
      s"""
         |WITH t1 as (SELECT 1 user_id)
         |SELECT *
         |FROM t1
         |WHERE 'true'""".stripMargin),
      expectedErrorCondition = "DATATYPE_MISMATCH.FILTER_NOT_BOOLEAN",
      expectedMessageParameters = Map(
        "sqlExpr" -> "\"true\"", "filter" -> "\"true\"", "type" -> "\"STRING\"")
      ,
      queryContext = Array(ExpectedContext("SELECT *\nFROM t1\nWHERE 'true'", 31, 59)))
  }

  test("SPARK-38591: resolve left and right CoGroup sort order on respective side only") {
    def func(k: Int, left: Iterator[Int], right: Iterator[Int]): Iterator[Int] = {
      Iterator.empty
    }

    implicit val intEncoder = ExpressionEncoder[Int]()

    val left = testRelation2.select($"e").analyze
    val right = testRelation3.select($"e").analyze
    val leftWithKey = AppendColumns[Int, Int]((x: Int) => x, left)
    val rightWithKey = AppendColumns[Int, Int]((x: Int) => x, right)
    val order = SortOrder($"e", Ascending)

    val cogroup = leftWithKey.cogroup[Int, Int, Int, Int](
      rightWithKey,
      func,
      leftWithKey.newColumns,
      rightWithKey.newColumns,
      left.output,
      right.output,
      order :: Nil,
      order :: Nil
    )

    // analyze the plan
    val actualPlan = getAnalyzer.executeAndCheck(cogroup, new QueryPlanningTracker)
    val cg = actualPlan.collectFirst {
      case cg: CoGroup => cg
    }
    // assert sort order reference only their respective plan
    assert(cg.isDefined)
    cg.foreach { cg =>
      assert(cg.leftOrder != cg.rightOrder)

      assert(cg.leftOrder.flatMap(_.references).nonEmpty)
      assert(cg.leftOrder.flatMap(_.references).forall(cg.left.output.contains))
      assert(!cg.leftOrder.flatMap(_.references).exists(cg.right.output.contains))

      assert(cg.rightOrder.flatMap(_.references).nonEmpty)
      assert(cg.rightOrder.flatMap(_.references).forall(cg.right.output.contains))
      assert(!cg.rightOrder.flatMap(_.references).exists(cg.left.output.contains))
    }
  }

  test("SPARK-40149: add metadata column with no extra project") {
    val t1 = LocalRelation($"key".int, $"value".string).as("t1")
    val t2 = LocalRelation($"key".int, $"value".string).as("t2")
    val query =
      Project(Seq($"t1.key", $"t2.key"),
        Join(t1, t2, UsingJoin(FullOuter, Seq("key")), None, JoinHint.NONE))
    checkAnalysis(
      query,
      Project(Seq($"t1.key", $"t2.key"),
        Project(Seq(coalesce($"t1.key", $"t2.key").as("key"),
          $"t1.value", $"t2.value", $"t1.key", $"t2.key"),
          Join(t1, t2, FullOuter, Some($"t1.key" === $"t2.key"), JoinHint.NONE)
        )
      ).analyze
    )
  }

  test("SPARK-43030: deduplicate relations in CTE relation definitions") {
    val join = testRelation.as("left").join(testRelation.as("right"))
    val cteDef = CTERelationDef(join)
    val cteRef = CTERelationRef(cteDef.id, false, Nil, false)

    withClue("flat CTE") {
      val plan = WithCTE(cteRef.select($"left.a"), Seq(cteDef)).analyze
      val relations = plan.collect {
        case r: LocalRelation => r
      }
      assert(relations.length == 2)
      assert(relations.map(_.output).distinct.length == 2)
    }

    withClue("nested CTE") {
      val cteDef2 = CTERelationDef(WithCTE(cteRef.join(testRelation), Seq(cteDef)))
      val cteRef2 = CTERelationRef(cteDef2.id, false, Nil, false)
      val plan = WithCTE(cteRef2, Seq(cteDef2)).analyze
      val relations = plan.collect {
        case r: LocalRelation => r
      }
      assert(relations.length == 3)
      assert(relations.map(_.output).distinct.length == 3)
    }
  }

  test("SPARK-43030: deduplicate CTE relation references") {
    val cteDef = CTERelationDef(testRelation.select($"a"))
    val cteRef = CTERelationRef(cteDef.id, false, Nil, false)

    withClue("single reference") {
      val plan = WithCTE(cteRef.where($"a" > 1), Seq(cteDef)).analyze
      val refs = plan.collect {
        case r: CTERelationRef => r
      }
      // Only one CTE ref, no need to deduplicate
      assert(refs.length == 1)
      assert(refs(0).output == testRelation.output.take(1))
    }

    withClue("two references") {
      val plan = WithCTE(cteRef.join(cteRef), Seq(cteDef)).analyze
      val refs = plan.collect {
        case r: CTERelationRef => r
      }
      assert(refs.length == 2)
      assert(refs.map(_.output).distinct.length == 2)
    }

    withClue("CTE relation has duplicated attributes") {
      val cteDef = CTERelationDef(testRelation.select($"a", $"a"))
      val cteRef = CTERelationRef(cteDef.id, false, Nil, false)
      val plan = WithCTE(cteRef.join(cteRef.select($"a")), Seq(cteDef)).analyze
      val refs = plan.collect {
        case r: CTERelationRef => r
      }
      assert(refs.length == 2)
      assert(refs.map(_.output).distinct.length == 2)
    }

    withClue("CTE relation has duplicate aliases") {
      val alias = Alias($"a", "x")()
      val cteDef = CTERelationDef(testRelation.select(alias, alias).where($"x" === 1))
      val cteRef = CTERelationRef(cteDef.id, false, Nil, false)
      // Should not fail with the assertion failure: Found duplicate rewrite attributes.
      WithCTE(cteRef.join(cteRef), Seq(cteDef)).analyze
    }

    withClue("references in both CTE relation definition and main query") {
      val cteDef2 = CTERelationDef(cteRef.where($"a" > 2))
      val cteRef2 = CTERelationRef(cteDef2.id, false, Nil, false)
      val plan = WithCTE(cteRef.union(cteRef2), Seq(cteDef, cteDef2)).analyze
      val refs = plan.collect {
        case r: CTERelationRef => r
      }
      assert(refs.length == 3)
      assert(refs.map(_.cteId).distinct.length == 2)
      assert(refs.map(_.output).distinct.length == 3)
    }
  }

  test("SPARK-43190: ListQuery.childOutput should be consistent with child output") {
    val listQuery1 = ListQuery(testRelation2.select($"a"))
    val listQuery2 = ListQuery(testRelation2.select($"b"))
    val plan = testRelation3.where($"f".in(listQuery1) && $"f".in(listQuery2)).analyze
    val resolvedCondition = plan.expressions.head
    val finalPlan = testRelation2.join(testRelation3).where(resolvedCondition).analyze
    val resolvedListQueries = finalPlan.expressions.flatMap(_.collect {
      case l: ListQuery => l
    })
    assert(resolvedListQueries.length == 2)

    def collectLocalRelations(plan: LogicalPlan): Seq[LocalRelation] = plan.collect {
      case l: LocalRelation => l
    }
    val localRelations = resolvedListQueries.flatMap(l => collectLocalRelations(l.plan))
    assert(localRelations.length == 2)
    // DeduplicateRelations should deduplicate plans in subquery expressions as well.
    assert(localRelations.head.output != localRelations.last.output)

    resolvedListQueries.foreach { l =>
      assert(l.childOutputs == l.plan.output)
    }
  }

  test("SPARK-43293: __qualified_access_only should be ignored in normal columns") {
    val attr = $"a".int.markAsQualifiedAccessOnly()
    val rel = LocalRelation(attr)
    checkAnalysis(rel.select($"a"), rel.select(attr.markAsAllowAnyAccess()))
  }

  test("SPARK-43030: deduplicate relations with duplicate aliases") {
    // Should not fail with the assertion failure: Found duplicate rewrite attributes.
    val alias = Alias($"a", "x")()

    withClue("project") {
      val plan = testRelation.select(alias, alias).where($"x" === 1)
      plan.join(plan).analyze
    }

    withClue("aggregate") {
      val plan = testRelation.groupBy($"a")(alias, alias).where($"x" === 1)
      plan.join(plan).analyze
    }

    withClue("window") {
      val frame = SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)
      val spec = windowSpec(Seq($"c"), Seq(), frame)
      val plan = testRelation2
        .window(
          Seq(alias, alias, windowExpr(min($"b"), spec).as("min_b")),
          Seq($"c"),
          Seq())
        .where($"x" === 1)
      plan.join(plan).analyze
    }
  }

  test("IDENTIFIER with alias and RuntimeReplaceable") {
    val name = Literal("a").as("name")
    val replaceable = new Nvl(Literal("a"), Literal("b"))
    withClue("IDENTIFIER as column") {
      val ident = new ExpressionWithUnresolvedIdentifier(name, UnresolvedAttribute.apply)
      checkAnalysis(testRelation.select(ident), testRelation.select($"a").analyze)
      val ident2 = new ExpressionWithUnresolvedIdentifier(replaceable, UnresolvedAttribute.apply)
      checkAnalysis(testRelation.select(ident2), testRelation.select($"a").analyze)
    }
    withClue("IDENTIFIER as table") {
      val ident = new PlanWithUnresolvedIdentifier(name, _ => testRelation)
      checkAnalysis(ident.select($"a"), testRelation.select($"a").analyze)
      val ident2 = new PlanWithUnresolvedIdentifier(replaceable, _ => testRelation)
      checkAnalysis(ident2.select($"a"), testRelation.select($"a").analyze)
    }
  }

  test("SPARK-46064 Basic functionality of elimination for watermark node in batch query") {
    val dfWithEventTimeWatermark = EventTimeWatermark($"ts",
      IntervalUtils.fromIntervalString("10 seconds"), batchRelationWithTs)

    val analyzed = getAnalyzer.executeAndCheck(dfWithEventTimeWatermark, new QueryPlanningTracker)

    // EventTimeWatermark node is eliminated via EliminateEventTimeWatermark.
    assert(!analyzed.exists(_.isInstanceOf[EventTimeWatermark]))
  }

  test("SPARK-46064 EliminateEventTimeWatermark properly handles the case where the child of " +
    "EventTimeWatermark changes the isStreaming flag during resolution") {
    // UnresolvedRelation which is batch initially and will be resolved as streaming
    val dfWithTempView = UnresolvedRelation(TableIdentifier("streamingTable"))
    val dfWithEventTimeWatermark = EventTimeWatermark($"ts",
      IntervalUtils.fromIntervalString("10 seconds"), dfWithTempView)

    val analyzed = getAnalyzer.executeAndCheck(dfWithEventTimeWatermark, new QueryPlanningTracker)

    // EventTimeWatermark node is NOT eliminated.
    assert(analyzed.exists(_.isInstanceOf[EventTimeWatermark]))
  }

  test("SPARK-46062: isStreaming flag is synced from CTE definition to CTE reference") {
    val cteDef = CTERelationDef(streamingRelation.select($"a", $"ts"))
    // Intentionally marking the flag _resolved to false, so that analyzer has a chance to sync
    // the flag isStreaming on syncing the flag _resolved.
    val cteRef = CTERelationRef(cteDef.id, _resolved = false, Nil, isStreaming = false)
    val plan = WithCTE(cteRef, Seq(cteDef)).analyze

    val refs = plan.collect {
      case r: CTERelationRef => r
    }
    assert(refs.length == 1)
    assert(refs.head.resolved)
    assert(refs.head.isStreaming)
  }

  test("SPARK-47927: ScalaUDF output nullability") {
    val udf = ScalaUDF(
      function = (i: Int) => i + 1,
      dataType = IntegerType,
      children = $"a" :: Nil,
      nullable = false,
      inputEncoders = Seq(Some(ExpressionEncoder[Int]().resolveAndBind())))
    val plan = testRelation.select(udf.as("u")).select($"u").analyze
    assert(plan.output.head.nullable)
  }

  test("test methods of PreemptedError") {
    val preemptedError = new PreemptedError()
    assert(preemptedError.getErrorOpt().isEmpty)

    val internalError = SparkException.internalError("some internal error to be preempted")
    preemptedError.set(internalError)
    assert(preemptedError.getErrorOpt().contains(internalError))

    // set error with higher priority will overwrite
    val regularError = QueryCompilationErrors.unresolvedColumnError("name", Seq("a"))
      .asInstanceOf[AnalysisException]
    preemptedError.set(regularError)
    assert(preemptedError.getErrorOpt().contains(regularError))

    // set error with lower priority is noop
    preemptedError.set(internalError)
    assert(preemptedError.getErrorOpt().contains(regularError))

    preemptedError.clear()
    assert(preemptedError.getErrorOpt().isEmpty)
  }

  test("SPARK-49782: ResolveDataFrameDropColumns rule resolves complex UnresolvedAttribute") {
    val function = UnresolvedFunction("trim", Seq(UnresolvedAttribute("i")), isDistinct = false)
    val addF = Project(Seq(UnresolvedAttribute("i"), Alias(function, "f")()), testRelation5)
    val dropF = DataFrameDropColumns(Seq(UnresolvedAttribute("f")), addF)
    val dropRule = dropF.collectFirst {
      case d: DataFrameDropColumns => d
    }
    assert(dropRule.isDefined)
    val trim = UnresolvedFunction("trim", Seq(UnresolvedAttribute("i")), isDistinct = false)
    val expected = Project(Seq(UnresolvedAttribute("i")),
      Project(Seq(UnresolvedAttribute("i"), Alias(trim, "f")()), testRelation5))
    checkAnalysis(dropF, expected.analyze)
  }
}
