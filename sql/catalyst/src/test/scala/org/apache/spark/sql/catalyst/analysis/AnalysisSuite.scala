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

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import org.apache.logging.log4j.Level
import org.scalatest.matchers.must.Matchers

import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{AliasIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.{InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count, Sum}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser.parsePlan
import org.apache.spark.sql.catalyst.plans.{Cross, Inner}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, RangePartitioning, RoundRobinPartitioning}
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.connector.catalog.InMemoryTable
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
      intercept[IllegalStateException] {
        DataSourceV2Relation(
          table, schema.toAttributes, None, None, CaseInsensitiveStringMap.empty()).analyze
      }
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

    assertAnalysisErrorClass(
      Project(Seq(UnresolvedAttribute("tBl.a")),
        SubqueryAlias("TbL", UnresolvedRelation(TableIdentifier("TaBlE")))),
      "MISSING_COLUMN",
      Array("tBl.a", "TbL.a"))

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
    assertAnalysisError(plan, Seq("data type mismatch: Arguments must be same type"))
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
      SubqueryAlias("t", UnresolvedTableValuedFunction("range", args.map(Literal(_)), outputNames))
        .select(star())
    }
    assertAnalysisSuccess(rangeWithAliases(3 :: Nil, "a" :: Nil))
    assertAnalysisSuccess(rangeWithAliases(1 :: 4 :: Nil, "b" :: Nil))
    assertAnalysisSuccess(rangeWithAliases(2 :: 6 :: 2 :: Nil, "c" :: Nil))
    assertAnalysisError(
      rangeWithAliases(3 :: Nil, "a" :: "b" :: Nil),
      Seq("Number of given aliases does not match number of output columns. "
        + "Function name: range; number of aliases: 2; number of output columns: 1."))
  }

  test("SPARK-20841 Support table column aliases in FROM clause") {
    def tableColumnsWithAliases(outputNames: Seq[String]): LogicalPlan = {
      UnresolvedSubqueryColumnAliases(
        outputNames,
        SubqueryAlias("t", UnresolvedRelation(TableIdentifier("TaBlE3")))
      ).select(star())
    }
    assertAnalysisSuccess(tableColumnsWithAliases("col1" :: "col2" :: "col3" :: "col4" :: Nil))
    assertAnalysisError(
      tableColumnsWithAliases("col1" :: Nil),
      Seq("Number of column aliases does not match number of columns. " +
        "Number of column aliases: 1; number of columns: 4."))
    assertAnalysisError(
      tableColumnsWithAliases("col1" :: "col2" :: "col3" :: "col4" :: "col5" :: Nil),
      Seq("Number of column aliases does not match number of columns. " +
        "Number of column aliases: 5; number of columns: 4."))
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
    assertAnalysisError(
      tableColumnsWithAliases("col1" :: Nil),
      Seq("Number of column aliases does not match number of columns. " +
        "Number of column aliases: 1; number of columns: 4."))
    assertAnalysisError(
      tableColumnsWithAliases("col1" :: "col2" :: "col3" :: "col4" :: "col5" :: Nil),
      Seq("Number of column aliases does not match number of columns. " +
        "Number of column aliases: 5; number of columns: 4."))
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
    assertAnalysisError(
      joinRelationWithAliases("col1" :: Nil),
      Seq("Number of column aliases does not match number of columns. " +
        "Number of column aliases: 1; number of columns: 4."))
    assertAnalysisError(
      joinRelationWithAliases("col1" :: "col2" :: "col3" :: "col4" :: "col5" :: Nil),
      Seq("Number of column aliases does not match number of columns. " +
        "Number of column aliases: 5; number of columns: 4."))
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
    val output = pythonUdf.dataType.asInstanceOf[StructType].toAttributes
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
    val output = pythonUdf.dataType.asInstanceOf[StructType].toAttributes
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
    val output = pythonUdf.dataType.asInstanceOf[StructType].toAttributes
    val project = Project(Seq(UnresolvedAttribute("a")), testRelation)
    val mapInPandas = MapInPandas(
      pythonUdf,
      output,
      project)
    val left = SubqueryAlias("temp0", mapInPandas)
    val right = SubqueryAlias("temp1", mapInPandas)
    val join = Join(left, right, Inner, None, JoinHint.NONE)
    assertAnalysisSuccess(
      Project(Seq(UnresolvedAttribute("temp0.a"), UnresolvedAttribute("temp1.a")), join))
  }

  test("SPARK-34741: Avoid ambiguous reference in MergeIntoTable") {
    val cond = $"a" > 1
    assertAnalysisError(
      MergeIntoTable(
        testRelation,
        testRelation,
        cond,
        UpdateAction(Some(cond), Assignment($"a", $"a") :: Nil) :: Nil,
        Nil
      ),
      "Reference 'a' is ambiguous" :: Nil)
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
    assertAnalysisErrorClass(parsePlan("WITH t(x) AS (SELECT 1) SELECT * FROM t WHERE y = 1"),
      "MISSING_COLUMN",
      Array("y", "t.x"))
  }

  test("CTE with non-matching column alias") {
    assertAnalysisError(parsePlan("WITH t(x, y) AS (SELECT 1) SELECT * FROM t WHERE x = 1"),
      Seq("Number of column aliases does not match number of columns. Number of column aliases: " +
        "2; number of columns: 1."))
  }

  test("SPARK-28251: Insert into non-existing table error message is user friendly") {
    assertAnalysisError(parsePlan("INSERT INTO test VALUES (1)"),
      Seq("Table not found: test"))
  }

  test("check CollectMetrics resolved") {
    val a = testRelation.output.head
    val sum = Sum(a).toAggregateExpression().as("sum")
    val random_sum = Sum(Rand(1L)).toAggregateExpression().as("rand_sum")
    val literal = Literal(1).as("lit")

    // Ok
    assert(CollectMetrics("event", literal :: sum :: random_sum :: Nil, testRelation).resolved)

    // Bad name
    assert(!CollectMetrics("", sum :: Nil, testRelation).resolved)
    assertAnalysisError(CollectMetrics("", sum :: Nil, testRelation),
      "observed metrics should be named" :: Nil)

    // No columns
    assert(!CollectMetrics("evt", Nil, testRelation).resolved)

    def checkAnalysisError(exprs: Seq[NamedExpression], errors: String*): Unit = {
      assertAnalysisError(CollectMetrics("event", exprs, testRelation), errors)
    }

    // Unwrapped attribute
    checkAnalysisError(
      a :: Nil,
      "Attribute", "can only be used as an argument to an aggregate function")

    // Unwrapped non-deterministic expression
    checkAnalysisError(
      Rand(10).as("rnd") :: Nil,
      "non-deterministic expression", "can only be used as an argument to an aggregate function")

    // Distinct aggregate
    checkAnalysisError(
      Sum(a).toAggregateExpression(isDistinct = true).as("sum") :: Nil,
    "distinct aggregates are not allowed in observed metrics, but found")

    // Nested aggregate
    checkAnalysisError(
      Sum(Sum(a).toAggregateExpression()).toAggregateExpression().as("sum") :: Nil,
      "nested aggregates are not allowed in observed metrics, but found")

    // Windowed aggregate
    val windowExpr = WindowExpression(
      RowNumber(),
      WindowSpecDefinition(Nil, a.asc :: Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow)))
    checkAnalysisError(
      windowExpr.as("rn") :: Nil,
      "window expressions are not allowed in observed metrics, but found")
  }

  test("check CollectMetrics duplicates") {
    val a = testRelation.output.head
    val sum = Sum(a).toAggregateExpression().as("sum")
    val count = Count(Literal(1)).toAggregateExpression().as("cnt")

    // Same result - duplicate names are allowed
    assertAnalysisSuccess(Union(
      CollectMetrics("evt1", count :: Nil, testRelation) ::
      CollectMetrics("evt1", count :: Nil, testRelation) :: Nil))

    // Same children, structurally different metrics - fail
    assertAnalysisError(Union(
      CollectMetrics("evt1", count :: Nil, testRelation) ::
      CollectMetrics("evt1", sum :: Nil, testRelation) :: Nil),
      "Multiple definitions of observed metrics" :: "evt1" :: Nil)

    // Different children, same metrics - fail
    val b = $"b".string
    val tblB = LocalRelation(b)
    assertAnalysisError(Union(
      CollectMetrics("evt1", count :: Nil, testRelation) ::
      CollectMetrics("evt1", count :: Nil, tblB) :: Nil),
      "Multiple definitions of observed metrics" :: "evt1" :: Nil)

    // Subquery different tree - fail
    val subquery = Aggregate(Nil, sum :: Nil, CollectMetrics("evt1", count :: Nil, testRelation))
    val query = Project(
      b :: ScalarSubquery(subquery, Nil).as("sum") :: Nil,
      CollectMetrics("evt1", count :: Nil, tblB))
    assertAnalysisError(query, "Multiple definitions of observed metrics" :: "evt1" :: Nil)

    // Aggregate with filter predicate - fail
    val sumWithFilter = sum.transform {
      case a: AggregateExpression => a.copy(filter = Some(true))
    }.asInstanceOf[NamedExpression]
    assertAnalysisError(
      CollectMetrics("evt1", sumWithFilter :: Nil, testRelation),
      "aggregates with filter predicate are not allowed" :: Nil)
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

    val r1 = Union(firstTable, secondTable)
    val r2 = Union(firstTable, thirdTable)
    val r3 = Union(firstTable, fourthTable)
    val r4 = Except(firstTable, secondTable, isAll = false)
    val r5 = Intersect(firstTable, secondTable, isAll = false)

    assertAnalysisError(r1,
      Seq("Union can only be performed on tables with the compatible column types. " +
        "The second column of the second table is timestamp type which is not compatible " +
        "with double at same column of first table"))

    assertAnalysisError(r2,
      Seq("Union can only be performed on tables with the compatible column types. " +
        "The third column of the second table is timestamp type which is not compatible " +
        "with int at same column of first table"))

    assertAnalysisError(r3,
      Seq("Union can only be performed on tables with the compatible column types. " +
        "The 4th column of the second table is timestamp type which is not compatible " +
        "with float at same column of first table"))

    assertAnalysisError(r4,
      Seq("Except can only be performed on tables with the compatible column types. " +
        "The second column of the second table is timestamp type which is not compatible " +
        "with double at same column of first table"))

    assertAnalysisError(r5,
      Seq("Intersect can only be performed on tables with the compatible column types. " +
        "The second column of the second table is timestamp type which is not compatible " +
        "with double at same column of first table"))
  }

  test("SPARK-31975: Throw user facing error when use WindowFunction directly") {
    assertAnalysisError(testRelation2.select(RowNumber()),
      Seq("Window function row_number() requires an OVER clause."))

    assertAnalysisError(testRelation2.select(Sum(RowNumber())),
      Seq("Window function row_number() requires an OVER clause."))

    assertAnalysisError(testRelation2.select(RowNumber() + 1),
      Seq("Window function row_number() requires an OVER clause."))
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

    assertAnalysisError(parsePlan(
      """
        |SELECT grouping__id FROM (
        |  SELECT a, b, count(1), grouping__id FROM TaBlE2
        |    GROUP BY a, b
        |)
      """.stripMargin),
      Seq("grouping_id() can only be used with GroupingSets/Cube/Rollup"),
      false)
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

    assertAnalysisErrorClass(parsePlan(
     """
        |SELECT c.x
        |FROM VALUES NAMED_STRUCT('x', 'A', 'y', 1), NAMED_STRUCT('x', 'A', 'y', 2) AS t(c)
        |GROUP BY c.x
        |ORDER BY c.x + c.y
        |""".stripMargin),
      "MISSING_COLUMN",
      Array("c.y", "x"))
  }

  test("SPARK-38118: Func(wrong_type) in the HAVING clause should throw data mismatch error") {
    Seq("mean", "abs").foreach { func =>
      assertAnalysisError(parsePlan(
        s"""
           |WITH t as (SELECT true c)
           |SELECT t.c
           |FROM t
           |GROUP BY t.c
           |HAVING ${func}(t.c) > 0d""".stripMargin),
        Seq(s"cannot resolve '$func(t.c)' due to data type mismatch"),
        false)

      assertAnalysisError(parsePlan(
        s"""
           |WITH t as (SELECT true c, false d)
           |SELECT (t.c AND t.d) c
           |FROM t
           |GROUP BY t.c, t.d
           |HAVING ${func}(c) > 0d""".stripMargin),
        Seq(s"cannot resolve '$func(c)' due to data type mismatch"),
        false)
    }
  }

  test("SPARK-39354: should be `Table or view not found`") {
    assertAnalysisError(parsePlan(
      s"""
         |WITH t1 as (SELECT 1 user_id, CAST("2022-06-02" AS DATE) dt)
         |SELECT *
         |FROM t1
         |JOIN t2 ON t1.user_id = t2.user_id
         |WHERE t1.dt >= DATE_SUB('2020-12-27', 90)""".stripMargin),
      Seq(s"Table or view not found: t2"),
      false)
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
}
