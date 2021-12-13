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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.ReplaceUnboundedFollowingWithUnboundedPreceding
import org.apache.spark.sql.catalyst.plans.{Inner, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf

class WindowFunctionOptimizerTestSuite extends QueryTest with PlanTest{

  object TestOptimizer extends RuleExecutor[LogicalPlan] {
    override val batches =
      Batch("Replace UnboundedFollowing", FixedPoint(100),
        ReplaceUnboundedFollowingWithUnboundedPreceding) :: Nil
  }

  protected var spark: SparkSession = null

  /**
   * Create a new [[SparkSession]] running in local-cluster mode with unsafe and codegen enabled.
   */
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local-cluster[2,1,1024]")
      .appName("WindowFunctionOptimizerTestSuite")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try {
      spark.stop()
      spark = null
    } finally {
      super.afterAll()
    }
  }

  val unboundedFollowing = SpecifiedWindowFrame(RowFrame, CurrentRow, UnboundedFollowing)
  val unboundedPreceding = SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow)

  private def buildQueries(windowExpr: WindowExpression,
                           otherWindowExpr: Seq[WindowExpression] = Seq(),
                           buildExpectQuery: Boolean = false): Seq[LogicalPlan] = {

    val testRelationA = {
      val attributeA = 'a.int
      val attributeB = 'b.int
      val attributeC = 'c.int

      LocalRelation.fromExternalRows(
        Seq(attributeA, attributeB, attributeC),
        Seq(Row(1, 2, 3),
          Row(2, 2, 4),
          Row(3, 2, 5),
          Row(4, 3, 6),
          Row(5, 3, 7),
          Row(6, 4, 8)))
    }

    val testRelationB = {
      val attributeD = 'a.int
      val attributeE = 'b.int
      val attributeF = 'c.int

      LocalRelation.fromExternalRows(
        Seq(attributeD, attributeE, attributeF),
        Seq(Row(0, 16, 23),
          Row(2, 16, 24),
          Row(3, 16, 25),
          Row(4, 16, 26),
          Row(7, 17, 27),
          Row(8, 18, 28)))
    }

    val isSameWinSpec: Boolean = {
      if (otherWindowExpr.nonEmpty) {
        !otherWindowExpr.exists(oExpr => oExpr.windowSpec != windowExpr.windowSpec)
      } else {
        false
      }
    }

    val otherNamedExpr = otherWindowExpr.zipWithIndex.map {
      case (winExpr, index) => winExpr.as(s"otherWinExpr$index")
    }

    val simpleQuery = if (otherWindowExpr.isEmpty || isSameWinSpec) {
      testRelationA
        .where('a > 1)
        .select('a, 'b, 'c).logicalPlan
        .window(Seq(windowExpr.as('window)) ++ otherNamedExpr,
          windowExpr.windowSpec.partitionSpec, windowExpr.windowSpec.orderSpec)
        .select('a, 'b, 'window).logicalPlan
    } else {
      testRelationA
        .where('a > 1)
        .select('a, 'b, 'c).logicalPlan
        .window(Seq(windowExpr.as('window)),
          windowExpr.windowSpec.partitionSpec, windowExpr.windowSpec.orderSpec)
        .window(otherNamedExpr,
          otherWindowExpr.head.windowSpec.partitionSpec,
          otherWindowExpr.head.windowSpec.orderSpec)
        .select('a, 'b, 'window).logicalPlan
    }

    val expectedSimpleQuery = if (otherWindowExpr.isEmpty || isSameWinSpec) {
      simpleQuery
    } else {
      testRelationA
        .where('a > 1)
        .select('a, 'b, 'c).logicalPlan
        .window(Seq(windowExpr.as('window)),
          windowExpr.windowSpec.partitionSpec, windowExpr.windowSpec.orderSpec)
        .window(otherNamedExpr,
          otherWindowExpr.head.windowSpec.partitionSpec,
          otherWindowExpr.head.windowSpec.orderSpec)
        // Added an extra Projection to restore the output order
        .select({Seq(Column("a"), Column("b"), Column("c"), Column("window")).map(_.expr) ++
          otherNamedExpr.map(x => Column(x.name).expr)}: _*).logicalPlan
        .select('a, 'b, 'window).logicalPlan
    }

    val multipleWindowFunctions = if (otherWindowExpr.isEmpty || isSameWinSpec) {
      testRelationA
        .where('a > 1)
        .select('a, 'b, 'c).logicalPlan
        .window(Seq(windowExpr.as('window1)) ++ otherNamedExpr,
          windowExpr.windowSpec.partitionSpec, windowExpr.windowSpec.orderSpec)
        .window(Seq(windowExpr.as('window2)) ++ otherNamedExpr,
          windowExpr.windowSpec.partitionSpec, windowExpr.windowSpec.orderSpec)
        .select('a, 'b, 'window1, 'window2).logicalPlan
    } else {
      testRelationA
        .where('a > 1)
        .select('a, 'b, 'c).logicalPlan
        .window(Seq(windowExpr.as('window1)),
          windowExpr.windowSpec.partitionSpec, windowExpr.windowSpec.orderSpec)
        .window(Seq(windowExpr.as('window2)),
          windowExpr.windowSpec.partitionSpec, windowExpr.windowSpec.orderSpec)
        .window(otherNamedExpr,
          otherWindowExpr.head.windowSpec.partitionSpec,
          otherWindowExpr.head.windowSpec.orderSpec)
        .select('a, 'b, 'window1, 'window2).logicalPlan
    }

    val expectedMultipleWindowFunctions: LogicalPlan =
      if (otherWindowExpr.isEmpty || isSameWinSpec) {
        multipleWindowFunctions
      } else {
        testRelationA
          .where('a > 1)
          .select('a, 'b, 'c).logicalPlan
          .window(Seq(windowExpr.as('window1)),
            windowExpr.windowSpec.partitionSpec, windowExpr.windowSpec.orderSpec)
          .window(otherNamedExpr,
            otherWindowExpr.head.windowSpec.partitionSpec,
            otherWindowExpr.head.windowSpec.orderSpec)
          // Added an extra Projection to restore the output order
          .select({
            Seq(Column("a"), Column("b"), Column("c"), Column("window1")).map(_.expr) ++
              otherNamedExpr.map(x => Column(x.name).expr)
          }: _*).logicalPlan
          .window(Seq(windowExpr.as('window2)),
            windowExpr.windowSpec.partitionSpec, windowExpr.windowSpec.orderSpec)
          .window(otherNamedExpr,
            otherWindowExpr.head.windowSpec.partitionSpec,
            otherWindowExpr.head.windowSpec.orderSpec)
          // Added an extra Projection to restore the output order
          .select(Seq(Column("a"), Column("b"), Column("c"), Column("window1")).map(_.expr) ++
            otherNamedExpr.map(x => Column(x.name).expr) ++ Seq(Column("window2").expr) ++
            otherNamedExpr.map(x => Column(x.name).expr): _*).logicalPlan
          .select('a, 'b, 'window1, 'window2).logicalPlan
      }

    val leftSide = if (otherWindowExpr.isEmpty || isSameWinSpec) {
      testRelationA
        .where('a > 1)
        .select('a, 'b, 'c).logicalPlan
        .window(Seq(windowExpr.as('window)) ++ otherNamedExpr,
          windowExpr.windowSpec.partitionSpec, windowExpr.windowSpec.orderSpec)
        .select('a, 'b, 'window).logicalPlan
    } else {
      testRelationA
        .where('a > 1)
        .select('a, 'b, 'c).logicalPlan
        .window(Seq(windowExpr.as('window)),
          windowExpr.windowSpec.partitionSpec, windowExpr.windowSpec.orderSpec)
        .window(otherNamedExpr,
          otherWindowExpr.head.windowSpec.partitionSpec,
          otherWindowExpr.head.windowSpec.orderSpec)
        .select('a, 'b, 'window).logicalPlan
    }

    val rightSide = if (otherWindowExpr.isEmpty || isSameWinSpec) {
      testRelationB
        .where('a > 2)
        .window(Seq(windowExpr.as('window)) ++ otherNamedExpr,
          windowExpr.windowSpec.partitionSpec, windowExpr.windowSpec.orderSpec)
        .select('a, 'b, 'window).logicalPlan
    } else {
      testRelationB
        .where('a > 2)
        .window(Seq(windowExpr.as('window)),
          windowExpr.windowSpec.partitionSpec, windowExpr.windowSpec.orderSpec)
        .window(otherNamedExpr,
          otherWindowExpr.head.windowSpec.partitionSpec,
          otherWindowExpr.head.windowSpec.orderSpec)
        .select('a, 'b, 'window).logicalPlan
    }

    val complexPlan =
      leftSide.as("tA").join(rightSide.as("tB"), Inner)
      .select(Symbol("tA.a"), Symbol("tA.b"), Symbol("tB.b"), Symbol("tA.window"),
        Symbol("tB.window")).logicalPlan
      .groupBy(Symbol("tA.window"))(sum("tB.window")).logicalPlan

    val expectedComplexPlan = {
      val expectedLeftSide = if (otherWindowExpr.isEmpty || isSameWinSpec) {
        leftSide
      } else {
        testRelationA
          .where('a > 1)
          .select('a, 'b, 'c).logicalPlan
          .window(Seq(windowExpr.as('window)),
            windowExpr.windowSpec.partitionSpec, windowExpr.windowSpec.orderSpec)
          .window(otherNamedExpr,
            otherWindowExpr.head.windowSpec.partitionSpec,
            otherWindowExpr.head.windowSpec.orderSpec)
          // Added an extra Projection to restore the output order
          .select({Seq(Column("a"), Column("b"), Column("c"), Column("window")).map(_.expr) ++
            otherNamedExpr.map(x => Column(x.name).expr)}: _*).logicalPlan
          .select('a, 'b, 'window).logicalPlan
      }

      val expectedRightSide = if (otherWindowExpr.isEmpty || isSameWinSpec) {
        rightSide
      } else {
        testRelationB
          .where('a > 2)
          .window(Seq(windowExpr.as('window)),
            windowExpr.windowSpec.partitionSpec, windowExpr.windowSpec.orderSpec)
          .window(otherNamedExpr,
            otherWindowExpr.head.windowSpec.partitionSpec,
            otherWindowExpr.head.windowSpec.orderSpec)
          // Added an extra Projection to restore the output order
          .select({Seq(Column("a"), Column("b"), Column("c"), Column("window")).map(_.expr) ++
            otherNamedExpr.map(x => Column(x.name).expr)}: _*).logicalPlan
          .select('a, 'b, 'window).logicalPlan
      }

      expectedLeftSide.as("tA").join(expectedRightSide.as("tB"), Inner)
        .select(Column("tA.a").expr, Column("tA.b").expr, Column("tB.b").expr,
          Column("tA.window").expr, Column("tB.window").expr)
        .groupBy(Column("tA.window").expr)(sum("tB.window").expr).logicalPlan
    }

    if (buildExpectQuery) {
      Seq(expectedSimpleQuery, expectedMultipleWindowFunctions, expectedComplexPlan)
    } else {
      Seq(simpleQuery, multipleWindowFunctions, complexPlan)
    }
  }

  private def verifyDataFrame(query: LogicalPlan): Unit = {
    val actualDataFrame = Dataset.ofRows(spark, TestOptimizer.execute(query.analyze))
    val originalDataFrame = {
      var df: DataFrame = null
      withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
        "org.apache.spark.sql.catalyst.optimizer.ReplaceUnboundedFollowingWithUnboundedPreceding") {
        df = Dataset.ofRows(spark, query.analyze)
      }
      df
    }

    checkAnswer(actualDataFrame, originalDataFrame)
  }

  test("Replace UnboundedFollowing with UnboundedPreceding for First") {

    val originalWinExpr = windowExpr(first('c),
      windowSpec('b :: Nil, 'a.desc :: Nil, unboundedFollowing))
    val expectedWinExpr = windowExpr(last('c),
      windowSpec('b :: Nil, 'a.asc :: Nil, unboundedPreceding))

    val originalQueries = buildQueries(originalWinExpr)
    val expectedQueries = buildQueries(expectedWinExpr).map(_.analyze)

    originalQueries.zip(expectedQueries).foreach {
      case (original, expected) =>
        comparePlans(TestOptimizer.execute(original.analyze), expected)
        verifyDataFrame(original)
    }
  }

  test("Replace UnboundedFollowing with UnboundedPreceding for first and other func") {

    val originalWinExpr = windowExpr(first('c),
      windowSpec('b :: Nil, 'a.desc :: Nil, unboundedFollowing))
    val avgExpr = windowExpr(avg('c),
      windowSpec('b :: Nil, 'a.desc :: Nil, unboundedFollowing))
    val maxExpr = windowExpr(max('c),
      windowSpec('b :: Nil, 'a.desc :: Nil, unboundedFollowing))

    val expectedWinExpr = windowExpr(last('c),
      windowSpec('b :: Nil, 'a.asc :: Nil, unboundedPreceding))

    val originalQueries = buildQueries(originalWinExpr, Seq(avgExpr, maxExpr))
    val expectedQueries = buildQueries(expectedWinExpr, Seq(avgExpr, maxExpr),
      buildExpectQuery = true).map(_.analyze)

    originalQueries.zip(expectedQueries).foreach {
      case (original, expected) =>
        comparePlans(TestOptimizer.execute(original.analyze), expected)
        verifyDataFrame(original)
    }
  }

  test("Keep unchanged with UnboundedPreceding for First") {

    val originalWinExpr = windowExpr(first('c),
      windowSpec('b :: Nil, 'a.asc :: Nil, unboundedPreceding))
    val expectedWinExpr = originalWinExpr

    val originalQueries = buildQueries(originalWinExpr)
    val expectedQueries = buildQueries(expectedWinExpr).map(_.analyze)

    originalQueries.zip(expectedQueries).foreach {
      case (original, expected) =>
        comparePlans(TestOptimizer.execute(original.analyze), expected)
        verifyDataFrame(original)
    }
  }

  test("Replace UnboundedFollowing with UnboundedPreceding for Last") {

    val originalWinExpr = windowExpr(last('c),
      windowSpec('b :: Nil, 'a.asc :: Nil, unboundedFollowing))
    val expectedWinExpr = windowExpr(first('c),
      windowSpec('b :: Nil, 'a.desc :: Nil, unboundedPreceding))

    val originalQueries = buildQueries(originalWinExpr)
    val expectedQueries = buildQueries(expectedWinExpr).map(_.analyze)

    originalQueries.zip(expectedQueries).foreach {
      case (original, expected) =>
        comparePlans(TestOptimizer.execute(original.analyze), expected)
        verifyDataFrame(original)
    }
  }

  test("Keep unchanged with UnboundedPreceding for Last") {

    val originalWinExpr = windowExpr(last('b),
      windowSpec('b :: Nil, 'a.asc :: Nil, unboundedPreceding))
    val expectedWinExpr = originalWinExpr

    val originalQueries = buildQueries(originalWinExpr)
    val expectedQueries = buildQueries(expectedWinExpr).map(_.analyze)

    originalQueries.zip(expectedQueries).foreach {
      case (original, expected) =>
        comparePlans(TestOptimizer.execute(original.analyze), expected)
        verifyDataFrame(original)
    }
  }
}
