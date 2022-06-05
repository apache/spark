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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Cast, CheckOverflow, CheckOverflowInSum, Divide, Expression, If, Literal, PromotePrecision}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Sum}
import org.apache.spark.sql.catalyst.optimizer.customAnalyze._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, DecimalType, DoubleType, LongType}

// Custom Analyzer to exclude DecimalPrecision rule
object ExcludeDecimalPrecisionAnalyzer extends Analyzer(
  new CatalogManager(
    FakeV2SessionCatalog,
    new SessionCatalog(
      new InMemoryCatalog,
      EmptyFunctionRegistry,
      EmptyTableFunctionRegistry) {
      override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = {}
    })) {
  override def resolver: Resolver = caseSensitiveResolution

  override def batches: Seq[Batch] = {
    super.batches.map { b =>
      val newRules = b.rules.map {
        case t: TypeCoercion.CombinedTypeCoercionRule =>
          // Exclude DecimalPrecision rules.
          val typeCoercionRules = t.rules.filterNot(_.ruleName.equals(DecimalPrecision.ruleName))
          new TypeCoercion.CombinedTypeCoercionRule(typeCoercionRules)
        case t: AnsiTypeCoercion.CombinedTypeCoercionRule =>
          // Exclude DecimalPrecision rules.
          val typeCoercionRules = t.rules.filterNot(_.ruleName.equals(DecimalPrecision.ruleName))
          new AnsiTypeCoercion.CombinedTypeCoercionRule(typeCoercionRules)
        case r => r
      }
      Batch(b.name, b.strategy, newRules: _*)
    }
  }
}

// Custom Analyzer to exclude DecimalPrecision rule
object customAnalyze { // scalastyle:ignore
  implicit class CustomDslLogicalPlan(val logicalPlan: LogicalPlan) {
    def analyzePlan: LogicalPlan = {
      val analyzed = ExcludeDecimalPrecisionAnalyzer.execute(logicalPlan)
      ExcludeDecimalPrecisionAnalyzer.checkAnalysis(analyzed)
      EliminateSubqueryAliases(analyzed)
    }
  }
}

class PushPartialAggregationThroughJoinSuite extends PlanTest {

  override def beforeAll(): Unit = {
    super.beforeAll()
    SQLConf.get.setConf(SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED, true)
    SQLConf.get.setConf(SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO, 1.0)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    SQLConf.get.unsetConf(SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED)
    SQLConf.get.unsetConf(SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO)
  }

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", Once,
        EliminateSubqueryAliases) ::
      Batch("Finish Analysis", Once,
        PullOutGroupingExpressions) ::
      Batch("Filter Pushdown", FixedPoint(10),
        PullOutGroupingExpressions,
        CombineFilters,
        PushPredicateThroughNonJoin,
        BooleanSimplification,
        PushPredicateThroughJoin,
        ColumnPruning,
        SimplifyCasts,
        CollapseProject) ::
      Batch("PushPartialAggregationThroughJoin", Once,
        PushPartialAggregationThroughJoin) :: Nil
  }

  private val testRelation1 = LocalRelation($"a".int, $"b".int, $"c".int)
  private val testRelation2 = LocalRelation($"x".int, $"y".int, $"z".int)

  private val testRelation3 =
    LocalRelation($"a".decimal(17, 2), $"b".decimal(17, 2), $"c".decimal(17, 2))
  private val testRelation4 =
    LocalRelation($"x".decimal(17, 2), $"y".decimal(17, 2), $"z".decimal(17, 2))

  private def sumWithDataType(
      sum: Expression,
      useAnsiAdd: Boolean = conf.ansiEnabled,
      datatype: Option[DataType] = None): AggregateExpression = {
    Sum(sum, useAnsiAdd, resultDataType = datatype).toAggregateExpression()
  }

  test("Push down sum") {
    val originalQuery = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some('a === 'x))
      .groupBy('b)(sum('c).as("sum_c"))
      .analyze

    val correctLeft = PartialAggregate(Seq('a, 'b), Seq(sum('c).as("_pushed_sum_c"), 'a, 'b),
      testRelation1.select('a, 'b, 'c)).as("l")
    val correctRight = PartialAggregate(Seq('x), Seq(count(1).as("cnt"), 'x),
      testRelation2.select('x)).as("r")

    val correctAnswer =
      correctLeft.join(correctRight,
        joinType = Inner, condition = Some('a === 'x))
      .select('_pushed_sum_c, 'b, 'cnt)
      .groupBy('b)(sumWithDataType('_pushed_sum_c * 'cnt, datatype = Some(LongType)).as("sum_c"))
      .analyze

    comparePlans(Optimize.execute(originalQuery), correctAnswer)
  }

  test("Push down count") {
    val originalQuery = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some('a === 'x))
      .groupBy('b)(count(1).as("cnt"))
      .analyze

    val correctLeft = PartialAggregate(Seq('a, 'b), Seq('a, 'b, count(1).as("cnt")),
      testRelation1.select('a, 'b)).as("l")
    val correctRight = PartialAggregate(Seq('x), Seq(count(1).as("cnt"), 'x),
      testRelation2.select('x)).as("r")

    val correctAnswer = correctLeft.join(correctRight, joinType = Inner,
        condition = Some('a === 'x))
      .select('b, $"l.cnt", $"r.cnt")
      .groupBy('b)(sumWithDataType($"l.cnt" * $"r.cnt", datatype = Some(LongType)).as("cnt"))
      .analyze


    comparePlans(Optimize.execute(originalQuery), correctAnswer)
  }

  test("Push down avg") {
    val originalQuery = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some('a === 'x))
      .groupBy('b)(avg('c).as("avg_c"))
      .analyze

    val correctLeft = PartialAggregate(Seq('a, 'b),
      Seq(sumWithDataType('c, datatype = Some(DoubleType)).as("_pushed_sum_c"), 'a, 'b,
        count(1).as("cnt")),
      testRelation1.select('a, 'b, 'c)).as("l")
    val correctRight = PartialAggregate(Seq('x),
      Seq(count(1).as("cnt"), 'x),
      testRelation2.select('x)).as("r")
    val newAvg =
      Divide(Sum($"l._pushed_sum_c" * $"r.cnt".cast(DoubleType), resultDataType = Some(DoubleType))
        .toAggregateExpression(),
        Sum($"l.cnt" * $"r.cnt", resultDataType = Some(LongType))
          .toAggregateExpression().cast(DoubleType),
        failOnError = false)

    val correctAnswer = correctLeft.join(correctRight, joinType = Inner,
      condition = Some('a === 'x))
      .select( $"l._pushed_sum_c", 'b, $"l.cnt", $"r.cnt")
      .groupBy('b)(newAvg.as("avg_c"))
      .analyze

    comparePlans(Optimize.execute(originalQuery), correctAnswer)
  }

  test("Push down first and last") {
    val originalQuery = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some('a === 'x))
      .groupBy('b)(first('c).as("first_c"), last('c).as("last_c"))
      .analyze

    val correctLeft = PartialAggregate(Seq('a, 'b),
      Seq(first('c).as("_pushed_first_c"), last('c).as("_pushed_last_c"), 'a, 'b),
      testRelation1.select('a, 'b, 'c)).as("l")
    val correctRight = PartialAggregate(Seq('x), Seq('x), testRelation2.select('x)).as("r")

    val correctAnswer = correctLeft.join(correctRight, joinType = Inner,
      condition = Some('a === 'x))
      .select($"l._pushed_first_c", $"l._pushed_last_c", 'b)
      .groupBy('b)(first('_pushed_first_c).as("first_c"), last('_pushed_last_c).as("last_c"))
      .analyze

    comparePlans(Optimize.execute(originalQuery), correctAnswer)
  }

  test("Push down max and min") {
    val originalQuery = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some('a === 'x))
      .groupBy('b)(max('c).as("max_c"), min('c).as("min_c"))
      .analyze

    val correctLeft = PartialAggregate(Seq('a, 'b),
      Seq(max('c).as("_pushed_max_c"), min('c).as("_pushed_min_c"), 'a, 'b),
      testRelation1.select('a, 'b, 'c)).as("l")
    val correctRight = PartialAggregate(Seq('x), Seq('x), testRelation2.select('x)).as("r")

    val correctAnswer = correctLeft.join(correctRight, joinType = Inner,
      condition = Some('a === 'x))
      .select($"l._pushed_max_c", $"l._pushed_min_c", 'b)
      .groupBy('b)(max('_pushed_max_c).as("max_c"), min('_pushed_min_c).as("min_c"))
      .analyze

    comparePlans(Optimize.execute(originalQuery), correctAnswer)
  }

  test("Push down sum(2), sum(2.5BD), avg(2), min(2), max(2), first(2) and last(2)") {
    val originalQuery = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some('a === 'x))
      .groupBy('b)(sum(Literal(2)).as("sum_2"), sum(Literal(BigDecimal("2.5"))).as("sum_25"),
        avg(Literal(2)).as("avg_2"),
        min(Literal(2)).as("min_2"), max(Literal(2)).as("max_2"),
        first(Literal(2)).as("first_2"), last(Literal(2)).as("last_2"))
      .analyze

    val correctLeft = PartialAggregate(Seq('a, 'b), Seq('a, 'b, count(1).as("cnt")),
      testRelation1.select('a, 'b)).as("l")
    val correctRight = PartialAggregate(Seq('x), Seq(count(1).as("cnt"), 'x),
      testRelation2.select('x)).as("r")

    val correctAnswer =
      correctLeft.join(correctRight,
        joinType = Inner, condition = Some('a === 'x))
        .select('b, $"l.cnt", $"r.cnt")
        .groupBy('b)(sumWithDataType(Literal(2).cast(LongType) * ($"l.cnt" * $"r.cnt"),
          datatype = Some(LongType)).as("sum_2"),
          sumWithDataType(CheckOverflow(Literal(BigDecimal("2.5")).cast(DecimalType(12, 1)) *
            ($"l.cnt" * $"r.cnt").cast(DecimalType(12, 1)), DecimalType(12, 1), !conf.ansiEnabled),
            conf.ansiEnabled, datatype = Some(DecimalType(12, 1))).as("sum_25"),
          avg(Literal(2)).as("avg_2"),
          min(Literal(2)).as("min_2"), max(Literal(2)).as("max_2"),
          first(Literal(2)).as("first_2"), last(Literal(2)).as("last_2"))
        .analyzePlan

    comparePlans(Optimize.execute(originalQuery), correctAnswer)
  }

  test("Push distinct") {
    Seq(-1, 10000).foreach { threshold =>
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> threshold.toString) {
        Seq(Inner, LeftOuter, RightOuter, FullOuter, Cross).foreach { joinType =>
          val originalQuery = testRelation1
            .join(testRelation2, joinType = joinType, condition = Some('a === 'x))
            .groupBy('b, 'y)('b, 'y)
            .analyze

          val correctLeft = PartialAggregate(Seq('a, 'b), Seq('a, 'b),
            testRelation1.select('a, 'b)).as("l")
          val correctRight = PartialAggregate(Seq('x, 'y), Seq('x, 'y),
            testRelation2.select('x, 'y)).as("r")
          val correctAnswer = correctLeft.join(correctRight, joinType = joinType,
            condition = Some('a === 'x))
            .select('b, 'y)
            .groupBy('b, 'y)('b, 'y)
            .analyze

          comparePlans(Optimize.execute(originalQuery), correctAnswer)
        }
      }
    }
  }

  test("Complex join condition") {
    val originalQuery = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some('a + 1 === 'x + 2))
      .groupBy('b)(max('c).as("max_c"))
      .analyze

    val correctLeft = PartialAggregate(Seq('_pullout_add_a, 'b),
      Seq('_pullout_add_a, max('c).as("_pushed_max_c"), 'b),
      testRelation1.select(('a + 1).as("_pullout_add_a"), 'b, 'c)).as("l")
    val correctRight = PartialAggregate(Seq('_pullout_add_x), Seq('_pullout_add_x),
      testRelation2.select(('x + 2).as("_pullout_add_x"))).as("r")

    val correctAnswer = correctLeft.join(correctRight, joinType = Inner,
      condition = Some('_pullout_add_a === '_pullout_add_x))
      .select($"l._pushed_max_c", 'b)
      .groupBy('b)(max('_pushed_max_c).as("max_c"))
      .analyzePlan

    comparePlans(Optimize.execute(originalQuery), correctAnswer)
  }

  test("Complex grouping keys") {
    val originalQuery = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some('a === 'x))
      .groupBy('b + 1)(max('c).as("max_c"))
      .analyze

    val correctLeft = PartialAggregate(Seq('a, '_groupingexpression),
      Seq('_groupingexpression, max('c).as("_pushed_max_c"), 'a),
      testRelation1.select(('b + 1).as("_groupingexpression"), 'a, 'c)).as("l")
    val correctRight = PartialAggregate(Seq('x), Seq('x),
      testRelation2.select('x)).as("r")

    val correctAnswer = correctLeft.join(correctRight, joinType = Inner,
      condition = Some('x === 'a))
      .select($"l._groupingexpression", $"l._pushed_max_c")
      .groupBy('_groupingexpression)(max('_pushed_max_c).as("max_c"))
      .analyzePlan

    comparePlans(Optimize.execute(originalQuery), correctAnswer)
  }

  test("Complex expressions between Aggregate and Join") {
    val originalQuery = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some('a === 'x))
      .select(('a + 1).as("a1"), 'b)
      .groupBy('a1)(max('b).as("max_b"))
      .analyze

    val correctLeft = PartialAggregate(Seq('a, 'a1),
      Seq(max('b).as("_pushed_max_b"), 'a, 'a1),
      testRelation1.select('a, ('a + 1).as("a1"), 'b)).as("l")
    val correctRight = PartialAggregate(Seq('x), Seq('x),
      testRelation2.select('x)).as("r")

    val correctAnswer = correctLeft.join(correctRight, joinType = Inner,
      condition = Some('x === 'a))
      .select($"l._pushed_max_b", 'a1)
      .groupBy('a1)(max('_pushed_max_b).as("max_b"))
      .analyzePlan

    comparePlans(Optimize.execute(originalQuery), correctAnswer)
  }

  test("Decimal type sum") {
    Seq(true, false).foreach { ansiEnabled =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> s"$ansiEnabled") {
        val originalQuery = testRelation3
          .join(testRelation4, joinType = Inner, condition = Some('a === 'x))
          .groupBy('b)(sum('c).as("sum_c"))
          .analyze

        val correctLeft = PartialAggregate(Seq('a, 'b), Seq(sum('c).as("_pushed_sum_c"), 'a, 'b),
          testRelation3.select('a, 'b, 'c)).as("l")
        val correctRight = PartialAggregate(Seq('x), Seq(count(1).as("cnt"), 'x),
          testRelation4.select('x)).as("r")

        val correctAnswer =
          correctLeft.join(correctRight,
            joinType = Inner, condition = Some('a === 'x))
            .select('_pushed_sum_c, 'b, 'cnt)
            .groupBy('b)(sumWithDataType(
              CheckOverflow('_pushed_sum_c * 'cnt.cast(DecimalType(27, 2)), DecimalType(27, 2),
                !conf.ansiEnabled),
              ansiEnabled,
              Some(DecimalType(27, 2))).as("sum_c"))
            .analyzePlan

        comparePlans(Optimize.execute(originalQuery), correctAnswer)
      }
    }
  }

  test("Decimal type count") {
    Seq(true, false).foreach { ansiEnabled =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> s"$ansiEnabled") {
        val originalQuery = testRelation3
          .join(testRelation4, joinType = Inner, condition = Some('a === 'x))
          .groupBy('b)(count('c).as("count_c"))

        val correctLeft = PartialAggregate(Seq('a, 'b),
          Seq(count('c).as("_pushed_count_c"), 'a, 'b),
          testRelation3.select('a, 'b, 'c)).as("l")
        val correctRight = PartialAggregate(Seq('x), Seq(count(1).as("cnt"), 'x),
          testRelation4.select('x)).as("r")

        val correctAnswer =
          correctLeft.join(correctRight,
            joinType = Inner, condition = Some('a === 'x))
            .select('_pushed_count_c, 'b, 'cnt)
            .groupBy('b)(sumWithDataType('_pushed_count_c * 'cnt, ansiEnabled, Some(LongType))
              .as("count_c"))
            .analyzePlan

        comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer)
      }
    }
  }

  test("Decimal type avg") {
    Seq(true, false).foreach { ansiEnabled =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> s"$ansiEnabled") {
        val originalQuery = testRelation3
          .join(testRelation4, joinType = Inner, condition = Some('a === 'x))
          .groupBy('b)(avg('c).as("avg_c"))
          .analyze

        val correctLeft = PartialAggregate(Seq('a, 'b),
          Seq(sumWithDataType('c, ansiEnabled, Some(DecimalType(27, 2))).as("_pushed_sum_c"),
            'a, 'b, count(1).as("cnt")),
          testRelation3.select('a, 'b, 'c)).as("l")
        val correctRight = PartialAggregate(Seq('x), Seq(count(1).as("cnt"), 'x),
          testRelation4.select('x)).as("r")

        val correctAnswer =
          correctLeft.join(correctRight,
            joinType = Inner, condition = Some('a === 'x))
            .select('_pushed_sum_c, 'b, $"l.cnt", $"r.cnt")
            .groupBy('b)(Cast(CheckOverflow(Divide(PromotePrecision(CheckOverflowInSum(
              sumWithDataType(CheckOverflow($"_pushed_sum_c" * Cast($"r.cnt", DecimalType(27, 2)),
                DecimalType(27, 2), !ansiEnabled), ansiEnabled, Some(DecimalType(27, 2))),
              DecimalType(27, 2), !ansiEnabled)),
              PromotePrecision(Cast(sumWithDataType($"l.cnt" * $"r.cnt", ansiEnabled,
                Some(LongType)), DecimalType(27, 2))), failOnError = false),
              DecimalType(38, 13),
              !ansiEnabled), DecimalType(21, 6)).as("avg_c"))
            .analyzePlan

        comparePlans(Optimize.execute(originalQuery), correctAnswer)
      }
    }
  }

  test("Add join condition references to split list") {
    val originalQuery = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some('a === 'x))
      .groupBy('b)(sum('c).as("sum_c"))
      .analyze

    val correctLeft = PartialAggregate(Seq('a, 'b), Seq(sum('c).as("_pushed_sum_c"), 'a, 'b),
      testRelation1.select('a, 'b, 'c)).as("l")
    val correctRight = PartialAggregate(Seq('x), Seq(count(1).as("cnt"), 'x),
      testRelation2.select('x)).as("r")

    val correctAnswer =
      correctLeft.join(correctRight,
        joinType = Inner, condition = Some('a === 'x))
        .select('_pushed_sum_c, 'b, 'cnt)
        .groupBy('b)(sumWithDataType('_pushed_sum_c * 'cnt, datatype = Some(LongType)).as("sum_c"))
        .analyzePlan

    comparePlans(Optimize.execute(originalQuery), correctAnswer)
  }

  // The following tests are unsupported cases

  test("Do not push down count if grouping is empty") {
    val originalQuery = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some('a === 'x))
      .groupBy()(count(1).as("cnt"))
      .analyze

    comparePlans(Optimize.execute(originalQuery), ColumnPruning(originalQuery))
  }

  test("Do not push down avg if grouping is empty") {
    val originalQuery = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some('a === 'x))
      .groupBy()(avg('y).as("avg_y"))
      .analyze

    comparePlans(Optimize.execute(originalQuery), ColumnPruning(originalQuery))
  }

  test("Do not push down if the aggregate references from both left and right side") {
    val originalQuery1 = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some('a === 'x))
      .groupBy('b)(sum('c + 'y).as("sum_c_y"))
      .analyze

    comparePlans(Optimize.execute(originalQuery1), ColumnPruning(originalQuery1))

    val originalQuery2 = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some('a === 'x))
      .select('b, ('c + 'y).as("cy"))
      .groupBy('b)(sum('cy).as("sum_c_y"))
      .analyze

    comparePlans(Optimize.execute(originalQuery2), ColumnPruning(originalQuery2))
  }

  test("Do not push down if grouping references from left and right side") {
    val originalQuery = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some('a === 'x))
      .groupBy('b + 'y)(sum('z).as("sum_z"))
      .analyze

    comparePlans(Optimize.execute(originalQuery),
      CollapseProject(ColumnPruning(PullOutGroupingExpressions(originalQuery))))
  }

  test("Do not push down if join condition is empty or contains unequal expression") {
    Seq(None, Some('a > 'x)).foreach { condition =>
      val originalQuery = testRelation1
        .join(testRelation2, joinType = Inner, condition = condition)
        .groupBy('b)(sum('y).as("sum_y"))
        .analyze

      comparePlans(Optimize.execute(originalQuery), ColumnPruning(originalQuery))
    }
  }

  test("Do not push down aggregate expressions if it's not Inner Join") {
    Seq(LeftOuter, RightOuter, FullOuter).foreach { joinType =>
      val originalQuery = testRelation1
        .join(testRelation2, joinType = joinType, condition = Some('a === 'x))
        .groupBy('b)(sum('c).as("sum_c"))
        .analyze

      comparePlans(Optimize.execute(originalQuery), ColumnPruning(originalQuery))
    }
  }

  test("Do not push down aggregate expressions if it's not pushable expression") {
    val originalQuery = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some('a === 'x))
      .groupBy('b)(bitAnd('c).as("bitAnd_c"))
      .analyze

    comparePlans(Optimize.execute(originalQuery), ColumnPruning(originalQuery))
  }

  test("Do not push down aggregate expressions if the aggregate leaves size exceeds 2") {
    val originalQuery = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some('a === 'x))
      .groupBy('b)(sum(If('y.likeAny("%a%"), 'z + 1, 'z + 2)).as("sum_z"))
      .analyze

    comparePlans(Optimize.execute(originalQuery), ColumnPruning(originalQuery))
  }

  test("Do not push down aggregate expressions if the aggregate filter is not empty") {
    val originalQuery = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some('a === 'x))
      .groupBy('b)(sum('c, Some('c > 1)).as("sum_c"))
      .analyze

    comparePlans(Optimize.execute(originalQuery), ColumnPruning(originalQuery))
  }
}
