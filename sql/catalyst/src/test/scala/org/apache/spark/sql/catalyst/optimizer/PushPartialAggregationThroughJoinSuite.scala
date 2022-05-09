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
import org.apache.spark.sql.catalyst.expressions.{Cast, CheckOverflow, CheckOverflowInSum, Divide, Expression, PromotePrecision}
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

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", Once,
        EliminateSubqueryAliases) ::
      Batch("Push Partial Aggregation", FixedPoint(10),
        PullOutGroupingExpressions,
        CombineFilters,
        PushPredicateThroughNonJoin,
        BooleanSimplification,
        PushPredicateThroughJoin,
        ColumnPruning,
        PushPartialAggregationThroughJoin,
        ResolveTimeZone,
        SimplifyCasts,
        CollapseProject) :: Nil
  }

  val testRelation1 = LocalRelation($"a".int, $"b".int, $"c".int)
  val testRelation2 = LocalRelation($"x".int, $"y".int, $"z".int)

  val testRelation3 = LocalRelation($"a".decimal(17, 2), $"b".decimal(17, 2), $"c".decimal(17, 2))
  val testRelation4 = LocalRelation($"x".decimal(17, 2), $"y".decimal(17, 2), $"z".decimal(17, 2))

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

    val correctLeft = PartialAggregate(Seq('a, 'b), Seq(sum('c).as("_pushed_sum_c"), 'a, 'b),
      testRelation1.select('b, 'c, 'a)).as("l")
    val correctRight = PartialAggregate(Seq('x), Seq(count(1).as("cnt"), 'x),
      testRelation2.select('x)).as("r")

    val correctAnswer =
      correctLeft.join(correctRight,
        joinType = Inner, condition = Some('a === 'x))
      .select('_pushed_sum_c, 'b, 'cnt)
      .groupBy('b)(sumWithDataType('_pushed_sum_c * 'cnt, datatype = Some(LongType)).as("sum_c"))
      .analyze

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer)
  }

  test("Push down count") {
    val originalQuery = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some('a === 'x))
      .groupBy('b)(count(1).as("cnt"))

    val correctLeft = PartialAggregate(Seq('a, 'b), Seq('a, 'b, count(1).as("cnt")),
      testRelation1.select('b, 'a)).as("l")
    val correctRight = PartialAggregate(Seq('x), Seq(count(1).as("cnt"), 'x),
      testRelation2.select('x)).as("r")

    val correctAnswer = correctLeft.join(correctRight, joinType = Inner,
        condition = Some('a === 'x))
      .select('b, $"l.cnt", $"r.cnt")
      .groupBy('b)(sumWithDataType($"l.cnt" * $"r.cnt", datatype = Some(LongType)).as("cnt"))


    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("Push down avg") {
    val originalQuery = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some('a === 'x))
      .groupBy('b)(avg('c).as("avg_c"))

    val correctLeft = PartialAggregate(Seq('a, 'b),
      Seq(sumWithDataType('c, datatype = Some(DoubleType)).as("_pushed_sum_c"), 'a, 'b,
        count(1).as("cnt")),
      testRelation1.select('b, 'c, 'a)).as("l")
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

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("Push down first and last") {
    val originalQuery = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some('a === 'x))
      .groupBy('b)(first('c).as("first_c"), last('c).as("last_c"))

    val correctLeft = PartialAggregate(Seq('a, 'b),
      Seq(first('c).as("_pushed_first_c"), last('c).as("_pushed_last_c"), 'a, 'b),
      testRelation1.select('b, 'c, 'a)).as("l")
    val correctRight = PartialAggregate(Seq('x), Seq('x), testRelation2.select('x)).as("r")

    val correctAnswer = correctLeft.join(correctRight, joinType = Inner,
      condition = Some('a === 'x))
      .select($"l._pushed_first_c", $"l._pushed_last_c", 'b)
      .groupBy('b)(first('_pushed_first_c).as("first_c"), last('_pushed_last_c).as("last_c"))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("Push down max and min") {
    val originalQuery = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some('a === 'x))
      .groupBy('b)(max('c).as("max_c"), min('c).as("min_c"))

    val correctLeft = PartialAggregate(Seq('a, 'b),
      Seq(max('c).as("_pushed_max_c"), min('c).as("_pushed_min_c"), 'a, 'b),
      testRelation1.select('b, 'c, 'a)).as("l")
    val correctRight = PartialAggregate(Seq('x), Seq('x), testRelation2.select('x)).as("r")

    val correctAnswer = correctLeft.join(correctRight, joinType = Inner,
      condition = Some('a === 'x))
      .select($"l._pushed_max_c", $"l._pushed_min_c", 'b)
      .groupBy('b)(max('_pushed_max_c).as("max_c"), min('_pushed_min_c).as("min_c"))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("Push distinct") {
    Seq(-1, 10000).foreach { threshold =>
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> threshold.toString) {
        Seq(Inner, LeftOuter, RightOuter, FullOuter).foreach { joinType =>
          val originalQuery = testRelation1
            .join(testRelation2, joinType = joinType, condition = Some('a === 'x))
            .groupBy('b, 'y)('b, 'y)
            .analyze

          val correctLeft = PartialAggregate(Seq('a, 'b), Seq('a, 'b),
            testRelation1.select('a, 'b)).as("l")
          val correctRight = PartialAggregate(Seq('x, 'y), Seq('x, 'y),
            testRelation2.select('x, 'y)).as("r")

          if (threshold < 0) {
            val correctAnswer = correctLeft.join(correctRight, joinType = joinType,
              condition = Some('a === 'x))
              .select('b, 'y)
              .groupBy('b, 'y)('b, 'y)
              .analyze

            comparePlans(Optimize.execute(originalQuery), correctAnswer)
          } else {
            val correctAnswer = joinType match {
              case RightOuter =>
                testRelation1.select('a, 'b).join(correctRight, joinType = joinType,
                  condition = Some('a === 'x))
                  .select('b, 'y)
                  .groupBy('b, 'y)('b, 'y)
                  .analyze
              case FullOuter =>
                correctLeft.join(correctRight, joinType = joinType,
                  condition = Some('a === 'x))
                  .select('b, 'y)
                  .groupBy('b, 'y)('b, 'y)
                  .analyze
              case _ =>
                correctLeft.join(testRelation2.select('x, 'y), joinType = joinType,
                  condition = Some('a === 'x))
                  .select('b, 'y)
                  .groupBy('b, 'y)('b, 'y)
                  .analyze
            }

            comparePlans(Optimize.execute(originalQuery), correctAnswer)
          }
        }
      }
    }
  }

  test("Push distinct for sum(distinct c) and count(distinct c)") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      Seq(Inner, LeftOuter, RightOuter, FullOuter).foreach { joinType =>
        val originalQuery = testRelation1
          .join(testRelation2, joinType = joinType, condition = Some('a === 'x))
          .groupBy('b)(sumDistinct('c), countDistinct('c))

        val correctLeft = PartialAggregate(Seq('a, 'b, 'c), Seq('a, 'b, 'c), testRelation1).as("l")
        val correctRight = PartialAggregate(Seq('x), Seq('x), testRelation2.select('x)).as("r")

        val correctAnswer = correctLeft.join(correctRight, joinType = joinType,
          condition = Some('a === 'x))
          .select('b, 'c)
          .groupBy('b)(sumDistinct('c), countDistinct('c))

        comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
      }
    }
  }

  test("Push the right side of left semi/anti Join") {
    Seq(-1, 10000).foreach { threshold =>
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> threshold.toString) {
        Seq(LeftSemi, LeftAnti).foreach { joinType =>
          val originalQuery = testRelation1
            .join(testRelation2, joinType = joinType, condition = Some('a === 'x))
            .analyze

          val correctRight = PartialAggregate(Seq('x), Seq('x), testRelation2.select('x)).as("r")
          val correctAnswer = testRelation1.join(correctRight, joinType = joinType,
            condition = Some('a === 'x))
            .analyze

          if (threshold < 0) {
            comparePlans(Optimize.execute(originalQuery), correctAnswer)
          } else {
            comparePlans(Optimize.execute(originalQuery), ColumnPruning(originalQuery))
          }
        }
      }
    }
  }

  test("Complex join condition") {
    val originalQuery = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some('a + 1 === 'x + 2))
      .groupBy('b)(max('c).as("max_c"))

    val correctLeft = PartialAggregate(Seq('_pullout_add_a, 'b),
      Seq('_pullout_add_a, max('c).as("_pushed_max_c"), 'b),
      testRelation1.select('b, 'c, ('a + 1).as("_pullout_add_a"))).as("l")
    val correctRight = PartialAggregate(Seq('_pullout_add_x), Seq('_pullout_add_x),
      testRelation2.select(('x + 2).as("_pullout_add_x"))).as("r")

    val correctAnswer = correctLeft.join(correctRight, joinType = Inner,
      condition = Some('_pullout_add_a === '_pullout_add_x))
      .select($"l._pushed_max_c", 'b)
      .groupBy('b)(max('_pushed_max_c).as("max_c"))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("Complex grouping keys") {
    val originalQuery = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some('a === 'x))
      .groupBy('b + 1)(max('c).as("max_c"))

    val correctLeft = PartialAggregate(Seq('a, '_groupingexpression),
      Seq('_groupingexpression, max('c).as("_pushed_max_c"), 'a),
      testRelation1.select('c, ('b + 1).as("_groupingexpression"), 'a)).as("l")
    val correctRight = PartialAggregate(Seq('x), Seq('x),
      testRelation2.select('x)).as("r")

    val correctAnswer = correctLeft.join(correctRight, joinType = Inner,
      condition = Some('x === 'a))
      .select($"l._groupingexpression", $"l._pushed_max_c")
      .groupBy('_groupingexpression)(max('_pushed_max_c).as("max_c"))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("Complex expressions between Aggregate and Join") {
    val originalQuery = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some('a === 'x))
      .select(('a + 1).as("a1"), 'b)
      .groupBy('a1)(max('b).as("max_b"))

    val correctLeft = PartialAggregate(Seq('a, 'a1),
      Seq(max('b).as("_pushed_max_b"), 'a, 'a1),
      testRelation1.select(('a + 1).as("a1"), 'b, 'a)).as("l")
    val correctRight = PartialAggregate(Seq('x), Seq('x),
      testRelation2.select('x)).as("r")

    val correctAnswer = correctLeft.join(correctRight, joinType = Inner,
      condition = Some('x === 'a))
      .select($"l._pushed_max_b", 'a1)
      .groupBy('a1)(max('_pushed_max_b).as("max_b"))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("Decimal type sum") {
    Seq(true, false).foreach { ansiEnabled =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> s"$ansiEnabled") {
        val originalQuery = testRelation3
          .join(testRelation4, joinType = Inner, condition = Some('a === 'x))
          .groupBy('b)(sum('c).as("sum_c"))
          .analyze

        val correctLeft = PartialAggregate(Seq('a, 'b), Seq(sum('c).as("_pushed_sum_c"), 'a, 'b),
          testRelation3.select('b, 'c, 'a)).as("l")
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
          testRelation3.select('b, 'c, 'a)).as("l")
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
          testRelation3.select('b, 'c, 'a)).as("l")
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

  // The following tests are unsupported cases

  test("Do not push down count if grouping is empty") {
    val originalQuery = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some('a === 'x))
      .groupBy()(count(1).as("cnt"))
      .analyze

    comparePlans(Optimize.execute(originalQuery), ColumnPruning(originalQuery))
  }

  test("Do not push down there are too many aggregate expressions") {
    val originalQuery = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some('a === 'x))
      .groupBy('b)(first('c).as("first_c"), last('c).as("last_c"), max('c).as("max_c"),
        min('c).as("min_c"), sum('c).as("max_c"))
      .analyze

    comparePlans(Optimize.execute(originalQuery), ColumnPruning(originalQuery))
  }
}
