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

import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{AttributeMap, Expression}
import org.apache.spark.sql.catalyst.planning.ExtractFiltersAndInnerJoins
import org.apache.spark.sql.catalyst.plans.{Cross, Inner, InnerLike, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.statsEstimation.StatsTestPlan
import org.apache.spark.sql.internal.SQLConf

class JoinOptimizationSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", Once,
        EliminateSubqueryAliases) ::
      Batch("Filter Pushdown", FixedPoint(100),
        CombineFilters,
        PushPredicateThroughNonJoin,
        BooleanSimplification,
        ReorderJoin,
        PushPredicateThroughJoin,
        ColumnPruning,
        RemoveNoopOperators,
        CollapseProject) :: Nil

  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)
  val testRelation1 = LocalRelation('d.int)

  private def testExtractCheckCross(
      plan: LogicalPlan,
      expected: Option[(Seq[(LogicalPlan, InnerLike)], Seq[Expression])]): Unit = {
    ExtractFiltersAndInnerJoins.unapply(plan) match {
      case Some((input, conditions)) =>
        expected.map { case (expectedPlans, expectedConditions) =>
          assert(expectedPlans === input)
          assert(expectedConditions.toSet === conditions.toSet)
        }
      case None =>
        assert(expected.isEmpty)
    }
  }

  test("extract filters and joins") {
    val x = testRelation.subquery('x)
    val y = testRelation1.subquery('y)
    val z = testRelation.subquery('z)

    def testExtract(plan: LogicalPlan,
        expected: Option[(Seq[LogicalPlan], Seq[Expression])]): Unit = {
      val expectedNoCross = expected map {
        seq_pair => {
          val plans = seq_pair._1
          val noCartesian = plans map { plan => (plan, Inner) }
          (noCartesian, seq_pair._2)
        }
      }
      testExtractCheckCross(plan, expectedNoCross)
    }

    testExtract(x, None)
    testExtract(x.where("x.b".attr === 1), None)
    testExtract(x.join(y), Some((Seq(x, y), Seq())))
    testExtract(x.join(y, condition = Some("x.b".attr === "y.d".attr)),
      Some((Seq(x, y), Seq("x.b".attr === "y.d".attr))))
    testExtract(x.join(y).where("x.b".attr === "y.d".attr),
      Some((Seq(x, y), Seq("x.b".attr === "y.d".attr))))
    testExtract(x.join(y).join(z), Some((Seq(x, y, z), Seq())))
    testExtract(x.join(y).where("x.b".attr === "y.d".attr).join(z),
      Some((Seq(x, y, z), Seq("x.b".attr === "y.d".attr))))
    testExtract(x.join(y).join(x.join(z)), Some((Seq(x, y, x.join(z)), Seq())))
    testExtract(x.join(y).join(x.join(z)).where("x.b".attr === "y.d".attr),
      Some((Seq(x, y, x.join(z)), Seq("x.b".attr === "y.d".attr))))

    testExtractCheckCross(x.join(y, Cross), Some((Seq((x, Cross), (y, Cross)), Seq())))
    testExtractCheckCross(x.join(y, Cross).join(z, Cross),
      Some((Seq((x, Cross), (y, Cross), (z, Cross)), Seq())))
    testExtractCheckCross(x.join(y, Cross, Some("x.b".attr === "y.d".attr)).join(z, Cross),
      Some((Seq((x, Cross), (y, Cross), (z, Cross)), Seq("x.b".attr === "y.d".attr))))
    testExtractCheckCross(x.join(y, Inner, Some("x.b".attr === "y.d".attr)).join(z, Cross),
      Some((Seq((x, Inner), (y, Inner), (z, Cross)), Seq("x.b".attr === "y.d".attr))))
    testExtractCheckCross(x.join(y, Cross, Some("x.b".attr === "y.d".attr)).join(z, Inner),
      Some((Seq((x, Cross), (y, Cross), (z, Inner)), Seq("x.b".attr === "y.d".attr))))
  }

  test("reorder inner joins") {
    val x = testRelation.subquery('x)
    val y = testRelation1.subquery('y)
    val z = testRelation.subquery('z)

    val queryAnswers = Seq(
      (
        x.join(y).join(z).where(("x.b".attr === "z.b".attr) && ("y.d".attr === "z.a".attr)),
        x.join(z, condition = Some("x.b".attr === "z.b".attr))
          .join(y, condition = Some("y.d".attr === "z.a".attr))
          .select(Seq("x.a", "x.b", "x.c", "y.d", "z.a", "z.b", "z.c").map(_.attr): _*)
      ),
      (
        x.join(y, Cross).join(z, Cross)
          .where(("x.b".attr === "z.b".attr) && ("y.d".attr === "z.a".attr)),
        x.join(z, Cross, Some("x.b".attr === "z.b".attr))
          .join(y, Cross, Some("y.d".attr === "z.a".attr))
          .select(Seq("x.a", "x.b", "x.c", "y.d", "z.a", "z.b", "z.c").map(_.attr): _*)
      ),
      (
        x.join(y, Inner).join(z, Cross).where("x.b".attr === "z.a".attr),
        x.join(z, Cross, Some("x.b".attr === "z.a".attr)).join(y, Inner)
          .select(Seq("x.a", "x.b", "x.c", "y.d", "z.a", "z.b", "z.c").map(_.attr): _*)
      )
    )

    queryAnswers foreach { queryAnswerPair =>
      val optimized = Optimize.execute(queryAnswerPair._1.analyze)
      comparePlans(optimized, queryAnswerPair._2.analyze)
    }
  }

  test("SPARK-23172 skip projections when flattening joins") {
    def checkExtractInnerJoins(plan: LogicalPlan): Unit = {
      val expectedTables = plan.collectLeaves().map { case p => (p, Inner) }
      val expectedConditions = plan.collect {
        case Join(_, _, _, Some(cond), _) => cond
        case Filter(cond, _) => cond
      }
      testExtractCheckCross(plan, Some((expectedTables, expectedConditions)))
    }

    val x = testRelation.subquery('x)
    val y = testRelation1.subquery('y)
    val z = testRelation.subquery('z)
    var joined = x.join(z, Inner, Some($"x.b" === $"z.b"))
      .select($"x.a", $"z.a", $"z.c")
      .join(y, Inner, Some($"y.d" === $"z.a")).analyze
    checkExtractInnerJoins(joined)

    // test case for project-over-filter
    joined = x.join(z, Inner, Some($"x.b" === $"z.b"))
      .select($"x.a", $"z.a", $"z.c")
      .where($"y.d" === 3)
      .join(y, Inner, Some($"y.d" === $"z.a")).analyze
    checkExtractInnerJoins(joined)

    // test case for filter-over-project
    joined = x.join(z, Inner, Some($"x.b" === $"z.b"))
      .where($"z.a" === 1)
      .select($"x.a", $"z.a", $"z.c")
      .join(y, Inner, Some($"y.d" === $"z.a")).analyze
    checkExtractInnerJoins(joined)
  }

  test("SPARK-23172 reorder joins with projections") {
    withSQLConf(
        SQLConf.STARSCHEMA_DETECTION.key -> "true",
        SQLConf.CBO_ENABLED.key -> "false") {
      val r0output = Seq('a.int, 'b.int, 'c.int)
      val r0colStat = ColumnStat(distinctCount = Some(100000000), nullCount = Some(0))
      val r0colStats = AttributeMap(r0output.map(_ -> r0colStat))
      val r0 = StatsTestPlan(r0output, 100000000, r0colStats, identifier = Some("r0")).subquery('r0)

      val r1output = Seq('a.int, 'd.int)
      val r1colStat = ColumnStat(distinctCount = Some(10), nullCount = Some(0))
      val r1colStats = AttributeMap(r1output.map(_ -> r1colStat))
      val r1 = StatsTestPlan(r1output, 10, r1colStats, identifier = Some("r1")).subquery('r1)

      val r2output = Seq('b.int, 'e.int)
      val r2colStat = ColumnStat(distinctCount = Some(100), nullCount = Some(0))
      val r2colStats = AttributeMap(r2output.map(_ -> r2colStat))
      val r2 = StatsTestPlan(r2output, 100, r2colStats, identifier = Some("r2")).subquery('r2)

      val r3output = Seq('c.int, 'f.int)
      val r3colStat = ColumnStat(distinctCount = Some(1), nullCount = Some(0))
      val r3colStats = AttributeMap(r3output.map(_ -> r3colStat))
      val r3 = StatsTestPlan(r3output, 1, r3colStats, identifier = Some("r3")).subquery('r3)

      val joined = r0.join(r1, Inner, Some($"r0.a" === $"r1.a"))
        .select($"r0.b", $"r0.c", $"r1.d")
        .where($"r1.d" >= 3)
        .join(r2, Inner, Some($"r0.b" === $"r2.b"))
        .where($"r2.e" >= 5)
        .select($"r0.c", $"r1.d", $"r2.e")
        .join(r3, Inner, Some($"r0.c" === $"r3.c"))
        .select($"r1.d", $"r2.e", $"r3.f")
        .where($"r3.f" <= 100)
        .analyze

      val optimized = Optimize.execute(joined)
      val optJoins = ReorderJoin.extractLeftDeepInnerJoins(optimized)
      val joinOrder = optJoins.flatMap(_.collect{ case p: StatsTestPlan => p }.headOption)
        .flatMap(_.identifier)
      assert(joinOrder === Seq("r2", "r1", "r3", "r0"))
    }
  }
}
