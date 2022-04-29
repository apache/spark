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
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.planning.ExtractFiltersAndInnerJoins
import org.apache.spark.sql.catalyst.plans.{Cross, Inner, InnerLike, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor

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

  val testRelation = LocalRelation($"a".int, $"b".int, $"c".int)
  val testRelation1 = LocalRelation($"d".int)

  test("extract filters and joins") {
    val x = testRelation.subquery(Symbol("x"))
    val y = testRelation1.subquery(Symbol("y"))
    val z = testRelation.subquery(Symbol("z"))

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

    def testExtractCheckCross(plan: LogicalPlan,
        expected: Option[(Seq[(LogicalPlan, InnerLike)], Seq[Expression])]): Unit = {
      assert(
        ExtractFiltersAndInnerJoins.unapply(plan) === expected.map(e => (e._1, e._2)))
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
    val x = testRelation.subquery(Symbol("x"))
    val y = testRelation1.subquery(Symbol("y"))
    val z = testRelation.subquery(Symbol("z"))

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
}
