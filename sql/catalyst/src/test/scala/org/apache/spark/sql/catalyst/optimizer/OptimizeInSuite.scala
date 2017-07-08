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

import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf.OPTIMIZER_INSET_CONVERSION_THRESHOLD
import org.apache.spark.sql.types._

class OptimizeInSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("AnalysisNodes", Once,
        EliminateSubqueryAliases) ::
      Batch("ConstantFolding", FixedPoint(10),
        NullPropagation(conf),
        ConstantFolding,
        BooleanSimplification,
        OptimizeIn(conf)) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)

  test("OptimizedIn test: Remove deterministic repetitions") {
    val originalQuery =
      testRelation
        .where(In(UnresolvedAttribute("a"),
          Seq(Literal(1), Literal(1), Literal(2), Literal(2), Literal(1), Literal(2))))
        .where(In(UnresolvedAttribute("b"),
          Seq(UnresolvedAttribute("a"), UnresolvedAttribute("a"),
            Round(UnresolvedAttribute("a"), 0), Round(UnresolvedAttribute("a"), 0),
            Rand(0), Rand(0))))
        .analyze

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .where(In(UnresolvedAttribute("a"), Seq(Literal(1), Literal(2))))
        .where(In(UnresolvedAttribute("b"),
          Seq(UnresolvedAttribute("a"), UnresolvedAttribute("a"),
            Round(UnresolvedAttribute("a"), 0), Round(UnresolvedAttribute("a"), 0),
            Rand(0), Rand(0))))
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("OptimizedIn test: In clause not optimized to InSet when less than 10 items") {
    val originalQuery =
      testRelation
        .where(In(UnresolvedAttribute("a"), Seq(Literal(1), Literal(2))))
        .analyze

    val optimized = Optimize.execute(originalQuery.analyze)
    comparePlans(optimized, originalQuery)
  }

  test("OptimizedIn test: In clause optimized to InSet when more than 10 items") {
    val originalQuery =
      testRelation
        .where(In(UnresolvedAttribute("a"), (1 to 11).map(Literal(_))))
        .analyze

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .where(InSet(UnresolvedAttribute("a"), (1 to 11).toSet))
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("OptimizedIn test: In clause not optimized in case filter has attributes") {
    val originalQuery =
      testRelation
        .where(In(UnresolvedAttribute("a"), Seq(Literal(1), Literal(2), UnresolvedAttribute("b"))))
        .analyze

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .where(In(UnresolvedAttribute("a"), Seq(Literal(1), Literal(2), UnresolvedAttribute("b"))))
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("OptimizedIn test: NULL IN (expr1, ..., exprN) gets transformed to Filter(null)") {
    val originalQuery =
      testRelation
        .where(In(Literal.create(null, NullType), Seq(Literal(1), Literal(2))))
        .analyze

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .where(Literal.create(null, BooleanType))
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("OptimizedIn test: Inset optimization disabled as " +
    "list expression contains attribute)") {
    val originalQuery =
      testRelation
        .where(In(Literal.create(null, StringType), Seq(Literal(1), UnresolvedAttribute("b"))))
        .analyze

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .where(Literal.create(null, BooleanType))
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("OptimizedIn test: Inset optimization disabled as " +
    "list expression contains attribute - select)") {
    val originalQuery =
      testRelation
        .select(In(Literal.create(null, StringType),
        Seq(Literal(1), UnresolvedAttribute("b"))).as("a")).analyze

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .select(Literal.create(null, BooleanType).as("a"))
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("OptimizedIn test: Setting the threshold for turning Set into InSet.") {
    val plan =
      testRelation
        .where(In(UnresolvedAttribute("a"), Seq(Literal(1), Literal(2), Literal(3))))
        .analyze

    val notOptimizedPlan = OptimizeIn(conf)(plan)
    comparePlans(notOptimizedPlan, plan)

    // Reduce the threshold to turning into InSet.
    val optimizedPlan = OptimizeIn(conf.copy(OPTIMIZER_INSET_CONVERSION_THRESHOLD -> 2))(plan)
    optimizedPlan match {
      case Filter(cond, _)
        if cond.isInstanceOf[InSet] && cond.asInstanceOf[InSet].getSet().size == 3 =>
          // pass
      case _ => fail("Unexpected result for OptimizedIn")
    }
  }
}
