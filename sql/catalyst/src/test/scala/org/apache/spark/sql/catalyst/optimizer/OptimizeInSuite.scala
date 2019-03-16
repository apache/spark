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
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf.OPTIMIZER_INSET_CONVERSION_THRESHOLD
import org.apache.spark.sql.types._

class OptimizeInSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("AnalysisNodes", Once,
        EliminateSubqueryAliases) ::
      Batch("ConstantFolding", FixedPoint(10),
        NullPropagation,
        ConstantFolding,
        BooleanSimplification,
        OptimizeIn) :: Nil
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

  test("OptimizedIn test: NULL IN (subquery) gets transformed to Filter(null)") {
    val subquery = ListQuery(testRelation.select(UnresolvedAttribute("a")))
    val originalQuery =
      testRelation
        .where(InSubquery(Seq(Literal.create(null, NullType)), subquery))
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

    withSQLConf(OPTIMIZER_INSET_CONVERSION_THRESHOLD.key -> "10") {
      val notOptimizedPlan = OptimizeIn(plan)
      comparePlans(notOptimizedPlan, plan)
    }

    // Reduce the threshold to turning into InSet.
    withSQLConf(OPTIMIZER_INSET_CONVERSION_THRESHOLD.key -> "2") {
      val optimizedPlan = OptimizeIn(plan)
      optimizedPlan match {
        case Filter(cond, _)
          if cond.isInstanceOf[InSet] && cond.asInstanceOf[InSet].set.size == 3 =>
        // pass
        case _ => fail("Unexpected result for OptimizedIn")
      }
    }
  }

  test("OptimizedIn test: one element in list gets transformed to EqualTo.") {
    val originalQuery =
      testRelation
        .where(In(UnresolvedAttribute("a"), Seq(Literal(1))))
        .analyze

    val optimized = Optimize.execute(originalQuery)
    val correctAnswer =
      testRelation
        .where(EqualTo(UnresolvedAttribute("a"), Literal(1)))
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("OptimizedIn test: In empty list gets transformed to FalseLiteral " +
    "when value is not nullable") {
    val originalQuery =
      testRelation
        .where(In(Literal("a"), Nil))
        .analyze

    val optimized = Optimize.execute(originalQuery)
    val correctAnswer =
      testRelation
        .where(Literal(false))
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("OptimizedIn test: In empty list gets transformed to `If` expression " +
    "when value is nullable") {
    val originalQuery =
      testRelation
        .where(In(UnresolvedAttribute("a"), Nil))
        .analyze

    val optimized = Optimize.execute(originalQuery)
    val correctAnswer =
      testRelation
        .where(If(IsNotNull(UnresolvedAttribute("a")),
          Literal(false), Literal.create(null, BooleanType)))
        .analyze

    comparePlans(optimized, correctAnswer)
  }
}
