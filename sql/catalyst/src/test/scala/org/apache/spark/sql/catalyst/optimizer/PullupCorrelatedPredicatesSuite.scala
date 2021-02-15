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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, DeleteAction, DeleteFromTable, InsertAction, LocalRelation, LogicalPlan, MergeIntoTable, UpdateTable}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class PullupCorrelatedPredicatesSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    override protected val excludedOnceBatches = Set("PullupCorrelatedPredicates")

    val batches =
      Batch("PullupCorrelatedPredicates", Once,
        PullupCorrelatedPredicates) :: Nil
  }

  val testRelation = LocalRelation(Symbol("a").int, Symbol("b").double)
  val testRelation2 = LocalRelation(Symbol("c").int, Symbol("d").double)

  test("PullupCorrelatedPredicates should not produce unresolved plan") {
    val subPlan =
      testRelation2
        .where(Symbol("b") < Symbol("d"))
        .select(Symbol("c"))
    val inSubquery =
      testRelation
        .where(InSubquery(Seq(Symbol("a")), ListQuery(subPlan)))
        .select(Symbol("a")).analyze
    assert(inSubquery.resolved)

    val optimized = Optimize.execute(inSubquery)
    assert(optimized.resolved)
  }

  test("PullupCorrelatedPredicates in correlated subquery idempotency check") {
    val subPlan =
      testRelation2
      .where(Symbol("b") < Symbol("d"))
      .select(Symbol("c"))
    val inSubquery =
      testRelation
      .where(InSubquery(Seq(Symbol("a")), ListQuery(subPlan)))
      .select(Symbol("a")).analyze
    assert(inSubquery.resolved)

    val optimized = Optimize.execute(inSubquery)
    val doubleOptimized = Optimize.execute(optimized)
    comparePlans(optimized, doubleOptimized)
  }

  test("PullupCorrelatedPredicates exists correlated subquery idempotency check") {
    val subPlan =
      testRelation2
        .where(Symbol("b") === Symbol("d") && Symbol("d") === 1)
        .select(Literal(1))
    val existsSubquery =
      testRelation
        .where(Exists(subPlan))
        .select(Symbol("a")).analyze
    assert(existsSubquery.resolved)

    val optimized = Optimize.execute(existsSubquery)
    val doubleOptimized = Optimize.execute(optimized)
    comparePlans(optimized, doubleOptimized)
  }

  test("PullupCorrelatedPredicates scalar correlated subquery idempotency check") {
    val subPlan =
      testRelation2
        .where(Symbol("b") === Symbol("d") && Symbol("d") === 1)
        .select(max(Symbol("d")))
    val scalarSubquery =
      testRelation
        .where(ScalarSubquery(subPlan) === 1)
        .select(Symbol("a")).analyze

    val optimized = Optimize.execute(scalarSubquery)
    val doubleOptimized = Optimize.execute(optimized)
    comparePlans(optimized, doubleOptimized, false)
  }

  test("PullupCorrelatedPredicates should handle deletes") {
    val subPlan = testRelation2.where(Symbol("a") === Symbol("c")).select(Symbol("c"))
    val cond = InSubquery(Seq(Symbol("a")), ListQuery(subPlan))
    val deletePlan = DeleteFromTable(testRelation, Some(cond)).analyze
    assert(deletePlan.resolved)

    val optimized = Optimize.execute(deletePlan)
    assert(optimized.resolved)

    optimized match {
      case DeleteFromTable(_, Some(s: InSubquery)) =>
        val outerRefs = SubExprUtils.getOuterReferences(s.query.plan)
        assert(outerRefs.isEmpty, "should be no outer refs")
      case other =>
        fail(s"unexpected logical plan: $other")
    }
  }

  test("PullupCorrelatedPredicates should handle updates") {
    val subPlan = testRelation2.where(Symbol("a") === Symbol("c")).select(Symbol("c"))
    val cond = InSubquery(Seq(Symbol("a")), ListQuery(subPlan))
    val updatePlan = UpdateTable(testRelation, Seq.empty, Some(cond)).analyze
    assert(updatePlan.resolved)

    val optimized = Optimize.execute(updatePlan)
    assert(optimized.resolved)

    optimized match {
      case UpdateTable(_, _, Some(s: InSubquery)) =>
        val outerRefs = SubExprUtils.getOuterReferences(s.query.plan)
        assert(outerRefs.isEmpty, "should be no outer refs")
      case other =>
        fail(s"unexpected logical plan: $other")
    }
  }

  test("PullupCorrelatedPredicates should handle merge") {
    val testRelation3 = LocalRelation(Symbol("e").int, Symbol("f").double)
    val subPlan = testRelation3.where(Symbol("a") === Symbol("e")).select(Symbol("e"))
    val cond = InSubquery(Seq(Symbol("a")), ListQuery(subPlan))

    val mergePlan = MergeIntoTable(
      testRelation,
      testRelation2,
      cond,
      Seq(DeleteAction(None)),
      Seq(InsertAction(None, Seq(Assignment(Symbol("a"),
        Symbol("c")), Assignment(Symbol("b"), Symbol("d"))))))
    val analyzedMergePlan = mergePlan.analyze
    assert(analyzedMergePlan.resolved)

    val optimized = Optimize.execute(analyzedMergePlan)
    assert(optimized.resolved)

    optimized match {
      case MergeIntoTable(_, _, s: InSubquery, _, _) =>
        val outerRefs = SubExprUtils.getOuterReferences(s.query.plan)
        assert(outerRefs.isEmpty, "should be no outer refs")
      case other =>
        fail(s"unexpected logical plan: $other")
    }
  }
}
