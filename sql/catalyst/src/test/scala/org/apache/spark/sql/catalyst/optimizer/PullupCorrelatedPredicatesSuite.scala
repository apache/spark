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

  val testRelation = LocalRelation("a".attr.int, "b".attr.double)
  val testRelation2 = LocalRelation("c".attr.int, "d".attr.double)

  test("PullupCorrelatedPredicates should not produce unresolved plan") {
    val subPlan =
      testRelation2
        .where("b".attr < "d".attr)
        .select("c".attr)
    val inSubquery =
      testRelation
        .where(InSubquery(Seq("a".attr), ListQuery(subPlan)))
        .select("a".attr).analyze
    assert(inSubquery.resolved)

    val optimized = Optimize.execute(inSubquery)
    assert(optimized.resolved)
  }

  test("PullupCorrelatedPredicates in correlated subquery idempotency check") {
    val subPlan =
      testRelation2
      .where("b".attr < "d".attr)
      .select("c".attr)
    val inSubquery =
      testRelation
      .where(InSubquery(Seq("a".attr), ListQuery(subPlan)))
      .select("a".attr).analyze
    assert(inSubquery.resolved)

    val optimized = Optimize.execute(inSubquery)
    val doubleOptimized = Optimize.execute(optimized)
    comparePlans(optimized, doubleOptimized)
  }

  test("PullupCorrelatedPredicates exists correlated subquery idempotency check") {
    val subPlan =
      testRelation2
        .where('b === 'd && 'd === 1)
        .select(Literal(1))
    val existsSubquery =
      testRelation
        .where(Exists(subPlan))
        .select("a".attr).analyze
    assert(existsSubquery.resolved)

    val optimized = Optimize.execute(existsSubquery)
    val doubleOptimized = Optimize.execute(optimized)
    comparePlans(optimized, doubleOptimized)
  }

  test("PullupCorrelatedPredicates scalar correlated subquery idempotency check") {
    val subPlan =
      testRelation2
        .where('b === 'd && 'd === 1)
        .select(max("d".attr))
    val scalarSubquery =
      testRelation
        .where(ScalarSubquery(subPlan) === 1)
        .select("a".attr).analyze

    val optimized = Optimize.execute(scalarSubquery)
    val doubleOptimized = Optimize.execute(optimized)
    comparePlans(optimized, doubleOptimized, false)
  }

  test("PullupCorrelatedPredicates should handle deletes") {
    val subPlan = testRelation2.where('a === "c".attr).select("c".attr)
    val cond = InSubquery(Seq("a".attr), ListQuery(subPlan))
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
    val subPlan = testRelation2.where('a === "c".attr).select("c".attr)
    val cond = InSubquery(Seq("a".attr), ListQuery(subPlan))
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
    val testRelation3 = LocalRelation("e".attr.int, "f".attr.double)
    val subPlan = testRelation3.where('a === "e".attr).select("e".attr)
    val cond = InSubquery(Seq("a".attr), ListQuery(subPlan))

    val mergePlan = MergeIntoTable(
      testRelation,
      testRelation2,
      cond,
      Seq(DeleteAction(None)),
      Seq(InsertAction(None, Seq(Assignment("a".attr, "c".attr), Assignment("b".attr, "d".attr)))))
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
