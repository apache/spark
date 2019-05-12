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
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.internal.SQLConf.OPTIMIZER_EXCLUDED_RULES


class OptimizerRuleExclusionSuite extends PlanTest {

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)

  private def verifyExcludedRules(optimizer: Optimizer, rulesToExclude: Seq[String]) {
    val nonExcludableRules = optimizer.nonExcludableRules

    val excludedRuleNames = rulesToExclude.filter(!nonExcludableRules.contains(_))
    // Batches whose rules are all to be excluded should be removed as a whole.
    val excludedBatchNames = optimizer.batches
      .filter(batch => batch.rules.forall(rule => excludedRuleNames.contains(rule.ruleName)))
      .map(_.name)

    withSQLConf(
      OPTIMIZER_EXCLUDED_RULES.key -> excludedRuleNames.foldLeft("")((l, r) => l + "," + r)) {
      val batches = optimizer.batches
      // Verify removed batches.
      assert(batches.forall(batch => !excludedBatchNames.contains(batch.name)))
      // Verify removed rules.
      assert(
        batches
          .forall(batch => batch.rules.forall(rule => !excludedRuleNames.contains(rule.ruleName))))
      // Verify non-excludable rules retained.
      nonExcludableRules.foreach { nonExcludableRule =>
        assert(
          optimizer.batches
            .exists(batch => batch.rules.exists(rule => rule.ruleName == nonExcludableRule)))
      }
    }
  }

  test("Exclude a single rule from multiple batches") {
    verifyExcludedRules(
      new SimpleTestOptimizer(),
      Seq(
        PushPredicateThroughJoin.ruleName))
  }

  test("Exclude multiple rules from single or multiple batches") {
    verifyExcludedRules(
      new SimpleTestOptimizer(),
      Seq(
        CombineUnions.ruleName,
        RemoveLiteralFromGroupExpressions.ruleName,
        RemoveRepetitionFromGroupExpressions.ruleName))
  }

  test("Exclude non-existent rule with other valid rules") {
    verifyExcludedRules(
      new SimpleTestOptimizer(),
      Seq(
        LimitPushDown.ruleName,
        InferFiltersFromConstraints.ruleName,
        "DummyRuleName"))
  }

  test("Try to exclude some non-excludable rules") {
    verifyExcludedRules(
      new SimpleTestOptimizer(),
      Seq(
        ReplaceIntersectWithSemiJoin.ruleName,
        PullupCorrelatedPredicates.ruleName,
        RewriteCorrelatedScalarSubquery.ruleName,
        RewritePredicateSubquery.ruleName,
        RewriteExceptAll.ruleName,
        RewriteIntersectAll.ruleName))
  }

  test("Custom optimizer") {
    val optimizer = new SimpleTestOptimizer() {
      override def defaultBatches: Seq[Batch] =
        Batch("push", Once,
          PushDownPredicate,
          PushPredicateThroughJoin,
          PushProjectionThroughUnion) ::
        Batch("pull", Once,
          PullupCorrelatedPredicates) :: Nil

      override def nonExcludableRules: Seq[String] =
        PushDownPredicate.ruleName ::
          PullupCorrelatedPredicates.ruleName :: Nil
    }

    verifyExcludedRules(
      optimizer,
      Seq(
        PushDownPredicate.ruleName,
        PushProjectionThroughUnion.ruleName,
        PullupCorrelatedPredicates.ruleName))
  }

  test("Verify optimized plan after excluding CombineUnions rule") {
    val excludedRules = Seq(
      ConvertToLocalRelation.ruleName,
      PropagateEmptyRelation.ruleName,
      CombineUnions.ruleName)

    val testRelation1 = LocalRelation('a.int, 'b.int, 'c.int)
    val testRelation2 = LocalRelation('a.int, 'b.int, 'c.int)
    val testRelation3 = LocalRelation('a.int, 'b.int, 'c.int)

    withSQLConf(
      OPTIMIZER_EXCLUDED_RULES.key -> excludedRules.foldLeft("")((l, r) => l + "," + r)) {
      val optimizer = new SimpleTestOptimizer()
      val originalQuery = testRelation1.union(testRelation2.union(testRelation3)).analyze
      val optimized = optimizer.execute(originalQuery)
      comparePlans(originalQuery, optimized)
    }
  }
}
