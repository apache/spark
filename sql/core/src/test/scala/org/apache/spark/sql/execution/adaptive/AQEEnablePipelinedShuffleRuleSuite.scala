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

package org.apache.spark.sql.execution.adaptive

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution.{SparkPlan, UnionExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Unit coverage for [[AQEEnablePipelinedShuffle]]'s flip step keyed on instance identity
 * (SPARK-57399). It exercises `flipEligibleExchanges` directly on a hand-built plan, bypassing
 * `apply`'s environment guards (opt-in flag / local mode / channel manager), because the hazard
 * is purely about how the collected exchanges are matched during the rewrite, not about the
 * environment.
 */
class AQEEnablePipelinedShuffleRuleSuite extends QueryTest with SharedSparkSession {

  private def exchangesWithPipelined(plan: SparkPlan): Seq[Boolean] =
    plan.collect { case s: ShuffleExchangeExec => s.pipelined }

  test("a structural twin of a flipped exchange on a blocked path is NOT flipped") {
    // The rule collects a FREE exchange to flip but must leave a STRUCTURALLY IDENTICAL twin that
    // sits on a blocked path (a join input) regular. Matching the collected set structurally
    // (TreeNode overrides hashCode but not equals) would flip the twin too; that twin, below the
    // join's regular boundary, would make the scheduler reject the whole job. Keying on
    // SparkPlan.id flips exactly the free exchange the collector chose.
    //
    // Build two structurally-identical exchanges (same partitioning, same child) so they are
    // twins with different instance ids, with exchange reuse OFF so the rule's duplicate guard is
    // empty (the condition under which the structural-key bug bit).
    withSQLConf("spark.sql.exchange.reuse" -> "false") {
      import testImplicits._
      val leaf = spark.range(10).select($"id" as Symbol("k")).queryExecution.executedPlan
      val hp = HashPartitioning(leaf.output, 4)

      // Build a plan with two structurally-identical ShuffleExchangeExec nodes (same hp, same leaf
      // child), where ONE is free (E1) and ONE is blocked (E2):
      //   - E1 = ShuffleExchangeExec(hp, leaf) as a UnionExec child (free, will be collected)
      //   - E2 = ShuffleExchangeExec(hp, leaf) as a left input to a SortMergeJoinExec (blocked,
      //     under a BinaryExecNode which sets blocked=true for its children, so not collected)
      // UnionExec is not stats-sensitive so its children stay free, but when the walk reaches the
      // join (a BinaryExecNode) it becomes blocked for that join's inputs.
      // collectCandidates collects E1 but not E2; the structural-key bug would flip BOTH.

      val otherLeaf = spark.range(10).select($"id" as Symbol("k")).queryExecution.executedPlan

      // E1: free twin as a UnionExec child
      val freeTwin = ShuffleExchangeExec(hp, leaf)

      // E2: blocked twin inside the join's left input (same partitioning, same leaf child)
      val blockedTwin = ShuffleExchangeExec(hp, leaf)
      // Join with E2 on the left, otherLeaf on the right (asymmetric, so no join-paired flip)
      val join = SortMergeJoinExec(
        leaf.output, otherLeaf.output, Inner, None, blockedTwin, otherLeaf)

      // Root: UnionExec with E1 on one side (free) and join subtree with E2 on the other (blocked)
      val root = UnionExec(Seq(freeTwin, join))

      val rule = AQEEnablePipelinedShuffle
      val flipped = rule.flipEligibleExchanges(root)

      val flippedUnion = flipped match {
        case u: UnionExec => u
        case other => fail(s"expected a top UnionExec; got:\n$other")
      }
      // The free exchange (E1) should have flipped...
      val flippedExchanges = flippedUnion.children.flatMap { child =>
        child.collect { case s: ShuffleExchangeExec if s.pipelined => s }
      }
      assert(flippedExchanges.nonEmpty,
        s"the free exchange should be pipelined; plan:\n$flipped")

      // ...and its structural twin (E2) down the join input must NOT (exactly one overall).
      val pipelinedCount = exchangesWithPipelined(flipped).count(identity)
      assert(pipelinedCount == 1,
        s"exactly the free exchange should be pipelined, its blocked structural twin must stay " +
          s"regular; found $pipelinedCount pipelined in:\n$flipped")

      // And the blocked twin (E2) under the join is specifically regular.
      val blockedExchanges = flippedUnion.children(1).collect {
        case s: ShuffleExchangeExec => s.pipelined
      }
      assert(blockedExchanges == Seq(false),
        s"the join-input twin must stay regular; plan:\n$flipped")
    }
  }
}
