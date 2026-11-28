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
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, SinglePartition}
import org.apache.spark.sql.execution.{SparkPlan, UnionExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/** Plan-level coverage of the conservative AQE channel eligibility policy. */
class AQEEnablePipelinedShuffleRuleSuite extends QueryTest with SharedSparkSession {

  private def exchangesWithPipelined(plan: SparkPlan): Seq[Boolean] =
    plan.collect { case s: ShuffleExchangeExec => s.pipelined }

  test("a regular sibling prevents a mixed pipelined job") {
    // A free candidate and its blocked twin would form a mixed job. The regular sibling must
    // remain recoverable across actions, so the entire plan must retain regular shuffles.
    withSQLConf("spark.sql.exchange.reuse" -> "false") {
      import testImplicits._
      val leaf = spark.range(10).select($"id" as Symbol("k")).queryExecution.executedPlan
      val hp = HashPartitioning(leaf.output, 4)

      val otherLeaf = spark.range(10).select($"id" as Symbol("k")).queryExecution.executedPlan

      // E1: free twin as a UnionExec child
      val freeTwin = ShuffleExchangeExec(hp, leaf)

      // E2: blocked twin inside the join's left input (same partitioning, same leaf child)
      val blockedTwin = ShuffleExchangeExec(hp, leaf)
      // Join with E2 on the left, otherLeaf on the right (asymmetric, so no join-paired flip)
      val join = SortMergeJoinExec(
        leaf.output, otherLeaf.output, Inner, None, blockedTwin, otherLeaf)

      // A candidate and a blocked sibling must not become a mixed result job.
      val root = UnionExec(Seq(freeTwin, join))

      val rule = AQEEnablePipelinedShuffle
      val flipped = rule.flipEligibleExchanges(root)

      val flippedUnion = flipped match {
        case u: UnionExec => u
        case other => fail(s"expected a top UnionExec; got:\n$other")
      }
      assert(exchangesWithPipelined(flippedUnion) == Seq(false, false),
        s"both branches must stay regular so the sibling can recover lost outputs: $flipped")
    }
  }

  test("an eligible exchange beside a CTE reuse stage stays regular") {
    // A replan can leave an unstaged eligible exchange beside a CTE reuse stage. The stage's
    // shuffle is materialized by its inner adaptive plan, which the regular-prefix check cannot
    // see through, so the stage itself must count as a regular prefix. Every branch is
    // single-partition and the union reports a single partition, so the plan fits the shared
    // session's local task slots and only that guard keeps the exchange regular.
    withSQLConf(SQLConf.UNION_OUTPUT_PARTITIONING.key -> "true") {
      import testImplicits._
      def oneRow(): SparkPlan =
        spark.range(0, 1, 1, 1).select($"id" as Symbol("k")).queryExecution.sparkPlan
      def flipUnion(sibling: SparkPlan): SparkPlan = {
        val leaf = oneRow()
        assert(leaf.outputPartitioning == SinglePartition && sibling.outputPartitioning ==
          SinglePartition, s"expected single-partition branches: $leaf\n$sibling")
        AQEEnablePipelinedShuffle.flipEligibleExchanges(
          UnionExec(Seq(ShuffleExchangeExec(SinglePartition, leaf), sibling)))
      }

      // Control: beside a plain one-partition leaf, the exchange is flipped.
      val control = flipUnion(oneRow())
      assert(exchangesWithPipelined(control) == Seq(true),
        s"expected the exchange beside a plain leaf to be pipelined: $control")

      val body = oneRow()
      val inner = AdaptiveSparkPlanExec(
        body, AdaptiveExecutionContext(spark, spark.range(1).queryExecution), Nil,
        isSubquery = true)
      val stage = CTEReuseQueryStageExec(0, inner, body.output, body.canonicalized)
      val flipped = flipUnion(stage)
      assert(exchangesWithPipelined(flipped) == Seq(false),
        s"an exchange beside a CTE reuse stage must stay regular: $flipped")
    }
  }
}
