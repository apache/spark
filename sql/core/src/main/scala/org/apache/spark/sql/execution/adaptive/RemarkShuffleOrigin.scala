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

import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, BroadcastDistribution, Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, ShuffleExchangeExec}

/**
 * Remark the shuffle origin to `ENSURE_REQUIREMENTS` as far as possible.
 * For example:
 *   SELECT * FROM t1 JOIN (SELECT /*+ REPARTITION(1024, col) */ * FROM t2)t2 ON t1.col = t2.col
 * The shuffle origin of join right side is `REPARTITION_BY_NUM` but we can use
 * `ENSURE_REQUIREMENTS` instead so that AQE can optimize it using more rules.
 *
 * Note that, this rule must be applied after [[EnsureRequirements]].
 */
object RemarkShuffleOrigin extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformDown {
    case plan =>
      def tryRemarkShuffleOrigin(
          child: SparkPlan, distribution: Distribution): SparkPlan = child match {
        case shuffle: ShuffleExchangeExec if shuffle.shuffleOrigin != ENSURE_REQUIREMENTS &&
          shuffle.outputPartitioning.satisfies(distribution) =>
          shuffle.copy(shuffleOrigin = ENSURE_REQUIREMENTS)
        case shuffle: ShuffleExchangeExec => shuffle
        // For simple, here only track ShuffleExchangeExec through UnaryExecNode and
        // in general it's enough.
        case unary: UnaryExecNode
            if unary.requiredChildDistribution.head == UnspecifiedDistribution =>
          unary.withNewChildren(tryRemarkShuffleOrigin(unary.child, distribution) :: Nil)
        case other => other
      }

      assert(plan.requiredChildDistribution.length == plan.children.length)
      val newChildren = plan.children.zip(plan.requiredChildDistribution).map {
        case (child, UnspecifiedDistribution | AllTuples | _: BroadcastDistribution) => child
        case (child, distribution) => tryRemarkShuffleOrigin(child, distribution)
      }
      plan.withNewChildren(newChildren)
  }
}
