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

package org.apache.spark.sql.execution

import scala.annotation.tailrec

import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf

/**
 * When LIMIT/OFFSET is the root node, Spark plans it as CollectLimitExec which preserves the data
 * ordering. However, when OFFSET/LIMIT is not the root node, Spark uses GlobalLimitExec which
 * shuffles all the data into one partition and then gets a slice of it. Unfortunately, the shuffle
 * reader fetches shuffle blocks in a random order and can not preserve the data ordering, which
 * violates the requirement of LIMIT/OFFSET.
 *
 * This rule inserts an extra local sort before LIMIT/OFFSET to preserve the data ordering.
 * TODO: add a order preserving mode in the shuffle reader.
 */
object InsertSortForLimitAndOffset extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.ORDERING_AWARE_LIMIT_OFFSET)) return plan

    plan transform {
      case l @ GlobalLimitExec(
          _,
          SinglePartitionShuffleWithGlobalOrdering(ordering),
          _) =>
        val newChild = SortExec(ordering, global = false, child = l.child)
        l.withNewChildren(Seq(newChild))
    }
  }

  object SinglePartitionShuffleWithGlobalOrdering {
    @tailrec
    def unapply(plan: SparkPlan): Option[Seq[SortOrder]] = plan match {
      case ShuffleExchangeExec(SinglePartition, SparkPlanWithGlobalOrdering(ordering), _, _) =>
        Some(ordering)
      case p: AQEShuffleReadExec => unapply(p.child)
      case p: ShuffleQueryStageExec => unapply(p.plan)
      case _ => None
    }
  }

  // Note: this is not implementing a generalized notion of "global order preservation", but just
  // tackles the regular ORDER BY semantics with optional LIMIT (top-K).
  object SparkPlanWithGlobalOrdering {
    @tailrec
    def unapply(plan: SparkPlan): Option[Seq[SortOrder]] = plan match {
      case p: SortExec if p.global => Some(p.sortOrder)
      case p: LocalLimitExec => unapply(p.child)
      case p: WholeStageCodegenExec => unapply(p.child)
      case _ => None
    }
  }
}
