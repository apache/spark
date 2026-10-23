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

package org.apache.spark.sql.execution.exchange

import org.apache.spark.sql.catalyst.plans.physical.{CoalescedHashPartitioning, HashPartitioning, PartitioningCollection}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.SparkPlan

/** The hash layout of a user repartition elided at this plan node. */
private[execution] case class PartitioningOrigin(
    partitioning: HashPartitioning,
    shuffleOrigin: ShuffleOrigin)

private[execution] object PartitioningOrigin {
  private val ORIGIN = TreeNodeTag[PartitioningOrigin]("outputPartitioningOrigin")

  /**
   * Record the user layout that justified removing an exchange. EnsureRequirements visits
   * bottom-up, so an outer repartition replaces the origin already recorded at this node.
   */
  def record(
      plan: SparkPlan,
      partitioning: HashPartitioning,
      shuffleOrigin: ShuffleOrigin): Unit = {
    plan.setTagValue(ORIGIN, PartitioningOrigin(partitioning, shuffleOrigin))
  }

  def inherit(original: SparkPlan, rebuilt: SparkPlan): SparkPlan = {
    if (original ne rebuilt) {
      original.getTagValue(ORIGIN).foreach(rebuilt.setTagValue(ORIGIN, _))
    }
    rebuilt
  }

  /**
   * Preserve the hash layout that justified elision. Coalesced layouts retain the same keys
   * in order without requiring the original reducer count; explicit output counts still hold.
   */
  def validateNode(plan: SparkPlan): Boolean = {
    plan.getTagValue(ORIGIN).forall { origin =>
      PartitioningCollection.flatten(plan.outputPartitioning).exists {
        case hash: HashPartitioning => hash.semanticEquals(origin.partitioning)
        case hash: CoalescedHashPartitioning =>
          hash.expressions.length == origin.partitioning.expressions.length &&
            hash.expressions.zip(origin.partitioning.expressions).forall {
              case (actual, required) => actual.semanticEquals(required)
            } && (origin.shuffleOrigin != REPARTITION_BY_NUM ||
              hash.numPartitions == origin.partitioning.numPartitions)
        case _ => false
      }
    }
  }
}
