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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution, HashPartitioning, UnspecifiedDistribution}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.{CollectMetricsExec, DeserializeToObjectExec, FilterExec, ProjectExec, SortExec, SparkPlan}
import org.apache.spark.sql.execution.exchange.{REPARTITION_BY_COL, REPARTITION_BY_NUM, ShuffleExchangeExec}

object AQEUtils {

  private[adaptive] val PUSHED_DOWN_SHUFFLE = TreeNodeTag[Boolean]("unionPushedDownShuffle")
  private[adaptive] val PUSHED_THROUGH_UNION = TreeNodeTag[Boolean]("shufflePushedThroughUnion")

  /**
   * Whether this node carries the mark of a shuffle inserted below a union by push-down.
   * Custom cost evaluators can use it to distinguish these exchanges from ordinary shuffles;
   * exchanges only retargeted by the rule are not newly marked.
   */
  def isPushedDownShuffle(plan: SparkPlan): Boolean = {
    plan.getTagValue(PUSHED_DOWN_SHUFFLE).contains(true)
  }

  /**
   * Whether this node carries the mark of a union rebuilt after removing its input exchange.
   * The default cost evaluator counts a marked, non-plain union in place of added branch
   * shuffles. The mark survives plan copies and does not imply its inputs are still unstaged.
   */
  def isPushedThroughUnion(plan: SparkPlan): Boolean = {
    plan.getTagValue(PUSHED_THROUGH_UNION).contains(true)
  }

  /**
   * Canonical shuffle forms occurring more than once in the current plan, its shuffle query
   * stages, and visible subquery plans. Stages contribute their pre-optimization canonical form.
   * Adaptive subquery plans are opaque to this walk, so their internal exchanges are not counted.
   */
  private[adaptive] def duplicatedShuffleForms(plan: SparkPlan): Set[SparkPlan] = {
    val counts = mutable.HashMap.empty[SparkPlan, Int]
    def count(canonical: SparkPlan): Unit = {
      counts.update(canonical, counts.getOrElse(canonical, 0) + 1)
    }
    def visit(p: SparkPlan): Unit = p.foreach {
      case exchange: ShuffleExchangeExec => count(exchange.canonicalized)
      case stage: ShuffleQueryStageExec => count(stage.canonicalized)
      case _ =>
    }
    visit(plan)
    plan.subqueriesAll.foreach(visit)
    counts.collect { case (exchange, copies) if copies > 1 => exchange }.toSet
  }

  // Analyze the given plan and calculate the required distribution of this plan w.r.t. the
  // user-specified repartition.
  def getRequiredDistribution(p: SparkPlan): Option[Distribution] = p match {
    // User-specified repartition is only effective when it's the root node, or under
    // Project/Filter/LocalSort/CollectMetrics.
    // Note: we only care about `HashPartitioning` as `EnsureRequirements` can only optimize out
    // user-specified repartition with `HashPartitioning`.
    case ShuffleExchangeExec(h: HashPartitioning, _, shuffleOrigin, _, _)
        if shuffleOrigin == REPARTITION_BY_COL || shuffleOrigin == REPARTITION_BY_NUM =>
      val numPartitions = if (shuffleOrigin == REPARTITION_BY_NUM) {
        Some(h.numPartitions)
      } else {
        None
      }
      Some(ClusteredDistribution(h.expressions, requiredNumPartitions = numPartitions))
    case f: FilterExec => getRequiredDistribution(f.child)
    case s: SortExec if !s.global => getRequiredDistribution(s.child)
    case c: CollectMetricsExec => getRequiredDistribution(c.child)
    case d: DeserializeToObjectExec => getRequiredDistribution(d.child)
    case p: ProjectExec =>
      getRequiredDistribution(p.child).flatMap {
        case h: ClusteredDistribution =>
          if (h.allClusterKeysAmong(p.projectList)) {
            Some(h)
          } else {
            // It's possible that the user-specified repartition is effective but the output
            // partitioning is not retained, e.g. `df.repartition(a, b).select(c)`. We can't
            // handle this case with required distribution. Here we return None and later on
            // `EnsureRequirements` will skip optimizing out the user-specified repartition.
            None
          }
        case other => Some(other)
      }
    case _ => Some(UnspecifiedDistribution)
  }
}
