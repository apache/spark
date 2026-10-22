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
import org.apache.spark.sql.execution.{CollectMetricsExec, DeserializeToObjectExec, FilterExec, ProjectExec, SortExec, SparkPlan}
import org.apache.spark.sql.execution.exchange.{REPARTITION_BY_COL, REPARTITION_BY_NUM, ReusedExchangeExec, ShuffleExchangeExec}

object AQEUtils {

  /**
   * Canonicalized forms of shuffle exchanges occurring more than once across the plan,
   * materialized stages, and subquery plans -- flipping one of these loses stage reuse.
   *
   * Note: `ShuffleExchangeExec.pipelined` is a plain case-class field with no doCanonicalize
   * override, so it PARTICIPATES in the canonical form. An already-flipped (pipelined = true)
   * exchange and a structurally identical unflipped twin therefore canonicalize DIFFERENTLY and
   * would not be paired here. The counts are taken over the CURRENT plan on each replanning
   * round (and this rule does re-run in later rounds on a plan that may already contain an
   * earlier round's flip -- it is NOT a before-any-flip pass). That is still safe: a flipped and
   * an unflipped structural twin cannot coexist as a real duplicate, because reuse collapses
   * twins (ReuseExchangeAndSubquery / ShuffleQueryStageExec) before this rule runs, and if one
   * somehow slipped through, the DAGScheduler's fan-out check rejects a multi-consumer pipelined
   * producer rather than producing wrong results. So missing such a pair here is fail-safe.
   */
  private[adaptive] def duplicatedShuffleForms(plan: SparkPlan): Set[SparkPlan] = {
    val counts = mutable.HashMap.empty[SparkPlan, Int]
    def count(exchange: ShuffleExchangeExec): Unit = {
      val canonical = exchange.canonicalized
      counts.update(canonical, counts.getOrElse(canonical, 0) + 1)
    }
    def visit(p: SparkPlan): Unit = p.foreach {
      case exchange: ShuffleExchangeExec => count(exchange)
      case stage: ShuffleQueryStageExec =>
        stage.plan match {
          case exchange: ShuffleExchangeExec => count(exchange)
          case reused: ReusedExchangeExec =>
            reused.child match {
              case exchange: ShuffleExchangeExec => count(exchange)
              case _ =>
            }
          case _ =>
        }
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
