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

import org.apache.spark.sql.catalyst.plans.physical.{Distribution, HashClusteredDistribution, HashPartitioning, OrderedDistribution, RangePartitioning, UnspecifiedDistribution}
import org.apache.spark.sql.execution.{CollectMetricsExec, FilterExec, ProjectExec, SortExec, SparkPlan}
import org.apache.spark.sql.execution.exchange.{REPARTITION_BY_COL, ShuffleExchangeExec}

object AQEUtils {

  // Analyze the given plan and calculate the required distribution of this plan w.r.t. the
  // user-specified repartition.
  def getRequiredDistribution(p: SparkPlan): Option[Distribution] = p match {
    // user-specified repartition is only effective when it's the root node, or under
    // Project/Filter/LocalSort/CollectMetrics.
    case s: ShuffleExchangeExec if s.shuffleOrigin == REPARTITION_BY_COL =>
      s.outputPartitioning match {
        case h: HashPartitioning => Some(HashClusteredDistribution(h.expressions))
        // Note: Ideally we need a special version of `OrderedDistribution` that requires an
        // exact match of the `RangePartitioning`, otherwise `RangePartitioning(a, b)` can
        // satisfy `OrderedDistribution(a, b, c)`. But we are fine in AQE as AQE rules can only
        // change `RangePartitioning` to a totally different partitioning, so
        // `OrderedDistribution` can do the job and avoid changing the final output partitioning.
        case r: RangePartitioning => Some(OrderedDistribution(r.ordering))
        case other => throw new IllegalStateException(
          "Unexpected partitioning for REPARTITION_BY_COL: " + other)
      }
    case f: FilterExec => getRequiredDistribution(f.child)
    case s: SortExec if !s.global => getRequiredDistribution(s.child)
    case c: CollectMetricsExec => getRequiredDistribution(c.child)
    case p: ProjectExec =>
      getRequiredDistribution(p.child).flatMap {
        case h: HashClusteredDistribution =>
          if (h.expressions.forall(e => p.projectList.exists(_.semanticEquals(e)))) {
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
