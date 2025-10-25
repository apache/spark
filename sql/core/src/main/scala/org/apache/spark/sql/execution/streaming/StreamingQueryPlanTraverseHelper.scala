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

package org.apache.spark.sql.execution.streaming

import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec

/**
 * This is an utility object placing methods to traverse the query plan for streaming query.
 * This is used for patterns of traversal which are repeated in multiple places.
 */
object StreamingQueryPlanTraverseHelper extends Logging {
  def collectFromUnfoldedPlan[B](
      executedPlan: SparkPlan)(
      pf: PartialFunction[SparkPlan, B]): Seq[B] = {
    executedPlan.flatMap {
      // InMemoryTableScanExec is a node to represent a cached plan. The node has underlying
      // actual executed plan, which we should traverse to collect the required information.
      case s: InMemoryTableScanExec => collectFromUnfoldedPlan(s.relation.cachedPlan)(pf)

      // AQE physical node contains the executed plan, pick the plan.
      // In most cases, AQE physical node is expected to contain the final plan, which is
      // appropriate for the caller.
      // Even it does not contain the final plan (in whatever reason), we just provide the
      // plan as best effort, as there is no better way around.
      case a: AdaptiveSparkPlanExec =>
        if (!a.isFinalPlan) {
          logWarning(log"AQE plan is captured, but the executed plan in AQE plan is not" +
            log"the final one. Providing incomplete executed plan. AQE plan: ${MDC(
              LogKeys.AQE_PLAN, a)}")
        }
        collectFromUnfoldedPlan(a.executedPlan)(pf)

      // There are several AQE-specific leaf nodes which covers shuffle. We should pick the
      // underlying plan of these nodes, since the underlying plan has the actual executed
      // nodes which we want to collect metrics.
      case e: QueryStageExec => collectFromUnfoldedPlan(e.plan)(pf)

      case p if pf.isDefinedAt(p) => Seq(pf(p))
      case _ => Seq.empty[B]
    }
  }
}
