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

package org.apache.spark.sql.catalyst.analysis.resolver

import com.databricks.spark.util.FrameProfiler
import com.databricks.sql.DatabricksSQLConf

import org.apache.spark.sql.catalyst.{MetricKey, MetricKeyUtils, QueryPlanningTracker}
import org.apache.spark.sql.catalyst.rules.QueryExecutionMetering
import org.apache.spark.sql.internal.SQLConf

/**
 * Trait for tracking and logging timing metrics for single-pass resolver.
 */
trait ResolverMetricTracker {
  private val profilerGroup: String = getClass.getSimpleName // EDGE

  /**
   * Log top-level timing metrics for single-pass analyzer. In order to utilize existing logging
   * infrastructure, we are going to log single-pass metrics as if single pass resolver was a
   * standalone Catalyst rule. We log every run of single-pass resolver as effective.
   */
  protected def recordTopLevelMetrics[R](tracker: QueryPlanningTracker)(body: => R): R =
    QueryPlanningTracker.withTracker(tracker) {
      // BEGIN-EDGE
      val ruleResult = QueryPlanningTracker.measureRule(
        SQLConf.get.getConf(DatabricksSQLConf.TRACK_CATALYST_RULES_MEMORY_ALLOCATION),
        profilerGroup
      )(body)
      // END-EDGE

      collectQueryExecutionMetrics(ruleResult.totalTimeNs)
      tracker.recordRuleInvocation(
        rule = ResolverMetricTracker.SINGLE_PASS_RESOLVER_METRIC_LOGGING_ALIAS,
        timeNs = ruleResult.totalTimeNs,
        effective = true,
        memoryAllocatedBytes = ruleResult.memoryAllocatedBytes // Edge
      )

      ruleResult.result
    }
  // BEGIN-EDGE

  /**
   * Records the QPL [[MetricKey]] counter and the frame profile of a specific code block in the
   * resolver. We generally use this mechanism for blocking and potentially slow code paths.
   */
  protected def recordProfileAndLatency[R](methodName: String, metricKey: MetricKey.MetricKey)(
      body: => R): R = {
    val startTime = System.nanoTime()
    try {
      FrameProfiler.record(profilerGroup, methodName)(body)
    } finally {
      val duration = System.nanoTime() - startTime
      QueryPlanningTracker.incrementMetric(metricKey, duration)
    }
  }

  /**
   * Records the frame profile of a specific code block in the resolver. We generally use this
   * mechanism for blocking and potentially slow code paths.
   */
  protected def recordProfile[R](methodName: String)(body: => R) =
    FrameProfiler.record(profilerGroup, methodName) {
      body
    }
  // END-EDGE

  private def collectQueryExecutionMetrics(runTime: Long) = {
    val queryExecutionMetrics = QueryExecutionMetering.INSTANCE

    queryExecutionMetrics.incNumEffectiveExecution(
      ResolverMetricTracker.SINGLE_PASS_RESOLVER_METRIC_LOGGING_ALIAS
    )
    queryExecutionMetrics.incTimeEffectiveExecutionBy(
      ResolverMetricTracker.SINGLE_PASS_RESOLVER_METRIC_LOGGING_ALIAS,
      runTime
    )
    queryExecutionMetrics.incNumExecution(
      ResolverMetricTracker.SINGLE_PASS_RESOLVER_METRIC_LOGGING_ALIAS
    )
    queryExecutionMetrics.incExecutionTimeBy(
      ResolverMetricTracker.SINGLE_PASS_RESOLVER_METRIC_LOGGING_ALIAS,
      runTime
    )
  }
}

object ResolverMetricTracker {

  /**
   * Name under which single-pass resolver metrics will show up.
   */
  val SINGLE_PASS_RESOLVER_METRIC_LOGGING_ALIAS = "SinglePassResolver"
}
