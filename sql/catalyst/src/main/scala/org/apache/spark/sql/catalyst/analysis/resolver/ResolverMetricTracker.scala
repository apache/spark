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

import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.rules.QueryExecutionMetering

/**
 * Trait for tracking and logging timing metrics for single-pass resolver.
 */
trait ResolverMetricTracker {

  /**
   * Log timing metrics for single-pass analyzer. In order to utilize existing logging
   * infrastructure, we are going to log single-pass metrics as if single pass resolver was a
   * standalone Catalyst rule. We log every run of single-pass resolver as effective.
   */
  protected def recordMetrics[R](tracker: QueryPlanningTracker)(body: => R): R =
    QueryPlanningTracker.withTracker(tracker) {
      val startTime = System.nanoTime()

      val result = body

      val runTime = System.nanoTime() - startTime

      collectQueryExecutionMetrics(runTime)
      tracker.recordRuleInvocation(
        rule = ResolverMetricTracker.SINGLE_PASS_RESOLVER_METRIC_LOGGING_ALIAS,
        timeNs = runTime,
        effective = true
      )

      result
    }

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
