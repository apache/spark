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

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.util.{AccumulatorContext, Utils}

/** Testing only helpers for AQE. */
object AQETestHelper {
  // See [withForcedCancellation].
  @volatile private var metricIdsForForcedCancellation: Set[Long] = Set.empty

  /**
   * Set `triggerMetrics` to induce a forced cancellation into the execution when any of the
   * metrics is non-empty.
   * In this case the results will be discarded and the stage re-run, causing the metrics to be
   * incremented again.
   */
  def withForcedCancellation[T](triggerMetrics: SQLMetric*)(thunk: => T): T = {
    metricIdsForForcedCancellation = triggerMetrics.map(_.id).toSet
    val res = try {
      thunk
    } finally {
      metricIdsForForcedCancellation = Set.empty
      forcedCancellationTriggeredForPlans.clear()
    }
    res
  }

  /*
   * Track for which plans we have already triggered the forced replanning so we only do it once.
   */
  private val forcedCancellationTriggeredForPlans = mutable.HashSet.empty[Int]

  /** Return `true` if forced cancellation mechanism is enabled. */
  def isForcedCancellationEnabled: Boolean =
    Utils.isTesting && metricIdsForForcedCancellation.nonEmpty

  /** Return `true` if forced cancellation has already been triggered for `plan`. */
  private def wasForcedCancellationTriggeredForPlan(plan: SparkPlan): Boolean = synchronized {
    forcedCancellationTriggeredForPlans.contains(plan.id)
  }

  /** Mark that force cancellation was successfully triggered for `plan`. */
  def markForcedCancellationTriggeredForPlan(plan: SparkPlan): Unit = synchronized {
    assert(!forcedCancellationTriggeredForPlans.contains(plan.id),
      "A plan was forced to cancel a second time.")
    forcedCancellationTriggeredForPlans += plan.id
  }

  /** Return `true` if we should try to force cancellation for `plan` at this point. */
  def shouldForceCancellation(plan: SparkPlan): Boolean = {
    // Trigger the forced cancellation only if we are in testing
    Utils.isTesting &&
      // ...and if we haven't triggered it yet
      !wasForcedCancellationTriggeredForPlan(plan) &&
      // ...and if any of the trigger metrics > 0.
      metricIdsForForcedCancellation.exists { id =>
        AccumulatorContext.get(id).map(!_.isZero).getOrElse(false)
      }
  }
}
