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
package org.apache.spark.scheduler.cluster

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.{DYN_ALLOCATION_MAX_EXECUTORS, DYN_ALLOCATION_MIN_EXECUTORS, EXECUTOR_INSTANCES, MAX_EXECUTOR_FAILURES, SCHEDULER_MIN_RESOURCES_TO_SURVIVE_RATIO}
import org.apache.spark.internal.config.Streaming.STREAMING_DYN_ALLOCATION_MAX_EXECUTORS
import org.apache.spark.util.Utils

private[spark] object SchedulerBackendUtils {
  val DEFAULT_NUMBER_EXECUTORS = 2

  /**
   * Getting the initial target number of executors depends on whether dynamic allocation is
   * enabled.
   * If not using dynamic allocation it gets the number of executors requested by the user.
   */
  def getInitialTargetExecutorNumber(
      conf: SparkConf,
      numExecutors: Int = DEFAULT_NUMBER_EXECUTORS): Int = {
    if (Utils.isDynamicAllocationEnabled(conf)) {
      val minNumExecutors = conf.get(DYN_ALLOCATION_MIN_EXECUTORS)
      val initialNumExecutors = Utils.getDynamicAllocationInitialExecutors(conf)
      val maxNumExecutors = conf.get(DYN_ALLOCATION_MAX_EXECUTORS)
      require(initialNumExecutors >= minNumExecutors && initialNumExecutors <= maxNumExecutors,
        s"initial executor number $initialNumExecutors must between min executor number " +
          s"$minNumExecutors and max executor number $maxNumExecutors")

      initialNumExecutors
    } else {
      conf.get(EXECUTOR_INSTANCES).getOrElse(numExecutors)
    }
  }

  def getMaxTargetExecutorNumber(conf: SparkConf): Int = {
    if (Utils.isStreamingDynamicAllocationEnabled(conf)) {
      conf.get(STREAMING_DYN_ALLOCATION_MAX_EXECUTORS)
    } else if (Utils.isDynamicAllocationEnabled(conf)) {
      conf.get(DYN_ALLOCATION_MAX_EXECUTORS)
    } else {
      conf.get(EXECUTOR_INSTANCES).getOrElse(0)
    }
  }

  def formatExecutorFailureError(
      maxNumExecutorFailures: Int,
      numOfExecutorRunning: Int,
      maxExecutors: Int): String = {
    s"Max number of executor failures ($maxNumExecutorFailures) reached and the current running " +
      s"executors ratio $numOfExecutorRunning/$maxExecutors is insufficient. Consider " +
      s"increasing ${MAX_EXECUTOR_FAILURES.key} or " +
      s"${SCHEDULER_MIN_RESOURCES_TO_SURVIVE_RATIO.key} for app being more tolerant to " +
      s"executor failures"
  }
}
