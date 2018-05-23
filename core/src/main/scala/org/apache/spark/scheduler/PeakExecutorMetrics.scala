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

package org.apache.spark.scheduler

import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.status.api.v1.PeakMemoryMetrics

/**
 * Records the peak values for executor level metrics. If jvmUsedHeapMemory is -1, then no
 * values have been recorded yet.
 */
private[spark] class PeakExecutorMetrics {
  val metrics = new Array[Long](MemoryTypes.values().length)
  metrics(0) = -1

  /**
   * Compare the specified memory values with the saved peak executor memory
   * values, and update if there is a new peak value.
   *
   * @param executorMetrics the executor metrics to compare
   * @return if there is a new peak value for any metric
   */
  def compareAndUpdate(executorMetrics: ExecutorMetrics): Boolean = {
    var updated: Boolean = false

    (0 until MemoryTypes.values().length).foreach { metricIdx =>
      val newVal = executorMetrics.metrics(metricIdx)
      if ( newVal > metrics(metricIdx)) {
        updated = true
        metrics(metricIdx) = newVal
      }
    }
    updated
  }

  /**
   * @return None if no peak metrics have been recorded, else PeakMemoryMetrics with the peak
   *         values set.
   */
  def getPeakMemoryMetrics: Option[PeakMemoryMetrics] = {
    if (metrics(0) < 0) {
      None
    } else {
      val copy = new PeakMemoryMetrics
      System.arraycopy(this.metrics, 0, copy.metrics, 0, this.metrics.length)
      Some(copy)
    }
  }

  /** Clears/resets the saved peak values. */
  def reset(): Unit = {
    (0 until metrics.length).foreach { idx => metrics(idx) = 0}
    metrics(0) = -1
  }
}
