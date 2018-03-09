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
 * Records the peak values for executor level metrics. If jvmUsedMemory is -1, then no values have
 * been recorded yet.
 */
private[spark] class PeakExecutorMetrics {
  private var jvmUsedMemory = -1L;
  private var onHeapExecutionMemory = 0L
  private var offHeapExecutionMemory = 0L
  private var onHeapStorageMemory = 0L
  private var offHeapStorageMemory = 0L
  private var onHeapUnifiedMemory = 0L
  private var offHeapUnifiedMemory = 0L

  /**
   * Compare the specified memory values with the saved peak executor memory
   * values, and update if there is a new peak value.
   *
   * @param executorMetrics the executor metrics to compare
   * @return if there is a new peak value for any metric
   */
  def compareAndUpdate(executorMetrics: ExecutorMetrics): Boolean = {
    var updated: Boolean = false

    if (executorMetrics.jvmUsedMemory > jvmUsedMemory) {
      jvmUsedMemory = executorMetrics.jvmUsedMemory
      updated = true
    }
    if (executorMetrics.onHeapExecutionMemory > onHeapExecutionMemory) {
      onHeapExecutionMemory = executorMetrics.onHeapExecutionMemory
      updated = true
    }
    if (executorMetrics.offHeapExecutionMemory > offHeapExecutionMemory) {
      offHeapExecutionMemory = executorMetrics.offHeapExecutionMemory
      updated = true
    }
    if (executorMetrics.onHeapStorageMemory > onHeapStorageMemory) {
      onHeapStorageMemory = executorMetrics.onHeapStorageMemory
      updated = true
    }
    if (executorMetrics.offHeapStorageMemory > offHeapStorageMemory) {
      offHeapStorageMemory = executorMetrics.offHeapStorageMemory
      updated = true
    }
    val newOnHeapUnifiedMemory = (executorMetrics.onHeapExecutionMemory +
      executorMetrics.onHeapStorageMemory)
    if (newOnHeapUnifiedMemory > onHeapUnifiedMemory) {
      onHeapUnifiedMemory = newOnHeapUnifiedMemory
      updated = true
    }
    val newOffHeapUnifiedMemory = (executorMetrics.offHeapExecutionMemory +
      executorMetrics.offHeapStorageMemory)
    if ( newOffHeapUnifiedMemory > offHeapUnifiedMemory) {
      offHeapUnifiedMemory = newOffHeapUnifiedMemory
      updated = true
    }

    updated
  }

  /**
   * @return None if no peak metrics have been recorded, else PeakMemoryMetrics with the peak
   *         values set.
   */
  def getPeakMemoryMetrics: Option[PeakMemoryMetrics] = {
    if (jvmUsedMemory < 0) {
      None
    } else {
      Some(new PeakMemoryMetrics(jvmUsedMemory, onHeapExecutionMemory,
        offHeapExecutionMemory, onHeapStorageMemory, offHeapStorageMemory,
        onHeapUnifiedMemory, offHeapUnifiedMemory))
    }
  }

  /** Clears/resets the saved peak values. */
  def reset(): Unit = {
    jvmUsedMemory = -1L;
    onHeapExecutionMemory = 0L
    offHeapExecutionMemory = 0L
    onHeapStorageMemory = 0L
    offHeapStorageMemory = 0L
    onHeapUnifiedMemory = 0L
    offHeapUnifiedMemory = 0L
  }
}
