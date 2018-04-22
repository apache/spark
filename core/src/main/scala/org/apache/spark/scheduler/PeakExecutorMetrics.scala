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
  private var _jvmUsedHeapMemory = -1L;
  private var _jvmUsedNonHeapMemory = 0L;
  private var _onHeapExecutionMemory = 0L
  private var _offHeapExecutionMemory = 0L
  private var _onHeapStorageMemory = 0L
  private var _offHeapStorageMemory = 0L
  private var _onHeapUnifiedMemory = 0L
  private var _offHeapUnifiedMemory = 0L
  private var _directMemory = 0L
  private var _mappedMemory = 0L

  def jvmUsedHeapMemory: Long = _jvmUsedHeapMemory

  def jvmUsedNonHeapMemory: Long = _jvmUsedNonHeapMemory

  def onHeapExecutionMemory: Long = _onHeapExecutionMemory

  def offHeapExecutionMemory: Long = _offHeapExecutionMemory

  def onHeapStorageMemory: Long = _onHeapStorageMemory

  def offHeapStorageMemory: Long = _offHeapStorageMemory

  def onHeapUnifiedMemory: Long = _onHeapUnifiedMemory

  def offHeapUnifiedMemory: Long = _offHeapUnifiedMemory

  def directMemory: Long = _directMemory

  def mappedMemory: Long = _mappedMemory

  /**
   * Compare the specified memory values with the saved peak executor memory
   * values, and update if there is a new peak value.
   *
   * @param executorMetrics the executor metrics to compare
   * @return if there is a new peak value for any metric
   */
  def compareAndUpdate(executorMetrics: ExecutorMetrics): Boolean = {
    var updated: Boolean = false

    if (executorMetrics.jvmUsedHeapMemory > _jvmUsedHeapMemory) {
      _jvmUsedHeapMemory = executorMetrics.jvmUsedHeapMemory
      updated = true
    }
    if (executorMetrics.jvmUsedNonHeapMemory > _jvmUsedNonHeapMemory) {
      _jvmUsedNonHeapMemory = executorMetrics.jvmUsedNonHeapMemory
      updated = true
    }
    if (executorMetrics.onHeapExecutionMemory > _onHeapExecutionMemory) {
      _onHeapExecutionMemory = executorMetrics.onHeapExecutionMemory
      updated = true
    }
    if (executorMetrics.offHeapExecutionMemory > _offHeapExecutionMemory) {
      _offHeapExecutionMemory = executorMetrics.offHeapExecutionMemory
      updated = true
    }
    if (executorMetrics.onHeapStorageMemory > _onHeapStorageMemory) {
      _onHeapStorageMemory = executorMetrics.onHeapStorageMemory
      updated = true
    }
    if (executorMetrics.offHeapStorageMemory > _offHeapStorageMemory) {
      _offHeapStorageMemory = executorMetrics.offHeapStorageMemory
      updated = true
    }
    if (executorMetrics.onHeapUnifiedMemory > _onHeapUnifiedMemory) {
      _onHeapUnifiedMemory = executorMetrics.onHeapUnifiedMemory
      updated = true
    }
    if (executorMetrics.offHeapUnifiedMemory > _offHeapUnifiedMemory) {
      _offHeapUnifiedMemory = executorMetrics.offHeapUnifiedMemory
      updated = true
    }
    if (executorMetrics.directMemory > _directMemory) {
      _directMemory = executorMetrics.directMemory
      updated = true
    }
    if (executorMetrics.mappedMemory > _mappedMemory) {
      _mappedMemory = executorMetrics.mappedMemory
      updated = true
    }

    updated
  }

  /**
   * @return None if no peak metrics have been recorded, else PeakMemoryMetrics with the peak
   *         values set.
   */
  def getPeakMemoryMetrics: Option[PeakMemoryMetrics] = {
    if (_jvmUsedHeapMemory < 0) {
      None
    } else {
      Some(new PeakMemoryMetrics(_jvmUsedHeapMemory, _jvmUsedNonHeapMemory,
        _onHeapExecutionMemory, _offHeapExecutionMemory, _onHeapStorageMemory,
        _offHeapStorageMemory, _onHeapUnifiedMemory, _offHeapUnifiedMemory,
        _directMemory, _mappedMemory))
    }
  }
}
