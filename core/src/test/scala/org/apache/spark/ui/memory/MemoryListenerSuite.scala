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

package org.apache.spark.ui.memory

import org.apache.spark._
import org.apache.spark.executor._
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster._

class MemoryListenerSuite extends SparkFunSuite with LocalSparkContext {
  test("test HashMap size for MemoryListener") {
    val listener = new MemoryListener
    val execId1 = "exec-1"
    val execId2 = "exec-2"

    (1 to 2).foreach { i =>
      listener.onStageSubmitted(MemoryListenerSuite.createStageStartEvent(i))
      listener.onStageCompleted(MemoryListenerSuite.createStageEndEvent(i))
    }
    // stages are all completed, no activeStages now
    assert(listener.activeStagesToMem.isEmpty)

    listener.onExecutorMetricsUpdate(MemoryListenerSuite.createExecutorMetricsUpdateEvent(
      execId1, new ExecutorMetrics))
    // ExecutorMetrics is not related with Stages directly
    assert(listener.activeStagesToMem.isEmpty)

    listener.onStageSubmitted(MemoryListenerSuite.createStageStartEvent(3))
    listener.onExecutorMetricsUpdate(MemoryListenerSuite.createExecutorMetricsUpdateEvent(
      execId2, new ExecutorMetrics))
    // totally 2 executors updated their metrics
    assert(listener.activeExecutorIdToMem.size === 2)
    assert(listener.activeStagesToMem.size === 1)
    listener.onStageCompleted(MemoryListenerSuite.createStageEndEvent(3))

    assert(listener.activeStagesToMem.isEmpty)
    assert(listener.completedStagesToMem.size === 3)
    assert(listener.activeExecutorIdToMem.size === listener.latestExecIdToExecMetrics.size)
    assert(listener.removedExecutorIdToMem.isEmpty)
  }

  test("test first stage with no executor metrics update") {
    val listener = new MemoryListener
    val execId1 = "exec-1"

    listener.onExecutorAdded(
      SparkListenerExecutorAdded(0L, execId1, new ExecutorInfo("host1", 1, Map.empty)))

    // stage 1, no metrics update
    listener.onStageSubmitted(MemoryListenerSuite.createStageStartEvent(1))
    listener.onStageCompleted(MemoryListenerSuite.createStageEndEvent(1))

    // stage 2, with one metrics update
    listener.onStageSubmitted(MemoryListenerSuite.createStageStartEvent(2))
    val execMetrics = MemoryListenerSuite.createExecutorMetrics("host-1", 0L, 20, 10)
    listener.onExecutorMetricsUpdate(MemoryListenerSuite.createExecutorMetricsUpdateEvent(
      execId1, execMetrics))
    listener.onStageCompleted(MemoryListenerSuite.createStageEndEvent(2))

    val mapForStage1 = listener.completedStagesToMem.get((1, 0)).get
    // no metrics for stage 1 since no metrics update for stage 1
    assert(mapForStage1.get(execId1).get.transportInfo === None)
    val mapForStage2 = listener.completedStagesToMem.get((2, 0)).get
    assert(mapForStage2.size === 1)
    val memInfo = mapForStage2.get(execId1).get
    assert(memInfo.transportInfo.isDefined)
    val transMetrics = memInfo.transportInfo.get
    assert((20, 10, MemTime(20, 0), MemTime(10, 0)) === (transMetrics.onHeapSize,
      transMetrics.offHeapSize, transMetrics.peakOnHeapSizeTime, transMetrics.peakOffHeapSizeTime))

    listener.onExecutorRemoved(SparkListenerExecutorRemoved(0L, execId1, ""))
  }

  test("test multiple metrics updated in one stage") {
    val listener = new MemoryListener
    val execId1 = "exec-1"

    listener.onExecutorAdded(
      SparkListenerExecutorAdded(0L, execId1, new ExecutorInfo("host1", 1, Map.empty)))

    // multiple metrics updated in one stage
    listener.onStageSubmitted(MemoryListenerSuite.createStageStartEvent(1))
    val execMetrics1 = MemoryListenerSuite.createExecutorMetrics("host-1", 0L, 20, 10)
    listener.onExecutorMetricsUpdate(MemoryListenerSuite.createExecutorMetricsUpdateEvent(
      execId1, execMetrics1))
    val execMetrics2 = MemoryListenerSuite.createExecutorMetrics("host-1", 0L, 30, 5)
    listener.onExecutorMetricsUpdate(MemoryListenerSuite.createExecutorMetricsUpdateEvent(
      execId1, execMetrics2))
    val execMetrics3 = MemoryListenerSuite.createExecutorMetrics("host-1", 0L, 15, 15)
    listener.onExecutorMetricsUpdate(MemoryListenerSuite.createExecutorMetricsUpdateEvent(
      execId1, execMetrics3))
    listener.onStageCompleted(MemoryListenerSuite.createStageEndEvent(1))

    val mapForStage1 = listener.completedStagesToMem.get((1, 0)).get
    val memInfo = mapForStage1.get(execId1).get
    assert(memInfo.transportInfo.isDefined)
    val transMetrics = memInfo.transportInfo.get
    assert((15, 15, MemTime(30, 0), MemTime(15, 0)) === (transMetrics.onHeapSize,
      transMetrics.offHeapSize, transMetrics.peakOnHeapSizeTime, transMetrics.peakOffHeapSizeTime))

    listener.onExecutorRemoved(SparkListenerExecutorRemoved(0L, execId1, ""))
  }

  test("test stages use executor metrics updated in previous stages") {
    val listener = new MemoryListener
    val execId1 = "exec-1"

    listener.onExecutorAdded(
      SparkListenerExecutorAdded(0L, execId1, new ExecutorInfo("host1", 1, Map.empty)))

    // multiple metrics updated in one stage
    listener.onStageSubmitted(MemoryListenerSuite.createStageStartEvent(1))
    val execMetrics1 = MemoryListenerSuite.createExecutorMetrics("host-1", 0L, 20, 10)
    listener.onExecutorMetricsUpdate(MemoryListenerSuite.createExecutorMetricsUpdateEvent(
      execId1, execMetrics1))
    val execMetrics2 = MemoryListenerSuite.createExecutorMetrics("host-1", 0L, 30, 5)
    listener.onExecutorMetricsUpdate(MemoryListenerSuite.createExecutorMetricsUpdateEvent(
      execId1, execMetrics2))
    val execMetrics3 = MemoryListenerSuite.createExecutorMetrics("host-1", 0L, 15, 15)
    listener.onExecutorMetricsUpdate(MemoryListenerSuite.createExecutorMetricsUpdateEvent(
      execId1, execMetrics3))
    listener.onStageCompleted(MemoryListenerSuite.createStageEndEvent(1))

    // stage 2 and stage 3 don't get metrics
    listener.onStageSubmitted(MemoryListenerSuite.createStageStartEvent(2))
    listener.onStageCompleted(MemoryListenerSuite.createStageEndEvent(2))
    listener.onStageSubmitted(MemoryListenerSuite.createStageStartEvent(3))
    listener.onStageCompleted(MemoryListenerSuite.createStageEndEvent(3))

    // both stage 2 and stage 3 will use the metrics last updated in stage 1
    val mapForStage2 = listener.completedStagesToMem.get((2, 0)).get
    val memInfo2 = mapForStage2.get(execId1).get
    assert(memInfo2.transportInfo.isDefined)
    val transMetrics2 = memInfo2.transportInfo.get
    assert((15, 15, MemTime(15, 0), MemTime(15, 0)) === (transMetrics2.onHeapSize,
      transMetrics2.offHeapSize,
      transMetrics2.peakOnHeapSizeTime,
      transMetrics2.peakOffHeapSizeTime))

    val mapForStage3 = listener.completedStagesToMem.get((3, 0)).get
    val memInfo3 = mapForStage3.get(execId1).get
    assert(memInfo3.transportInfo.isDefined)
    val transMetrics3 = memInfo3.transportInfo.get
    assert((15, 15, MemTime(15, 0), MemTime(15, 0)) === (transMetrics3.onHeapSize,
      transMetrics3.offHeapSize,
      transMetrics3.peakOnHeapSizeTime,
      transMetrics3.peakOffHeapSizeTime))

    listener.onExecutorRemoved(SparkListenerExecutorRemoved(0L, execId1, ""))
  }

  test("test multiple executors") {
    val listener = new MemoryListener
    val execId1 = "exec-1"
    val execId2 = "exec-2"
    val execId3 = "exec-3"

    // two executors added first
    listener.onExecutorAdded(
      SparkListenerExecutorAdded(0L, execId1, new ExecutorInfo("host1", 1, Map.empty)))
    listener.onExecutorAdded(
      SparkListenerExecutorAdded(0L, execId2, new ExecutorInfo("host2", 1, Map.empty)))

    // three executors running in one stage and one executor is removed before stage complete
    listener.onStageSubmitted(MemoryListenerSuite.createStageStartEvent(1))
    val exec1Metrics = MemoryListenerSuite.createExecutorMetrics("host-1", 1446336000L, 20, 10)
    listener.onExecutorMetricsUpdate(MemoryListenerSuite.createExecutorMetricsUpdateEvent(
      execId1, exec1Metrics))
    val exec2Metrics = MemoryListenerSuite.createExecutorMetrics("host-2", 1446337000L, 15, 5)
    listener.onExecutorMetricsUpdate(MemoryListenerSuite.createExecutorMetricsUpdateEvent(
      execId2, exec2Metrics))
    // one more executor added during the stage is running
    listener.onExecutorAdded(
      SparkListenerExecutorAdded(0L, execId3, new ExecutorInfo("host3", 1, Map.empty)))
    val exec3Metrics = MemoryListenerSuite.createExecutorMetrics("host-3", 1446338000L, 30, 15)
    listener.onExecutorMetricsUpdate(MemoryListenerSuite.createExecutorMetricsUpdateEvent(
      execId3, exec3Metrics))
    // executor 2 removed before stage complete
    listener.onExecutorRemoved(SparkListenerExecutorRemoved(0L, execId2, ""))
    listener.onStageCompleted(MemoryListenerSuite.createStageEndEvent(1))

    listener.onExecutorRemoved(SparkListenerExecutorRemoved(0L, execId1, ""))
    listener.onExecutorRemoved(SparkListenerExecutorRemoved(0L, execId3, ""))

    // the completedStagesToMem will maintain the metrics of both the removed executors and new
    // added executors
    val mapForStage1 = listener.completedStagesToMem.get((1, 0)).get
    assert(mapForStage1.size === 3)
    val memInfo1 = mapForStage1.get(execId1).get
    val memInfo2 = mapForStage1.get(execId2).get
    val memInfo3 = mapForStage1.get(execId3).get
    val transMetrics1 = memInfo1.transportInfo.get
    val transMetrics2 = memInfo2.transportInfo.get
    val transMetrics3 = memInfo3.transportInfo.get
    assert((20, 10, MemTime(20, 1446336000), MemTime(10, 1446336000)) === (
      transMetrics1.onHeapSize,
      transMetrics1.offHeapSize,
      transMetrics1.peakOnHeapSizeTime,
      transMetrics1.peakOffHeapSizeTime))
    assert((15, 5, MemTime(15, 1446337000), MemTime(5, 1446337000)) === (
      transMetrics2.onHeapSize,
      transMetrics2.offHeapSize,
      transMetrics2.peakOnHeapSizeTime,
      transMetrics2.peakOffHeapSizeTime))
    assert((30, 15, MemTime(30, 1446338000), MemTime(15, 1446338000)) === (
      transMetrics3.onHeapSize,
      transMetrics3.offHeapSize,
      transMetrics3.peakOnHeapSizeTime,
      transMetrics3.peakOffHeapSizeTime))
  }
}

object MemoryListenerSuite {
  def createStageStartEvent(stageId: Int): SparkListenerStageSubmitted = {
    val stageInfo = new StageInfo(stageId, 0, stageId.toString, 0, Seq.empty, Seq.empty, "")
    SparkListenerStageSubmitted(stageInfo)
  }

  def createStageEndEvent(stageId: Int, failed: Boolean = false): SparkListenerStageCompleted = {
    val stageInfo = new StageInfo(stageId, 0, stageId.toString, 0, Seq.empty, Seq.empty, "")
    if (failed) {
      stageInfo.failureReason = Some("Failed!")
    }
    SparkListenerStageCompleted(stageInfo)
  }

  def createExecutorMetricsUpdateEvent(
      execId: String,
      executorMetrics: ExecutorMetrics): SparkListenerExecutorMetricsUpdate = {
    SparkListenerExecutorMetricsUpdate(execId, executorMetrics, Seq.empty)
  }

  def createExecutorMetrics(
      hostname: String,
      timeStamp: Long,
      onHeapSize: Long,
      offHeapSize: Long): ExecutorMetrics = {
    val execMetrics = new ExecutorMetrics
    execMetrics.setHostname(hostname)
    execMetrics.setTransportMetrics(TransportMetrics(timeStamp, onHeapSize, offHeapSize))
    execMetrics
  }
}
