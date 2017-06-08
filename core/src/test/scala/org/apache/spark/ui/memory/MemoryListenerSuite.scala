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

class MemoryListenerSuite extends SparkFunSuite {

  test("test stages use executor metrics updated in previous stages") {
    val listener = new MemoryListener
    val execId1 = "exec-1"
    val host1 = "host-1"
    val port: Option[Int] = Some(80)

    listener.onExecutorAdded(
      SparkListenerExecutorAdded(1L, execId1, new ExecutorInfo(host1, 1, Map.empty)))

    // stage 1, no metrics update
    listener.onStageSubmitted(MemoryListenerSuite.createStageStartEvent(1))
    listener.onStageCompleted(MemoryListenerSuite.createStageEndEvent(1))

    // multiple metrics updated in stage 2
    listener.onStageSubmitted(MemoryListenerSuite.createStageStartEvent(2))
    val execMetrics1 = MemoryListenerSuite.createExecutorMetrics(host1, port, 2L, 20, 10)
    listener.onExecutorMetricsUpdate(MemoryListenerSuite.createExecutorMetricsUpdateEvent(
      execId1, execMetrics1))
    val execMetrics2 = MemoryListenerSuite.createExecutorMetrics(host1, port, 3L, 30, 5)
    listener.onExecutorMetricsUpdate(MemoryListenerSuite.createExecutorMetricsUpdateEvent(
      execId1, execMetrics2))
    val execMetrics3 = MemoryListenerSuite.createExecutorMetrics(host1, port, 4L, 15, 15)
    listener.onExecutorMetricsUpdate(MemoryListenerSuite.createExecutorMetricsUpdateEvent(
      execId1, execMetrics3))
    listener.onStageCompleted(MemoryListenerSuite.createStageEndEvent(2))

    // stage 3 and stage 4 don't get metrics
    listener.onStageSubmitted(MemoryListenerSuite.createStageStartEvent(3))
    listener.onStageCompleted(MemoryListenerSuite.createStageEndEvent(3))
    listener.onStageSubmitted(MemoryListenerSuite.createStageStartEvent(4))
    listener.onStageCompleted(MemoryListenerSuite.createStageEndEvent(4))

    // no metrics for stage 1 since no metrics updated for stage 1
    val mapForStage1 = listener.completedStagesToMem((1, 0))
    assert(mapForStage1.get(execId1).get.transportInfo === None)

    // metrics is with aggregated value for stage 2 when there are more than one metrics updated
    val mapForStage2 = listener.completedStagesToMem((2, 0))
    val transMetrics2 = mapForStage2(execId1).transportInfo.get
    MemoryListenerSuite.assertTransMetrics(
      15, 15, MemTime(30, 3), MemTime(15, 4), transMetrics2)

    // both stage 3 and stage 4 will use the metrics last updated in stage 2
    val mapForStage3 = listener.completedStagesToMem((3, 0))
    val memInfo3 = mapForStage3(execId1)
    assert(memInfo3.transportInfo.isDefined)
    val transMetrics3 = memInfo3.transportInfo.get
    MemoryListenerSuite.assertTransMetrics(
      15, 15, MemTime(15, 4), MemTime(15, 4), transMetrics3)

    val mapForStage4 = listener.completedStagesToMem((4, 0))
    val memInfo4 = mapForStage4(execId1)
    assert(memInfo4.transportInfo.isDefined)
    val transMetrics4 = memInfo4.transportInfo.get
    MemoryListenerSuite.assertTransMetrics(
      15, 15, MemTime(15, 4), MemTime(15, 4), transMetrics4)

    listener.onExecutorRemoved(SparkListenerExecutorRemoved(0L, execId1, ""))
  }

  test("test multiple executors with multiple stages") {
    val listener = new MemoryListener
    val (execId1, execId2, execId3) = ("exec-1", "exec-2", "exec-3")
    val (host1, host2, host3) = ("host-1", "host-2", "host-3")
    val (port1, port2, port3): (Option[Int], Option[Int], Option[Int]) =
      (Some(80), Some(80), Some(80))

    // two executors added first
    listener.onExecutorAdded(
      SparkListenerExecutorAdded(1L, execId1, new ExecutorInfo(host1, 1, Map.empty)))
    listener.onExecutorAdded(
      SparkListenerExecutorAdded(2L, execId2, new ExecutorInfo(host2, 1, Map.empty)))

    // three executors running in one stage and one executor is removed before stage complete
    listener.onStageSubmitted(MemoryListenerSuite.createStageStartEvent(1))
    val exec1Metrics = MemoryListenerSuite.createExecutorMetrics(host1, port1, 3L, 20, 10)
    listener.onExecutorMetricsUpdate(MemoryListenerSuite.createExecutorMetricsUpdateEvent(
      execId1, exec1Metrics))
    val exec2Metrics = MemoryListenerSuite.createExecutorMetrics(host2, port2, 4L, 15, 5)
    listener.onExecutorMetricsUpdate(MemoryListenerSuite.createExecutorMetricsUpdateEvent(
      execId2, exec2Metrics))
    // one more executor added during the stage is running
    listener.onExecutorAdded(
      SparkListenerExecutorAdded(0L, execId3, new ExecutorInfo(host3, 1, Map.empty)))
    val exec3Metrics = MemoryListenerSuite.createExecutorMetrics(host3, port3, 5L, 30, 15)
    listener.onExecutorMetricsUpdate(MemoryListenerSuite.createExecutorMetricsUpdateEvent(
      execId3, exec3Metrics))

    assert(listener.activeExecutorIdToMem.size === 3)
    assert(listener.removedExecutorIdToMem.isEmpty)

    // executor 2 removed before stage complete
    listener.onExecutorRemoved(SparkListenerExecutorRemoved(6L, execId2, ""))
    listener.onStageCompleted(MemoryListenerSuite.createStageEndEvent(1))
    (2 to 3).foreach { i =>
      listener.onStageSubmitted(MemoryListenerSuite.createStageStartEvent(i))
      listener.onStageCompleted(MemoryListenerSuite.createStageEndEvent(i))
    }

    // stages are all completed, no activeStages now
    assert(listener.activeStagesToMem.isEmpty)

    listener.onStageSubmitted(MemoryListenerSuite.createStageStartEvent(4))
    listener.onExecutorMetricsUpdate(MemoryListenerSuite.createExecutorMetricsUpdateEvent(
      execId2, new ExecutorMetrics))

    assert(listener.activeExecutorIdToMem.size === 3)
    assert(listener.activeStagesToMem.size === 1)

    listener.onStageCompleted(MemoryListenerSuite.createStageEndEvent(4))

    assert(listener.activeStagesToMem.isEmpty)
    assert(listener.completedStagesToMem.size === 4)
    assert(listener.removedExecutorIdToMem.size === 1)

    listener.onExecutorRemoved(SparkListenerExecutorRemoved(7L, execId1, ""))
    listener.onExecutorRemoved(SparkListenerExecutorRemoved(8L, execId3, ""))

    assert(listener.removedExecutorIdToMem.size === 3)

    // the {{completedStagesToMem}} will maintain the metrics of both the removed executors and
    // new added executors
    val mapForStage1 = listener.completedStagesToMem((1, 0))
    assert(mapForStage1.size === 3)

    val transMetrics1 = mapForStage1(execId1).transportInfo.get
    val transMetrics2 = mapForStage1(execId2).transportInfo.get
    val transMetrics3 = mapForStage1(execId3).transportInfo.get

    MemoryListenerSuite.assertTransMetrics(
      20, 10, MemTime(20, 3), MemTime(10, 3), transMetrics1)
    MemoryListenerSuite.assertTransMetrics(
      15, 5, MemTime(15, 4), MemTime(5, 4), transMetrics2)
    MemoryListenerSuite.assertTransMetrics(
      30, 15, MemTime(30, 5), MemTime(15, 5), transMetrics3)
  }
}

object MemoryListenerSuite extends SparkFunSuite {
  def createStageStartEvent(stageId: Int): SparkListenerStageSubmitted = {
    val stageInfo = new StageInfo(stageId, 0, stageId.toString, 0, Seq.empty, Seq.empty, "")
    SparkListenerStageSubmitted(stageInfo)
  }

  def createStageEndEvent(stageId: Int): SparkListenerStageCompleted = {
    val stageInfo = new StageInfo(stageId, 0, stageId.toString, 0, Seq.empty, Seq.empty, "")
    SparkListenerStageCompleted(stageInfo)
  }

  def createExecutorMetricsUpdateEvent(
      execId: String,
      executorMetrics: ExecutorMetrics): SparkListenerExecutorMetricsUpdate = {
    SparkListenerExecutorMetricsUpdate(execId, executorMetrics, Seq.empty)
  }

  def createExecutorMetrics(
      hostname: String,
      port: Option[Int],
      timeStamp: Long,
      onHeapSize: Long,
      offHeapSize: Long): ExecutorMetrics = {
    ExecutorMetrics(hostname, port, TransportMetrics(timeStamp, onHeapSize, offHeapSize))
  }

  def assertTransMetrics(
      onHeapSize: Long,
      offHeapSize: Long,
      peakOnHeapSizeTime: MemTime,
      peakOffHeapSizTime: MemTime,
      transMemSize: TransportMemSize): Unit = {
    assert(onHeapSize === transMemSize.onHeapSize)
    assert(offHeapSize === transMemSize.offHeapSize)
    assert(peakOnHeapSizeTime === transMemSize.peakOnHeapSizeTime)
    assert(peakOffHeapSizTime === transMemSize.peakOffHeapSizeTime)
  }
}
