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

import java.util.Properties

import org.scalatest.Matchers

import org.apache.spark._
import org.apache.spark.{LocalSparkContext, SparkConf, Success}
import org.apache.spark.executor._
import org.apache.spark.scheduler._
import org.apache.spark.util.Utils

class MemoryListenerSuite extends SparkFunSuite with LocalSparkContext with Matchers {
  private def createStageStartEvent(stageId: Int) = {
    val stageInfo = new StageInfo(stageId, 0, stageId.toString, 0, null, null, "")
    SparkListenerStageSubmitted(stageInfo)
  }

  private def createStageEndEvent(stageId: Int, failed: Boolean = false) = {
    val stageInfo = new StageInfo(stageId, 0, stageId.toString, 0, null, null, "")
    if (failed) {
      stageInfo.failureReason = Some("Failed!")
    }
    SparkListenerStageCompleted(stageInfo)
  }

  private def createExecutorMetricsUpdateEvent(
      execId: String,
      executorMetrics: ExecutorMetrics) = {
    SparkListenerExecutorMetricsUpdate(execId, executorMetrics, null)
  }

  private def createExecutorMetrics(
      hostname: String,
      timeStamp: Long,
      onHeapSize: Long,
      offHeapSize: Long): ExecutorMetrics = {
    val execMetrics = new ExecutorMetrics
    execMetrics.setHostname(hostname)
    execMetrics.setTransportMetrics(TransportMetrics(timeStamp, onHeapSize, offHeapSize))
    execMetrics
  }

  test("test HashMap size for MemoryListener") {
    val listener = new MemoryListener
    val execId1 = "exec-1"
    val execId2 = "exec-2"

    (1 to 2).foreach { i =>
      listener.onStageSubmitted(createStageStartEvent(i))
      listener.onStageCompleted(createStageEndEvent(i))
    }
    // stages are all completed, no activeStages now
    assert(listener.activeStagesToMem.isEmpty)

    listener.onExecutorMetricsUpdate(createExecutorMetricsUpdateEvent(
      execId1, new ExecutorMetrics))
    // ExecutorMetrics is not related with Stages directly
    assert(listener.activeStagesToMem.isEmpty)

    listener.onStageSubmitted(createStageStartEvent(3))
    listener.onExecutorMetricsUpdate(createExecutorMetricsUpdateEvent(
      execId2, new ExecutorMetrics))
    // totally 2 executors updated their metrics
    assert(listener.activeExecutorIdToMem.size == 2)
    assert(listener.activeStagesToMem.size == 1)
    listener.onStageCompleted(createStageEndEvent(3))

    assert(listener.activeStagesToMem.isEmpty)
    assert(listener.completedStagesToMem.size == 3)
    assert(listener.activeExecutorIdToMem.size == listener.latestExecIdToExecMetrics.size)
    assert(listener.removedExecutorIdToMem.isEmpty)
  }
}
