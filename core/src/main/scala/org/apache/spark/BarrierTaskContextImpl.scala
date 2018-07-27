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

package org.apache.spark

import java.util.Properties

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.metrics.MetricsSystem

/** A [[BarrierTaskContext]] implementation. */
private[spark] class BarrierTaskContextImpl(
    override val stageId: Int,
    override val stageAttemptNumber: Int,
    override val partitionId: Int,
    override val taskAttemptId: Long,
    override val attemptNumber: Int,
    override val taskMemoryManager: TaskMemoryManager,
    localProperties: Properties,
    @transient private val metricsSystem: MetricsSystem,
    // The default value is only used in tests.
    override val taskMetrics: TaskMetrics = TaskMetrics.empty)
  extends TaskContextImpl(stageId, stageAttemptNumber, partitionId, taskAttemptId, attemptNumber,
      taskMemoryManager, localProperties, metricsSystem, taskMetrics)
    with BarrierTaskContext {

  // TODO SPARK-24817 implement global barrier.
  override def barrier(): Unit = {}

  override def getTaskInfos(): Array[BarrierTaskInfo] = {
    val addressesStr = localProperties.getProperty("addresses", "")
    addressesStr.split(",").map(_.trim()).map(new BarrierTaskInfo(_))
  }
}
