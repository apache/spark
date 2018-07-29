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

import java.util.{Properties, Timer, TimerTask}

import scala.concurrent.duration._
import scala.language.postfixOps

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.rpc.{RpcEndpointRef, RpcTimeout}
import org.apache.spark.util.RpcUtils

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

  private val barrierCoordinator: RpcEndpointRef = {
    val env = SparkEnv.get
    RpcUtils.makeDriverRef("barrierSync", env.conf, env.rpcEnv)
  }

  private val timer = new Timer("Barrier task timer for barrier() calls.")

  private var barrierEpoch = 0

  private lazy val numTasks = localProperties.getProperty("numTasks", "0").toInt

  override def barrier(): Unit = {
    logInfo(s"Task $taskAttemptId from Stage $stageId(Attempt $stageAttemptNumber) has entered " +
      s"the global sync, current barrier epoch is $barrierEpoch.")

    val startTime = System.currentTimeMillis()
    val timerTask = new TimerTask {
      override def run(): Unit = {
        logInfo(s"Task $taskAttemptId from Stage $stageId(Attempt $stageAttemptNumber) waiting " +
          s"under the global sync since $startTime, has been waiting for " +
          s"${(System.currentTimeMillis() - startTime) / 1000} seconds, current barrier epoch " +
          s"is $barrierEpoch.")
      }
    }
    // Log the update of global sync every 60 seconds.
    timer.schedule(timerTask, 60000, 60000)

    try {
      barrierCoordinator.askSync[Unit](
        message = RequestToSync(numTasks, stageId, stageAttemptNumber, taskAttemptId, barrierEpoch),
        timeout = new RpcTimeout(31536000 /** = 3600 * 24 * 365 */ seconds, "barrierTimeout"))
      barrierEpoch += 1
      logInfo(s"Task $taskAttemptId from Stage $stageId(Attempt $stageAttemptNumber) finished " +
        "global sync successfully, waited for " +
        s"${(System.currentTimeMillis() - startTime) / 1000} seconds, current barrier epoch is " +
        s"$barrierEpoch.")
    } catch {
      case e: SparkException =>
        logInfo(s"Task $taskAttemptId from Stage $stageId(Attempt $stageAttemptNumber) failed " +
          "to perform global sync, waited for " +
          s"${(System.currentTimeMillis() - startTime) / 1000} seconds, current barrier epoch " +
          s"is $barrierEpoch.")
        throw e
    } finally {
      timerTask.cancel()
    }
  }

  override def getTaskInfos(): Array[BarrierTaskInfo] = {
    val addressesStr = localProperties.getProperty("addresses", "")
    addressesStr.split(",").map(_.trim()).map(new BarrierTaskInfo(_))
  }
}
