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

import java.util.{Timer, TimerTask}

import scala.collection.mutable.{ArrayBuffer, HashMap}

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}

class BarrierCoordinator(
    timeout: Long,
    override val rpcEnv: RpcEnv) extends ThreadSafeRpcEndpoint with Logging {

  private val timer = new Timer("BarrierCoordinator barrier epoch increment timer")

  // Barrier epoch for each stage attempt, fail a sync request if the barrier epoch in the request
  // mismatches the barrier epoch in the coordinator.
  private val barrierEpochByStageIdAndAttempt = new HashMap[Int, HashMap[Int, Int]]

  // Any access to this should be synchronized.
  private val syncRequestsByStageIdAndAttempt =
    new HashMap[Int, HashMap[Int, ArrayBuffer[RpcCallContext]]]

  /**
   * Get the array of [[RpcCallContext]]s that correspond to a barrier sync request from a stage
   * attempt.
   */
  private def getOrInitSyncRequests(
      stageId: Int,
      stageAttemptId: Int,
      numTasks: Int = 0): ArrayBuffer[RpcCallContext] = synchronized {
    val syncRequestsByStage = syncRequestsByStageIdAndAttempt
      .getOrElseUpdate(stageId, new HashMap[Int, ArrayBuffer[RpcCallContext]])
    syncRequestsByStage.getOrElseUpdate(stageAttemptId, new ArrayBuffer[RpcCallContext](numTasks))
  }

  /**
   * Clean up the array of [[RpcCallContext]]s that correspond to a barrier sync request from a
   * stage attempt.
   */
  private def cleanupSyncRequests(stageId: Int, stageAttemptId: Int): Unit = synchronized {
    syncRequestsByStageIdAndAttempt.get(stageId).foreach { syncRequestByStage =>
      syncRequestByStage.get(stageAttemptId).foreach { syncRequests =>
        syncRequests.clear()
      }
      syncRequestByStage -= stageAttemptId
      if (syncRequestByStage.isEmpty) {
        syncRequestsByStageIdAndAttempt -= stageId
      }
      logInfo(s"Removed all the pending barrier sync requests from Stage $stageId(Attempt " +
        s"$stageAttemptId).")
    }
  }

  /**
   * Get the barrier epoch that correspond to a barrier sync request from a stage attempt.
   */
  private def getOrInitBarrierEpoch(stageId: Int, stageAttemptId: Int): Int = synchronized {
    val barrierEpochByStage = barrierEpochByStageIdAndAttempt
      .getOrElseUpdate(stageId, new HashMap[Int, Int])
    val barrierEpoch = barrierEpochByStage.getOrElseUpdate(stageAttemptId, 0)
    logInfo(s"Current barrier epoch for Stage $stageId(Attempt $stageAttemptId) is $barrierEpoch.")
    barrierEpoch
  }

  /**
   * Update the barrier epoch that correspond to a barrier sync request from a stage attempt.
   */
  private def updateBarrierEpoch(
      stageId: Int,
      stageAttemptId: Int,
      newBarrierEpoch: Int): Unit = synchronized {
    val barrierEpochByStage = barrierEpochByStageIdAndAttempt
      .getOrElseUpdate(stageId, new HashMap[Int, Int])
    barrierEpochByStage.put(stageAttemptId, newBarrierEpoch)
    logInfo(s"Current barrier epoch for Stage $stageId(Attempt $stageAttemptId) is " +
      s"$newBarrierEpoch.")
  }

  /**
   * Send failure to all the blocking barrier sync requests from a stage attempt with proper
   * failure message.
   */
  private def failAllSyncRequests(
      syncRequests: ArrayBuffer[RpcCallContext],
      message: String): Unit = {
    syncRequests.foreach(_.sendFailure(new SparkException(message)))
  }

  /**
   * Finish all the blocking barrier sync requests from a stage attempt successfully if we
   * have received all the sync requests.
   */
  private def maybeFinishAllSyncRequests(
      syncRequests: ArrayBuffer[RpcCallContext],
      numTasks: Int): Boolean = {
    if (syncRequests.size == numTasks) {
      syncRequests.foreach(_.reply(()))
      return true
    }

    false
  }


  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RequestToSync(numTasks, stageId, stageAttemptId, taskAttemptId, barrierEpoch) =>
      // Check the barrier epoch, fail the sync request if barrier epoch mismatches.
      val currentBarrierEpoch = getOrInitBarrierEpoch(stageId, stageAttemptId)
      val syncRequests = getOrInitSyncRequests(stageId, stageAttemptId)
      if (barrierEpoch != currentBarrierEpoch) {
        syncRequests += context
        failAllSyncRequests(syncRequests,
          "The request to sync fails due to mismatched barrier epoch, the barrier epoch from " +
            s"task $taskAttemptId is $barrierEpoch, while the barrier epoch from the " +
            s"coordinator is $currentBarrierEpoch.")
        cleanupSyncRequests(stageId, stageAttemptId)
        // The global sync fails so the stage is expected to retry another attempt, all sync
        // messages come from current stage attempt shall fail.
        updateBarrierEpoch(stageId, stageAttemptId, -1)
      } else {
        // If this is the first sync message received for a barrier() call, init a timer to ensure
        // we may timeout for the sync.
        if (syncRequests.isEmpty) {
          timer.schedule(new TimerTask {
            override def run(): Unit = {
              // Timeout for current barrier() call, fail all the sync requests and reset the
              // barrier epoch.
              val requests = getOrInitSyncRequests(stageId, stageAttemptId)
              failAllSyncRequests(requests,
                "The coordinator didn't get all barrier sync requests for barrier epoch " +
                  s"$barrierEpoch from Stage $stageId(Attempt $stageAttemptId) within $timeout " +
                  "ms.")
              cleanupSyncRequests(stageId, stageAttemptId)
              // The global sync fails so the stage is expected to retry another attempt, all sync
              // messages come from current stage attempt shall fail.
              updateBarrierEpoch(stageId, stageAttemptId, -1)
            }
          }, timeout)
        }

        syncRequests += context
        logInfo(s"Barrier sync epoch $barrierEpoch from Stage $stageId(Attempt $stageAttemptId) " +
          s"received update from Task $taskAttemptId, current progress: " +
          s"${syncRequests.size}/$numTasks.")
        if (maybeFinishAllSyncRequests(syncRequests, numTasks)) {
          // Finished current barrier() call successfully, clean up internal data and increase the
          // barrier epoch.
          logInfo(s"Barrier sync epoch $barrierEpoch from Stage $stageId(Attempt " +
            s"$stageAttemptId) received all updates from tasks, finished successfully.")
          cleanupSyncRequests(stageId, stageAttemptId)
          updateBarrierEpoch(stageId, stageAttemptId, currentBarrierEpoch + 1)
        }
      }
  }

  override def onStop(): Unit = timer.cancel()
}

private[spark] sealed trait BarrierCoordinatorMessage extends Serializable

private[spark] case class RequestToSync(
    numTasks: Int,
    stageId: Int,
    stageAttemptId: Int,
    taskAttemptId: Long,
    barrierEpoch: Int) extends BarrierCoordinatorMessage
