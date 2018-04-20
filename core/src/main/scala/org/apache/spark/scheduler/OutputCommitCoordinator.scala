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

import scala.collection.mutable

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.util.{RpcUtils, ThreadUtils}

private sealed trait OutputCommitCoordinationMessage extends Serializable

private case object StopCoordinator extends OutputCommitCoordinationMessage
private case class AskPermissionToCommitOutput(stage: Int, partition: Int, attemptNumber: Int)

/**
 * Authority that decides whether tasks can commit output to HDFS. Uses a "first committer wins"
 * policy.
 *
 * OutputCommitCoordinator is instantiated in both the drivers and executors. On executors, it is
 * configured with a reference to the driver's OutputCommitCoordinatorEndpoint, so requests to
 * commit output will be forwarded to the driver's OutputCommitCoordinator.
 *
 * This class was introduced in SPARK-4879; see that JIRA issue (and the associated pull requests)
 * for an extensive design discussion.
 */
private[spark] class OutputCommitCoordinator(conf: SparkConf, isDriver: Boolean) extends Logging {

  // Initialized by SparkEnv
  var coordinatorRef: Option[RpcEndpointRef] = None

  private type StageId = Int
  private type PartitionId = Int
  private type TaskAttemptNumber = Int
  private val NO_AUTHORIZED_COMMITTER: TaskAttemptNumber = -1
  private case class StageState(numPartitions: Int) {
    val authorizedCommitters = Array.fill[TaskAttemptNumber](numPartitions)(NO_AUTHORIZED_COMMITTER)
    val failures = mutable.Map[PartitionId, mutable.Set[TaskAttemptNumber]]()
  }

  /**
   * Map from active stages's id => authorized task attempts for each partition id, which hold an
   * exclusive lock on committing task output for that partition, as well as any known failed
   * attempts in the stage.
   *
   * Entries are added to the top-level map when stages start and are removed they finish
   * (either successfully or unsuccessfully).
   *
   * Access to this map should be guarded by synchronizing on the OutputCommitCoordinator instance.
   */
  private val stageStates = mutable.Map[StageId, StageState]()

  /**
   * Returns whether the OutputCommitCoordinator's internal data structures are all empty.
   */
  def isEmpty: Boolean = {
    stageStates.isEmpty
  }

  /**
   * Called by tasks to ask whether they can commit their output to HDFS.
   *
   * If a task attempt has been authorized to commit, then all other attempts to commit the same
   * task will be denied.  If the authorized task attempt fails (e.g. due to its executor being
   * lost), then a subsequent task attempt may be authorized to commit its output.
   *
   * @param stage the stage number
   * @param partition the partition number
   * @param attemptNumber how many times this task has been attempted
   *                      (see [[TaskContext.attemptNumber()]])
   * @return true if this task is authorized to commit, false otherwise
   */
  def canCommit(
      stage: StageId,
      partition: PartitionId,
      attemptNumber: TaskAttemptNumber): Boolean = {
    val msg = AskPermissionToCommitOutput(stage, partition, attemptNumber)
    coordinatorRef match {
      case Some(endpointRef) =>
        ThreadUtils.awaitResult(endpointRef.ask[Boolean](msg),
          RpcUtils.askRpcTimeout(conf).duration)
      case None =>
        logError(
          "canCommit called after coordinator was stopped (is SparkEnv shutdown in progress)?")
        false
    }
  }

  /**
   * Called by the DAGScheduler when a stage starts.
   *
   * @param stage the stage id.
   * @param maxPartitionId the maximum partition id that could appear in this stage's tasks (i.e.
   *                       the maximum possible value of `context.partitionId`).
   */
  private[scheduler] def stageStart(stage: StageId, maxPartitionId: Int): Unit = synchronized {
    stageStates(stage) = new StageState(maxPartitionId + 1)
  }

  // Called by DAGScheduler
  private[scheduler] def stageEnd(stage: StageId): Unit = synchronized {
    stageStates.remove(stage)
  }

  // Called by DAGScheduler
  private[scheduler] def taskCompleted(
      stage: StageId,
      partition: PartitionId,
      attemptNumber: TaskAttemptNumber,
      reason: TaskEndReason): Unit = synchronized {
    val stageState = stageStates.getOrElse(stage, {
      logDebug(s"Ignoring task completion for completed stage")
      return
    })
    reason match {
      case Success =>
      // The task output has been committed successfully
      case denied: TaskCommitDenied =>
        logInfo(s"Task was denied committing, stage: $stage, partition: $partition, " +
          s"attempt: $attemptNumber")
      case otherReason =>
        // Mark the attempt as failed to blacklist from future commit protocol
        stageState.failures.getOrElseUpdate(partition, mutable.Set()) += attemptNumber
        if (stageState.authorizedCommitters(partition) == attemptNumber) {
          logDebug(s"Authorized committer (attemptNumber=$attemptNumber, stage=$stage, " +
            s"partition=$partition) failed; clearing lock")
          stageState.authorizedCommitters(partition) = NO_AUTHORIZED_COMMITTER
        }
    }
  }

  def stop(): Unit = synchronized {
    if (isDriver) {
      coordinatorRef.foreach(_ send StopCoordinator)
      coordinatorRef = None
      stageStates.clear()
    }
  }

  // Marked private[scheduler] instead of private so this can be mocked in tests
  private[scheduler] def handleAskPermissionToCommit(
      stage: StageId,
      partition: PartitionId,
      attemptNumber: TaskAttemptNumber): Boolean = synchronized {
    stageStates.get(stage) match {
      case Some(state) if attemptFailed(state, partition, attemptNumber) =>
        logInfo(s"Denying attemptNumber=$attemptNumber to commit for stage=$stage," +
          s" partition=$partition as task attempt $attemptNumber has already failed.")
        false
      case Some(state) =>
        state.authorizedCommitters(partition) match {
          case NO_AUTHORIZED_COMMITTER =>
            logDebug(s"Authorizing attemptNumber=$attemptNumber to commit for stage=$stage, " +
              s"partition=$partition")
            state.authorizedCommitters(partition) = attemptNumber
            true
          case existingCommitter =>
            // Coordinator should be idempotent when receiving AskPermissionToCommit.
            if (existingCommitter == attemptNumber) {
              logWarning(s"Authorizing duplicate request to commit for " +
                s"attemptNumber=$attemptNumber to commit for stage=$stage," +
                s" partition=$partition; existingCommitter = $existingCommitter." +
                s" This can indicate dropped network traffic.")
              true
            } else {
              logDebug(s"Denying attemptNumber=$attemptNumber to commit for stage=$stage, " +
                s"partition=$partition; existingCommitter = $existingCommitter")
              false
            }
        }
      case None =>
        logDebug(s"Stage $stage has completed, so not allowing" +
          s" attempt number $attemptNumber of partition $partition to commit")
        false
    }
  }

  private def attemptFailed(
      stageState: StageState,
      partition: PartitionId,
      attempt: TaskAttemptNumber): Boolean = synchronized {
    stageState.failures.get(partition).exists(_.contains(attempt))
  }
}

private[spark] object OutputCommitCoordinator {

  // This endpoint is used only for RPC
  private[spark] class OutputCommitCoordinatorEndpoint(
      override val rpcEnv: RpcEnv, outputCommitCoordinator: OutputCommitCoordinator)
    extends RpcEndpoint with Logging {

    logDebug("init") // force eager creation of logger

    override def receive: PartialFunction[Any, Unit] = {
      case StopCoordinator =>
        logInfo("OutputCommitCoordinator stopped!")
        stop()
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case AskPermissionToCommitOutput(stage, partition, attemptNumber) =>
        context.reply(
          outputCommitCoordinator.handleAskPermissionToCommit(stage, partition, attemptNumber))
    }
  }
}
