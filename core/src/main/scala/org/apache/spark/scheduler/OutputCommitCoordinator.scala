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

private sealed trait OutputCommitCoordinationMessage extends Serializable

private case object StopCoordinator extends OutputCommitCoordinationMessage
private case class AskPermissionToCommitOutput(
    stage: Int,
    stageAttempt: Int,
    partition: Int,
    attemptNumber: Int)

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

  // Class used to identify a committer. The task ID for a committer is implicitly defined by
  // the partition being processed, but the coordinator needs to keep track of both the stage
  // attempt and the task attempt, because in some situations the same task may be running
  // concurrently in two different attempts of the same stage.
  private case class TaskIdentifier(stageAttempt: Int, taskAttempt: Int)

  /**
   * Map from active stages's id => partition id => task attempt with exclusive lock on committing
   * output for that partition.
   *
   * Entries are added to the top-level map when stages start and are removed they finish
   * (either successfully or unsuccessfully).
   *
   * Access to this map should be guarded by synchronizing on the OutputCommitCoordinator instance.
   */
  private val authorizedCommittersByStage = mutable.Map[Int, Array[TaskIdentifier]]()

  /**
   * Returns whether the OutputCommitCoordinator's internal data structures are all empty.
   */
  def isEmpty: Boolean = {
    authorizedCommittersByStage.isEmpty
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
      stage: Int,
      stageAttempt: Int,
      partition: Int,
      attemptNumber: Int): Boolean = {
    val msg = AskPermissionToCommitOutput(stage, stageAttempt, partition, attemptNumber)
    coordinatorRef match {
      case Some(endpointRef) =>
        endpointRef.askWithRetry[Boolean](msg)
      case None =>
        logError(
          "canCommit called after coordinator was stopped (is SparkEnv shutdown in progress)?")
        false
    }
  }

  /**
   * Called by the DAGScheduler when a stage starts. Initializes the stage's state if it hasn't
   * yet been initialized.
   *
   * @param stage the stage id.
   * @param maxPartitionId the maximum partition id that could appear in this stage's tasks (i.e.
   *                       the maximum possible value of `context.partitionId`).
   */
  private[scheduler] def stageStart(stage: Int, maxPartitionId: Int): Unit = synchronized {
    authorizedCommittersByStage.get(stage) match {
      case Some(committers) =>
        require(committers.length == maxPartitionId + 1)
        logInfo(s"Reusing state from previous attempt of stage $stage.")

      case _ =>
        val arr = Array.fill[TaskIdentifier](maxPartitionId + 1)(null)
        authorizedCommittersByStage(stage) = arr
    }
  }

  // Called by DAGScheduler
  private[scheduler] def stageEnd(stage: Int): Unit = synchronized {
    authorizedCommittersByStage.remove(stage)
  }

  // Called by DAGScheduler
  private[scheduler] def taskCompleted(
      stage: Int,
      stageAttempt: Int,
      partition: Int,
      attemptNumber: Int,
      reason: TaskEndReason): Unit = synchronized {
    val authorizedCommitters = authorizedCommittersByStage.getOrElse(stage, {
      logDebug(s"Ignoring task completion for completed stage")
      return
    })
    reason match {
      case Success =>
        // The task output has been committed successfully
      case _: TaskCommitDenied =>
        logInfo(s"Task was denied committing, stage: $stage.$stageAttempt, " +
          s"partition: $partition, attempt: $attemptNumber")
      case _ =>
        val taskId = TaskIdentifier(stageAttempt, attemptNumber)
        if (authorizedCommitters(partition) == taskId) {
          logDebug(s"Authorized committer (attemptNumber=$attemptNumber, stage=$stage, " +
            s"partition=$partition) failed; clearing lock")
          authorizedCommitters(partition) = null
        }
    }
  }

  def stop(): Unit = synchronized {
    if (isDriver) {
      coordinatorRef.foreach(_ send StopCoordinator)
      coordinatorRef = None
      authorizedCommittersByStage.clear()
    }
  }

  // Marked private[scheduler] instead of private so this can be mocked in tests
  private[scheduler] def handleAskPermissionToCommit(
      stage: Int,
      stageAttempt: Int,
      partition: Int,
      attemptNumber: Int): Boolean = synchronized {
    authorizedCommittersByStage.get(stage) match {
      case Some(authorizedCommitters) =>
        val existing = authorizedCommitters(partition)
        if (existing == null) {
          logDebug(s"Commit allowed for stage=$stage.$stageAttempt, partition=$partition, " +
            s"task attempt $attemptNumber")
          authorizedCommitters(partition) = TaskIdentifier(stageAttempt, attemptNumber)
          true
        } else {
          logDebug(s"Commit denied for stage=$stage.$stageAttempt, partition=$partition: " +
            s"already committed by $existing")
          false
        }
      case None =>
        logDebug(s"Commit denied for stage=$stage.$stageAttempt, partition=$partition: " +
          "stage already marked as completed.")
        false
    }
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
      case AskPermissionToCommitOutput(stage, stageAttempt, partition, attemptNumber) =>
        context.reply(
          outputCommitCoordinator.handleAskPermissionToCommit(stage, stageAttempt, partition,
            attemptNumber))
    }
  }
}
