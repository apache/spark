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

import akka.actor.{ActorRef, Actor}

import org.apache.spark._
import org.apache.spark.util.{AkkaUtils, ActorLogReceive}

private sealed trait OutputCommitCoordinationMessage extends Serializable

private case object StopCoordinator extends OutputCommitCoordinationMessage
private case class AskPermissionToCommitOutput(stage: Int, task: Long, taskAttempt: Long)

/**
 * Authority that decides whether tasks can commit output to HDFS.
 *
 * This lives on the driver, but the actor allows the tasks that commit to Hadoop to invoke it.
 *
 * This class was introduced in SPARK-4879; see that JIRA issue (and the associated pull requests)
 * for an extensive design discussion.
 */
private[spark] class OutputCommitCoordinator(conf: SparkConf) extends Logging {

  // Initialized by SparkEnv
  var coordinatorActor: Option[ActorRef] = None
  private val timeout = AkkaUtils.askTimeout(conf)
  private val maxAttempts = AkkaUtils.numRetries(conf)
  private val retryInterval = AkkaUtils.retryWaitMs(conf)

  private type StageId = Int
  private type TaskId = Long
  private type TaskAttemptId = Long
  private type CommittersByStageMap = mutable.Map[StageId, mutable.Map[TaskId, TaskAttemptId]]

  // Access to this state should be guarded by synchronizing on the instance.
  private val authorizedCommittersByStage: CommittersByStageMap = mutable.Map()

  /**
   * Called by tasks to ask whether they can commit their output to HDFS.
   *
   * If a task attempt has been authorized to commit, then all other attempts to commit the same
   * task will be denied.  If the authorized task attempt fails (e.g. due to its executor being
   * lost), then a subsequent task attempt may be authorized to commit its output.
   *
   * @param stage the stage number
   * @param task the task number
   * @param attempt a unique identifier for this task attempt
   * @return true if this task is authorized to commit, false otherwise
   */
  def canCommit(
      stage: StageId,
      task: TaskId,
      attempt: TaskAttemptId): Boolean = {
    val msg = AskPermissionToCommitOutput(stage, task, attempt)
    coordinatorActor match {
      case Some(actor) =>
        AkkaUtils.askWithReply[Boolean](msg, actor, maxAttempts, retryInterval, timeout)
      case None =>
        logError(
          "canCommit called after coordinator was stopped (is SparkEnv shutdown in progress)?")
        false
    }
  }

  // Called by DAGScheduler
  private[scheduler] def stageStart(stage: StageId): Unit = synchronized {
    authorizedCommittersByStage(stage) = mutable.HashMap[TaskId, TaskAttemptId]()
  }

  // Called by DAGScheduler
  private[scheduler] def stageEnd(stage: StageId): Unit = synchronized {
    authorizedCommittersByStage.remove(stage)
  }

  // Called by DAGScheduler
  private[scheduler] def taskCompleted(
      stage: StageId,
      task: TaskId,
      attempt: TaskAttemptId,
      reason: TaskEndReason): Unit = synchronized {
    val authorizedCommitters = authorizedCommittersByStage.getOrElse(stage, {
      logDebug(s"Ignoring task completion for completed stage")
      return
    })
    reason match {
      case Success =>
      // The task output has been committed successfully
      case TaskCommitDenied(jobID, splitID, attemptID) =>
        logInfo(s"Task was denied committing, stage: $stage, taskId: $task, attempt: $attempt")
      case otherReason =>
        logDebug(s"Authorized committer $attempt (stage=$stage, task=$task) failed;" +
          s" clearing lock")
        authorizedCommitters.remove(task)
    }
  }

  def stop(): Unit = synchronized {
    coordinatorActor.foreach(_ ! StopCoordinator)
    coordinatorActor = None
    authorizedCommittersByStage.foreach(_._2.clear)
    authorizedCommittersByStage.clear
  }

  private def handleAskPermissionToCommit(
      stage: StageId,
      task: TaskId,
      attempt: TaskAttemptId): Boolean = synchronized {
    authorizedCommittersByStage.get(stage) match {
      case Some(authorizedCommitters) =>
        authorizedCommitters.get(task) match {
          case Some(existingCommitter) =>
            logDebug(s"Denying $attempt to commit for stage=$stage, task=$task; " +
              s"existingCommitter = $existingCommitter")
            false
          case None =>
            logDebug(s"Authorizing $attempt to commit for stage=$stage, task=$task")
            authorizedCommitters(task) = attempt
            true
        }
      case None =>
        logDebug(s"Stage $stage has completed, so not allowing task attempt $attempt to commit")
        false
    }
  }
}

private[spark] object OutputCommitCoordinator {

  // This actor is used only for RPC
  class OutputCommitCoordinatorActor(outputCommitCoordinator: OutputCommitCoordinator)
    extends Actor with ActorLogReceive with Logging {

    override def receiveWithLogging = {
      case AskPermissionToCommitOutput(stage, task, taskAttempt) =>
        sender ! outputCommitCoordinator.handleAskPermissionToCommit(stage, task, taskAttempt)
      case StopCoordinator =>
        logInfo("OutputCommitCoordinator stopped!")
        context.stop(self)
        sender ! true
    }
  }
}
