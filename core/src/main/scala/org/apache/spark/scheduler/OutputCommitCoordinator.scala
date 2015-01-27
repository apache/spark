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

import java.util.concurrent.{ExecutorService, TimeUnit, ConcurrentHashMap}

import scala.collection.{Map => ScalaImmutableMap}
import scala.collection.convert.decorateAsScala._

import akka.actor.{ActorRef, Actor}

import org.apache.spark.{SparkConf, Logging}
import org.apache.spark.util.{Utils, AkkaUtils, ActorLogReceive}

private[spark] sealed trait OutputCommitCoordinationMessage

private[spark] case class StageStarted(stage: Int, partitionIds: Seq[Int])
  extends OutputCommitCoordinationMessage
private[spark] case class StageEnded(stage: Int) extends OutputCommitCoordinationMessage
private[spark] case object StopCoordinator extends OutputCommitCoordinationMessage

private[spark] case class AskPermissionToCommitOutput(
    stage: Int,
    task: Long,
    partId: Int,
    taskAttempt: Long)
  extends OutputCommitCoordinationMessage with Serializable

private[spark] case class TaskCompleted(
    stage: Int,
    task: Long,
    partId: Int,
    attempt: Long,
    successful: Boolean)
  extends OutputCommitCoordinationMessage

/**
 * Authority that decides whether tasks can commit output to HDFS.
 *
 * This lives on the driver, but the actor allows the tasks that commit
 * to Hadoop to invoke it.
 */
private[spark] class OutputCommitCoordinator(conf: SparkConf) extends Logging {

  private type StageId = Int
  private type PartitionId = Int
  private type TaskId = Long
  private type TaskAttemptId = Long

  // Wrapper for an int option that allows it to be locked via a synchronized block
  // while still setting option itself to Some(...) or None.
  private class LockableAttemptId(var value: Option[TaskAttemptId])

  private type CommittersByStageHashMap =
    ConcurrentHashMap[StageId, ScalaImmutableMap[PartitionId, LockableAttemptId]]

  // Initialized by SparkEnv
  private var coordinatorActor: Option[ActorRef] = None
  private val timeout = AkkaUtils.askTimeout(conf)
  private val maxAttempts = AkkaUtils.numRetries(conf)
  private val retryInterval = AkkaUtils.retryWaitMs(conf)
  private val authorizedCommittersByStage = new CommittersByStageHashMap().asScala

  private var executorRequestHandlingThreadPool: Option[ExecutorService] = None

  def stageStart(stage: StageId, partitionIds: Seq[Int]): Unit = {
    sendToActor(StageStarted(stage, partitionIds))
  }

  def stageEnd(stage: StageId): Unit = {
    sendToActor(StageEnded(stage))
  }

  def canCommit(
      stage: StageId,
      task: TaskId,
      partId: PartitionId,
      attempt: TaskAttemptId): Boolean = {
    askActor(AskPermissionToCommitOutput(stage, task, partId, attempt))
  }

  def taskCompleted(
      stage: StageId,
      task: TaskId,
      partId: PartitionId,
      attempt: TaskAttemptId,
      successful: Boolean): Unit = {
    sendToActor(TaskCompleted(stage, task, partId, attempt, successful))
  }

  def stop(): Unit = {
    executorRequestHandlingThreadPool.foreach { pool =>
      pool.shutdownNow()
      pool.awaitTermination(10, TimeUnit.SECONDS)
    }
    sendToActor(StopCoordinator)
    coordinatorActor = None
    executorRequestHandlingThreadPool = None
    authorizedCommittersByStage.clear
  }

  def initialize(actor: ActorRef, isDriver: Boolean): Unit = {
    coordinatorActor = Some(actor)
    executorRequestHandlingThreadPool = {
      if (isDriver) {
        Some(Utils.newDaemonFixedThreadPool(8, "OutputCommitCoordinator"))
      } else {
        None
      }
    }
  }

  // Methods that mutate the internal state of the coordinator shouldn't be
  // called directly, and are thus made private instead of public. The
  // private methods should be called from the Actor, and callers use the
  // public methods to send messages to the actor.
  private def handleStageStart(stage: StageId, partitionIds: Seq[Int]): Unit = {
    val initialLockStates = partitionIds.map(partId => {
      partId -> new LockableAttemptId(None)
    }).toMap
    authorizedCommittersByStage.put(stage, initialLockStates)
  }

  private def handleStageEnd(stage: StageId): Unit = {
    authorizedCommittersByStage.remove(stage)
  }

  private def determineIfCommitAllowed(
      stage: StageId,
      task: TaskId,
      partId: PartitionId,
      attempt: TaskAttemptId): Boolean = {
    authorizedCommittersByStage.get(stage) match {
      case Some(authorizedCommitters) =>
        val authorizedCommitMetadataForPart = authorizedCommitters(partId)
        authorizedCommitMetadataForPart.synchronized {
          // Don't use match - we'll be setting the value of the option in the else block
          if (authorizedCommitMetadataForPart.value.isDefined) {
            val existingCommitter = authorizedCommitMetadataForPart.value.get
            logDebug(s"Denying $attempt to commit for stage=$stage, task=$task; " +
              s"existingCommitter = $existingCommitter")
            false
          } else {
            logDebug(s"Authorizing $attempt to commit for stage=$stage, task=$task")
            authorizedCommitMetadataForPart.value = Some(attempt)
            true
          }
        }
      case None =>
        logDebug(s"Stage $stage has completed, so not allowing task attempt $attempt to commit")
        false
    }
  }

  private def handleAskPermissionToCommitOutput(
      requester: ActorRef,
      stage: StageId,
      task: TaskId,
      partId: PartitionId,
      attempt: TaskAttemptId): Unit = {
    executorRequestHandlingThreadPool match {
      case Some(threadPool) =>
        threadPool.submit(new AskCommitRunnable(requester, this, stage, task, partId, attempt))
      case None =>
        logWarning("Got a request to commit output, but the OutputCommitCoordinator was already" +
          " shut down. Request is being denied.")
        requester ! false
    }

  }

  private def handleTaskCompletion(
      stage: StageId,
      task: TaskId,
      partId: PartitionId,
      attempt: TaskAttemptId,
      successful: Boolean): Unit = {
    authorizedCommittersByStage.get(stage) match {
      case Some(authorizedCommitters) =>
        val authorizedCommitMetadataForPart = authorizedCommitters(partId)
        authorizedCommitMetadataForPart.synchronized {
          if (authorizedCommitMetadataForPart.value == Some(attempt) && !successful) {
            logDebug(s"Authorized committer $attempt (stage=$stage," +
              s" task=$task) failed; clearing lock")
            // The authorized committer failed; clear the lock so future attempts can
            // commit their output
            authorizedCommitMetadataForPart.value = None
          }
        }
      case None =>
        logDebug(s"Ignoring task completion for completed stage")
    }
  }

  private def sendToActor(msg: OutputCommitCoordinationMessage): Unit = {
    coordinatorActor.foreach(_ ! msg)
  }

  private def askActor(msg: OutputCommitCoordinationMessage): Boolean = {
    coordinatorActor
      .map(AkkaUtils.askWithReply[Boolean](msg, _, maxAttempts, retryInterval, timeout))
      .getOrElse(false)
  }

  class AskCommitRunnable(
      private val requester: ActorRef,
      private val outputCommitCoordinator: OutputCommitCoordinator,
      private val stage: StageId,
      private val task: TaskId,
      private val partId: PartitionId,
      private val taskAttempt: TaskAttemptId)
  extends Runnable {
    override def run(): Unit = {
     requester ! outputCommitCoordinator.determineIfCommitAllowed(stage, task, partId, taskAttempt)
    }
  }
}

private[spark] object OutputCommitCoordinator {

  // Actor is defined inside the OutputCommitCoordinator object so that receiveWithLogging()
  // can call the private methods, where it is safe to do so because it is in the actor event
  // loop.
  class OutputCommitCoordinatorActor(outputCommitCoordinator: OutputCommitCoordinator)
    extends Actor with ActorLogReceive with Logging {

    override def receiveWithLogging() = {
      case StageStarted(stage, partitionIds) =>
        outputCommitCoordinator.handleStageStart(stage, partitionIds)
      case StageEnded(stage) =>
        outputCommitCoordinator.handleStageEnd(stage)
      case AskPermissionToCommitOutput(stage, task, partId, taskAttempt) =>
        outputCommitCoordinator.handleAskPermissionToCommitOutput(
          sender, stage, task, partId, taskAttempt)
      case TaskCompleted(stage, task, partId, attempt, successful) =>
        outputCommitCoordinator.handleTaskCompletion(stage, task, partId, attempt, successful)
      case StopCoordinator =>
        logInfo("OutputCommitCoordinator stopped!")
        context.stop(self)
        sender ! true
    }
  }
}
