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

import java.util.Properties

import scala.collection.mutable

import org.apache.spark.{MapOutputTrackerMaster, SparkContext, SparkEnv, SparkException, SparkRuntimeException, Success}
import org.apache.spark.internal.LogKeys
import org.apache.spark.internal.config.{SPECULATION_ENABLED, STREAMING_REALTIME_MODE_SLOTS_CHECK_DISABLED}
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.storage.BlockManagerMaster
import org.apache.spark.util.Clock
import org.apache.spark.util.SystemClock

/**
 *  A [[DAGScheduler]] that runs all the stages in a job without waiting for its parents
 *  complete. This combined with streaming shuffle between the stages, allows for low latency
 *  execution of streaming queries in real-time mode.
 */
class ConcurrentStageDAGScheduler(
    sc: SparkContext,
    taskScheduler: TaskScheduler,
    listenerBus: LiveListenerBus,
    mapOutputTracker: MapOutputTrackerMaster,
    blockManagerMaster: BlockManagerMaster,
    env: SparkEnv,
    clock: Clock = new SystemClock())
  extends DAGScheduler(
    sc, taskScheduler, listenerBus, mapOutputTracker, blockManagerMaster, env, clock) {

  import ConcurrentStageDAGScheduler._

  def this(sc: SparkContext, taskScheduler: TaskScheduler) = {
    this(
      sc,
      taskScheduler,
      sc.listenerBus,
      sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster],
      sc.env.blockManager.master,
      sc.env
    )
  }

  def this(sc: SparkContext) = this(sc, sc.taskScheduler)

  // This contains all the concurrent stages that are yet to be scheduled across all the jobs.
  private[spark] val concurrentStages = new mutable.HashSet[Stage]

  private[scheduler] case class DependentStageInfo(
    parents: mutable.HashSet[Stage] = mutable.HashSet.empty,
    delayedTaskCompletionEvents: mutable.ListBuffer[CompletionEvent] = mutable.ListBuffer.empty)

  // This map holds parents of concurrently scheduled stages. When tasks for such a stage complete,
  // and if any of the parents are still running, we delay processing of such events until parent
  // stages are complete. We save these events in this map until then.
  private[spark] val dependentStageMap = new mutable.HashMap[Stage, DependentStageInfo]

  private def totalNumCoreForStage(stage: Stage): Int = {
    val numTask = stage match {
      case r: ResultStage => r.partitions.length
      case m: ShuffleMapStage => m.numPartitions
    }
    val resourceProfile = sc.resourceProfileManager.resourceProfileFromId(stage.resourceProfileId)
    val taskCpus = ResourceProfile.getTaskCpusOrDefaultForProfile(resourceProfile, sc.conf)
    taskCpus * numTask
  }

  /**
   * Hook invoked after the final stage is created. Registers stages reachable from
   * the final stage as concurrent so they can be submitted in parallel.
   */
  override def onFinalStageCreated(finalStage: Stage, properties: Properties): Unit = {

    val queryBatchId = getStreamingBatchIdFromProperties(properties)

    if (queryBatchId.nonEmpty && isConcurrentStagesEnabled(properties)) {
      // Speculation is not supported with concurrent stages. Check both the per-job local
      // property (for jobs that override the cluster default via setLocalProperty) and the
      // SparkConf (the documented way to enable speculation cluster-wide).
      if (properties.getProperty(SPECULATION_ENABLED.key) == "true" ||
          sc.conf.get(SPECULATION_ENABLED)) {
        throw new SparkException(
          "Speculative execution is not supported with concurrent stages " +
          s"(streaming query: $queryBatchId). Please disable ${SPECULATION_ENABLED.key} config."
        )
      }

      logInfo(log"Concurrent stages is enabled for [query ${MDC(LogKeys.STREAMING_QUERY_ID,
        queryBatchId.get.queryId)} batch ${MDC(LogKeys.BATCH_ID, queryBatchId.get.batchId)}]")

      // Mark current stage and all its ancestors as concurrent.
      // Collect into a local set first so a slot-check failure below does not leak partial
      // state into concurrentStages.
      val visitedStages = new mutable.HashSet[Stage]
      var totalCoresNeeded = 0
      def visit(stage: Stage): Unit = {
        if (!visitedStages.contains(stage)) {
          logInfo(log"Marking stage '${MDC(LogKeys.STAGE, stage)}' concurrent for [query ${MDC(
            LogKeys.STREAMING_QUERY_ID, queryBatchId.get.queryId)} batch ${MDC(
            LogKeys.BATCH_ID, queryBatchId.get.batchId)}]")
          visitedStages += stage
          totalCoresNeeded += totalNumCoreForStage(stage)
          stage.parents.foreach(visit)
        }
      }
      visit(finalStage)

      if (!sc.conf.get(STREAMING_REALTIME_MODE_SLOTS_CHECK_DISABLED)) {
        try {
          val totalSlots = sc.schedulerBackend.defaultParallelism()
          val coresInUse = runningStages.toArray.map(totalNumCoreForStage(_)).sum
          if (totalSlots - coresInUse < totalCoresNeeded) {
            throw new SparkRuntimeException(
              errorClass = "CONCURRENT_SCHEDULER_INSUFFICIENT_SLOT",
              messageParameters = Map(
                "numSlots" -> (totalSlots - coresInUse).toString,
                "numTasks" -> totalCoresNeeded.toString))
          }
        } catch {
          case e: UnsupportedOperationException =>
            logWarning(log"${MDC(LogKeys.ERROR, e)}. Skipping slot check for RTM.")
        }
      }

      // Slot check passed (or was disabled) — commit the visited stages.
      concurrentStages ++= visitedStages
    } else {
      super.onFinalStageCreated(finalStage, properties)
    }
  }

  override def submitStage(stage: Stage): Unit = {
    super.submitStage(stage)

    if (!waitingStages.contains(stage) && concurrentStages.contains(stage)) {
      // The current stage is not registered in waitingStages, which means it has
      // no parents. This case we should remove it from concurrentStages since it is already
      // running.
      assert(runningStages.contains(stage), "stage should be running if not in waitingStages")
      logInfo(log"Removing stage ${MDC(LogKeys.STAGE, stage)} from concurrentStages")
      concurrentStages -= stage
    }

    // Find the stages that should be submitted concurrently with this stage.
    waitingStages.intersect(concurrentStages).foreach { stage =>
      logInfo(log"Submitting stage concurrently: ${MDC(LogKeys.STAGE, stage)}")
      concurrentStages -= stage // Don't submit this stage concurrently for subsequent attempts.
      stage.parents.foreach { parent =>
        if (isRunningStage(parent)) {
          logInfo(log"Updating dependent map for stage ${MDC(LogKeys.STAGE, stage)} with parent ${
            MDC(LogKeys.PARENT_STAGE, parent)}")
          dependentStageMap.getOrElseUpdate(stage, DependentStageInfo()).parents += parent
        }
      }
      // Remove stage and its parents from concurrentStages
      def removeFromConcurrentStages(stage: Stage): Unit = {
        if (concurrentStages.contains(stage)) {
          logInfo(log"Removing stage ${MDC(LogKeys.STAGE, stage)} from concurrentStages")
          concurrentStages -= stage
        }
        stage.parents.foreach { parent =>
          assert(!waitingStages.contains(parent), "Parent stage should not still be waiting")
          removeFromConcurrentStages(parent)
        }
      }
      removeFromConcurrentStages(stage)
      submitConcurrentStage(stage)
    }
  }

  /**
   * Submits a child stage even while its parents are still running. Distinct from
   * `submitStage` in that it bypasses the missing-parent check.
   */
  private def submitConcurrentStage(stage: Stage): Unit = {
    assert(waitingStages.contains(stage))
    activeJobForStage(stage) match {
      case Some(job) =>
        waitingStages -= stage
        submitMissingTasks(stage, job)
      case None => // Not expected.
        throw new IllegalStateException(s"No active job for stage $stage")
    }
  }

  // This is overridden to check if the task completion event should be delayed because a
  // parent stage still has running tasks. See comment for `dependentStageMap` for more details.
  override private[scheduler] def handleTaskCompletion(event: CompletionEvent): Unit = {
    val stageId = event.task.stageId
    val taskId = event.taskInfo.taskId

    getStage(stageId) match {
      case Some(stage) if event.reason == Success && dependentStageMap.contains(stage) =>
        val dependentStageInfo = dependentStageMap(stage)
        logInfo(log"Delaying completion event for task ${MDC(LogKeys.TASK_ID, taskId)} in stage ${
          MDC(LogKeys.STAGE, stage)}. Active parent(s): ${MDC(LogKeys.PARENT_STAGES,
          dependentStageInfo.parents.mkString(", "))}")
        dependentStageInfo.delayedTaskCompletionEvents += event

      case _ =>  // Otherwise handle the event as usual.
        super.handleTaskCompletion(event)
    }
  }

  // This is overridden to handle any delayed task completion events for dependent stages.
  override def markStageAsFinished(
    stage: Stage,
    errorMessage: Option[String] = None,
    willRetry: Boolean = false): Unit = {

    super.markStageAsFinished(stage, errorMessage, willRetry)

    // If this is a parent of a stage in dependentStageMap, remove it from parents.
    val dependentStages = dependentStageMap
      .filter(_._2.parents.contains(stage))
      .keys

    dependentStages.foreach { dependent =>
      if (errorMessage.isEmpty) {
        assert(
          isRunningStage(dependent),
          s"Parent stages $stage's dependent stage $dependent should be running")
      }
      logInfo(log"Removing parent stage ${MDC(LogKeys.PARENT_STAGE, stage)} from dependent map " +
        log"for stage ${MDC(LogKeys.STAGE, dependent)}")
      dependentStageMap(dependent).parents -= stage
      checkDependentStageTasks(dependent)
    }

    // Drop this stage's own entry from the map. On the success path
    // `checkDependentStageTasks` (invoked when the stage's last parent finishes) has already
    // removed the entry, so this is a no-op. On failure / cancellation / abort the entry —
    // and any buffered completion events — would otherwise leak for the lifetime of the
    // scheduler.
    //
    // `willRetry=true` paths (e.g. FetchFailed) also reach this cleanup. That is safe under
    // concurrent scheduling because stage retries are not supported here: TaskSchedulerImpl
    // pins `maxFailures=1` for concurrent TaskSets, and any failure restarts the streaming
    // query from its checkpoint rather than retrying tasks against an in-flight streaming
    // shuffle. With no retry to preserve state for, it's correct to drop the entry along
    // with any buffered events.
    dependentStageMap.remove(stage)
  }

  // Checks if the dependent stage's parents are all done. If all the parents are done,
  // enqueues any saved task completion event (if any).
  private def checkDependentStageTasks(stage: Stage): Unit = {
    val dependentStageInfo = dependentStageMap.getOrElse(
      stage, throw new RuntimeException(s"Stage $stage is not in dependentStageMap")
    )

    if (dependentStageInfo.parents.isEmpty) {
      val delayedEvents = dependentStageInfo.delayedTaskCompletionEvents
      logInfo(log"All the parents are done for ${MDC(LogKeys.STAGE, stage)}. Removing it from " +
        log"the map. It has ${MDC(LogKeys.NUM_EVENTS, delayedEvents.size.toLong)} " +
        log"task completion events")
      dependentStageMap -= stage
      delayedEvents.foreach { event =>
        logInfo(log"Posting delayed task ${MDC(LogKeys.TASK_ID, event.taskInfo.taskId)} " +
          log"completion event for stage ${MDC(LogKeys.STAGE, stage)}")
        eventProcessLoop.post(event)
      }
    }
  }
}

object ConcurrentStageDAGScheduler {

  val CONCURRENT_STAGES_ENABLED_PROPERTY: String = "streaming.concurrent.stages.enabled"

  def isConcurrentStagesEnabled(properties: Properties): Boolean = {
    properties != null &&
      properties.getProperty(CONCURRENT_STAGES_ENABLED_PROPERTY) == "true"
  }

  /**
   * Extracts the [[StreamingBatchId]] from the given properties if both the streaming
   * query id and batch id are present.
   */
  def getStreamingBatchIdFromProperties(properties: Properties): Option[StreamingBatchId] = {
    if (properties == null) {
      return None
    }

    val queryId = Option(properties.getProperty(
      StructuredStreamingIdAwareSchedulerLogging.QUERY_ID_KEY))
    val batchId = Option(properties.getProperty(
      StructuredStreamingIdAwareSchedulerLogging.BATCH_ID_KEY))
    if (queryId.nonEmpty && batchId.nonEmpty) {
      Some(StreamingBatchId(queryId.get, batchId.get.toLong))
    } else {
      None
    }
  }
}

/**
 * Case class to identify a batch in a streaming query.
 *
 * @param queryId - Streaming query id
 * @param batchId - Batch id for a micro batch in a streaming query
 */
case class StreamingBatchId(queryId: String, batchId: Long)
