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

package org.apache.spark.sql.connect.execution

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import org.apache.spark.connect.proto.ExecutePlanResponse
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerStageCompleted, SparkListenerTaskEnd, SparkListenerTaskStart}

/**
 * A listener that tracks the execution of jobs and stages for a given set of tags. This is used
 * to track the progress of a job that is being executed through the connect API.
 *
 * The listener is instantiated once for the SparkConnectService and then used to track all the
 * current query executions.
 */
private[connect] class ConnectProgressExecutionListener extends SparkListener with Logging {

  /**
   * A tracker for a given tag. This is used to track the progress of an operation is being
   * executed through the connect API.
   */
  class ExecutionTracker(val tag: String) {

    class StageInfo(
        val stageId: Int,
        var numTasks: Int,
        var completedTasks: Int = 0,
        var inputBytesRead: Long = 0,
        var completed: Boolean = false) {

      val lock = new Object
      def update(i: StageInfo => Unit): Unit = {
        lock.synchronized {
          i(this)
        }
      }

      def toProto(): ExecutePlanResponse.ExecutionProgress.StageInfo = {
        ExecutePlanResponse.ExecutionProgress.StageInfo
          .newBuilder()
          .setStageId(stageId)
          .setNumTasks(numTasks)
          .setNumCompletedTasks(completedTasks)
          .setInputBytesRead(inputBytesRead)
          .setDone(completed)
          .build()
      }
    }

    // The set of jobs that are being tracked by this tracker. We always only add to this list
    // but never remove. This is to avoid concurrency issues.
    private[ConnectProgressExecutionListener] var jobs: Set[Int] = Set()
    // The set of stages that are being tracked by this tracker. We always only add to this list
    // but never remove. This is to avoid concurrency issues.
    private[ConnectProgressExecutionListener] var stages: Map[Int, StageInfo] = Map.empty
    // The tracker is marked as dirty if it has new progress to report.
    private[ConnectProgressExecutionListener] val dirty = new AtomicBoolean(false)
    // Tracks all currently running tasks for a particular tracker.
    private[ConnectProgressExecutionListener] val inFlightTasks = new AtomicInteger(0)

    /**
     * Yield the current state of the tracker if it is dirty. A consumer of the tracker can
     * provide a callback that will be called with the current state of the tracker if the tracker
     * has new progress to report.
     *
     * If the tracker was marked as dirty, the state is reset after.
     */
    def yieldWhenDirty(force: Boolean = false)(thunk: (Seq[StageInfo], Long) => Unit): Unit = {
      if (force) {
        thunk(stages.values.toSeq, inFlightTasks.get())
      } else {
        if (dirty.get()) {
          thunk(stages.values.toSeq, inFlightTasks.get())
          dirty.set(false)
        }
      }
    }

    /**
     * Add a job to the tracker. This will add the job to the list of jobs that are being tracked
     */
    def addJob(job: SparkListenerJobStart): Unit = synchronized {
      jobs = jobs + job.jobId
      job.stageInfos.foreach { stage =>
        stages = stages + (stage.stageId -> new StageInfo(stage.stageId, stage.numTasks))
      }
      dirty.set(true)
    }

    def jobCount(): Int = {
      jobs.size
    }

    def stageCount(): Int = {
      stages.size
    }
  }

  val trackedTags = collection.concurrent.TrieMap[String, ExecutionTracker]()

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val tags = jobStart.properties.getProperty("spark.job.tags")
    if (tags != null) {
      val thisJobTags = tags.split(",").map(_.trim).toSet
      thisJobTags.foreach { tag =>
        trackedTags.get(tag).foreach { tracker =>
          tracker.addJob(jobStart)
        }
      }
    }
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    // Check if the task belongs to a job that we are tracking.
    trackedTags.foreach({ case (_, tracker) =>
      if (tracker.stages.contains(taskStart.stageId)) {
        tracker.inFlightTasks.incrementAndGet()
        tracker.dirty.set(true)
      }
    })
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    // Check if the task belongs to a job that we are tracking.
    trackedTags.foreach({ case (_, tracker) =>
      if (tracker.stages.contains(taskEnd.stageId)) {
        tracker.stages.get(taskEnd.stageId).foreach { stage =>
          stage.update { i =>
            i.completedTasks += 1
            i.inputBytesRead += Option(taskEnd.taskMetrics)
              .map(_.inputMetrics.bytesRead)
              .getOrElse(0L)
          }
        }
        // This should never become negative, simply reset to zero if it does.
        tracker.inFlightTasks.decrementAndGet()
        if (tracker.inFlightTasks.get() < 0) {
          tracker.inFlightTasks.set(0)
        }
        tracker.dirty.set(true)
      }
    })
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    trackedTags.foreach({ case (_, tracker) =>
      if (tracker.stages.contains(stageCompleted.stageInfo.stageId)) {
        tracker.stages(stageCompleted.stageInfo.stageId).update { stage =>
          stage.completed = true
        }
        tracker.dirty.set(true)
      }
    })
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    trackedTags.foreach({ case (_, tracker) =>
      if (tracker.jobs.contains(jobEnd.jobId)) {
        tracker.dirty.set(true)
      }
    })
  }

  def tryGetTracker(tag: String): Option[ExecutionTracker] = {
    trackedTags.get(tag)
  }

  def registerJobTag(tag: String): Unit = {
    trackedTags += tag -> new ExecutionTracker(tag)
  }

  def removeJobTag(tag: String): Unit = {
    trackedTags -= tag
  }

  def clearJobTags(): Unit = {
    trackedTags.clear()
  }

}
