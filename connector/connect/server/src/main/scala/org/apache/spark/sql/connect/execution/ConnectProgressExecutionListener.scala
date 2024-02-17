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

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerStageCompleted, SparkListenerTaskEnd}

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
  class ExecutionTracker(var tag: String) {
    private[ConnectProgressExecutionListener] var jobs: Set[Int] = Set()
    private[ConnectProgressExecutionListener] var stages: Set[Int] = Set()
    private[ConnectProgressExecutionListener] var totalTasks = 0
    private[ConnectProgressExecutionListener] var completedTasks = 0
    private[ConnectProgressExecutionListener] var completedStages = 0
    private[ConnectProgressExecutionListener] var inputBytesRead = 0L
    // The tracker is marked as dirty if it has new progress to report. This variable does
    // not need to be protected by a mutex even if multiple threads would read the same dirty
    // state the output is expected to be identical.
    @volatile private[ConnectProgressExecutionListener] var dirty = false

    /**
     * Yield the current state of the tracker if it is dirty. A consumer of the tracker can
     * provide a callback that will be called with the current state of the tracker if the tracker
     * has new progress to report.
     *
     * If the tracker was marked as dirty, the state is reset after.
     */
    def yieldWhenDirty(thunk: (Int, Int, Int, Int, Long) => Unit): Unit = {
      if (dirty) {
        thunk(totalTasks, completedTasks, stages.size, completedStages, inputBytesRead)
        dirty = false
      }
    }

    /**
     * Add a job to the tracker. This will add the job to the list of jobs that are being tracked
     */
    def addJob(job: SparkListenerJobStart): Unit = {
      jobs = jobs + job.jobId
      stages = stages ++ job.stageIds
      totalTasks += job.stageInfos.map(_.numTasks).sum
      dirty = true
    }

    def jobCount(): Int = {
      jobs.size
    }

    def stageCount(): Int = {
      stages.size
    }
  }

  val trackedTags = collection.mutable.Map[String, ExecutionTracker]()

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val tags = jobStart.properties.getProperty("spark.job.tags")
    if (tags != null) {
      val thisJobTags = tags.split(",").map(_.trim).toSet
      thisJobTags.foreach { tag =>
        if (trackedTags.contains(tag)) {
          trackedTags(tag).addJob(jobStart)
        }
      }
    }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    // Check if the task belongs to a job that we are tracking.
    trackedTags.foreach({ case (tag, tracker) =>
      if (tracker.stages.contains(taskEnd.stageId)) {
        tracker.completedTasks += 1
        tracker.inputBytesRead += taskEnd.taskMetrics.inputMetrics.bytesRead
        tracker.dirty = true
      }
    })
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    trackedTags.foreach({ case (tag, tracker) =>
      if (tracker.stages.contains(stageCompleted.stageInfo.stageId)) {
        tracker.completedStages += 1
        tracker.dirty = true
      }
    })
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    trackedTags.foreach({ case (tag, tracker) =>
      if (tracker.jobs.contains(jobEnd.jobId)) {
        tracker.jobs -= jobEnd.jobId
        tracker.dirty = true
      }
    })
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
