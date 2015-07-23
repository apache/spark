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

package org.apache.spark.ui.scope

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.SparkListenerStageSubmitted
import org.apache.spark.scheduler.SparkListenerStageCompleted
import org.apache.spark.scheduler.SparkListenerJobStart

/**
 * Tests that this listener populates and cleans up its data structures properly.
 */
class RDDOperationGraphListenerSuite extends SparkFunSuite {
  private var jobIdCounter = 0
  private var stageIdCounter = 0
  private val maxRetainedJobs = 10
  private val maxRetainedStages = 10
  private val conf = new SparkConf()
    .set("spark.ui.retainedJobs", maxRetainedJobs.toString)
    .set("spark.ui.retainedStages", maxRetainedStages.toString)

  test("run normal jobs") {
    val startingJobId = jobIdCounter
    val startingStageId = stageIdCounter
    val listener = new RDDOperationGraphListener(conf)
    assert(listener.jobIdToStageIds.isEmpty)
    assert(listener.jobIdToSkippedStageIds.isEmpty)
    assert(listener.stageIdToJobId.isEmpty)
    assert(listener.stageIdToGraph.isEmpty)
    assert(listener.completedStageIds.isEmpty)
    assert(listener.jobIds.isEmpty)
    assert(listener.stageIds.isEmpty)

    // Run a few jobs, but not enough for clean up yet
    (1 to 3).foreach { numStages => startJob(numStages, listener) } // start 3 jobs and 6 stages
    (0 to 5).foreach { i => endStage(startingStageId + i, listener) } // finish all 6 stages
    (0 to 2).foreach { i => endJob(startingJobId + i, listener) } // finish all 3 jobs

    assert(listener.jobIdToStageIds.size === 3)
    assert(listener.jobIdToStageIds(startingJobId).size === 1)
    assert(listener.jobIdToStageIds(startingJobId + 1).size === 2)
    assert(listener.jobIdToStageIds(startingJobId + 2).size === 3)
    assert(listener.jobIdToSkippedStageIds.size === 3)
    assert(listener.jobIdToSkippedStageIds.values.forall(_.isEmpty)) // no skipped stages
    assert(listener.stageIdToJobId.size === 6)
    assert(listener.stageIdToJobId(startingStageId) === startingJobId)
    assert(listener.stageIdToJobId(startingStageId + 1) === startingJobId + 1)
    assert(listener.stageIdToJobId(startingStageId + 2) === startingJobId + 1)
    assert(listener.stageIdToJobId(startingStageId + 3) === startingJobId + 2)
    assert(listener.stageIdToJobId(startingStageId + 4) === startingJobId + 2)
    assert(listener.stageIdToJobId(startingStageId + 5) === startingJobId + 2)
    assert(listener.stageIdToGraph.size === 6)
    assert(listener.completedStageIds.size === 6)
    assert(listener.jobIds.size === 3)
    assert(listener.stageIds.size === 6)
  }

  test("run jobs with skipped stages") {
    val startingJobId = jobIdCounter
    val startingStageId = stageIdCounter
    val listener = new RDDOperationGraphListener(conf)

    // Run a few jobs, but not enough for clean up yet
    // Leave some stages unfinished so that they are marked as skipped
    (1 to 3).foreach { numStages => startJob(numStages, listener) } // start 3 jobs and 6 stages
    (4 to 5).foreach { i => endStage(startingStageId + i, listener) } // finish only last 2 stages
    (0 to 2).foreach { i => endJob(startingJobId + i, listener) } // finish all 3 jobs

    assert(listener.jobIdToSkippedStageIds.size === 3)
    assert(listener.jobIdToSkippedStageIds(startingJobId).size === 1)
    assert(listener.jobIdToSkippedStageIds(startingJobId + 1).size === 2)
    assert(listener.jobIdToSkippedStageIds(startingJobId + 2).size === 1) // 2 stages not skipped
    assert(listener.completedStageIds.size === 2)

    // The rest should be the same as before
    assert(listener.jobIdToStageIds.size === 3)
    assert(listener.jobIdToStageIds(startingJobId).size === 1)
    assert(listener.jobIdToStageIds(startingJobId + 1).size === 2)
    assert(listener.jobIdToStageIds(startingJobId + 2).size === 3)
    assert(listener.stageIdToJobId.size === 6)
    assert(listener.stageIdToJobId(startingStageId) === startingJobId)
    assert(listener.stageIdToJobId(startingStageId + 1) === startingJobId + 1)
    assert(listener.stageIdToJobId(startingStageId + 2) === startingJobId + 1)
    assert(listener.stageIdToJobId(startingStageId + 3) === startingJobId + 2)
    assert(listener.stageIdToJobId(startingStageId + 4) === startingJobId + 2)
    assert(listener.stageIdToJobId(startingStageId + 5) === startingJobId + 2)
    assert(listener.stageIdToGraph.size === 6)
    assert(listener.jobIds.size === 3)
    assert(listener.stageIds.size === 6)
  }

  test("clean up metadata") {
    val startingJobId = jobIdCounter
    val startingStageId = stageIdCounter
    val listener = new RDDOperationGraphListener(conf)

    // Run many jobs and stages to trigger clean up
    (1 to 10000).foreach { i =>
      // Note: this must be less than `maxRetainedStages`
      val numStages = i % (maxRetainedStages - 2) + 1
      val startingStageIdForJob = stageIdCounter
      val jobId = startJob(numStages, listener)
      // End some, but not all, stages that belong to this job
      // This is to ensure that we have both completed and skipped stages
      (startingStageIdForJob until stageIdCounter)
        .filter { i => i % 2 == 0 }
        .foreach { i => endStage(i, listener) }
      // End all jobs
      endJob(jobId, listener)
    }

    // Ensure we never exceed the max retained thresholds
    assert(listener.jobIdToStageIds.size <= maxRetainedJobs)
    assert(listener.jobIdToSkippedStageIds.size <= maxRetainedJobs)
    assert(listener.stageIdToJobId.size <= maxRetainedStages)
    assert(listener.stageIdToGraph.size <= maxRetainedStages)
    assert(listener.completedStageIds.size <= maxRetainedStages)
    assert(listener.jobIds.size <= maxRetainedJobs)
    assert(listener.stageIds.size <= maxRetainedStages)

    // Also ensure we're actually populating these data structures
    // Otherwise the previous group of asserts will be meaningless
    assert(listener.jobIdToStageIds.nonEmpty)
    assert(listener.jobIdToSkippedStageIds.nonEmpty)
    assert(listener.stageIdToJobId.nonEmpty)
    assert(listener.stageIdToGraph.nonEmpty)
    assert(listener.completedStageIds.nonEmpty)
    assert(listener.jobIds.nonEmpty)
    assert(listener.stageIds.nonEmpty)

    // Ensure we clean up old jobs and stages, not arbitrary ones
    assert(!listener.jobIdToStageIds.contains(startingJobId))
    assert(!listener.jobIdToSkippedStageIds.contains(startingJobId))
    assert(!listener.stageIdToJobId.contains(startingStageId))
    assert(!listener.stageIdToGraph.contains(startingStageId))
    assert(!listener.completedStageIds.contains(startingStageId))
    assert(!listener.stageIds.contains(startingStageId))
    assert(!listener.jobIds.contains(startingJobId))
  }

  test("fate sharing between jobs and stages") {
    val startingJobId = jobIdCounter
    val startingStageId = stageIdCounter
    val listener = new RDDOperationGraphListener(conf)

    // Run 3 jobs and 8 stages, finishing all 3 jobs but only 2 stages
    startJob(5, listener)
    startJob(1, listener)
    startJob(2, listener)
    (0 until 8).foreach { i => startStage(i + startingStageId, listener) }
    endStage(startingStageId + 3, listener)
    endStage(startingStageId + 4, listener)
    (0 until 3).foreach { i => endJob(i + startingJobId, listener) }

    // First, assert the old stuff
    assert(listener.jobIdToStageIds.size === 3)
    assert(listener.jobIdToSkippedStageIds.size === 3)
    assert(listener.stageIdToJobId.size === 8)
    assert(listener.stageIdToGraph.size === 8)
    assert(listener.completedStageIds.size === 2)

    // Cleaning the third job should clean all of its stages
    listener.cleanJob(startingJobId + 2)
    assert(listener.jobIdToStageIds.size === 2)
    assert(listener.jobIdToSkippedStageIds.size === 2)
    assert(listener.stageIdToJobId.size === 6)
    assert(listener.stageIdToGraph.size === 6)
    assert(listener.completedStageIds.size === 2)

    // Cleaning one of the stages in the first job should clean that job and all of its stages
    // Note that we still keep around the last stage because it belongs to a different job
    listener.cleanStage(startingStageId)
    assert(listener.jobIdToStageIds.size === 1)
    assert(listener.jobIdToSkippedStageIds.size === 1)
    assert(listener.stageIdToJobId.size === 1)
    assert(listener.stageIdToGraph.size === 1)
    assert(listener.completedStageIds.size === 0)
  }

  /** Start a job with the specified number of stages. */
  private def startJob(numStages: Int, listener: RDDOperationGraphListener): Int = {
    assert(numStages > 0, "I will not run a job with 0 stages for you.")
    val stageInfos = (0 until numStages).map { _ =>
      val stageInfo = new StageInfo(stageIdCounter, 0, "s", 0, Seq.empty, Seq.empty, "d")
      stageIdCounter += 1
      stageInfo
    }
    val jobId = jobIdCounter
    listener.onJobStart(new SparkListenerJobStart(jobId, 0, stageInfos))
    // Also start all stages that belong to this job
    stageInfos.map(_.stageId).foreach { sid => startStage(sid, listener) }
    jobIdCounter += 1
    jobId
  }

  /** Start the stage specified by the given ID. */
  private def startStage(stageId: Int, listener: RDDOperationGraphListener): Unit = {
    val stageInfo = new StageInfo(stageId, 0, "s", 0, Seq.empty, Seq.empty, "d")
    listener.onStageSubmitted(new SparkListenerStageSubmitted(stageInfo))
  }

  /** Finish the stage specified by the given ID. */
  private def endStage(stageId: Int, listener: RDDOperationGraphListener): Unit = {
    val stageInfo = new StageInfo(stageId, 0, "s", 0, Seq.empty, Seq.empty, "d")
    listener.onStageCompleted(new SparkListenerStageCompleted(stageInfo))
  }

  /** Finish the job specified by the given ID. */
  private def endJob(jobId: Int, listener: RDDOperationGraphListener): Unit = {
    listener.onJobEnd(new SparkListenerJobEnd(jobId, 0, JobSucceeded))
  }

}
