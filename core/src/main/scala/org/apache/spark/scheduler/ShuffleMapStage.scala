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

import scala.collection.mutable.HashSet

import org.apache.spark.{MapOutputTrackerMaster, PipelinedShuffleDependency, ShuffleDependency}
import org.apache.spark.rdd.{DeterministicLevel, RDD}
import org.apache.spark.util.CallSite

/**
 * ShuffleMapStages are intermediate stages in the execution DAG that produce data for a shuffle.
 * They occur right before each shuffle operation, and might contain multiple pipelined operations
 * before that (e.g. map and filter). When executed, they save map output files that can later be
 * fetched by reduce tasks. The `shuffleDep` field describes the shuffle each stage is part of,
 * and variables like `outputLocs` and `numAvailableOutputs` track how many map outputs are ready.
 *
 * ShuffleMapStages can also be submitted independently as jobs with DAGScheduler.submitMapStage.
 * For such stages, the ActiveJobs that submitted them are tracked in `mapStageJobs`. Note that
 * there can be multiple ActiveJobs trying to compute the same shuffle map stage.
 */
private[spark] class ShuffleMapStage(
    id: Int,
    rdd: RDD[_],
    numTasks: Int,
    parents: List[Stage],
    firstJobId: Int,
    callSite: CallSite,
    val shuffleDep: ShuffleDependency[_, _, _],
    mapOutputTrackerMaster: MapOutputTrackerMaster,
    resourceProfileId: Int)
  extends Stage(id, rdd, numTasks, parents, firstJobId, callSite, resourceProfileId) {

  private[this] var _mapStageJobs: List[ActiveJob] = Nil

  /**
   * Partitions that either haven't yet been computed, or that were computed on an executor
   * that has since been lost, so should be re-computed.  This variable is used by the
   * DAGScheduler to determine when a stage has completed. Task successes in both the active
   * attempt for the stage or in earlier attempts for this stage can cause partition ids to get
   * removed from pendingPartitions. As a result, this variable may be inconsistent with the pending
   * tasks in the TaskSetManager for the active attempt for the stage (the partitions stored here
   * will always be a subset of the partitions that the TaskSetManager thinks are pending).
   */
  val pendingPartitions = new HashSet[Int]

  /** Whether this stage produces a pipelined (incrementally-readable) shuffle. */
  val isPipelined: Boolean = shuffleDep.isInstanceOf[PipelinedShuffleDependency[_, _, _]]

  /**
   * Availability tracking for a pipelined shuffle. A pipelined shuffle is transient and is NOT
   * registered with the `MapOutputTracker` as a durable, addressable output (spec S4), so its
   * map-stage availability cannot be read from the tracker. Instead we track completed partitions
   * here, monotonically: a partition is added when its map task succeeds and is NEVER removed on
   * executor/host loss.
   *
   * This is the crux of avoiding the streaming-writer resubmit hang: if a pipelined shuffle's
   * availability were
   * read from the `MapOutputTracker`, losing an executor that held a completed (already-consumed)
   * pipelined output would strip it there, flip `isAvailable` to false, and make the DAGScheduler
   * resubmit the producer -- whose streaming writer then blocks forever waiting for termination
   * acks from reducers that already finished. Keeping availability local and monotonic means
   * executor loss never triggers such a resubmit; a genuine mid-group failure is handled
   * group-atomically instead (S6). Unused (and empty) for a non-pipelined stage.
   */
  private[this] val pipelinedCompletedPartitions = new HashSet[Int]

  /** Record a successful map task's partition as completed (pipelined stages only). */
  private[scheduler] def addPipelinedCompletedPartition(partitionId: Int): Unit = {
    pipelinedCompletedPartitions += partitionId
  }

  override def toString: String = "ShuffleMapStage " + id

  /**
   * Returns the list of active jobs,
   * i.e. map-stage jobs that were submitted to execute this stage independently (if any).
   */
  def mapStageJobs: Seq[ActiveJob] = _mapStageJobs

  /** Adds the job to the active job list. */
  def addActiveJob(job: ActiveJob): Unit = {
    _mapStageJobs = job :: _mapStageJobs
  }

  /** Removes the job from the active job list. */
  def removeActiveJob(job: ActiveJob): Unit = {
    _mapStageJobs = _mapStageJobs.filter(_ != job)
  }

  /**
   * Number of partitions that have shuffle outputs.
   * When this reaches [[numPartitions]], this map stage is ready.
   */
  def numAvailableOutputs: Int = {
    // A pipelined shuffle is not tracked in the MapOutputTracker (see
    // pipelinedCompletedPartitions); read its locally-tracked, monotonic completed set instead.
    if (isPipelined) pipelinedCompletedPartitions.size
    else mapOutputTrackerMaster.getNumAvailableOutputs(shuffleDep.shuffleId)
  }

  /**
   * Returns true if the map stage is ready, i.e. all partitions have shuffle outputs.
   */
  def isAvailable: Boolean = numAvailableOutputs == numPartitions

  /** Returns the sequence of partition ids that are missing (i.e. needs to be computed). */
  override def findMissingPartitions(): Seq[Int] = {
    if (isPipelined) {
      (0 until numPartitions).filterNot(pipelinedCompletedPartitions.contains)
    } else {
      mapOutputTrackerMaster
        .findMissingPartitions(shuffleDep.shuffleId)
        .getOrElse(0 until numPartitions)
    }
  }

  /**
   * Whether the stage is statically declared as indeterminate based on the RDD's
   * outputDeterministicLevel property. This is known at RDD creation time.
   */
  def isStaticallyIndeterminate: Boolean = {
    rdd.outputDeterministicLevel == DeterministicLevel.INDETERMINATE
  }

  /**
   * Whether the stage has been detected as indeterminate at runtime via checksum mismatch.
   * This means different stage attempts have produced different data for the same partition.
   */
  def isRuntimeIndeterminate: Boolean = {
    !rdd.isReliablyCheckpointed && isChecksumMismatched
  }
}
