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

import org.apache.spark.ShuffleDependency
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.BlockManagerId
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
    val shuffleDep: ShuffleDependency[_, _, _])
  extends Stage(id, rdd, numTasks, parents, firstJobId, callSite) {

  private[this] var _mapStageJobs: List[ActiveJob] = Nil

  private[this] var _numAvailableOutputs: Int = 0

  /**
   * List of [[MapStatus]] for each partition. The index of the array is the map partition id,
   * and each value in the array is the list of possible [[MapStatus]] for a partition
   * (a single task might run multiple times).
   */
  private[this] val outputLocs = Array.fill[List[MapStatus]](numPartitions)(Nil)

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
   * This should be kept consistent as `outputLocs.filter(!_.isEmpty).size`.
   */
  def numAvailableOutputs: Int = _numAvailableOutputs

  /**
   * Returns true if the map stage is ready, i.e. all partitions have shuffle outputs.
   * This should be the same as `outputLocs.contains(Nil)`.
   */
  def isAvailable: Boolean = _numAvailableOutputs == numPartitions

  /** Returns the sequence of partition ids that are missing (i.e. needs to be computed). */
  override def findMissingPartitions(): Seq[Int] = {
    val missing = (0 until numPartitions).filter(id => outputLocs(id).isEmpty)
    assert(missing.size == numPartitions - _numAvailableOutputs,
      s"${missing.size} missing, expected ${numPartitions - _numAvailableOutputs}")
    missing
  }

  def addOutputLoc(partition: Int, status: MapStatus): Unit = {
    val prevList = outputLocs(partition)
    outputLocs(partition) = status :: prevList
    if (prevList == Nil) {
      _numAvailableOutputs += 1
    }
  }

  def removeOutputLoc(partition: Int, bmAddress: BlockManagerId): Unit = {
    val prevList = outputLocs(partition)
    val newList = prevList.filterNot(_.location == bmAddress)
    outputLocs(partition) = newList
    if (prevList != Nil && newList == Nil) {
      _numAvailableOutputs -= 1
    }
  }

  /**
   * Returns an array of [[MapStatus]] (index by partition id). For each partition, the returned
   * value contains only one (i.e. the first) [[MapStatus]]. If there is no entry for the partition,
   * that position is filled with null.
   */
  def outputLocInMapOutputTrackerFormat(): Array[MapStatus] = {
    outputLocs.map(_.headOption.orNull)
  }

  /**
   * Removes all shuffle outputs associated with this executor. Note that this will also remove
   * outputs which are served by an external shuffle server (if one exists), as they are still
   * registered with this execId.
   */
  def removeOutputsOnExecutor(execId: String): Unit = {
    var becameUnavailable = false
    for (partition <- 0 until numPartitions) {
      val prevList = outputLocs(partition)
      val newList = prevList.filterNot(_.location.executorId == execId)
      outputLocs(partition) = newList
      if (prevList != Nil && newList == Nil) {
        becameUnavailable = true
        _numAvailableOutputs -= 1
      }
    }
    if (becameUnavailable) {
      logInfo("%s is now unavailable on executor %s (%d/%d, %s)".format(
        this, execId, _numAvailableOutputs, numPartitions, isAvailable))
    }
  }
}
