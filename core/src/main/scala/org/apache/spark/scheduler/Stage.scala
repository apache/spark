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

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.CallSite

/**
 * A stage is a set of independent tasks all computing the same function that need to run as part
 * of a Spark job, where all the tasks have the same shuffle dependencies. Each DAG of tasks run
 * by the scheduler is split up into stages at the boundaries where shuffle occurs, and then the
 * DAGScheduler runs these stages in topological order.
 *
 * Each Stage can either be a shuffle map stage, in which case its tasks' results are input for
 * another stage, or a result stage, in which case its tasks directly compute the action that
 * initiated a job (e.g. count(), save(), etc). For shuffle map stages, we also track the nodes
 * that each output partition is on.
 *
 * Each Stage also has a jobId, identifying the job that first submitted the stage.  When FIFO
 * scheduling is used, this allows Stages from earlier jobs to be computed first or recovered
 * faster on failure.
 *
 * The callSite provides a location in user code which relates to the stage. For a shuffle map
 * stage, the callSite gives the user code that created the RDD being shuffled. For a result
 * stage, the callSite gives the user code that executes the associated action (e.g. count()).
 *
 */
private[spark] class Stage(
    val id: Int,
    val rdd: RDD[_],
    val numTasks: Int,
    val shuffleDep: Option[ShuffleDependency[_, _, _]],  // Output shuffle if stage is a map stage
    val parents: List[Stage],
    val jobId: Int,
    val callSite: CallSite)
  extends Logging {

  val isShuffleMap = shuffleDep.isDefined
  val numPartitions = rdd.partitions.size
  val outputLocs = Array.fill[List[MapStatus]](numPartitions)(Nil)
  var numAvailableOutputs = 0

  /** Set of jobs that this stage belongs to. */
  val jobIds = new HashSet[Int]

  /** For stages that are the final (consists of only ResultTasks), link to the ActiveJob. */
  var resultOfJob: Option[ActiveJob] = None
  var pendingTasks = new HashSet[Task[_]]

  private var nextAttemptId = 0

  val name = callSite.shortForm
  val details = callSite.longForm

  /** Pointer to the [StageInfo] object, set by DAGScheduler. */
  var info: StageInfo = StageInfo.fromStage(this)

  def isAvailable: Boolean = {
    if (!isShuffleMap) {
      true
    } else {
      numAvailableOutputs == numPartitions
    }
  }

  def addOutputLoc(partition: Int, status: MapStatus) {
    val prevList = outputLocs(partition)
    outputLocs(partition) = status :: prevList
    if (prevList == Nil) {
      numAvailableOutputs += 1
    }
  }

  def removeOutputLoc(partition: Int, bmAddress: BlockManagerId) {
    val prevList = outputLocs(partition)
    val newList = prevList.filterNot(_.location == bmAddress)
    outputLocs(partition) = newList
    if (prevList != Nil && newList == Nil) {
      numAvailableOutputs -= 1
    }
  }

  def removeOutputsOnExecutor(execId: String) {
    var becameUnavailable = false
    for (partition <- 0 until numPartitions) {
      val prevList = outputLocs(partition)
      val newList = prevList.filterNot(_.location.executorId == execId)
      outputLocs(partition) = newList
      if (prevList != Nil && newList == Nil) {
        becameUnavailable = true
        numAvailableOutputs -= 1
      }
    }
    if (becameUnavailable) {
      logInfo("%s is now unavailable on executor %s (%d/%d, %s)".format(
        this, execId, numAvailableOutputs, numPartitions, isAvailable))
    }
  }

  def newAttemptId(): Int = {
    val id = nextAttemptId
    nextAttemptId += 1
    id
  }

  def attemptId: Int = nextAttemptId

  override def toString = "Stage " + id

  override def hashCode(): Int = id
}
