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
import scala.collection.mutable.HashSet

import org.apache.spark._
import org.apache.spark.rdd.RDD
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
 * A single stage can consist of multiple attempts. In that case, the latestInfo field will
 * be updated for each attempt.
 *
 */
private[spark] abstract class Stage(
    val id: Int,
    val rdd: RDD[_],
    val numTasks: Int,
    val parents: List[Stage],
    val jobId: Int,
    val callSite: CallSite)
  extends Logging {

  val numPartitions = rdd.partitions.size

  /** Set of jobs that this stage belongs to. */
  val jobIds = new HashSet[Int]

  var pendingTasks = new HashSet[Task[_]]

  private var nextAttemptId: Int = 0

  val name = callSite.shortForm
  val details = callSite.longForm

  /** Pointer to the latest [StageInfo] object, set by DAGScheduler. */
  var latestInfo: StageInfo = StageInfo.fromStage(this)

  /**
   * Spark is resilient to executors dying by retrying stages on FetchFailures. Here, we keep track
   * of the number of stage failures to prevent endless stage retries. However, because 
   * FetchFailures may cause multiple tasks to retry a Stage in parallel, we cannot count stage 
   * failures alone since the same stage may be attempted multiple times simultaneously. 
   * We deal with this by tracking the number of failures per attemptId.
   */
  private val failedStageAttemptIds = new mutable.HashMap[Int, HashSet[Int]]
  private var failedStageCount = 0
  
  private[scheduler] def clearFailures() : Unit = { 
    failedStageAttemptIds.clear()
    failedStageCount = 0
  }

  /**
   * Check whether we should abort the failedStage due to multiple failures.
   * This method updates the running count of failures for a particular stage and returns 
   * true if the number of failures exceeds the allowable number of failures.
   */
  private[scheduler] def failAndShouldAbort(): Boolean = {
    // We increment the failure count on the first attempt for a particular Stage
    if (latestInfo.attemptId == 0)
    {
      failedStageCount += 1
    }
    
    val concurrentFailures = failedStageAttemptIds
      .getOrElseUpdate(failedStageCount, new HashSet[Int]())
      
    concurrentFailures.add(latestInfo.attemptId)
    
    // Check for multiple FetchFailures in a Stage and for the stage failing repeatedly following
    // resubmissions.
    failedStageCount >= Stage.MAX_STAGE_FAILURES || 
      concurrentFailures.size >= Stage.MAX_STAGE_FAILURES
  }
  
  /** Return a new attempt id, starting with 0. */
  def newAttemptId(): Int = {
    val id = nextAttemptId
    nextAttemptId += 1
    id
  }

  def attemptId: Int = nextAttemptId

  override final def hashCode(): Int = id
  override final def equals(other: Any): Boolean = other match {
    case stage: Stage => stage != null && stage.id == id
    case _ => false
  }
}

private[spark] object Stage {
  // The maximum number of times to retry a stage before aborting 
  private[scheduler] val MAX_STAGE_FAILURES = 4
}
