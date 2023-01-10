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

import scala.collection.mutable.HashMap

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.storage.RDDInfo

/**
 * :: DeveloperApi ::
 * Stores information about a stage to pass from the scheduler to SparkListeners.
 */
@DeveloperApi
class StageInfo(
    val stageId: Int,
    private val attemptId: Int,
    val name: String,
    val numTasks: Int,
    val rddInfos: Seq[RDDInfo],
    val parentIds: Seq[Int],
    val details: String,
    val taskMetrics: TaskMetrics = null,
    private[spark] val taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty,
    private[spark] val shuffleDepId: Option[Int] = None,
    val resourceProfileId: Int,
    private[spark] var isShufflePushEnabled: Boolean = false,
    private[spark] var shuffleMergerCount: Int = 0) {
  /** When this stage was submitted from the DAGScheduler to a TaskScheduler. */
  var submissionTime: Option[Long] = None
  /** Time when the stage completed or when the stage was cancelled. */
  var completionTime: Option[Long] = None
  /** If the stage failed, the reason why. */
  var failureReason: Option[String] = None

  /**
   * Terminal values of accumulables updated during this stage, including all the user-defined
   * accumulators.
   */
  val accumulables = HashMap[Long, AccumulableInfo]()

  def stageFailed(reason: String): Unit = {
    failureReason = Some(reason)
    completionTime = Some(System.currentTimeMillis)
  }

  // This would just be the second constructor arg, except we need to maintain this method
  // with parentheses for compatibility
  def attemptNumber(): Int = attemptId

  private[spark] def getStatusString: String = {
    if (completionTime.isDefined) {
      if (failureReason.isDefined) {
        "failed"
      } else {
        "succeeded"
      }
    } else {
      "running"
    }
  }

  private[spark] def setShuffleMergerCount(mergers: Int): Unit = {
    shuffleMergerCount = mergers
  }

  private[spark] def setPushBasedShuffleEnabled(pushBasedShuffleEnabled: Boolean): Unit = {
    isShufflePushEnabled = pushBasedShuffleEnabled
  }
}

private[spark] object StageInfo {
  /**
   * Construct a StageInfo from a Stage.
   *
   * Each Stage is associated with one or many RDDs, with the boundary of a Stage marked by
   * shuffle dependencies. Therefore, all ancestor RDDs related to this Stage's RDD through a
   * sequence of narrow dependencies should also be associated with this Stage.
   */
  def fromStage(
      stage: Stage,
      attemptId: Int,
      numTasks: Option[Int] = None,
      taskMetrics: TaskMetrics = null,
      taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty,
      resourceProfileId: Int
    ): StageInfo = {
    val ancestorRddInfos = stage.rdd.getNarrowAncestors.map(RDDInfo.fromRdd)
    val rddInfos = Seq(RDDInfo.fromRdd(stage.rdd)) ++ ancestorRddInfos
    val shuffleDepId = stage match {
      case sms: ShuffleMapStage => Option(sms.shuffleDep).map(_.shuffleId)
      case _ => None
    }
    new StageInfo(
      stage.id,
      attemptId,
      stage.name,
      numTasks.getOrElse(stage.numTasks),
      rddInfos,
      stage.parents.map(_.id),
      stage.details,
      taskMetrics,
      taskLocalityPreferences,
      shuffleDepId,
      resourceProfileId,
      false,
      0)
  }
}
