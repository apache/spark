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

package org.apache.spark

private[spark] class StatusAPIImpl(sc: SparkContext) {

  def jobIdsForGroup(jobGroup: String): Array[Int] = {
    sc.jobProgressListener.synchronized {
      val jobData = sc.jobProgressListener.jobIdToData.valuesIterator
      jobData.filter(_.jobGroup == Some(jobGroup)).map(_.jobId).toArray
    }
  }

  def newJobInfo(jobId: Int): Option[SparkJobInfo] = {
    sc.jobProgressListener.synchronized {
      sc.jobProgressListener.jobIdToData.get(jobId).map { data =>
        new SparkJobInfoImpl(jobId, data.stageIds.toArray, data.status)
      }
    }
  }

  def newStageInfo(stageId: Int): Option[SparkStageInfo] = {
    sc.jobProgressListener.synchronized {
      for (
        info <- sc.jobProgressListener.stageIdToInfo.get(stageId);
        data <- sc.jobProgressListener.stageIdToData.get((stageId, info.attemptId))
      ) yield {
        new SparkStageInfoImpl(
          stageId,
          info.name,
          numTasks = info.numTasks,
          numActiveTasks = data.numActiveTasks,
          numCompleteTasks = data.numCompleteTasks,
          numFailedTasks = data.numFailedTasks)
      }
    }
  }
}

private class SparkJobInfoImpl (
  val jobId: Int,
  val stageIds: Array[Int],
  val status: String)
 extends SparkJobInfo

private class SparkStageInfoImpl(
  val stageId: Int,
  val name: String,
  val numTasks: Int,
  val numActiveTasks: Int,
  val numCompleteTasks: Int,
  val numFailedTasks: Int)
 extends SparkStageInfo