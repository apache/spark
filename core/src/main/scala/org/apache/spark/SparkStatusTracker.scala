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

/**
 * Low-level status reporting APIs for monitoring job and stage progress.
 *
 * These APIs intentionally provide very weak consistency semantics; consumers of these APIs should
 * be prepared to handle empty / missing information.  For example, a job's stage ids may be known
 * but the status API may not have any information about the details of those stages, so
 * `getStageInfo` could potentially return `None` for a valid stage id.
 *
 * To limit memory usage, these APIs only provide information on recent jobs / stages.  These APIs
 * will provide information for the last `spark.ui.retainedStages` stages and
 * `spark.ui.retainedJobs` jobs.
 *
 * NOTE: this class's constructor should be considered private and may be subject to change.
 */
class SparkStatusTracker private[spark] (sc: SparkContext) {

  private val jobProgressListener = sc.jobProgressListener

  /**
   * Return a list of all known jobs in a particular job group.  If `jobGroup` is `null`, then
   * returns all known jobs that are not associated with a job group.
   *
   * The returned list may contain running, failed, and completed jobs, and may vary across
   * invocations of this method.  This method does not guarantee the order of the elements in
   * its result.
   */
  def getJobIdsForGroup(jobGroup: String): Array[Int] = {
    jobProgressListener.synchronized {
      val jobData = jobProgressListener.jobIdToData.valuesIterator
      jobData.filter(_.jobGroup.orNull == jobGroup).map(_.jobId).toArray
    }
  }

  /**
   * Returns an array containing the ids of all active stages.
   *
   * This method does not guarantee the order of the elements in its result.
   */
  def getActiveStageIds(): Array[Int] = {
    jobProgressListener.synchronized {
      jobProgressListener.activeStages.values.map(_.stageId).toArray
    }
  }

  /**
   * Returns an array containing the ids of all active jobs.
   *
   * This method does not guarantee the order of the elements in its result.
   */
  def getActiveJobIds(): Array[Int] = {
    jobProgressListener.synchronized {
      jobProgressListener.activeJobs.values.map(_.jobId).toArray
    }
  }

  /**
   * Returns job information, or `None` if the job info could not be found or was garbage collected.
   */
  def getJobInfo(jobId: Int): Option[SparkJobInfo] = {
    jobProgressListener.synchronized {
      jobProgressListener.jobIdToData.get(jobId).map { data =>
        new SparkJobInfoImpl(jobId, data.stageIds.toArray, data.status)
      }
    }
  }

  /**
   * Returns stage information, or `None` if the stage info could not be found or was
   * garbage collected.
   */
  def getStageInfo(stageId: Int): Option[SparkStageInfo] = {
    jobProgressListener.synchronized {
      for (
        info <- jobProgressListener.stageIdToInfo.get(stageId);
        data <- jobProgressListener.stageIdToData.get((stageId, info.attemptId))
      ) yield {
        new SparkStageInfoImpl(
          stageId,
          info.attemptId,
          info.submissionTime.getOrElse(0),
          info.name,
          info.numTasks,
          data.numActiveTasks,
          data.numCompleteTasks,
          data.numFailedTasks)
      }
    }
  }
}
