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

package org.apache.spark.ui.jobs

import scala.collection.mutable.{HashMap, HashSet, ListBuffer}

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.ui.jobs.UIData._

/**
 * :: DeveloperApi ::
 * Tracks task-level information to be displayed in the UI.
 *
 * All access to the data structures in this class must be synchronized on the
 * class, since the UI thread and the EventBus loop may otherwise be reading and
 * updating the internal data structures concurrently.
 */
@DeveloperApi
class JobProgressListener(conf: SparkConf) extends SparkListener with Logging {

  import JobProgressListener._

  // Define a handful of type aliases so that data structures' types can serve as documentation.
  // These type aliases are public because they're used in the types of public fields:

  type JobId = Int
  type StageId = Int
  type StageAttemptId = Int
  type PoolName = String
  type ExecutorId = String

  // Jobs:
  val activeJobs = new HashMap[JobId, JobUIData]
  val completedJobs = ListBuffer[JobUIData]()
  val failedJobs = ListBuffer[JobUIData]()
  val jobIdToData = new HashMap[JobId, JobUIData]

  // Stages:
  val pendingStages = new HashMap[StageId, StageInfo]
  val activeStages = new HashMap[StageId, StageInfo]
  val completedStages = ListBuffer[StageInfo]()
  val skippedStages = ListBuffer[StageInfo]()
  val failedStages = ListBuffer[StageInfo]()
  val stageIdToData = new HashMap[(StageId, StageAttemptId), StageUIData]
  val stageIdToInfo = new HashMap[StageId, StageInfo]
  val stageIdToActiveJobIds = new HashMap[StageId, HashSet[JobId]]
  val poolToActiveStages = HashMap[PoolName, HashMap[StageId, StageInfo]]()
  // Total of completed and failed stages that have ever been run.  These may be greater than
  // `completedStages.size` and `failedStages.size` if we have run more stages or jobs than
  // JobProgressListener's retention limits.
  var numCompletedStages = 0
  var numFailedStages = 0

  // Misc:
  val executorIdToBlockManagerId = HashMap[ExecutorId, BlockManagerId]()
  def blockManagerIds = executorIdToBlockManagerId.values.toSeq

  var schedulingMode: Option[SchedulingMode] = None

  // To limit the total memory usage of JobProgressListener, we only track information for a fixed
  // number of non-active jobs and stages (there is no limit for active jobs and stages):

  val retainedStages = conf.getInt("spark.ui.retainedStages", DEFAULT_RETAINED_STAGES)
  val retainedJobs = conf.getInt("spark.ui.retainedJobs", DEFAULT_RETAINED_JOBS)

  // We can test for memory leaks by ensuring that collections that track non-active jobs and
  // stages do not grow without bound and that collections for active jobs/stages eventually become
  // empty once Spark is idle.  Let's partition our collections into ones that should be empty
  // once Spark is idle and ones that should have a hard- or soft-limited sizes.
  // These methods are used by unit tests, but they're defined here so that people don't forget to
  // update the tests when adding new collections.  Some collections have multiple levels of
  // nesting, etc, so this lets us customize our notion of "size" for each structure:

  // These collections should all be empty once Spark is idle (no active stages / jobs):
  private[spark] def getSizesOfActiveStateTrackingCollections: Map[String, Int] = {
    Map(
      "activeStages" -> activeStages.size,
      "activeJobs" -> activeJobs.size,
      "poolToActiveStages" -> poolToActiveStages.values.map(_.size).sum,
      "stageIdToActiveJobIds" -> stageIdToActiveJobIds.values.map(_.size).sum
    )
  }

  // These collections should stop growing once we have run at least `spark.ui.retainedStages`
  // stages and `spark.ui.retainedJobs` jobs:
  private[spark] def getSizesOfHardSizeLimitedCollections: Map[String, Int] = {
    Map(
      "completedJobs" -> completedJobs.size,
      "failedJobs" -> failedJobs.size,
      "completedStages" -> completedStages.size,
      "skippedStages" -> skippedStages.size,
      "failedStages" -> failedStages.size
    )
  }
  
  // These collections may grow arbitrarily, but once Spark becomes idle they should shrink back to
  // some bound based on the `spark.ui.retainedStages` and `spark.ui.retainedJobs` settings:
  private[spark] def getSizesOfSoftSizeLimitedCollections: Map[String, Int] = {
    Map(
      "jobIdToData" -> jobIdToData.size,
      "stageIdToData" -> stageIdToData.size,
      "stageIdToStageInfo" -> stageIdToInfo.size
    )
  }

  /** If stages is too large, remove and garbage collect old stages */
  private def trimStagesIfNecessary(stages: ListBuffer[StageInfo]) = synchronized {
    if (stages.size > retainedStages) {
      val toRemove = math.max(retainedStages / 10, 1)
      stages.take(toRemove).foreach { s =>
        stageIdToData.remove((s.stageId, s.attemptId))
        stageIdToInfo.remove(s.stageId)
      }
      stages.trimStart(toRemove)
    }
  }

  /** If jobs is too large, remove and garbage collect old jobs */
  private def trimJobsIfNecessary(jobs: ListBuffer[JobUIData]) = synchronized {
    if (jobs.size > retainedJobs) {
      val toRemove = math.max(retainedJobs / 10, 1)
      jobs.take(toRemove).foreach { job =>
        jobIdToData.remove(job.jobId)
      }
      jobs.trimStart(toRemove)
    }
  }

  override def onJobStart(jobStart: SparkListenerJobStart) = synchronized {
    val jobGroup = for (
      props <- Option(jobStart.properties);
      group <- Option(props.getProperty(SparkContext.SPARK_JOB_GROUP_ID))
    ) yield group
    val jobData: JobUIData =
      new JobUIData(
        jobId = jobStart.jobId,
        submissionTime = Option(jobStart.time).filter(_ >= 0),
        stageIds = jobStart.stageIds,
        jobGroup = jobGroup,
        status = JobExecutionStatus.RUNNING)
    jobStart.stageInfos.foreach(x => pendingStages(x.stageId) = x)
    // Compute (a potential underestimate of) the number of tasks that will be run by this job.
    // This may be an underestimate because the job start event references all of the result
    // stages' transitive stage dependencies, but some of these stages might be skipped if their
    // output is available from earlier runs.
    // See https://github.com/apache/spark/pull/3009 for a more extensive discussion.
    jobData.numTasks = {
      val allStages = jobStart.stageInfos
      val missingStages = allStages.filter(_.completionTime.isEmpty)
      missingStages.map(_.numTasks).sum
    }
    jobIdToData(jobStart.jobId) = jobData
    activeJobs(jobStart.jobId) = jobData
    for (stageId <- jobStart.stageIds) {
      stageIdToActiveJobIds.getOrElseUpdate(stageId, new HashSet[StageId]).add(jobStart.jobId)
    }
    // If there's no information for a stage, store the StageInfo received from the scheduler
    // so that we can display stage descriptions for pending stages:
    for (stageInfo <- jobStart.stageInfos) {
      stageIdToInfo.getOrElseUpdate(stageInfo.stageId, stageInfo)
      stageIdToData.getOrElseUpdate((stageInfo.stageId, stageInfo.attemptId), new StageUIData)
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd) = synchronized {
    val jobData = activeJobs.remove(jobEnd.jobId).getOrElse {
      logWarning(s"Job completed for unknown job ${jobEnd.jobId}")
      new JobUIData(jobId = jobEnd.jobId)
    }
    jobData.completionTime = Option(jobEnd.time).filter(_ >= 0)

    jobData.stageIds.foreach(pendingStages.remove)
    jobEnd.jobResult match {
      case JobSucceeded =>
        completedJobs += jobData
        trimJobsIfNecessary(completedJobs)
        jobData.status = JobExecutionStatus.SUCCEEDED
      case JobFailed(exception) =>
        failedJobs += jobData
        trimJobsIfNecessary(failedJobs)
        jobData.status = JobExecutionStatus.FAILED
    }
    for (stageId <- jobData.stageIds) {
      stageIdToActiveJobIds.get(stageId).foreach { jobsUsingStage =>
        jobsUsingStage.remove(jobEnd.jobId)
        stageIdToInfo.get(stageId).foreach { stageInfo =>
          if (stageInfo.submissionTime.isEmpty) {
            // if this stage is pending, it won't complete, so mark it as "skipped":
            skippedStages += stageInfo
            trimStagesIfNecessary(skippedStages)
            jobData.numSkippedStages += 1
            jobData.numSkippedTasks += stageInfo.numTasks
          }
        }
      }
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) = synchronized {
    val stage = stageCompleted.stageInfo
    stageIdToInfo(stage.stageId) = stage
    val stageData = stageIdToData.getOrElseUpdate((stage.stageId, stage.attemptId), {
      logWarning("Stage completed for unknown stage " + stage.stageId)
      new StageUIData
    })

    for ((id, info) <- stageCompleted.stageInfo.accumulables) {
      stageData.accumulables(id) = info
    }

    poolToActiveStages.get(stageData.schedulingPool).foreach { hashMap =>
      hashMap.remove(stage.stageId)
    }
    activeStages.remove(stage.stageId)
    if (stage.failureReason.isEmpty) {
      completedStages += stage
      numCompletedStages += 1
      trimStagesIfNecessary(completedStages)
    } else {
      failedStages += stage
      numFailedStages += 1
      trimStagesIfNecessary(failedStages)
    }

    for (
      activeJobsDependentOnStage <- stageIdToActiveJobIds.get(stage.stageId);
      jobId <- activeJobsDependentOnStage;
      jobData <- jobIdToData.get(jobId)
    ) {
      jobData.numActiveStages -= 1
      if (stage.failureReason.isEmpty) {
        jobData.completedStageIndices.add(stage.stageId)
      } else {
        jobData.numFailedStages += 1
      }
    }
  }

  /** For FIFO, all stages are contained by "default" pool but "default" pool here is meaningless */
  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) = synchronized {
    val stage = stageSubmitted.stageInfo
    activeStages(stage.stageId) = stage
    pendingStages.remove(stage.stageId)
    val poolName = Option(stageSubmitted.properties).map {
      p => p.getProperty("spark.scheduler.pool", DEFAULT_POOL_NAME)
    }.getOrElse(DEFAULT_POOL_NAME)

    stageIdToInfo(stage.stageId) = stage
    val stageData = stageIdToData.getOrElseUpdate((stage.stageId, stage.attemptId), new StageUIData)
    stageData.schedulingPool = poolName

    stageData.description = Option(stageSubmitted.properties).flatMap {
      p => Option(p.getProperty(SparkContext.SPARK_JOB_DESCRIPTION))
    }

    val stages = poolToActiveStages.getOrElseUpdate(poolName, new HashMap[Int, StageInfo])
    stages(stage.stageId) = stage

    for (
      activeJobsDependentOnStage <- stageIdToActiveJobIds.get(stage.stageId);
      jobId <- activeJobsDependentOnStage;
      jobData <- jobIdToData.get(jobId)
    ) {
      jobData.numActiveStages += 1
    }
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart) = synchronized {
    val taskInfo = taskStart.taskInfo
    if (taskInfo != null) {
      val stageData = stageIdToData.getOrElseUpdate((taskStart.stageId, taskStart.stageAttemptId), {
        logWarning("Task start for unknown stage " + taskStart.stageId)
        new StageUIData
      })
      stageData.numActiveTasks += 1
      stageData.taskData.put(taskInfo.taskId, new TaskUIData(taskInfo))
    }
    for (
      activeJobsDependentOnStage <- stageIdToActiveJobIds.get(taskStart.stageId);
      jobId <- activeJobsDependentOnStage;
      jobData <- jobIdToData.get(jobId)
    ) {
      jobData.numActiveTasks += 1
    }
  }

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult) {
    // Do nothing: because we don't do a deep copy of the TaskInfo, the TaskInfo in
    // stageToTaskInfos already has the updated status.
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) = synchronized {
    val info = taskEnd.taskInfo
    // If stage attempt id is -1, it means the DAGScheduler had no idea which attempt this task
    // completion event is for. Let's just drop it here. This means we might have some speculation
    // tasks on the web ui that's never marked as complete.
    if (info != null && taskEnd.stageAttemptId != -1) {
      val stageData = stageIdToData.getOrElseUpdate((taskEnd.stageId, taskEnd.stageAttemptId), {
        logWarning("Task end for unknown stage " + taskEnd.stageId)
        new StageUIData
      })

      for (accumulableInfo <- info.accumulables) {
        stageData.accumulables(accumulableInfo.id) = accumulableInfo
      }

      val execSummaryMap = stageData.executorSummary
      val execSummary = execSummaryMap.getOrElseUpdate(info.executorId, new ExecutorSummary)

      taskEnd.reason match {
        case Success =>
          execSummary.succeededTasks += 1
        case _ =>
          execSummary.failedTasks += 1
      }
      execSummary.taskTime += info.duration
      stageData.numActiveTasks -= 1

      val (errorMessage, metrics): (Option[String], Option[TaskMetrics]) =
        taskEnd.reason match {
          case org.apache.spark.Success =>
            stageData.completedIndices.add(info.index)
            stageData.numCompleteTasks += 1
            (None, Option(taskEnd.taskMetrics))
          case e: ExceptionFailure =>  // Handle ExceptionFailure because we might have metrics
            stageData.numFailedTasks += 1
            (Some(e.toErrorString), e.metrics)
          case e: TaskFailedReason =>  // All other failure cases
            stageData.numFailedTasks += 1
            (Some(e.toErrorString), None)
        }

      if (!metrics.isEmpty) {
        val oldMetrics = stageData.taskData.get(info.taskId).flatMap(_.taskMetrics)
        updateAggregateMetrics(stageData, info.executorId, metrics.get, oldMetrics)
      }

      val taskData = stageData.taskData.getOrElseUpdate(info.taskId, new TaskUIData(info))
      taskData.taskInfo = info
      taskData.taskMetrics = metrics
      taskData.errorMessage = errorMessage

      for (
        activeJobsDependentOnStage <- stageIdToActiveJobIds.get(taskEnd.stageId);
        jobId <- activeJobsDependentOnStage;
        jobData <- jobIdToData.get(jobId)
      ) {
        jobData.numActiveTasks -= 1
        taskEnd.reason match {
          case Success =>
            jobData.numCompletedTasks += 1
          case _ =>
            jobData.numFailedTasks += 1
        }
      }
    }
  }

  /**
   * Upon receiving new metrics for a task, updates the per-stage and per-executor-per-stage
   * aggregate metrics by calculating deltas between the currently recorded metrics and the new
   * metrics.
   */
  def updateAggregateMetrics(
      stageData: StageUIData,
      execId: String,
      taskMetrics: TaskMetrics,
      oldMetrics: Option[TaskMetrics]) {
    val execSummary = stageData.executorSummary.getOrElseUpdate(execId, new ExecutorSummary)

    val shuffleWriteDelta =
      (taskMetrics.shuffleWriteMetrics.map(_.shuffleBytesWritten).getOrElse(0L)
      - oldMetrics.flatMap(_.shuffleWriteMetrics).map(_.shuffleBytesWritten).getOrElse(0L))
    stageData.shuffleWriteBytes += shuffleWriteDelta
    execSummary.shuffleWrite += shuffleWriteDelta

    val shuffleReadDelta =
      (taskMetrics.shuffleReadMetrics.map(_.remoteBytesRead).getOrElse(0L)
      - oldMetrics.flatMap(_.shuffleReadMetrics).map(_.remoteBytesRead).getOrElse(0L))
    stageData.shuffleReadBytes += shuffleReadDelta
    execSummary.shuffleRead += shuffleReadDelta

    val inputBytesDelta =
      (taskMetrics.inputMetrics.map(_.bytesRead).getOrElse(0L)
      - oldMetrics.flatMap(_.inputMetrics).map(_.bytesRead).getOrElse(0L))
    stageData.inputBytes += inputBytesDelta
    execSummary.inputBytes += inputBytesDelta

    val outputBytesDelta =
      (taskMetrics.outputMetrics.map(_.bytesWritten).getOrElse(0L)
        - oldMetrics.flatMap(_.outputMetrics).map(_.bytesWritten).getOrElse(0L))
    stageData.outputBytes += outputBytesDelta
    execSummary.outputBytes += outputBytesDelta

    val diskSpillDelta =
      taskMetrics.diskBytesSpilled - oldMetrics.map(_.diskBytesSpilled).getOrElse(0L)
    stageData.diskBytesSpilled += diskSpillDelta
    execSummary.diskBytesSpilled += diskSpillDelta

    val memorySpillDelta =
      taskMetrics.memoryBytesSpilled - oldMetrics.map(_.memoryBytesSpilled).getOrElse(0L)
    stageData.memoryBytesSpilled += memorySpillDelta
    execSummary.memoryBytesSpilled += memorySpillDelta

    val timeDelta =
      taskMetrics.executorRunTime - oldMetrics.map(_.executorRunTime).getOrElse(0L)
    stageData.executorRunTime += timeDelta
  }

  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate) {
    for ((taskId, sid, sAttempt, taskMetrics) <- executorMetricsUpdate.taskMetrics) {
      val stageData = stageIdToData.getOrElseUpdate((sid, sAttempt), {
        logWarning("Metrics update for task in unknown stage " + sid)
        new StageUIData
      })
      val taskData = stageData.taskData.get(taskId)
      taskData.map { t =>
        if (!t.taskInfo.finished) {
          updateAggregateMetrics(stageData, executorMetricsUpdate.execId, taskMetrics,
            t.taskMetrics)

          // Overwrite task metrics
          t.taskMetrics = Some(taskMetrics)
        }
      }
    }
  }

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate) {
    synchronized {
      schedulingMode = environmentUpdate
        .environmentDetails("Spark Properties").toMap
        .get("spark.scheduler.mode")
        .map(SchedulingMode.withName)
    }
  }

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded) {
    synchronized {
      val blockManagerId = blockManagerAdded.blockManagerId
      val executorId = blockManagerId.executorId
      executorIdToBlockManagerId(executorId) = blockManagerId
    }
  }

  override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved) {
    synchronized {
      val executorId = blockManagerRemoved.blockManagerId.executorId
      executorIdToBlockManagerId.remove(executorId)
    }
  }

}

private object JobProgressListener {
  val DEFAULT_POOL_NAME = "default"
  val DEFAULT_RETAINED_STAGES = 1000
  val DEFAULT_RETAINED_JOBS = 1000
}
