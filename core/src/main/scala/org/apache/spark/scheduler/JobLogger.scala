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

import java.io.{File, FileNotFoundException, IOException, PrintWriter}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import java.util.concurrent.LinkedBlockingQueue

import scala.collection.mutable.HashMap

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.{DataReadMethod, TaskMetrics}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * :: DeveloperApi ::
 * A logger class to record runtime information for jobs in Spark. This class outputs one log file
 * for each Spark job, containing tasks start/stop and shuffle information. JobLogger is a subclass
 * of SparkListener, use addSparkListener to add JobLogger to a SparkContext after the SparkContext
 * is created. Note that each JobLogger only works for one SparkContext
 *
 * NOTE: The functionality of this class is heavily stripped down to accommodate for a general
 * refactor of the SparkListener interface. In its place, the EventLoggingListener is introduced
 * to log application information as SparkListenerEvents. To enable this functionality, set
 * spark.eventLog.enabled to true.
 */
@DeveloperApi
@deprecated("Log application information by setting spark.eventLog.enabled.", "1.0.0")
class JobLogger(val user: String, val logDirName: String) extends SparkListener with Logging {

  def this() = this(System.getProperty("user.name", "<unknown>"),
    String.valueOf(System.currentTimeMillis()))

  private val logDir =
    if (System.getenv("SPARK_LOG_DIR") != null) {
      System.getenv("SPARK_LOG_DIR")
    } else {
      "/tmp/spark-%s".format(user)
    }

  private val jobIdToPrintWriter = new HashMap[Int, PrintWriter]
  private val stageIdToJobId = new HashMap[Int, Int]
  private val jobIdToStageIds = new HashMap[Int, Seq[Int]]
  private val dateFormat = new ThreadLocal[SimpleDateFormat]() {
    override def initialValue() = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
  }
  private val eventQueue = new LinkedBlockingQueue[SparkListenerEvent]

  createLogDir()

  // The following 5 functions are used only in testing.
  private[scheduler] def getLogDir = logDir
  private[scheduler] def getJobIdToPrintWriter = jobIdToPrintWriter
  private[scheduler] def getStageIdToJobId = stageIdToJobId
  private[scheduler] def getJobIdToStageIds = jobIdToStageIds
  private[scheduler] def getEventQueue = eventQueue

  /** Create a folder for log files, the folder's name is the creation time of jobLogger */
  protected def createLogDir() {
    val dir = new File(logDir + "/" + logDirName + "/")
    if (dir.exists()) {
      return
    }
    if (dir.mkdirs() == false) {
      // JobLogger should throw a exception rather than continue to construct this object.
      throw new IOException("create log directory error:" + logDir + "/" + logDirName + "/")
    }
  }

  /**
   * Create a log file for one job
   * @param jobId ID of the job
   * @throws FileNotFoundException Fail to create log file
   */
  protected def createLogWriter(jobId: Int) {
    try {
      val fileWriter = new PrintWriter(logDir + "/" + logDirName + "/" + jobId)
      jobIdToPrintWriter += (jobId -> fileWriter)
    } catch {
      case e: FileNotFoundException => e.printStackTrace()
    }
  }

  /**
   * Close log file, and clean the stage relationship in stageIdToJobId
   * @param jobId ID of the job
   */
  protected def closeLogWriter(jobId: Int) {
    jobIdToPrintWriter.get(jobId).foreach { fileWriter =>
      fileWriter.close()
      jobIdToStageIds.get(jobId).foreach(_.foreach { stageId =>
        stageIdToJobId -= stageId
      })
      jobIdToPrintWriter -= jobId
      jobIdToStageIds -= jobId
    }
  }

  /**
   * Build up the maps that represent stage-job relationships
   * @param jobId ID of the job
   * @param stageIds IDs of the associated stages
   */
  protected def buildJobStageDependencies(jobId: Int, stageIds: Seq[Int]) = {
    jobIdToStageIds(jobId) = stageIds
    stageIds.foreach { stageId => stageIdToJobId(stageId) = jobId }
  }

  /**
   * Write info into log file
   * @param jobId ID of the job
   * @param info Info to be recorded
   * @param withTime Controls whether to record time stamp before the info, default is true
   */
  protected def jobLogInfo(jobId: Int, info: String, withTime: Boolean = true) {
    var writeInfo = info
    if (withTime) {
      val date = new Date(System.currentTimeMillis())
      writeInfo = dateFormat.get.format(date) + ": " + info
    }
    jobIdToPrintWriter.get(jobId).foreach(_.println(writeInfo))
  }

  /**
   * Write info into log file
   * @param stageId ID of the stage
   * @param info Info to be recorded
   * @param withTime Controls whether to record time stamp before the info, default is true
   */
  protected def stageLogInfo(stageId: Int, info: String, withTime: Boolean = true) {
    stageIdToJobId.get(stageId).foreach(jobId => jobLogInfo(jobId, info, withTime))
  }

  /**
   * Record task metrics into job log files, including execution info and shuffle metrics
   * @param stageId Stage ID of the task
   * @param status Status info of the task
   * @param taskInfo Task description info
   * @param taskMetrics Task running metrics
   */
  protected def recordTaskMetrics(stageId: Int, status: String,
                                taskInfo: TaskInfo, taskMetrics: TaskMetrics) {
    val info = " TID=" + taskInfo.taskId + " STAGE_ID=" + stageId +
               " START_TIME=" + taskInfo.launchTime + " FINISH_TIME=" + taskInfo.finishTime +
               " EXECUTOR_ID=" + taskInfo.executorId +  " HOST=" + taskMetrics.hostname
    val executorRunTime = " EXECUTOR_RUN_TIME=" + taskMetrics.executorRunTime
    val gcTime = " GC_TIME=" + taskMetrics.jvmGCTime
    val inputMetrics = taskMetrics.inputMetrics match {
      case Some(metrics) =>
        " READ_METHOD=" + metrics.readMethod.toString +
        " INPUT_BYTES=" + metrics.bytesRead
      case None => ""
    }
    val shuffleReadMetrics = taskMetrics.shuffleReadMetrics match {
      case Some(metrics) =>
        " BLOCK_FETCHED_TOTAL=" + metrics.totalBlocksFetched +
        " BLOCK_FETCHED_LOCAL=" + metrics.localBlocksFetched +
        " BLOCK_FETCHED_REMOTE=" + metrics.remoteBlocksFetched +
        " REMOTE_FETCH_WAIT_TIME=" + metrics.fetchWaitTime +
        " REMOTE_BYTES_READ=" + metrics.remoteBytesRead
      case None => ""
    }
    val writeMetrics = taskMetrics.shuffleWriteMetrics match {
      case Some(metrics) =>
        " SHUFFLE_BYTES_WRITTEN=" + metrics.shuffleBytesWritten +
        " SHUFFLE_WRITE_TIME=" + metrics.shuffleWriteTime
      case None => ""
    }
    stageLogInfo(stageId, status + info + executorRunTime + gcTime + inputMetrics +
      shuffleReadMetrics + writeMetrics)
  }

  /**
   * When stage is submitted, record stage submit info
   * @param stageSubmitted Stage submitted event
   */
  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) {
    val stageInfo = stageSubmitted.stageInfo
    stageLogInfo(stageInfo.stageId, "STAGE_ID=%d STATUS=SUBMITTED TASK_SIZE=%d".format(
      stageInfo.stageId, stageInfo.numTasks))
  }

  /**
   * When stage is completed, record stage completion status
   * @param stageCompleted Stage completed event
   */
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
    val stageId = stageCompleted.stageInfo.stageId
    if (stageCompleted.stageInfo.failureReason.isEmpty) {
      stageLogInfo(stageId, s"STAGE_ID=$stageId STATUS=COMPLETED")
    } else {
      stageLogInfo(stageId, s"STAGE_ID=$stageId STATUS=FAILED")
    }
  }

  /**
   * When task ends, record task completion status and metrics
   * @param taskEnd Task end event
   */
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    val taskInfo = taskEnd.taskInfo
    var taskStatus = "TASK_TYPE=%s".format(taskEnd.taskType)
    val taskMetrics = if (taskEnd.taskMetrics != null) taskEnd.taskMetrics else TaskMetrics.empty
    taskEnd.reason match {
      case Success => taskStatus += " STATUS=SUCCESS"
        recordTaskMetrics(taskEnd.stageId, taskStatus, taskInfo, taskMetrics)
      case Resubmitted =>
        taskStatus += " STATUS=RESUBMITTED TID=" + taskInfo.taskId +
                      " STAGE_ID=" + taskEnd.stageId
        stageLogInfo(taskEnd.stageId, taskStatus)
      case FetchFailed(bmAddress, shuffleId, mapId, reduceId) =>
        taskStatus += " STATUS=FETCHFAILED TID=" + taskInfo.taskId + " STAGE_ID=" +
                      taskEnd.stageId + " SHUFFLE_ID=" + shuffleId + " MAP_ID=" +
                      mapId + " REDUCE_ID=" + reduceId
        stageLogInfo(taskEnd.stageId, taskStatus)
      case _ =>
    }
  }

  /**
   * When job ends, recording job completion status and close log file
   * @param jobEnd Job end event
   */
  override def onJobEnd(jobEnd: SparkListenerJobEnd) {
    val jobId = jobEnd.jobId
    var info = "JOB_ID=" + jobId
    jobEnd.jobResult match {
      case JobSucceeded => info += " STATUS=SUCCESS"
      case JobFailed(exception) =>
        info += " STATUS=FAILED REASON="
        exception.getMessage.split("\\s+").foreach(info += _ + "_")
      case _ =>
    }
    jobLogInfo(jobId, info.substring(0, info.length - 1).toUpperCase)
    closeLogWriter(jobId)
  }

  /**
   * Record job properties into job log file
   * @param jobId ID of the job
   * @param properties Properties of the job
   */
  protected def recordJobProperties(jobId: Int, properties: Properties) {
    if (properties != null) {
      val description = properties.getProperty(SparkContext.SPARK_JOB_DESCRIPTION, "")
      jobLogInfo(jobId, description, false)
    }
  }

  /**
   * When job starts, record job property and stage graph
   * @param jobStart Job start event
   */
  override def onJobStart(jobStart: SparkListenerJobStart) {
    val jobId = jobStart.jobId
    val properties = jobStart.properties
    createLogWriter(jobId)
    recordJobProperties(jobId, properties)
    buildJobStageDependencies(jobId, jobStart.stageIds)
    jobLogInfo(jobId, "JOB_ID=" + jobId + " STATUS=STARTED")
  }
}
