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

import java.io.PrintWriter
import java.io.File
import java.io.FileNotFoundException
import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import java.util.concurrent.LinkedBlockingQueue

import scala.collection.mutable.{Map, HashMap, ListBuffer}
import scala.io.Source

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.cluster.TaskInfo

// Used to record runtime information for each job, including RDD graph 
// tasks' start/stop shuffle information and information from outside

class JobLogger(val logDirName: String) extends SparkListener with Logging {
  private val logDir =  
    if (System.getenv("SPARK_LOG_DIR") != null)  
      System.getenv("SPARK_LOG_DIR")
    else 
      "/tmp/spark"
  private val jobIDToPrintWriter = new HashMap[Int, PrintWriter] 
  private val stageIDToJobID = new HashMap[Int, Int]
  private val jobIDToStages = new HashMap[Int, ListBuffer[Stage]]
  private val DATE_FORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
  private val eventQueue = new LinkedBlockingQueue[SparkListenerEvents]
  
  createLogDir()
  def this() = this(String.valueOf(System.currentTimeMillis()))
  
  def getLogDir = logDir
  def getJobIDtoPrintWriter = jobIDToPrintWriter
  def getStageIDToJobID = stageIDToJobID
  def getJobIDToStages = jobIDToStages
  def getEventQueue = eventQueue
  
  // Create a folder for log files, the folder's name is the creation time of the jobLogger
  protected def createLogDir() {
    val dir = new File(logDir + "/" + logDirName + "/")
    if (dir.exists()) {
      return
    }
    if (dir.mkdirs() == false) {
      logError("create log directory error:" + logDir + "/" + logDirName + "/")
    }
  }
  
  // Create a log file for one job, the file name is the jobID
  protected def createLogWriter(jobID: Int) {
    try{
      val fileWriter = new PrintWriter(logDir + "/" + logDirName + "/" + jobID)
      jobIDToPrintWriter += (jobID -> fileWriter)
      } catch {
        case e: FileNotFoundException => e.printStackTrace()
      }
  }
  
  // Close log file, and clean the stage relationship in stageIDToJobID 
  protected def closeLogWriter(jobID: Int) = 
    jobIDToPrintWriter.get(jobID).foreach { fileWriter => 
      fileWriter.close()
      jobIDToStages.get(jobID).foreach(_.foreach{ stage => 
        stageIDToJobID -= stage.id
      })
      jobIDToPrintWriter -= jobID
      jobIDToStages -= jobID
    }
  
  // Write log information to log file, withTime parameter controls whether to recored 
  // time stamp for the information
  protected def jobLogInfo(jobID: Int, info: String, withTime: Boolean = true) {
    var writeInfo = info
    if (withTime) {
      val date = new Date(System.currentTimeMillis())
      writeInfo = DATE_FORMAT.format(date) + ": " +info
    }
    jobIDToPrintWriter.get(jobID).foreach(_.println(writeInfo))
  }
  
  protected def stageLogInfo(stageID: Int, info: String, withTime: Boolean = true) = 
    stageIDToJobID.get(stageID).foreach(jobID => jobLogInfo(jobID, info, withTime))

  protected def buildJobDep(jobID: Int, stage: Stage) {
    if (stage.jobId == jobID) {
      jobIDToStages.get(jobID) match {
        case Some(stageList) => stageList += stage
        case None => val stageList = new  ListBuffer[Stage]
                     stageList += stage
                     jobIDToStages += (jobID -> stageList)
      }
      stageIDToJobID += (stage.id -> jobID)
      stage.parents.foreach(buildJobDep(jobID, _))
    }
  }

  protected def recordStageDep(jobID: Int) {
    def getRddsInStage(rdd: RDD[_]): ListBuffer[RDD[_]] = {
      var rddList = new ListBuffer[RDD[_]]
      rddList += rdd
      rdd.dependencies.foreach{ dep => dep match {
          case shufDep: ShuffleDependency[_,_] =>
          case _ => rddList ++= getRddsInStage(dep.rdd)
        }
      }
      rddList
    }
    jobIDToStages.get(jobID).foreach {_.foreach { stage => 
        var depRddDesc: String = ""
        getRddsInStage(stage.rdd).foreach { rdd => 
          depRddDesc += rdd.id + ","
        }
        var depStageDesc: String = ""
        stage.parents.foreach { stage => 
          depStageDesc += "(" + stage.id + "," + stage.shuffleDep.get.shuffleId + ")"
        }
        jobLogInfo(jobID, "STAGE_ID=" + stage.id + " RDD_DEP=(" + 
                   depRddDesc.substring(0, depRddDesc.length - 1) + ")" + 
                   " STAGE_DEP=" + depStageDesc, false)
      }
    }
  }
  
  // Generate indents and convert to String
  protected def indentString(indent: Int) = {
    val sb = new StringBuilder()
    for (i <- 1 to indent) {
      sb.append(" ")
    }
    sb.toString()
  }
  
  protected def getRddName(rdd: RDD[_]) = {
    var rddName = rdd.getClass.getName
    if (rdd.name != null) {
      rddName = rdd.name 
    }
    rddName
  }
  
  protected def recordRddInStageGraph(jobID: Int, rdd: RDD[_], indent: Int) {
    val rddInfo = "RDD_ID=" + rdd.id + "(" + getRddName(rdd) + "," + rdd.generator + ")"
    jobLogInfo(jobID, indentString(indent) + rddInfo, false)
    rdd.dependencies.foreach{ dep => dep match {
        case shufDep: ShuffleDependency[_,_] => 
          val depInfo = "SHUFFLE_ID=" + shufDep.shuffleId
          jobLogInfo(jobID, indentString(indent + 1) + depInfo, false)
        case _ => recordRddInStageGraph(jobID, dep.rdd, indent + 1)
      }
    }
  }
  
  protected def recordStageDepGraph(jobID: Int, stage: Stage, indent: Int = 0) {
    var stageInfo: String = ""
    if (stage.isShuffleMap) {
      stageInfo = "STAGE_ID=" + stage.id + " MAP_STAGE SHUFFLE_ID=" + 
                  stage.shuffleDep.get.shuffleId
    }else{
      stageInfo = "STAGE_ID=" + stage.id + " RESULT_STAGE"
    }
    if (stage.jobId == jobID) {
      jobLogInfo(jobID, indentString(indent) + stageInfo, false)
      recordRddInStageGraph(jobID, stage.rdd, indent)
      stage.parents.foreach(recordStageDepGraph(jobID, _, indent + 2))
    } else
      jobLogInfo(jobID, indentString(indent) + stageInfo + " JOB_ID=" + stage.jobId, false)
  }
  
  // Record task metrics into job log files
  protected def recordTaskMetrics(stageID: Int, status: String, 
                                taskInfo: TaskInfo, taskMetrics: TaskMetrics) {
    val info = " TID=" + taskInfo.taskId + " STAGE_ID=" + stageID + 
               " START_TIME=" + taskInfo.launchTime + " FINISH_TIME=" + taskInfo.finishTime + 
               " EXECUTOR_ID=" + taskInfo.executorId +  " HOST=" + taskMetrics.hostname
    val executorRunTime = " EXECUTOR_RUN_TIME=" + taskMetrics.executorRunTime
    val readMetrics = 
      taskMetrics.shuffleReadMetrics match {
        case Some(metrics) => 
          " SHUFFLE_FINISH_TIME=" + metrics.shuffleFinishTime + 
          " BLOCK_FETCHED_TOTAL=" + metrics.totalBlocksFetched + 
          " BLOCK_FETCHED_LOCAL=" + metrics.localBlocksFetched + 
          " BLOCK_FETCHED_REMOTE=" + metrics.remoteBlocksFetched + 
          " REMOTE_FETCH_WAIT_TIME=" + metrics.fetchWaitTime + 
          " REMOTE_FETCH_TIME=" + metrics.remoteFetchTime + 
          " REMOTE_BYTES_READ=" + metrics.remoteBytesRead
        case None => ""
      }
    val writeMetrics = 
      taskMetrics.shuffleWriteMetrics match {
        case Some(metrics) => 
          " SHUFFLE_BYTES_WRITTEN=" + metrics.shuffleBytesWritten
        case None => ""
      }
    stageLogInfo(stageID, status + info + executorRunTime + readMetrics + writeMetrics)
  }
  
  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) {
    stageLogInfo(
      stageSubmitted.stage.id,
      "STAGE_ID=%d STATUS=SUBMITTED TASK_SIZE=%d".format(
        stageSubmitted.stage.id, stageSubmitted.taskSize))
  }
  
  override def onStageCompleted(stageCompleted: StageCompleted) {
    stageLogInfo(
      stageCompleted.stageInfo.stage.id,
      "STAGE_ID=%d STATUS=COMPLETED".format(stageCompleted.stageInfo.stage.id))
    
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart) { }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    val task = taskEnd.task
    val taskInfo = taskEnd.taskInfo
    var taskStatus = ""
    task match {
      case resultTask: ResultTask[_, _] => taskStatus = "TASK_TYPE=RESULT_TASK"
      case shuffleMapTask: ShuffleMapTask => taskStatus = "TASK_TYPE=SHUFFLE_MAP_TASK"
    }
    taskEnd.reason match {
      case Success => taskStatus += " STATUS=SUCCESS"
        recordTaskMetrics(task.stageId, taskStatus, taskInfo, taskEnd.taskMetrics)
      case Resubmitted => 
        taskStatus += " STATUS=RESUBMITTED TID=" + taskInfo.taskId + 
                      " STAGE_ID=" + task.stageId
        stageLogInfo(task.stageId, taskStatus)
      case FetchFailed(bmAddress, shuffleId, mapId, reduceId) => 
        taskStatus += " STATUS=FETCHFAILED TID=" + taskInfo.taskId + " STAGE_ID=" + 
                      task.stageId + " SHUFFLE_ID=" + shuffleId + " MAP_ID=" + 
                      mapId + " REDUCE_ID=" + reduceId
        stageLogInfo(task.stageId, taskStatus)
      case OtherFailure(message) => 
        taskStatus += " STATUS=FAILURE TID=" + taskInfo.taskId + 
                      " STAGE_ID=" + task.stageId + " INFO=" + message
        stageLogInfo(task.stageId, taskStatus)
      case _ =>
    }
  }
  
  override def onJobEnd(jobEnd: SparkListenerJobEnd) {
    val job = jobEnd.job
    var info = "JOB_ID=" + job.jobId
    jobEnd.jobResult match {
      case JobSucceeded => info += " STATUS=SUCCESS"
      case JobFailed(exception, _) =>
        info += " STATUS=FAILED REASON="
        exception.getMessage.split("\\s+").foreach(info += _ + "_")
      case _ =>
    }
    jobLogInfo(job.jobId, info.substring(0, info.length - 1).toUpperCase)
    closeLogWriter(job.jobId)
  }

  protected def recordJobProperties(jobID: Int, properties: Properties) {
    if(properties != null) {
      val description = properties.getProperty(SparkContext.SPARK_JOB_DESCRIPTION, "")
      jobLogInfo(jobID, description, false)
    }
  }

  override def onJobStart(jobStart: SparkListenerJobStart) {
    val job = jobStart.job
    val properties = jobStart.properties
    createLogWriter(job.jobId)
    recordJobProperties(job.jobId, properties)
    buildJobDep(job.jobId, job.finalStage)
    recordStageDep(job.jobId)
    recordStageDepGraph(job.jobId, job.finalStage)
    jobLogInfo(job.jobId, "JOB_ID=" + job.jobId + " STATUS=STARTED")
  }
}
