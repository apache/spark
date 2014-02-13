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

package org.apache.spark.ui.exec

import javax.servlet.http.HttpServletRequest

import scala.collection.mutable.HashMap
import scala.xml.Node

import org.eclipse.jetty.server.Handler

import org.apache.spark.{ExceptionFailure, Logging, SparkContext}
import org.apache.spark.scheduler._
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.ui.Page.Executors
import org.apache.spark.ui.UIUtils
import org.apache.spark.util.{Utils, FileLogger}

import net.liftweb.json.JsonAST._
import net.liftweb.json.JsonDSL._

private[spark] class ExecutorsUI(val sc: SparkContext) {

  private var _listener: Option[ExecutorsListener] = None
  def listener = _listener.get

  def start() {
    _listener = Some(new ExecutorsListener(sc))
    sc.addSparkListener(listener)
  }

  def getHandlers = Seq[(String, Handler)](
    ("/executors", (request: HttpServletRequest) => render(request))
  )

  /** Render an HTML page that encodes executor information */
  def render(request: HttpServletRequest): Seq[Node] = {
    listener.updateStorageStatusFromEnv()
    val summaryJson = listener.summaryJson
    val executorsJson = listener.executorIdToJson.values.toSeq
    renderFromJson(summaryJson, executorsJson)
  }

  /** Render an HTML page that encodes executor information from the given JSON representations */
  def renderFromJson(summaryJson: JValue, executorsJson: Seq[JValue]): Seq[Node] = {
    val memoryAvailable = Utils.extractLongFromJson(summaryJson, "Memory Available").getOrElse(0L)
    val memoryUsed = Utils.extractLongFromJson(summaryJson, "Memory Used").getOrElse(0L)
    val diskSpaceUsed = Utils.extractLongFromJson(summaryJson, "Disk Space Used").getOrElse(0L)
    val execTable = UIUtils.listingTable[JValue](execHeader, execRow, executorsJson)
    val content =
      <div class="row-fluid">
        <div class="span12">
          <ul class="unstyled">
            <li>
              <strong>Memory:</strong>
              {Utils.bytesToString(memoryAvailable)} Used
              ({Utils.bytesToString(memoryUsed)} Total)
            </li>
            <li><strong>Disk:</strong> {Utils.bytesToString(diskSpaceUsed)} Used</li>
          </ul>
        </div>
      </div>
      <div class = "row">
        <div class="span12">
          {execTable}
        </div>
      </div>;

    UIUtils.headerSparkPage(content, sc, "Executors (" + executorsJson.size + ")", Executors)
  }

  /** Header fields in the executors table */
  private def execHeader = Seq(
    "Executor ID",
    "Address",
    "RDD Blocks",
    "Memory Used",
    "Disk Used",
    "Active Tasks",
    "Failed Tasks",
    "Complete Tasks",
    "Total Tasks",
    "Task Time",
    "Shuffle Read",
    "Shuffle Write")

  /** Render an HTML table row representing an executor from the given JSON representation */
  private def execRow(executorJson: JValue): Seq[Node] = {
    def getString(field: String) = Utils.extractStringFromJson(executorJson, field).getOrElse("")
    def getLong(field: String) = Utils.extractLongFromJson(executorJson, field).getOrElse(0L)
    def getInt(field: String) = Utils.extractIntFromJson(executorJson, field).getOrElse(0)
    val memoryUsed = getLong("Memory Used")
    val memoryAvailable = getLong("Memory Available")
    val diskUsed = getLong("Disk Used")
    val activeTasks = getInt("Active Tasks")
    val failedTasks = getInt("Failed Tasks")
    val completeTasks = getInt("Complete Tasks")
    val totalTasks = activeTasks + failedTasks + completeTasks

    <tr>
      <td>{getString("Executor ID")}</td>
      <td>{getString("Address")}</td>
      <td>{getString("RDD Blocks")}</td>
      <td sorttable_customkey={memoryUsed.toString}>
        {Utils.bytesToString(memoryUsed)} /
        {Utils.bytesToString(memoryAvailable)}
      </td>
      <td sorttable_customkey={diskUsed.toString}>
        {Utils.bytesToString(diskUsed)}
      </td>
      <td>{activeTasks}</td>
      <td>{failedTasks}</td>
      <td>{completeTasks}</td>
      <td>{totalTasks}</td>
      <td>{Utils.msDurationToString(getLong("Task Time"))}</td>
      <td>{Utils.bytesToString(getLong("Shuffle Read"))}</td>
      <td>{Utils.bytesToString(getLong("Shuffle Write"))}</td>
    </tr>
  }

  /**
   * A SparkListener that maintains and logs information to be displayed on the Executors UI.
   *
   * Both intermediate data that resides in memory and persisted data that resides on disk are
   * in JSON format.
   */
  private[spark] class ExecutorsListener(sc: SparkContext) extends SparkListener with Logging {
    var summaryJson: JValue = JNothing
    val executorIdToJson = HashMap[String, JValue]()
    private val logger = new FileLogger("executors-ui")

    /** Return the JSON representation of a newly discovered executor */
    private def newExecutorJson(execId: String): JValue = {
      ("Executor ID" -> execId) ~
      ("Address" -> "") ~
      ("RDD Blocks" -> "") ~
      ("Memory Used" -> 0L) ~
      ("Memory Available" -> 0L) ~
      ("Disk Used" -> 0L) ~
      ("Active Tasks" -> 0) ~
      ("Failed Tasks" -> 0) ~
      ("Complete Tasks" -> 0) ~
      ("Task Time" -> 0L) ~
      ("Shuffle Read" -> 0L) ~
      ("Shuffle Write" -> 0L)
    }

    /**
     * Update the summary and per-executor storage status from SparkEnv. This involves querying
     * the driver and waiting for a reply, and so should be called sparingly.
     */
    def updateStorageStatusFromEnv() {

      // Update summary storage information
      val storageStatusList = sc.getExecutorStorageStatus
      val memoryAvailable = storageStatusList.map(_.maxMem).fold(0L)(_+_)
      val memoryUsed = storageStatusList.map(_.memUsed).fold(0L)(_+_)
      val diskSpaceUsed = storageStatusList.flatMap(_.blocks.values.map(_.diskSize)).fold(0L)(_+_)

      summaryJson =
        ("Memory Available" -> memoryAvailable) ~
        ("Memory Used" -> memoryUsed) ~
        ("Disk Space Used" -> diskSpaceUsed)

      // Update storage status for each executor
      storageStatusList.foreach { status =>
        val execId = status.blockManagerId.executorId
        val address = status.blockManagerId.hostPort
        val rddBlocks = status.blocks.size
        val memoryUsed = status.memUsed
        val memoryAvailable = status.maxMem
        val diskUsed = status.diskUsed
        val json = executorIdToJson.getOrElse(execId, newExecutorJson(execId))
        executorIdToJson(execId) = json.transform {
          case JField("Address", _) => JField("Address", JString(address))
          case JField("RDD Blocks", _) => JField("RDD Blocks", JInt(rddBlocks))
          case JField("Memory Used", _) => JField("Memory Used", JInt(memoryUsed))
          case JField("Memory Available", _) => JField("Memory Available", JInt(memoryAvailable))
          case JField("Disk Used", _) => JField("Disk Used", JInt(diskUsed))
        }
        logJson(executorIdToJson(execId))
      }
    }

    override def onJobStart(jobStart: SparkListenerJobStart) = logger.start()

    override def onJobEnd(jobEnd: SparkListenerJobEnd) = logger.close()

    override def onStageSubmitted(stageStart: SparkListenerStageSubmitted) = {
      updateStorageStatusFromEnv()
      logger.flush()
    }

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) = {
      updateStorageStatusFromEnv()
      logger.flush()
    }

    override def onTaskStart(taskStart: SparkListenerTaskStart) = {
      /**
       * In the local mode, there is a discrepancy between the executor ID according to the
       * task ("localhost") and that according to SparkEnv ("<driver>"). This results in
       * duplicate rows for the same executor. Thus, in this mode, we aggregate these two
       * rows and use the executor ID of "<driver>" to be consistent.
       */
      val execId = if (sc.isLocal) "<driver>" else taskStart.taskInfo.executorId
      val json = executorIdToJson.getOrElse(execId, {
        // The executor ID according to the task is different from that according to SparkEnv
        // This should never happen under normal circumstances...
        logWarning("New executor detected during task start (%s)".format(execId))
        newExecutorJson(execId)
      })
      executorIdToJson(execId) = json.transform {
        case JField("Active Tasks", JInt(s)) => JField("Active Tasks", JInt(s + 1))
      }
      logJson(executorIdToJson(execId))
    }

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd) = {
      val execId = if (sc.isLocal) "<driver>" else taskEnd.taskInfo.executorId
      val newDuration = taskEnd.taskInfo.duration
      var newShuffleRead = 0L
      var newShuffleWrite = 0L
      if (taskEnd.taskMetrics != null) {
        taskEnd.taskMetrics.shuffleReadMetrics.foreach(newShuffleRead += _.remoteBytesRead)
        taskEnd.taskMetrics.shuffleWriteMetrics.foreach(newShuffleWrite += _.shuffleBytesWritten)
      }
      val success = taskEnd.reason match {
        case _: ExceptionFailure => false
        case _ => true
      }
      val json = executorIdToJson.getOrElse(execId, {
        // Information for this executor has vanished over the course of the task execution
        // This should never happen under normal circumstances...
        logWarning("New executor detected during task end (%s)".format(execId))
        newExecutorJson(execId)
      })
      executorIdToJson(execId) = json.transform {
        case JField("Active Tasks", JInt(t)) if t > 0 => JField("Active Tasks", JInt(t - 1))
        case JField("Failed Tasks", JInt(t)) if !success => JField("Failed Tasks", JInt(t + 1))
        case JField("Complete Tasks", JInt(t)) if success => JField("Complete Tasks", JInt(t + 1))
        case JField("Task Time", JInt(s)) => JField("Task Time", JInt(s + newDuration))
        case JField("Shuffle Read", JInt(s)) => JField("Shuffle Read", JInt(s + newShuffleRead))
        case JField("Shuffle Write", JInt(s)) => JField("Shuffle Write", JInt(s + newShuffleWrite))
      }
      logJson(executorIdToJson(execId))
    }

    /** Log summary storage status **/
    def logSummary() = logJson(summaryJson)

    /** Log storage status for the executor with the given ID */
    def logExecutor(execId: String) = logJson(executorIdToJson.getOrElse(execId, JNothing))

    private def logJson(json: JValue) = {
      if (json != JNothing) {
        logger.logLine(compactRender(json))
      }
    }
  }
}
