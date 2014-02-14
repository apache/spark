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

  private val data = new ExecutorsData
  private var _listener: Option[ExecutorsListener] = None
  def listener = _listener.get

  def start() {
    _listener = Some(new ExecutorsListener(this))
    sc.addSparkListener(listener)
  }

  def getHandlers = Seq[(String, Handler)](
    ("/executors", (request: HttpServletRequest) => render(request))
  )

  /**
   * Render an HTML page that encodes executor information
   */
  def render(request: HttpServletRequest): Seq[Node] = {
    listener.getExecutorStorageStatus()
    val summaryJson = data.summaryJson
    val executorsJson = data.executorIdToJson.values.toSeq
    renderFromJson(summaryJson, executorsJson)
  }

  /**
   * Render an HTML page that encodes executor information from the given JSON representations
   */
  def renderFromJson(summaryJson: JValue, executorsJson: Seq[JValue]): Seq[Node] = {
    val maximumMemory = Utils.extractLongFromJson(summaryJson \ "Maximum Memory")
    val memoryUsed = Utils.extractLongFromJson(summaryJson \ "Memory Used")
    val diskSpaceUsed = Utils.extractLongFromJson(summaryJson \ "Disk Space Used")
    val execTable = UIUtils.listingTable[JValue](execHeader, execRow, executorsJson)
    val content =
      <div class="row-fluid">
        <div class="span12">
          <ul class="unstyled">
            <li>
              <strong>Memory:</strong>
              {Utils.bytesToString(memoryUsed)} Used
              ({Utils.bytesToString(maximumMemory)} Total)
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

  /**
   * Render an HTML table row representing an executor from the given JSON representation
   */
  private def execRow(executorJson: JValue): Seq[Node] = {
    def getString(field: String) = Utils.extractStringFromJson(executorJson \ field)
    def getLong(field: String) = Utils.extractLongFromJson(executorJson \ field)
    def getInt(field: String) = Utils.extractIntFromJson(executorJson \ field)

    val maximumMemory = getLong("Maximum Memory")
    val memoryUsed = getLong("Memory Used")
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
        {Utils.bytesToString(maximumMemory)}
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
   * A vessel for storing executors information in JSON format.
   *
   * The source of data can come from two sources: (1) If the job is live, ExecutorsListener
   * invokes ExecutorsData to update its data on certain Spark events. (2) If we are rendering
   * the UI for a job from the past, ExecutorsData parses from these log files the necessary
   * states to reconstruct the UI.
   */
  private[spark] class ExecutorsData extends Logging {
    var summaryJson: JValue = JNothing
    val executorIdToJson = HashMap[String, JValue]()

    /** JSON representation of a newly discovered executor */
    private def newExecutorJson(execId: String): JValue = {
      ("Executor ID" -> execId) ~
      ("Address" -> "") ~
      ("RDD Blocks" -> "") ~
      ("Maximum Memory" -> 0L) ~
      ("Memory Used" -> 0L) ~
      ("Disk Used" -> 0L) ~
      ("Active Tasks" -> 0) ~
      ("Failed Tasks" -> 0) ~
      ("Complete Tasks" -> 0) ~
      ("Task Time" -> 0L) ~
      ("Shuffle Read" -> 0L) ~
      ("Shuffle Write" -> 0L)
    }

    /**
     * Update the summary and per-executor storage status from JSON
     */
    def storageStatusFetch(storageStatusFetchJson: JValue) {
      val storageStatusList =
        Utils.extractListFromJson(storageStatusFetchJson \ "Storage Status List")

      // Update summary storage information
      val maximumMemory = storageStatusList.map { status =>
        Utils.extractLongFromJson(status \ "Maximum Memory")
      }.fold(0L)(_+_)
      val memoryUsed = storageStatusList.map { status =>
        Utils.extractLongFromJson(status \ "Memory Used")
      }.fold(0L)(_+_)
      val diskSpaceUsed = storageStatusList.flatMap { status =>
        Utils.extractListFromJson(status \ "Blocks").map { block =>
          Utils.extractLongFromJson(block \ "Status" \ "Disk Size")
        }
      }.fold(0L)(_+_)

      summaryJson =
        ("Maximum Memory" -> maximumMemory) ~
        ("Memory Used" -> memoryUsed) ~
        ("Disk Space Used" -> diskSpaceUsed)

      // Update storage status for each executor
      storageStatusList.foreach { status =>
        val execId = formatExecutorId(
          Utils.extractStringFromJson(status \ "Block Manager ID" \ "Executor ID"))
        if (execId != "") {
          val address = Utils.extractStringFromJson(status \ "Block Manager ID" \ "Host Port")
          val rddBlocks = Utils.extractListFromJson(status \ "Blocks").size
          val maximumMemory = Utils.extractLongFromJson(status \ "Maximum Memory")
          val memoryUsed = Utils.extractLongFromJson(status \ "Memory Used")
          val diskUsed = Utils.extractLongFromJson(status \ "Disk Used")
          val json = executorIdToJson.getOrElse(execId, newExecutorJson(execId))
          executorIdToJson(execId) = json.transform {
            case JField("Address", _) => JField("Address", JString(address))
            case JField("RDD Blocks", _) => JField("RDD Blocks", JInt(rddBlocks))
            case JField("Maximum Memory", _) => JField("Maximum Memory", JInt(maximumMemory))
            case JField("Memory Used", _) => JField("Memory Used", JInt(memoryUsed))
            case JField("Disk Used", _) => JField("Disk Used", JInt(diskUsed))
          }
        }
      }
    }

    /**
     * Update executor information in response to a task start event
     */
    def taskStart(taskStartJson: JValue) {
      val execId = formatExecutorId(
        Utils.extractStringFromJson(taskStartJson \ "Task Info" \ "Executor ID"))
      val json = executorIdToJson.getOrElse(execId, {
        // The executor ID according to the task is different from that according to SparkEnv
        // This should never happen under normal circumstances...
        logWarning("New executor detected during task start (%s)".format(execId))
        newExecutorJson(execId)
      })
      executorIdToJson(execId) = json.transform {
        case JField("Active Tasks", JInt(s)) => JField("Active Tasks", JInt(s + 1))
      }
    }

    /**
     * Update executor information in response to a task end event
     */
    def taskEnd(taskEndJson: JValue) {
      val execId = formatExecutorId(
        Utils.extractStringFromJson(taskEndJson \ "Task Info" \ "Executor ID"))
      val duration = Utils.extractLongFromJson(taskEndJson \ "Task Info" \ "Duration")
      val taskEndReason = Utils.extractStringFromJson(taskEndJson \ "Task End Reason")
      val failed = taskEndReason == ExceptionFailure.getClass.getSimpleName
      val shuffleRead = Utils.extractLongFromJson(
        taskEndJson \ "Task Metrics" \ "Shuffle Read Metrics" \ "Remote Bytes Read")
      val shuffleWrite = Utils.extractLongFromJson(
        taskEndJson \ "Task Metrics" \ "Shuffle Write Metrics" \ "Shuffle Bytes Written")

      val json = executorIdToJson.getOrElse(execId, {
        // Information for this executor has vanished over the course of the task execution
        // This should never happen under normal circumstances...
        logWarning("New executor detected during task end (%s)".format(execId))
        newExecutorJson(execId)
      })
      executorIdToJson(execId) = json.transform {
        case JField("Active Tasks", JInt(t)) if t > 0 => JField("Active Tasks", JInt(t - 1))
        case JField("Failed Tasks", JInt(t)) if failed => JField("Failed Tasks", JInt(t + 1))
        case JField("Complete Tasks", JInt(t)) if !failed => JField("Complete Tasks", JInt(t + 1))
        case JField("Task Time", JInt(s)) => JField("Task Time", JInt(s + duration))
        case JField("Shuffle Read", JInt(s)) => JField("Shuffle Read", JInt(s + shuffleRead))
        case JField("Shuffle Write", JInt(s)) => JField("Shuffle Write", JInt(s + shuffleWrite))
      }
    }

    /**
     * In the local mode, there is a discrepancy between the executor ID according to the
     * task ("localhost") and that according to SparkEnv ("<driver>"). This results in
     * duplicate rows for the same executor. Thus, in this mode, we aggregate these two
     * rows and use the executor ID of "<driver>" to be consistent.
     */
    private def formatExecutorId(execId: String): String = {
      if (execId == "localhost") "<driver>" else execId
    }
  }

  /**
   * A SparkListener that logs information to be displayed on the Executors UI.
   *
   * Currently, ExecutorsListener only fetches executor storage information from the driver
   * on stage submit and completion. However, this is arbitrary and needs not be true. More
   * specifically, each stage could be very long, in which case it would take a while before
   * this information is updated and persisted to disk. An alternative approach would be to
   * fetch every constant number of task events.
   */
  private[spark] class ExecutorsListener(ui: ExecutorsUI) extends SparkListener with Logging {
    private val logger = new FileLogger("executors-ui")

    /**
     * Invoke SparkEnv to ask the driver for executor storage status. This should be
     * called sparingly.
     */
    def getExecutorStorageStatus() = {
      val storageStatusList = ui.sc.getExecutorStorageStatus
      val event = SparkListenerStorageStatusFetch(storageStatusList)
      onStorageStatusFetch(event)
    }

    override def onJobStart(jobStart: SparkListenerJobStart) = logger.start()

    override def onJobEnd(jobEnd: SparkListenerJobEnd) = logger.close()

    override def onStageSubmitted(stageStart: SparkListenerStageSubmitted) = {
      getExecutorStorageStatus()
      logger.flush()
    }

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) = {
      getExecutorStorageStatus()
      logger.flush()
    }

    override def onTaskStart(taskStart: SparkListenerTaskStart) = {
      val eventJson = taskStart.toJson
      ui.data.taskStart(eventJson)
      logEvent(eventJson)
    }

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd) = {
      val eventJson = taskEnd.toJson
      ui.data.taskEnd(eventJson)
      logEvent(eventJson)
    }

    def onStorageStatusFetch(storageStatusFetch: SparkListenerStorageStatusFetch) = {
      val eventJson = storageStatusFetch.toJson
      ui.data.storageStatusFetch(eventJson)
      logEvent(eventJson)
    }

    private def logEvent(event: JValue) = {
      if (event != JNothing) {
        logger.logLine(compactRender(event))
      }
    }
  }
}
