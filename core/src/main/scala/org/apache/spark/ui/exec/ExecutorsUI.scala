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
import org.apache.spark.scheduler.{SparkListenerJobEnd, SparkListenerTaskStart, SparkListenerTaskEnd, SparkListener}
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.ui.Page.Executors
import org.apache.spark.ui.UIUtils
import org.apache.spark.util.{Utils, FileLogger}

import net.liftweb.json.DefaultFormats
import net.liftweb.json.JsonAST._
import net.liftweb.json.JsonDSL._

private[spark] class ExecutorsUI(val sc: SparkContext) {

  private var _listener: Option[ExecutorsListener] = None
  private implicit val format = DefaultFormats

  def listener = _listener.get

  def start() {
    _listener = Some(new ExecutorsListener)
    sc.addSparkListener(listener)
  }

  def getHandlers = Seq[(String, Handler)](
    ("/executors", (request: HttpServletRequest) => render(request))
  )

  def render(request: HttpServletRequest): Seq[Node] = {
    val storageStatusList = sc.getExecutorStorageStatus

    val maxMem = storageStatusList.map(_.maxMem).fold(0L)(_ + _)
    val memUsed = storageStatusList.map(_.memUsed()).fold(0L)(_ + _)
    val diskSpaceUsed = storageStatusList.flatMap(_.blocks.values.map(_.diskSize)).fold(0L)(_ + _)

    val execHead = Seq("Executor ID", "Address", "RDD blocks", "Memory used", "Disk used",
      "Active tasks", "Failed tasks", "Complete tasks", "Total tasks", "Task Time", "Shuffle Read",
      "Shuffle Write")

    def execRow(kv: Seq[String]) = {
      <tr>
        <td>{kv(0)}</td>
        <td>{kv(1)}</td>
        <td>{kv(2)}</td>
        <td sorttable_customkey={kv(3)}>
          {Utils.bytesToString(kv(3).toLong)} / {Utils.bytesToString(kv(4).toLong)}
        </td>
        <td sorttable_customkey={kv(5)}>
          {Utils.bytesToString(kv(5).toLong)}
        </td>
        <td>{kv(6)}</td>
        <td>{kv(7)}</td>
        <td>{kv(8)}</td>
        <td>{kv(9)}</td>
        <td>{Utils.msDurationToString(kv(10).toLong)}</td>
        <td>{Utils.bytesToString(kv(11).toLong)}</td>
        <td>{Utils.bytesToString(kv(12).toLong)}</td>
      </tr>
    }

    val execInfo = for (statusId <- 0 until storageStatusList.size) yield getExecInfo(statusId)
    val execTable = UIUtils.listingTable(execHead, execRow, execInfo)

    val content =
      <div class="row-fluid">
        <div class="span12">
          <ul class="unstyled">
            <li><strong>Memory:</strong>
              {Utils.bytesToString(memUsed)} Used
              ({Utils.bytesToString(maxMem)} Total) </li>
            <li><strong>Disk:</strong> {Utils.bytesToString(diskSpaceUsed)} Used </li>
          </ul>
        </div>
      </div>
      <div class = "row">
        <div class="span12">
          {execTable}
        </div>
      </div>;

    UIUtils.headerSparkPage(content, sc, "Executors (" + execInfo.size + ")", Executors)
  }

  def getExecInfo(statusId: Int): Seq[String] = {
    val status = sc.getExecutorStorageStatus(statusId)
    val execId = status.blockManagerId.executorId
    val hostPort = status.blockManagerId.hostPort
    val rddBlocks = status.blocks.size.toString
    val memUsed = status.memUsed().toString
    val maxMem = status.maxMem.toString
    val diskUsed = status.diskUsed().toString
    val activeTasks = listener.getJson(execId, "Active Tasks").extract[Int]
    val failedTasks = listener.getJson(execId, "Failed Tasks").extract[Int]
    val completeTasks = listener.getJson(execId, "Complete Tasks").extract[Int]
    val totalTasks = activeTasks + failedTasks + completeTasks
    val totalDuration = listener.getJson(execId, "Task Time").extract[Long]
    val totalShuffleRead = listener.getJson(execId, "Shuffle Read").extract[Long]
    val totalShuffleWrite = listener.getJson(execId, "Shuffle Write").extract[Long]

    Seq(
      execId,
      hostPort,
      rddBlocks,
      memUsed,
      maxMem,
      diskUsed,
      activeTasks.toString,
      failedTasks.toString,
      completeTasks.toString,
      totalTasks.toString,
      totalDuration.toString,
      totalShuffleRead.toString,
      totalShuffleWrite.toString
    )
  }

  private[spark] class ExecutorsListener extends SparkListener with Logging {
    val executorIdToJson = HashMap[String, JValue]()
    val logger = new FileLogger("executors-ui")

    def newJson(execId: String): JValue = {
      ("Executor ID" -> execId) ~
      ("Active Tasks" -> 0) ~
      ("Failed Tasks" -> 0) ~
      ("Complete Tasks" -> 0) ~
      ("Task Time" -> 0L) ~
      ("Shuffle Read" -> 0L) ~
      ("Shuffle Write" -> 0L)
    }

    def getJson(execId: String, field: String): JValue = {
      executorIdToJson.get(execId) match {
        case Some(json) => (json \ field)
        case None => JNothing
      }
    }

    def logJson(json: JValue) = logger.logLine(compactRender(json))

    override def onTaskStart(taskStart: SparkListenerTaskStart) = {
      val eid = taskStart.taskInfo.executorId
      var json = executorIdToJson.getOrElseUpdate(eid, newJson(eid))
      json = json.transform {
        case JField("Active Tasks", JInt(s)) => JField("Active Tasks", JInt(s + 1))
      }
      executorIdToJson(eid) = json
      logJson(json)
    }

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd) = {
      val eid = taskEnd.taskInfo.executorId
      val exception = taskEnd.reason match {
        case _: ExceptionFailure => true
        case _ => false
      }
      val newDuration = taskEnd.taskInfo.duration
      var newShuffleRead = 0
      var newShuffleWrite = 0
      if (taskEnd.taskMetrics != null) {
        taskEnd.taskMetrics.shuffleReadMetrics.foreach(newShuffleRead += _.remoteBytesRead)
        taskEnd.taskMetrics.shuffleWriteMetrics.foreach(newShuffleWrite += _.shuffleBytesWritten)
      }
      var json = executorIdToJson.getOrElseUpdate(eid, newJson(eid))
      json = json.transform {
        case JField("Active Tasks", JInt(s)) if s > 0 => JField("Active Tasks", JInt(s - 1))
        case JField("Failed Tasks", JInt(s)) if exception => JField("Failed Tasks", JInt(s + 1))
        case JField("Complete Tasks", JInt(s)) if !exception =>
          JField("Complete Tasks", JInt(s + 1))
        case JField("Task Time", JInt(s)) => JField("Task Time", JInt(s + newDuration))
        case JField("Shuffle Read", JInt(s)) => JField("Shuffle Read", JInt(s + newShuffleRead))
        case JField("Shuffle Write", JInt(s)) => JField("Shuffle Write", JInt(s + newShuffleWrite))
      }
      executorIdToJson(eid) = json
      logJson(json)
    }

    override def onJobEnd(jobEnd: SparkListenerJobEnd) = logger.close()
  }
}
