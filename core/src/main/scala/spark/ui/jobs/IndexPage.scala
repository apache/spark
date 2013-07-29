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

package spark.ui.jobs

import java.util.Date

import javax.servlet.http.HttpServletRequest

import scala.Some
import scala.xml.{NodeSeq, Node}

import spark.scheduler.Stage
import spark.ui.UIUtils._
import spark.ui.Page._
import spark.storage.StorageLevel

/** Page showing list of all ongoing and recently finished stages */
private[spark] class IndexPage(parent: JobProgressUI) {
  def listener = parent.listener
  val dateFmt = parent.dateFmt

  def render(request: HttpServletRequest): Seq[Node] = {
    val activeStages = listener.activeStages.toSeq
    val completedStages = listener.completedStages.reverse.toSeq
    val failedStages = listener.failedStages.reverse.toSeq

    /** Special table which merges two header cells. */
    def stageTable[T](makeRow: T => Seq[Node], rows: Seq[T]): Seq[Node] = {
      <table class="table table-bordered table-striped table-condensed sortable">
        <thead>
          <th>Stage Id</th>
          <th>Origin</th>
          <th>Submitted</th>
          <th>Duration</th>
          <th colspan="2">Tasks: Complete/Total</th>
          <th>Shuffle Activity</th>
          <th>Stored RDD</th>
        </thead>
        <tbody>
          {rows.map(r => makeRow(r))}
        </tbody>
      </table>
    }

    val activeStageTable: NodeSeq = stageTable(stageRow, activeStages)
    val completedStageTable = stageTable(stageRow, completedStages)
    val failedStageTable: NodeSeq = stageTable(stageRow, failedStages)

    val content = <h2>Active Stages</h2> ++ activeStageTable ++
                  <h2>Completed Stages</h2>  ++ completedStageTable ++
                  <h2>Failed Stages</h2>  ++ failedStageTable

    headerSparkPage(content, parent.sc, "Spark Stages", Jobs)
  }

  def getElapsedTime(submitted: Option[Long], completed: Long): String = {
    submitted match {
      case Some(t) => parent.formatDuration(completed - t)
      case _ => "Unknown"
    }
  }

  def makeProgressBar(started: Int, completed: Int, total: Int): Seq[Node] = {
    val completeWidth = "width: %s%%".format((completed.toDouble/total)*100)
    val startWidth = "width: %s%%".format((started.toDouble/total)*100)

    <div class="progress">
      <div class="bar bar-success" style={completeWidth}></div>
      <div class="bar bar-warning" style={startWidth}></div>
    </div>
  }


  def stageRow(s: Stage): Seq[Node] = {
    val submissionTime = s.submissionTime match {
      case Some(t) => dateFmt.format(new Date(t))
      case None => "Unknown"
    }
    val (read, write) = (listener.hasShuffleRead(s.id), listener.hasShuffleWrite(s.id))
    val shuffleInfo = (read, write) match {
      case (true, true) => "Read/Write"
      case (true, false) => "Read"
      case (false, true) => "Write"
      case _ => ""
    }
    val startedTasks = listener.stageToTasksActive.getOrElse(s.id, Seq[Long]()).size
    val completedTasks = listener.stageToTasksComplete.getOrElse(s.id, 0)
    val totalTasks = s.numPartitions

    <tr>
      <td>{s.id}</td>
      <td><a href={"/stages/stage?id=%s".format(s.id)}>{s.name}</a></td>
      <td>{submissionTime}</td>
      <td>{getElapsedTime(s.submissionTime,
             s.completionTime.getOrElse(System.currentTimeMillis()))}</td>
      <td class="progress-cell">{makeProgressBar(startedTasks, completedTasks, totalTasks)}</td>
      <td style="border-left: 0; text-align: center;">{completedTasks} / {totalTasks}
        {listener.stageToTasksFailed.getOrElse(s.id, 0) match {
        case f if f > 0 => "(%s failed)".format(f)
        case _ =>
      }}
      </td>
      <td>{shuffleInfo}</td>
      <td>{if (s.rdd.getStorageLevel != StorageLevel.NONE) {
             <a href={"/storage/rdd?id=%s".format(s.rdd.id)}>
               {Option(s.rdd.name).getOrElse(s.rdd.id)}
             </a>
          }}
      </td>
    </tr>
  }
}
