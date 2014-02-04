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

import java.util.Date

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.spark.{ExceptionFailure}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.ui.UIUtils._
import org.apache.spark.ui.Page._
import org.apache.spark.util.{Utils, Distribution}
import org.apache.spark.scheduler.TaskInfo

/** Page showing statistics and task list for a given stage */
private[spark] class StagePage(parent: JobProgressUI) {
  def listener = parent.listener
  val dateFmt = parent.dateFmt

  def render(request: HttpServletRequest): Seq[Node] = {
    listener.synchronized {
      val stageId = request.getParameter("id").toInt
      val now = System.currentTimeMillis()

      if (!listener.stageIdToTaskInfos.contains(stageId)) {
        val content =
          <div>
            <h4>Summary Metrics</h4> No tasks have started yet
            <h4>Tasks</h4> No tasks have started yet
          </div>
        return headerSparkPage(content, parent.sc, "Details for Stage %s".format(stageId), Stages)
      }

      val tasks = listener.stageIdToTaskInfos(stageId).toSeq.sortBy(_._1.launchTime)

      val numCompleted = tasks.count(_._1.finished)
      val shuffleReadBytes = listener.stageIdToShuffleRead.getOrElse(stageId, 0L)
      val hasShuffleRead = shuffleReadBytes > 0
      val shuffleWriteBytes = listener.stageIdToShuffleWrite.getOrElse(stageId, 0L)
      val hasShuffleWrite = shuffleWriteBytes > 0
      val memoryBytesSpilled = listener.stageIdToMemoryBytesSpilled.getOrElse(stageId, 0L)
      val diskBytesSpilled = listener.stageIdToDiskBytesSpilled.getOrElse(stageId, 0L)
      val hasBytesSpilled = (memoryBytesSpilled > 0 && diskBytesSpilled > 0)

      var activeTime = 0L
      listener.stageIdToTasksActive(stageId).foreach(activeTime += _.timeRunning(now))

      val finishedTasks = listener.stageIdToTaskInfos(stageId).filter(_._1.finished)

      val summary =
        <div>
          <ul class="unstyled">
            <li>
              <strong>Total task time across all tasks: </strong>
              {parent.formatDuration(listener.stageIdToTime.getOrElse(stageId, 0L) + activeTime)}
            </li>
            {if (hasShuffleRead)
              <li>
                <strong>Shuffle read: </strong>
                {Utils.bytesToString(shuffleReadBytes)}
              </li>
            }
            {if (hasShuffleWrite)
              <li>
                <strong>Shuffle write: </strong>
                {Utils.bytesToString(shuffleWriteBytes)}
              </li>
            }
            {if (hasBytesSpilled)
            <li>
              <strong>Shuffle spill (memory): </strong>
              {Utils.bytesToString(memoryBytesSpilled)}
            </li>
            <li>
              <strong>Shuffle spill (disk): </strong>
              {Utils.bytesToString(diskBytesSpilled)}
            </li>
            }
          </ul>
        </div>

      val taskHeaders: Seq[String] =
        Seq("Task Index", "Task ID", "Status", "Locality Level", "Executor", "Launch Time") ++
        Seq("Duration", "GC Time", "Result Ser Time") ++
        {if (hasShuffleRead) Seq("Shuffle Read")  else Nil} ++
        {if (hasShuffleWrite) Seq("Write Time", "Shuffle Write") else Nil} ++
        {if (hasBytesSpilled) Seq("Shuffle Spill (Memory)", "Shuffle Spill (Disk)") else Nil} ++
        Seq("Errors")

      val taskTable = listingTable(taskHeaders, taskRow(hasShuffleRead, hasShuffleWrite, hasBytesSpilled), tasks)

      // Excludes tasks which failed and have incomplete metrics
      val validTasks = tasks.filter(t => t._1.status == "SUCCESS" && (t._2.isDefined))

      val summaryTable: Option[Seq[Node]] =
        if (validTasks.size == 0) {
          None
        }
        else {
          val serializationTimes = validTasks.map{case (info, metrics, exception) =>
            metrics.get.resultSerializationTime.toDouble}
          val serializationQuantiles = "Result serialization time" +: Distribution(serializationTimes).get.getQuantiles().map(
            ms => parent.formatDuration(ms.toLong))

          val serviceTimes = validTasks.map{case (info, metrics, exception) =>
            metrics.get.executorRunTime.toDouble}
          val serviceQuantiles = "Duration" +: Distribution(serviceTimes).get.getQuantiles().map(
            ms => parent.formatDuration(ms.toLong))

          val gettingResultTimes = validTasks.map{case (info, metrics, exception) =>
            if (info.gettingResultTime > 0) {
              (info.finishTime - info.gettingResultTime).toDouble
            } else {
              0.0
            }
          }
          val gettingResultQuantiles = ("Time spent fetching task results" +:
            Distribution(gettingResultTimes).get.getQuantiles().map(
              millis => parent.formatDuration(millis.toLong)))
          // The scheduler delay includes the network delay to send the task to the worker
          // machine and to send back the result (but not the time to fetch the task result,
          // if it needed to be fetched from the block manager on the worker).
          val schedulerDelays = validTasks.map{case (info, metrics, exception) =>
            val totalExecutionTime = {
              if (info.gettingResultTime > 0) {
                (info.gettingResultTime - info.launchTime).toDouble
              } else {
                (info.finishTime - info.launchTime).toDouble
              }
            }
            totalExecutionTime - metrics.get.executorRunTime
          }
          val schedulerDelayQuantiles = ("Scheduler delay" +:
            Distribution(schedulerDelays).get.getQuantiles().map(
              millis => parent.formatDuration(millis.toLong)))

          def getQuantileCols(data: Seq[Double]) =
            Distribution(data).get.getQuantiles().map(d => Utils.bytesToString(d.toLong))

          val shuffleReadSizes = validTasks.map {
            case(info, metrics, exception) =>
              metrics.get.shuffleReadMetrics.map(_.remoteBytesRead).getOrElse(0L).toDouble
          }
          val shuffleReadQuantiles = "Shuffle Read (Remote)" +: getQuantileCols(shuffleReadSizes)

          val shuffleWriteSizes = validTasks.map {
            case(info, metrics, exception) =>
              metrics.get.shuffleWriteMetrics.map(_.shuffleBytesWritten).getOrElse(0L).toDouble
          }
          val shuffleWriteQuantiles = "Shuffle Write" +: getQuantileCols(shuffleWriteSizes)

          val memoryBytesSpilledSizes = validTasks.map {
            case(info, metrics, exception) =>
              metrics.get.memoryBytesSpilled.toDouble
          }
          val memoryBytesSpilledQuantiles = "Shuffle spill (memory)" +:
            getQuantileCols(memoryBytesSpilledSizes)

          val diskBytesSpilledSizes = validTasks.map {
            case(info, metrics, exception) =>
              metrics.get.diskBytesSpilled.toDouble
          }
          val diskBytesSpilledQuantiles = "Shuffle spill (disk)" +:
            getQuantileCols(diskBytesSpilledSizes)

          val listings: Seq[Seq[String]] = Seq(
            serializationQuantiles,
            serviceQuantiles,
            gettingResultQuantiles,
            schedulerDelayQuantiles,
            if (hasShuffleRead) shuffleReadQuantiles else Nil,
            if (hasShuffleWrite) shuffleWriteQuantiles else Nil,
            if (hasBytesSpilled) memoryBytesSpilledQuantiles else Nil,
            if (hasBytesSpilled) diskBytesSpilledQuantiles else Nil)

          val quantileHeaders = Seq("Metric", "Min", "25th percentile",
            "Median", "75th percentile", "Max")
          def quantileRow(data: Seq[String]): Seq[Node] = <tr> {data.map(d => <td>{d}</td>)} </tr>
          Some(listingTable(quantileHeaders, quantileRow, listings, fixedWidth = true))
        }
      val executorTable = new ExecutorTable(parent, stageId)
      val content =
        summary ++
        <h4>Summary Metrics for {numCompleted} Completed Tasks</h4> ++
        <div>{summaryTable.getOrElse("No tasks have reported metrics yet.")}</div> ++
        <h4>Aggregated Metrics by Executor</h4> ++ executorTable.toNodeSeq() ++
        <h4>Tasks</h4> ++ taskTable

      headerSparkPage(content, parent.sc, "Details for Stage %d".format(stageId), Stages)
    }
  }

  def taskRow(shuffleRead: Boolean, shuffleWrite: Boolean, bytesSpilled: Boolean)
             (taskData: (TaskInfo, Option[TaskMetrics], Option[ExceptionFailure])): Seq[Node] = {
    def fmtStackTrace(trace: Seq[StackTraceElement]): Seq[Node] =
      trace.map(e => <span style="display:block;">{e.toString}</span>)
    val (info, metrics, exception) = taskData

    val duration = if (info.status == "RUNNING") info.timeRunning(System.currentTimeMillis())
      else metrics.map(m => m.executorRunTime).getOrElse(1)
    val formatDuration = if (info.status == "RUNNING") parent.formatDuration(duration)
      else metrics.map(m => parent.formatDuration(m.executorRunTime)).getOrElse("")
    val gcTime = metrics.map(m => m.jvmGCTime).getOrElse(0L)
    val serializationTime = metrics.map(m => m.resultSerializationTime).getOrElse(0L)

    val maybeShuffleRead = metrics.flatMap{m => m.shuffleReadMetrics}.map{s => s.remoteBytesRead}
    val shuffleReadSortable = maybeShuffleRead.map(_.toString).getOrElse("")
    val shuffleReadReadable = maybeShuffleRead.map{Utils.bytesToString(_)}.getOrElse("")

    val maybeShuffleWrite = metrics.flatMap{m => m.shuffleWriteMetrics}.map{s => s.shuffleBytesWritten}
    val shuffleWriteSortable = maybeShuffleWrite.map(_.toString).getOrElse("")
    val shuffleWriteReadable = maybeShuffleWrite.map{Utils.bytesToString(_)}.getOrElse("")

    val maybeWriteTime = metrics.flatMap{m => m.shuffleWriteMetrics}.map{s => s.shuffleWriteTime}
    val writeTimeSortable = maybeWriteTime.map(_.toString).getOrElse("")
    val writeTimeReadable = maybeWriteTime.map{ t => t / (1000 * 1000)}.map{ ms =>
      if (ms == 0) "" else parent.formatDuration(ms)}.getOrElse("")

    val maybeMemoryBytesSpilled = metrics.map{m => m.memoryBytesSpilled}
    val memoryBytesSpilledSortable = maybeMemoryBytesSpilled.map(_.toString).getOrElse("")
    val memoryBytesSpilledReadable = maybeMemoryBytesSpilled.map{Utils.bytesToString(_)}.getOrElse("")

    val maybeDiskBytesSpilled = metrics.map{m => m.diskBytesSpilled}
    val diskBytesSpilledSortable = maybeDiskBytesSpilled.map(_.toString).getOrElse("")
    val diskBytesSpilledReadable = maybeDiskBytesSpilled.map{Utils.bytesToString(_)}.getOrElse("")

    <tr>
      <td>{info.index}</td>
      <td>{info.taskId}</td>
      <td>{info.status}</td>
      <td>{info.taskLocality}</td>
      <td>{info.host}</td>
      <td>{dateFmt.format(new Date(info.launchTime))}</td>
      <td sorttable_customkey={duration.toString}>
        {formatDuration}
      </td>
      <td sorttable_customkey={gcTime.toString}>
        {if (gcTime > 0) parent.formatDuration(gcTime) else ""}
      </td>
      <td sorttable_customkey={serializationTime.toString}>
        {if (serializationTime > 0) parent.formatDuration(serializationTime) else ""}
      </td>
      {if (shuffleRead) {
         <td sorttable_customkey={shuffleReadSortable}>
           {shuffleReadReadable}
         </td>
      }}
      {if (shuffleWrite) {
         <td sorttable_customkey={writeTimeSortable}>
           {writeTimeReadable}
         </td>
         <td sorttable_customkey={shuffleWriteSortable}>
           {shuffleWriteReadable}
         </td>
      }}
      {if (bytesSpilled) {
        <td sorttable_customkey={memoryBytesSpilledSortable}>
          {memoryBytesSpilledReadable}
        </td>
        <td sorttable_customkey={diskBytesSpilledSortable}>
          {diskBytesSpilledReadable}
        </td>
      }}
      <td>{exception.map(e =>
        <span>
          {e.className} ({e.description})<br/>
          {fmtStackTrace(e.stackTrace)}
        </span>).getOrElse("")}
      </td>
    </tr>
  }
}
