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

import scala.xml.{Node, Unparsed}

import org.apache.spark.ui.{ToolTips, WebUIPage, UIUtils}
import org.apache.spark.ui.jobs.UIData._
import org.apache.spark.util.{Utils, Distribution}
import org.apache.spark.scheduler.AccumulableInfo

/** Page showing statistics and task list for a given stage */
private[ui] class StagePage(parent: JobProgressTab) extends WebUIPage("stage") {
  private val listener = parent.listener

  def render(request: HttpServletRequest): Seq[Node] = {
    listener.synchronized {
      val stageId = request.getParameter("id").toInt
      val stageAttemptId = request.getParameter("attempt").toInt
      val stageDataOption = listener.stageIdToData.get((stageId, stageAttemptId))

      if (stageDataOption.isEmpty || stageDataOption.get.taskData.isEmpty) {
        val content =
          <div>
            <h4>Summary Metrics</h4> No tasks have started yet
            <h4>Tasks</h4> No tasks have started yet
          </div>
        return UIUtils.headerSparkPage(
          s"Details for Stage $stageId (Attempt $stageAttemptId)", content, parent)
      }

      val stageData = stageDataOption.get
      val tasks = stageData.taskData.values.toSeq.sortBy(_.taskInfo.launchTime)

      val numCompleted = tasks.count(_.taskInfo.finished)
      val accumulables = listener.stageIdToData((stageId, stageAttemptId)).accumulables
      val hasInput = stageData.inputBytes > 0
      val hasShuffleRead = stageData.shuffleReadBytes > 0
      val hasShuffleWrite = stageData.shuffleWriteBytes > 0
      val hasBytesSpilled = stageData.memoryBytesSpilled > 0 && stageData.diskBytesSpilled > 0

      // scalastyle:off
      val summary =
        <div>
          <ul class="unstyled">
            <li>
              <strong>Total task time across all tasks: </strong>
              {UIUtils.formatDuration(stageData.executorRunTime)}
            </li>
            {if (hasInput)
              <li>
                <strong>Input: </strong>
                {Utils.bytesToString(stageData.inputBytes)}
              </li>
            }
            {if (hasShuffleRead)
              <li>
                <strong>Shuffle read: </strong>
                {Utils.bytesToString(stageData.shuffleReadBytes)}
              </li>
            }
            {if (hasShuffleWrite)
              <li>
                <strong>Shuffle write: </strong>
                {Utils.bytesToString(stageData.shuffleWriteBytes)}
              </li>
            }
            {if (hasBytesSpilled)
            <li>
              <strong>Shuffle spill (memory): </strong>
              {Utils.bytesToString(stageData.memoryBytesSpilled)}
            </li>
            <li>
              <strong>Shuffle spill (disk): </strong>
              {Utils.bytesToString(stageData.diskBytesSpilled)}
            </li>
            }
          </ul>
        </div>
        // scalastyle:on
      val accumulableHeaders: Seq[String] = Seq("Accumulable", "Value")
      def accumulableRow(acc: AccumulableInfo) = <tr><td>{acc.name}</td><td>{acc.value}</td></tr>
      val accumulableTable = UIUtils.listingTable(accumulableHeaders, accumulableRow,
        accumulables.values.toSeq)

      val taskHeaders: Seq[String] =
        Seq(
          "Index", "ID", "Attempt", "Status", "Locality Level", "Executor",
          "Launch Time", "Duration", "GC Time", "Accumulators") ++
        {if (hasInput) Seq("Input") else Nil} ++
        {if (hasShuffleRead) Seq("Shuffle Read")  else Nil} ++
        {if (hasShuffleWrite) Seq("Write Time", "Shuffle Write") else Nil} ++
        {if (hasBytesSpilled) Seq("Shuffle Spill (Memory)", "Shuffle Spill (Disk)") else Nil} ++
        Seq("Errors")

      val taskTable = UIUtils.listingTable(
        taskHeaders, taskRow(hasInput, hasShuffleRead, hasShuffleWrite, hasBytesSpilled), tasks)

      // Excludes tasks which failed and have incomplete metrics
      val validTasks = tasks.filter(t => t.taskInfo.status == "SUCCESS" && t.taskMetrics.isDefined)

      val summaryTable: Option[Seq[Node]] =
        if (validTasks.size == 0) {
          None
        }
        else {
          val serializationTimes = validTasks.map { case TaskUIData(_, metrics, _) =>
            metrics.get.resultSerializationTime.toDouble
          }
          val serializationQuantiles =
            <td>Result serialization time</td> +: Distribution(serializationTimes).
              get.getQuantiles().map(ms => <td>{UIUtils.formatDuration(ms.toLong)}</td>)

          val serviceTimes = validTasks.map { case TaskUIData(_, metrics, _) =>
            metrics.get.executorRunTime.toDouble
          }
          val serviceQuantiles = <td>Duration</td> +: Distribution(serviceTimes).get.getQuantiles()
            .map(ms => <td>{UIUtils.formatDuration(ms.toLong)}</td>)

          val gettingResultTimes = validTasks.map { case TaskUIData(info, _, _) =>
            if (info.gettingResultTime > 0) {
              (info.finishTime - info.gettingResultTime).toDouble
            } else {
              0.0
            }
          }
          val gettingResultQuantiles = <td>Time spent fetching task results</td> +:
            Distribution(gettingResultTimes).get.getQuantiles().map { millis =>
              <td>{UIUtils.formatDuration(millis.toLong)}</td>
            }
          // The scheduler delay includes the network delay to send the task to the worker
          // machine and to send back the result (but not the time to fetch the task result,
          // if it needed to be fetched from the block manager on the worker).
          val schedulerDelays = validTasks.map { case TaskUIData(info, metrics, _) =>
            val totalExecutionTime = {
              if (info.gettingResultTime > 0) {
                (info.gettingResultTime - info.launchTime).toDouble
              } else {
                (info.finishTime - info.launchTime).toDouble
              }
            }
            totalExecutionTime - metrics.get.executorRunTime
          }
          val schedulerDelayTitle = <td><span data-toggle="tooltip"
            title={ToolTips.SCHEDULER_DELAY} data-placement="right">Scheduler delay</span></td>
          val schedulerDelayQuantiles = schedulerDelayTitle +:
            Distribution(schedulerDelays).get.getQuantiles().map { millis =>
              <td>{UIUtils.formatDuration(millis.toLong)}</td>
            }

          def getQuantileCols(data: Seq[Double]) =
            Distribution(data).get.getQuantiles().map(d => <td>{Utils.bytesToString(d.toLong)}</td>)

          val inputSizes = validTasks.map { case TaskUIData(_, metrics, _) =>
            metrics.get.inputMetrics.map(_.bytesRead).getOrElse(0L).toDouble
          }
          val inputQuantiles = <td>Input</td> +: getQuantileCols(inputSizes)

          val shuffleReadSizes = validTasks.map { case TaskUIData(_, metrics, _) =>
            metrics.get.shuffleReadMetrics.map(_.remoteBytesRead).getOrElse(0L).toDouble
          }
          val shuffleReadQuantiles = <td>Shuffle Read (Remote)</td> +:
            getQuantileCols(shuffleReadSizes)

          val shuffleWriteSizes = validTasks.map { case TaskUIData(_, metrics, _) =>
            metrics.get.shuffleWriteMetrics.map(_.shuffleBytesWritten).getOrElse(0L).toDouble
          }
          val shuffleWriteQuantiles = <td>Shuffle Write</td> +: getQuantileCols(shuffleWriteSizes)

          val memoryBytesSpilledSizes = validTasks.map { case TaskUIData(_, metrics, _) =>
            metrics.get.memoryBytesSpilled.toDouble
          }
          val memoryBytesSpilledQuantiles = <td>Shuffle spill (memory)</td> +:
            getQuantileCols(memoryBytesSpilledSizes)

          val diskBytesSpilledSizes = validTasks.map { case TaskUIData(_, metrics, _) =>
            metrics.get.diskBytesSpilled.toDouble
          }
          val diskBytesSpilledQuantiles = <td>Shuffle spill (disk)</td> +:
            getQuantileCols(diskBytesSpilledSizes)

          val listings: Seq[Seq[Node]] = Seq(
            serializationQuantiles,
            serviceQuantiles,
            gettingResultQuantiles,
            schedulerDelayQuantiles,
            if (hasInput) inputQuantiles else Nil,
            if (hasShuffleRead) shuffleReadQuantiles else Nil,
            if (hasShuffleWrite) shuffleWriteQuantiles else Nil,
            if (hasBytesSpilled) memoryBytesSpilledQuantiles else Nil,
            if (hasBytesSpilled) diskBytesSpilledQuantiles else Nil)

          val quantileHeaders = Seq("Metric", "Min", "25th percentile",
            "Median", "75th percentile", "Max")
          def quantileRow(data: Seq[Node]): Seq[Node] = <tr>{data}</tr>
          Some(UIUtils.listingTable(quantileHeaders, quantileRow, listings, fixedWidth = true))
        }

      val executorTable = new ExecutorTable(stageId, stageAttemptId, parent)

      val maybeAccumulableTable: Seq[Node] =
        if (accumulables.size > 0) { <h4>Accumulators</h4> ++ accumulableTable } else Seq()

      val content =
        summary ++
        <h4>Summary Metrics for {numCompleted} Completed Tasks</h4> ++
        <div>{summaryTable.getOrElse("No tasks have reported metrics yet.")}</div> ++
        <h4>Aggregated Metrics by Executor</h4> ++ executorTable.toNodeSeq ++
        maybeAccumulableTable ++
        <h4>Tasks</h4> ++ taskTable

      UIUtils.headerSparkPage("Details for Stage %d".format(stageId), content, parent)
    }
  }

  def taskRow(
      hasInput: Boolean,
      hasShuffleRead: Boolean,
      hasShuffleWrite: Boolean,
      hasBytesSpilled: Boolean)(taskData: TaskUIData): Seq[Node] = {
    taskData match { case TaskUIData(info, metrics, errorMessage) =>
      val duration = if (info.status == "RUNNING") info.timeRunning(System.currentTimeMillis())
        else metrics.map(_.executorRunTime).getOrElse(1L)
      val formatDuration = if (info.status == "RUNNING") UIUtils.formatDuration(duration)
        else metrics.map(m => UIUtils.formatDuration(m.executorRunTime)).getOrElse("")
      val gcTime = metrics.map(_.jvmGCTime).getOrElse(0L)
      val serializationTime = metrics.map(_.resultSerializationTime).getOrElse(0L)

      val maybeInput = metrics.flatMap(_.inputMetrics)
      val inputSortable = maybeInput.map(_.bytesRead.toString).getOrElse("")
      val inputReadable = maybeInput
        .map(m => s"${Utils.bytesToString(m.bytesRead)} (${m.readMethod.toString.toLowerCase()})")
        .getOrElse("")

      val maybeShuffleRead = metrics.flatMap(_.shuffleReadMetrics).map(_.remoteBytesRead)
      val shuffleReadSortable = maybeShuffleRead.map(_.toString).getOrElse("")
      val shuffleReadReadable = maybeShuffleRead.map(Utils.bytesToString).getOrElse("")

      val maybeShuffleWrite =
        metrics.flatMap(_.shuffleWriteMetrics).map(_.shuffleBytesWritten)
      val shuffleWriteSortable = maybeShuffleWrite.map(_.toString).getOrElse("")
      val shuffleWriteReadable = maybeShuffleWrite.map(Utils.bytesToString).getOrElse("")

      val maybeWriteTime = metrics.flatMap(_.shuffleWriteMetrics).map(_.shuffleWriteTime)
      val writeTimeSortable = maybeWriteTime.map(_.toString).getOrElse("")
      val writeTimeReadable = maybeWriteTime.map(t => t / (1000 * 1000)).map { ms =>
        if (ms == 0) "" else UIUtils.formatDuration(ms)
      }.getOrElse("")

      val maybeMemoryBytesSpilled = metrics.map(_.memoryBytesSpilled)
      val memoryBytesSpilledSortable = maybeMemoryBytesSpilled.map(_.toString).getOrElse("")
      val memoryBytesSpilledReadable =
        maybeMemoryBytesSpilled.map(Utils.bytesToString).getOrElse("")

      val maybeDiskBytesSpilled = metrics.map(_.diskBytesSpilled)
      val diskBytesSpilledSortable = maybeDiskBytesSpilled.map(_.toString).getOrElse("")
      val diskBytesSpilledReadable = maybeDiskBytesSpilled.map(Utils.bytesToString).getOrElse("")

      <tr>
        <td>{info.index}</td>
        <td>{info.taskId}</td>
        <td sorttable_customkey={info.attempt.toString}>{
          if (info.speculative) s"${info.attempt} (speculative)" else info.attempt.toString
        }</td>
        <td>{info.status}</td>
        <td>{info.taskLocality}</td>
        <td>{info.host}</td>
        <td>{UIUtils.formatDate(new Date(info.launchTime))}</td>
        <td sorttable_customkey={duration.toString}>
          {formatDuration}
        </td>
        <td sorttable_customkey={gcTime.toString}>
          {if (gcTime > 0) UIUtils.formatDuration(gcTime) else ""}
        </td>
        <td>
          {Unparsed(
            info.accumulables.map{acc => s"${acc.name}: ${acc.update.get}"}.mkString("<br/>")
          )}
        </td>
        <!--
        TODO: Add this back after we add support to hide certain columns.
        <td sorttable_customkey={serializationTime.toString}>
          {if (serializationTime > 0) UIUtils.formatDuration(serializationTime) else ""}
        </td>
        -->
        {if (hasInput) {
          <td sorttable_customkey={inputSortable}>
            {inputReadable}
          </td>
        }}
        {if (hasShuffleRead) {
           <td sorttable_customkey={shuffleReadSortable}>
             {shuffleReadReadable}
           </td>
        }}
        {if (hasShuffleWrite) {
           <td sorttable_customkey={writeTimeSortable}>
             {writeTimeReadable}
           </td>
           <td sorttable_customkey={shuffleWriteSortable}>
             {shuffleWriteReadable}
           </td>
        }}
        {if (hasBytesSpilled) {
          <td sorttable_customkey={memoryBytesSpilledSortable}>
            {memoryBytesSpilledReadable}
          </td>
          <td sorttable_customkey={diskBytesSpilledSortable}>
            {diskBytesSpilledReadable}
          </td>
        }}
        <td>
          {errorMessage.map { e => <pre>{e}</pre> }.getOrElse("")}
        </td>
      </tr>
    }
  }
}
