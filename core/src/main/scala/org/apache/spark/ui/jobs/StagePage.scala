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

import java.net.URLEncoder
import java.util.Date
import java.util.concurrent.TimeUnit
import javax.servlet.http.HttpServletRequest

import scala.collection.mutable.{HashMap, HashSet}
import scala.xml.{Node, Unparsed}

import org.apache.commons.lang3.StringEscapeUtils

import org.apache.spark.scheduler.TaskLocality
import org.apache.spark.status._
import org.apache.spark.status.api.v1._
import org.apache.spark.ui._
import org.apache.spark.util.Utils

/** Page showing statistics and task list for a given stage */
private[ui] class StagePage(parent: StagesTab, store: AppStatusStore) extends WebUIPage("stage") {
  import ApiHelper._

  private val TIMELINE_LEGEND = {
    <div class="legend-area">
      <svg>
        {
          val legendPairs = List(("scheduler-delay-proportion", "Scheduler Delay"),
            ("deserialization-time-proportion", "Task Deserialization Time"),
            ("shuffle-read-time-proportion", "Shuffle Read Time"),
            ("executor-runtime-proportion", "Executor Computing Time"),
            ("shuffle-write-time-proportion", "Shuffle Write Time"),
            ("serialization-time-proportion", "Result Serialization Time"),
            ("getting-result-time-proportion", "Getting Result Time"))

          legendPairs.zipWithIndex.map {
            case ((classAttr, name), index) =>
              <rect x={5 + (index / 3) * 210 + "px"} y={10 + (index % 3) * 15 + "px"}
                width="10px" height="10px" class={classAttr}></rect>
                <text x={25 + (index / 3) * 210 + "px"}
                  y={20 + (index % 3) * 15 + "px"}>{name}</text>
          }
        }
      </svg>
    </div>
  }

  // TODO: We should consider increasing the number of this parameter over time
  // if we find that it's okay.
  private val MAX_TIMELINE_TASKS = parent.conf.getInt("spark.ui.timeline.tasks.maximum", 1000)

  private def getLocalitySummaryString(localitySummary: Map[String, Long]): String = {
    val names = Map(
      TaskLocality.PROCESS_LOCAL.toString() -> "Process local",
      TaskLocality.NODE_LOCAL.toString() -> "Node local",
      TaskLocality.RACK_LOCAL.toString() -> "Rack local",
      TaskLocality.ANY.toString() -> "Any")
    val localityNamesAndCounts = names.flatMap { case (key, name) =>
      localitySummary.get(key).map { count =>
        s"$name: $count"
      }
    }.toSeq
    localityNamesAndCounts.sorted.mkString("; ")
  }

  def render(request: HttpServletRequest): Seq[Node] = {
    // stripXSS is called first to remove suspicious characters used in XSS attacks
    val parameterId = UIUtils.stripXSS(request.getParameter("id"))
    require(parameterId != null && parameterId.nonEmpty, "Missing id parameter")

    val parameterAttempt = UIUtils.stripXSS(request.getParameter("attempt"))
    require(parameterAttempt != null && parameterAttempt.nonEmpty, "Missing attempt parameter")

    val parameterTaskPage = UIUtils.stripXSS(request.getParameter("task.page"))
    val parameterTaskSortColumn = UIUtils.stripXSS(request.getParameter("task.sort"))
    val parameterTaskSortDesc = UIUtils.stripXSS(request.getParameter("task.desc"))
    val parameterTaskPageSize = UIUtils.stripXSS(request.getParameter("task.pageSize"))
    val parameterTaskPrevPageSize = UIUtils.stripXSS(request.getParameter("task.prevPageSize"))

    val taskPage = Option(parameterTaskPage).map(_.toInt).getOrElse(1)
    val taskSortColumn = Option(parameterTaskSortColumn).map { sortColumn =>
      UIUtils.decodeURLParameter(sortColumn)
    }.getOrElse("Index")
    val taskSortDesc = Option(parameterTaskSortDesc).map(_.toBoolean).getOrElse(false)
    val taskPageSize = Option(parameterTaskPageSize).map(_.toInt).getOrElse(100)
    val taskPrevPageSize = Option(parameterTaskPrevPageSize).map(_.toInt).getOrElse(taskPageSize)

    val stageId = parameterId.toInt
    val stageAttemptId = parameterAttempt.toInt

    val stageHeader = s"Details for Stage $stageId (Attempt $stageAttemptId)"
    val stageData = parent.store
      .asOption(parent.store.stageAttempt(stageId, stageAttemptId, details = false))
      .getOrElse {
        val content =
          <div id="no-info">
            <p>No information to display for Stage {stageId} (Attempt {stageAttemptId})</p>
          </div>
        return UIUtils.headerSparkPage(request, stageHeader, content, parent)
      }

    val localitySummary = store.localitySummary(stageData.stageId, stageData.attemptId)

    val totalTasks = stageData.numActiveTasks + stageData.numCompleteTasks +
      stageData.numFailedTasks + stageData.numKilledTasks
    if (totalTasks == 0) {
      val content =
        <div>
          <h4>Summary Metrics</h4> No tasks have started yet
          <h4>Tasks</h4> No tasks have started yet
        </div>
      return UIUtils.headerSparkPage(request, stageHeader, content, parent)
    }

    val storedTasks = store.taskCount(stageData.stageId, stageData.attemptId)
    val numCompleted = stageData.numCompleteTasks
    val totalTasksNumStr = if (totalTasks == storedTasks) {
      s"$totalTasks"
    } else {
      s"$totalTasks, showing $storedTasks"
    }

    val summary =
      <div>
        <ul class="unstyled">
          <li>
            <strong>Total Time Across All Tasks: </strong>
            {UIUtils.formatDuration(stageData.executorRunTime)}
          </li>
          <li>
            <strong>Locality Level Summary: </strong>
            {getLocalitySummaryString(localitySummary)}
          </li>
          {if (hasInput(stageData)) {
            <li>
              <strong>Input Size / Records: </strong>
              {s"${Utils.bytesToString(stageData.inputBytes)} / ${stageData.inputRecords}"}
            </li>
          }}
          {if (hasOutput(stageData)) {
            <li>
              <strong>Output: </strong>
              {s"${Utils.bytesToString(stageData.outputBytes)} / ${stageData.outputRecords}"}
            </li>
          }}
          {if (hasShuffleRead(stageData)) {
            <li>
              <strong>Shuffle Read: </strong>
              {s"${Utils.bytesToString(stageData.shuffleReadBytes)} / " +
               s"${stageData.shuffleReadRecords}"}
            </li>
          }}
          {if (hasShuffleWrite(stageData)) {
            <li>
              <strong>Shuffle Write: </strong>
               {s"${Utils.bytesToString(stageData.shuffleWriteBytes)} / " +
               s"${stageData.shuffleWriteRecords}"}
            </li>
          }}
          {if (hasBytesSpilled(stageData)) {
            <li>
              <strong>Shuffle Spill (Memory): </strong>
              {Utils.bytesToString(stageData.memoryBytesSpilled)}
            </li>
            <li>
              <strong>Shuffle Spill (Disk): </strong>
              {Utils.bytesToString(stageData.diskBytesSpilled)}
            </li>
          }}
        </ul>
      </div>

    val showAdditionalMetrics =
      <div>
        <span class="expand-additional-metrics">
          <span class="expand-additional-metrics-arrow arrow-closed"></span>
          <a>Show Additional Metrics</a>
        </span>
        <div class="additional-metrics collapsed">
          <ul>
            <li>
                <input type="checkbox" id="select-all-metrics"/>
                <span class="additional-metric-title"><em>(De)select All</em></span>
            </li>
            <li>
              <span data-toggle="tooltip"
                    title={ToolTips.SCHEDULER_DELAY} data-placement="right">
                <input type="checkbox" name={TaskDetailsClassNames.SCHEDULER_DELAY}/>
                <span class="additional-metric-title">Scheduler Delay</span>
              </span>
            </li>
            <li>
              <span data-toggle="tooltip"
                    title={ToolTips.TASK_DESERIALIZATION_TIME} data-placement="right">
                <input type="checkbox" name={TaskDetailsClassNames.TASK_DESERIALIZATION_TIME}/>
                <span class="additional-metric-title">Task Deserialization Time</span>
              </span>
            </li>
            {if (stageData.shuffleReadBytes > 0) {
              <li>
                <span data-toggle="tooltip"
                      title={ToolTips.SHUFFLE_READ_BLOCKED_TIME} data-placement="right">
                  <input type="checkbox" name={TaskDetailsClassNames.SHUFFLE_READ_BLOCKED_TIME}/>
                  <span class="additional-metric-title">Shuffle Read Blocked Time</span>
                </span>
              </li>
              <li>
                <span data-toggle="tooltip"
                      title={ToolTips.SHUFFLE_READ_REMOTE_SIZE} data-placement="right">
                  <input type="checkbox" name={TaskDetailsClassNames.SHUFFLE_READ_REMOTE_SIZE}/>
                  <span class="additional-metric-title">Shuffle Remote Reads</span>
                </span>
              </li>
            }}
            <li>
              <span data-toggle="tooltip"
                    title={ToolTips.RESULT_SERIALIZATION_TIME} data-placement="right">
                <input type="checkbox" name={TaskDetailsClassNames.RESULT_SERIALIZATION_TIME}/>
                <span class="additional-metric-title">Result Serialization Time</span>
              </span>
            </li>
            <li>
              <span data-toggle="tooltip"
                    title={ToolTips.GETTING_RESULT_TIME} data-placement="right">
                <input type="checkbox" name={TaskDetailsClassNames.GETTING_RESULT_TIME}/>
                <span class="additional-metric-title">Getting Result Time</span>
              </span>
            </li>
            <li>
              <span data-toggle="tooltip"
                    title={ToolTips.PEAK_EXECUTION_MEMORY} data-placement="right">
                <input type="checkbox" name={TaskDetailsClassNames.PEAK_EXECUTION_MEMORY}/>
                <span class="additional-metric-title">Peak Execution Memory</span>
              </span>
            </li>
          </ul>
        </div>
      </div>

    val stageGraph = parent.store.asOption(parent.store.operationGraphForStage(stageId))
    val dagViz = UIUtils.showDagVizForStage(stageId, stageGraph)

    val accumulableHeaders: Seq[String] = Seq("Accumulable", "Value")
    def accumulableRow(acc: AccumulableInfo): Seq[Node] = {
      if (acc.name != null && acc.value != null) {
        <tr><td>{acc.name}</td><td>{acc.value}</td></tr>
      } else {
        Nil
      }
    }
    val accumulableTable = UIUtils.listingTable(
      accumulableHeaders,
      accumulableRow,
      stageData.accumulatorUpdates.toSeq)

    val page: Int = {
      // If the user has changed to a larger page size, then go to page 1 in order to avoid
      // IndexOutOfBoundsException.
      if (taskPageSize <= taskPrevPageSize) {
        taskPage
      } else {
        1
      }
    }
    val currentTime = System.currentTimeMillis()
    val (taskTable, taskTableHTML) = try {
      val _taskTable = new TaskPagedTable(
        stageData,
        UIUtils.prependBaseUri(request, parent.basePath) +
          s"/stages/stage/?id=${stageId}&attempt=${stageAttemptId}",
        currentTime,
        pageSize = taskPageSize,
        sortColumn = taskSortColumn,
        desc = taskSortDesc,
        store = parent.store
      )
      (_taskTable, _taskTable.table(page))
    } catch {
      case e @ (_ : IllegalArgumentException | _ : IndexOutOfBoundsException) =>
        val errorMessage =
          <div class="alert alert-error">
            <p>Error while rendering stage table:</p>
            <pre>
              {Utils.exceptionString(e)}
            </pre>
          </div>
        (null, errorMessage)
    }

    val jsForScrollingDownToTaskTable =
      <script>
        {Unparsed {
          """
            |$(function() {
            |  if (/.*&task.sort=.*$/.test(location.search)) {
            |    var topOffset = $("#tasks-section").offset().top;
            |    $("html,body").animate({scrollTop: topOffset}, 200);
            |  }
            |});
          """.stripMargin
          }
        }
      </script>

    val metricsSummary = store.taskSummary(stageData.stageId, stageData.attemptId,
      Array(0, 0.25, 0.5, 0.75, 1.0))

    val summaryTable = metricsSummary.map { metrics =>
      def timeQuantiles(data: IndexedSeq[Double]): Seq[Node] = {
        data.map { millis =>
          <td>{UIUtils.formatDuration(millis.toLong)}</td>
        }
      }

      def sizeQuantiles(data: IndexedSeq[Double]): Seq[Node] = {
        data.map { size =>
          <td>{Utils.bytesToString(size.toLong)}</td>
        }
      }

      def sizeQuantilesWithRecords(
          data: IndexedSeq[Double],
          records: IndexedSeq[Double]) : Seq[Node] = {
        data.zip(records).map { case (d, r) =>
          <td>{s"${Utils.bytesToString(d.toLong)} / ${r.toLong}"}</td>
        }
      }

      def titleCell(title: String, tooltip: String): Seq[Node] = {
        <td>
          <span data-toggle="tooltip" title={tooltip} data-placement="right">
            {title}
          </span>
        </td>
      }

      def simpleTitleCell(title: String): Seq[Node] = <td>{title}</td>

      val deserializationQuantiles = titleCell("Task Deserialization Time",
        ToolTips.TASK_DESERIALIZATION_TIME) ++ timeQuantiles(metrics.executorDeserializeTime)

      val serviceQuantiles = simpleTitleCell("Duration") ++ timeQuantiles(metrics.executorRunTime)

      val gcQuantiles = titleCell("GC Time", ToolTips.GC_TIME) ++ timeQuantiles(metrics.jvmGcTime)

      val serializationQuantiles = titleCell("Result Serialization Time",
        ToolTips.RESULT_SERIALIZATION_TIME) ++ timeQuantiles(metrics.resultSerializationTime)

      val gettingResultQuantiles = titleCell("Getting Result Time", ToolTips.GETTING_RESULT_TIME) ++
        timeQuantiles(metrics.gettingResultTime)

      val peakExecutionMemoryQuantiles = titleCell("Peak Execution Memory",
        ToolTips.PEAK_EXECUTION_MEMORY) ++ sizeQuantiles(metrics.peakExecutionMemory)

      // The scheduler delay includes the network delay to send the task to the worker
      // machine and to send back the result (but not the time to fetch the task result,
      // if it needed to be fetched from the block manager on the worker).
      val schedulerDelayQuantiles = titleCell("Scheduler Delay", ToolTips.SCHEDULER_DELAY) ++
        timeQuantiles(metrics.schedulerDelay)

      def inputQuantiles: Seq[Node] = {
        simpleTitleCell("Input Size / Records") ++
          sizeQuantilesWithRecords(metrics.inputMetrics.bytesRead, metrics.inputMetrics.recordsRead)
      }

      def outputQuantiles: Seq[Node] = {
        simpleTitleCell("Output Size / Records") ++
          sizeQuantilesWithRecords(metrics.outputMetrics.bytesWritten,
            metrics.outputMetrics.recordsWritten)
      }

      def shuffleReadBlockedQuantiles: Seq[Node] = {
        titleCell("Shuffle Read Blocked Time", ToolTips.SHUFFLE_READ_BLOCKED_TIME) ++
          timeQuantiles(metrics.shuffleReadMetrics.fetchWaitTime)
      }

      def shuffleReadTotalQuantiles: Seq[Node] = {
        titleCell("Shuffle Read Size / Records", ToolTips.SHUFFLE_READ) ++
          sizeQuantilesWithRecords(metrics.shuffleReadMetrics.readBytes,
            metrics.shuffleReadMetrics.readRecords)
      }

      def shuffleReadRemoteQuantiles: Seq[Node] = {
        titleCell("Shuffle Remote Reads", ToolTips.SHUFFLE_READ_REMOTE_SIZE) ++
          sizeQuantiles(metrics.shuffleReadMetrics.remoteBytesRead)
      }

      def shuffleWriteQuantiles: Seq[Node] = {
        simpleTitleCell("Shuffle Write Size / Records") ++
          sizeQuantilesWithRecords(metrics.shuffleWriteMetrics.writeBytes,
            metrics.shuffleWriteMetrics.writeRecords)
      }

      def memoryBytesSpilledQuantiles: Seq[Node] = {
        simpleTitleCell("Shuffle spill (memory)") ++ sizeQuantiles(metrics.memoryBytesSpilled)
      }

      def diskBytesSpilledQuantiles: Seq[Node] = {
        simpleTitleCell("Shuffle spill (disk)") ++ sizeQuantiles(metrics.diskBytesSpilled)
      }

      val listings: Seq[Seq[Node]] = Seq(
        <tr>{serviceQuantiles}</tr>,
        <tr class={TaskDetailsClassNames.SCHEDULER_DELAY}>{schedulerDelayQuantiles}</tr>,
        <tr class={TaskDetailsClassNames.TASK_DESERIALIZATION_TIME}>
          {deserializationQuantiles}
        </tr>
        <tr>{gcQuantiles}</tr>,
        <tr class={TaskDetailsClassNames.RESULT_SERIALIZATION_TIME}>
          {serializationQuantiles}
        </tr>,
        <tr class={TaskDetailsClassNames.GETTING_RESULT_TIME}>{gettingResultQuantiles}</tr>,
        <tr class={TaskDetailsClassNames.PEAK_EXECUTION_MEMORY}>
          {peakExecutionMemoryQuantiles}
        </tr>,
        if (hasInput(stageData)) <tr>{inputQuantiles}</tr> else Nil,
        if (hasOutput(stageData)) <tr>{outputQuantiles}</tr> else Nil,
        if (hasShuffleRead(stageData)) {
          <tr class={TaskDetailsClassNames.SHUFFLE_READ_BLOCKED_TIME}>
            {shuffleReadBlockedQuantiles}
          </tr>
          <tr>{shuffleReadTotalQuantiles}</tr>
          <tr class={TaskDetailsClassNames.SHUFFLE_READ_REMOTE_SIZE}>
            {shuffleReadRemoteQuantiles}
          </tr>
        } else {
          Nil
        },
        if (hasShuffleWrite(stageData)) <tr>{shuffleWriteQuantiles}</tr> else Nil,
        if (hasBytesSpilled(stageData)) <tr>{memoryBytesSpilledQuantiles}</tr> else Nil,
        if (hasBytesSpilled(stageData)) <tr>{diskBytesSpilledQuantiles}</tr> else Nil)

      val quantileHeaders = Seq("Metric", "Min", "25th percentile", "Median", "75th percentile",
        "Max")
      // The summary table does not use CSS to stripe rows, which doesn't work with hidden
      // rows (instead, JavaScript in table.js is used to stripe the non-hidden rows).
      UIUtils.listingTable(
        quantileHeaders,
        identity[Seq[Node]],
        listings,
        fixedWidth = true,
        id = Some("task-summary-table"),
        stripeRowsWithCss = false)
    }

    val executorTable = new ExecutorTable(stageData, parent.store)

    val maybeAccumulableTable: Seq[Node] =
      if (hasAccumulators(stageData)) { <h4>Accumulators</h4> ++ accumulableTable } else Seq()

    val aggMetrics =
      <span class="collapse-aggregated-metrics collapse-table"
            onClick="collapseTable('collapse-aggregated-metrics','aggregated-metrics')">
        <h4>
          <span class="collapse-table-arrow arrow-open"></span>
          <a>Aggregated Metrics by Executor</a>
        </h4>
      </span>
      <div class="aggregated-metrics collapsible-table">
        {executorTable.toNodeSeq}
      </div>

    val content =
      summary ++
      dagViz ++
      showAdditionalMetrics ++
      makeTimeline(
        // Only show the tasks in the table
        Option(taskTable).map(_.dataSource.tasks).getOrElse(Nil),
        currentTime) ++
      <h4>Summary Metrics for <a href="#tasks-section">{numCompleted} Completed Tasks</a></h4> ++
      <div>{summaryTable.getOrElse("No tasks have reported metrics yet.")}</div> ++
      aggMetrics ++
      maybeAccumulableTable ++
      <span id="tasks-section" class="collapse-aggregated-tasks collapse-table"
          onClick="collapseTable('collapse-aggregated-tasks','aggregated-tasks')">
        <h4>
          <span class="collapse-table-arrow arrow-open"></span>
          <a>Tasks ({totalTasksNumStr})</a>
        </h4>
      </span> ++
      <div class="aggregated-tasks collapsible-table">
        {taskTableHTML ++ jsForScrollingDownToTaskTable}
      </div>
    UIUtils.headerSparkPage(request, stageHeader, content, parent, showVisualization = true)
  }

  def makeTimeline(tasks: Seq[TaskData], currentTime: Long): Seq[Node] = {
    val executorsSet = new HashSet[(String, String)]
    var minLaunchTime = Long.MaxValue
    var maxFinishTime = Long.MinValue

    val executorsArrayStr =
      tasks.sortBy(-_.launchTime.getTime()).take(MAX_TIMELINE_TASKS).map { taskInfo =>
        val executorId = taskInfo.executorId
        val host = taskInfo.host
        executorsSet += ((executorId, host))

        val launchTime = taskInfo.launchTime.getTime()
        val finishTime = taskInfo.duration.map(taskInfo.launchTime.getTime() + _)
          .getOrElse(currentTime)
        val totalExecutionTime = finishTime - launchTime
        minLaunchTime = launchTime.min(minLaunchTime)
        maxFinishTime = finishTime.max(maxFinishTime)

        def toProportion(time: Long) = time.toDouble / totalExecutionTime * 100

        val metricsOpt = taskInfo.taskMetrics
        val shuffleReadTime =
          metricsOpt.map(_.shuffleReadMetrics.fetchWaitTime).getOrElse(0L)
        val shuffleReadTimeProportion = toProportion(shuffleReadTime)
        val shuffleWriteTime =
          (metricsOpt.map(_.shuffleWriteMetrics.writeTime).getOrElse(0L) / 1e6).toLong
        val shuffleWriteTimeProportion = toProportion(shuffleWriteTime)

        val serializationTime = metricsOpt.map(_.resultSerializationTime).getOrElse(0L)
        val serializationTimeProportion = toProportion(serializationTime)
        val deserializationTime = metricsOpt.map(_.executorDeserializeTime).getOrElse(0L)
        val deserializationTimeProportion = toProportion(deserializationTime)
        val gettingResultTime = AppStatusUtils.gettingResultTime(taskInfo)
        val gettingResultTimeProportion = toProportion(gettingResultTime)
        val schedulerDelay = AppStatusUtils.schedulerDelay(taskInfo)
        val schedulerDelayProportion = toProportion(schedulerDelay)

        val executorOverhead = serializationTime + deserializationTime
        val executorRunTime = if (taskInfo.duration.isDefined) {
          math.max(totalExecutionTime - executorOverhead - gettingResultTime - schedulerDelay, 0)
        } else {
          metricsOpt.map(_.executorRunTime).getOrElse(
            math.max(totalExecutionTime - executorOverhead - gettingResultTime - schedulerDelay, 0))
        }
        val executorComputingTime = executorRunTime - shuffleReadTime - shuffleWriteTime
        val executorComputingTimeProportion =
          math.max(100 - schedulerDelayProportion - shuffleReadTimeProportion -
            shuffleWriteTimeProportion - serializationTimeProportion -
            deserializationTimeProportion - gettingResultTimeProportion, 0)

        val schedulerDelayProportionPos = 0
        val deserializationTimeProportionPos =
          schedulerDelayProportionPos + schedulerDelayProportion
        val shuffleReadTimeProportionPos =
          deserializationTimeProportionPos + deserializationTimeProportion
        val executorRuntimeProportionPos =
          shuffleReadTimeProportionPos + shuffleReadTimeProportion
        val shuffleWriteTimeProportionPos =
          executorRuntimeProportionPos + executorComputingTimeProportion
        val serializationTimeProportionPos =
          shuffleWriteTimeProportionPos + shuffleWriteTimeProportion
        val gettingResultTimeProportionPos =
          serializationTimeProportionPos + serializationTimeProportion

        val index = taskInfo.index
        val attempt = taskInfo.attempt

        val svgTag =
          if (totalExecutionTime == 0) {
            // SPARK-8705: Avoid invalid attribute error in JavaScript if execution time is 0
            """<svg class="task-assignment-timeline-duration-bar"></svg>"""
          } else {
           s"""<svg class="task-assignment-timeline-duration-bar">
                 |<rect class="scheduler-delay-proportion"
                   |x="$schedulerDelayProportionPos%" y="0px" height="26px"
                   |width="$schedulerDelayProportion%"></rect>
                 |<rect class="deserialization-time-proportion"
                   |x="$deserializationTimeProportionPos%" y="0px" height="26px"
                   |width="$deserializationTimeProportion%"></rect>
                 |<rect class="shuffle-read-time-proportion"
                   |x="$shuffleReadTimeProportionPos%" y="0px" height="26px"
                   |width="$shuffleReadTimeProportion%"></rect>
                 |<rect class="executor-runtime-proportion"
                   |x="$executorRuntimeProportionPos%" y="0px" height="26px"
                   |width="$executorComputingTimeProportion%"></rect>
                 |<rect class="shuffle-write-time-proportion"
                   |x="$shuffleWriteTimeProportionPos%" y="0px" height="26px"
                   |width="$shuffleWriteTimeProportion%"></rect>
                 |<rect class="serialization-time-proportion"
                   |x="$serializationTimeProportionPos%" y="0px" height="26px"
                   |width="$serializationTimeProportion%"></rect>
                 |<rect class="getting-result-time-proportion"
                   |x="$gettingResultTimeProportionPos%" y="0px" height="26px"
                   |width="$gettingResultTimeProportion%"></rect></svg>""".stripMargin
          }
        val timelineObject =
          s"""
             |{
               |'className': 'task task-assignment-timeline-object',
               |'group': '$executorId',
               |'content': '<div class="task-assignment-timeline-content"
                 |data-toggle="tooltip" data-placement="top"
                 |data-html="true" data-container="body"
                 |data-title="${s"Task " + index + " (attempt " + attempt + ")"}<br>
                 |Status: ${taskInfo.status}<br>
                 |Launch Time: ${UIUtils.formatDate(new Date(launchTime))}
                 |${
                     if (!taskInfo.duration.isDefined) {
                       s"""<br>Finish Time: ${UIUtils.formatDate(new Date(finishTime))}"""
                     } else {
                        ""
                      }
                   }
                 |<br>Scheduler Delay: $schedulerDelay ms
                 |<br>Task Deserialization Time: ${UIUtils.formatDuration(deserializationTime)}
                 |<br>Shuffle Read Time: ${UIUtils.formatDuration(shuffleReadTime)}
                 |<br>Executor Computing Time: ${UIUtils.formatDuration(executorComputingTime)}
                 |<br>Shuffle Write Time: ${UIUtils.formatDuration(shuffleWriteTime)}
                 |<br>Result Serialization Time: ${UIUtils.formatDuration(serializationTime)}
                 |<br>Getting Result Time: ${UIUtils.formatDuration(gettingResultTime)}">
                 |$svgTag',
               |'start': new Date($launchTime),
               |'end': new Date($finishTime)
             |}
           |""".stripMargin.replaceAll("""[\r\n]+""", " ")
        timelineObject
      }.mkString("[", ",", "]")

    val groupArrayStr = executorsSet.map {
      case (executorId, host) =>
        s"""
            {
              'id': '$executorId',
              'content': '$executorId / $host',
            }
          """
    }.mkString("[", ",", "]")

    <span class="expand-task-assignment-timeline">
      <span class="expand-task-assignment-timeline-arrow arrow-closed"></span>
      <a>Event Timeline</a>
    </span> ++
    <div id="task-assignment-timeline" class="collapsed">
      {
        if (MAX_TIMELINE_TASKS < tasks.size) {
          <strong>
            This page has more than the maximum number of tasks that can be shown in the
            visualization! Only the most recent {MAX_TIMELINE_TASKS} tasks
            (of {tasks.size} total) are shown.
          </strong>
        } else {
          Seq.empty
        }
      }
      <div class="control-panel">
        <div id="task-assignment-timeline-zoom-lock">
          <input type="checkbox"></input>
          <span>Enable zooming</span>
        </div>
      </div>
      {TIMELINE_LEGEND}
    </div> ++
    <script type="text/javascript">
      {Unparsed(s"drawTaskAssignmentTimeline(" +
      s"$groupArrayStr, $executorsArrayStr, $minLaunchTime, $maxFinishTime, " +
        s"${UIUtils.getTimeZoneOffset()})")}
    </script>
  }

}

private[ui] class TaskDataSource(
    stage: StageData,
    currentTime: Long,
    pageSize: Int,
    sortColumn: String,
    desc: Boolean,
    store: AppStatusStore) extends PagedDataSource[TaskData](pageSize) {
  import ApiHelper._

  // Keep an internal cache of executor log maps so that long task lists render faster.
  private val executorIdToLogs = new HashMap[String, Map[String, String]]()

  private var _tasksToShow: Seq[TaskData] = null

  override def dataSize: Int = store.taskCount(stage.stageId, stage.attemptId).toInt

  override def sliceData(from: Int, to: Int): Seq[TaskData] = {
    if (_tasksToShow == null) {
      _tasksToShow = store.taskList(stage.stageId, stage.attemptId, from, to - from,
        indexName(sortColumn), !desc)
    }
    _tasksToShow
  }

  def tasks: Seq[TaskData] = _tasksToShow

  def executorLogs(id: String): Map[String, String] = {
    executorIdToLogs.getOrElseUpdate(id,
      store.asOption(store.executorSummary(id)).map(_.executorLogs).getOrElse(Map.empty))
  }

}

private[ui] class TaskPagedTable(
    stage: StageData,
    basePath: String,
    currentTime: Long,
    pageSize: Int,
    sortColumn: String,
    desc: Boolean,
    store: AppStatusStore) extends PagedTable[TaskData] {

  import ApiHelper._

  override def tableId: String = "task-table"

  override def tableCssClass: String =
    "table table-bordered table-condensed table-striped table-head-clickable"

  override def pageSizeFormField: String = "task.pageSize"

  override def prevPageSizeFormField: String = "task.prevPageSize"

  override def pageNumberFormField: String = "task.page"

  override val dataSource: TaskDataSource = new TaskDataSource(
    stage,
    currentTime,
    pageSize,
    sortColumn,
    desc,
    store)

  override def pageLink(page: Int): String = {
    val encodedSortColumn = URLEncoder.encode(sortColumn, "UTF-8")
    basePath +
      s"&$pageNumberFormField=$page" +
      s"&task.sort=$encodedSortColumn" +
      s"&task.desc=$desc" +
      s"&$pageSizeFormField=$pageSize"
  }

  override def goButtonFormPath: String = {
    val encodedSortColumn = URLEncoder.encode(sortColumn, "UTF-8")
    s"$basePath&task.sort=$encodedSortColumn&task.desc=$desc"
  }

  def headers: Seq[Node] = {
    import ApiHelper._

    val taskHeadersAndCssClasses: Seq[(String, String)] =
      Seq(
        (HEADER_TASK_INDEX, ""), (HEADER_ID, ""), (HEADER_ATTEMPT, ""), (HEADER_STATUS, ""),
        (HEADER_LOCALITY, ""), (HEADER_EXECUTOR, ""), (HEADER_HOST, ""), (HEADER_LAUNCH_TIME, ""),
        (HEADER_DURATION, ""), (HEADER_SCHEDULER_DELAY, TaskDetailsClassNames.SCHEDULER_DELAY),
        (HEADER_DESER_TIME, TaskDetailsClassNames.TASK_DESERIALIZATION_TIME),
        (HEADER_GC_TIME, ""),
        (HEADER_SER_TIME, TaskDetailsClassNames.RESULT_SERIALIZATION_TIME),
        (HEADER_GETTING_RESULT_TIME, TaskDetailsClassNames.GETTING_RESULT_TIME),
        (HEADER_PEAK_MEM, TaskDetailsClassNames.PEAK_EXECUTION_MEMORY)) ++
        {if (hasAccumulators(stage)) Seq((HEADER_ACCUMULATORS, "")) else Nil} ++
        {if (hasInput(stage)) Seq((HEADER_INPUT_SIZE, "")) else Nil} ++
        {if (hasOutput(stage)) Seq((HEADER_OUTPUT_SIZE, "")) else Nil} ++
        {if (hasShuffleRead(stage)) {
          Seq((HEADER_SHUFFLE_READ_TIME, TaskDetailsClassNames.SHUFFLE_READ_BLOCKED_TIME),
            (HEADER_SHUFFLE_TOTAL_READS, ""),
            (HEADER_SHUFFLE_REMOTE_READS, TaskDetailsClassNames.SHUFFLE_READ_REMOTE_SIZE))
        } else {
          Nil
        }} ++
        {if (hasShuffleWrite(stage)) {
          Seq((HEADER_SHUFFLE_WRITE_TIME, ""), (HEADER_SHUFFLE_WRITE_SIZE, ""))
        } else {
          Nil
        }} ++
        {if (hasBytesSpilled(stage)) {
          Seq((HEADER_MEM_SPILL, ""), (HEADER_DISK_SPILL, ""))
        } else {
          Nil
        }} ++
        Seq((HEADER_ERROR, ""))

    if (!taskHeadersAndCssClasses.map(_._1).contains(sortColumn)) {
      throw new IllegalArgumentException(s"Unknown column: $sortColumn")
    }

    val headerRow: Seq[Node] = {
      taskHeadersAndCssClasses.map { case (header, cssClass) =>
        if (header == sortColumn) {
          val headerLink = Unparsed(
            basePath +
              s"&task.sort=${URLEncoder.encode(header, "UTF-8")}" +
              s"&task.desc=${!desc}" +
              s"&task.pageSize=$pageSize")
          val arrow = if (desc) "&#x25BE;" else "&#x25B4;" // UP or DOWN
          <th class={cssClass}>
            <a href={headerLink}>
              {header}
              <span>&nbsp;{Unparsed(arrow)}</span>
            </a>
          </th>
        } else {
          val headerLink = Unparsed(
            basePath +
              s"&task.sort=${URLEncoder.encode(header, "UTF-8")}" +
              s"&task.pageSize=$pageSize")
          <th class={cssClass}>
            <a href={headerLink}>
              {header}
            </a>
          </th>
        }
      }
    }
    <thead>{headerRow}</thead>
  }

  def row(task: TaskData): Seq[Node] = {
    def formatDuration(value: Option[Long], hideZero: Boolean = false): String = {
      value.map { v =>
        if (v > 0 || !hideZero) UIUtils.formatDuration(v) else ""
      }.getOrElse("")
    }

    def formatBytes(value: Option[Long]): String = {
      Utils.bytesToString(value.getOrElse(0L))
    }

    <tr>
      <td>{task.index}</td>
      <td>{task.taskId}</td>
      <td>{if (task.speculative) s"${task.attempt} (speculative)" else task.attempt.toString}</td>
      <td>{task.status}</td>
      <td>{task.taskLocality}</td>
      <td>{task.executorId}</td>
      <td>
        <div style="float: left">{task.host}</div>
        <div style="float: right">
        {
          dataSource.executorLogs(task.executorId).map {
            case (logName, logUrl) => <div><a href={logUrl}>{logName}</a></div>
          }
        }
        </div>
      </td>
      <td>{UIUtils.formatDate(task.launchTime)}</td>
      <td>{formatDuration(task.taskMetrics.map(_.executorRunTime))}</td>
      <td class={TaskDetailsClassNames.SCHEDULER_DELAY}>
        {UIUtils.formatDuration(AppStatusUtils.schedulerDelay(task))}
      </td>
      <td class={TaskDetailsClassNames.TASK_DESERIALIZATION_TIME}>
        {formatDuration(task.taskMetrics.map(_.executorDeserializeTime))}
      </td>
      <td>
        {formatDuration(task.taskMetrics.map(_.jvmGcTime), hideZero = true)}
      </td>
      <td class={TaskDetailsClassNames.RESULT_SERIALIZATION_TIME}>
        {formatDuration(task.taskMetrics.map(_.resultSerializationTime))}
      </td>
      <td class={TaskDetailsClassNames.GETTING_RESULT_TIME}>
        {UIUtils.formatDuration(AppStatusUtils.gettingResultTime(task))}
      </td>
      <td class={TaskDetailsClassNames.PEAK_EXECUTION_MEMORY}>
        {formatBytes(task.taskMetrics.map(_.peakExecutionMemory))}
      </td>
      {if (hasAccumulators(stage)) {
        <td>{accumulatorsInfo(task)}</td>
      }}
      {if (hasInput(stage)) {
        <td>{
          metricInfo(task) { m =>
            val bytesRead = Utils.bytesToString(m.inputMetrics.bytesRead)
            val records = m.inputMetrics.recordsRead
            Unparsed(s"$bytesRead / $records")
          }
        }</td>
      }}
      {if (hasOutput(stage)) {
        <td>{
          metricInfo(task) { m =>
            val bytesWritten = Utils.bytesToString(m.outputMetrics.bytesWritten)
            val records = m.outputMetrics.recordsWritten
            Unparsed(s"$bytesWritten / $records")
          }
        }</td>
      }}
      {if (hasShuffleRead(stage)) {
        <td class={TaskDetailsClassNames.SHUFFLE_READ_BLOCKED_TIME}>
          {formatDuration(task.taskMetrics.map(_.shuffleReadMetrics.fetchWaitTime))}
        </td>
        <td>{
          metricInfo(task) { m =>
            val bytesRead = Utils.bytesToString(totalBytesRead(m.shuffleReadMetrics))
            val records = m.shuffleReadMetrics.recordsRead
            Unparsed(s"$bytesRead / $records")
          }
        }</td>
        <td class={TaskDetailsClassNames.SHUFFLE_READ_REMOTE_SIZE}>
          {formatBytes(task.taskMetrics.map(_.shuffleReadMetrics.remoteBytesRead))}
        </td>
      }}
      {if (hasShuffleWrite(stage)) {
        <td>{
          formatDuration(
            task.taskMetrics.map { m =>
              TimeUnit.NANOSECONDS.toMillis(m.shuffleWriteMetrics.writeTime)
            },
            hideZero = true)
        }</td>
        <td>{
          metricInfo(task) { m =>
            val bytesWritten = Utils.bytesToString(m.shuffleWriteMetrics.bytesWritten)
            val records = m.shuffleWriteMetrics.recordsWritten
            Unparsed(s"$bytesWritten / $records")
          }
        }</td>
      }}
      {if (hasBytesSpilled(stage)) {
        <td>{formatBytes(task.taskMetrics.map(_.memoryBytesSpilled))}</td>
        <td>{formatBytes(task.taskMetrics.map(_.diskBytesSpilled))}</td>
      }}
      {errorMessageCell(task.errorMessage.getOrElse(""))}
    </tr>
  }

  private def accumulatorsInfo(task: TaskData): Seq[Node] = {
    task.accumulatorUpdates.flatMap { acc =>
      if (acc.name != null && acc.update.isDefined) {
        Unparsed(StringEscapeUtils.escapeHtml4(s"${acc.name}: ${acc.update.get}")) ++ <br />
      } else {
        Nil
      }
    }
  }

  private def metricInfo(task: TaskData)(fn: TaskMetrics => Seq[Node]): Seq[Node] = {
    task.taskMetrics.map(fn).getOrElse(Nil)
  }

  private def errorMessageCell(error: String): Seq[Node] = {
    val isMultiline = error.indexOf('\n') >= 0
    // Display the first line by default
    val errorSummary = StringEscapeUtils.escapeHtml4(
      if (isMultiline) {
        error.substring(0, error.indexOf('\n'))
      } else {
        error
      })
    val details = if (isMultiline) {
      // scalastyle:off
      <span onclick="this.parentNode.querySelector('.stacktrace-details').classList.toggle('collapsed')"
            class="expand-details">
        +details
      </span> ++
        <div class="stacktrace-details collapsed">
          <pre>{error}</pre>
        </div>
      // scalastyle:on
    } else {
      ""
    }
    <td>{errorSummary}{details}</td>
  }
}

private[ui] object ApiHelper {

  val HEADER_ID = "ID"
  val HEADER_TASK_INDEX = "Index"
  val HEADER_ATTEMPT = "Attempt"
  val HEADER_STATUS = "Status"
  val HEADER_LOCALITY = "Locality Level"
  val HEADER_EXECUTOR = "Executor ID"
  val HEADER_HOST = "Host"
  val HEADER_LAUNCH_TIME = "Launch Time"
  val HEADER_DURATION = "Duration"
  val HEADER_SCHEDULER_DELAY = "Scheduler Delay"
  val HEADER_DESER_TIME = "Task Deserialization Time"
  val HEADER_GC_TIME = "GC Time"
  val HEADER_SER_TIME = "Result Serialization Time"
  val HEADER_GETTING_RESULT_TIME = "Getting Result Time"
  val HEADER_PEAK_MEM = "Peak Execution Memory"
  val HEADER_ACCUMULATORS = "Accumulators"
  val HEADER_INPUT_SIZE = "Input Size / Records"
  val HEADER_OUTPUT_SIZE = "Output Size / Records"
  val HEADER_SHUFFLE_READ_TIME = "Shuffle Read Blocked Time"
  val HEADER_SHUFFLE_TOTAL_READS = "Shuffle Read Size / Records"
  val HEADER_SHUFFLE_REMOTE_READS = "Shuffle Remote Reads"
  val HEADER_SHUFFLE_WRITE_TIME = "Write Time"
  val HEADER_SHUFFLE_WRITE_SIZE = "Shuffle Write Size / Records"
  val HEADER_MEM_SPILL = "Shuffle Spill (Memory)"
  val HEADER_DISK_SPILL = "Shuffle Spill (Disk)"
  val HEADER_ERROR = "Errors"

  private[ui] val COLUMN_TO_INDEX = Map(
    HEADER_ID -> null.asInstanceOf[String],
    HEADER_TASK_INDEX -> TaskIndexNames.TASK_INDEX,
    HEADER_ATTEMPT -> TaskIndexNames.ATTEMPT,
    HEADER_STATUS -> TaskIndexNames.STATUS,
    HEADER_LOCALITY -> TaskIndexNames.LOCALITY,
    HEADER_EXECUTOR -> TaskIndexNames.EXECUTOR,
    HEADER_HOST -> TaskIndexNames.HOST,
    HEADER_LAUNCH_TIME -> TaskIndexNames.LAUNCH_TIME,
    // SPARK-26109: Duration of task as executorRunTime to make it consistent with the
    // aggregated tasks summary metrics table and the previous versions of Spark.
    HEADER_DURATION -> TaskIndexNames.EXEC_RUN_TIME,
    HEADER_SCHEDULER_DELAY -> TaskIndexNames.SCHEDULER_DELAY,
    HEADER_DESER_TIME -> TaskIndexNames.DESER_TIME,
    HEADER_GC_TIME -> TaskIndexNames.GC_TIME,
    HEADER_SER_TIME -> TaskIndexNames.SER_TIME,
    HEADER_GETTING_RESULT_TIME -> TaskIndexNames.GETTING_RESULT_TIME,
    HEADER_PEAK_MEM -> TaskIndexNames.PEAK_MEM,
    HEADER_ACCUMULATORS -> TaskIndexNames.ACCUMULATORS,
    HEADER_INPUT_SIZE -> TaskIndexNames.INPUT_SIZE,
    HEADER_OUTPUT_SIZE -> TaskIndexNames.OUTPUT_SIZE,
    HEADER_SHUFFLE_READ_TIME -> TaskIndexNames.SHUFFLE_READ_TIME,
    HEADER_SHUFFLE_TOTAL_READS -> TaskIndexNames.SHUFFLE_TOTAL_READS,
    HEADER_SHUFFLE_REMOTE_READS -> TaskIndexNames.SHUFFLE_REMOTE_READS,
    HEADER_SHUFFLE_WRITE_TIME -> TaskIndexNames.SHUFFLE_WRITE_TIME,
    HEADER_SHUFFLE_WRITE_SIZE -> TaskIndexNames.SHUFFLE_WRITE_SIZE,
    HEADER_MEM_SPILL -> TaskIndexNames.MEM_SPILL,
    HEADER_DISK_SPILL -> TaskIndexNames.DISK_SPILL,
    HEADER_ERROR -> TaskIndexNames.ERROR)

  def hasAccumulators(stageData: StageData): Boolean = {
    stageData.accumulatorUpdates.exists { acc => acc.name != null && acc.value != null }
  }

  def hasInput(stageData: StageData): Boolean = stageData.inputBytes > 0

  def hasOutput(stageData: StageData): Boolean = stageData.outputBytes > 0

  def hasShuffleRead(stageData: StageData): Boolean = stageData.shuffleReadBytes > 0

  def hasShuffleWrite(stageData: StageData): Boolean = stageData.shuffleWriteBytes > 0

  def hasBytesSpilled(stageData: StageData): Boolean = {
    stageData.diskBytesSpilled > 0 || stageData.memoryBytesSpilled > 0
  }

  def totalBytesRead(metrics: ShuffleReadMetrics): Long = {
    metrics.localBytesRead + metrics.remoteBytesRead
  }

  def indexName(sortColumn: String): Option[String] = {
    COLUMN_TO_INDEX.get(sortColumn) match {
      case Some(v) => Option(v)
      case _ => throw new IllegalArgumentException(s"Invalid sort column: $sortColumn")
    }
  }

  def lastStageNameAndDescription(store: AppStatusStore, job: JobData): (String, String) = {
    val stage = store.asOption(store.stageAttempt(job.stageIds.max, 0))
    (stage.map(_.name).getOrElse(""), stage.flatMap(_.description).getOrElse(job.name))
  }

}
