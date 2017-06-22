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
import javax.servlet.http.HttpServletRequest

import scala.collection.mutable.HashSet
import scala.xml.{Elem, Node, Unparsed}

import org.apache.commons.lang3.StringEscapeUtils

import org.apache.spark.SparkConf
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.{AccumulableInfo, TaskInfo, TaskLocality}
import org.apache.spark.ui._
import org.apache.spark.ui.exec.ExecutorsListener
import org.apache.spark.ui.jobs.UIData._
import org.apache.spark.util.{Distribution, Utils}

/** Page showing statistics and task list for a given stage */
private[ui] class StagePage(parent: StagesTab) extends WebUIPage("stage") {
  import StagePage._

  private val progressListener = parent.progressListener
  private val operationGraphListener = parent.operationGraphListener
  private val executorsListener = parent.executorsListener

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

  private def getLocalitySummaryString(stageData: StageUIData): String = {
    val localities = stageData.taskData.values.map(_.taskInfo.taskLocality)
    val localityCounts = localities.groupBy(identity).mapValues(_.size)
    val localityNamesAndCounts = localityCounts.toSeq.map { case (locality, count) =>
      val localityName = locality match {
        case TaskLocality.PROCESS_LOCAL => "Process local"
        case TaskLocality.NODE_LOCAL => "Node local"
        case TaskLocality.RACK_LOCAL => "Rack local"
        case TaskLocality.ANY => "Any"
      }
      s"$localityName: $count"
    }
    localityNamesAndCounts.sorted.mkString("; ")
  }

  def render(request: HttpServletRequest): Seq[Node] = {
    progressListener.synchronized {
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
      val stageDataOption = progressListener.stageIdToData.get((stageId, stageAttemptId))

      val stageHeader = s"Details for Stage $stageId (Attempt $stageAttemptId)"
      if (stageDataOption.isEmpty) {
        val content =
          <div id="no-info">
            <p>No information to display for Stage {stageId} (Attempt {stageAttemptId})</p>
          </div>
        return UIUtils.headerSparkPage(stageHeader, content, parent)

      }
      if (stageDataOption.get.taskData.isEmpty) {
        val content =
          <div>
            <h4>Summary Metrics</h4> No tasks have started yet
            <h4>Tasks</h4> No tasks have started yet
          </div>
        return UIUtils.headerSparkPage(stageHeader, content, parent)
      }

      val stageData = stageDataOption.get
      val tasks = stageData.taskData.values.toSeq.sortBy(_.taskInfo.launchTime)
      val numCompleted = stageData.numCompleteTasks
      val totalTasks = stageData.numActiveTasks +
        stageData.numCompleteTasks + stageData.numFailedTasks
      val totalTasksNumStr = if (totalTasks == tasks.size) {
        s"$totalTasks"
      } else {
        s"$totalTasks, showing ${tasks.size}"
      }

      val allAccumulables = progressListener.stageIdToData((stageId, stageAttemptId)).accumulables
      val externalAccumulables = allAccumulables.values.filter { acc => !acc.internal }
      val hasAccumulators = externalAccumulables.nonEmpty

      val summary =
        <div>
          <ul class="unstyled">
            <li>
              <strong>Total Time Across All Tasks: </strong>
              {UIUtils.formatDuration(stageData.executorRunTime)}
            </li>
            <li>
              <strong>Locality Level Summary: </strong>
              {getLocalitySummaryString(stageData)}
            </li>
            {if (stageData.hasInput) {
              <li>
                <strong>Input Size / Records: </strong>
                {s"${Utils.bytesToString(stageData.inputBytes)} / ${stageData.inputRecords}"}
              </li>
            }}
            {if (stageData.hasOutput) {
              <li>
                <strong>Output: </strong>
                {s"${Utils.bytesToString(stageData.outputBytes)} / ${stageData.outputRecords}"}
              </li>
            }}
            {if (stageData.hasShuffleRead) {
              <li>
                <strong>Shuffle Read: </strong>
                {s"${Utils.bytesToString(stageData.shuffleReadTotalBytes)} / " +
                 s"${stageData.shuffleReadRecords}"}
              </li>
            }}
            {if (stageData.hasShuffleWrite) {
              <li>
                <strong>Shuffle Write: </strong>
                 {s"${Utils.bytesToString(stageData.shuffleWriteBytes)} / " +
                 s"${stageData.shuffleWriteRecords}"}
              </li>
            }}
            {if (stageData.hasBytesSpilled) {
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
              {if (stageData.hasShuffleRead) {
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

      val dagViz = UIUtils.showDagVizForStage(
        stageId, operationGraphListener.getOperationGraphForStage(stageId))

      val accumulableHeaders: Seq[String] = Seq("Accumulable", "Value")
      def accumulableRow(acc: AccumulableInfo): Seq[Node] = {
        (acc.name, acc.value) match {
          case (Some(name), Some(value)) => <tr><td>{name}</td><td>{value}</td></tr>
          case _ => Seq.empty[Node]
        }
      }
      val accumulableTable = UIUtils.listingTable(
        accumulableHeaders,
        accumulableRow,
        externalAccumulables.toSeq)

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
          parent.conf,
          UIUtils.prependBaseUri(parent.basePath) +
            s"/stages/stage?id=${stageId}&attempt=${stageAttemptId}",
          tasks,
          hasAccumulators,
          stageData.hasInput,
          stageData.hasOutput,
          stageData.hasShuffleRead,
          stageData.hasShuffleWrite,
          stageData.hasBytesSpilled,
          currentTime,
          pageSize = taskPageSize,
          sortColumn = taskSortColumn,
          desc = taskSortDesc,
          executorsListener = executorsListener
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

      val taskIdsInPage = if (taskTable == null) Set.empty[Long]
        else taskTable.dataSource.slicedTaskIds

      // Excludes tasks which failed and have incomplete metrics
      val validTasks = tasks.filter(t => t.taskInfo.status == "SUCCESS" && t.metrics.isDefined)

      val summaryTable: Option[Seq[Node]] =
        if (validTasks.isEmpty) {
          None
        }
        else {
          def getDistributionQuantiles(data: Seq[Double]): IndexedSeq[Double] =
            Distribution(data).get.getQuantiles()
          def getFormattedTimeQuantiles(times: Seq[Double]): Seq[Node] = {
            getDistributionQuantiles(times).map { millis =>
              <td>{UIUtils.formatDuration(millis.toLong)}</td>
            }
          }
          def getFormattedSizeQuantiles(data: Seq[Double]): Seq[Elem] = {
            getDistributionQuantiles(data).map(d => <td>{Utils.bytesToString(d.toLong)}</td>)
          }

          val deserializationTimes = validTasks.map { taskUIData: TaskUIData =>
            taskUIData.metrics.get.executorDeserializeTime.toDouble
          }
          val deserializationQuantiles =
            <td>
              <span data-toggle="tooltip" title={ToolTips.TASK_DESERIALIZATION_TIME}
                    data-placement="right">
                Task Deserialization Time
              </span>
            </td> +: getFormattedTimeQuantiles(deserializationTimes)

          val serviceTimes = validTasks.map { taskUIData: TaskUIData =>
            taskUIData.metrics.get.executorRunTime.toDouble
          }
          val serviceQuantiles = <td>Duration</td> +: getFormattedTimeQuantiles(serviceTimes)

          val gcTimes = validTasks.map { taskUIData: TaskUIData =>
            taskUIData.metrics.get.jvmGCTime.toDouble
          }
          val gcQuantiles =
            <td>
              <span data-toggle="tooltip"
                  title={ToolTips.GC_TIME} data-placement="right">GC Time
              </span>
            </td> +: getFormattedTimeQuantiles(gcTimes)

          val serializationTimes = validTasks.map { taskUIData: TaskUIData =>
            taskUIData.metrics.get.resultSerializationTime.toDouble
          }
          val serializationQuantiles =
            <td>
              <span data-toggle="tooltip"
                    title={ToolTips.RESULT_SERIALIZATION_TIME} data-placement="right">
                Result Serialization Time
              </span>
            </td> +: getFormattedTimeQuantiles(serializationTimes)

          val gettingResultTimes = validTasks.map { taskUIData: TaskUIData =>
            getGettingResultTime(taskUIData.taskInfo, currentTime).toDouble
          }
          val gettingResultQuantiles =
            <td>
              <span data-toggle="tooltip"
                  title={ToolTips.GETTING_RESULT_TIME} data-placement="right">
                Getting Result Time
              </span>
            </td> +:
            getFormattedTimeQuantiles(gettingResultTimes)

          val peakExecutionMemory = validTasks.map { taskUIData: TaskUIData =>
            taskUIData.metrics.get.peakExecutionMemory.toDouble
          }
          val peakExecutionMemoryQuantiles = {
            <td>
              <span data-toggle="tooltip"
                    title={ToolTips.PEAK_EXECUTION_MEMORY} data-placement="right">
                Peak Execution Memory
              </span>
            </td> +: getFormattedSizeQuantiles(peakExecutionMemory)
          }

          // The scheduler delay includes the network delay to send the task to the worker
          // machine and to send back the result (but not the time to fetch the task result,
          // if it needed to be fetched from the block manager on the worker).
          val schedulerDelays = validTasks.map { taskUIData: TaskUIData =>
            getSchedulerDelay(taskUIData.taskInfo, taskUIData.metrics.get, currentTime).toDouble
          }
          val schedulerDelayTitle = <td><span data-toggle="tooltip"
            title={ToolTips.SCHEDULER_DELAY} data-placement="right">Scheduler Delay</span></td>
          val schedulerDelayQuantiles = schedulerDelayTitle +:
            getFormattedTimeQuantiles(schedulerDelays)
          def getFormattedSizeQuantilesWithRecords(data: Seq[Double], records: Seq[Double])
            : Seq[Elem] = {
            val recordDist = getDistributionQuantiles(records).iterator
            getDistributionQuantiles(data).map(d =>
              <td>{s"${Utils.bytesToString(d.toLong)} / ${recordDist.next().toLong}"}</td>
            )
          }

          val inputSizes = validTasks.map { taskUIData: TaskUIData =>
            taskUIData.metrics.get.inputMetrics.bytesRead.toDouble
          }

          val inputRecords = validTasks.map { taskUIData: TaskUIData =>
            taskUIData.metrics.get.inputMetrics.recordsRead.toDouble
          }

          val inputQuantiles = <td>Input Size / Records</td> +:
            getFormattedSizeQuantilesWithRecords(inputSizes, inputRecords)

          val outputSizes = validTasks.map { taskUIData: TaskUIData =>
            taskUIData.metrics.get.outputMetrics.bytesWritten.toDouble
          }

          val outputRecords = validTasks.map { taskUIData: TaskUIData =>
            taskUIData.metrics.get.outputMetrics.recordsWritten.toDouble
          }

          val outputQuantiles = <td>Output Size / Records</td> +:
            getFormattedSizeQuantilesWithRecords(outputSizes, outputRecords)

          val shuffleReadBlockedTimes = validTasks.map { taskUIData: TaskUIData =>
            taskUIData.metrics.get.shuffleReadMetrics.fetchWaitTime.toDouble
          }
          val shuffleReadBlockedQuantiles =
            <td>
              <span data-toggle="tooltip"
                    title={ToolTips.SHUFFLE_READ_BLOCKED_TIME} data-placement="right">
                Shuffle Read Blocked Time
              </span>
            </td> +:
            getFormattedTimeQuantiles(shuffleReadBlockedTimes)

          val shuffleReadTotalSizes = validTasks.map { taskUIData: TaskUIData =>
            taskUIData.metrics.get.shuffleReadMetrics.totalBytesRead.toDouble
          }
          val shuffleReadTotalRecords = validTasks.map { taskUIData: TaskUIData =>
            taskUIData.metrics.get.shuffleReadMetrics.recordsRead.toDouble
          }
          val shuffleReadTotalQuantiles =
            <td>
              <span data-toggle="tooltip"
                    title={ToolTips.SHUFFLE_READ} data-placement="right">
                Shuffle Read Size / Records
              </span>
            </td> +:
            getFormattedSizeQuantilesWithRecords(shuffleReadTotalSizes, shuffleReadTotalRecords)

          val shuffleReadRemoteSizes = validTasks.map { taskUIData: TaskUIData =>
            taskUIData.metrics.get.shuffleReadMetrics.remoteBytesRead.toDouble
          }
          val shuffleReadRemoteQuantiles =
            <td>
              <span data-toggle="tooltip"
                    title={ToolTips.SHUFFLE_READ_REMOTE_SIZE} data-placement="right">
                Shuffle Remote Reads
              </span>
            </td> +:
            getFormattedSizeQuantiles(shuffleReadRemoteSizes)

          val shuffleWriteSizes = validTasks.map { taskUIData: TaskUIData =>
            taskUIData.metrics.get.shuffleWriteMetrics.bytesWritten.toDouble
          }

          val shuffleWriteRecords = validTasks.map { taskUIData: TaskUIData =>
            taskUIData.metrics.get.shuffleWriteMetrics.recordsWritten.toDouble
          }

          val shuffleWriteQuantiles = <td>Shuffle Write Size / Records</td> +:
            getFormattedSizeQuantilesWithRecords(shuffleWriteSizes, shuffleWriteRecords)

          val memoryBytesSpilledSizes = validTasks.map { taskUIData: TaskUIData =>
            taskUIData.metrics.get.memoryBytesSpilled.toDouble
          }
          val memoryBytesSpilledQuantiles = <td>Shuffle spill (memory)</td> +:
            getFormattedSizeQuantiles(memoryBytesSpilledSizes)

          val diskBytesSpilledSizes = validTasks.map { taskUIData: TaskUIData =>
            taskUIData.metrics.get.diskBytesSpilled.toDouble
          }
          val diskBytesSpilledQuantiles = <td>Shuffle spill (disk)</td> +:
            getFormattedSizeQuantiles(diskBytesSpilledSizes)

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
            if (stageData.hasInput) <tr>{inputQuantiles}</tr> else Nil,
            if (stageData.hasOutput) <tr>{outputQuantiles}</tr> else Nil,
            if (stageData.hasShuffleRead) {
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
            if (stageData.hasShuffleWrite) <tr>{shuffleWriteQuantiles}</tr> else Nil,
            if (stageData.hasBytesSpilled) <tr>{memoryBytesSpilledQuantiles}</tr> else Nil,
            if (stageData.hasBytesSpilled) <tr>{diskBytesSpilledQuantiles}</tr> else Nil)

          val quantileHeaders = Seq("Metric", "Min", "25th percentile",
            "Median", "75th percentile", "Max")
          // The summary table does not use CSS to stripe rows, which doesn't work with hidden
          // rows (instead, JavaScript in table.js is used to stripe the non-hidden rows).
          Some(UIUtils.listingTable(
            quantileHeaders,
            identity[Seq[Node]],
            listings,
            fixedWidth = true,
            id = Some("task-summary-table"),
            stripeRowsWithCss = false))
        }

      val executorTable = new ExecutorTable(stageId, stageAttemptId, parent)

      val maybeAccumulableTable: Seq[Node] =
        if (hasAccumulators) { <h4>Accumulators</h4> ++ accumulableTable } else Seq()

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
          stageData.taskData.values.toSeq.filter(t => taskIdsInPage.contains(t.taskInfo.taskId)),
          currentTime) ++
        <h4>Summary Metrics for <a href="#tasks-section">{numCompleted} Completed Tasks</a></h4> ++
        <div>{summaryTable.getOrElse("No tasks have reported metrics yet.")}</div> ++
        aggMetrics ++
        maybeAccumulableTable ++
        <h4 id="tasks-section">Tasks ({totalTasksNumStr})</h4> ++
          taskTableHTML ++ jsForScrollingDownToTaskTable
      UIUtils.headerSparkPage(stageHeader, content, parent, showVisualization = true)
    }
  }

  def makeTimeline(tasks: Seq[TaskUIData], currentTime: Long): Seq[Node] = {
    val executorsSet = new HashSet[(String, String)]
    var minLaunchTime = Long.MaxValue
    var maxFinishTime = Long.MinValue

    val executorsArrayStr =
      tasks.sortBy(-_.taskInfo.launchTime).take(MAX_TIMELINE_TASKS).map { taskUIData =>
        val taskInfo = taskUIData.taskInfo
        val executorId = taskInfo.executorId
        val host = taskInfo.host
        executorsSet += ((executorId, host))

        val launchTime = taskInfo.launchTime
        val finishTime = if (!taskInfo.running) taskInfo.finishTime else currentTime
        val totalExecutionTime = finishTime - launchTime
        minLaunchTime = launchTime.min(minLaunchTime)
        maxFinishTime = finishTime.max(maxFinishTime)

        def toProportion(time: Long) = time.toDouble / totalExecutionTime * 100

        val metricsOpt = taskUIData.metrics
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
        val gettingResultTime = getGettingResultTime(taskUIData.taskInfo, currentTime)
        val gettingResultTimeProportion = toProportion(gettingResultTime)
        val schedulerDelay =
          metricsOpt.map(getSchedulerDelay(taskInfo, _, currentTime)).getOrElse(0L)
        val schedulerDelayProportion = toProportion(schedulerDelay)

        val executorOverhead = serializationTime + deserializationTime
        val executorRunTime = if (taskInfo.running) {
          totalExecutionTime - executorOverhead - gettingResultTime
        } else {
          metricsOpt.map(_.executorRunTime).getOrElse(
            totalExecutionTime - executorOverhead - gettingResultTime)
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
        val attempt = taskInfo.attemptNumber

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
                     if (!taskInfo.running) {
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
            This stage has more than the maximum number of tasks that can be shown in the
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

private[ui] object StagePage {
  private[ui] def getGettingResultTime(info: TaskInfo, currentTime: Long): Long = {
    if (info.gettingResult) {
      if (info.finished) {
        info.finishTime - info.gettingResultTime
      } else {
        // The task is still fetching the result.
        currentTime - info.gettingResultTime
      }
    } else {
      0L
    }
  }

  private[ui] def getSchedulerDelay(
      info: TaskInfo, metrics: TaskMetricsUIData, currentTime: Long): Long = {
    if (info.finished) {
      val totalExecutionTime = info.finishTime - info.launchTime
      val executorOverhead = metrics.executorDeserializeTime +
        metrics.resultSerializationTime
      math.max(
        0,
        totalExecutionTime - metrics.executorRunTime - executorOverhead -
          getGettingResultTime(info, currentTime))
    } else {
      // The task is still running and the metrics like executorRunTime are not available.
      0L
    }
  }
}

private[ui] case class TaskTableRowInputData(inputSortable: Long, inputReadable: String)

private[ui] case class TaskTableRowOutputData(outputSortable: Long, outputReadable: String)

private[ui] case class TaskTableRowShuffleReadData(
    shuffleReadBlockedTimeSortable: Long,
    shuffleReadBlockedTimeReadable: String,
    shuffleReadSortable: Long,
    shuffleReadReadable: String,
    shuffleReadRemoteSortable: Long,
    shuffleReadRemoteReadable: String)

private[ui] case class TaskTableRowShuffleWriteData(
    writeTimeSortable: Long,
    writeTimeReadable: String,
    shuffleWriteSortable: Long,
    shuffleWriteReadable: String)

private[ui] case class TaskTableRowBytesSpilledData(
    memoryBytesSpilledSortable: Long,
    memoryBytesSpilledReadable: String,
    diskBytesSpilledSortable: Long,
    diskBytesSpilledReadable: String)

/**
 * Contains all data that needs for sorting and generating HTML. Using this one rather than
 * TaskUIData to avoid creating duplicate contents during sorting the data.
 */
private[ui] class TaskTableRowData(
    val index: Int,
    val taskId: Long,
    val attempt: Int,
    val speculative: Boolean,
    val status: String,
    val taskLocality: String,
    val executorIdAndHost: String,
    val launchTime: Long,
    val duration: Long,
    val formatDuration: String,
    val schedulerDelay: Long,
    val taskDeserializationTime: Long,
    val gcTime: Long,
    val serializationTime: Long,
    val gettingResultTime: Long,
    val peakExecutionMemoryUsed: Long,
    val accumulators: Option[String], // HTML
    val input: Option[TaskTableRowInputData],
    val output: Option[TaskTableRowOutputData],
    val shuffleRead: Option[TaskTableRowShuffleReadData],
    val shuffleWrite: Option[TaskTableRowShuffleWriteData],
    val bytesSpilled: Option[TaskTableRowBytesSpilledData],
    val error: String,
    val logs: Map[String, String])

private[ui] class TaskDataSource(
    tasks: Seq[TaskUIData],
    hasAccumulators: Boolean,
    hasInput: Boolean,
    hasOutput: Boolean,
    hasShuffleRead: Boolean,
    hasShuffleWrite: Boolean,
    hasBytesSpilled: Boolean,
    currentTime: Long,
    pageSize: Int,
    sortColumn: String,
    desc: Boolean,
    executorsListener: ExecutorsListener) extends PagedDataSource[TaskTableRowData](pageSize) {
  import StagePage._

  // Convert TaskUIData to TaskTableRowData which contains the final contents to show in the table
  // so that we can avoid creating duplicate contents during sorting the data
  private val data = tasks.map(taskRow).sorted(ordering(sortColumn, desc))

  private var _slicedTaskIds: Set[Long] = _

  override def dataSize: Int = data.size

  override def sliceData(from: Int, to: Int): Seq[TaskTableRowData] = {
    val r = data.slice(from, to)
    _slicedTaskIds = r.map(_.taskId).toSet
    r
  }

  def slicedTaskIds: Set[Long] = _slicedTaskIds

  private def taskRow(taskData: TaskUIData): TaskTableRowData = {
    val info = taskData.taskInfo
    val metrics = taskData.metrics
    val duration = taskData.taskDuration.getOrElse(1L)
    val formatDuration = taskData.taskDuration.map(d => UIUtils.formatDuration(d)).getOrElse("")
    val schedulerDelay = metrics.map(getSchedulerDelay(info, _, currentTime)).getOrElse(0L)
    val gcTime = metrics.map(_.jvmGCTime).getOrElse(0L)
    val taskDeserializationTime = metrics.map(_.executorDeserializeTime).getOrElse(0L)
    val serializationTime = metrics.map(_.resultSerializationTime).getOrElse(0L)
    val gettingResultTime = getGettingResultTime(info, currentTime)

    val externalAccumulableReadable = info.accumulables
      .filterNot(_.internal)
      .flatMap { a =>
        (a.name, a.update) match {
          case (Some(name), Some(update)) => Some(StringEscapeUtils.escapeHtml4(s"$name: $update"))
          case _ => None
        }
      }
    val peakExecutionMemoryUsed = metrics.map(_.peakExecutionMemory).getOrElse(0L)

    val maybeInput = metrics.map(_.inputMetrics)
    val inputSortable = maybeInput.map(_.bytesRead).getOrElse(0L)
    val inputReadable = maybeInput
      .map(m => s"${Utils.bytesToString(m.bytesRead)}")
      .getOrElse("")
    val inputRecords = maybeInput.map(_.recordsRead.toString).getOrElse("")

    val maybeOutput = metrics.map(_.outputMetrics)
    val outputSortable = maybeOutput.map(_.bytesWritten).getOrElse(0L)
    val outputReadable = maybeOutput
      .map(m => s"${Utils.bytesToString(m.bytesWritten)}")
      .getOrElse("")
    val outputRecords = maybeOutput.map(_.recordsWritten.toString).getOrElse("")

    val maybeShuffleRead = metrics.map(_.shuffleReadMetrics)
    val shuffleReadBlockedTimeSortable = maybeShuffleRead.map(_.fetchWaitTime).getOrElse(0L)
    val shuffleReadBlockedTimeReadable =
      maybeShuffleRead.map(ms => UIUtils.formatDuration(ms.fetchWaitTime)).getOrElse("")

    val totalShuffleBytes = maybeShuffleRead.map(_.totalBytesRead)
    val shuffleReadSortable = totalShuffleBytes.getOrElse(0L)
    val shuffleReadReadable = totalShuffleBytes.map(Utils.bytesToString).getOrElse("")
    val shuffleReadRecords = maybeShuffleRead.map(_.recordsRead.toString).getOrElse("")

    val remoteShuffleBytes = maybeShuffleRead.map(_.remoteBytesRead)
    val shuffleReadRemoteSortable = remoteShuffleBytes.getOrElse(0L)
    val shuffleReadRemoteReadable = remoteShuffleBytes.map(Utils.bytesToString).getOrElse("")

    val maybeShuffleWrite = metrics.map(_.shuffleWriteMetrics)
    val shuffleWriteSortable = maybeShuffleWrite.map(_.bytesWritten).getOrElse(0L)
    val shuffleWriteReadable = maybeShuffleWrite
      .map(m => s"${Utils.bytesToString(m.bytesWritten)}").getOrElse("")
    val shuffleWriteRecords = maybeShuffleWrite
      .map(_.recordsWritten.toString).getOrElse("")

    val maybeWriteTime = metrics.map(_.shuffleWriteMetrics.writeTime)
    val writeTimeSortable = maybeWriteTime.getOrElse(0L)
    val writeTimeReadable = maybeWriteTime.map(t => t / (1000 * 1000)).map { ms =>
      if (ms == 0) "" else UIUtils.formatDuration(ms)
    }.getOrElse("")

    val maybeMemoryBytesSpilled = metrics.map(_.memoryBytesSpilled)
    val memoryBytesSpilledSortable = maybeMemoryBytesSpilled.getOrElse(0L)
    val memoryBytesSpilledReadable =
      maybeMemoryBytesSpilled.map(Utils.bytesToString).getOrElse("")

    val maybeDiskBytesSpilled = metrics.map(_.diskBytesSpilled)
    val diskBytesSpilledSortable = maybeDiskBytesSpilled.getOrElse(0L)
    val diskBytesSpilledReadable = maybeDiskBytesSpilled.map(Utils.bytesToString).getOrElse("")

    val input =
      if (hasInput) {
        Some(TaskTableRowInputData(inputSortable, s"$inputReadable / $inputRecords"))
      } else {
        None
      }

    val output =
      if (hasOutput) {
        Some(TaskTableRowOutputData(outputSortable, s"$outputReadable / $outputRecords"))
      } else {
        None
      }

    val shuffleRead =
      if (hasShuffleRead) {
        Some(TaskTableRowShuffleReadData(
          shuffleReadBlockedTimeSortable,
          shuffleReadBlockedTimeReadable,
          shuffleReadSortable,
          s"$shuffleReadReadable / $shuffleReadRecords",
          shuffleReadRemoteSortable,
          shuffleReadRemoteReadable
        ))
      } else {
        None
      }

    val shuffleWrite =
      if (hasShuffleWrite) {
        Some(TaskTableRowShuffleWriteData(
          writeTimeSortable,
          writeTimeReadable,
          shuffleWriteSortable,
          s"$shuffleWriteReadable / $shuffleWriteRecords"
        ))
      } else {
        None
      }

    val bytesSpilled =
      if (hasBytesSpilled) {
        Some(TaskTableRowBytesSpilledData(
          memoryBytesSpilledSortable,
          memoryBytesSpilledReadable,
          diskBytesSpilledSortable,
          diskBytesSpilledReadable
        ))
      } else {
        None
      }

    val logs = executorsListener.executorToTaskSummary.get(info.executorId)
      .map(_.executorLogs).getOrElse(Map.empty)
    new TaskTableRowData(
      info.index,
      info.taskId,
      info.attemptNumber,
      info.speculative,
      info.status,
      info.taskLocality.toString,
      s"${info.executorId} / ${info.host}",
      info.launchTime,
      duration,
      formatDuration,
      schedulerDelay,
      taskDeserializationTime,
      gcTime,
      serializationTime,
      gettingResultTime,
      peakExecutionMemoryUsed,
      if (hasAccumulators) Some(externalAccumulableReadable.mkString("<br/>")) else None,
      input,
      output,
      shuffleRead,
      shuffleWrite,
      bytesSpilled,
      taskData.errorMessage.getOrElse(""),
      logs)
  }

  /**
   * Return Ordering according to sortColumn and desc
   */
  private def ordering(sortColumn: String, desc: Boolean): Ordering[TaskTableRowData] = {
    val ordering: Ordering[TaskTableRowData] = sortColumn match {
      case "Index" => Ordering.by(_.index)
      case "ID" => Ordering.by(_.taskId)
      case "Attempt" => Ordering.by(_.attempt)
      case "Status" => Ordering.by(_.status)
      case "Locality Level" => Ordering.by(_.taskLocality)
      case "Executor ID / Host" => Ordering.by(_.executorIdAndHost)
      case "Launch Time" => Ordering.by(_.launchTime)
      case "Duration" => Ordering.by(_.duration)
      case "Scheduler Delay" => Ordering.by(_.schedulerDelay)
      case "Task Deserialization Time" => Ordering.by(_.taskDeserializationTime)
      case "GC Time" => Ordering.by(_.gcTime)
      case "Result Serialization Time" => Ordering.by(_.serializationTime)
      case "Getting Result Time" => Ordering.by(_.gettingResultTime)
      case "Peak Execution Memory" => Ordering.by(_.peakExecutionMemoryUsed)
      case "Accumulators" =>
        if (hasAccumulators) {
          Ordering.by(_.accumulators.get)
        } else {
          throw new IllegalArgumentException(
            "Cannot sort by Accumulators because of no accumulators")
        }
      case "Input Size / Records" =>
        if (hasInput) {
          Ordering.by(_.input.get.inputSortable)
        } else {
          throw new IllegalArgumentException(
            "Cannot sort by Input Size / Records because of no inputs")
        }
      case "Output Size / Records" =>
        if (hasOutput) {
          Ordering.by(_.output.get.outputSortable)
        } else {
          throw new IllegalArgumentException(
            "Cannot sort by Output Size / Records because of no outputs")
        }
      // ShuffleRead
      case "Shuffle Read Blocked Time" =>
        if (hasShuffleRead) {
          Ordering.by(_.shuffleRead.get.shuffleReadBlockedTimeSortable)
        } else {
          throw new IllegalArgumentException(
            "Cannot sort by Shuffle Read Blocked Time because of no shuffle reads")
        }
      case "Shuffle Read Size / Records" =>
        if (hasShuffleRead) {
          Ordering.by(_.shuffleRead.get.shuffleReadSortable)
        } else {
          throw new IllegalArgumentException(
            "Cannot sort by Shuffle Read Size / Records because of no shuffle reads")
        }
      case "Shuffle Remote Reads" =>
        if (hasShuffleRead) {
          Ordering.by(_.shuffleRead.get.shuffleReadRemoteSortable)
        } else {
          throw new IllegalArgumentException(
            "Cannot sort by Shuffle Remote Reads because of no shuffle reads")
        }
      // ShuffleWrite
      case "Write Time" =>
        if (hasShuffleWrite) {
          Ordering.by(_.shuffleWrite.get.writeTimeSortable)
        } else {
          throw new IllegalArgumentException(
            "Cannot sort by Write Time because of no shuffle writes")
        }
      case "Shuffle Write Size / Records" =>
        if (hasShuffleWrite) {
          Ordering.by(_.shuffleWrite.get.shuffleWriteSortable)
        } else {
          throw new IllegalArgumentException(
            "Cannot sort by Shuffle Write Size / Records because of no shuffle writes")
        }
      // BytesSpilled
      case "Shuffle Spill (Memory)" =>
        if (hasBytesSpilled) {
          Ordering.by(_.bytesSpilled.get.memoryBytesSpilledSortable)
        } else {
          throw new IllegalArgumentException(
            "Cannot sort by Shuffle Spill (Memory) because of no spills")
        }
      case "Shuffle Spill (Disk)" =>
        if (hasBytesSpilled) {
          Ordering.by(_.bytesSpilled.get.diskBytesSpilledSortable)
        } else {
          throw new IllegalArgumentException(
            "Cannot sort by Shuffle Spill (Disk) because of no spills")
        }
      case "Errors" => Ordering.by(_.error)
      case unknownColumn => throw new IllegalArgumentException(s"Unknown column: $unknownColumn")
    }
    if (desc) {
      ordering.reverse
    } else {
      ordering
    }
  }

}

private[ui] class TaskPagedTable(
    conf: SparkConf,
    basePath: String,
    data: Seq[TaskUIData],
    hasAccumulators: Boolean,
    hasInput: Boolean,
    hasOutput: Boolean,
    hasShuffleRead: Boolean,
    hasShuffleWrite: Boolean,
    hasBytesSpilled: Boolean,
    currentTime: Long,
    pageSize: Int,
    sortColumn: String,
    desc: Boolean,
    executorsListener: ExecutorsListener) extends PagedTable[TaskTableRowData] {

  override def tableId: String = "task-table"

  override def tableCssClass: String =
    "table table-bordered table-condensed table-striped table-head-clickable"

  override def pageSizeFormField: String = "task.pageSize"

  override def prevPageSizeFormField: String = "task.prevPageSize"

  override def pageNumberFormField: String = "task.page"

  override val dataSource: TaskDataSource = new TaskDataSource(
    data,
    hasAccumulators,
    hasInput,
    hasOutput,
    hasShuffleRead,
    hasShuffleWrite,
    hasBytesSpilled,
    currentTime,
    pageSize,
    sortColumn,
    desc,
    executorsListener)

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
    val taskHeadersAndCssClasses: Seq[(String, String)] =
      Seq(
        ("Index", ""), ("ID", ""), ("Attempt", ""), ("Status", ""), ("Locality Level", ""),
        ("Executor ID / Host", ""), ("Launch Time", ""), ("Duration", ""),
        ("Scheduler Delay", TaskDetailsClassNames.SCHEDULER_DELAY),
        ("Task Deserialization Time", TaskDetailsClassNames.TASK_DESERIALIZATION_TIME),
        ("GC Time", ""),
        ("Result Serialization Time", TaskDetailsClassNames.RESULT_SERIALIZATION_TIME),
        ("Getting Result Time", TaskDetailsClassNames.GETTING_RESULT_TIME),
        ("Peak Execution Memory", TaskDetailsClassNames.PEAK_EXECUTION_MEMORY)) ++
        {if (hasAccumulators) Seq(("Accumulators", "")) else Nil} ++
        {if (hasInput) Seq(("Input Size / Records", "")) else Nil} ++
        {if (hasOutput) Seq(("Output Size / Records", "")) else Nil} ++
        {if (hasShuffleRead) {
          Seq(("Shuffle Read Blocked Time", TaskDetailsClassNames.SHUFFLE_READ_BLOCKED_TIME),
            ("Shuffle Read Size / Records", ""),
            ("Shuffle Remote Reads", TaskDetailsClassNames.SHUFFLE_READ_REMOTE_SIZE))
        } else {
          Nil
        }} ++
        {if (hasShuffleWrite) {
          Seq(("Write Time", ""), ("Shuffle Write Size / Records", ""))
        } else {
          Nil
        }} ++
        {if (hasBytesSpilled) {
          Seq(("Shuffle Spill (Memory)", ""), ("Shuffle Spill (Disk)", ""))
        } else {
          Nil
        }} ++
        Seq(("Errors", ""))

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

  def row(task: TaskTableRowData): Seq[Node] = {
    <tr>
      <td>{task.index}</td>
      <td>{task.taskId}</td>
      <td>{if (task.speculative) s"${task.attempt} (speculative)" else task.attempt.toString}</td>
      <td>{task.status}</td>
      <td>{task.taskLocality}</td>
      <td>
        <div style="float: left">{task.executorIdAndHost}</div>
        <div style="float: right">
        {
          task.logs.map {
            case (logName, logUrl) => <div><a href={logUrl}>{logName}</a></div>
          }
        }
        </div>
      </td>
      <td>{UIUtils.formatDate(new Date(task.launchTime))}</td>
      <td>{task.formatDuration}</td>
      <td class={TaskDetailsClassNames.SCHEDULER_DELAY}>
        {UIUtils.formatDuration(task.schedulerDelay)}
      </td>
      <td class={TaskDetailsClassNames.TASK_DESERIALIZATION_TIME}>
        {UIUtils.formatDuration(task.taskDeserializationTime)}
      </td>
      <td>
        {if (task.gcTime > 0) UIUtils.formatDuration(task.gcTime) else ""}
      </td>
      <td class={TaskDetailsClassNames.RESULT_SERIALIZATION_TIME}>
        {UIUtils.formatDuration(task.serializationTime)}
      </td>
      <td class={TaskDetailsClassNames.GETTING_RESULT_TIME}>
        {UIUtils.formatDuration(task.gettingResultTime)}
      </td>
      <td class={TaskDetailsClassNames.PEAK_EXECUTION_MEMORY}>
        {Utils.bytesToString(task.peakExecutionMemoryUsed)}
      </td>
      {if (task.accumulators.nonEmpty) {
        <td>{Unparsed(task.accumulators.get)}</td>
      }}
      {if (task.input.nonEmpty) {
        <td>{task.input.get.inputReadable}</td>
      }}
      {if (task.output.nonEmpty) {
        <td>{task.output.get.outputReadable}</td>
      }}
      {if (task.shuffleRead.nonEmpty) {
        <td class={TaskDetailsClassNames.SHUFFLE_READ_BLOCKED_TIME}>
          {task.shuffleRead.get.shuffleReadBlockedTimeReadable}
        </td>
        <td>{task.shuffleRead.get.shuffleReadReadable}</td>
        <td class={TaskDetailsClassNames.SHUFFLE_READ_REMOTE_SIZE}>
          {task.shuffleRead.get.shuffleReadRemoteReadable}
        </td>
      }}
      {if (task.shuffleWrite.nonEmpty) {
        <td>{task.shuffleWrite.get.writeTimeReadable}</td>
        <td>{task.shuffleWrite.get.shuffleWriteReadable}</td>
      }}
      {if (task.bytesSpilled.nonEmpty) {
        <td>{task.bytesSpilled.get.memoryBytesSpilledReadable}</td>
        <td>{task.bytesSpilled.get.diskBytesSpilledReadable}</td>
      }}
      {errorMessageCell(task.error)}
    </tr>
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
