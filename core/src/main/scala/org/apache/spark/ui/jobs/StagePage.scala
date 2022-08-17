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
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Date
import java.util.concurrent.TimeUnit
import javax.servlet.http.HttpServletRequest

import scala.collection.mutable.{HashMap, HashSet}
import scala.xml.{Node, Unparsed}

import org.apache.commons.text.StringEscapeUtils

import org.apache.spark.internal.config.UI._
import org.apache.spark.scheduler.TaskLocality
import org.apache.spark.status._
import org.apache.spark.status.api.v1._
import org.apache.spark.ui._
import org.apache.spark.util.Utils

/** Page showing statistics and task list for a given stage */
private[ui] class StagePage(parent: StagesTab, store: AppStatusStore) extends WebUIPage("stage") {
  import ApiHelper._

  private val TIMELINE_ENABLED = parent.conf.get(UI_TIMELINE_ENABLED)

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
  private val MAX_TIMELINE_TASKS = parent.conf.get(UI_TIMELINE_TASKS_MAXIMUM)

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
    val parameterId = request.getParameter("id")
    require(parameterId != null && parameterId.nonEmpty, "Missing id parameter")

    val parameterAttempt = request.getParameter("attempt")
    require(parameterAttempt != null && parameterAttempt.nonEmpty, "Missing attempt parameter")

    val parameterTaskSortColumn = request.getParameter("task.sort")
    val parameterTaskSortDesc = request.getParameter("task.desc")
    val parameterTaskPageSize = request.getParameter("task.pageSize")

    val eventTimelineParameterTaskPage = request.getParameter("task.eventTimelinePageNumber")
    val eventTimelineParameterTaskPageSize = request.getParameter("task.eventTimelinePageSize")
    var eventTimelineTaskPage = Option(eventTimelineParameterTaskPage).map(_.toInt).getOrElse(1)
    var eventTimelineTaskPageSize = Option(
      eventTimelineParameterTaskPageSize).map(_.toInt).getOrElse(100)

    val taskSortColumn = Option(parameterTaskSortColumn).map { sortColumn =>
      UIUtils.decodeURLParameter(sortColumn)
    }.getOrElse("Index")
    val taskSortDesc = Option(parameterTaskSortDesc).map(_.toBoolean).getOrElse(false)
    val taskPageSize = Option(parameterTaskPageSize).map(_.toInt).getOrElse(100)
    val stageId = parameterId.toInt
    val stageAttemptId = parameterAttempt.toInt

    val stageHeader = s"Details for Stage $stageId (Attempt $stageAttemptId)"
    val (stageData, stageJobIds) = parent.store
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

    if (eventTimelineTaskPageSize < 1 || eventTimelineTaskPageSize > totalTasks) {
      eventTimelineTaskPageSize = totalTasks
    }
    val eventTimelineTotalPages =
      (totalTasks + eventTimelineTaskPageSize - 1) / eventTimelineTaskPageSize
    if (eventTimelineTaskPage < 1 || eventTimelineTaskPage > eventTimelineTotalPages) {
      eventTimelineTaskPage = 1
    }

    val summary =
      <div>
        <ul class="list-unstyled">
          <li>
            <strong>Resource Profile Id: </strong>
            {stageData.resourceProfileId}
          </li>
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
              <strong>Output Size / Records: </strong>
              {s"${Utils.bytesToString(stageData.outputBytes)} / ${stageData.outputRecords}"}
            </li>
          }}
          {if (hasShuffleRead(stageData)) {
            <li>
              <strong>Shuffle Read Size / Records: </strong>
              {s"${Utils.bytesToString(stageData.shuffleReadBytes)} / " +
               s"${stageData.shuffleReadRecords}"}
            </li>
          }}
          {if (hasShuffleWrite(stageData)) {
            <li>
              <strong>Shuffle Write Size / Records: </strong>
               {s"${Utils.bytesToString(stageData.shuffleWriteBytes)} / " +
               s"${stageData.shuffleWriteRecords}"}
            </li>
          }}
          {if (hasBytesSpilled(stageData)) {
            <li>
              <strong>Spill (Memory): </strong>
              {Utils.bytesToString(stageData.memoryBytesSpilled)}
            </li>
            <li>
              <strong>Spill (Disk): </strong>
              {Utils.bytesToString(stageData.diskBytesSpilled)}
            </li>
          }}
          {if (!stageJobIds.isEmpty) {
            <li>
              <strong>Associated Job Ids: </strong>
              {stageJobIds.sorted.map { jobId =>
                val jobURL = "%s/jobs/job/?id=%s"
                  .format(UIUtils.prependBaseUri(request, parent.basePath), jobId)
                <a href={jobURL}>{jobId.toString}</a><span>&nbsp;</span>
              }}
            </li>
          }}
        </ul>
      </div>

    val stageGraph = parent.store.asOption(parent.store.operationGraphForStage(stageId))
    val dagViz = UIUtils.showDagVizForStage(stageId, stageGraph)

    val currentTime = System.currentTimeMillis()
    val taskTable = try {
      val _taskTable = new TaskPagedTable(
        stageData,
        UIUtils.prependBaseUri(request, parent.basePath) +
          s"/stages/stage/?id=${stageId}&attempt=${stageAttemptId}",
        pageSize = taskPageSize,
        sortColumn = taskSortColumn,
        desc = taskSortDesc,
        store = parent.store
      )
      _taskTable
    } catch {
      case e @ (_ : IllegalArgumentException | _ : IndexOutOfBoundsException) =>
        null
    }

    val content =
      summary ++
      dagViz ++ <div id="showAdditionalMetrics"></div> ++
      makeTimeline(
        // Only show the tasks in the table
        Option(taskTable).map({ taskPagedTable =>
          val from = (eventTimelineTaskPage - 1) * eventTimelineTaskPageSize
          val to = taskPagedTable.dataSource.dataSize.min(
            eventTimelineTaskPage * eventTimelineTaskPageSize)
          taskPagedTable.dataSource.sliceData(from, to)}).getOrElse(Nil), currentTime,
        eventTimelineTaskPage, eventTimelineTaskPageSize, eventTimelineTotalPages, stageId,
        stageAttemptId, totalTasks) ++
        <div id="parent-container">
          <script src={UIUtils.prependBaseUri(request, "/static/utils.js")}></script>
          <script src={UIUtils.prependBaseUri(request, "/static/stagepage.js")}></script>
        </div>
        UIUtils.headerSparkPage(request, stageHeader, content, parent, showVisualization = true,
          useDataTables = true)

  }

  def makeTimeline(
      tasks: Seq[TaskData],
      currentTime: Long,
      page: Int,
      pageSize: Int,
      totalPages: Int,
      stageId: Int,
      stageAttemptId: Int,
      totalTasks: Int): Seq[Node] = {

    if (!TIMELINE_ENABLED) return Seq.empty[Node]

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
            Only the most recent {MAX_TIMELINE_TASKS} tasks
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
        <div>
          <form id={s"form-event-timeline-page"}
                method="get"
                action=""
                class="form-inline float-right justify-content-end"
                style="margin-bottom: 0px;">
            <label>Tasks: {totalTasks}. {totalPages} Pages. Jump to</label>
            <input type="hidden" name="id" value={stageId.toString} />
            <input type="hidden" name="attempt" value={stageAttemptId.toString} />
            <input type="text"
                   name="task.eventTimelinePageNumber"
                   id={s"form-event-timeline-page-no"}
                   value={page.toString}
                   class="col-1 form-control" />

            <label>. Show </label>
            <input type="text"
                   id={s"form-event-timeline-page-size"}
                   name="task.eventTimelinePageSize"
                   value={pageSize.toString}
                   class="col-1 form-control" />
            <label>items in a page.</label>

            <button type="submit" class="btn btn-spark">Go</button>
          </form>
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

  def executorLogs(id: String): Map[String, String] = {
    executorIdToLogs.getOrElseUpdate(id,
      store.asOption(store.executorSummary(id)).map(_.executorLogs).getOrElse(Map.empty))
  }

}

private[ui] class TaskPagedTable(
    stage: StageData,
    basePath: String,
    pageSize: Int,
    sortColumn: String,
    desc: Boolean,
    store: AppStatusStore) extends PagedTable[TaskData] {

  import ApiHelper._

  private val encodedSortColumn = URLEncoder.encode(sortColumn, UTF_8.name())

  override def tableId: String = "task-table"

  override def tableCssClass: String =
    "table table-bordered table-sm table-striped table-head-clickable"

  override def pageSizeFormField: String = "task.pageSize"

  override def pageNumberFormField: String = "task.page"

  override val dataSource: TaskDataSource = new TaskDataSource(
    stage,
    pageSize,
    sortColumn,
    desc,
    store)

  override def pageLink(page: Int): String = {
    basePath +
      s"&$pageNumberFormField=$page" +
      s"&task.sort=$encodedSortColumn" +
      s"&task.desc=$desc" +
      s"&$pageSizeFormField=$pageSize"
  }

  override def goButtonFormPath: String = s"$basePath&task.sort=$encodedSortColumn&task.desc=$desc"

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
          Seq((HEADER_SHUFFLE_READ_FETCH_WAIT_TIME,
            TaskDetailsClassNames.SHUFFLE_READ_FETCH_WAIT_TIME),
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
              s"&task.sort=${URLEncoder.encode(header, UTF_8.name())}" +
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
              s"&task.sort=${URLEncoder.encode(header, UTF_8.name())}" +
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
        <td class={TaskDetailsClassNames.SHUFFLE_READ_FETCH_WAIT_TIME}>
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
    val details = UIUtils.detailsUINode(isMultiline, error)
    <td>{errorSummary}{details}</td>
  }
}

private[spark] object ApiHelper {

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
  val HEADER_SHUFFLE_READ_FETCH_WAIT_TIME = "Shuffle Read Fetch Wait Time"
  val HEADER_SHUFFLE_TOTAL_READS = "Shuffle Read Size / Records"
  val HEADER_SHUFFLE_REMOTE_READS = "Shuffle Remote Reads"
  val HEADER_SHUFFLE_WRITE_TIME = "Shuffle Write Time"
  val HEADER_SHUFFLE_WRITE_SIZE = "Shuffle Write Size / Records"
  val HEADER_MEM_SPILL = "Spill (Memory)"
  val HEADER_DISK_SPILL = "Spill (Disk)"
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
    HEADER_SHUFFLE_READ_FETCH_WAIT_TIME -> TaskIndexNames.SHUFFLE_READ_FETCH_WAIT_TIME,
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
    // Some jobs have only 0 partitions.
    if (job.stageIds.isEmpty) {
      ("", job.name)
    } else {
      val stage = store.asOption(store.stageAttempt(job.stageIds.max, 0)._1)
      (stage.map(_.name).getOrElse(""), stage.flatMap(_.description).getOrElse(job.name))
    }
  }

}
