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
import java.util.{Date, Locale}

import javax.servlet.http.HttpServletRequest

import scala.collection.JavaConverters._
import scala.collection.immutable.{ListMap, ListSet}
import scala.xml._
import org.apache.commons.text.StringEscapeUtils
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1
import org.apache.spark.ui._
import org.apache.spark.ui.jobs.StagePagedTable._
import org.apache.spark.util.Utils

private[ui] class StageTableBase(
    store: AppStatusStore,
    request: HttpServletRequest,
    stages: Seq[v1.StageData],
    tableHeaderID: String,
    stageTag: String,
    basePath: String,
    subPath: String,
    metrics: ListSet[StageTableMetric],
    isFairScheduler: Boolean,
    killEnabled: Boolean,
    isFailedStage: Boolean) {
  val parameterOtherTable = request.getParameterMap().asScala
    .filterNot(_._1.startsWith(stageTag))
    .map(para => para._1 + "=" + para._2(0))

  val parameterStagePage = request.getParameter(stageTag + ".page")
  val parameterStageSortColumn = request.getParameter(stageTag + ".sort")
  val parameterStageSortDesc = request.getParameter(stageTag + ".desc")
  val parameterStagePageSize = request.getParameter(stageTag + ".pageSize")

  val stagePage = Option(parameterStagePage).map(_.toInt).getOrElse(1)
  val stageSortColumn = Option(parameterStageSortColumn).map { sortColumn =>
    UIUtils.decodeURLParameter(sortColumn)
  }.getOrElse("Stage Id")
  val stageSortDesc = Option(parameterStageSortDesc).map(_.toBoolean).getOrElse(
    // New stages should be shown above old jobs by default.
    stageSortColumn == "Stage Id"
  )
  val stagePageSize = Option(parameterStagePageSize).map(_.toInt).getOrElse(100)

  val currentTime = System.currentTimeMillis()

  val toNodeSeq = try {
    new StagePagedTable(
      store,
      stages,
      tableHeaderID,
      stageTag,
      basePath,
      subPath,
      isFairScheduler,
      killEnabled,
      currentTime,
      metrics,
      stagePageSize,
      stageSortColumn,
      stageSortDesc,
      isFailedStage,
      parameterOtherTable,
      request
    ).table(stagePage)
  } catch {
    case e @ (_ : IllegalArgumentException | _ : IndexOutOfBoundsException) =>
      <div class="alert alert-error">
        <p>Error while rendering stage table:</p>
        <pre>
          {Utils.exceptionString(e)}
        </pre>
      </div>
  }
}

private[ui] class StageTableRowData(
    val stage: v1.StageData,
    val option: Option[v1.StageData],
    val stageId: Int,
    val attemptId: Int,
    val schedulingPool: String,
    val descriptionOption: Option[String],
    val submissionTime: Date,
    val formattedSubmissionTime: String,
    val duration: Long,
    val formattedDuration: String,
    val inputRead: Long,
    val inputReadWithUnit: String,
    val outputWrite: Long,
    val outputWriteWithUnit: String,
    val shuffleRead: Long,
    val shuffleReadWithUnit: String,
    val shuffleWrite: Long,
    val shuffleWriteWithUnit: String,
    val peakExecutionMemory: Long,
    val peakExecutionMemoryWithUnit: String,
    val memoryBytesSpilled: Long,
    val memoryBytesSpilledWithUnit: String,
    val diskBytesSpilled: Long,
    val diskBytesSpilledWithUnit: String,
    val jvmGcTime: Long,
    val formattedJvmGcTime: String)

private[ui] class MissingStageTableRowData(
    stageInfo: v1.StageData,
    stageId: Int,
    attemptId: Int) extends StageTableRowData(
  stageInfo, None, stageId, attemptId, "", None, new Date(0), "", -1, "", 0, "", 0, "", 0, "", 0,
    "", 0, "", 0, "", 0, "", 0, "")

/** Page showing list of all ongoing and recently finished stages */
private[ui] class StagePagedTable(
    store: AppStatusStore,
    stages: Seq[v1.StageData],
    tableHeaderId: String,
    stageTag: String,
    basePath: String,
    subPath: String,
    isFairScheduler: Boolean,
    killEnabled: Boolean,
    currentTime: Long,
    metrics: ListSet[StageTableMetric],
    pageSize: Int,
    sortColumn: String,
    desc: Boolean,
    isFailedStage: Boolean,
    parameterOtherTable: Iterable[String],
    request: HttpServletRequest) extends PagedTable[StageTableRowData] {

  override def tableId: String = stageTag + "-table"

  override def tableCssClass: String =
    "table table-bordered table-condensed table-striped " +
      "table-head-clickable table-cell-width-limited"

  override def pageSizeFormField: String = stageTag + ".pageSize"

  override def pageNumberFormField: String = stageTag + ".page"

  val parameterPath = UIUtils.prependBaseUri(request, basePath) + s"/$subPath/?" +
    parameterOtherTable.mkString("&")

  override val dataSource = new StageDataSource(
    store,
    stages,
    currentTime,
    pageSize,
    sortColumn,
    desc
  )

  override def pageLink(page: Int): String = {
    val encodedSortColumn = URLEncoder.encode(sortColumn, UTF_8.name())
    parameterPath +
      s"&$pageNumberFormField=$page" +
      s"&$stageTag.sort=$encodedSortColumn" +
      s"&$stageTag.desc=$desc" +
      s"&$pageSizeFormField=$pageSize" +
      s"#$tableHeaderId"
  }

  override def goButtonFormPath: String = {
    val encodedSortColumn = URLEncoder.encode(sortColumn, UTF_8.name())
    s"$parameterPath&$stageTag.sort=$encodedSortColumn&$stageTag.desc=$desc#$tableHeaderId"
  }

  override def headers: Seq[Node] = {
    if (metrics.exists(m => !optionalMetricsMap.contains(m.name))) {
      throw new IllegalArgumentException(
        s"Unknown metric: ${metrics.map(_.name).diff(optionalMetricsMap.keySet).mkString(", ")}"
      )
    }

    // stageHeadersAndCssClasses has three parts: header title, tooltip information, and sortable.
    // The tooltip information could be None, which indicates it does not have a tooltip.
    // Otherwise, it has two parts: tooltip text, and position (true for left, false for default).
    val stageHeadersAndCssClasses: Seq[(String, String, Boolean)] =
      Seq(("Stage Id", null, true)) ++
      {if (isFairScheduler) {Seq(("Pool Name", null, true))} else Seq.empty} ++
      Seq(
        ("Description", null, true),
        ("Submitted", null, true),
        ("Duration", ToolTips.DURATION, true),
        ("Tasks: Succeeded/Total", null, false)
      ) ++
        metrics.map(metric => (metric.name, metric.tooltip, metric.sortable)) ++
      {if (isFailedStage) {Seq(("Failure Reason", null, false))} else Seq.empty}

    if (!stageHeadersAndCssClasses.filter(_._3).map(_._1).contains(sortColumn)) {
      throw new IllegalArgumentException(s"Unknown column: $sortColumn")
    }

    val headerRow: Seq[Node] = {
      stageHeadersAndCssClasses.map { case (header, tooltip, sortable) =>
        val headerSpan = if (null != tooltip && !tooltip.isEmpty) {
            <span data-toggle="tooltip" data-placement="top" title={tooltip}>
              {header}
            </span>
        } else {
          {header}
        }

        if (header == sortColumn) {
          val headerLink = Unparsed(
            parameterPath +
              s"&$stageTag.sort=${URLEncoder.encode(header, UTF_8.name())}" +
              s"&$stageTag.desc=${!desc}" +
              s"&$stageTag.pageSize=$pageSize") +
              s"#$tableHeaderId"
          val arrow = if (desc) "&#x25BE;" else "&#x25B4;" // UP or DOWN

          <th>
            <a href={headerLink}>
              {headerSpan}<span>
              &nbsp;{Unparsed(arrow)}
            </span>
            </a>
          </th>
        } else {
          if (sortable) {
            val headerLink = Unparsed(
              parameterPath +
                s"&$stageTag.sort=${URLEncoder.encode(header, UTF_8.name())}" +
                s"&$stageTag.pageSize=$pageSize") +
                s"#$tableHeaderId"

            <th>
              <a href={headerLink}>
                {headerSpan}
              </a>
            </th>
          } else {
            <th>
              {headerSpan}
            </th>
          }
        }
      }
    }
    <thead>{headerRow}</thead>
  }

  override def row(data: StageTableRowData): Seq[Node] = {
    <tr id={"stage-" + data.stageId + "-" + data.attemptId}>
      {rowContent(data)}
    </tr>
  }

  private def rowContent(data: StageTableRowData): Seq[Node] = {
    data.option match {
      case None => missingStageRow(data.stageId)
      case Some(stageData) =>
        val info = data.stage

        {if (data.attemptId > 0) {
          <td>{data.stageId} (retry {data.attemptId})</td>
        } else {
          <td>{data.stageId}</td>
        }} ++
        {if (isFairScheduler) {
          <td>
            <a href={"%s/stages/pool?poolname=%s"
              .format(UIUtils.prependBaseUri(request, basePath), data.schedulingPool)}>
              {data.schedulingPool}
            </a>
          </td>
        } else {
          Seq.empty
        }} ++
        <td>{makeDescription(info, data.descriptionOption)}</td>
        <td valign="middle">
          {data.formattedSubmissionTime}
        </td>
        <td>{data.formattedDuration}</td>
        <td class="progress-cell">
          {UIUtils.makeProgressBar(started = stageData.numActiveTasks,
          completed = stageData.numCompleteTasks, failed = stageData.numFailedTasks,
          skipped = 0, reasonToNumKilled = stageData.killedTasksSummary, total = info.numTasks)}
        </td> ++
        metrics.toSeq.map( metric =>
          <td>{metric.value(data)}</td>
        ) ++
        {if (isFailedStage) {
          failureReasonHtml(info)
        } else {
          Seq.empty
        }}
    }
  }

  private def failureReasonHtml(s: v1.StageData): Seq[Node] = {
    val failureReason = s.failureReason.getOrElse("")
    val isMultiline = failureReason.indexOf('\n') >= 0
    // Display the first line by default
    val failureReasonSummary = StringEscapeUtils.escapeHtml4(
      if (isMultiline) {
        failureReason.substring(0, failureReason.indexOf('\n'))
      } else {
        failureReason
      })
    val details = if (isMultiline) {
      // scalastyle:off
      <span onclick="this.parentNode.querySelector('.stacktrace-details').classList.toggle('collapsed')"
            class="expand-details">
        +details
      </span> ++
        <div class="stacktrace-details collapsed">
          <pre>{failureReason}</pre>
        </div>
      // scalastyle:on
    } else {
      ""
    }
    <td valign="middle">{failureReasonSummary}{details}</td>
  }

  private def makeDescription(s: v1.StageData, descriptionOption: Option[String]): Seq[Node] = {
    val basePathUri = UIUtils.prependBaseUri(request, basePath)

    val killLink = if (killEnabled) {
      val confirm =
        s"if (window.confirm('Are you sure you want to kill stage ${s.stageId} ?')) " +
        "{ this.parentNode.submit(); return true; } else { return false; }"
      // SPARK-6846 this should be POST-only but YARN AM won't proxy POST
      /*
      val killLinkUri = s"$basePathUri/stages/stage/kill/"
      <form action={killLinkUri} method="POST" style="display:inline">
        <input type="hidden" name="id" value={s.stageId.toString}/>
        <a href="#" onclick={confirm} class="kill-link">(kill)</a>
      </form>
       */
      val killLinkUri = s"$basePathUri/stages/stage/kill/?id=${s.stageId}"
      <a href={killLinkUri} onclick={confirm} class="kill-link">(kill)</a>
    } else {
      Seq.empty
    }

    val nameLinkUri = s"$basePathUri/stages/stage/?id=${s.stageId}&attempt=${s.attemptId}"
    val nameLink = <a href={nameLinkUri} class="name-link">{s.name}</a>

    val cachedRddInfos = store.rddList().filter { rdd => s.rddIds.contains(rdd.id) }
    val details = if (s.details != null && s.details.nonEmpty) {
      <span onclick="this.parentNode.querySelector('.stage-details').classList.toggle('collapsed')"
            class="expand-details">
        +details
      </span> ++
      <div class="stage-details collapsed">
        {if (cachedRddInfos.nonEmpty) {
          Text("RDD: ") ++
          cachedRddInfos.map { i =>
            <a href={s"$basePathUri/storage/rdd/?id=${i.id}"}>{i.name}</a>
          }
        }}
        <pre>{s.details}</pre>
      </div>
    }

    val stageDesc = descriptionOption.map(UIUtils.makeDescription(_, basePathUri))
    <div>{stageDesc.getOrElse("")} {killLink} {nameLink} {details}</div>
  }

  protected def missingStageRow(stageId: Int): Seq[Node] = {
    <td>{stageId}</td> ++
    {if (isFairScheduler) {<td>-</td>} else Seq.empty} ++
    <td>No data available for this stage</td> ++ // Description
    <td></td> ++ // Submitted
    <td></td> ++ // Duration
    <td></td> ++ // Tasks: Succeeded/Total
    {metrics.map(_ =>
      <td></td> // selected metrics
    )}
  }
}

private[ui] object StagePagedTable {

  val inputMetric = StageTableMetric("Input", ToolTips.INPUT, true, _.inputReadWithUnit)
  val outputMetric = StageTableMetric("Output", ToolTips.OUTPUT, true, _.outputWriteWithUnit)
  val shuffleReadMetric = StageTableMetric("Shuffle Read",
    ToolTips.SHUFFLE_READ, true, _.shuffleReadWithUnit)
  val shuffleWriteMetric = StageTableMetric("Shuffle Write",
    ToolTips.SHUFFLE_WRITE, true, _.shuffleWriteWithUnit)
  val peakExecutionMemoryMetric = StageTableMetric("Peak Execution Memory",
    ToolTips.PEAK_EXECUTION_MEMORY, true, _.peakExecutionMemoryWithUnit)
  val spillMemoryMetric = StageTableMetric("Spill (Memory)",
    ToolTips.SHUFFLE_READ, true, _.memoryBytesSpilledWithUnit)  // TODO
  val spillDiskMetric = StageTableMetric("Spill (Disk)",
    ToolTips.SHUFFLE_WRITE, true, _.diskBytesSpilledWithUnit)  // TODO
  val gcTimeMetric = StageTableMetric("GC Time", ToolTips.GC_TIME, true, _.formattedJvmGcTime)

  val optionalMetrics = ListSet(
    inputMetric, outputMetric,
    shuffleReadMetric, shuffleWriteMetric,
    peakExecutionMemoryMetric,
    spillMemoryMetric, spillDiskMetric,
    gcTimeMetric
  )

  val optionalMetricsMap = ListMap(optionalMetrics.toSeq.map(m => m.name -> m): _*)

  val defaultMetrics = ListSet(
    inputMetric, outputMetric,
    shuffleReadMetric, shuffleWriteMetric
  )

  def selectedMetrics(request: HttpServletRequest): ListSet[StageTableMetric] = {
    // this must reserve order of optionalMetrics
    val parameterMetrics = optionalMetrics.map(m => m -> request.getParameter(s"metric.${m.name}"))
    parameterMetrics.filter(m => Option(m._2).isDefined) match {
      case Seq() => defaultMetrics
      case seq => seq.filter(_._2.equalsIgnoreCase("true")).map(_._1)
    }
  }

  def additionalMetrics(shownMetrics: ListSet[StageTableMetric],
                        request: HttpServletRequest): Seq[Node] = {

    val parameterExceptMetrics = request.getParameterMap().asScala
      .filterNot(_._1.startsWith("metric."))
      .map(para => para._1 + "=" + para._2(0))
    val parameterPath = "?" + parameterExceptMetrics.mkString("&")

    val shownMetricNames = shownMetrics.map(_.name)
    <div id="parent-container">
      <script src={UIUtils.prependBaseUri(request, "/static/utils.js")}></script>
      <script src={UIUtils.prependBaseUri(request, "/static/stagetable.js")}></script>
    </div>
    <div><a id='additionalMetrics'>
      <span class='expand-input-rate-arrow arrow-closed' id='arrowtoggle1'></span>
      Show Additional Metrics
    </a></div>
    <div class='container-fluid container-fluid-div' id='toggle-metrics' hidden="true">
      <div id='select_all' class='select-all-checkbox-div'>
        <input type='checkbox' class='toggle-vis'> Select All</input>
      </div>
      {StagePagedTable.optionalMetrics.map{metric =>
      val checked = Option(shownMetricNames.contains(metric.name))
        .filter(identity).map(_ => "checked")
      def toggleMetric(m: StageTableMetric): Boolean =
        shownMetricNames.contains(m.name) ^ metric.name == m.name
      val metricLink =
        Unparsed(
          parameterPath +
            StagePagedTable.optionalMetrics.map(m =>
              s"&metric.${URLEncoder.encode(m.name, UTF_8.name())}=${toggleMetric(m)}"
            ).mkString("")
        )
      val id = metric.name.toLowerCase(Locale.ROOT).replaceAll(" ", "_")
      <div id={id}>
        <a href={metricLink}>
          <input type='checkbox' class='toggle-vis' checked={checked.orNull}> {metric.name}</input>
        </a>
      </div>
    }}
    </div>
  }
}

private[ui] case class StageTableMetric(name: String,
                                        tooltip: String,
                                        sortable: Boolean,
                                        value: StageTableRowData => Any)

private[ui] class StageDataSource(
    store: AppStatusStore,
    stages: Seq[v1.StageData],
    currentTime: Long,
    pageSize: Int,
    sortColumn: String,
    desc: Boolean) extends PagedDataSource[StageTableRowData](pageSize) {
  // Convert v1.StageData to StageTableRowData which contains the final contents to show in the
  // table so that we can avoid creating duplicate contents during sorting the data
  private val data = stages.map(stageRow).sorted(ordering(sortColumn, desc))

  private var _slicedStageIds: Set[Int] = _

  override def dataSize: Int = data.size

  override def sliceData(from: Int, to: Int): Seq[StageTableRowData] = {
    val r = data.slice(from, to)
    _slicedStageIds = r.map(_.stageId).toSet
    r
  }

  private def stageRow(stageData: v1.StageData): StageTableRowData = {
    val formattedSubmissionTime = stageData.submissionTime match {
      case Some(t) => UIUtils.formatDate(t)
      case None => "Unknown"
    }
    val finishTime = stageData.completionTime.map(_.getTime()).getOrElse(currentTime)

    // The submission time for a stage is misleading because it counts the time
    // the stage waits to be launched. (SPARK-10930)
    val duration = stageData.firstTaskLaunchedTime.map { date =>
      val time = date.getTime()
      if (finishTime > time) {
        finishTime - time
      } else {
        None
        currentTime - time
      }
    }
    val formattedDuration = duration.map(d => UIUtils.formatDuration(d)).getOrElse("Unknown")

    val inputRead = stageData.inputBytes
    val inputReadWithUnit = if (inputRead > 0) Utils.bytesToString(inputRead) else ""
    val outputWrite = stageData.outputBytes
    val outputWriteWithUnit = if (outputWrite > 0) Utils.bytesToString(outputWrite) else ""
    val shuffleRead = stageData.shuffleReadBytes
    val shuffleReadWithUnit = if (shuffleRead > 0) Utils.bytesToString(shuffleRead) else ""
    val shuffleWrite = stageData.shuffleWriteBytes
    val shuffleWriteWithUnit = if (shuffleWrite > 0) Utils.bytesToString(shuffleWrite) else ""
    val peakExecMemory = stageData.peakExecutionMemory
    val peakExecMemoryWithUnit = if (peakExecMemory > 0) Utils.bytesToString(peakExecMemory) else ""
    val memorySpilled = stageData.memoryBytesSpilled
    val memorySpilledWithUnit = if (memorySpilled > 0) Utils.bytesToString(memorySpilled) else ""
    val diskSpilled = stageData.diskBytesSpilled
    val diskSpilledWithUnit = if (diskSpilled > 0) Utils.bytesToString(diskSpilled) else ""
    val jvmGcTime = stageData.jvmGcTime
    val formattedJvmGcTime = if (jvmGcTime > 0) UIUtils.formatDuration(jvmGcTime) else ""
    // TODO: add stageData.executorRunTime + stageData.executorCpuTime to jvm: Run / CPU / GC Time
    // SPARK-26109: Duration of task as executorRunTime to make it consistent with the
    // aggregated tasks summary metrics table and the previous versions of Spark.


    new StageTableRowData(
      stageData,
      Some(stageData),
      stageData.stageId,
      stageData.attemptId,
      stageData.schedulingPool,
      stageData.description,
      stageData.submissionTime.getOrElse(new Date(0)),
      formattedSubmissionTime,
      duration.getOrElse(-1),
      formattedDuration,
      inputRead,
      inputReadWithUnit,
      outputWrite,
      outputWriteWithUnit,
      shuffleRead,
      shuffleReadWithUnit,
      shuffleWrite,
      shuffleWriteWithUnit,
      peakExecMemory,
      peakExecMemoryWithUnit,
      memorySpilled,
      memorySpilledWithUnit,
      diskSpilled,
      diskSpilledWithUnit,
      jvmGcTime,
      formattedJvmGcTime
    )
  }

  /**
   * Return Ordering according to sortColumn and desc
   */
  private def ordering(sortColumn: String, desc: Boolean): Ordering[StageTableRowData] = {
    val ordering: Ordering[StageTableRowData] = sortColumn match {
      case "Stage Id" => Ordering.by(_.stageId)
      case "Pool Name" => Ordering.by(_.schedulingPool)
      case "Description" => Ordering.by(x => (x.descriptionOption, x.stage.name))
      case "Submitted" => Ordering.by(_.submissionTime)
      case "Duration" => Ordering.by(_.duration)
      case "Input" => Ordering.by(_.inputRead)
      case "Output" => Ordering.by(_.outputWrite)
      case "Shuffle Read" => Ordering.by(_.shuffleRead)
      case "Shuffle Write" => Ordering.by(_.shuffleWrite)
      case "Peak Execution Memory" => Ordering.by(_.peakExecutionMemory)
      case "Spill (Memory)" => Ordering.by(_.memoryBytesSpilled)
      case "Spill (Disk)" => Ordering.by(_.diskBytesSpilled)
      case "GC Time" => Ordering.by(_.jvmGcTime)
      case "Tasks: Succeeded/Total" =>
        throw new IllegalArgumentException(s"Unsortable column: $sortColumn")
      case unknownColumn => throw new IllegalArgumentException(s"Unknown column: $unknownColumn")
    }
    if (desc) {
      ordering.reverse
    } else {
      ordering
    }
  }
}
