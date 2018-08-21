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

import scala.collection.JavaConverters._
import scala.xml._

import org.apache.commons.lang3.StringEscapeUtils

import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1
import org.apache.spark.ui._
import org.apache.spark.util.Utils

private[ui] class StageTableBase(
    store: AppStatusStore,
    request: HttpServletRequest,
    stages: Seq[v1.StageData],
    tableHeaderID: String,
    stageTag: String,
    basePath: String,
    subPath: String,
    isFairScheduler: Boolean,
    killEnabled: Boolean,
    isFailedStage: Boolean) {
  // stripXSS is called to remove suspicious characters used in XSS attacks
  val allParameters = request.getParameterMap.asScala.toMap.map { case (k, v) =>
    UIUtils.stripXSS(k) -> v.map(UIUtils.stripXSS).toSeq
  }
  val parameterOtherTable = allParameters.filterNot(_._1.startsWith(stageTag))
    .map(para => para._1 + "=" + para._2(0))

  val parameterStagePage = UIUtils.stripXSS(request.getParameter(stageTag + ".page"))
  val parameterStageSortColumn = UIUtils.stripXSS(request.getParameter(stageTag + ".sort"))
  val parameterStageSortDesc = UIUtils.stripXSS(request.getParameter(stageTag + ".desc"))
  val parameterStagePageSize = UIUtils.stripXSS(request.getParameter(stageTag + ".pageSize"))
  val parameterStagePrevPageSize =
    UIUtils.stripXSS(request.getParameter(stageTag + ".prevPageSize"))

  val stagePage = Option(parameterStagePage).map(_.toInt).getOrElse(1)
  val stageSortColumn = Option(parameterStageSortColumn).map { sortColumn =>
    UIUtils.decodeURLParameter(sortColumn)
  }.getOrElse("Stage Id")
  val stageSortDesc = Option(parameterStageSortDesc).map(_.toBoolean).getOrElse(
    // New stages should be shown above old jobs by default.
    stageSortColumn == "Stage Id"
  )
  val stagePageSize = Option(parameterStagePageSize).map(_.toInt).getOrElse(100)
  val stagePrevPageSize = Option(parameterStagePrevPageSize).map(_.toInt)
    .getOrElse(stagePageSize)

  val page: Int = {
    // If the user has changed to a larger page size, then go to page 1 in order to avoid
    // IndexOutOfBoundsException.
    if (stagePageSize <= stagePrevPageSize) {
      stagePage
    } else {
      1
    }
  }
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
      stagePageSize,
      stageSortColumn,
      stageSortDesc,
      isFailedStage,
      parameterOtherTable,
      request
    ).table(page)
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
    val shuffleWriteWithUnit: String)

private[ui] class MissingStageTableRowData(
    stageInfo: v1.StageData,
    stageId: Int,
    attemptId: Int) extends StageTableRowData(
  stageInfo, None, stageId, attemptId, "", None, new Date(0), "", -1, "", 0, "", 0, "", 0, "", 0,
    "")

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

  override def prevPageSizeFormField: String = stageTag + ".prevPageSize"

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
    val encodedSortColumn = URLEncoder.encode(sortColumn, "UTF-8")
    parameterPath +
      s"&$pageNumberFormField=$page" +
      s"&$stageTag.sort=$encodedSortColumn" +
      s"&$stageTag.desc=$desc" +
      s"&$pageSizeFormField=$pageSize" +
      s"#$tableHeaderId"
  }

  override def goButtonFormPath: String = {
    val encodedSortColumn = URLEncoder.encode(sortColumn, "UTF-8")
    s"$parameterPath&$stageTag.sort=$encodedSortColumn&$stageTag.desc=$desc#$tableHeaderId"
  }

  override def headers: Seq[Node] = {
    // stageHeadersAndCssClasses has three parts: header title, tooltip information, and sortable.
    // The tooltip information could be None, which indicates it does not have a tooltip.
    // Otherwise, it has two parts: tooltip text, and position (true for left, false for default).
    val stageHeadersAndCssClasses: Seq[(String, Option[(String, Boolean)], Boolean)] =
      Seq(("Stage Id", None, true)) ++
      {if (isFairScheduler) {Seq(("Pool Name", None, true))} else Seq.empty} ++
      Seq(
        ("Description", None, true), ("Submitted", None, true), ("Duration", None, true),
        ("Tasks: Succeeded/Total", None, false),
        ("Input", Some((ToolTips.INPUT, false)), true),
        ("Output", Some((ToolTips.OUTPUT, false)), true),
        ("Shuffle Read", Some((ToolTips.SHUFFLE_READ, false)), true),
        ("Shuffle Write", Some((ToolTips.SHUFFLE_WRITE, true)), true)
      ) ++
      {if (isFailedStage) {Seq(("Failure Reason", None, false))} else Seq.empty}

    if (!stageHeadersAndCssClasses.filter(_._3).map(_._1).contains(sortColumn)) {
      throw new IllegalArgumentException(s"Unknown column: $sortColumn")
    }

    val headerRow: Seq[Node] = {
      stageHeadersAndCssClasses.map { case (header, tooltip, sortable) =>
        val headerSpan = tooltip.map { case (title, left) =>
          if (left) {
            /* Place the shuffle write tooltip on the left (rather than the default position
            of on top) because the shuffle write column is the last column on the right side and
            the tooltip is wider than the column, so it doesn't fit on top. */
            <span data-toggle="tooltip" data-placement="left" title={title}>
              {header}
            </span>
          } else {
            <span data-toggle="tooltip" title={title}>
              {header}
            </span>
          }
        }.getOrElse(
          {header}
        )

        if (header == sortColumn) {
          val headerLink = Unparsed(
            parameterPath +
              s"&$stageTag.sort=${URLEncoder.encode(header, "UTF-8")}" +
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
                s"&$stageTag.sort=${URLEncoder.encode(header, "UTF-8")}" +
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
        </td>
        <td>{data.inputReadWithUnit}</td>
        <td>{data.outputWriteWithUnit}</td>
        <td>{data.shuffleReadWithUnit}</td>
        <td>{data.shuffleWriteWithUnit}</td> ++
        {
          if (isFailedStage) {
            failureReasonHtml(info)
          } else {
            Seq.empty
          }
        }
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
            <a href={s"$basePathUri/storage/rdd?id=${i.id}"}>{i.name}</a>
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
    <td></td> ++ // Input
    <td></td> ++ // Output
    <td></td> ++ // Shuffle Read
    <td></td> // Shuffle Write
  }
}

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
    val description = stageData.description.getOrElse("")

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
      shuffleWriteWithUnit
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
