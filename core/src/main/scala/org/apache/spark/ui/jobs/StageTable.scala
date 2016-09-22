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

import org.apache.spark.scheduler.StageInfo
import org.apache.spark.ui._
import org.apache.spark.ui.jobs.UIData.StageUIData
import org.apache.spark.util.Utils

private[ui] class StageTableBase(
    request: HttpServletRequest,
    stages: Seq[StageInfo],
    stageTag: String,
    basePath: String,
    subPath: String,
    progressListener: JobProgressListener,
    isFairScheduler: Boolean,
    killEnabled: Boolean,
    isFailedStage: Boolean) {
  val allParameters = request.getParameterMap().asScala.toMap
  val parameterOtherTable = allParameters.filterNot(_._1.startsWith(stageTag))
    .map(para => para._1 + "=" + para._2(0))

  val parameterStagePage = request.getParameter(stageTag + ".page")
  val parameterStageSortColumn = request.getParameter(stageTag + ".sort")
  val parameterStageSortDesc = request.getParameter(stageTag + ".desc")
  val parameterStagePageSize = request.getParameter(stageTag + ".pageSize")
  val parameterStagePrevPageSize = request.getParameter(stageTag + ".prevPageSize")

  val stagePage = Option(parameterStagePage).map(_.toInt).getOrElse(1)
  val stageSortColumn = Option(parameterStageSortColumn).map { sortColumn =>
    UIUtils.decodeURLParameter(sortColumn)
  }.getOrElse("Stage Id")
  val stageSortDesc = Option(parameterStageSortDesc).map(_.toBoolean).getOrElse(
    // New stages should be shown above old jobs by default.
    if (stageSortColumn == "Stage Id") true else false
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
      stages,
      stageTag,
      basePath,
      subPath,
      progressListener,
      isFairScheduler,
      killEnabled,
      currentTime,
      stagePageSize,
      stageSortColumn,
      stageSortDesc,
      isFailedStage,
      parameterOtherTable
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
    val stageInfo: StageInfo,
    val stageData: Option[StageUIData],
    val stageId: Int,
    val attemptId: Int,
    val schedulingPool: String,
    val description: String,
    val descriptionOption: Option[String],
    val submissionTime: Long,
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
    stageInfo: StageInfo,
    stageId: Int,
    attemptId: Int) extends StageTableRowData(
  stageInfo, None, stageId, attemptId, "", "", None, 0, "", -1, "", 0, "", 0, "", 0, "", 0, "")

/** Page showing list of all ongoing and recently finished stages */
private[ui] class StagePagedTable(
    stages: Seq[StageInfo],
    stageTag: String,
    basePath: String,
    subPath: String,
    listener: JobProgressListener,
    isFairScheduler: Boolean,
    killEnabled: Boolean,
    currentTime: Long,
    pageSize: Int,
    sortColumn: String,
    desc: Boolean,
    isFailedStage: Boolean,
    parameterOtherTable: Iterable[String]) extends PagedTable[StageTableRowData] {

  override def tableId: String = stageTag + "-table"

  override def tableCssClass: String =
    "table table-bordered table-condensed table-striped table-head-clickable"

  override def pageSizeFormField: String = stageTag + ".pageSize"

  override def prevPageSizeFormField: String = stageTag + ".prevPageSize"

  override def pageNumberFormField: String = stageTag + ".page"

  val parameterPath = UIUtils.prependBaseUri(basePath) + s"/$subPath/?" +
    parameterOtherTable.mkString("&")

  override val dataSource = new StageDataSource(
    stages,
    listener,
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
      s"&$pageSizeFormField=$pageSize"
  }

  override def goButtonFormPath: String = {
    val encodedSortColumn = URLEncoder.encode(sortColumn, "UTF-8")
    s"$parameterPath&$stageTag.sort=$encodedSortColumn&$stageTag.desc=$desc"
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
              s"&$stageTag.pageSize=$pageSize")
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
                s"&$stageTag.pageSize=$pageSize")

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
    data.stageData match {
      case None => missingStageRow(data.stageId)
      case Some(stageData) =>
        val info = data.stageInfo

        {if (data.attemptId > 0) {
          <td>{data.stageId} (retry {data.attemptId})</td>
        } else {
          <td>{data.stageId}</td>
        }} ++
        {if (isFairScheduler) {
          <td>
            <a href={"%s/stages/pool?poolname=%s"
              .format(UIUtils.prependBaseUri(basePath), data.schedulingPool)}>
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
          completed = stageData.completedIndices.size, failed = stageData.numFailedTasks,
          skipped = 0, killed = stageData.numKilledTasks, total = info.numTasks)}
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

  private def failureReasonHtml(s: StageInfo): Seq[Node] = {
    val failureReason = s.failureReason.getOrElse("")
    val logUrlMap = s.logUrlMap
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
    val logUrls = logUrlMap.map(_.flatMap(x => Seq(<a href={x._2}>{x._1}</a>, "    ")))
    <td valign="middle"><p>{logUrls.getOrElse("")}</p>{failureReasonSummary}{details}</td>
  }

  private def makeDescription(s: StageInfo, descriptionOption: Option[String]): Seq[Node] = {
    val basePathUri = UIUtils.prependBaseUri(basePath)

    val killLink = if (killEnabled) {
      val confirm =
        s"if (window.confirm('Are you sure you want to kill stage ${s.stageId} ?')) " +
        "{ this.parentNode.submit(); return true; } else { return false; }"
      // SPARK-6846 this should be POST-only but YARN AM won't proxy POST
      /*
      val killLinkUri = s"$basePathUri/stages/stage/kill/"
      <form action={killLinkUri} method="POST" style="display:inline">
        <input type="hidden" name="id" value={s.stageId.toString}/>
        <input type="hidden" name="terminate" value="true"/>
        <a href="#" onclick={confirm} class="kill-link">(kill)</a>
      </form>
       */
      val killLinkUri = s"$basePathUri/stages/stage/kill/?id=${s.stageId}&terminate=true"
      <a href={killLinkUri} onclick={confirm} class="kill-link">(kill)</a>
    }

    val nameLinkUri = s"$basePathUri/stages/stage?id=${s.stageId}&attempt=${s.attemptId}"
    val nameLink = <a href={nameLinkUri} class="name-link">{s.name}</a>

    val cachedRddInfos = s.rddInfos.filter(_.numCachedPartitions > 0)
    val details = if (s.details.nonEmpty) {
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
    stages: Seq[StageInfo],
    listener: JobProgressListener,
    currentTime: Long,
    pageSize: Int,
    sortColumn: String,
    desc: Boolean) extends PagedDataSource[StageTableRowData](pageSize) {
  // Convert StageInfo to StageTableRowData which contains the final contents to show in the table
  // so that we can avoid creating duplicate contents during sorting the data
  private val data = stages.map(stageRow).sorted(ordering(sortColumn, desc))

  private var _slicedStageIds: Set[Int] = null

  override def dataSize: Int = data.size

  override def sliceData(from: Int, to: Int): Seq[StageTableRowData] = {
    val r = data.slice(from, to)
    _slicedStageIds = r.map(_.stageId).toSet
    r
  }

  private def stageRow(s: StageInfo): StageTableRowData = {
    val stageDataOption = listener.stageIdToData.get((s.stageId, s.attemptId))

    if (stageDataOption.isEmpty) {
      return new MissingStageTableRowData(s, s.stageId, s.attemptId)
    }
    val stageData = stageDataOption.get

    val description = stageData.description

    val formattedSubmissionTime = s.submissionTime match {
      case Some(t) => UIUtils.formatDate(new Date(t))
      case None => "Unknown"
    }
    val finishTime = s.completionTime.getOrElse(currentTime)

    // The submission time for a stage is misleading because it counts the time
    // the stage waits to be launched. (SPARK-10930)
    val taskLaunchTimes =
      stageData.taskData.values.map(_.taskInfo.launchTime).filter(_ > 0)
    val duration: Option[Long] =
      if (taskLaunchTimes.nonEmpty) {
        val startTime = taskLaunchTimes.min
        if (finishTime > startTime) {
          Some(finishTime - startTime)
        } else {
          Some(currentTime - startTime)
        }
      } else {
        None
      }
    val formattedDuration = duration.map(d => UIUtils.formatDuration(d)).getOrElse("Unknown")

    val inputRead = stageData.inputBytes
    val inputReadWithUnit = if (inputRead > 0) Utils.bytesToString(inputRead) else ""
    val outputWrite = stageData.outputBytes
    val outputWriteWithUnit = if (outputWrite > 0) Utils.bytesToString(outputWrite) else ""
    val shuffleRead = stageData.shuffleReadTotalBytes
    val shuffleReadWithUnit = if (shuffleRead > 0) Utils.bytesToString(shuffleRead) else ""
    val shuffleWrite = stageData.shuffleWriteBytes
    val shuffleWriteWithUnit = if (shuffleWrite > 0) Utils.bytesToString(shuffleWrite) else ""


    new StageTableRowData(
      s,
      stageDataOption,
      s.stageId,
      s.attemptId,
      stageData.schedulingPool,
      description.getOrElse(""),
      description,
      s.submissionTime.getOrElse(0),
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
    val ordering = sortColumn match {
      case "Stage Id" => new Ordering[StageTableRowData] {
        override def compare(x: StageTableRowData, y: StageTableRowData): Int =
          Ordering.Int.compare(x.stageId, y.stageId)
      }
      case "Pool Name" => new Ordering[StageTableRowData] {
        override def compare(x: StageTableRowData, y: StageTableRowData): Int =
          Ordering.String.compare(x.schedulingPool, y.schedulingPool)
      }
      case "Description" => new Ordering[StageTableRowData] {
        override def compare(x: StageTableRowData, y: StageTableRowData): Int =
          Ordering.String.compare(x.description, y.description)
      }
      case "Submitted" => new Ordering[StageTableRowData] {
        override def compare(x: StageTableRowData, y: StageTableRowData): Int =
          Ordering.Long.compare(x.submissionTime, y.submissionTime)
      }
      case "Duration" => new Ordering[StageTableRowData] {
        override def compare(x: StageTableRowData, y: StageTableRowData): Int =
          Ordering.Long.compare(x.duration, y.duration)
      }
      case "Input" => new Ordering[StageTableRowData] {
        override def compare(x: StageTableRowData, y: StageTableRowData): Int =
          Ordering.Long.compare(x.inputRead, y.inputRead)
      }
      case "Output" => new Ordering[StageTableRowData] {
        override def compare(x: StageTableRowData, y: StageTableRowData): Int =
          Ordering.Long.compare(x.outputWrite, y.outputWrite)
      }
      case "Shuffle Read" => new Ordering[StageTableRowData] {
        override def compare(x: StageTableRowData, y: StageTableRowData): Int =
          Ordering.Long.compare(x.shuffleRead, y.shuffleRead)
      }
      case "Shuffle Write" => new Ordering[StageTableRowData] {
        override def compare(x: StageTableRowData, y: StageTableRowData): Int =
          Ordering.Long.compare(x.shuffleWrite, y.shuffleWrite)
      }
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

