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

package org.apache.spark.streaming.ui

import java.net.URLEncoder
import java.nio.charset.StandardCharsets.UTF_8
import javax.servlet.http.HttpServletRequest

import scala.xml.{Node, Unparsed}

import org.apache.spark.ui.{PagedDataSource, PagedTable, UIUtils => SparkUIUtils}

private[ui] class StreamingBatchPagedTable(
    request: HttpServletRequest,
    parent: StreamingTab,
    batchInterval: Long,
    batchData: Seq[BatchUIData],
    streamingBatchTag: String,
    basePath: String,
    subPath: String,
    parameterOtherTable: Iterable[String],
    pageSize: Int,
    sortColumn: String,
    desc: Boolean) extends PagedTable[BatchUIData] {

  override val dataSource = new StreamingBatchTableDataSource(batchData, pageSize, sortColumn, desc)

  private val parameterPath = s"$basePath/$subPath/?${parameterOtherTable.mkString("&")}"

  private val firstFailureReason = getFirstFailureReason(batchData)

  override def tableId: String = streamingBatchTag

  override def tableCssClass: String =
    "table table-bordered table-condensed table-striped " +
      "table-head-clickable table-cell-width-limited"

  override def pageLink(page: Int): String = {
    val encodedSortColumn = URLEncoder.encode(sortColumn, UTF_8.name())
    parameterPath +
      s"&$pageNumberFormField=$page" +
      s"&$streamingBatchTag.sort=$encodedSortColumn" +
      s"&$streamingBatchTag.desc=$desc" +
      s"&$pageSizeFormField=$pageSize"
  }

  override def pageSizeFormField: String = s"$streamingBatchTag.pageSize"

  override def pageNumberFormField: String = s"$streamingBatchTag.page"

  override def goButtonFormPath: String = {
    val encodedSortColumn = URLEncoder.encode(sortColumn, UTF_8.name())
    s"$parameterPath&$streamingBatchTag.sort=$encodedSortColumn&$streamingBatchTag.desc=$desc"
  }

  override def headers: Seq[Node] = {
    val completedBatchTableHeaders = Seq("Batch Time", "Records", "Scheduling Delay",
      "Processing Delay", "Total Delay", "Output Ops: Succeeded/Total")

    val tooltips = Seq(None, None, Some("Time taken by Streaming scheduler to" +
      " submit jobs of a batch"), Some("Time taken to process all jobs of a batch"),
      Some("Total time taken to handle a batch"), None)

    assert(completedBatchTableHeaders.length == tooltips.length)

    val headerRow: Seq[Node] = {
      completedBatchTableHeaders.zip(tooltips).map { case (header, tooltip) =>
        if (header == sortColumn) {
          val headerLink = Unparsed(
            parameterPath +
              s"&$streamingBatchTag.sort=${URLEncoder.encode(header, UTF_8.name())}" +
              s"&$streamingBatchTag.desc=${!desc}" +
              s"&$streamingBatchTag.pageSize=$pageSize" +
              s"#$streamingBatchTag")
          val arrow = if (desc) "&#x25BE;" else "&#x25B4;" // UP or DOWN

          if (tooltip.nonEmpty) {
            <th>
              <a href={headerLink}>
                <span data-toggle="tooltip" title={tooltip.get}>
                  {header}&nbsp;{Unparsed(arrow)}
                </span>
              </a>
            </th>
          } else {
            <th>
              <a href={headerLink}>
                {header}&nbsp;{Unparsed(arrow)}
              </a>
            </th>
          }
        } else {
          val headerLink = Unparsed(
            parameterPath +
              s"&$streamingBatchTag.sort=${URLEncoder.encode(header, UTF_8.name())}" +
              s"&$streamingBatchTag.pageSize=$pageSize" +
              s"#$streamingBatchTag")

          if(tooltip.nonEmpty) {
            <th>
              <a href={headerLink}>
                <span data-toggle="tooltip" title={tooltip.get}>
                  {header}
                </span>
              </a>
            </th>
          } else {
            <th>
              <a href={headerLink}>
                {header}
              </a>
            </th>
          }
        }
      }
    }
    <thead>
      {headerRow}
    </thead>
  }

  override def row(batch: BatchUIData): Seq[Node] = {
    val batchTime = batch.batchTime.milliseconds
    val formattedBatchTime = UIUtils.formatBatchTime(batchTime, batchInterval)
    val numRecords = batch.numRecords
    val schedulingDelay = batch.schedulingDelay
    val formattedSchedulingDelay = schedulingDelay.map(SparkUIUtils.formatDuration).getOrElse("-")
    val processingTime = batch.processingDelay
    val formattedProcessingTime = processingTime.map(SparkUIUtils.formatDuration).getOrElse("-")
    val batchTimeId = s"batch-$batchTime"
    val totalDelay = batch.totalDelay
    val formattedTotalDelay = totalDelay.map(SparkUIUtils.formatDuration).getOrElse("-")

    <tr>
      <td id={batchTimeId}>
        <a href={s"batch?id=$batchTime"}>
          {formattedBatchTime}
        </a>
      </td>
      <td>
        {numRecords.toString} records
      </td>
      <td>
        {formattedSchedulingDelay}
      </td>
      <td>
        {formattedProcessingTime}
      </td>
      <td>
        {formattedTotalDelay}
      </td>
      <td class="progress-cell">
        {SparkUIUtils.makeProgressBar(started = batch.numActiveOutputOp,
        completed = batch.numCompletedOutputOp, failed = batch.numFailedOutputOp, skipped = 0,
        reasonToNumKilled = Map.empty, total = batch.outputOperations.size)}
      </td>
      {
        if (firstFailureReason.nonEmpty) {
          getFirstFailureTableCell(batch)
        } else {
          Nil
        }
      }
    </tr>
  }

  protected def getFirstFailureReason(batches: Seq[BatchUIData]): Option[String] = {
    batches.flatMap(_.outputOperations.flatMap(_._2.failureReason)).headOption
  }

  protected def getFirstFailureTableCell(batch: BatchUIData): Seq[Node] = {
    val firstFailureReason = batch.outputOperations.flatMap(_._2.failureReason).headOption
    firstFailureReason.map { failureReason =>
      val failureReasonForUI = UIUtils.createOutputOperationFailureForUI(failureReason)
      UIUtils.failureReasonCell(
        failureReasonForUI, rowspan = 1, includeFirstLineInExpandDetails = false)
    }.getOrElse(<td>-</td>)
  }
}

private[ui] class StreamingBatchTableDataSource(
    info: Seq[BatchUIData],
    pageSize: Int,
    sortColumn: String,
    desc: Boolean) extends PagedDataSource[BatchUIData](pageSize) {

  private val data = info.sorted(ordering(sortColumn, desc))

  private var _slicedStartTime: Set[Long] = null

  override def dataSize: Int = data.size

  override def sliceData(from: Int, to: Int): Seq[BatchUIData] = {
    val r = data.slice(from, to)
    _slicedStartTime = r.map(_.batchTime.milliseconds).toSet
    r
  }

  /**
   * Return Ordering according to sortColumn and desc.
   */
  private def ordering(sortColumn: String, desc: Boolean): Ordering[BatchUIData] = {
    val ordering: Ordering[BatchUIData] = sortColumn match {
      case "Batch Time" => Ordering.by(_.batchTime)
      case "Records" => Ordering.by(_.numRecords)
      case "Scheduling Delay" => Ordering.by(_.schedulingDelay)
      case "Processing Delay" => Ordering.by(_.processingDelay)
      case "Total Delay" => Ordering.by(_.totalDelay)
      case "Output Ops: Succeeded/Total" => Ordering.by(_.batchTime)
      case unknownColumn => throw new IllegalArgumentException(s"Unknown column: $unknownColumn")
    }
    if (desc) {
      ordering.reverse
    } else {
      ordering
    }
  }
}
