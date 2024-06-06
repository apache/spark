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

import scala.xml.Node

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.ui.{PagedDataSource, PagedTable, UIUtils => SparkUIUtils}

private[ui] class StreamingPagedTable(
    request: HttpServletRequest,
    tableTag: String,
    batches: Seq[BatchUIData],
    basePath: String,
    subPath: String,
    batchInterval: Long) extends PagedTable[BatchUIData] {

  private val(sortColumn, desc, pageSize) = getTableParameters(request, tableTag, "Batch Time")
  private val parameterPath = s"$basePath/$subPath/?${getParameterOtherTable(request, tableTag)}"
  private val encodedSortColumn = URLEncoder.encode(sortColumn, UTF_8.name())

  private val firstFailureReason: Option[String] =
    if (!tableTag.equals("waitingBatches")) {
      getFirstFailureReason(batches)
    } else {
      None
    }

  /**
   * Return the first failure reason if finding in the batches.
   */
  private def getFirstFailureReason(batches: Seq[BatchUIData]): Option[String] = {
    batches.flatMap(_.outputOperations.flatMap(_._2.failureReason)).headOption
  }

  private def getFirstFailureTableCell(batch: BatchUIData): Seq[Node] = {
    val firstFailureReason = batch.outputOperations.flatMap(_._2.failureReason).headOption
    firstFailureReason.map { failureReason =>
      val failureReasonForUI = UIUtils.createOutputOperationFailureForUI(failureReason)
      UIUtils.failureReasonCell(
        failureReasonForUI, rowspan = 1, includeFirstLineInExpandDetails = false)
    }.getOrElse(<td>-</td>)
  }

  private def createOutputOperationProgressBar(batch: BatchUIData): Seq[Node] = {
    <td class="progress-cell">
      {
        SparkUIUtils.makeProgressBar(
          started = batch.numActiveOutputOp,
          completed = batch.numCompletedOutputOp,
          failed = batch.numFailedOutputOp,
          skipped = 0,
          reasonToNumKilled = Map.empty,
          total = batch.outputOperations.size)
      }
    </td>
  }

  override def tableId: String = s"$tableTag-table"

  override def tableCssClass: String =
    "table table-bordered table-sm table-striped table-head-clickable table-cell-width-limited"

  override def pageSizeFormField: String = s"$tableTag.pageSize"

  override def pageNumberFormField: String = s"$tableTag.page"

  override def pageLink(page: Int): String = {
    parameterPath +
    s"&$tableTag.sort=$encodedSortColumn" +
    s"&$tableTag.desc=$desc" +
    s"&$pageNumberFormField=$page" +
    s"&$pageSizeFormField=$pageSize" +
    s"#$tableTag"
  }

  override def goButtonFormPath: String =
    s"$parameterPath&$tableTag.sort=$encodedSortColumn&$tableTag.desc=$desc#$tableTag"

  override def dataSource: PagedDataSource[BatchUIData] =
    new StreamingDataSource(batches, pageSize, sortColumn, desc)

  override def headers: Seq[Node] = {
    // headers, sortable and tooltips
    val headersAndCssClasses: Seq[(String, Boolean, Option[String])] = {
      Seq(
        ("Batch Time", true, None),
        ("Records", true, None),
        ("Scheduling Delay", true, Some("Time taken by Streaming scheduler to submit jobs " +
          "of a batch")),
        ("Processing Time", true, Some("Time taken to process all jobs of a batch"))) ++ {
        if (tableTag.equals("completedBatches")) {
          Seq(
            ("Total Delay", true, Some("Total time taken to handle a batch")),
            ("Output Ops: Succeeded/Total", false, None))
        } else {
          Seq(
            ("Output Ops: Succeeded/Total", false, None),
            ("Status", false, None))
        }
      } ++ {
        if (firstFailureReason.nonEmpty) {
          Seq(("Error", false, None))
        } else {
          Nil
        }
      }
    }
    // check if sort column is a valid sortable column
    isSortColumnValid(headersAndCssClasses, sortColumn)

    headerRow(headersAndCssClasses, desc, pageSize, sortColumn, parameterPath, tableTag, tableTag)
  }

  override def row(batch: BatchUIData): Seq[Node] = {
    val batchTime = batch.batchTime.milliseconds
    val formattedBatchTime = SparkUIUtils.formatBatchTime(batchTime, batchInterval)
    val numRecords = batch.numRecords
    val schedulingDelay = batch.schedulingDelay
    val formattedSchedulingDelay = schedulingDelay.map(SparkUIUtils.formatDuration).getOrElse("-")
    val processingTime = batch.processingDelay
    val formattedProcessingTime = processingTime.map(SparkUIUtils.formatDuration).getOrElse("-")
    val batchTimeId = s"batch-$batchTime"
    val totalDelay = batch.totalDelay
    val formattedTotalDelay = totalDelay.map(SparkUIUtils.formatDuration).getOrElse("-")

    <tr>
      <td id={batchTimeId} isFailed={batch.isFailed.toString}>
        <a href={s"batch?id=$batchTime"}>
          {formattedBatchTime}
        </a>
      </td>
      <td> {numRecords.toString} records </td>
      <td> {formattedSchedulingDelay} </td>
      <td> {formattedProcessingTime} </td>
      {
        if (tableTag.equals("completedBatches")) {
          <td> {formattedTotalDelay} </td> ++
          createOutputOperationProgressBar(batch) ++ {
            if (firstFailureReason.nonEmpty) {
              getFirstFailureTableCell(batch)
            } else {
              Nil
            }
          }
        } else if (tableTag.equals("runningBatches")) {
          createOutputOperationProgressBar(batch) ++
          <td> processing </td>  ++ {
            if (firstFailureReason.nonEmpty) {
              getFirstFailureTableCell(batch)
            } else {
              Nil
            }
          }
        } else {
          createOutputOperationProgressBar(batch) ++
          <td> queued </td> ++ {
            if (firstFailureReason.nonEmpty) {
              // Waiting batches have not run yet, so must have no failure reasons.
              <td>-</td>
            } else {
              Nil
            }
          }
        }
      }
    </tr>
  }
}

private[ui] class StreamingDataSource(info: Seq[BatchUIData], pageSize: Int, sortColumn: String,
    desc: Boolean) extends PagedDataSource[BatchUIData](pageSize) {

  private val data = info.sorted(ordering(sortColumn, desc))

  override protected def dataSize: Int = data.size

  override protected def sliceData(from: Int, to: Int): Seq[BatchUIData] = data.slice(from, to)

  private def ordering(column: String, desc: Boolean): Ordering[BatchUIData] = {
    val ordering: Ordering[BatchUIData] = column match {
      case "Batch Time" => Ordering.by(_.batchTime.milliseconds)
      case "Records" => Ordering.by(_.numRecords)
      case "Scheduling Delay" => Ordering.by(_.schedulingDelay.getOrElse(Long.MaxValue))
      case "Processing Time" => Ordering.by(_.processingDelay.getOrElse(Long.MaxValue))
      case "Total Delay" => Ordering.by(_.totalDelay.getOrElse(Long.MaxValue))
      case unknownColumn => throw new IllegalArgumentException(s"Unknown Column: $unknownColumn")
    }
    if (desc) {
      ordering.reverse
    } else {
      ordering
    }
  }
}
