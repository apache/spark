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

import scala.xml.Node

import org.apache.spark.ui.{UIUtils => SparkUIUtils}

private[ui] abstract class BatchTableBase(tableId: String, batchInterval: Long) {

  protected def columns: Seq[Node] = {
    <th>Batch Time</th>
      <th>Records</th>
      <th>Scheduling Delay
        {SparkUIUtils.tooltip("Time taken by Streaming scheduler to submit jobs of a batch", "top")}
      </th>
      <th>Processing Time
        {SparkUIUtils.tooltip("Time taken to process all jobs of a batch", "top")}</th>
  }

  /**
   * Return the first failure reason if finding in the batches.
   */
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

  protected def baseRow(batch: BatchUIData): Seq[Node] = {
    val batchTime = batch.batchTime.milliseconds
    val formattedBatchTime = UIUtils.formatBatchTime(batchTime, batchInterval)
    val numRecords = batch.numRecords
    val schedulingDelay = batch.schedulingDelay
    val formattedSchedulingDelay = schedulingDelay.map(SparkUIUtils.formatDuration).getOrElse("-")
    val processingTime = batch.processingDelay
    val formattedProcessingTime = processingTime.map(SparkUIUtils.formatDuration).getOrElse("-")
    val batchTimeId = s"batch-$batchTime"

    <td id={batchTimeId} sorttable_customkey={batchTime.toString}
        isFailed={batch.isFailed.toString}>
      <a href={s"batch?id=$batchTime"}>
        {formattedBatchTime}
      </a>
    </td>
      <td sorttable_customkey={numRecords.toString}>{numRecords.toString} records</td>
      <td sorttable_customkey={schedulingDelay.getOrElse(Long.MaxValue).toString}>
        {formattedSchedulingDelay}
      </td>
      <td sorttable_customkey={processingTime.getOrElse(Long.MaxValue).toString}>
        {formattedProcessingTime}
      </td>
  }

  private def batchTable: Seq[Node] = {
    <table id={tableId} class="table table-bordered table-striped table-condensed sortable">
      <thead>
        {columns}
      </thead>
      <tbody>
        {renderRows}
      </tbody>
    </table>
  }

  def toNodeSeq: Seq[Node] = {
    batchTable
  }

  protected def createOutputOperationProgressBar(batch: BatchUIData): Seq[Node] = {
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

  /**
   * Return HTML for all rows of this table.
   */
  protected def renderRows: Seq[Node]
}

private[ui] class ActiveBatchTable(
    runningBatches: Seq[BatchUIData],
    waitingBatches: Seq[BatchUIData],
    batchInterval: Long) extends BatchTableBase("active-batches-table", batchInterval) {

  private val firstFailureReason = getFirstFailureReason(runningBatches)

  override protected def columns: Seq[Node] = super.columns ++ {
    <th>Output Ops: Succeeded/Total</th>
      <th>Status</th> ++ {
      if (firstFailureReason.nonEmpty) {
        <th>Error</th>
      } else {
        Nil
      }
    }
  }

  override protected def renderRows: Seq[Node] = {
    // The "batchTime"s of "waitingBatches" must be greater than "runningBatches"'s, so display
    // waiting batches before running batches
    waitingBatches.flatMap(batch => <tr>{waitingBatchRow(batch)}</tr>) ++
      runningBatches.flatMap(batch => <tr>{runningBatchRow(batch)}</tr>)
  }

  private def runningBatchRow(batch: BatchUIData): Seq[Node] = {
    baseRow(batch) ++ createOutputOperationProgressBar(batch) ++ <td>processing</td> ++ {
      if (firstFailureReason.nonEmpty) {
        getFirstFailureTableCell(batch)
      } else {
        Nil
      }
    }
  }

  private def waitingBatchRow(batch: BatchUIData): Seq[Node] = {
    baseRow(batch) ++ createOutputOperationProgressBar(batch) ++ <td>queued</td>++ {
      if (firstFailureReason.nonEmpty) {
        // Waiting batches have not run yet, so must have no failure reasons.
        <td>-</td>
      } else {
        Nil
      }
    }
  }
}

private[ui] class CompletedBatchTable(batches: Seq[BatchUIData], batchInterval: Long)
  extends BatchTableBase("completed-batches-table", batchInterval) {

  private val firstFailureReason = getFirstFailureReason(batches)

  override protected def columns: Seq[Node] = super.columns ++ {
    <th>Total Delay {SparkUIUtils.tooltip("Total time taken to handle a batch", "top")}</th>
      <th>Output Ops: Succeeded/Total</th> ++ {
      if (firstFailureReason.nonEmpty) {
        <th>Error</th>
      } else {
        Nil
      }
    }
  }

  override protected def renderRows: Seq[Node] = {
    batches.flatMap(batch => <tr>{completedBatchRow(batch)}</tr>)
  }

  private def completedBatchRow(batch: BatchUIData): Seq[Node] = {
    val totalDelay = batch.totalDelay
    val formattedTotalDelay = totalDelay.map(SparkUIUtils.formatDuration).getOrElse("-")

    baseRow(batch) ++ {
      <td sorttable_customkey={totalDelay.getOrElse(Long.MaxValue).toString}>
        {formattedTotalDelay}
      </td>
    } ++ createOutputOperationProgressBar(batch)++ {
      if (firstFailureReason.nonEmpty) {
        getFirstFailureTableCell(batch)
      } else {
        Nil
      }
    }
  }
}
