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

import org.apache.spark.streaming.scheduler.BatchInfo
import org.apache.spark.ui.UIUtils

private[ui] abstract class BatchTableBase(tableId: String) {

  protected def columns: Seq[Node] = {
    <th>Batch Time</th>
      <th>Input Size</th>
      <th>Scheduling Delay</th>
      <th>Processing Time</th>
  }

  protected def baseRow(batch: BatchInfo): Seq[Node] = {
    val batchTime = batch.batchTime.milliseconds
    val formattedBatchTime = UIUtils.formatDate(batch.batchTime.milliseconds)
    val eventCount = batch.receivedBlockInfo.values.map {
      receivers => receivers.map(_.numRecords).sum
    }.sum
    val schedulingDelay = batch.schedulingDelay
    val formattedSchedulingDelay = schedulingDelay.map(UIUtils.formatDuration).getOrElse("-")
    val processingTime = batch.processingDelay
    val formattedProcessingTime = processingTime.map(UIUtils.formatDuration).getOrElse("-")

    <td sorttable_customkey={batchTime.toString}>{formattedBatchTime}</td>
      <td sorttable_customkey={eventCount.toString}>{eventCount.toString} events</td>
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

  /**
   * Return HTML for all rows of this table.
   */
  protected def renderRows: Seq[Node]
}

private[ui] class ActiveBatchTable(runningBatches: Seq[BatchInfo], waitingBatches: Seq[BatchInfo])
  extends BatchTableBase("active-batches-table") {

  override protected def columns: Seq[Node] = super.columns ++ <th>Status</th>

  override protected def renderRows: Seq[Node] = {
    // The "batchTime"s of "waitingBatches" must be greater than "runningBatches"'s, so display
    // waiting batches before running batches
    waitingBatches.flatMap(batch => <tr>{waitingBatchRow(batch)}</tr>) ++
      runningBatches.flatMap(batch => <tr>{runningBatchRow(batch)}</tr>)
  }

  private def runningBatchRow(batch: BatchInfo): Seq[Node] = {
    baseRow(batch) ++ <td>processing</td>
  }

  private def waitingBatchRow(batch: BatchInfo): Seq[Node] = {
    baseRow(batch) ++ <td>queued</td>
  }
}

private[ui] class CompletedBatchTable(batches: Seq[BatchInfo])
  extends BatchTableBase("completed-batches-table") {

  override protected def columns: Seq[Node] = super.columns ++ <th>Total Delay</th>

  override protected def renderRows: Seq[Node] = {
    batches.flatMap(batch => <tr>{completedBatchRow(batch)}</tr>)
  }

  private def completedBatchRow(batch: BatchInfo): Seq[Node] = {
    val totalDelay = batch.totalDelay
    val formattedTotalDelay = totalDelay.map(UIUtils.formatDuration).getOrElse("-")
    baseRow(batch) ++
      <td sorttable_customkey={totalDelay.getOrElse(Long.MaxValue).toString}>
        {formattedTotalDelay}
      </td>
  }
}
