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

package org.apache.spark.ui.storage

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.spark.status.api.v1.{AllRDDResource, RDDDataDistribution, RDDPartitionInfo}
import org.apache.spark.ui.{UIUtils, WebUIPage}
import org.apache.spark.util.Utils

/** Page showing storage details for a given RDD */
private[ui] class RDDPage(parent: StorageTab) extends WebUIPage("rdd") {
  private val listener = parent.listener

  def render(request: HttpServletRequest): Seq[Node] = {
    val parameterId = request.getParameter("id")
    require(parameterId != null && parameterId.nonEmpty, "Missing id parameter")
    val rddId = parameterId.toInt
    val rddStorageInfo = AllRDDResource.getRDDStorageInfo(rddId, listener,includeDetails = true)
      .getOrElse {
        // Rather than crashing, render an "RDD Not Found" page
        return UIUtils.headerSparkPage("RDD Not Found", Seq[Node](), parent)
      }

    // Worker table
    val workerTable = UIUtils.listingTable(workerHeader, workerRow,
      rddStorageInfo.dataDistribution.get, id = Some("rdd-storage-by-worker-table"))

    // Block table
    val blockTable = UIUtils.listingTable(blockHeader, blockRow, rddStorageInfo.partitions.get,
      id = Some("rdd-storage-by-block-table"))

    val content =
      <div class="row-fluid">
        <div class="span12">
          <ul class="unstyled">
            <li>
              <strong>Storage Level:</strong>
              {rddStorageInfo.storageLevel}
            </li>
            <li>
              <strong>Cached Partitions:</strong>
              {rddStorageInfo.numCachedPartitions}
            </li>
            <li>
              <strong>Total Partitions:</strong>
              {rddStorageInfo.numPartitions}
            </li>
            <li>
              <strong>Memory Size:</strong>
              {Utils.bytesToString(rddStorageInfo.memoryUsed)}
            </li>
            <li>
              <strong>Disk Size:</strong>
              {Utils.bytesToString(rddStorageInfo.diskUsed)}
            </li>
          </ul>
        </div>
      </div>

      <div class="row-fluid">
        <div class="span12">
          <h4> Data Distribution on {rddStorageInfo.dataDistribution.size} Executors </h4>
          {workerTable}
        </div>
      </div>

      <div class="row-fluid">
        <div class="span12">
          <h4> {rddStorageInfo.partitions.size} Partitions </h4>
          {blockTable}
        </div>
      </div>;

    UIUtils.headerSparkPage("RDD Storage Info for " + rddStorageInfo.name, content, parent)
  }

  /** Header fields for the worker table */
  private def workerHeader = Seq(
    "Host",
    "Memory Usage",
    "Disk Usage")

  /** Header fields for the block table */
  private def blockHeader = Seq(
    "Block Name",
    "Storage Level",
    "Size in Memory",
    "Size on Disk",
    "Executors")

  /** Render an HTML row representing a worker */
  private def workerRow(worker: RDDDataDistribution): Seq[Node] = {
    <tr>
      <td>{worker.address}</td>
      <td>
        {Utils.bytesToString(worker.memoryUsed)}
        ({Utils.bytesToString(worker.memoryRemaining)} Remaining)
      </td>
      <td>{Utils.bytesToString(worker.diskUsed)}</td>
    </tr>
  }

  /** Render an HTML row representing a block */
  private def blockRow(row: RDDPartitionInfo): Seq[Node] = {
    <tr>
      <td>{row.blockName}</td>
      <td>
        {row.storageLevel}
      </td>
      <td sorttable_customkey={row.memoryUsed.toString}>
        {Utils.bytesToString(row.memoryUsed)}
      </td>
      <td sorttable_customkey={row.diskUsed.toString}>
        {Utils.bytesToString(row.diskUsed)}
      </td>
      <td>
        {row.executors.map(l => <span>{l}<br/></span>)}
      </td>
    </tr>
  }
}
