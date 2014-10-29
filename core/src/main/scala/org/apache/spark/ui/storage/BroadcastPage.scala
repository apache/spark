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

import org.apache.spark.storage.{BlockStatus, BlockId, StorageStatus, StorageUtils}
import org.apache.spark.ui.{UIUtils, WebUIPage}
import org.apache.spark.util.Utils

import scala.xml.Node

private[ui] class BroadcastPage(parent: StorageTab) extends WebUIPage("broadcast"){

  private val listener = parent.listener

  override def render(request: HttpServletRequest): Seq[Node] = {
    val broadcastId = request.getParameter("id").toLong
    val storageStatusList = listener.storageStatusList
    val broadcastInfo = listener.broadcastInfoList.find(_.id == broadcastId).getOrElse {
      // Rather than crashing, render an "RDD Not Found" page
      return UIUtils.headerSparkPage("Broadcast Not Found", Seq[Node](), parent)
    }

    // Worker table
    val workers = storageStatusList.map((broadcastId, _))
    val workerTable = UIUtils.listingTable(workerHeader, workerRowForBroadcast, workers,
      id = Some("broadcast-storage-by-worker-table"))

    // Block table
    val blockLocations = StorageUtils.getBroadcastBlockLocation(broadcastId, storageStatusList)
    val blocks = storageStatusList
      .flatMap(_.broadcastBlocksById(broadcastId))
      .sortWith(_._1.name < _._1.name)
      .map { case (blockId, status) =>
      (blockId, status, blockLocations.get(blockId).getOrElse(Seq[String]("Unknown")))
    }
    val blockTable = UIUtils.listingTable(blockHeader, blockRow, blocks,
      id = Some("rdd-storage-by-block-table"))

    val content =
      <div class="row-fluid">
        <div class="span12">
          <ul class="unstyled">
            <li>
              <strong>Total Partitions:</strong>
              {broadcastInfo.numPartitions}
            </li>
            <li>
              <strong>Memory Size:</strong>
              {Utils.bytesToString(broadcastInfo.memSize)}
            </li>
            <li>
              <strong>Disk Size:</strong>
              {Utils.bytesToString(broadcastInfo.diskSize)}
            </li>
          </ul>
        </div>
      </div>

        <div class="row-fluid">
          <div class="span12">
            <h4> Data Distribution on {workers.size} Executors </h4>
            {workerTable}
          </div>
        </div>

        <div class="row-fluid">
          <div class="span12">
            <h4> {blocks.size} Partitions </h4>
            {blockTable}
          </div>
        </div>;

    UIUtils.headerSparkPage("Storage Info for " + broadcastInfo.name, content, parent)
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
  private def workerRowForRDD(worker: (Int, StorageStatus)): Seq[Node] = {
    val (rddId, status) = worker
    <tr>
      <td>{status.blockManagerId.host + ":" + status.blockManagerId.port}</td>
      <td>
        {Utils.bytesToString(status.memUsedByRdd(rddId))}
        ({Utils.bytesToString(status.memRemaining)} Remaining)
      </td>
      <td>{Utils.bytesToString(status.diskUsedByRdd(rddId))}</td>
    </tr>
  }

  /** Render an HTML row representing a worker */
  private def workerRowForBroadcast(worker: (Long, StorageStatus)): Seq[Node] = {
    val (broadcastId, status) = worker
    <tr>
      <td>{status.blockManagerId.host + ":" + status.blockManagerId.port}</td>
      <td>
        {Utils.bytesToString(status.memUsedByBroadcast(broadcastId))}
        ({Utils.bytesToString(status.memRemaining)} Remaining)
      </td>
      <td>{Utils.bytesToString(status.diskUsedByBroadcast(broadcastId))}</td>
    </tr>
  }

  /** Render an HTML row representing a block */
  private def blockRow(row: (BlockId, BlockStatus, Seq[String])): Seq[Node] = {
    val (id, block, locations) = row
    <tr>
      <td>{id}</td>
      <td>
        {block.storageLevel.description}
      </td>
      <td sorttable_customkey={block.memSize.toString}>
        {Utils.bytesToString(block.memSize)}
      </td>
      <td sorttable_customkey={block.diskSize.toString}>
        {Utils.bytesToString(block.diskSize)}
      </td>
      <td>
        {locations.map(l => <span>{l}<br/></span>)}
      </td>
    </tr>
  }
}
