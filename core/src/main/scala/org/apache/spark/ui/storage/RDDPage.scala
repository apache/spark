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

import org.json4s.{JNothing, JValue}
import org.json4s.JsonDSL._

import org.apache.spark.storage._
import org.apache.spark.ui.{WebUIPage, UIUtils}
import org.apache.spark.util.Utils

/** Page showing storage details for a given RDD */
private[ui] class RDDPage(parent: StorageTab) extends WebUIPage("rdd") {
  private val listener = parent.listener

  override def renderJson(request: HttpServletRequest): JValue = {
    val rddId = request.getParameter("id").toInt
    val storageStatusList = listener.storageStatusList
    val rddInfoOpt = listener.rddInfoList.find(_.id == rddId)

    var rddInfoJson: JValue = JNothing

    if (rddInfoOpt.isDefined) {
      val rddInfo = rddInfoOpt.get

      val rddSummaryJson = ("RDD Summary" ->
        ("RDD ID" -> rddId) ~
        ("Storage Level" -> rddInfo.storageLevel.description) ~
        ("Cached Partitions" -> rddInfo.numCachedPartitions) ~
        ("Total Partitions" -> rddInfo.numPartitions) ~
        ("Memory Size" -> rddInfo.memSize) ~
        ("Disk Size" -> rddInfo.diskSize))

      val dataDistributionList =
        storageStatusList.map { status =>
          ("Host" -> (status.blockManagerId.host + ":" + status.blockManagerId.port)) ~
          ("Memory Usage" -> status.memUsedByRdd(rddId)) ~
          ("Memory Remaining" -> status.memRemaining) ~
          ("Disk Usage" -> status.diskUsedByRdd(rddId))
        }

      val dataDistributionJson = ("Data Distribution" -> dataDistributionList)

      val blockLocations = StorageUtils.getRddBlockLocations(rddId, storageStatusList)
      val blocks = storageStatusList
        .flatMap(_.rddBlocksById(rddId))
        .sortWith(_._1.name < _._1.name)
        .map { case (blockId, status) =>
          (blockId, status, blockLocations.get(blockId).getOrElse(Seq[String]("Unknown")))
        }

      val partitionList =
        blocks.map { case (id, block, locations) =>
          ("Block Name" -> id.toString) ~
          ("Storage Level" -> block.storageLevel.description) ~
          ("Size in Memory" -> block.memSize) ~
          ("Size on Disk" -> block.diskSize) ~
          ("Executors" -> locations)
        }

      val partitionsJson = ("Partitions" -> partitionList)

      rddInfoJson =
        rddSummaryJson ~
        dataDistributionJson ~
        partitionsJson
    }
    rddInfoJson
  }

  def render(request: HttpServletRequest): Seq[Node] = {
    val rddId = request.getParameter("id").toInt
    val storageStatusList = listener.storageStatusList
    val rddInfo = listener.rddInfoList.find(_.id == rddId).getOrElse {
      // Rather than crashing, render an "RDD Not Found" page
      return UIUtils.headerSparkPage("RDD Not Found", Seq[Node](), parent)
    }

    // Worker table
    val workers = storageStatusList.map((rddId, _))
    val workerTable = UIUtils.listingTable(workerHeader, workerRow, workers,
      id = Some("rdd-storage-by-worker-table"))

    // Block table
    val blockLocations = StorageUtils.getRddBlockLocations(rddId, storageStatusList)
    val blocks = storageStatusList
      .flatMap(_.rddBlocksById(rddId))
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
              <strong>Storage Level:</strong>
              {rddInfo.storageLevel.description}
            </li>
            <li>
              <strong>Cached Partitions:</strong>
              {rddInfo.numCachedPartitions}
            </li>
            <li>
              <strong>Total Partitions:</strong>
              {rddInfo.numPartitions}
            </li>
            <li>
              <strong>Memory Size:</strong>
              {Utils.bytesToString(rddInfo.memSize)}
            </li>
            <li>
              <strong>Disk Size:</strong>
              {Utils.bytesToString(rddInfo.diskSize)}
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

    UIUtils.headerSparkPage("RDD Storage Info for " + rddInfo.name, content, parent)
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
  private def workerRow(worker: (Int, StorageStatus)): Seq[Node] = {
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
