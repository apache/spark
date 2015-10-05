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

import org.apache.spark.storage._
import org.apache.spark.ui.{UIUtils, WebUIPage}
import org.apache.spark.util.Utils

/** Page showing list of RDD's currently stored in the cluster */
private[ui] class StoragePage(parent: StorageTab) extends WebUIPage("") {
  private val listener = parent.listener

  def render(request: HttpServletRequest): Seq[Node] = {
    val content = rddTable(listener.rddInfoList) ++
      receiverBlockTables(listener.allExecutorStreamBlockStatus.sortBy(_.executorId))
    UIUtils.headerSparkPage("Storage", content, parent)
  }

  private[storage] def rddTable(rdds: Seq[RDDInfo]): Seq[Node] = {
    if (rdds.isEmpty) {
      // Don't show the rdd table if there is no RDD persisted.
      Nil
    } else {
      <div>
        <h4>RDDs</h4>
        {UIUtils.listingTable(rddHeader, rddRow, rdds, id = Some("storage-by-rdd-table"))}
      </div>
    }
  }

  /** Header fields for the RDD table */
  private val rddHeader = Seq(
    "RDD Name",
    "Storage Level",
    "Cached Partitions",
    "Fraction Cached",
    "Size in Memory",
    "Size in ExternalBlockStore",
    "Size on Disk")

  /** Render an HTML row representing an RDD */
  private def rddRow(rdd: RDDInfo): Seq[Node] = {
    // scalastyle:off
    <tr>
      <td>
        <a href={"%s/storage/rdd?id=%s".format(UIUtils.prependBaseUri(parent.basePath), rdd.id)}>
          {rdd.name}
        </a>
      </td>
      <td>{rdd.storageLevel.description}
      </td>
      <td>{rdd.numCachedPartitions.toString}</td>
      <td>{"%.0f%%".format(rdd.numCachedPartitions * 100.0 / rdd.numPartitions)}</td>
      <td sorttable_customkey={rdd.memSize.toString}>{Utils.bytesToString(rdd.memSize)}</td>
      <td sorttable_customkey={rdd.externalBlockStoreSize.toString}>{Utils.bytesToString(rdd.externalBlockStoreSize)}</td>
      <td sorttable_customkey={rdd.diskSize.toString} >{Utils.bytesToString(rdd.diskSize)}</td>
    </tr>
    // scalastyle:on
  }

  private[storage] def receiverBlockTables(statuses: Seq[ExecutorStreamBlockStatus]): Seq[Node] = {
    if (statuses.map(_.numStreamBlocks).sum == 0) {
      // Don't show the tables if there is no stream block
      Nil
    } else {
      val blocks = statuses.flatMap(_.blocks).groupBy(_.blockId).toSeq.sortBy(_._1.toString)

      <div>
        <h4>Receiver Blocks</h4>
        {executorMetricsTable(statuses)}
        {streamBlockTable(blocks)}
      </div>
    }
  }

  private def executorMetricsTable(statuses: Seq[ExecutorStreamBlockStatus]): Seq[Node] = {
    <div>
      <h5>Aggregated Block Metrics by Executor</h5>
      {UIUtils.listingTable(executorMetricsTableHeader, executorMetricsTableRow, statuses,
        id = Some("storage-by-executor-stream-blocks"))}
    </div>
  }

  private val executorMetricsTableHeader = Seq(
    "Executor ID",
    "Address",
    "Total Size in Memory",
    "Total Size in ExternalBlockStore",
    "Total Size on Disk",
    "Stream Blocks")

  private def executorMetricsTableRow(status: ExecutorStreamBlockStatus): Seq[Node] = {
    <tr>
      <td>
        {status.executorId}
      </td>
      <td>
        {status.location}
      </td>
      <td sorttable_customkey={status.totalMemSize.toString}>
        {Utils.bytesToString(status.totalMemSize)}
      </td>
      <td sorttable_customkey={status.totalExternalBlockStoreSize.toString}>
        {Utils.bytesToString(status.totalExternalBlockStoreSize)}
      </td>
      <td sorttable_customkey={status.totalDiskSize.toString}>
        {Utils.bytesToString(status.totalDiskSize)}
      </td>
      <td>
        {status.numStreamBlocks.toString}
      </td>
    </tr>
  }

  private def streamBlockTable(blocks: Seq[(BlockId, Seq[BlockUIData])]): Seq[Node] = {
    if (blocks.isEmpty) {
      Nil
    } else {
      <div>
        <h5>Blocks</h5>
        {UIUtils.listingTable(
          streamBlockTableHeader,
          streamBlockTableRow,
          blocks,
          id = Some("storage-by-block-table"),
          sortable = false)}
      </div>
    }
  }

  private val streamBlockTableHeader = Seq(
    "Block ID",
    "Replication Level",
    "Location",
    "Storage Level",
    "Size")

  /** Render a stream block */
  private def streamBlockTableRow(block: (BlockId, Seq[BlockUIData])): Seq[Node] = {
    val replications = block._2
    assert(replications.size > 0) // This must be true because it's the result of "groupBy"
    if (replications.size == 1) {
      streamBlockTableSubrow(block._1, replications.head, replications.size, true)
    } else {
      streamBlockTableSubrow(block._1, replications.head, replications.size, true) ++
        replications.tail.map(streamBlockTableSubrow(block._1, _, replications.size, false)).flatten
    }
  }

  private def streamBlockTableSubrow(
      blockId: BlockId, block: BlockUIData, replication: Int, firstSubrow: Boolean): Seq[Node] = {
    val (storageLevel, size) = streamBlockStorageLevelDescriptionAndSize(block)

    <tr>
      {
        if (firstSubrow) {
          <td rowspan={replication.toString}>
            {block.blockId.toString}
          </td>
          <td rowspan={replication.toString}>
            {replication.toString}
          </td>
        }
      }
      <td>{block.location}</td>
      <td>{storageLevel}</td>
      <td>{Utils.bytesToString(size)}</td>
    </tr>
  }

  private[storage] def streamBlockStorageLevelDescriptionAndSize(
      block: BlockUIData): (String, Long) = {
    if (block.storageLevel.useDisk) {
      ("Disk", block.diskSize)
    } else if (block.storageLevel.useMemory && block.storageLevel.deserialized) {
      ("Memory", block.memSize)
    } else if (block.storageLevel.useMemory && !block.storageLevel.deserialized) {
      ("Memory Serialized", block.memSize)
    } else if (block.storageLevel.useOffHeap) {
      ("External", block.externalBlockStoreSize)
    } else {
      throw new IllegalStateException(s"Invalid Storage Level: ${block.storageLevel}")
    }
  }

}
