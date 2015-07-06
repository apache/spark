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
    val statuses = listener.allExecutorStreamBlockStatus.sortBy(_.executorId)
    val content = rddTable ++ streamBlockTables(statuses)
    UIUtils.headerSparkPage("Storage", content, parent)
  }

  private def rddTable: Seq[Node] = {
    val rdds = listener.rddInfoList
    <div>
      <h4>RDDs</h4>
      {UIUtils.listingTable(rddHeader, rddRow, rdds, id = Some("storage-by-rdd-table"))}
    </div>
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
      <td>{rdd.numCachedPartitions}</td>
      <td>{"%.0f%%".format(rdd.numCachedPartitions * 100.0 / rdd.numPartitions)}</td>
      <td sorttable_customkey={rdd.memSize.toString}>{Utils.bytesToString(rdd.memSize)}</td>
      <td sorttable_customkey={rdd.externalBlockStoreSize.toString}>{Utils.bytesToString(rdd.externalBlockStoreSize)}</td>
      <td sorttable_customkey={rdd.diskSize.toString} >{Utils.bytesToString(rdd.diskSize)}</td>
    </tr>
    // scalastyle:on
  }

  private def streamBlockTables(statuses: Seq[ExecutorStreamBlockStatus]): Seq[Node] = {
    if (statuses.map(_.numStreamBlocks).sum == 0) {
      Nil
    } else {
      <div>
        <h4>Receiver Blocks</h4>
        {executorMetricsTable(statuses)}
        {executorBlockTable(statuses)}
      </div>
    }
  }

  private def executorMetricsTable(statuses: Seq[ExecutorStreamBlockStatus]): Seq[Node] = {
    <div>
      <h5>Aggregated Stream Block Metrics by Executor</h5>
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

  private def executorBlockTable(statuses: Seq[ExecutorStreamBlockStatus]): Seq[Node] = {
    if (statuses.isEmpty) {
      Nil
    } else {
      <div>
        <h5>Executor Stream Blocks Details</h5>
        {UIUtils.listingTable(
          executorBlockTableHeader,
          executorBlockRow,
          statuses,
          id = Some("storage-by-block-table"),
          sortable = false)}
      </div>
    }
  }

  private val executorBlockTableHeader = Seq(
    "Executor ID",
    "Block ID",
    "Storage Level",
    "Size in Memory",
    "Size in ExternalBlockStore",
    "Size on Disk")

  /** Render all blocks in an executor */
  private def executorBlockRow(status: ExecutorStreamBlockStatus): Seq[Node] = {
    if (status.blocks.isEmpty) {
      Nil
    } else {
      blockRow(status.executorId, status.blocks.head, status.numStreamBlocks, true) ++
        status.blocks.tail.flatMap { block =>
          blockRow(status.executorId, block, status.numStreamBlocks, false)
        }
    }
  }

  /** Render a block in an executor */
  private def blockRow(
      executorId: String, block: BlockUIData, numBlocks: Int, firstRow: Boolean): Seq[Node] = {
    <tr>
      {
        if (firstRow) {
          <td rowspan={numBlocks.toString}>
            {executorId}
          </td>
        }
      }
      <td>
        {block.blockId.toString}
      </td>
      <td>
        {streamBlockStorageLevelDescription(block.storageLevel)}
      </td>
      <td sorttable_customkey={block.memSize.toString}>
        {Utils.bytesToString(block.memSize)}
      </td>
      <td sorttable_customkey={block.externalBlockStoreSize.toString}>
        {Utils.bytesToString(block.externalBlockStoreSize)}
      </td>
      <td sorttable_customkey={block.diskSize.toString}>
        {Utils.bytesToString(block.diskSize)}
      </td>
    </tr>
  }

  private def streamBlockStorageLevelDescription(storageLevel: StorageLevel): String = {
    // Unlike storageLevel.description, this method doesn't show the replication
    var result = ""
    result += (if (storageLevel.useDisk) "Disk " else "")
    result += (if (storageLevel.useMemory) "Memory " else "")
    result += (if (storageLevel.useOffHeap) "ExternalBlockStore " else "")
    result += (if (storageLevel.deserialized) "Deserialized " else "Serialized ")
    result
  }

}
