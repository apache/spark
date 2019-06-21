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

import scala.collection.SortedMap
import scala.xml.Node

import org.apache.spark.status.{AppStatusStore, StreamBlockData}
import org.apache.spark.status.api.v1
import org.apache.spark.ui._
import org.apache.spark.util.Utils

/** Page showing list of RDD's currently stored in the cluster */
private[ui] class StoragePage(parent: SparkUITab, store: AppStatusStore) extends WebUIPage("") {

  def render(request: HttpServletRequest): Seq[Node] = {
    val content = rddTable(request, store.rddList()) ++
      receiverBlockTables(store.streamBlocksList())
    UIUtils.headerSparkPage(request, "Storage", content, parent)
  }

  private[storage] def rddTable(
      request: HttpServletRequest,
      rdds: Seq[v1.RDDStorageInfo]): Seq[Node] = {
    if (rdds.isEmpty) {
      // Don't show the rdd table if there is no RDD persisted.
      Nil
    } else {
      <div>
        <span class="collapse-aggregated-rdds collapse-table"
            onClick="collapseTable('collapse-aggregated-rdds','aggregated-rdds')">
          <h4>
            <span class="collapse-table-arrow arrow-open"></span>
            <a>RDDs</a>
          </h4>
        </span>
        <div class="aggregated-rdds collapsible-table">
          {UIUtils.listingTable(
            rddHeader,
            rddRow(request, _: v1.RDDStorageInfo),
            rdds,
            id = Some("storage-by-rdd-table"))}
        </div>
      </div>
    }
  }

  /** Header fields for the RDD table */
  private val rddHeader = Seq(
    "ID",
    "RDD Name",
    "Storage Level",
    "Cached Partitions",
    "Fraction Cached",
    "Size in Memory",
    "Size on Disk")

  /** Render an HTML row representing an RDD */
  private def rddRow(request: HttpServletRequest, rdd: v1.RDDStorageInfo): Seq[Node] = {
    // scalastyle:off
    <tr>
      <td>{rdd.id}</td>
      <td>
        <a href={"%s/storage/rdd/?id=%s".format(
          UIUtils.prependBaseUri(request, parent.basePath), rdd.id)}>
          {rdd.name}
        </a>
      </td>
      <td>{rdd.storageLevel}
      </td>
      <td>{rdd.numCachedPartitions.toString}</td>
      <td>{"%.0f%%".format(rdd.numCachedPartitions * 100.0 / rdd.numPartitions)}</td>
      <td sorttable_customkey={rdd.memoryUsed.toString}>{Utils.bytesToString(rdd.memoryUsed)}</td>
      <td sorttable_customkey={rdd.diskUsed.toString} >{Utils.bytesToString(rdd.diskUsed)}</td>
    </tr>
    // scalastyle:on
  }

  private[storage] def receiverBlockTables(blocks: Seq[StreamBlockData]): Seq[Node] = {
    if (blocks.isEmpty) {
      // Don't show the tables if there is no stream block
      Nil
    } else {
      val sorted = blocks.groupBy(_.name).toSeq.sortBy(_._1.toString)

      <div>
        <h4>Receiver Blocks</h4>
        {executorMetricsTable(blocks)}
        {streamBlockTable(sorted)}
      </div>
    }
  }

  private def executorMetricsTable(blocks: Seq[StreamBlockData]): Seq[Node] = {
    val blockManagers = SortedMap(blocks.groupBy(_.executorId).toSeq: _*)
      .map { case (id, blocks) =>
        new ExecutorStreamSummary(blocks)
      }

    <div>
      <h5>Aggregated Block Metrics by Executor</h5>
      {UIUtils.listingTable(executorMetricsTableHeader, executorMetricsTableRow, blockManagers,
        id = Some("storage-by-executor-stream-blocks"))}
    </div>
  }

  private val executorMetricsTableHeader = Seq(
    "Executor ID",
    "Address",
    "Total Size in Memory",
    "Total Size on Disk",
    "Stream Blocks")

  private def executorMetricsTableRow(status: ExecutorStreamSummary): Seq[Node] = {
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
      <td sorttable_customkey={status.totalDiskSize.toString}>
        {Utils.bytesToString(status.totalDiskSize)}
      </td>
      <td>
        {status.numStreamBlocks.toString}
      </td>
    </tr>
  }

  private def streamBlockTable(blocks: Seq[(String, Seq[StreamBlockData])]): Seq[Node] = {
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
  private def streamBlockTableRow(block: (String, Seq[StreamBlockData])): Seq[Node] = {
    val replications = block._2
    assert(replications.nonEmpty) // This must be true because it's the result of "groupBy"
    if (replications.size == 1) {
      streamBlockTableSubrow(block._1, replications.head, replications.size, true)
    } else {
      streamBlockTableSubrow(block._1, replications.head, replications.size, true) ++
        replications.tail.flatMap(streamBlockTableSubrow(block._1, _, replications.size, false))
    }
  }

  private def streamBlockTableSubrow(
      blockId: String,
      block: StreamBlockData,
      replication: Int,
      firstSubrow: Boolean): Seq[Node] = {
    val (storageLevel, size) = streamBlockStorageLevelDescriptionAndSize(block)

    <tr>
      {
        if (firstSubrow) {
          <td rowspan={replication.toString}>
            {block.name}
          </td>
          <td rowspan={replication.toString}>
            {replication.toString}
          </td>
        }
      }
      <td>{block.hostPort}</td>
      <td>{storageLevel}</td>
      <td>{Utils.bytesToString(size)}</td>
    </tr>
  }

  private[storage] def streamBlockStorageLevelDescriptionAndSize(
      block: StreamBlockData): (String, Long) = {
    if (block.useDisk) {
      ("Disk", block.diskSize)
    } else if (block.useMemory && block.deserialized) {
      ("Memory", block.memSize)
    } else if (block.useMemory && !block.deserialized) {
      ("Memory Serialized", block.memSize)
    } else {
      throw new IllegalStateException(s"Invalid Storage Level: ${block.storageLevel}")
    }
  }

}

private class ExecutorStreamSummary(blocks: Seq[StreamBlockData]) {

  def executorId: String = blocks.head.executorId

  def location: String = blocks.head.hostPort

  def totalMemSize: Long = blocks.map(_.memSize).sum

  def totalDiskSize: Long = blocks.map(_.diskSize).sum

  def numStreamBlocks: Int = blocks.size

}
