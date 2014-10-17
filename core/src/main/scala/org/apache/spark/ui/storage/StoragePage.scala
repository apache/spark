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

import org.apache.spark.storage.{BlockId, BlockStatus, RDDInfo}
import org.apache.spark.ui.{WebUIPage, UIUtils}
import org.apache.spark.util.Utils

/** Page showing list of RDD's currently stored in the cluster */
private[ui] class StoragePage(parent: StorageTab) extends WebUIPage("") {
  private val listener = parent.listener

  def render(request: HttpServletRequest): Seq[Node] = {
    val rdds = listener.rddInfoList
    val rddContent = UIUtils.listingTable(rddHeader, rddRow, rdds)
    val broadcastContent = UIUtils.listingTable(broadcastHeader, blockRow, List())
    UIUtils.headerSparkPageWithMultipleTables("Storage", parent, None,
      subTitleContentPair = ("RDDs", rddContent), ("Broadcast", broadcastContent))
  }

  /** Header fields for the RDD table */
  private def rddHeader = Seq(
    "RDD Name",
    "Storage Level",
    "Cached Partitions",
    "Fraction Cached",
    "Size in Memory",
    "Size in Tachyon",
    "Size on Disk")

  private def broadcastHeader = Seq(
    "Variable Name",
    "Storage Level",
    "Size in Memory",
    "Size in Tachyon",
    "Size on Disk")

  /** Render an HTML row representing an block variable */
  private def blockRow(blockIdStatusPair: (BlockId, BlockStatus)): Seq[Node] = {
    val blockId = blockIdStatusPair._1
    val blockStatus = blockIdStatusPair._2
    // scalastyle:off
    <tr>
      <td>
        <a href={"%s/storage/broadcast?id=%s".format(UIUtils.prependBaseUri(parent.basePath), blockId)}>
          {blockId}
        </a>
      </td>
      <td>{blockStatus.storageLevel.description}
      </td>
      <td sorttable_customkey={blockStatus.memSize.toString}>{Utils.bytesToString(blockStatus.memSize)}</td>
      <td sorttable_customkey={blockStatus.tachyonSize.toString}>{Utils.bytesToString(blockStatus.tachyonSize)}</td>
      <td sorttable_customkey={blockStatus.diskSize.toString} >{Utils.bytesToString(blockStatus.diskSize)}</td>
    </tr>
    // scalastyle:on
  }

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
      <td sorttable_customkey={rdd.tachyonSize.toString}>{Utils.bytesToString(rdd.tachyonSize)}</td>
      <td sorttable_customkey={rdd.diskSize.toString} >{Utils.bytesToString(rdd.diskSize)}</td>
    </tr>
    // scalastyle:on
  }
}
