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

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.spark.Logging
import org.apache.spark.ui.WebUIPage
import org.apache.spark.ui.{UIUtils => SparkUIUtils}
import org.apache.spark.util.Utils

private[ui] class StreamingStoragePage(parent: StreamingStorageTab)
  extends WebUIPage("") with Logging {

  private val listener = parent.listener

  override def render(request: HttpServletRequest): Seq[Node] = {
    // Sort by (timestamp, streamId) in descending order
    val blockInfos =
      listener.getAllBlocks.sortBy(b => (b.blockId.uniqueId, b.blockId.streamId)).reverse
    val content = SparkUIUtils.listingTable(blockHeader, blockRow, blockInfos)
    SparkUIUtils.headerSparkPage("Receiver Blocks", content, parent)
  }

  private def blockHeader = Seq(
    "Block Id",
    "Storage Level",
    "Size in Memory",
    "Size in ExternalBlockStore",
    "Size on Disk")

  private def blockRow(blockInfo: StreamBlockUIData): Seq[Node] = {
    <tr>
      <td>
        {blockInfo.blockId.toString}
      </td>
      <td>
        {blockInfo.storageLevel.description}
      </td>
      <td sorttable_customkey={blockInfo.memSize.toString}>
        {Utils.bytesToString(blockInfo.memSize)}
      </td>
      <td sorttable_customkey={blockInfo.externalBlockStoreSize.toString}>
        {Utils.bytesToString(blockInfo.externalBlockStoreSize)}
      </td>
      <td sorttable_customkey={blockInfo.diskSize.toString}>
        {Utils.bytesToString(blockInfo.diskSize)}
      </td>
    </tr>
  }
}
