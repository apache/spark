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

import org.apache.spark.storage._
import org.apache.spark.ui.{UIUtils, WebUIPage}
import org.apache.spark.util.Utils

import scala.xml.Node

private[ui] abstract class StorageDetailPage(pageName: String, parent: StorageTab)
  extends WebUIPage(pageName){
  
  protected val listener = parent.listener
  
  // define the displayed info when the storage object is not found
  protected val nonFoundErrorInfo = {
    <div class="container-fluid">
      <div class="row-fluid">
        <div class="span12">
          <h3 style="vertical-align: bottom; display: inline-block;">
            Storage Object Not Found
          </h3>
        </div>
      </div>
    </div>
  }
  
  // the object
  protected def objectList: Seq[StorageObjectInfo]
  
  protected def storageStatusList = listener.storageStatusList
  
  // ID of the workerTable
  protected val workerTableID: String = ""
  
  protected def getWorkerTableAndSize(objectId: Any): (Seq[Node], Int) = {
    // Worker table
    val workerStatus = storageStatusList
    (UIUtils.listingTable(workerHeader, workerRow, workerStatus, id = Some(workerTableID)), 
      workerStatus.size)
  }
  
  // get the rendered table of blocks and the total number of blocks
  protected def getBlockTableAndSize(objectId: Any): (Seq[Node], Int)
  
  // generate the content displayed on the page
  protected def generateContent(objectId: Long): (String, Seq[Node])
  
  def render(request: HttpServletRequest): Seq[Node] = {
    val parameterId = request.getParameter("id")
    require(parameterId != null && parameterId.nonEmpty, "Missing id parameter")

    val objectId = parameterId.toLong
    
    val (name, content) = generateContent(objectId)
    
    UIUtils.headerSparkPage("Storage Info for " + name, content, parent)
  }

  /** Header fields for the worker table */
  protected def workerHeader = Seq(
    "Host",
    "Memory Usage",
    "Disk Usage")

  /** Header fields for the block table */
  protected def blockHeader = Seq(
    "Block Name",
    "Storage Level",
    "Size in Memory",
    "Size on Disk",
    "Executors")

  /** Render an HTML row representing a worker */
  private def workerRow(workerStatus: StorageStatus): Seq[Node] = {
    <tr>
      <td>{workerStatus.blockManagerId.host + ":" + workerStatus.blockManagerId.port}</td>
      <td>
        {Utils.bytesToString(workerStatus.memUsed)}
        ({Utils.bytesToString(workerStatus.memRemaining)} Remaining)
      </td>
      <td>{Utils.bytesToString(workerStatus.diskUsed)}</td>
    </tr>
  }

  /** Render an HTML row representing a block */
  protected def blockRow(row: (BlockId, BlockStatus, Seq[String])): Seq[Node] = {
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
