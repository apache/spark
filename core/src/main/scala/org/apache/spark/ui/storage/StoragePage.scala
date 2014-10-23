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

import org.json4s.DefaultFormats
import org.json4s.JsonDSL._
import org.json4s.JsonAST._

import scala.xml.Node

import org.apache.spark.storage.RDDInfo
import org.apache.spark.ui.{WebUIPage, UIUtils}
import org.apache.spark.util.Utils

/** Page showing list of RDD's currently stored in the cluster */
private[ui] class StoragePage(parent: StorageTab) extends WebUIPage("") {
  private val listener = parent.listener
  private val jsRenderingEnabled = parent.jsRenderingEnabled

  def render(request: HttpServletRequest): Seq[Node] = {
    val rdds = listener.rddInfoList
    val tableId = "storageTable"
    val content = if (jsRenderingEnabled) {
      UIUtils.listingEmptyTable(rddHeader, tableId) ++
        UIUtils.fillTableJavascript(parent.prefix, tableId)
    } else {
      UIUtils.listingTable(rddHeader, rddRow, rdds)
    }
    UIUtils.headerSparkPage("Storage ", content, parent)
  }

  /** Render the whole JSON */
  override def renderJson(request: HttpServletRequest): JValue = {
    val rdds = listener.rddInfoList
    val rddsJson = UIUtils.listingJson(rddRowJson, rdds)

    rddsJson
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

  /** Render a Json row representing an RDD */
  private def rddRowJson(rdd: RDDInfo): JValue = {
    val rddNameVal = "<a href=" +
      {"%s/storage/rdd?id=%s".format(UIUtils.prependBaseUri(parent.basePath), rdd.id)} +
      ">" + {rdd.name} + "</a>"
    ("RDD Name" -> rddNameVal) ~
    ("Storage Level" -> {rdd.storageLevel.description}) ~
    ("Cached Partitions" -> {rdd.numCachedPartitions}) ~
    ("Fraction Cached" -> {"%.0f%%".format(rdd.numCachedPartitions * 100.0 / rdd.numPartitions)}) ~
    ("Size in Memory" -> UIUtils.cellWithSorttableCustomKey(Utils.bytesToString(rdd.memSize),
      rdd.memSize.toString)) ~
    ("Size in Tachyon" -> UIUtils.cellWithSorttableCustomKey(Utils.bytesToString(rdd.tachyonSize),
      rdd.tachyonSize.toString)) ~
    ("Size on Disk" -> UIUtils.cellWithSorttableCustomKey(Utils.bytesToString(rdd.diskSize),
      rdd.diskSize.toString))
  }
}
