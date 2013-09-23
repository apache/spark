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

import org.apache.spark.storage.{RDDInfo, StorageUtils}
import org.apache.spark.ui.UIUtils._
import org.apache.spark.ui.Page._
import org.apache.spark.util.Utils

/** Page showing list of RDD's currently stored in the cluster */
private[spark] class IndexPage(parent: BlockManagerUI) {
  val sc = parent.sc

  def render(request: HttpServletRequest): Seq[Node] = {
    val storageStatusList = sc.getExecutorStorageStatus
    // Calculate macro-level statistics

    val rddHeaders = Seq(
      "RDD Name",
      "Storage Level",
      "Cached Partitions",
      "Fraction Cached",
      "Size in Memory",
      "Size on Disk")
    val rdds = StorageUtils.rddInfoFromStorageStatus(storageStatusList, sc)
    val content = listingTable(rddHeaders, rddRow, rdds)

    headerSparkPage(content, parent.sc, "Storage ", Storage)
  }

  def rddRow(rdd: RDDInfo): Seq[Node] = {
    <tr>
      <td>
        <a href={"%s/storage/rdd?id=%s".format(prependBaseUri(),rdd.id)}>
          {rdd.name}
        </a>
      </td>
      <td>{rdd.storageLevel.description}
      </td>
      <td>{rdd.numCachedPartitions}</td>
      <td>{"%.0f%%".format(rdd.numCachedPartitions * 100.0 / rdd.numPartitions)}</td>
      <td>{Utils.bytesToString(rdd.memSize)}</td>
      <td>{Utils.bytesToString(rdd.diskSize)}</td>
    </tr>
  }
}
