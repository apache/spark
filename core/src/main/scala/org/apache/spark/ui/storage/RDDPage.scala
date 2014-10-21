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

import scala.xml.{Text, Node}

import org.apache.spark.storage.{BlockId, BlockStatus, StorageStatus, StorageUtils}
import org.apache.spark.ui.{UITableBuilder, UITable, WebUIPage, UIUtils}
import org.apache.spark.util.Utils

/** Page showing storage details for a given RDD */
private[ui] class RDDPage(parent: StorageTab) extends WebUIPage("rdd") {
  private val listener = parent.listener

  private val workerTable: UITable[(Int, StorageStatus)] = {
    val builder = new UITableBuilder[(Int, StorageStatus)]()
    import builder._
    col("Host") { case (_, status) =>
      s"${status.blockManagerId.host}:${status.blockManagerId.port}"
    }
    def getMemUsed(x: (Int, StorageStatus)): String = x._2.memUsedByRdd(x._1).toString
    customCol(
      "Memory usage",
      sortKey = Some(getMemUsed)) { case (rddId, status) =>
        val used = Utils.bytesToString(status.memUsedByRdd(rddId))
        val remaining = Utils.bytesToString(status.memRemaining)
        Text(s"$used ($remaining Remaining)")
      }
    memCol("Disk Usage") { case (rddId, status) => status.diskUsedByRdd(rddId) }
    build
  }

  val blockTable: UITable[(BlockId, BlockStatus, Seq[String])] = {
    val builder = new UITableBuilder[(BlockId, BlockStatus, Seq[String])]()
    import builder._
    col("Block Name") { case (id, block, locations) => id.toString }
    col("Storage Level") { case (id, block, locations) => block.storageLevel.description }
    memCol("Size in Memory") { case (id, block, locations) => block.memSize }
    memCol("Size on Disk") { case (id, block, locations) => block.diskSize }
    customCol("Executors") { case (id, block, locations) =>
      locations.map(l => <span>{l}<br/></span>)
    }
    build
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
    val workerTable = this.workerTable.render(workers)

    // Block table
    val blockLocations = StorageUtils.getRddBlockLocations(rddId, storageStatusList)
    val blocks = storageStatusList
      .flatMap(_.rddBlocksById(rddId))
      .sortWith(_._1.name < _._1.name)
      .map { case (blockId, status) =>
        (blockId, status, blockLocations.get(blockId).getOrElse(Seq[String]("Unknown")))
      }
    val blockTable = this.blockTable.render(blocks)

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
}
