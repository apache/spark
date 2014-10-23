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

import org.apache.spark.storage.RDDInfo
import org.apache.spark.ui.{UITableBuilder, UITable, WebUIPage, UIUtils}

/** Page showing list of RDD's currently stored in the cluster */
private[ui] class StoragePage(parent: StorageTab) extends WebUIPage("") {
  private val listener = parent.listener

  val rddTable: UITable[RDDInfo] = {
    val t = new UITableBuilder[RDDInfo]()
    t.customCol("RDD Name") { rdd =>
      <a href={"%s/storage/rdd?id=%s".format(UIUtils.prependBaseUri(parent.basePath), rdd.id)}>
        {rdd.name}
      </a>
    }
    t.col("Storage Level") { _.storageLevel.description }
    t.intCol("Cached Partitions") { _.numCachedPartitions }
    t. col("Fraction Cached") { rdd =>
      "%.0f%%".format(rdd.numCachedPartitions * 100.0 / rdd.numPartitions)
    }
    t.memCol("Size in Memory") { _.memSize }
    t.memCol("Size in Tachyon") { _.tachyonSize }
    t.memCol("Size on Disk") { _.diskSize }
    t.build()
  }

  def render(request: HttpServletRequest): Seq[Node] = {
    val content = rddTable.render(listener.rddInfoList)
    UIUtils.headerSparkPage("Storage", content, parent)
  }
}
