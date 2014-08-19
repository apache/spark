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

import scala.xml.{Null, Node}

import org.apache.spark.storage._
import org.apache.spark.ui.{WebUIPage, UIUtils}
import org.apache.spark.util.Utils

/** Page showing storage details for a given RDD */
private[ui] class BlocksPage(parent: StorageTab) extends WebUIPage("rdd/blocks") {
  private val listener = parent.listener

  def render(request: HttpServletRequest): Seq[Node] = {
    return UIUtils.headerSparkPage("Only JSON view available", Seq[Node](), parent)
  }

  override def renderJson(request: HttpServletRequest): JValue = {
    val rddId = request.getParameter("id").toInt
    val storageStatusList = listener.storageStatusList
    val rddInfo = listener.rddInfoList.find(_.id == rddId).getOrElse {
      // Rather than crashing, return nothing
      return JNothing
    }

    // Block table
    val blocks = StorageUtils.blocksFromRDDId(rddId, storageStatusList)
    val blockJson = UIUtils.listingJson(blockRowJson, blocks)

    blockJson
  }

  /** Render a JSON row representing a block */
  private def blockRowJson(row: (BlockId, BlockStatus, Seq[String])): JValue = {
    val (id, block, locations) = row
    ("Block Name" -> id.name) ~
    ("Storage Level"-> block.storageLevel.description) ~
    ("Size in Memory"-> UIUtils.cellWithSorttableCustomKey(Utils.bytesToString(block.memSize),
      block.memSize.toString)) ~
    ("Size on Disk"-> UIUtils.cellWithSorttableCustomKey(Utils.bytesToString(block.diskSize),
      block.diskSize.toString)) ~
    ("Executors"-> locations)
  }
}
