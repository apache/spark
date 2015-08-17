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

package org.apache.spark.ui.memory

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.spark.ui.{UIUtils, WebUIPage}
import org.apache.spark.util.Utils

private[ui] class MemoryPage(parent: MemoryTab) extends WebUIPage("") {
  private val listener = parent.listener

  def render(request: HttpServletRequest): Seq[Node] = {

    val executorIdToMem = listener.executorIdToMem
    val memInfoSorted = executorIdToMem.toSeq.sortBy(_._1)

    val memTable =
      <table class={UIUtils.TABLE_CLASS_STRIPED}>
        <thead>
          <th>Executor ID</th>
          <th>Address</th>
          <th>Net Memory (on-heap)</th>
          <th>Net Memory (direct-heap)</th>
          <th>Peak Net Memory (on-heap) / Happen Time</th>
          <th>Peak Net Read (direct-heap) / Happen Time</th>
        </thead>
        <tbody>
          {memInfoSorted.map(showRow(_))}
        </tbody>
      </table>

    val content =
      <div class = "row">
        <div class="span12">
          {memTable}
        </div>
      </div>;

    UIUtils.headerSparkPage("Memory Usage", content, parent)
  }

  /** Render an HTML row representing an executor */
  private def showRow(info: (String, MemoryUIInfo)): Seq[Node] = {
    <tr>
      <td>{info._1}</td>
      <td>{info._2.executorAddress}</td>
      {if (info._2.transportInfo.isDefined) {
        <td>{Utils.bytesToString(info._2.transportInfo.get.onheapSize)}</td>
        <td>{Utils.bytesToString(info._2.transportInfo.get.directheapSize)}</td>
        <td>
          {Utils.bytesToString(info._2.transportInfo.get.peakOnheapSizeTime.memorySize)} /
          {UIUtils.formatDate(info._2.transportInfo.get.peakOnheapSizeTime.timeStamp)}
        </td>
        <td>
          {Utils.bytesToString(info._2.transportInfo.get.peakDirectheapSizeTime.memorySize)} /
          {UIUtils.formatDate(info._2.transportInfo.get.peakDirectheapSizeTime.timeStamp)}
        </td>
      } else {
        <td>N/A</td>
        <td>N/A</td>
        <td>N/A</td>
        <td>N/A</td>
      }}
    </tr>
  }
}