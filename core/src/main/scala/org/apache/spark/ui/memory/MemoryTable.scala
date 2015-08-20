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

import java.util.Date

import scala.xml.Node

import org.apache.spark.scheduler.StageInfo
import org.apache.spark.ui.jobs.JobProgressListener
import org.apache.spark.ui.UIUtils
import org.apache.spark.util.Utils


private[ui] class MemTableBase(
    memInfos: Seq[(String, MemoryUIInfo)],
    listener: MemoryListener) {

  protected def columns: Seq[Node] = {
    <th>Executor ID</th>
    <th>Address</th>
    <th>Net Memory (on-heap)</th>
    <th>Net Memory (direct-heap)</th>
    <th>Peak Net Memory (on-heap) / Happen Time</th>
    <th>Peak Net Read (direct-heap) / Happen Time</th>
  }

  def toNodeSeq: Seq[Node] = {
    listener.synchronized {
      memTable(showRow, memInfos)
    }
  }

  protected def memTable[T](makeRow: T => Seq[Node], rows: Seq[T]): Seq[Node] = {
    <table class={UIUtils.TABLE_CLASS_STRIPED}>
      <thead>
        {columns}
      </thead>
      <tbody>
        {rows.map(r => makeRow(r))}
      </tbody>
    </table>
  }

  /** Render an HTML row representing an executor */
  private def showRow(info: (String, MemoryUIInfo)): Seq[Node] = {
    <tr>
      <td>
        {info._1}
      </td>
      <td>
        {info._2.executorAddress}
      </td>
      {if (info._2.transportInfo.isDefined) {
        <td>
          {Utils.bytesToString(info._2.transportInfo.get.onheapSize)}
        </td>
        <td>
          {Utils.bytesToString(info._2.transportInfo.get.directheapSize)}
        </td>
        <td>
          {Utils.bytesToString(info._2.transportInfo.get.peakOnheapSizeTime.memorySize)}
          /
          {UIUtils.formatDate(info._2.transportInfo.get.peakOnheapSizeTime.timeStamp)}
        </td>
        <td>
          {Utils.bytesToString(info._2.transportInfo.get.peakDirectheapSizeTime.memorySize)}
          /
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

private[ui] class stagesTableBase(
    stageInfos: Seq[StageInfo],
    basePath: String,
    listener: JobProgressListener) {
  protected def columns: Seq[Node] = {
    <th>Stage Id</th>
    <th>Description</th>
    <th>Submitted</th>
  }

  def toNodeSeq: Seq[Node] = {
    listener.synchronized {
      stagesTable(showRow, stageInfos)
    }
  }

  protected def stagesTable[T](makeRow: T => Seq[Node], rows: Seq[T]): Seq[Node] = {
    <table class={UIUtils.TABLE_CLASS_STRIPED}>
      <thead>
        {columns}
      </thead>
      <tbody>
        {rows.map(r => makeRow(r))}
      </tbody>
    </table>
  }

  private def showRow(info: StageInfo): Seq[Node] = {
    val submissionTime = info.submissionTime match {
      case Some(t) => UIUtils.formatDate(new Date(t))
      case None => "Unknown"
    }

    <tr>
      <td>{info.stageId}</td>
      <td>{makeDescription(info)}</td>
      <td>{submissionTime}</td>
    </tr>
}

  private def makeDescription(s: StageInfo): Seq[Node] = {
    val basePathUri = UIUtils.prependBaseUri(basePath)
    val nameLinkUri = s"$basePathUri/memory/stage?id=${s.stageId}&attempt=${s.attemptId}"
    <div>
      <a href={nameLinkUri} class="name-link">{s.name}</a>
    </div>
  }
}
