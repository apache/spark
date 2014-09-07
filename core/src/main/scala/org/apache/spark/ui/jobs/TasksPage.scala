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

package org.apache.spark.ui.jobs

import java.util.Date
import javax.servlet.http.HttpServletRequest

import org.json4s.DefaultFormats
import org.json4s.JsonDSL._
import org.json4s.JsonAST._

import scala.xml.Node

import org.apache.spark.ui.{ToolTips, WebUIPage, UIUtils}
import org.apache.spark.ui.jobs.UIData._
import org.apache.spark.util.{Utils, Distribution}

/** Page showing statistics and task list for a given stage */
private[ui] class TasksPage(parent: JobProgressTab) extends WebUIPage("stage/tasks") {
  private val listener = parent.listener

  def render(request: HttpServletRequest): Seq[Node] = {
    return UIUtils.headerSparkPage("Only JSON view available", Seq[Node](), parent)
  }

  override def renderJson(request: HttpServletRequest): JValue = {
    listener.synchronized {
      val stageId = request.getParameter("id").toInt
      val stageAttemptId = request.getParameter("attempt").toInt
      val stageDataOption = listener.stageIdToData.get((stageId,stageAttemptId))

      if (stageDataOption.isEmpty || stageDataOption.get.taskData.isEmpty) {
        return JNothing
      }

      val stageData = stageDataOption.get
      val tasks = stageData.taskData.values.toSeq.sortBy(_.taskInfo.launchTime)

      val hasInput = stageData.inputBytes > 0
      val hasShuffleRead = stageData.shuffleReadBytes > 0
      val hasShuffleWrite = stageData.shuffleWriteBytes > 0
      val hasBytesSpilled = stageData.memoryBytesSpilled > 0 && stageData.diskBytesSpilled > 0

      val taskJson = UIUtils.listingJson(
        taskRowJson(hasInput, hasShuffleRead, hasShuffleWrite, hasBytesSpilled),
        tasks)

      taskJson
    }
  }

  def taskRowJson(
               hasInput: Boolean,
               hasShuffleRead: Boolean,
               hasShuffleWrite: Boolean,
               hasBytesSpilled: Boolean)(taskData: TaskUIData): JValue = {
    taskData match { case TaskUIData(info, metrics, errorMessage) =>
      val duration = if (info.status == "RUNNING") info.timeRunning(System.currentTimeMillis())
      else metrics.map(_.executorRunTime).getOrElse(1L)
      val formatDuration = if (info.status == "RUNNING") UIUtils.formatDuration(duration)
      else metrics.map(m => UIUtils.formatDuration(m.executorRunTime)).getOrElse("")
      val gcTime = metrics.map(_.jvmGCTime).getOrElse(0L)
      val serializationTime = metrics.map(_.resultSerializationTime).getOrElse(0L)

      val maybeInput = metrics.flatMap(_.inputMetrics)
      val inputSortable = maybeInput.map(_.bytesRead.toString).getOrElse("")
      val inputReadable = maybeInput
        .map(m => s"${Utils.bytesToString(m.bytesRead)} (${m.readMethod.toString.toLowerCase()})")
        .getOrElse("")

      val maybeShuffleRead = metrics.flatMap(_.shuffleReadMetrics).map(_.remoteBytesRead)
      val shuffleReadSortable = maybeShuffleRead.map(_.toString).getOrElse("")
      val shuffleReadReadable = maybeShuffleRead.map(Utils.bytesToString).getOrElse("")

      val maybeShuffleWrite =
        metrics.flatMap(_.shuffleWriteMetrics).map(_.shuffleBytesWritten)
      val shuffleWriteSortable = maybeShuffleWrite.map(_.toString).getOrElse("")
      val shuffleWriteReadable = maybeShuffleWrite.map(Utils.bytesToString).getOrElse("")

      val maybeWriteTime = metrics.flatMap(_.shuffleWriteMetrics).map(_.shuffleWriteTime)
      val writeTimeSortable = maybeWriteTime.map(_.toString).getOrElse("")
      val writeTimeReadable = maybeWriteTime.map(t => t / (1000 * 1000)).map { ms =>
        if (ms == 0) "" else UIUtils.formatDuration(ms)
      }.getOrElse("")

      val maybeMemoryBytesSpilled = metrics.map(_.memoryBytesSpilled)
      val memoryBytesSpilledSortable = maybeMemoryBytesSpilled.map(_.toString).getOrElse("")
      val memoryBytesSpilledReadable =
        maybeMemoryBytesSpilled.map(Utils.bytesToString).getOrElse("")

      val maybeDiskBytesSpilled = metrics.map(_.diskBytesSpilled)
      val diskBytesSpilledSortable = maybeDiskBytesSpilled.map(_.toString).getOrElse("")
      val diskBytesSpilledReadable = maybeDiskBytesSpilled.map(Utils.bytesToString).getOrElse("")

      var content = ("Index" -> info.index) ~
        ("ID" -> info.taskId) ~
        ("Attempt" -> UIUtils.cellWithSorttableCustomKey(
          {if (info.speculative) s"${info.attempt} (speculative)"
                     else info.attempt.toString},
          info.attempt.toString)) ~
        ("Status" -> {info.status}) ~
        ("Locality Level" -> {info.taskLocality.toString}) ~
        ("Executor" -> {info.host}) ~
        ("Launch Time" -> {UIUtils.formatDate(new Date(info.launchTime))}) ~
        ("Duration" -> UIUtils.cellWithSorttableCustomKey(formatDuration, duration.toString)) ~
        ("GC Time" -> {if (gcTime > 0) {
          UIUtils.cellWithSorttableCustomKey(UIUtils.formatDuration(gcTime), gcTime.toString)
          } else JString("") })
      if (hasInput) {
        content ~= ("Input" -> UIUtils.cellWithSorttableCustomKey(inputReadable,
          inputSortable))
      }
      if (hasShuffleRead) {
        content ~= ("Shuffle Read" ->
          UIUtils.cellWithSorttableCustomKey(shuffleReadReadable, shuffleReadSortable))
      }
      if (hasShuffleWrite) {
        content ~= ("Write Time" ->
          UIUtils.cellWithSorttableCustomKey(writeTimeReadable, writeTimeSortable)) ~
          ("Shuffle Write" ->
            UIUtils.cellWithSorttableCustomKey(shuffleWriteReadable, shuffleWriteSortable))
      }
      if (hasBytesSpilled) {
        content ~= ("Shuffle Spill (Memory)" ->
          UIUtils.cellWithSorttableCustomKey(memoryBytesSpilledReadable,
            memoryBytesSpilledSortable)) ~
          ("Shuffle Spill (Disk)" ->
            UIUtils.cellWithSorttableCustomKey(diskBytesSpilledReadable,
              diskBytesSpilledSortable))
      }
      content ~= ("Errors" -> {errorMessage.map { e => "<pre>" + e + "</pre>" }.getOrElse("")})

      content
    }
  }
}
