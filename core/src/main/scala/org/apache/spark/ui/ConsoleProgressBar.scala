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

package org.apache.spark.ui

import java.util.{Timer, TimerTask}

import org.apache.spark._

/**
 * ConsoleProgressBar shows the progress of stages in the next line of the console. It poll the
 * status of active stages from `sc.statusTracker` periodically, the progress bar will be showed
 * up after the stage has ran at least 500ms. If multiple stages run in the same time, the status
 * of them will be combined together, showed in one line.
 */
private[spark] class ConsoleProgressBar(sc: SparkContext) extends Logging {

  // Carrige return
  val CR = '\r'
  // Update period of progress bar, in milliseconds
  val UPDATE_PERIOD = 200L
  // Delay to show up a progress bar, in milliseconds
  val FIRST_DELAY = 500L

  // The width of terminal
  val TerminalWidth = if (!sys.env.getOrElse("COLUMNS", "").isEmpty) {
    sys.env.get("COLUMNS").get.toInt
  } else {
    80
  }

  var lastFinishTime = 0L
  var lastUpdateTime = 0L
  var lastProgressBar = ""

  // Schedule a refresh thread to run periodically
  private val timer = new Timer("refresh progress", true)
  timer.schedule(new TimerTask{
    override def run() {
      refresh()
    }
  }, FIRST_DELAY, UPDATE_PERIOD)

  /**
   * Try to refresh the progress bar in every cycle
   */
  private def refresh(): Unit = synchronized {
    val now = System.currentTimeMillis()
    if (now - lastFinishTime < FIRST_DELAY) {
      return
    }
    val stageIds = sc.statusTracker.getActiveStageIds()
    val stages = stageIds.map(sc.statusTracker.getStageInfo).flatten.filter(_.numTasks() > 1)
      .filter(now - _.submissionTime() > FIRST_DELAY).sortBy(_.stageId())
    if (stages.size > 0) {
      show(now, stages.take(3))  // display at most 3 stages in same time
    }
  }

  /**
   * Show progress bar in console. The progress bar is displayed in the next line
   * after your last output, keeps overwriting itself to hold in one line. The logging will follow
   * the progress bar, then progress bar will be showed in next line without overwrite logs.
   */
  private def show(now: Long, stages: Seq[SparkStageInfo]) {
    val width = TerminalWidth / stages.size
    val bar = stages.map { s =>
      val total = s.numTasks()
      val header = s"[Stage ${s.stageId()}:"
      val tailer = s"(${s.numCompletedTasks()} + ${s.numActiveTasks()}) / $total]"
      val w = width - header.size - tailer.size
      val bar = if (w > 0) {
        val percent = w * s.numCompletedTasks() / total
        (0 until w).map { i =>
          if (i < percent) "=" else if (i == percent) ">" else " "
        }.mkString("")
      } else {
        ""
      }
      header + bar + tailer
    }.mkString("")

    // only refresh if it's changed of after 1 minute (or the ssh connection will be closed
    // after idle some time)
    if (bar != lastProgressBar || now - lastUpdateTime > 60 * 1000L) {
      System.err.print(CR + bar)
      lastUpdateTime = now
    }
    lastProgressBar = bar
  }

  /**
   * Clear the progress bar if showed.
   */
  private def clear() {
    if (!lastProgressBar.isEmpty) {
      System.err.printf(CR + " " * TerminalWidth + CR)
      lastProgressBar = ""
    }
  }

  /**
   * Mark all the stages as finished, clear the progress bar if showed, then the progress will not
   * interweave with output of jobs.
   */
  def finishAll(): Unit = synchronized {
    clear()
    lastFinishTime = System.currentTimeMillis()
  }
}
