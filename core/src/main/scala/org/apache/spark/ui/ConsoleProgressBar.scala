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
import scala.collection.mutable.HashMap

import org.apache.spark._
import org.apache.spark.scheduler.{SparkListenerStageSubmitted, SparkListenerStageCompleted, SparkListener}

/**
 * ConsoleProgressBar shows the progress of stages in the next line of the console. It poll the
 * status of active stages from `sc.statusTracker` in every 200ms, the progress bar will be showed
 * up after the stage has ran at least 500ms. If multiple stages run in the same time, the status
 * of them will be combined together, showed in one line.
 */
private[spark] class ConsoleProgressBar(sc: SparkContext) extends Logging {

  // Update period of progress bar, in milli seconds
  val UPDATE_PERIOD = 200L
  // Delay to show up a progress bar, in milli seconds
  val DELAY_SHOW_UP = 500L
  // The width of terminal
  val TerminalWidth = sys.env.getOrElse("COLUMNS", "80").toInt

  @volatile var hasShowed = false

  /**
   * Track the life cycle of stages
   */
  val activeStages = new HashMap[Int, Long]()

  private class StageProgressListener extends SparkListener {
    override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) = {
      activeStages.synchronized {
        activeStages.put(stageSubmitted.stageInfo.stageId, System.currentTimeMillis())
      }
    }
    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) = {
      activeStages.synchronized {
        activeStages.remove(stageCompleted.stageInfo.stageId)
        if (activeStages.isEmpty) {
          clearProgressBar()
        }
      }
    }
  }
  sc.listenerBus.addListener(new StageProgressListener)

  // Schedule a update thread to run in every 200ms
  private val timer = new Timer("show progress", true)
  timer.schedule(new TimerTask{
    override def run() {
      var running = 0
      var finished = 0
      var tasks = 0
      var failed = 0
      val now = System.currentTimeMillis()
      val stageIds = sc.statusTracker.getActiveStageIds()
      stageIds.map(sc.statusTracker.getStageInfo).foreach{
        case Some(stage) =>
          activeStages.synchronized {
            // Don't show progress for stage which has only one task (useless),
            // also don't show progress for stage which had started in 500 ms
            if (stage.numTasks > 1 && activeStages.contains(stage.stageId)
              && now - activeStages(stage.stageId) > DELAY_SHOW_UP) {
              tasks += stage.numTasks
              running += stage.numActiveTasks
              finished += stage.numCompletedTasks
              failed += stage.numFailedTasks
            }
          }
      }
      if (tasks > 0) {
        showProgressBar(stageIds, tasks, running, finished, failed)
      }
    }
  }, DELAY_SHOW_UP, UPDATE_PERIOD)

  /**
   * Show progress in console (also in title). The progress bar is displayed in the next line
   * after your last output, keeps overwriting itself to hold in one line. The logging will follow
   * the progress bar, then progress bar will be showed in next line without overwrite logs.
   */
  private def showProgressBar(stageIds: Seq[Int], total: Int, running: Int, finished: Int,
                              failed: Int): Unit = {
    // show progress of all stages in one line progress bar
    val ids = stageIds.mkString("/")
    if (!log.isInfoEnabled) {
      if (finished < total) {
        val header = s"Stage $ids: ["
        val tailer = s"] $finished + $running / $total"
        val width = TerminalWidth - header.size - tailer.size
        val percent = finished * width / total
        val bar = (0 until width).map { i =>
          if (i < percent) "=" else if (i == percent) ">" else " "
        }.mkString("")
        System.err.printf("\r" + header + bar + tailer)
      }
    }
    hasShowed = true
  }

  /**
   * Clear the progress bar if showed.
   */
  private def clearProgressBar() = {
    if (hasShowed) {
      System.err.printf("\r" + " " * TerminalWidth + "\r")
      hasShowed = false
    }
  }

  /**
   * Mark all the stages as finished, clear the progress bar if showed, then the progress will not
   * interwave with output of jobs.
   */
  def finishAll() = {
    clearProgressBar()
    activeStages.synchronized {
      activeStages.clear()
    }
  }
}
