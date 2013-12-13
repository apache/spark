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

package org.apache.spark.streaming.scheduler

import org.apache.spark.SparkEnv
import org.apache.spark.Logging
import org.apache.spark.streaming.{Checkpoint, Time, CheckpointWriter}
import org.apache.spark.streaming.util.{ManualClock, RecurringTimer, Clock}

private[streaming]
class JobGenerator(jobScheduler: JobScheduler) extends Logging {

  initLogging()
  val ssc = jobScheduler.ssc
  val clockClass = System.getProperty(
    "spark.streaming.clock", "org.apache.spark.streaming.util.SystemClock")
  val clock = Class.forName(clockClass).newInstance().asInstanceOf[Clock]
  val timer = new RecurringTimer(clock, ssc.graph.batchDuration.milliseconds,
    longTime => generateJobs(new Time(longTime)))
  val graph = ssc.graph
  lazy val checkpointWriter = if (ssc.checkpointDuration != null && ssc.checkpointDir != null) {
    new CheckpointWriter(ssc.checkpointDir)
  } else {
    null
  }

  var latestTime: Time = null

  def start() = synchronized {
    if (ssc.isCheckpointPresent) {
      restart()
    } else {
      startFirstTime()
    }
    logInfo("JobGenerator started")
  }
  
  def stop() = synchronized {
    timer.stop()
    if (checkpointWriter != null) checkpointWriter.stop()
    ssc.graph.stop()
    logInfo("JobGenerator stopped")
  }

  private def startFirstTime() {
    val startTime = new Time(timer.getStartTime())
    graph.start(startTime - graph.batchDuration)
    timer.start(startTime.milliseconds)
    logInfo("JobGenerator's timer started at " + startTime)
  }

  private def restart() {
    // If manual clock is being used for testing, then
    // either set the manual clock to the last checkpointed time,
    // or if the property is defined set it to that time
    if (clock.isInstanceOf[ManualClock]) {
      val lastTime = ssc.initialCheckpoint.checkpointTime.milliseconds
      val jumpTime = System.getProperty("spark.streaming.manualClock.jump", "0").toLong
      clock.asInstanceOf[ManualClock].setTime(lastTime + jumpTime)
    }

    val batchDuration = ssc.graph.batchDuration

    // Batches when the master was down, that is,
    // between the checkpoint and current restart time
    val checkpointTime = ssc.initialCheckpoint.checkpointTime
    val restartTime = new Time(timer.getRestartTime(graph.zeroTime.milliseconds))
    val downTimes = checkpointTime.until(restartTime, batchDuration)
    logInfo("Batches during down time: " + downTimes.mkString(", "))

    // Batches that were unprocessed before failure
    val pendingTimes = ssc.initialCheckpoint.pendingTimes
    logInfo("Batches pending processing: " + pendingTimes.mkString(", "))
    // Reschedule jobs for these times
    val timesToReschedule = (pendingTimes ++ downTimes).distinct.sorted(Time.ordering)
    logInfo("Batches to reschedule: " + timesToReschedule.mkString(", "))
    timesToReschedule.foreach(time =>
      jobScheduler.runJobs(time, graph.generateJobs(time))
    )

    // Restart the timer
    timer.start(restartTime.milliseconds)
    logInfo("JobGenerator's timer restarted at " + restartTime)
  }

  /** Generate jobs and perform checkpoint for the given `time`.  */
  private def generateJobs(time: Time) {
    SparkEnv.set(ssc.env)
    logInfo("\n-----------------------------------------------------\n")
    jobScheduler.runJobs(time, graph.generateJobs(time))
    latestTime = time
    doCheckpoint(time)
  }

  /**
   * On batch completion, clear old metadata and checkpoint computation.
   */
  private[streaming] def onBatchCompletion(time: Time) {
    ssc.graph.clearOldMetadata(time)
    doCheckpoint(time)
  }

  /** Perform checkpoint for the give `time`. */
  private def doCheckpoint(time: Time) = synchronized {
    if (checkpointWriter != null && (time - graph.zeroTime).isMultipleOf(ssc.checkpointDuration)) {
      logInfo("Checkpointing graph for time " + time)
      ssc.graph.updateCheckpointData(time)
      checkpointWriter.write(new Checkpoint(ssc, time))
    }
  }
}

