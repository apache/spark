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

import akka.actor.{Props, Actor}
import org.apache.spark.SparkEnv
import org.apache.spark.Logging
import org.apache.spark.streaming.{Checkpoint, Time, CheckpointWriter}
import org.apache.spark.streaming.util.{ManualClock, RecurringTimer, Clock}

/** Event classes for JobGenerator */
private[scheduler] sealed trait JobGeneratorEvent
private[scheduler] case class GenerateJobs(time: Time) extends JobGeneratorEvent
private[scheduler] case class ClearOldMetadata(time: Time) extends JobGeneratorEvent
private[scheduler] case class DoCheckpoint(time: Time) extends JobGeneratorEvent

/**
 * This class generates jobs from DStreams as well as drives checkpointing and cleaning
 * up DStream metadata.
 */
private[streaming]
class JobGenerator(jobScheduler: JobScheduler) extends Logging {

  val ssc = jobScheduler.ssc
  val graph = ssc.graph
  val eventProcessorActor = ssc.env.actorSystem.actorOf(Props(new Actor {
    def receive = {
      case event: JobGeneratorEvent =>
        logDebug("Got event of type " + event.getClass.getName)
        processEvent(event)
    }
  }))
  val clock = {
    val clockClass = ssc.sc.conf.get(
      "spark.streaming.clock", "org.apache.spark.streaming.util.SystemClock")
    Class.forName(clockClass).newInstance().asInstanceOf[Clock]
  }
  val timer = new RecurringTimer(clock, ssc.graph.batchDuration.milliseconds,
    longTime => eventProcessorActor ! GenerateJobs(new Time(longTime)))
  lazy val checkpointWriter = if (ssc.checkpointDuration != null && ssc.checkpointDir != null) {
    new CheckpointWriter(ssc.conf, ssc.checkpointDir, ssc.sparkContext.hadoopConfiguration)
  } else {
    null
  }

  def start() = synchronized {
    if (ssc.isCheckpointPresent) {
      restart()
    } else {
      startFirstTime()
    }
  }

  def stop() {
    timer.stop()
    if (checkpointWriter != null) checkpointWriter.stop()
    ssc.graph.stop()
    logInfo("JobGenerator stopped")
  }

  /**
   * On batch completion, clear old metadata and checkpoint computation.
   */
  private[scheduler] def onBatchCompletion(time: Time) {
    eventProcessorActor ! ClearOldMetadata(time)
  }

  /** Processes all events */
  private def processEvent(event: JobGeneratorEvent) {
    event match {
      case GenerateJobs(time) => generateJobs(time)
      case ClearOldMetadata(time) => clearOldMetadata(time)
      case DoCheckpoint(time) => doCheckpoint(time)
    }
  }

  /** Starts the generator for the first time */
  private def startFirstTime() {
    val startTime = new Time(timer.getStartTime())
    graph.start(startTime - graph.batchDuration)
    timer.start(startTime.milliseconds)
    logInfo("JobGenerator started at " + startTime)
  }

  /** Restarts the generator based on the information in checkpoint */
  private def restart() {
    // If manual clock is being used for testing, then
    // either set the manual clock to the last checkpointed time,
    // or if the property is defined set it to that time
    if (clock.isInstanceOf[ManualClock]) {
      val lastTime = ssc.initialCheckpoint.checkpointTime.milliseconds
      val jumpTime = ssc.sc.conf.get("spark.streaming.manualClock.jump", "0").toLong
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
    logInfo("JobGenerator restarted at " + restartTime)
  }

  /** Generate jobs and perform checkpoint for the given `time`.  */
  private def generateJobs(time: Time) {
    SparkEnv.set(ssc.env)
    logInfo("\n-----------------------------------------------------\n")
    jobScheduler.runJobs(time, graph.generateJobs(time))
    eventProcessorActor ! DoCheckpoint(time)
  }

  /** Clear DStream metadata for the given `time`. */
  private def clearOldMetadata(time: Time) {
    ssc.graph.clearOldMetadata(time)
    eventProcessorActor ! DoCheckpoint(time)
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

