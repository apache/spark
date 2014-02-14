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

import akka.actor.{ActorRef, ActorSystem, Props, Actor}
import org.apache.spark.{SparkException, SparkEnv, Logging}
import org.apache.spark.streaming.{Checkpoint, Time, CheckpointWriter}
import org.apache.spark.streaming.util.{ManualClock, RecurringTimer, Clock}
import scala.util.{Failure, Success, Try}

/** Event classes for JobGenerator */
private[scheduler] sealed trait JobGeneratorEvent
private[scheduler] case class GenerateJobs(time: Time) extends JobGeneratorEvent
private[scheduler] case class ClearMetadata(time: Time) extends JobGeneratorEvent
private[scheduler] case class DoCheckpoint(time: Time) extends JobGeneratorEvent
private[scheduler] case class ClearCheckpointData(time: Time) extends JobGeneratorEvent

/**
 * This class generates jobs from DStreams as well as drives checkpointing and cleaning
 * up DStream metadata.
 */
private[streaming]
class JobGenerator(jobScheduler: JobScheduler) extends Logging {

  private val ssc = jobScheduler.ssc
  private val graph = ssc.graph
  val clock = {
    val clockClass = ssc.sc.conf.get(
      "spark.streaming.clock", "org.apache.spark.streaming.util.SystemClock")
    Class.forName(clockClass).newInstance().asInstanceOf[Clock]
  }
  private val timer = new RecurringTimer(clock, ssc.graph.batchDuration.milliseconds,
    longTime => eventActor ! GenerateJobs(new Time(longTime)))
  private lazy val checkpointWriter =
    if (ssc.checkpointDuration != null && ssc.checkpointDir != null) {
      new CheckpointWriter(this, ssc.conf, ssc.checkpointDir, ssc.sparkContext.hadoopConfiguration)
  } else {
    null
  }

  // eventActor is created when generator starts.
  // This not being null means the scheduler has been started and not stopped
  private var eventActor: ActorRef = null

  /** Start generation of jobs */
  def start() = synchronized {
    if (eventActor != null) {
      throw new SparkException("JobGenerator already started")
    }

    eventActor = ssc.env.actorSystem.actorOf(Props(new Actor {
      def receive = {
        case event: JobGeneratorEvent =>
          logDebug("Got event of type " + event.getClass.getName)
          processEvent(event)
      }
    }), "JobGenerator")
    if (ssc.isCheckpointPresent) {
      restart()
    } else {
      startFirstTime()
    }
  }

  /** Stop generation of jobs */
  def stop() = synchronized {
    if (eventActor != null) {
      timer.stop()
      ssc.env.actorSystem.stop(eventActor)
      if (checkpointWriter != null) checkpointWriter.stop()
      ssc.graph.stop()
      logInfo("JobGenerator stopped")
    }
  }

  /**
   * On batch completion, clear old metadata and checkpoint computation.
   */
  def onBatchCompletion(time: Time) {
    eventActor ! ClearMetadata(time)
  }
  
  def onCheckpointCompletion(time: Time) {
    eventActor ! ClearCheckpointData(time)
  }

  /** Processes all events */
  private def processEvent(event: JobGeneratorEvent) {
    event match {
      case GenerateJobs(time) => generateJobs(time)
      case ClearMetadata(time) => clearMetadata(time)
      case DoCheckpoint(time) => doCheckpoint(time)
      case ClearCheckpointData(time) => clearCheckpointData(time)
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
      val jumpTime = ssc.sc.conf.getLong("spark.streaming.manualClock.jump", 0)
      clock.asInstanceOf[ManualClock].setTime(lastTime + jumpTime)
    }

    val batchDuration = ssc.graph.batchDuration

    // Batches when the master was down, that is,
    // between the checkpoint and current restart time
    val checkpointTime = ssc.initialCheckpoint.checkpointTime
    val restartTime = new Time(timer.getRestartTime(graph.zeroTime.milliseconds))
    val downTimes = checkpointTime.until(restartTime, batchDuration)
    logInfo("Batches during down time (" + downTimes.size + " batches): "
      + downTimes.mkString(", "))

    // Batches that were unprocessed before failure
    val pendingTimes = ssc.initialCheckpoint.pendingTimes.sorted(Time.ordering)
    logInfo("Batches pending processing (" + pendingTimes.size + " batches): " +
      pendingTimes.mkString(", "))
    // Reschedule jobs for these times
    val timesToReschedule = (pendingTimes ++ downTimes).distinct.sorted(Time.ordering)
    logInfo("Batches to reschedule (" + timesToReschedule.size + " batches): " +
      timesToReschedule.mkString(", "))
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
    Try(graph.generateJobs(time)) match {
      case Success(jobs) => jobScheduler.runJobs(time, jobs)
      case Failure(e) => jobScheduler.reportError("Error generating jobs for time " + time, e)
    }
    eventActor ! DoCheckpoint(time)
  }

  /** Clear DStream metadata for the given `time`. */
  private def clearMetadata(time: Time) {
    ssc.graph.clearMetadata(time)
    eventActor ! DoCheckpoint(time)
  }

  /** Clear DStream checkpoint data for the given `time`. */
  private def clearCheckpointData(time: Time) {
    ssc.graph.clearCheckpointData(time)
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
