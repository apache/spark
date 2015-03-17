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

import java.util.{Collections, TreeSet}

import scala.collection.JavaConversions._
import scala.collection.mutable.HashSet
import scala.util.{Failure, Success, Try}

import akka.actor.{Actor, ActorRef, Props}

import org.apache.spark.{SparkEnv, Logging}
import org.apache.spark.streaming.{TimeComparator, Checkpoint, CheckpointWriter, Time}
import org.apache.spark.streaming.util.RecurringTimer
import org.apache.spark.util.{Clock, ManualClock}

/** Event classes for JobGenerator */
private[scheduler] sealed trait JobGeneratorEvent
private[scheduler] case class GenerateJobs(time: Time) extends JobGeneratorEvent
private[scheduler] case class ClearMetadata(time: Time) extends JobGeneratorEvent
private[scheduler] case class DoCheckpoint(
    time: Time, clearCheckpointData: Boolean) extends JobGeneratorEvent
private[scheduler] case class ClearCheckpointData(time: Time) extends JobGeneratorEvent

/**
 * This class generates jobs from DStreams as well as drives checkpointing and cleaning
 * up DStream metadata.
 */
private[streaming]
class JobGenerator(jobScheduler: JobScheduler) extends Logging {

  private val ssc = jobScheduler.ssc
  private val conf = ssc.conf
  private val graph = ssc.graph

  // Scala treeset makes it difficult to drop items while iterating over the set. dropWhile
  // creates a new set, which does not seem very efficient. So use Java treeset instead.
  private val activeBatches =
    Collections.synchronizedSet[Time](new TreeSet[Time](new TimeComparator()))

  // Batches that have been completed and checkpointed, unused if checkpointing is not enabled
  private lazy val inactiveBatches = new HashSet[Time]()

  val clock = {
    val clockClass = ssc.sc.conf.get(
      "spark.streaming.clock", "org.apache.spark.util.SystemClock")
    try {
      Class.forName(clockClass).newInstance().asInstanceOf[Clock]
    } catch {
      case e: ClassNotFoundException if clockClass.startsWith("org.apache.spark.streaming") =>
        val newClockClass = clockClass.replace("org.apache.spark.streaming", "org.apache.spark")
        Class.forName(newClockClass).newInstance().asInstanceOf[Clock]
    }
  }

  private val timer = new RecurringTimer(clock, ssc.graph.batchDuration.milliseconds,
    longTime => eventActor ! GenerateJobs(new Time(longTime)), "JobGenerator")

  // This is marked lazy so that this is initialized after checkpoint duration has been set
  // in the context and the generator has been started.
  private lazy val shouldCheckpoint = ssc.checkpointDuration != null && ssc.checkpointDir != null

  private lazy val checkpointWriter = if (shouldCheckpoint) {
    new CheckpointWriter(this, ssc.conf, ssc.checkpointDir, ssc.sparkContext.hadoopConfiguration)
  } else {
    null
  }

  // eventActor is created when generator starts.
  // This not being null means the scheduler has been started and not stopped
  private var eventActor: ActorRef = null

  /** Start generation of jobs */
  def start(): Unit = synchronized {
    if (eventActor != null) return // generator has already been started

    eventActor = ssc.env.actorSystem.actorOf(Props(new Actor {
      def receive = {
        case event: JobGeneratorEvent =>  processEvent(event)
      }
    }), "JobGenerator")
    if (ssc.isCheckpointPresent) {
      restart()
    } else {
      startFirstTime()
    }
  }

  /**
   * Stop generation of jobs. processReceivedData = true makes this wait until jobs
   * of current ongoing time interval has been generated, processed and corresponding
   * checkpoints written.
   */
  def stop(processReceivedData: Boolean): Unit = synchronized {
    if (eventActor == null) return // generator has already been stopped

    if (processReceivedData) {
      logInfo("Stopping JobGenerator gracefully")
      val timeWhenStopStarted = System.currentTimeMillis()
      val stopTimeout = conf.getLong(
        "spark.streaming.gracefulStopTimeout",
        10 * ssc.graph.batchDuration.milliseconds
      )
      val pollTime = 100

      // To prevent graceful stop to get stuck permanently
      def hasTimedOut = {
        val timedOut = System.currentTimeMillis() - timeWhenStopStarted > stopTimeout
        if (timedOut) {
          logWarning("Timed out while stopping the job generator (timeout = " + stopTimeout + ")")
        }
        timedOut
      }

      // Wait until all the received blocks in the network input tracker has
      // been consumed by network input DStreams, and jobs have been generated with them
      logInfo("Waiting for all received blocks to be consumed for job generation")
      while(!hasTimedOut && jobScheduler.receiverTracker.hasUnallocatedBlocks) {
        Thread.sleep(pollTime)
      }
      logInfo("Waited for all received blocks to be consumed for job generation")

      // Stop generating jobs
      timer.stop(interruptTimer = false)
      graph.stop()
      logInfo("Stopped generation timer")

      // Wait for the jobs to complete and checkpoints to be written
      // At this point, since the timer is stopped, we don't have to worry about batches being
      // added to be activeBatches set
      def haveAllBatchesBeenProcessed = activeBatches.isEmpty

      logInfo("Waiting for jobs to be processed and checkpoints to be written")
      while (!hasTimedOut && !haveAllBatchesBeenProcessed) {
        Thread.sleep(pollTime)
      }
      logInfo("Waited for jobs to be processed and checkpoints to be written")
    } else {
      logInfo("Stopping JobGenerator immediately")
      // Stop timer and graph immediately, ignore unprocessed data and pending jobs
      timer.stop(true)
      graph.stop()
    }

    // Stop the actor and checkpoint writer
    if (shouldCheckpoint) checkpointWriter.stop()
    ssc.env.actorSystem.stop(eventActor)
    logInfo("Stopped JobGenerator")
  }

  /**
   * Callback called when a batch has been completely processed.
   */
  def onBatchCompletion(time: Time) {
    // Update the lastCompletedBatch only if this batch is actually newer than the previously
    // completed ones.
    eventActor ! ClearMetadata(time)
  }

  /**
   * Callback called when the checkpoint of a batch has been written.
   */
  def onCheckpointCompletion(time: Time, clearCheckpointData: Boolean) {
    if (clearCheckpointData) {
      eventActor ! ClearCheckpointData(time)
    }
  }

  /** Processes all events */
  private def processEvent(event: JobGeneratorEvent) {
    logDebug("Got event " + event)
    event match {
      case GenerateJobs(time) => generateJobs(time)
      case ClearMetadata(time) => clearMetadata(time)
      case DoCheckpoint(time, clearCheckpointData) =>
        doCheckpoint(time, clearCheckpointData)
      case ClearCheckpointData(time) => clearCheckpointData(time)
    }
  }

  /** Starts the generator for the first time */
  private def startFirstTime() {
    val startTime = new Time(timer.getStartTime())
    graph.start(startTime - graph.batchDuration)
    timer.start(startTime.milliseconds)
    logInfo("Started JobGenerator at " + startTime)
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
    timesToReschedule.foreach { time =>
      // Allocate the related blocks when recovering from failure, because some blocks that were
      // added but not allocated, are dangling in the queue after recovering, we have to allocate
      // those blocks to the next batch, which is the batch they were supposed to go.
      jobScheduler.receiverTracker.allocateBlocksToBatch(time) // allocate received blocks to batch
      jobScheduler.submitJobSet(JobSet(time, graph.generateJobs(time)))
      activeBatches += time
    }

    // Restart the timer
    timer.start(restartTime.milliseconds)
    logInfo("Restarted JobGenerator at " + restartTime)
  }

  /** Generate jobs and perform checkpoint for the given `time`.  */
  private def generateJobs(time: Time) {
    // Set the SparkEnv in this thread, so that job generation code can access the environment
    // Example: BlockRDDs are created in this thread, and it needs to access BlockManager
    // Update: This is probably redundant after threadlocal stuff in SparkEnv has been removed.
    SparkEnv.set(ssc.env)
    Try {
      jobScheduler.receiverTracker.allocateBlocksToBatch(time) // allocate received blocks to batch
      graph.generateJobs(time) // generate jobs using allocated block
    } match {
      case Success(jobs) =>
        val receivedBlockInfos =
          jobScheduler.receiverTracker.getBlocksOfBatch(time).mapValues { _.toArray }
        jobScheduler.submitJobSet(JobSet(time, jobs, receivedBlockInfos))
        activeBatches += time
      case Failure(e) =>
        jobScheduler.reportError("Error generating jobs for time " + time, e)
    }
    eventActor ! DoCheckpoint(time, clearCheckpointData = false)
  }

  /** Clear DStream metadata for the given `time`. */
  private def clearMetadata(time: Time) {

    // If checkpointing is enabled, then checkpoint,
    // else mark batch to be fully processed
    if (isCheckpointRequired(time)) {
      eventActor ! DoCheckpoint(time, clearCheckpointData = true)
    } else {
      // If checkpointing is not enabled, then delete metadata information about
      // received blocks (block data not saved in any case). Otherwise, wait for
      // checkpointing of this batch to complete.
      // This is synchronized so that multiple batches completing update this only one by one
      cleanupOldBlocksAndBatches(time)
    }
  }

  /** Clear DStream checkpoint data for the given `time`. */
  private def clearCheckpointData(time: Time) {
    cleanupOldBlocksAndBatches(time)
  }

  private def cleanupOldBlocksAndBatches(time: Time): Unit = {
    var lastProcessedBatch: Option[Time] = None
    if (!isCheckpointRequired(time)) {
      // If checkpoint is not enabled, the lastProcessedBatch is
      // the one which is the oldest.
      // If checkpointing is enabled, lastProcessedBatch is the one that has been completed and
      // checkpointed, so don't update lastProcessedBatch if this is just one batch that does
      // not have to be checkpointed.
      if (!shouldCheckpoint && activeBatches.head == time) {
        lastProcessedBatch = Some(time)
      }
      activeBatches -= time
    } else {
      // Remove all oldest batches if they have been checkpointed.
      inactiveBatches += time
      var continue = true
      val activeIter = activeBatches.iterator
      while (continue && activeIter.hasNext) {
        val t = activeIter.next()
        if (inactiveBatches.remove(t)) {
          lastProcessedBatch = Some(t)
          activeIter.remove()
        } else {
          continue = false
        }
      }
    }
    // All the checkpoint information about which batches have been processed, etc have
    // been saved to checkpoints, so its safe to delete block metadata and data WAL files
    lastProcessedBatch.foreach { lastProcessed =>
      val removalTime = lastProcessed - graph.getMaxInputStreamRememberDuration()
      ssc.graph.clearMetadata(removalTime)
      if (shouldCheckpoint) ssc.graph.clearCheckpointData(removalTime)
      jobScheduler.receiverTracker
        .cleanupOldBlocksAndBatches(removalTime)
    }
  }

  /** Perform checkpoint for the give `time`. */
  private def doCheckpoint(time: Time, clearCheckpointData: Boolean) {
    if (isCheckpointRequired(time)) {
      logInfo("Checkpointing graph for time " + time)
      ssc.graph.updateCheckpointData(time)
      checkpointWriter.write(new Checkpoint(ssc, time), clearCheckpointData)
    }
  }

  private def isCheckpointRequired(time: Time): Boolean = {
    shouldCheckpoint && (time - graph.zeroTime).isMultipleOf(ssc.checkpointDuration)
  }

}
