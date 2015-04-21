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

import scala.util.{Failure, Success, Try}

import org.apache.spark.{SparkEnv, Logging}
import org.apache.spark.streaming.{Checkpoint, CheckpointWriter, Time}
import org.apache.spark.streaming.util.RecurringTimer
import org.apache.spark.util.{Clock, EventLoop, ManualClock}

/** Event classes for JobGenerator */
private[scheduler] sealed trait JobGeneratorEvent
private[scheduler] case class GenerateJobs(time: Time) extends JobGeneratorEvent
private[scheduler] case class ClearMetadata(time: Time) extends JobGeneratorEvent
private[scheduler] case class DoCheckpoint(
    time: Time, clearCheckpointDataLater: Boolean) extends JobGeneratorEvent
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
    longTime => eventLoop.post(GenerateJobs(new Time(longTime))), "JobGenerator")

  // This is marked lazy so that this is initialized after checkpoint duration has been set
  // in the context and the generator has been started.
  private lazy val shouldCheckpoint = ssc.checkpointDuration != null && ssc.checkpointDir != null

  private lazy val checkpointWriter = if (shouldCheckpoint) {
    new CheckpointWriter(this, ssc.conf, ssc.checkpointDir, ssc.sparkContext.hadoopConfiguration)
  } else {
    null
  }

  // eventLoop is created when generator starts.
  // This not being null means the scheduler has been started and not stopped
  private var eventLoop: EventLoop[JobGeneratorEvent] = null

  // last batch whose completion,checkpointing and metadata cleanup has been completed
  private var lastProcessedBatch: Time = null

  /** Start generation of jobs */
  def start(): Unit = synchronized {
    if (eventLoop != null) return // generator has already been started

    eventLoop = new EventLoop[JobGeneratorEvent]("JobGenerator") {
      override protected def onReceive(event: JobGeneratorEvent): Unit = processEvent(event)

      override protected def onError(e: Throwable): Unit = {
        jobScheduler.reportError("Error in job generator", e)
      }
    }
    eventLoop.start()

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
    if (eventLoop == null) return // generator has already been stopped

    if (processReceivedData) {
      logInfo("Stopping JobGenerator gracefully")
      val timeWhenStopStarted = System.currentTimeMillis()
      val stopTimeoutMs = conf.getTimeAsMs(
        "spark.streaming.gracefulStopTimeout", s"${10 * ssc.graph.batchDuration.milliseconds}ms")
      val pollTime = 100

      // To prevent graceful stop to get stuck permanently
      def hasTimedOut: Boolean = {
        val timedOut = (System.currentTimeMillis() - timeWhenStopStarted) > stopTimeoutMs
        if (timedOut) {
          logWarning("Timed out while stopping the job generator (timeout = " + stopTimeoutMs + ")")
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
      val stopTime = timer.stop(interruptTimer = false)
      graph.stop()
      logInfo("Stopped generation timer")

      // Wait for the jobs to complete and checkpoints to be written
      def haveAllBatchesBeenProcessed: Boolean = {
        lastProcessedBatch != null && lastProcessedBatch.milliseconds == stopTime
      }
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

    // Stop the event loop and checkpoint writer
    if (shouldCheckpoint) checkpointWriter.stop()
    eventLoop.stop()
    logInfo("Stopped JobGenerator")
  }

  /**
   * Callback called when a batch has been completely processed.
   */
  def onBatchCompletion(time: Time) {
    eventLoop.post(ClearMetadata(time))
  }

  /**
   * Callback called when the checkpoint of a batch has been written.
   */
  def onCheckpointCompletion(time: Time, clearCheckpointDataLater: Boolean) {
    if (clearCheckpointDataLater) {
      eventLoop.post(ClearCheckpointData(time))
    }
  }

  /** Processes all events */
  private def processEvent(event: JobGeneratorEvent) {
    logDebug("Got event " + event)
    event match {
      case GenerateJobs(time) => generateJobs(time)
      case ClearMetadata(time) => clearMetadata(time)
      case DoCheckpoint(time, clearCheckpointDataLater) =>
        doCheckpoint(time, clearCheckpointDataLater)
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
      case Failure(e) =>
        jobScheduler.reportError("Error generating jobs for time " + time, e)
    }
    eventLoop.post(DoCheckpoint(time, clearCheckpointDataLater = false))
  }

  /** Clear DStream metadata for the given `time`. */
  private def clearMetadata(time: Time) {
    ssc.graph.clearMetadata(time)

    // If checkpointing is enabled, then checkpoint,
    // else mark batch to be fully processed
    if (shouldCheckpoint) {
      eventLoop.post(DoCheckpoint(time, clearCheckpointDataLater = true))
    } else {
      // If checkpointing is not enabled, then delete metadata information about
      // received blocks (block data not saved in any case). Otherwise, wait for
      // checkpointing of this batch to complete.
      val maxRememberDuration = graph.getMaxInputStreamRememberDuration()
      jobScheduler.receiverTracker.cleanupOldBlocksAndBatches(time - maxRememberDuration)
      markBatchFullyProcessed(time)
    }
  }

  /** Clear DStream checkpoint data for the given `time`. */
  private def clearCheckpointData(time: Time) {
    ssc.graph.clearCheckpointData(time)

    // All the checkpoint information about which batches have been processed, etc have
    // been saved to checkpoints, so its safe to delete block metadata and data WAL files
    val maxRememberDuration = graph.getMaxInputStreamRememberDuration()
    jobScheduler.receiverTracker.cleanupOldBlocksAndBatches(time - maxRememberDuration)
    markBatchFullyProcessed(time)
  }

  /** Perform checkpoint for the give `time`. */
  private def doCheckpoint(time: Time, clearCheckpointDataLater: Boolean) {
    if (shouldCheckpoint && (time - graph.zeroTime).isMultipleOf(ssc.checkpointDuration)) {
      logInfo("Checkpointing graph for time " + time)
      ssc.graph.updateCheckpointData(time)
      checkpointWriter.write(new Checkpoint(ssc, time), clearCheckpointDataLater)
    }
  }

  private def markBatchFullyProcessed(time: Time) {
    lastProcessedBatch = time
  }
}
