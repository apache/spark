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

import java.util.concurrent.CountDownLatch

import scala.collection.mutable.{ArrayBuffer, HashMap, SynchronizedMap}
import scala.concurrent.ExecutionContext
import scala.language.existentials
import scala.math.max
import scala.util.{Failure, Success}

import org.apache.spark.streaming.util.WriteAheadLogUtils
import org.apache.spark.{TaskContext, Logging, SparkEnv, SparkException}
import org.apache.spark.rdd.RDD
import org.apache.spark.rpc._
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.receiver.{CleanupOldBlocks, Receiver, ReceiverSupervisorImpl,
  StopReceiver}
import org.apache.spark.util.{ThreadUtils, SerializableConfiguration}


/** Enumeration to identify current state of a Receiver */
private[streaming] object ReceiverState extends Enumeration {
  type ReceiverState = Value
  val INACTIVE, SCHEDULED, ACTIVE = Value
}

/**
 * Messages used by the NetworkReceiver and the ReceiverTracker to communicate
 * with each other.
 */
private[streaming] sealed trait ReceiverTrackerMessage
private[streaming] case class RegisterReceiver(
    streamId: Int,
    typ: String,
    host: String,
    receiverEndpoint: RpcEndpointRef
  ) extends ReceiverTrackerMessage
private[streaming] case class AddBlock(receivedBlockInfo: ReceivedBlockInfo)
  extends ReceiverTrackerMessage
private[streaming] case class ReportError(streamId: Int, message: String, error: String)
private[streaming] case class DeregisterReceiver(streamId: Int, msg: String, error: String)
  extends ReceiverTrackerMessage

/** It's used to ask ReceiverTracker to return a candidate executor list to run the receiver */
private[streaming] case class ScheduleReceiver(streamId: Int) extends ReceiverTrackerMessage

/**
 * This class manages the execution of the receivers of ReceiverInputDStreams. Instance of
 * this class must be created after all input streams have been added and StreamingContext.start()
 * has been called because it needs the final set of input streams at the time of instantiation.
 *
 * @param skipReceiverLaunch Do not launch the receiver. This is useful for testing.
 */
private[streaming]
class ReceiverTracker(ssc: StreamingContext, skipReceiverLaunch: Boolean = false) extends Logging {

  private val receiverInputStreams = ssc.graph.getReceiverInputStreams()
  private val receiverInputStreamIds = receiverInputStreams.map { _.id }
  private val receiverExecutor = new ReceiverLauncher()
  private val receiverInfo = new HashMap[Int, ReceiverInfo] with SynchronizedMap[Int, ReceiverInfo]
  private val receivedBlockTracker = new ReceivedBlockTracker(
    ssc.sparkContext.conf,
    ssc.sparkContext.hadoopConfiguration,
    receiverInputStreamIds,
    ssc.scheduler.clock,
    ssc.isCheckpointPresent,
    Option(ssc.checkpointDir)
  )
  private val listenerBus = ssc.scheduler.listenerBus

  /** Enumeration to identify current state of the ReceiverTracker */
  object TrackerState extends Enumeration {
    type CheckpointState = Value
    val Initialized, Started, Stopping, Stopped = Value
  }
  import TrackerState._

  /** State of the tracker. Protected by "trackerStateLock" */
  private var trackerState = Initialized

  /** "trackerStateLock" is used to protect reading/writing "trackerState" */
  private val trackerStateLock = new AnyRef

  // endpoint is created when generator starts.
  // This not being null means the tracker has been started and not stopped
  private var endpoint: RpcEndpointRef = null

  private val schedulingPolicy: ReceiverSchedulingPolicy =
    new LoadBalanceReceiverSchedulingPolicyImpl()

  /**
   * Track receivers' status for scheduling
   */
  private val receiverTrackingInfos = new HashMap[Int, ReceiverTrackingInfo]

  /**
   * Store all preferred locations for all receivers. We need this information to schedule receivers
   */
  private val receiverPreferredLocations = new HashMap[Int, Option[String]]

  /** Use a separate lock to avoid dead-lock */
  private val receiverTrackingInfosLock = new AnyRef

  /** Check if tracker has been marked for starting */
  private def isTrackerStarted(): Boolean = trackerStateLock.synchronized {
    trackerState == Started
  }
 
  /** Check if tracker has been marked for stopping */
  private def isTrackerStopping(): Boolean = trackerStateLock.synchronized {
    trackerState == Stopping
  }
 
  /** Check if tracker has been marked for stopped */
  private def isTrackerStopped(): Boolean = trackerStateLock.synchronized {
    trackerState == Stopped
  }

  /** Start the endpoint and receiver execution thread. */
  def start(): Unit = synchronized {
    if (isTrackerStarted) {
      throw new SparkException("ReceiverTracker already started")
    }

    if (!receiverInputStreams.isEmpty) {
      endpoint = ssc.env.rpcEnv.setupEndpoint(
        "ReceiverTracker", new ReceiverTrackerEndpoint(ssc.env.rpcEnv))
      if (!skipReceiverLaunch) receiverExecutor.start()
      logInfo("ReceiverTracker started")
      trackerStateLock.synchronized {
        trackerState = Started
      }
    }
  }

  /** Stop the receiver execution thread. */
  def stop(graceful: Boolean): Unit = synchronized {
    if (isTrackerStarted) {
      // First, stop the receivers
      trackerStateLock.synchronized {
        trackerState = Stopping
      }
      if (!skipReceiverLaunch) receiverExecutor.stop(graceful)

      // Finally, stop the endpoint
      ssc.env.rpcEnv.stop(endpoint)
      endpoint = null
      receivedBlockTracker.stop()
      logInfo("ReceiverTracker stopped")
      trackerStateLock.synchronized {
        trackerState = Stopped
      }
    }
  }

  /** Allocate all unallocated blocks to the given batch. */
  def allocateBlocksToBatch(batchTime: Time): Unit = {
    if (receiverInputStreams.nonEmpty) {
      receivedBlockTracker.allocateBlocksToBatch(batchTime)
    }
  }

  /** Get the blocks for the given batch and all input streams. */
  def getBlocksOfBatch(batchTime: Time): Map[Int, Seq[ReceivedBlockInfo]] = {
    receivedBlockTracker.getBlocksOfBatch(batchTime)
  }

  /** Get the blocks allocated to the given batch and stream. */
  def getBlocksOfBatchAndStream(batchTime: Time, streamId: Int): Seq[ReceivedBlockInfo] = {
    synchronized {
      receivedBlockTracker.getBlocksOfBatchAndStream(batchTime, streamId)
    }
  }

  /**
   * Clean up the data and metadata of blocks and batches that are strictly
   * older than the threshold time. Note that this does not
   */
  def cleanupOldBlocksAndBatches(cleanupThreshTime: Time) {
    // Clean up old block and batch metadata
    receivedBlockTracker.cleanupOldBatches(cleanupThreshTime, waitForCompletion = false)

    // Signal the receivers to delete old block data
    if (WriteAheadLogUtils.enableReceiverLog(ssc.conf)) {
      logInfo(s"Cleanup old received batch data: $cleanupThreshTime")
      receiverInfo.values.flatMap { info => Option(info.endpoint) }
        .foreach { _.send(CleanupOldBlocks(cleanupThreshTime)) }
    }
  }

  /** Register a receiver */
  private def registerReceiver(
      streamId: Int,
      typ: String,
      host: String,
      receiverEndpoint: RpcEndpointRef,
      senderAddress: RpcAddress
    ): Boolean = {
    if (!receiverInputStreamIds.contains(streamId)) {
      throw new SparkException("Register received for unexpected id " + streamId)
    }
    trackerStateLock.synchronized {
      if (isTrackerStopping || isTrackerStopped) {
        false
      } else {
        // When updating "receiverInfo", we should make sure "trackerState" won't be changed at the
        // same time. Therefore the following line should be in "trackerStateLock.synchronized".
        receiverInfo(streamId) = ReceiverInfo(
          streamId, s"${typ}-${streamId}", receiverEndpoint, true, host)
        updateReceiverRunningLocation(streamId, host)
        listenerBus.post(StreamingListenerReceiverStarted(receiverInfo(streamId)))
        logInfo("Registered receiver for stream " + streamId + " from " + senderAddress)
        true
      }
    }
  }

  /** Deregister a receiver */
  private def deregisterReceiver(streamId: Int, message: String, error: String) {
    val newReceiverInfo = receiverInfo.get(streamId) match {
      case Some(oldInfo) =>
        val lastErrorTime =
          if (error == null || error == "") -1 else ssc.scheduler.clock.getTimeMillis()
        oldInfo.copy(endpoint = null, active = false, lastErrorMessage = message,
          lastError = error, lastErrorTime = lastErrorTime)
      case None =>
        logWarning("No prior receiver info")
        val lastErrorTime =
          if (error == null || error == "") -1 else ssc.scheduler.clock.getTimeMillis()
        ReceiverInfo(streamId, "", null, false, "", lastErrorMessage = message,
          lastError = error, lastErrorTime = lastErrorTime)
    }
    receiverInfo -= streamId
    listenerBus.post(StreamingListenerReceiverStopped(newReceiverInfo))
    val messageWithError = if (error != null && !error.isEmpty) {
      s"$message - $error"
    } else {
      s"$message"
    }
    logError(s"Deregistered receiver for stream $streamId: $messageWithError")
  }

  /** Add new blocks for the given stream */
  private def addBlock(receivedBlockInfo: ReceivedBlockInfo): Boolean = {
    receivedBlockTracker.addBlock(receivedBlockInfo)
  }

  /** Report error sent by a receiver */
  private def reportError(streamId: Int, message: String, error: String) {
    val newReceiverInfo = receiverInfo.get(streamId) match {
      case Some(oldInfo) =>
        oldInfo.copy(lastErrorMessage = message, lastError = error)
      case None =>
        logWarning("No prior receiver info")
        ReceiverInfo(streamId, "", null, false, "", lastErrorMessage = message,
          lastError = error, lastErrorTime = ssc.scheduler.clock.getTimeMillis())
    }
    receiverInfo(streamId) = newReceiverInfo
    listenerBus.post(StreamingListenerReceiverError(receiverInfo(streamId)))
    val messageWithError = if (error != null && !error.isEmpty) {
      s"$message - $error"
    } else {
      s"$message"
    }
    logWarning(s"Error reported by receiver for stream $streamId: $messageWithError")
  }

  /** Check if any blocks are left to be processed */
  def hasUnallocatedBlocks: Boolean = {
    receivedBlockTracker.hasUnallocatedReceivedBlocks
  }

  /** RpcEndpoint to receive messages from the receivers. */
  private class ReceiverTrackerEndpoint(override val rpcEnv: RpcEnv) extends ThreadSafeRpcEndpoint {

    override def receive: PartialFunction[Any, Unit] = {
      case ReportError(streamId, message, error) =>
        reportError(streamId, message, error)
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case RegisterReceiver(streamId, typ, host, receiverEndpoint) =>
        val successful =
          registerReceiver(streamId, typ, host, receiverEndpoint, context.sender.address)
        context.reply(successful)
      case AddBlock(receivedBlockInfo) =>
        context.reply(addBlock(receivedBlockInfo))
      case DeregisterReceiver(streamId, message, error) =>
        deregisterReceiver(streamId, message, error)
        context.reply(true)
      case ScheduleReceiver(streamId) =>
        context.reply(scheduleReceiver(streamId))
    }
  }

  /** This thread class runs all the receivers on the cluster.  */
  class ReceiverLauncher {
    @transient val env = ssc.env
    @volatile @transient private var running = false
    @transient val thread = new Thread() {
      override def run() {
        try {
          SparkEnv.set(env)
          startReceivers()
        } catch {
          case ie: InterruptedException => logInfo("ReceiverLauncher interrupted")
        }
      }
    }

    def start() {
      thread.start()
    }

    def stop(graceful: Boolean) {
      // Send the stop signal to all the receivers
      stopReceivers()

      // Wait for the Spark job that runs the receivers to be over
      // That is, for the receivers to quit gracefully.
      thread.join(10000)

      if (graceful) {
        val pollTime = 100
        logInfo("Waiting for receiver job to terminate gracefully")
        while (receiverInfo.nonEmpty || running) {
          Thread.sleep(pollTime)
        }
        logInfo("Waited for receiver job to terminate gracefully")
      }

      // Check if all the receivers have been deregistered or not
      if (receiverInfo.nonEmpty) {
        logWarning("Not all of the receivers have deregistered, " + receiverInfo)
      } else {
        logInfo("All of the receivers have deregistered successfully")
      }
    }

    /** Set host location(s) for each receiver so as to distribute them over
     * executors in a round-robin fashion taking into account preferredLocation if set
     */
    private[streaming] def scheduleReceivers(receivers: Seq[Receiver[_]],
      executors: List[String]): Array[ArrayBuffer[String]] = {
      val locations = new Array[ArrayBuffer[String]](receivers.length)
      var i = 0
      for (i <- 0 until receivers.length) {
        locations(i) = new ArrayBuffer[String]()
        if (receivers(i).preferredLocation.isDefined) {
          locations(i) += receivers(i).preferredLocation.get
        }
      }
      var count = 0
      for (i <- 0 until max(receivers.length, executors.length)) {
        if (!receivers(i % receivers.length).preferredLocation.isDefined) {
          locations(i % receivers.length) += executors(count)
          count += 1
          if (count == executors.length) {
            count = 0
          }
        }
      }
      locations
    }

    private val submitJobThread = ExecutionContext.fromExecutorService(
      ThreadUtils.newDaemonSingleThreadExecutor("streaming-submit-job"))

    /**
     * Get the receivers from the ReceiverInputDStreams, distributes them to the
     * worker nodes as a parallel collection, and runs them.
     */
    private def startReceivers() {
      val receivers = receiverInputStreams.map(nis => {
        val rcvr = nis.getReceiver()
        rcvr.setReceiverId(nis.id)
        rcvr
      })

      initReceiverTrackingInfos(receivers)

      // Tracking the active receiver number. When a receiver exits, countDown will be called.
      val receiverExitLatch = new CountDownLatch(receivers.size)

      val checkpointDirOption = Option(ssc.checkpointDir)
      val serializableHadoopConf =
        new SerializableConfiguration(ssc.sparkContext.hadoopConfiguration)

      // Function to start the receiver on the worker node
      val startReceiver = (iterator: Iterator[Receiver[_]]) => {
        if (!iterator.hasNext) {
          throw new SparkException(
            "Could not start receiver as object not found.")
        }
        if (TaskContext.get().attemptNumber() == 0) {
          val receiver = iterator.next()
          val supervisor = new ReceiverSupervisorImpl(
            receiver, SparkEnv.get, serializableHadoopConf.value, checkpointDirOption)
          supervisor.start()
          supervisor.awaitTermination()
        } else {
          // It's restarted by TaskScheduler, but we want to reschedule it again. So exit it.
        }
      }

      // Run the dummy Spark job to ensure that all slaves have registered.
      // This avoids all the receivers to be scheduled on the same node.
      if (!ssc.sparkContext.isLocal) {
        ssc.sparkContext.makeRDD(1 to 50, 50).map(x => (x, 1)).reduceByKey(_ + _, 20).collect()
      }

      // Distribute the receivers and start them
      logInfo("Starting " + receivers.length + " receivers")
      running = true
      for (receiver <- receivers) {
        submitJobThread.execute(new Runnable {
          override def run(): Unit = {
            if (isTrackerStopping()) {
              receiverExitLatch.countDown()
              return
            }

            val self = this
            val receiverId = receiver.streamId
            val scheduledLocations = schedulingPolicy.scheduleReceiver(
              receiverId,
              receiver.preferredLocation,
              getReceiverTrackingInfoMap(),
              getExecutors(ssc))
            updateReceiverScheduledLocations(receiver.streamId, scheduledLocations)
            val receiverRDD: RDD[Receiver[_]] =
              if (scheduledLocations.isEmpty) {
                ssc.sc.makeRDD(Seq(receiver), 1)
              } else {
                ssc.sc.makeRDD(Seq(receiver -> scheduledLocations))
              }
            val future = ssc.sparkContext.submitJob[Receiver[_], Unit, Unit](
              receiverRDD, startReceiver, Seq(0), (_, _) => Unit, ())
            future.onComplete {
              case Success(_) =>
                if (isTrackerStopping()) {
                  receiverExitLatch.countDown()
                } else {
                  logInfo(s"Restarting Receiver $receiverId")
                  submitJobThread.execute(self)
                }
              case Failure(e) =>
                if (isTrackerStopping()) {
                  receiverExitLatch.countDown()
                } else {
                  logError("Receiver has been stopped. Try to restart it.", e)
                  logInfo(s"Restarting Receiver $receiverId")
                  submitJobThread.execute(self)
                }
            }(ThreadUtils.sameThread)
            logInfo(s"Receiver ${receiver.streamId} started")
          }
        })
      }
      try {
        // Wait until all receivers exit
        receiverExitLatch.await()
        logInfo("All of the receivers have been terminated")
      } finally {
        running = false
        submitJobThread.shutdownNow()
      }
    }

    /** Stops the receivers. */
    private def stopReceivers() {
      // Signal the receivers to stop
      receiverInfo.values.flatMap { info => Option(info.endpoint)}
                         .foreach { _.send(StopReceiver) }
      logInfo("Sent stop signal to all " + receiverInfo.size + " receivers")
    }
  }

  def initReceiverTrackingInfos(receivers: Seq[Receiver[_]]): Unit = synchronized {
    for (receiver <- receivers) {
      receiverPreferredLocations(receiver.streamId) = receiver.preferredLocation
      receiverTrackingInfos.put(receiver.streamId, ReceiverTrackingInfo(
        receiver.streamId,
        ReceiverState.INACTIVE,
        None,
        None))
    }
  }

  private def getReceiverTrackingInfoMap(): Map[Int, ReceiverTrackingInfo] =
    receiverTrackingInfosLock.synchronized {
      // Copy to an immutable Map so that we don't need to use `synchronized` when using it
      receiverTrackingInfos.toMap
    }

  private def updateReceiverScheduledLocations(
      receiverId: Int, scheduledLocations: Seq[String]): Unit =
    receiverTrackingInfosLock.synchronized {
      receiverTrackingInfos.put(receiverId, ReceiverTrackingInfo(
        receiverId,
        ReceiverState.SCHEDULED,
        Some(scheduledLocations),
        None))
    }

  private def updateReceiverRunningLocation(receiverId: Int, runningLocation: String): Unit =
    receiverTrackingInfosLock.synchronized {
      receiverTrackingInfos.put(receiverId, ReceiverTrackingInfo(
        receiverId,
        ReceiverState.ACTIVE,
        None,
        Some(runningLocation)))
    }

  private def scheduleReceiver(receiverId: Int): Seq[String] = {
    val preferredLocation = receiverTrackingInfosLock.synchronized {
      receiverPreferredLocations(receiverId)
    }
    val scheduledLocations = schedulingPolicy.scheduleReceiver(
      receiverId, preferredLocation, getReceiverTrackingInfoMap(), getExecutors(ssc))
    updateReceiverScheduledLocations(receiverId, scheduledLocations)
    scheduledLocations
  }

  /**
   * Get the list of executors excluding driver
   */
  private def getExecutors(ssc: StreamingContext): List[String] = {
    val executors = ssc.sparkContext.getExecutorMemoryStatus.map(_._1.split(":")(0)).toList
    val driver = ssc.sparkContext.getConf.get("spark.driver.host")
    executors.diff(List(driver))
  }
}
