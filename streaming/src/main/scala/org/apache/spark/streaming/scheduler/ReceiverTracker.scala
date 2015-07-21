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

import scala.collection.mutable.HashMap
import scala.concurrent.ExecutionContext
import scala.language.existentials
import scala.util.{Failure, Success}

import org.apache.spark.streaming.util.WriteAheadLogUtils
import org.apache.spark.{TaskContext, Logging, SparkEnv, SparkException}
import org.apache.spark.rdd.RDD
import org.apache.spark.rpc._
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.receiver._
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

/**
 * Messages used by the driver and ReceiverTrackerEndpoint to communicate locally.
 */
private[streaming] sealed trait ReceiverTrackerLocalMessage

/**
 * This message will trigger ReceiverTrackerEndpoint to start a Spark job for the receiver.
 */
private[streaming] case class StartReceiver(receiver: Receiver[_])
  extends ReceiverTrackerLocalMessage

/**
 * This message will trigger ReceiverTrackerEndpoint to send stop signals to all registered
 * receivers.
 */
private[streaming] case object StopAllReceivers extends ReceiverTrackerLocalMessage

/**
 * A message used by ReceiverTracker to ask all receiver's ids still stored in
 * ReceiverTrackerEndpoint.
 */
private[streaming] case object AllReceiverIds extends ReceiverTrackerLocalMessage

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
    type TrackerState = Value
    val Initialized, Started, Stopping, Stopped = Value
  }
  import TrackerState._

  /** State of the tracker. Protected by "trackerStateLock" */
  @volatile private var trackerState = Initialized

  // endpoint is created when generator starts.
  // This not being null means the tracker has been started and not stopped
  private var endpoint: RpcEndpointRef = null

  private val schedulingPolicy: ReceiverSchedulingPolicy = new ReceiverSchedulingPolicy()

  // Track the active receiver job number. When a receiver job exits ultimately, countDown will
  // be called.
  private val receiverJobExitLatch = new CountDownLatch(receiverInputStreams.size)

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
      trackerState = Started
    }
  }

  /** Stop the receiver execution thread. */
  def stop(graceful: Boolean): Unit = synchronized {
    if (isTrackerStarted) {
      // First, stop the receivers
      trackerState = Stopping
      if (!skipReceiverLaunch) {
        // Send the stop signal to all the receivers
        endpoint.askWithRetry[Boolean](StopAllReceivers)

        // Wait for the Spark job that runs the receivers to be over
        // That is, for the receivers to quit gracefully.
        receiverExecutor.awaitTermination(10000)

        if (graceful) {
          val pollTime = 100
          logInfo("Waiting for receiver job to terminate gracefully")
          while (receiverExecutor.running) {
            Thread.sleep(pollTime)
          }
          logInfo("Waited for receiver job to terminate gracefully")
        }

        // Check if all the receivers have been deregistered or not
        val receivers = endpoint.askWithRetry[Seq[Int]](AllReceiverIds)
        if (receivers.nonEmpty) {
          logWarning("Not all of the receivers have deregistered, " + receivers)
        } else {
          logInfo("All of the receivers have deregistered successfully")
        }
      }

      // Finally, stop the endpoint
      ssc.env.rpcEnv.stop(endpoint)
      endpoint = null
      receivedBlockTracker.stop()
      logInfo("ReceiverTracker stopped")
      trackerState = Stopped
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
      endpoint.send(CleanupOldBlocks(cleanupThreshTime))
    }
  }

  /** Check if any blocks are left to be processed */
  def hasUnallocatedBlocks: Boolean = {
    receivedBlockTracker.hasUnallocatedReceivedBlocks
  }

  /**
   * Get the list of executors excluding driver
   */
  private def getExecutors(ssc: StreamingContext): List[String] = {
    val executors = ssc.sparkContext.getExecutorMemoryStatus.map(_._1.split(":")(0)).toList
    val driver = ssc.sparkContext.getConf.get("spark.driver.host")
    executors.diff(List(driver))
  }

  /** Check if tracker has been marked for starting */
  private def isTrackerStarted(): Boolean = trackerState == Started

  /** Check if tracker has been marked for stopping */
  private def isTrackerStopping(): Boolean = trackerState == Stopping

  /** Check if tracker has been marked for stopped */
  private def isTrackerStopped(): Boolean = trackerState == Stopped

  /** RpcEndpoint to receive messages from the receivers. */
  private class ReceiverTrackerEndpoint(override val rpcEnv: RpcEnv) extends ThreadSafeRpcEndpoint {

    /**
     * Track all receivers' information. The key is the receiver id, the value is the receiver info.
     */
    private val receiverTrackingInfos = new HashMap[Int, ReceiverTrackingInfo]

    /**
     * Store all preferred locations for all receivers. We need this information to schedule
     * receivers
     */
    private val receiverPreferredLocations = new HashMap[Int, Option[String]]

    // TODO Remove this thread pool after https://github.com/apache/spark/issues/7385 is merged
    private val submitJobThreadPool = ExecutionContext.fromExecutorService(
      ThreadUtils.newDaemonCachedThreadPool("submit-job-thead-pool"))

    override def receive: PartialFunction[Any, Unit] = {
      // Local messages
      case StartReceiver(receiver) =>
        startReceiver(receiver)
      case c @ CleanupOldBlocks(cleanupThreshTime) =>
        receiverTrackingInfos.values.flatMap(_.endpoint).foreach(_.send(c))
      // Remote messages
      case ReportError(streamId, message, error) =>
        reportError(streamId, message, error)
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      // Remote messages
      case RegisterReceiver(streamId, typ, host, receiverEndpoint) =>
        val successful =
          registerReceiver(streamId, typ, host, receiverEndpoint, context.sender.address)
        context.reply(successful)
      case AddBlock(receivedBlockInfo) =>
        context.reply(addBlock(receivedBlockInfo))
      case DeregisterReceiver(streamId, message, error) =>
        deregisterReceiver(streamId, message, error)
        context.reply(true)
      // Local messages
      case AllReceiverIds =>
        context.reply(receiverTrackingInfos.keys.toSeq)
      case StopAllReceivers =>
        assert(isTrackerStopping || isTrackerStopped)
        stopReceivers()
        context.reply(true)
    }

    private def startReceiver(receiver: Receiver[_]): Unit = {
      val checkpointDirOption = Option(ssc.checkpointDir)
      val serializableHadoopConf =
        new SerializableConfiguration(ssc.sparkContext.hadoopConfiguration)

      // Function to start the receiver on the worker node
      val startReceiverFunc = new StartReceiverFunc(checkpointDirOption, serializableHadoopConf)

      receiverPreferredLocations(receiver.streamId) = receiver.preferredLocation
      val receiverId = receiver.streamId

      if (isTrackerStopping() || isTrackerStopped()) {
        onReceiverJobFinish(receiverId)
        return
      }

      val scheduledLocations = schedulingPolicy.scheduleReceiver(
        receiverId,
        receiver.preferredLocation,
        receiverTrackingInfos,
        getExecutors(ssc))
      updateReceiverScheduledLocations(receiver.streamId, scheduledLocations)

      // Create the RDD using the scheduledLocations to run the receiver in a Spark job
      val receiverRDD: RDD[Receiver[_]] =
        if (scheduledLocations.isEmpty) {
          ssc.sc.makeRDD(Seq(receiver), 1)
        } else {
          ssc.sc.makeRDD(Seq(receiver -> scheduledLocations))
        }
      val future = ssc.sparkContext.submitJob[Receiver[_], Unit, Unit](
        receiverRDD, startReceiverFunc, Seq(0), (_, _) => Unit, ())
      // We will keep restarting the receiver job until ReceiverTracker is stopped
      future.onComplete {
        case Success(_) =>
          if (isTrackerStopping() || isTrackerStopped()) {
            onReceiverJobFinish(receiverId)
          } else {
            logInfo(s"Restarting Receiver $receiverId")
            self.send(StartReceiver(receiver))
          }
        case Failure(e) =>
          if (isTrackerStopping() || isTrackerStopped()) {
            onReceiverJobFinish(receiverId)
          } else {
            logError("Receiver has been stopped. Try to restart it.", e)
            logInfo(s"Restarting Receiver $receiverId")
            self.send(StartReceiver(receiver))
          }
      }(submitJobThreadPool)
      logInfo(s"Receiver ${receiver.streamId} started")
    }

    override def onStop(): Unit = {
      submitJobThreadPool.shutdownNow()
    }

    /**
     * Call when a receiver is terminated. It means we won't restart its Spark job.
     */
    private def onReceiverJobFinish(receiverId: Int): Unit = {
      receiverJobExitLatch.countDown()
      receiverTrackingInfos.remove(receiverId).foreach { receiverTrackingInfo =>
        if (receiverTrackingInfo.state == ReceiverState.ACTIVE) {
          logWarning(s"Receiver $receiverId exited but didn't deregister")
        }
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

      if (isTrackerStopping || isTrackerStopped) {
        false
      } else if (!ssc.sparkContext.isLocal && // We don't need to schedule it in the local mode
        !scheduleReceiver(streamId).contains(host)) {
        // Refuse it since it's scheduled to a wrong executor
        false
      } else {
        val name = s"${typ}-${streamId}"
        val receiverInfo = ReceiverInfo(streamId, name, true, host)
        receiverTrackingInfos.put(streamId,
          ReceiverTrackingInfo(
            streamId,
            ReceiverState.ACTIVE,
            scheduledLocations = None,
            runningLocation = Some(host),
            name = Some(name),
            endpoint = Some(receiverEndpoint)))
        listenerBus.post(StreamingListenerReceiverStarted(receiverInfo))
        logInfo("Registered receiver for stream " + streamId + " from " + senderAddress)
        true
      }
    }

    /** Deregister a receiver */
    private def deregisterReceiver(streamId: Int, message: String, error: String) {
      val lastErrorTime =
        if (error == null || error == "") -1 else ssc.scheduler.clock.getTimeMillis()
      val errorInfo = ReceiverErrorInfo(
        lastErrorMessage = message, lastError = error, lastErrorTime = lastErrorTime)
      val newReceiverTrackingInfo = receiverTrackingInfos.get(streamId) match {
        case Some(oldInfo) =>
          oldInfo.copy(errorInfo = Some(errorInfo))
        case None =>
          logWarning("No prior receiver info")
          ReceiverTrackingInfo(
            streamId, ReceiverState.INACTIVE, None, None, None, None, Some(errorInfo))
      }
      receiverTrackingInfos -= streamId
      listenerBus.post(StreamingListenerReceiverStopped(newReceiverTrackingInfo.toReceiverInfo))
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
      val newReceiverTrackingInfo = receiverTrackingInfos.get(streamId) match {
        case Some(oldInfo) =>
          val errorInfo = ReceiverErrorInfo(lastErrorMessage = message, lastError = error,
            lastErrorTime = oldInfo.errorInfo.map(_.lastErrorTime).getOrElse(-1L))
          oldInfo.copy(errorInfo = Some(errorInfo))
        case None =>
          logWarning("No prior receiver info")
          val errorInfo = ReceiverErrorInfo(lastErrorMessage = message, lastError = error,
            lastErrorTime = ssc.scheduler.clock.getTimeMillis())
          ReceiverTrackingInfo(
            streamId, ReceiverState.INACTIVE, None, None, None, None, Some(errorInfo))
      }

      receiverTrackingInfos(streamId) = newReceiverTrackingInfo
      listenerBus.post(StreamingListenerReceiverError(newReceiverTrackingInfo.toReceiverInfo))
      val messageWithError = if (error != null && !error.isEmpty) {
        s"$message - $error"
      } else {
        s"$message"
      }
      logWarning(s"Error reported by receiver for stream $streamId: $messageWithError")
    }

    private def updateReceiverScheduledLocations(
        receiverId: Int, scheduledLocations: Seq[String]): Unit = {
      val newReceiverTrackingInfo = receiverTrackingInfos.get(receiverId) match {
        case Some(oldInfo) =>
          oldInfo.copy(state = ReceiverState.SCHEDULED,
            scheduledLocations = Some(scheduledLocations))
        case None =>
          ReceiverTrackingInfo(
            receiverId,
            ReceiverState.SCHEDULED,
            Some(scheduledLocations),
            None)
      }
      receiverTrackingInfos.put(receiverId, newReceiverTrackingInfo)
    }

    private def scheduleReceiver(receiverId: Int): Seq[String] = {
      val preferredLocation = receiverPreferredLocations.getOrElse(receiverId, None)
      val scheduledLocations = schedulingPolicy.scheduleReceiver(
        receiverId, preferredLocation, receiverTrackingInfos, getExecutors(ssc))
      updateReceiverScheduledLocations(receiverId, scheduledLocations)
      scheduledLocations
    }

    /** Send stop signal to the receivers. */
    private def stopReceivers() {
      // Signal the receivers to stop
      receiverTrackingInfos.values.flatMap(_.endpoint).foreach { _.send(StopReceiver) }
      logInfo("Sent stop signal to all " + receiverTrackingInfos.size + " receivers")
    }
  }

  /** This thread class runs all the receivers on the cluster.  */
  class ReceiverLauncher {
    @transient val env = ssc.env
    @volatile @transient var running = false
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

      // Run the dummy Spark job to ensure that all slaves have registered.
      // This avoids all the receivers to be scheduled on the same node.
      if (!ssc.sparkContext.isLocal) {
        ssc.sparkContext.makeRDD(1 to 50, 50).map(x => (x, 1)).reduceByKey(_ + _, 20).collect()
      }

      // Distribute the receivers and start them
      logInfo("Starting " + receivers.length + " receivers")
      running = true

      try {
        for (receiver <- receivers) {
          endpoint.send(StartReceiver(receiver))
        }
        // Wait until all receivers exit
        receiverJobExitLatch.await()
        logInfo("All of the receivers have been terminated")
      } finally {
        running = false
      }
    }

    /**
     * Wait until the Spark job that runs the receivers is terminated, or return when
     * `milliseconds` elapses
     */
    def awaitTermination(milliseconds: Long): Unit = {
      thread.join(milliseconds)
    }
  }

}

/**
 * Function to start the receiver on the worker node. Use a class instead of closure to avoid
 * the serialization issue.
 */
private class StartReceiverFunc(
    checkpointDirOption: Option[String],
    serializableHadoopConf: SerializableConfiguration)
  extends (Iterator[Receiver[_]] => Unit) with Serializable {

  override def apply(iterator: Iterator[Receiver[_]]): Unit = {
    if (!iterator.hasNext) {
      throw new SparkException(
        "Could not start receiver as object not found.")
    }
    if (TaskContext.get().attemptNumber() == 0) {
      val receiver = iterator.next()
      assert(iterator.hasNext == false)
      val supervisor = new ReceiverSupervisorImpl(
        receiver, SparkEnv.get, serializableHadoopConf.value, checkpointDirOption)
      supervisor.start()
      supervisor.awaitTermination()
    } else {
      // It's restarted by TaskScheduler, but we want to reschedule it again. So exit it.
    }
  }

}
