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

import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.collection.mutable.HashMap
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

import org.apache.spark._
import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.internal.LogKeys.{ERROR, MESSAGE, RECEIVER_ID, RECEIVER_IDS, STREAM_ID}
import org.apache.spark.rdd.RDD
import org.apache.spark.rpc._
import org.apache.spark.scheduler.{ExecutorCacheTaskLocation, TaskLocation}
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.receiver._
import org.apache.spark.streaming.util.WriteAheadLogUtils
import org.apache.spark.util.{SerializableConfiguration, ThreadUtils, Utils}
import org.apache.spark.util.ArrayImplicits._


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
    executorId: String,
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
 * This message will trigger ReceiverTrackerEndpoint to restart a Spark job for the receiver.
 */
private[streaming] case class RestartReceiver(receiver: Receiver[_])
  extends ReceiverTrackerLocalMessage

/**
 * This message is sent to ReceiverTrackerEndpoint when we start to launch Spark jobs for receivers
 * at the first time.
 */
private[streaming] case class StartAllReceivers(receiver: Seq[Receiver[_]])
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

private[streaming] case class UpdateReceiverRateLimit(streamUID: Int, newRate: Long)
  extends ReceiverTrackerLocalMessage

private[streaming] case object GetAllReceiverInfo extends ReceiverTrackerLocalMessage

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
  private val receivedBlockTracker = new ReceivedBlockTracker(
    ssc.sparkContext.conf,
    ssc.sparkContext.hadoopConfiguration,
    receiverInputStreamIds.toImmutableArraySeq,
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

  private val schedulingPolicy = new ReceiverSchedulingPolicy()

  // Track the active receiver job number. When a receiver job exits ultimately, countDown will
  // be called.
  private val receiverJobExitLatch = new CountDownLatch(receiverInputStreams.length)

  /**
   * Track all receivers' information. The key is the receiver id, the value is the receiver info.
   * It's only accessed in ReceiverTrackerEndpoint.
   */
  private val receiverTrackingInfos = new HashMap[Int, ReceiverTrackingInfo]

  /**
   * Store all preferred locations for all receivers. We need this information to schedule
   * receivers. It's only accessed in ReceiverTrackerEndpoint.
   */
  private val receiverPreferredLocations = new HashMap[Int, Option[String]]

  /** Start the endpoint and receiver execution thread. */
  def start(): Unit = synchronized {
    if (isTrackerStarted) {
      throw new SparkException("ReceiverTracker already started")
    }

    if (!receiverInputStreams.isEmpty) {
      endpoint = ssc.env.rpcEnv.setupEndpoint(
        "ReceiverTracker", new ReceiverTrackerEndpoint(ssc.env.rpcEnv))
      if (!skipReceiverLaunch) launchReceivers()
      logInfo("ReceiverTracker started")
      trackerState = Started
    }
  }

  /** Stop the receiver execution thread. */
  def stop(graceful: Boolean): Unit = synchronized {
    val isStarted: Boolean = isTrackerStarted
    trackerState = Stopping
    if (isStarted) {
      if (!skipReceiverLaunch) {
        // First, stop the receivers. Send the stop signal to all the receivers
        endpoint.askSync[Boolean](StopAllReceivers)

        // Wait for the Spark job that runs the receivers to be over
        // That is, for the receivers to quit gracefully.
        receiverJobExitLatch.await(10, TimeUnit.SECONDS)

        if (graceful) {
          logInfo("Waiting for receiver job to terminate gracefully")
          receiverJobExitLatch.await()
          logInfo("Waited for receiver job to terminate gracefully")
        }

        // Check if all the receivers have been deregistered or not
        val receivers = endpoint.askSync[Seq[Int]](AllReceiverIds)
        if (receivers.nonEmpty) {
          logWarning(log"Not all of the receivers have deregistered, " +
            log"${MDC(RECEIVER_IDS, receivers)}")
        } else {
          logInfo("All of the receivers have deregistered successfully")
        }
      }

      // Finally, stop the endpoint
      ssc.env.rpcEnv.stop(endpoint)
      endpoint = null
    }

    // `ReceivedBlockTracker` is open when this instance is created. We should
    // close this even if this `ReceiverTracker` is not started.
    receivedBlockTracker.stop()
    logInfo("ReceiverTracker stopped")
    trackerState = Stopped
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
    receivedBlockTracker.getBlocksOfBatchAndStream(batchTime, streamId)
  }

  /**
   * Clean up the data and metadata of blocks and batches that are strictly
   * older than the threshold time. Note that this does not
   */
  def cleanupOldBlocksAndBatches(cleanupThreshTime: Time): Unit = {
    // Clean up old block and batch metadata
    receivedBlockTracker.cleanupOldBatches(cleanupThreshTime, waitForCompletion = false)

    // Signal the receivers to delete old block data
    if (WriteAheadLogUtils.enableReceiverLog(ssc.conf)) {
      logInfo(log"Cleanup old received batch data: " +
        log"${MDC(LogKeys.CLEANUP_LOCAL_DIRS, cleanupThreshTime)}")
      synchronized {
        if (isTrackerStarted) {
          endpoint.send(CleanupOldBlocks(cleanupThreshTime))
        }
      }
    }
  }

  /**
   * Get the executors allocated to each receiver.
   * @return a map containing receiver ids to optional executor ids.
   */
  def allocatedExecutors(): Map[Int, Option[String]] = synchronized {
    if (isTrackerStarted) {
      endpoint.askSync[Map[Int, ReceiverTrackingInfo]](GetAllReceiverInfo).transform { (_, v) =>
        v.runningExecutor.map {
          _.executorId
        }
      }
    } else {
      Map.empty
    }
  }

  def numReceivers(): Int = receiverInputStreams.length

  /** Register a receiver */
  private def registerReceiver(
      streamId: Int,
      typ: String,
      host: String,
      executorId: String,
      receiverEndpoint: RpcEndpointRef,
      senderAddress: RpcAddress
    ): Boolean = {
    if (!receiverInputStreamIds.contains(streamId)) {
      throw new SparkException("Register received for unexpected id " + streamId)
    }

    if (isTrackerStopping || isTrackerStopped) {
      return false
    }

    val scheduledLocations = receiverTrackingInfos(streamId).scheduledLocations
    val acceptableExecutors = if (scheduledLocations.nonEmpty) {
        // This receiver is registering and it's scheduled by
        // ReceiverSchedulingPolicy.scheduleReceivers. So use "scheduledLocations" to check it.
        scheduledLocations.get
      } else {
        // This receiver is scheduled by "ReceiverSchedulingPolicy.rescheduleReceiver", so calling
        // "ReceiverSchedulingPolicy.rescheduleReceiver" again to check it.
        scheduleReceiver(streamId)
      }

    def isAcceptable: Boolean = acceptableExecutors.exists {
      case loc: ExecutorCacheTaskLocation => loc.executorId == executorId
      case loc: TaskLocation => loc.host == host
    }

    if (!isAcceptable) {
      // Refuse it since it's scheduled to a wrong executor
      false
    } else {
      val name = s"${typ}-${streamId}"
      val receiverTrackingInfo = ReceiverTrackingInfo(
        streamId,
        ReceiverState.ACTIVE,
        scheduledLocations = None,
        runningExecutor = Some(ExecutorCacheTaskLocation(host, executorId)),
        name = Some(name),
        endpoint = Some(receiverEndpoint))
      receiverTrackingInfos.put(streamId, receiverTrackingInfo)
      listenerBus.post(StreamingListenerReceiverStarted(receiverTrackingInfo.toReceiverInfo))
      logInfo(log"Registered receiver for stream ${MDC(LogKeys.STREAM_ID, streamId)} " +
        log"from ${MDC(LogKeys.RPC_ADDRESS, senderAddress)}")
      true
    }
  }

  /** Deregister a receiver */
  private def deregisterReceiver(streamId: Int, message: String, error: String): Unit = {
    val lastErrorTime =
      if (error == null || error == "") -1 else ssc.scheduler.clock.getTimeMillis()
    val errorInfo = ReceiverErrorInfo(
      lastErrorMessage = message, lastError = error, lastErrorTime = lastErrorTime)
    val newReceiverTrackingInfo = receiverTrackingInfos.get(streamId) match {
      case Some(oldInfo) =>
        oldInfo.copy(state = ReceiverState.INACTIVE, errorInfo = Some(errorInfo))
      case None =>
        logWarning("No prior receiver info")
        ReceiverTrackingInfo(
          streamId, ReceiverState.INACTIVE, None, None, None, None, Some(errorInfo))
    }
    receiverTrackingInfos(streamId) = newReceiverTrackingInfo
    listenerBus.post(StreamingListenerReceiverStopped(newReceiverTrackingInfo.toReceiverInfo))
    val messageWithError = if (error != null && !error.isEmpty) {
      s"$message - $error"
    } else {
      s"$message"
    }
    logError(log"Deregistered receiver for stream ${MDC(STREAM_ID, streamId)}: " +
      log"${MDC(ERROR, messageWithError)}")
  }

  /** Update a receiver's maximum ingestion rate */
  def sendRateUpdate(streamUID: Int, newRate: Long): Unit = synchronized {
    if (isTrackerStarted) {
      endpoint.send(UpdateReceiverRateLimit(streamUID, newRate))
    }
  }

  /** Add new blocks for the given stream */
  private def addBlock(receivedBlockInfo: ReceivedBlockInfo): Boolean = {
    receivedBlockTracker.addBlock(receivedBlockInfo)
  }

  /** Report error sent by a receiver */
  private def reportError(streamId: Int, message: String, error: String): Unit = {
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
      log"${MDC(MESSAGE, message)} - ${MDC(ERROR, error)}"
    } else {
      log"${MDC(MESSAGE, message)}"
    }
    logWarning(log"Error reported by receiver for stream ${MDC(STREAM_ID, streamId)}: " +
      messageWithError)
  }

  private def scheduleReceiver(receiverId: Int): Seq[TaskLocation] = {
    val preferredLocation = receiverPreferredLocations.getOrElse(receiverId, None)
    val scheduledLocations = schedulingPolicy.rescheduleReceiver(
      receiverId, preferredLocation, receiverTrackingInfos, getExecutors)
    updateReceiverScheduledExecutors(receiverId, scheduledLocations)
    scheduledLocations
  }

  private def updateReceiverScheduledExecutors(
      receiverId: Int, scheduledLocations: Seq[TaskLocation]): Unit = {
    val newReceiverTrackingInfo = receiverTrackingInfos.get(receiverId) match {
      case Some(oldInfo) =>
        oldInfo.copy(state = ReceiverState.SCHEDULED,
          scheduledLocations = Some(scheduledLocations))
      case None =>
        ReceiverTrackingInfo(
          receiverId,
          ReceiverState.SCHEDULED,
          Some(scheduledLocations),
          runningExecutor = None)
    }
    receiverTrackingInfos.put(receiverId, newReceiverTrackingInfo)
  }

  /** Check if any blocks are left to be processed */
  def hasUnallocatedBlocks: Boolean = {
    receivedBlockTracker.hasUnallocatedReceivedBlocks
  }

  /**
   * Get the list of executors excluding driver
   */
  private def getExecutors: Seq[ExecutorCacheTaskLocation] = {
    if (ssc.sc.isLocal) {
      val blockManagerId = ssc.sparkContext.env.blockManager.blockManagerId
      Seq(ExecutorCacheTaskLocation(blockManagerId.host, blockManagerId.executorId))
    } else {
      ssc.sparkContext.env.blockManager.master.getMemoryStatus.filter { case (blockManagerId, _) =>
        blockManagerId.executorId != SparkContext.DRIVER_IDENTIFIER // Ignore the driver location
      }.map { case (blockManagerId, _) =>
        ExecutorCacheTaskLocation(blockManagerId.host, blockManagerId.executorId)
      }.toSeq
    }
  }

  /**
   * Run the dummy Spark job to ensure that all executors have registered. This avoids all the
   * receivers to be scheduled on the same node.
   *
   * TODO Should poll the executor number and wait for executors according to
   * "spark.scheduler.minRegisteredResourcesRatio" and
   * "spark.scheduler.maxRegisteredResourcesWaitingTime" rather than running a dummy job.
   */
  private def runDummySparkJob(): Unit = {
    if (!ssc.sparkContext.isLocal) {
      ssc.sparkContext.makeRDD(1 to 50, 50).map(x => (x, 1)).reduceByKey(_ + _, 20).collect()
    }
    assert(getExecutors.nonEmpty)
  }

  /**
   * Get the receivers from the ReceiverInputDStreams, distributes them to the
   * worker nodes as a parallel collection, and runs them.
   */
  private def launchReceivers(): Unit = {
    val receivers = receiverInputStreams.map { nis =>
      val rcvr = nis.getReceiver()
      rcvr.setReceiverId(nis.id)
      rcvr
    }

    runDummySparkJob()

    logInfo(log"Starting ${MDC(LogKeys.NUM_RECEIVERS, receivers.length)} receivers")
    endpoint.send(StartAllReceivers(receivers.toImmutableArraySeq))
  }

  /** Check if tracker has been marked for starting */
  private def isTrackerStarted: Boolean = trackerState == Started

  /** Check if tracker has been marked for stopping */
  private def isTrackerStopping: Boolean = trackerState == Stopping

  /** Check if tracker has been marked for stopped */
  private def isTrackerStopped: Boolean = trackerState == Stopped

  /** RpcEndpoint to receive messages from the receivers. */
  private class ReceiverTrackerEndpoint(override val rpcEnv: RpcEnv) extends ThreadSafeRpcEndpoint {

    private val walBatchingThreadPool = ExecutionContext.fromExecutorService(
      ThreadUtils.newDaemonCachedThreadPool("wal-batching-thread-pool"))

    @volatile private var active: Boolean = true

    override def receive: PartialFunction[Any, Unit] = {
      // Local messages
      case StartAllReceivers(receivers) =>
        val scheduledLocations = schedulingPolicy.scheduleReceivers(receivers, getExecutors)
        for (receiver <- receivers) {
          val executors = scheduledLocations(receiver.streamId)
          updateReceiverScheduledExecutors(receiver.streamId, executors)
          receiverPreferredLocations(receiver.streamId) = receiver.preferredLocation
          startReceiver(receiver, executors)
        }
      case RestartReceiver(receiver) =>
        // Old scheduled executors minus the ones that are not active any more
        val oldScheduledExecutors = getStoredScheduledExecutors(receiver.streamId)
        val scheduledLocations = if (oldScheduledExecutors.nonEmpty) {
            // Try global scheduling again
            oldScheduledExecutors
          } else {
            val oldReceiverInfo = receiverTrackingInfos(receiver.streamId)
            // Clear "scheduledLocations" to indicate we are going to do local scheduling
            val newReceiverInfo = oldReceiverInfo.copy(
              state = ReceiverState.INACTIVE, scheduledLocations = None)
            receiverTrackingInfos(receiver.streamId) = newReceiverInfo
            schedulingPolicy.rescheduleReceiver(
              receiver.streamId,
              receiver.preferredLocation,
              receiverTrackingInfos,
              getExecutors)
          }
        // Assume there is one receiver restarting at one time, so we don't need to update
        // receiverTrackingInfos
        startReceiver(receiver, scheduledLocations)
      case c: CleanupOldBlocks =>
        receiverTrackingInfos.values.flatMap(_.endpoint).foreach(_.send(c))
      case UpdateReceiverRateLimit(streamUID, newRate) =>
        for (info <- receiverTrackingInfos.get(streamUID); eP <- info.endpoint) {
          eP.send(UpdateRateLimit(newRate))
        }
      // Remote messages
      case ReportError(streamId, message, error) =>
        reportError(streamId, message, error)
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      // Remote messages
      case RegisterReceiver(streamId, typ, host, executorId, receiverEndpoint) =>
        val successful =
          registerReceiver(streamId, typ, host, executorId, receiverEndpoint, context.senderAddress)
        context.reply(successful)
      case AddBlock(receivedBlockInfo) =>
        if (WriteAheadLogUtils.isBatchingEnabled(ssc.conf, isDriver = true)) {
          walBatchingThreadPool.execute(() => Utils.tryLogNonFatalError {
            if (active) {
              context.reply(addBlock(receivedBlockInfo))
            } else {
              context.sendFailure(
                new IllegalStateException("ReceiverTracker RpcEndpoint already shut down."))
            }
          })
        } else {
          context.reply(addBlock(receivedBlockInfo))
        }
      case DeregisterReceiver(streamId, message, error) =>
        deregisterReceiver(streamId, message, error)
        context.reply(true)

      // Local messages
      case AllReceiverIds =>
        context.reply(receiverTrackingInfos.filter(_._2.state != ReceiverState.INACTIVE).keys.toSeq)
      case GetAllReceiverInfo =>
        context.reply(receiverTrackingInfos.toMap)
      case StopAllReceivers =>
        assert(isTrackerStopping || isTrackerStopped)
        stopReceivers()
        context.reply(true)
    }

    /**
     * Return the stored scheduled executors that are still alive.
     */
    private def getStoredScheduledExecutors(receiverId: Int): Seq[TaskLocation] = {
      if (receiverTrackingInfos.contains(receiverId)) {
        val scheduledLocations = receiverTrackingInfos(receiverId).scheduledLocations
        if (scheduledLocations.nonEmpty) {
          val executors = getExecutors.toSet
          // Only return the alive executors
          scheduledLocations.get.filter {
            case loc: ExecutorCacheTaskLocation => executors(loc)
            case loc: TaskLocation => true
          }
        } else {
          Nil
        }
      } else {
        Nil
      }
    }

    /**
     * Start a receiver along with its scheduled executors
     */
    private def startReceiver(
        receiver: Receiver[_],
        scheduledLocations: Seq[TaskLocation]): Unit = {
      def shouldStartReceiver: Boolean = {
        // It's okay to start when trackerState is Initialized or Started
        !(isTrackerStopping || isTrackerStopped)
      }

      val receiverId = receiver.streamId
      if (!shouldStartReceiver) {
        onReceiverJobFinish(receiverId)
        return
      }

      val checkpointDirOption = Option(ssc.checkpointDir)
      val serializableHadoopConf =
        new SerializableConfiguration(ssc.sparkContext.hadoopConfiguration)

      // Function to start the receiver on the worker node
      val startReceiverFunc: Iterator[Receiver[_]] => Unit =
        (iterator: Iterator[Receiver[_]]) => {
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

      // Create the RDD using the scheduledLocations to run the receiver in a Spark job
      val receiverRDD: RDD[Receiver[_]] =
        if (scheduledLocations.isEmpty) {
          ssc.sc.makeRDD(Seq(receiver), 1)
        } else {
          val preferredLocations = scheduledLocations.map(_.toString).distinct
          ssc.sc.makeRDD(Seq(receiver -> preferredLocations))
        }
      receiverRDD.setName(s"Receiver $receiverId")
      ssc.sparkContext.setJobDescription(s"Streaming job running receiver $receiverId")
      ssc.sparkContext.setCallSite(Option(ssc.getStartSite()).getOrElse(Utils.getCallSite()))

      val future = ssc.sparkContext.submitJob[Receiver[_], Unit, Unit](
        receiverRDD, startReceiverFunc, Seq(0), (_, _) => (), ())
      // We will keep restarting the receiver job until ReceiverTracker is stopped
      future.onComplete {
        case Success(_) =>
          if (!shouldStartReceiver) {
            onReceiverJobFinish(receiverId)
          } else {
            logInfo(log"Restarting Receiver ${MDC(LogKeys.STREAM_ID, receiverId)}")
            self.send(RestartReceiver(receiver))
          }
        case Failure(e) =>
          if (!shouldStartReceiver) {
            onReceiverJobFinish(receiverId)
          } else {
            logError("Receiver has been stopped. Try to restart it.", e)
            logInfo(log"Restarting Receiver ${MDC(LogKeys.STREAM_ID, receiverId)}")
            self.send(RestartReceiver(receiver))
          }
      }(ThreadUtils.sameThread)
      logInfo(log"Receiver ${MDC(LogKeys.STREAM_ID, receiver.streamId)} started")
    }

    override def onStop(): Unit = {
      active = false
      walBatchingThreadPool.shutdown()
    }

    /**
     * Call when a receiver is terminated. It means we won't restart its Spark job.
     */
    private def onReceiverJobFinish(receiverId: Int): Unit = {
      receiverJobExitLatch.countDown()
      receiverTrackingInfos.remove(receiverId).foreach { receiverTrackingInfo =>
        if (receiverTrackingInfo.state == ReceiverState.ACTIVE) {
          logWarning(log"Receiver ${MDC(RECEIVER_ID, receiverId)} exited but didn't deregister")
        }
      }
    }

    /** Send stop signal to the receivers. */
    private def stopReceivers(): Unit = {
      receiverTrackingInfos.values.flatMap(_.endpoint).foreach { _.send(StopReceiver) }
      logInfo(log"Sent stop signal to all " +
        log"${MDC(LogKeys.NUM_RECEIVERS, receiverTrackingInfos.size)} receivers")
    }
  }

}
