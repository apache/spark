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

package org.apache.spark.streaming.dstream

import java.nio.ByteBuffer
import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}

import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.concurrent.Await
import scala.reflect.ClassTag

import akka.actor.{Actor, Props}
import akka.pattern.ask

import org.apache.spark.{Logging, SparkEnv}
import org.apache.spark.rdd.{BlockRDD, RDD}
import org.apache.spark.storage.{BlockId, StorageLevel, StreamBlockId}
import org.apache.spark.streaming._
import org.apache.spark.streaming.scheduler.{AddBlocks, DeregisterReceiver, ReceivedBlockInfo, RegisterReceiver}
import org.apache.spark.streaming.util.{RecurringTimer, SystemClock}
import org.apache.spark.util.{AkkaUtils, Utils}

/**
 * Abstract class for defining any [[org.apache.spark.streaming.dstream.InputDStream]]
 * that has to start a receiver on worker nodes to receive external data.
 * Specific implementations of NetworkInputDStream must
 * define the getReceiver() function that gets the receiver object of type
 * [[org.apache.spark.streaming.dstream.NetworkReceiver]] that will be sent
 * to the workers to receive data.
 * @param ssc_ Streaming context that will execute this input stream
 * @tparam T Class type of the object of this stream
 */
abstract class NetworkInputDStream[T: ClassTag](@transient ssc_ : StreamingContext)
  extends InputDStream[T](ssc_) {

  /** Keeps all received blocks information */
  private lazy val receivedBlockInfo = new HashMap[Time, Array[ReceivedBlockInfo]]

  /** This is an unique identifier for the network input stream. */
  val id = ssc.getNewNetworkStreamId()

  /**
   * Gets the receiver object that will be sent to the worker nodes
   * to receive data. This method needs to defined by any specific implementation
   * of a NetworkInputDStream.
   */
  def getReceiver(): NetworkReceiver[T]

  // Nothing to start or stop as both taken care of by the NetworkInputTracker.
  def start() {}

  def stop() {}

  /** Ask NetworkInputTracker for received data blocks and generates RDDs with them. */
  override def compute(validTime: Time): Option[RDD[T]] = {
    // If this is called for any time before the start time of the context,
    // then this returns an empty RDD. This may happen when recovering from a
    // master failure
    if (validTime >= graph.startTime) {
      val blockInfo = ssc.scheduler.networkInputTracker.getReceivedBlockInfo(id)
      receivedBlockInfo(validTime) = blockInfo
      val blockIds = blockInfo.map(_.blockId.asInstanceOf[BlockId])
      Some(new BlockRDD[T](ssc.sc, blockIds))
    } else {
      Some(new BlockRDD[T](ssc.sc, Array[BlockId]()))
    }
  }

  /** Get information on received blocks. */
  private[streaming] def getReceivedBlockInfo(time: Time) = {
    receivedBlockInfo(time)
  }

  /**
   * Clear metadata that are older than `rememberDuration` of this DStream.
   * This is an internal method that should not be called directly. This
   * implementation overrides the default implementation to clear received
   * block information.
   */
  private[streaming] override def clearMetadata(time: Time) {
    super.clearMetadata(time)
    val oldReceivedBlocks = receivedBlockInfo.filter(_._1 <= (time - rememberDuration))
    receivedBlockInfo --= oldReceivedBlocks.keys
    logDebug("Cleared " + oldReceivedBlocks.size + " RDDs that were older than " +
      (time - rememberDuration) + ": " + oldReceivedBlocks.keys.mkString(", "))
  }
}


private[streaming] sealed trait NetworkReceiverMessage
private[streaming] case class StopReceiver(msg: String) extends NetworkReceiverMessage

/**
 * Abstract class of a receiver that can be run on worker nodes to receive external data. See
 * [[org.apache.spark.streaming.dstream.NetworkInputDStream]] for an explanation.
 */
abstract class NetworkReceiver[T: ClassTag]() extends Serializable with Logging {

  /** Local SparkEnv */
  lazy protected val env = SparkEnv.get

  /** Remote Akka actor for the NetworkInputTracker */
  lazy protected val trackerActor = {
    val ip = env.conf.get("spark.driver.host", "localhost")
    val port = env.conf.getInt("spark.driver.port", 7077)
    val url = "akka.tcp://spark@%s:%s/user/NetworkInputTracker".format(ip, port)
    env.actorSystem.actorSelection(url)
  }

  /** Akka actor for receiving messages from the NetworkInputTracker in the driver */
  lazy protected val actor = env.actorSystem.actorOf(
    Props(new NetworkReceiverActor()), "NetworkReceiver-" + streamId)

  /** Timeout for Akka actor messages */
  lazy protected val askTimeout = AkkaUtils.askTimeout(env.conf)

  /** Thread that starts the receiver and stays blocked while data is being received */
  lazy protected val receivingThread = Thread.currentThread()

  /** Exceptions that occurs while receiving data */
  protected lazy val exceptions = new ArrayBuffer[Exception]

  /** Identifier of the stream this receiver is associated with */
  protected var streamId: Int = -1

  /**
   * This method will be called to start receiving data. All your receiver
   * starting code should be implemented by defining this function.
   */
  protected def onStart()

  /** This method will be called to stop receiving data. */
  protected def onStop()

  /** Conveys a placement preference (hostname) for this receiver. */
  def getLocationPreference() : Option[String] = None

  /**
   * Start the receiver. First is accesses all the lazy members to
   * materialize them. Then it calls the user-defined onStart() method to start
   * other threads, etc required to receiver the data.
   */
  def start() {
    try {
      // Access the lazy vals to materialize them
      env
      actor
      receivingThread

      // Call user-defined onStart()
      logInfo("Starting receiver")
      onStart()

      // Wait until interrupt is called on this thread
      while(true) Thread.sleep(100000)
    } catch {
      case ie: InterruptedException =>
        logInfo("Receiving thread has been interrupted, receiver "  + streamId + " stopped")
      case e: Exception =>
        logError("Error receiving data in receiver " + streamId, e)
        exceptions += e
    }

    // Call user-defined onStop()
    logInfo("Stopping receiver")
    try {
      onStop()
    } catch {
      case  e: Exception =>
        logError("Error stopping receiver " + streamId, e)
        exceptions += e
    }

    val message = if (exceptions.isEmpty) {
      null
    } else if (exceptions.size == 1) {
      val e = exceptions.head
      "Exception in receiver " + streamId + ": " + e.getMessage + "\n" + e.getStackTraceString
    } else {
      "Multiple exceptions in receiver " + streamId + "(" + exceptions.size + "):\n"
        exceptions.zipWithIndex.map {
          case (e, i) => "Exception " + i + ": " + e.getMessage + "\n" + e.getStackTraceString
        }.mkString("\n")
    }

    logInfo("Deregistering receiver " + streamId)
    val future = trackerActor.ask(DeregisterReceiver(streamId, message))(askTimeout)
    Await.result(future, askTimeout)
    logInfo("Deregistered receiver " + streamId)
    env.actorSystem.stop(actor)
    logInfo("Stopped receiver " + streamId)
  }

  /**
   * Stop the receiver. First it interrupts the main receiving thread,
   * that is, the thread that called receiver.start().
   */
  def stop() {
    // Stop receiving by interrupting the receiving thread
    receivingThread.interrupt()
    logInfo("Interrupted receiving thread " + receivingThread + " for stopping")
  }

  /**
   * Stop the receiver and reports exception to the tracker.
   * This should be called whenever an exception is to be handled on any thread
   * of the receiver.
   */
  protected def stopOnError(e: Exception) {
    logError("Error receiving data", e)
    exceptions += e
    stop()
  }

  /**
   * Push a block (as an ArrayBuffer filled with data) into the block manager.
   */
  def pushBlock(
      blockId: StreamBlockId,
      arrayBuffer: ArrayBuffer[T],
      metadata: Any,
      level: StorageLevel
    ) {
    env.blockManager.put(blockId, arrayBuffer.asInstanceOf[ArrayBuffer[Any]], level)
    trackerActor ! AddBlocks(ReceivedBlockInfo(streamId, blockId, arrayBuffer.size, metadata))
    logDebug("Pushed block " + blockId)
  }

  /**
   * Push a block (as bytes) into the block manager.
   */
  def pushBlock(
      blockId: StreamBlockId,
      bytes: ByteBuffer,
      metadata: Any,
      level: StorageLevel
    ) {
    env.blockManager.putBytes(blockId, bytes, level)
    trackerActor ! AddBlocks(ReceivedBlockInfo(streamId, blockId, -1, metadata))
  }

  /** Set the ID of the DStream that this receiver is associated with */
  protected[streaming] def setStreamId(id: Int) {
    streamId = id
  }

  /** A helper actor that communicates with the NetworkInputTracker */
  private class NetworkReceiverActor extends Actor {

    override def preStart() {
      val msg = RegisterReceiver(
        streamId, NetworkReceiver.this.getClass.getSimpleName, Utils.localHostName(), self)
      val future = trackerActor.ask(msg)(askTimeout)
      Await.result(future, askTimeout)
      logInfo("Registered receiver " + streamId)
    }

    override def receive() = {
      case StopReceiver =>
        logInfo("Received stop signal")
        stop()
    }
  }

  /**
   * Batches objects created by a [[org.apache.spark.streaming.dstream.NetworkReceiver]] and puts
   * them into appropriately named blocks at regular intervals. This class starts two threads,
   * one to periodically start a new batch and prepare the previous batch of as a block,
   * the other to push the blocks into the block manager.
   */
  class BlockGenerator(storageLevel: StorageLevel)
    extends Serializable with Logging {

    case class Block(id: StreamBlockId, buffer: ArrayBuffer[T], metadata: Any = null)

    val clock = new SystemClock()
    val blockInterval = env.conf.getLong("spark.streaming.blockInterval", 200)
    val blockIntervalTimer = new RecurringTimer(clock, blockInterval, updateCurrentBuffer,
      "BlockGenerator")
    val blockStorageLevel = storageLevel
    val blocksForPushing = new ArrayBlockingQueue[Block](1000)
    val blockPushingThread = new Thread() { override def run() { keepPushingBlocks() } }

    var currentBuffer = new ArrayBuffer[T]
    var stopped = false

    def start() {
      blockIntervalTimer.start()
      blockPushingThread.start()
      logInfo("Started BlockGenerator")
    }

    def stop() {
      blockIntervalTimer.stop(false)
      stopped = true
      blockPushingThread.join()
      logInfo("Stopped BlockGenerator")
    }

    def += (obj: T): Unit = synchronized {
      currentBuffer += obj
    }

    private def updateCurrentBuffer(time: Long): Unit = synchronized {
      try {
        val newBlockBuffer = currentBuffer
        currentBuffer = new ArrayBuffer[T]
        if (newBlockBuffer.size > 0) {
          val blockId = StreamBlockId(NetworkReceiver.this.streamId, time - blockInterval)
          val newBlock = new Block(blockId, newBlockBuffer)
          blocksForPushing.add(newBlock)
        }
      } catch {
        case ie: InterruptedException =>
          logInfo("Block updating timer thread was interrupted")
        case e: Exception =>
          NetworkReceiver.this.stopOnError(e)
      }
    }

    private def keepPushingBlocks() {
      logInfo("Started block pushing thread")
      try {
        while(!stopped) {
          Option(blocksForPushing.poll(100, TimeUnit.MILLISECONDS)) match {
            case Some(block) =>
              NetworkReceiver.this.pushBlock(block.id, block.buffer, block.metadata, storageLevel)
            case None =>
          }
        }
        // Push out the blocks that are still left
        logInfo("Pushing out the last " + blocksForPushing.size() + " blocks")
        while (!blocksForPushing.isEmpty) {
          val block = blocksForPushing.take()
          NetworkReceiver.this.pushBlock(block.id, block.buffer, block.metadata, storageLevel)
          logInfo("Blocks left to push " + blocksForPushing.size())
        }
        logInfo("Stopped blocks pushing thread")
      } catch {
        case ie: InterruptedException =>
          logInfo("Block pushing thread was interrupted")
        case e: Exception =>
          NetworkReceiver.this.stopOnError(e)
      }
    }
  }
}
