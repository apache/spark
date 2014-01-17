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

import java.util.concurrent.ArrayBlockingQueue
import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.reflect.ClassTag

import akka.actor.{Props, Actor}
import akka.pattern.ask

import org.apache.spark.streaming.util.{RecurringTimer, SystemClock}
import org.apache.spark.streaming._
import org.apache.spark.{Logging, SparkEnv}
import org.apache.spark.rdd.{RDD, BlockRDD}
import org.apache.spark.storage.{BlockId, StorageLevel, StreamBlockId}
import org.apache.spark.streaming.scheduler.{DeregisterReceiver, AddBlocks, RegisterReceiver}

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

  // This is an unique identifier that is used to match the network receiver with the
  // corresponding network input stream.
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

  override def compute(validTime: Time): Option[RDD[T]] = {
    // If this is called for any time before the start time of the context,
    // then this returns an empty RDD. This may happen when recovering from a
    // master failure
    if (validTime >= graph.startTime) {
      val blockIds = ssc.scheduler.networkInputTracker.getBlockIds(id, validTime)
      Some(new BlockRDD[T](ssc.sc, blockIds))
    } else {
      Some(new BlockRDD[T](ssc.sc, Array[BlockId]()))
    }
  }
}


private[streaming] sealed trait NetworkReceiverMessage
private[streaming] case class StopReceiver(msg: String) extends NetworkReceiverMessage
private[streaming] case class ReportBlock(blockId: BlockId, metadata: Any) extends NetworkReceiverMessage
private[streaming] case class ReportError(msg: String) extends NetworkReceiverMessage

/**
 * Abstract class of a receiver that can be run on worker nodes to receive external data. See
 * [[org.apache.spark.streaming.dstream.NetworkInputDStream]] for an explanation.
 */
abstract class NetworkReceiver[T: ClassTag]() extends Serializable with Logging {

  lazy protected val env = SparkEnv.get

  lazy protected val actor = env.actorSystem.actorOf(
    Props(new NetworkReceiverActor()), "NetworkReceiver-" + streamId)

  lazy protected val receivingThread = Thread.currentThread()

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
   * Starts the receiver. First is accesses all the lazy members to
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
      onStart()
    } catch {
      case ie: InterruptedException =>
        logInfo("Receiving thread interrupted")
        //println("Receiving thread interrupted")
      case e: Exception =>
        stopOnError(e)
    }
  }

  /**
   * Stops the receiver. First it interrupts the main receiving thread,
   * that is, the thread that called receiver.start(). Then it calls the user-defined
   * onStop() method to stop other threads and/or do cleanup.
   */
  def stop() {
    receivingThread.interrupt()
    onStop()
    //TODO: terminate the actor
  }

  /**
   * Stops the receiver and reports exception to the tracker.
   * This should be called whenever an exception is to be handled on any thread
   * of the receiver.
   */
  protected def stopOnError(e: Exception) {
    logError("Error receiving data", e)
    stop()
    actor ! ReportError(e.toString)
  }


  /**
   * Pushes a block (as an ArrayBuffer filled with data) into the block manager.
   */
  def pushBlock(blockId: BlockId, arrayBuffer: ArrayBuffer[T], metadata: Any, level: StorageLevel) {
    env.blockManager.put(blockId, arrayBuffer.asInstanceOf[ArrayBuffer[Any]], level)
    actor ! ReportBlock(blockId, metadata)
  }

  /**
   * Pushes a block (as bytes) into the block manager.
   */
  def pushBlock(blockId: BlockId, bytes: ByteBuffer, metadata: Any, level: StorageLevel) {
    env.blockManager.putBytes(blockId, bytes, level)
    actor ! ReportBlock(blockId, metadata)
  }

  /** A helper actor that communicates with the NetworkInputTracker */
  private class NetworkReceiverActor extends Actor {
    logInfo("Attempting to register with tracker")
    val ip = env.conf.get("spark.driver.host", "localhost")
    val port = env.conf.getInt("spark.driver.port", 7077)
    val url = "akka.tcp://spark@%s:%s/user/NetworkInputTracker".format(ip, port)
    val tracker = env.actorSystem.actorSelection(url)
    val timeout = 5.seconds

    override def preStart() {
      val future = tracker.ask(RegisterReceiver(streamId, self))(timeout)
      Await.result(future, timeout)
    }

    override def receive() = {
      case ReportBlock(blockId, metadata) =>
        tracker ! AddBlocks(streamId, Array(blockId), metadata)
      case ReportError(msg) =>
        tracker ! DeregisterReceiver(streamId, msg)
      case StopReceiver(msg) =>
        stop()
        tracker ! DeregisterReceiver(streamId, msg)
    }
  }

  protected[streaming] def setStreamId(id: Int) {
    streamId = id
  }

  /**
   * Batches objects created by a [[org.apache.spark.streaming.dstream.NetworkReceiver]] and puts them into
   * appropriately named blocks at regular intervals. This class starts two threads,
   * one to periodically start a new batch and prepare the previous batch of as a block,
   * the other to push the blocks into the block manager.
   */
  class BlockGenerator(storageLevel: StorageLevel)
    extends Serializable with Logging {

    case class Block(id: BlockId, buffer: ArrayBuffer[T], metadata: Any = null)

    val clock = new SystemClock()
    val blockInterval = env.conf.getLong("spark.streaming.blockInterval", 200)
    val blockIntervalTimer = new RecurringTimer(clock, blockInterval, updateCurrentBuffer)
    val blockStorageLevel = storageLevel
    val blocksForPushing = new ArrayBlockingQueue[Block](1000)
    val blockPushingThread = new Thread() { override def run() { keepPushingBlocks() } }

    var currentBuffer = new ArrayBuffer[T]

    def start() {
      blockIntervalTimer.start()
      blockPushingThread.start()
      logInfo("Data handler started")
    }

    def stop() {
      blockIntervalTimer.stop()
      blockPushingThread.interrupt()
      logInfo("Data handler stopped")
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
          logInfo("Block interval timer thread interrupted")
        case e: Exception =>
          NetworkReceiver.this.stop()
      }
    }

    private def keepPushingBlocks() {
      logInfo("Block pushing thread started")
      try {
        while(true) {
          val block = blocksForPushing.take()
          NetworkReceiver.this.pushBlock(block.id, block.buffer, block.metadata, storageLevel)
        }
      } catch {
        case ie: InterruptedException =>
          logInfo("Block pushing thread interrupted")
        case e: Exception =>
          NetworkReceiver.this.stop()
      }
    }
  }
}
