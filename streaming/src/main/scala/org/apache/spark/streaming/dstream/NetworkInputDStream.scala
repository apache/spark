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

import scala.collection.mutable.{ArrayBuffer, SynchronizedBuffer}
import scala.concurrent.Await
import scala.reflect.ClassTag

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{TimeUnit, ArrayBlockingQueue}

import akka.actor.{Props, Actor}
import akka.pattern.ask

import org.apache.spark.streaming.util.{RecurringTimer, SystemClock}
import org.apache.spark.streaming._
import org.apache.spark.{Logging, SparkEnv}
import org.apache.spark.rdd.{RDD, BlockRDD}
import org.apache.spark.storage.{BlockId, StorageLevel, StreamBlockId}
import org.apache.spark.streaming.scheduler.{DeregisterReceiver, AddBlocks, RegisterReceiver}
import org.apache.spark.util.AkkaUtils

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
private[streaming] case class ReportBlock(blockId: BlockId, metadata: Any)
  extends NetworkReceiverMessage
private[streaming] case class ReportError(msg: String) extends NetworkReceiverMessage

/**
 * Abstract class of a receiver that can be run on worker nodes to receive external data. A
 * custom receiver can be defined by defining the functions onStart() and onStop(). onStart()
 * should define the setup steps necessary to start receiving data,
 * and onStop() should define the cleanup steps necessary to stop receiving data. A custom
 * receiver would look something like this.
 *
 * class MyReceiver(storageLevel) extends NetworkReceiver[String](storageLevel) {
 *   def onStart() {
 *     // Setup stuff (start threads, open sockets, etc.) to start receiving data.
 *     // Call store(...) to store received data into Spark's memory.
 *     // Optionally, wait for other threads to complete or watch for exceptions.
 *     // Call stopOnError(...) if there is an error that you cannot ignore and need
 *     // the receiver to be terminated.
 *   }
 *
 *   def onStop() {
 *     // Cleanup stuff (stop threads, close sockets, etc.) to stop receiving data.
 *   }
 * }
 */
abstract class NetworkReceiver[T: ClassTag](val storageLevel: StorageLevel)
  extends Serializable {

  /**
   * This method is called by the system when the receiver is started to start receiving data.
   * All threads and resources set up in this method must be cleaned up in onStop().
   * If there are exceptions on other threads such that the receiver must be terminated,
   * then you must call stopOnError(exception). However, the thread that called onStart() must
   * never catch and ignore InterruptedException (it can catch and rethrow).
   */
  def onStart()

  /**
   * This method is called by the system when the receiver is stopped to stop receiving data.
   * All threads and resources setup in onStart() must be cleaned up in this method.
   */
  def onStop()

  /** Override this to specify a preferred location (hostname). */
  def preferredLocation : Option[String] = None

  /** Store a single item of received data to Spark's memory/ */
  def store(dataItem: T) {
    handler.pushSingle(dataItem)
  }

  /** Store a sequence of received data block into Spark's memory. */
  def store(dataBuffer: ArrayBuffer[T]) {
    handler.pushArrayBuffer(dataBuffer)
  }

  /** Store a sequence of received data block into Spark's memory. */
  def store(dataIterator: Iterator[T]) {
    handler.pushIterator(dataIterator)
  }

  /** Store the bytes of received data block into Spark's memory. */
  def store(bytes: ByteBuffer) {
    handler.pushBytes(bytes)
  }

  /** Stop the receiver. */
  def stop() {
    handler.stop()
  }

  /** Stop the receiver when an error occurred. */
  def stopOnError(e: Exception) {
    handler.stop(e)
  }

  /** Check if receiver has been marked for stopping */
  def isStopped: Boolean = {
    handler.isStopped
  }

  /** Get unique identifier of this receiver. */
  def receiverId = id

  /** Identifier of the stream this receiver is associated with. */
  private var id: Int = -1

  /** Handler object that runs the receiver. This is instantiated lazily in the worker. */
  private[streaming] lazy val handler = new NetworkReceiverHandler(this)

  /** Set the ID of the DStream that this receiver is associated with */
  private[streaming] def setReceiverId(id_ : Int) {
    id = id_
  }
}


private[streaming] class NetworkReceiverHandler(receiver: NetworkReceiver[_]) extends Logging {

  val env = SparkEnv.get
  val receiverId = receiver.receiverId
  val storageLevel = receiver.storageLevel

  /** Remote Akka actor for the NetworkInputTracker */
  private val trackerActor = {
    val ip = env.conf.get("spark.driver.host", "localhost")
    val port = env.conf.getInt("spark.driver.port", 7077)
    val url = "akka.tcp://spark@%s:%s/user/NetworkInputTracker".format(ip, port)
    env.actorSystem.actorSelection(url)
  }

  /** Timeout for Akka actor messages */
  private val askTimeout = AkkaUtils.askTimeout(env.conf)

  /** Akka actor for receiving messages from the NetworkInputTracker in the driver */
  private val actor = env.actorSystem.actorOf(
    Props(new Actor {
      override def preStart() {
        logInfo("Registered receiver " + receiverId)
        val future = trackerActor.ask(RegisterReceiver(receiverId, self))(askTimeout)
        Await.result(future, askTimeout)
      }

      override def receive() = {
        case StopReceiver =>
          logInfo("Received stop signal")
          stop()
      }
    }), "NetworkReceiver-" + receiverId)

  /** Divides received data records into data blocks for pushing in BlockManager */
  private val blockGenerator = new BlockGenerator(this)

  /** Exceptions that occurs while receiving data */
  private val exceptions = new ArrayBuffer[Exception] with SynchronizedBuffer[Exception]

  /** Unique block ids if one wants to add blocks directly */
  private val newBlockId = new AtomicLong(System.currentTimeMillis())

  /** Thread that starts the receiver and stays blocked while data is being received */
  private var receivingThread: Option[Thread] = None

  /** Has the receiver been marked for stop */
  private var stopped = false

  /**
   * Starts the receiver. First is accesses all the lazy members to
   * materialize them. Then it calls the user-defined onStart() method to start
   * other threads, etc. required to receive the data.
   */
  def run() {
    // Remember this thread as the receiving thread
    receivingThread = Some(Thread.currentThread())

    // Starting the block generator
    blockGenerator.start()

    try {
      // Call user-defined onStart()
      logInfo("Calling onStart")
      receiver.onStart()

      // Wait until interrupt is called on this thread
      while(true) Thread.sleep(100000)
    } catch {
      case ie: InterruptedException =>
        logInfo("Receiving thread has been interrupted, receiver "  + receiverId + " stopped")
      case e: Exception =>
        logError("Error receiving data in receiver " + receiverId, e)
        exceptions += e
    }

    // Call user-defined onStop()
    try {
      logInfo("Calling onStop")
      receiver.onStop()
    } catch {
      case  e: Exception =>
        logError("Error stopping receiver " + receiverId, e)
        exceptions += e
    }

    // Stopping BlockGenerator
    blockGenerator.stop()

    val message = if (exceptions.isEmpty) {
      null
    } else if (exceptions.size == 1) {
      val e = exceptions.head
      "Exception in receiver " + receiverId + ": " + e.getMessage + "\n" + e.getStackTraceString
    } else {
      "Multiple exceptions in receiver " + receiverId + "(" + exceptions.size + "):\n"
      exceptions.zipWithIndex.map {
        case (e, i) => "Exception " + i + ": " + e.getMessage + "\n" + e.getStackTraceString
      }.mkString("\n")
    }
    logInfo("Deregistering receiver " + receiverId)
    val future = trackerActor.ask(DeregisterReceiver(receiverId, message))(askTimeout)
    Await.result(future, askTimeout)
    logInfo("Deregistered receiver " + receiverId)
    env.actorSystem.stop(actor)
    logInfo("Stopped receiver " + receiverId)
  }


  /** Push a single record of received data into block generator. */
  def pushSingle(data: Any) {
    blockGenerator += data
  }

  /** Push a block of received data into block manager. */
  def pushArrayBuffer(
      arrayBuffer: ArrayBuffer[_],
      blockId: StreamBlockId = nextBlockId,
      metadata: Any = null
    ) {
    logDebug("Pushing block " + blockId)
    val time = System.currentTimeMillis
    env.blockManager.put(blockId, arrayBuffer.asInstanceOf[ArrayBuffer[Any]], storageLevel, true)
    logDebug("Pushed block " + blockId + " in " + (System.currentTimeMillis - time)  + " ms")
    trackerActor ! AddBlocks(receiverId, Array(blockId), null)
    logDebug("Reported block " + blockId)
  }

  /**
   * Push a received data into Spark as . Call this method from the data receiving
   * thread to submit
   * a block of data.
   */
  def pushIterator(
      iterator: Iterator[_],
      blockId: StreamBlockId = nextBlockId,
      metadata: Any = null
    ) {
    env.blockManager.put(blockId, iterator, storageLevel, true)
    trackerActor ! AddBlocks(receiverId, Array(blockId), null)
    logInfo("Pushed block " + blockId)
  }


  /**
   * Push a block (as bytes) into the block manager.
   */
  def pushBytes(
      bytes: ByteBuffer,
      blockId: StreamBlockId = nextBlockId,
      metadata: Any = null
    ) {
    env.blockManager.putBytes(blockId, bytes, storageLevel, true)
    trackerActor ! AddBlocks(receiverId, Array(blockId), null)
    logInfo("Pushed block " + blockId)
  }

  /**
   * Stop receiving data.
   */
  def stop(e: Exception = null) {
    // Mark has stopped
    stopped = true
    logInfo("Marked as stop")

    // Store the exception if any
    if (e != null) {
      logError("Error receiving data", e)
      exceptions += e
    }

    if (receivingThread.isDefined) {
      // Wait for the receiving thread to finish on its own
      receivingThread.get.join(env.conf.getLong("spark.streaming.receiverStopTimeout", 2000))

      // Stop receiving by interrupting the receiving thread
      receivingThread.get.interrupt()
      logInfo("Interrupted receiving thread of receiver " + receiverId + " for stopping")
    }
  }

  /** Check if receiver has been marked for stopping. */
  def isStopped = stopped

  private def nextBlockId = StreamBlockId(receiverId, newBlockId.getAndIncrement)
}

/**
 * Batches objects created by a [[org.apache.spark.streaming.dstream.NetworkReceiver]] and puts them into
 * appropriately named blocks at regular intervals. This class starts two threads,
 * one to periodically start a new batch and prepare the previous batch of as a block,
 * the other to push the blocks into the block manager.
 */
private[streaming] class BlockGenerator(handler: NetworkReceiverHandler) extends Logging {

  private case class Block(id: StreamBlockId, buffer: ArrayBuffer[Any], metadata: Any = null)

  private val env = handler.env
  private val blockInterval = env.conf.getLong("spark.streaming.blockInterval", 200)
  private val blockIntervalTimer =
    new RecurringTimer(new SystemClock(), blockInterval, updateCurrentBuffer)
  private val blocksForPushing = new ArrayBlockingQueue[Block](10)
  private val blockPushingThread = new Thread() { override def run() { keepPushingBlocks() } }

  private var currentBuffer = new ArrayBuffer[Any]
  private var stopped = false

  def start() {
    blockIntervalTimer.start()
    blockPushingThread.start()
    logInfo("Started BlockGenerator")
  }

  def stop() {
    // Stop generating blocks
    blockIntervalTimer.stop()

    // Mark as stopped
    synchronized { stopped = true }

    // Wait for all blocks to be pushed
    logDebug("Waiting for block pushing thread to terminate")
    blockPushingThread.join()
    logInfo("Stopped BlockGenerator")
  }

  def += (obj: Any): Unit = synchronized {
    currentBuffer += obj
  }

  private def isStopped = synchronized { stopped }

  private def updateCurrentBuffer(time: Long): Unit = synchronized {
    try {
      val newBlockBuffer = currentBuffer
      currentBuffer = new ArrayBuffer[Any]
      if (newBlockBuffer.size > 0) {
        val blockId = StreamBlockId(handler.receiverId, time - blockInterval)
        val newBlock = new Block(blockId, newBlockBuffer)
        blocksForPushing.add(newBlock)
        logDebug("Last element in " + blockId + " is " + newBlockBuffer.last)
      }
    } catch {
      case ie: InterruptedException =>
        logInfo("Block updating timer thread was interrupted")
      case e: Exception =>
        handler.stop(e)
    }
  }

  private def keepPushingBlocks() {
    logInfo("Started block pushing thread")

    def pushNextBlock() {
      Option(blocksForPushing.poll(100, TimeUnit.MILLISECONDS)) match {
        case Some(block) =>
          handler.pushArrayBuffer(block.buffer, block.id, block.metadata)
          logInfo("Pushed block "+ block.id)
        case None =>
      }
    }

    try {
      while(!isStopped) {
        Option(blocksForPushing.poll(100, TimeUnit.MILLISECONDS)) match {
          case Some(block) =>
            handler.pushArrayBuffer(block.buffer, block.id, block.metadata)
            logInfo("Pushed block "+ block.id)
          case None =>
        }
      }
      // Push out the blocks that are still left
      logInfo("Pushing out the last " + blocksForPushing.size() + " blocks")
      while (!blocksForPushing.isEmpty) {
        logDebug("Getting block ")
        val block = blocksForPushing.take()
        logDebug("Got block")
        handler.pushArrayBuffer(block.buffer, block.id, block.metadata)
        logInfo("Blocks left to push " + blocksForPushing.size())
      }
      logInfo("Stopped block pushing thread")
    } catch {
      case ie: InterruptedException =>
        logInfo("Block pushing thread was interrupted")
      case e: Exception =>
        handler.stop(e)
    }
  }
}
