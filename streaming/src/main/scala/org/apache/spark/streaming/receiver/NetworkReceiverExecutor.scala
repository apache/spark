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

package org.apache.spark.streaming.receiver

import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.storage.StreamBlockId

/**
 * Abstract class that is responsible for executing a NetworkReceiver in the worker.
 * It provides all the necessary interfaces for handling the data received by the receiver.
 */
private[streaming] abstract class NetworkReceiverExecutor(
    receiver: NetworkReceiver[_],
    conf: SparkConf = new SparkConf()
  ) extends Logging {

  receiver.attachExecutor(this)

  /** Receiver id */
  protected val receiverId = receiver.receiverId

  /** Thread that starts the receiver and stays blocked while data is being received. */
  @volatile protected var receivingThread: Option[Thread] = None

  /** Has the receiver been marked for stop. */
  @volatile private var stopped = false

  /** Push a single data item to backend data store. */
  def pushSingle(data: Any)

  /** Push a byte buffer to backend data store. */
  def pushBytes(
      bytes: ByteBuffer,
      optionalMetadata: Option[Any],
      optionalBlockId: Option[StreamBlockId]
    )

  /** Push an iterator of objects as a block to backend data store. */
  def pushIterator(
      iterator: Iterator[_],
      optionalMetadata: Option[Any],
      optionalBlockId: Option[StreamBlockId]
    )

  /** Push an ArrayBuffer of object as a block to back data store. */
  def pushArrayBuffer(
      arrayBuffer: ArrayBuffer[_],
      optionalMetadata: Option[Any],
      optionalBlockId: Option[StreamBlockId]
    )

  /** Report errors. */
  def reportError(message: String, throwable: Throwable)

  /**
   * Run the receiver. The thread that calls this is supposed to stay blocked
   * in this function until the stop() is called or there is an exception
   */
  def run() {
    // Remember this thread as the receiving thread
    receivingThread = Some(Thread.currentThread())

    try {
      // Call user-defined onStart()
      logInfo("Calling onStart")
      receiver.onStart()

      // Wait until interrupt is called on this thread
      while(true) {
        Thread.sleep(100)
      }
    } catch {
      case ie: InterruptedException =>
        logInfo("Receiving thread has been interrupted, receiver "  + receiverId + " stopped")
      case t: Throwable =>
        reportError("Error receiving data in receiver " + receiverId, t)
    }

    // Call user-defined onStop()
    try {
      logInfo("Calling onStop")
      receiver.onStop()
    } catch {
      case  t: Throwable =>
        reportError("Error stopping receiver " + receiverId, t)
    }
  }

  /**
   * Stop receiving data.
   */
  def stop() {
    // Mark has stopped

    if (receivingThread.isDefined) {
      // Interrupt the thread
      receivingThread.get.interrupt()

      // Wait for the receiving thread to finish on its own
      receivingThread.get.join(conf.getLong("spark.streaming.receiverStopTimeout", 2000))

      // Stop receiving by interrupting the receiving thread
      receivingThread.get.interrupt()
      logInfo("Interrupted receiving thread of receiver " + receiverId + " for stopping")
    }

    stopped = true
    logInfo("Marked as stop")
  }

  /** Check if receiver has been marked for stopping. */
  def isStopped = stopped
}
