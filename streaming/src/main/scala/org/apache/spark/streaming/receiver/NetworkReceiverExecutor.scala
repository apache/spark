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
import java.util.concurrent.CountDownLatch
import scala.concurrent._
import ExecutionContext.Implicits.global

/**
 * Abstract class that is responsible for executing a NetworkReceiver in the worker.
 * It provides all the necessary interfaces for handling the data received by the receiver.
 */
private[streaming] abstract class NetworkReceiverExecutor(
    receiver: NetworkReceiver[_],
    conf: SparkConf
  ) extends Logging {


  /** Enumeration to identify current state of the StreamingContext */
  object NetworkReceiverState extends Enumeration {
    type CheckpointState = Value
    val Initialized, Started, Stopped = Value
  }
  import NetworkReceiverState._

  // Attach the executor to the receiver
  receiver.attachExecutor(this)

  /** Receiver id */
  protected val receiverId = receiver.receiverId

  /** Message associated with the stopping of the receiver */
  protected var stopMessage = ""

  /** Exception associated with the stopping of the receiver */
  protected var stopException: Throwable = null

  /** Has the receiver been marked for stop. */
  private val stopLatch = new CountDownLatch(1)

  /** Time between a receiver is stopped */
  private val restartDelay = conf.getInt("spark.streaming.receiverRestartDelay", 2000)

  /** State of the receiver */
  private[streaming] var receiverState = Initialized

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

  /** Start the executor */
  def start() {
    startReceiver()
  }

  /**
   * Mark the executor and the receiver for stopping
   */
  def stop(message: String, exception: Throwable = null) {
    stopMessage = message
    stopException = exception
    stopReceiver()
    stopLatch.countDown()
    if (exception != null) {
      logError("Stopped executor: " + message, exception)
    } else {
      logWarning("Stopped executor: " + message)
    }
  }

  /** Start receiver */
  def startReceiver(): Unit = synchronized {
    try {
      logInfo("Starting receiver")
      stopMessage = ""
      stopException = null
      onReceiverStart()
      receiverState = Started
    } catch {
      case t: Throwable =>
        stop("Error starting receiver " + receiverId, t)
    }
  }

  /** Stop receiver */
  def stopReceiver(): Unit = synchronized {
    try {
      receiverState = Stopped
      onReceiverStop()
    } catch {
      case t: Throwable =>
        stop("Error stopping receiver " + receiverId, t)
    }
  }

  /** Restart receiver with delay */
  def restartReceiver(message: String, throwable: Throwable = null) {
    val defaultRestartDelay = conf.getInt("spark.streaming.receiverRestartDelay", 2000)
    restartReceiver(message, throwable, defaultRestartDelay)
  }

  /** Restart receiver with delay */
  def restartReceiver(message: String, exception: Throwable, delay: Int) {
    logWarning("Restarting receiver with delay " + delay + " ms: " + message, exception)
    reportError(message, exception)
    stopReceiver()
    future {
      logDebug("Sleeping for " + delay)
      Thread.sleep(delay)
      logDebug("Starting receiver again")
      startReceiver()
      logInfo("Receiver started again")
    }
  }

  /** Called when the receiver needs to be started */
  protected def onReceiverStart(): Unit = synchronized {
    // Call user-defined onStart()
    logInfo("Calling receiver onStart")
    receiver.onStart()
    logInfo("Called receiver onStart")
  }

  /** Called when the receiver needs to be stopped */
  protected def onReceiverStop(): Unit = synchronized {
    // Call user-defined onStop()
    logInfo("Calling receiver onStop")
    receiver.onStop()
    logInfo("Called receiver onStop")
  }

  /** Check if receiver has been marked for stopping */
  def isReceiverStarted() = {
    logDebug("state = " + receiverState)
    receiverState == Started
  }

  /** Wait the thread until the executor is stopped */
  def awaitStop() {
    stopLatch.await()
    logInfo("Waiting for executor stop is over")
    if (stopException != null) {
      throw new Exception(stopMessage, stopException)
    }
  }
}
