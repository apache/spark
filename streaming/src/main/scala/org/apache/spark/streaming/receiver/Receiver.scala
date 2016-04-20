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
import scala.collection.JavaConverters._

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.storage.StorageLevel

/**
 * :: DeveloperApi ::
 * Abstract class of a receiver that can be run on worker nodes to receive external data. A
 * custom receiver can be defined by defining the functions `onStart()` and `onStop()`. `onStart()`
 * should define the setup steps necessary to start receiving data,
 * and `onStop()` should define the cleanup steps necessary to stop receiving data.
 * Exceptions while receiving can be handled either by restarting the receiver with `restart(...)`
 * or stopped completely by `stop(...)` or
 *
 * A custom receiver in Scala would look like this.
 *
 * {{{
 *  class MyReceiver(storageLevel: StorageLevel) extends NetworkReceiver[String](storageLevel) {
 *      def onStart() {
 *          // Setup stuff (start threads, open sockets, etc.) to start receiving data.
 *          // Must start new thread to receive data, as onStart() must be non-blocking.
 *
 *          // Call store(...) in those threads to store received data into Spark's memory.
 *
 *          // Call stop(...), restart(...) or reportError(...) on any thread based on how
 *          // different errors needs to be handled.
 *
 *          // See corresponding method documentation for more details
 *      }
 *
 *      def onStop() {
 *          // Cleanup stuff (stop threads, close sockets, etc.) to stop receiving data.
 *      }
 *  }
 * }}}
 *
 * A custom receiver in Java would look like this.
 *
 * {{{
 * class MyReceiver extends Receiver<String> {
 *     public MyReceiver(StorageLevel storageLevel) {
 *         super(storageLevel);
 *     }
 *
 *     public void onStart() {
 *          // Setup stuff (start threads, open sockets, etc.) to start receiving data.
 *          // Must start new thread to receive data, as onStart() must be non-blocking.
 *
 *          // Call store(...) in those threads to store received data into Spark's memory.
 *
 *          // Call stop(...), restart(...) or reportError(...) on any thread based on how
 *          // different errors needs to be handled.
 *
 *          // See corresponding method documentation for more details
 *     }
 *
 *     public void onStop() {
 *          // Cleanup stuff (stop threads, close sockets, etc.) to stop receiving data.
 *     }
 * }
 * }}}
 */
@DeveloperApi
abstract class Receiver[T](val storageLevel: StorageLevel) extends Serializable {

  /**
   * This method is called by the system when the receiver is started. This function
   * must initialize all resources (threads, buffers, etc.) necessary for receiving data.
   * This function must be non-blocking, so receiving the data must occur on a different
   * thread. Received data can be stored with Spark by calling `store(data)`.
   *
   * If there are errors in threads started here, then following options can be done
   * (i) `reportError(...)` can be called to report the error to the driver.
   * The receiving of data will continue uninterrupted.
   * (ii) `stop(...)` can be called to stop receiving data. This will call `onStop()` to
   * clear up all resources allocated (threads, buffers, etc.) during `onStart()`.
   * (iii) `restart(...)` can be called to restart the receiver. This will call `onStop()`
   * immediately, and then `onStart()` after a delay.
   */
  def onStart(): Unit

  /**
   * This method is called by the system when the receiver is stopped. All resources
   * (threads, buffers, etc.) set up in `onStart()` must be cleaned up in this method.
   */
  def onStop(): Unit

  /** Override this to specify a preferred location (hostname). */
  def preferredLocation: Option[String] = None

  /**
   * Store a single item of received data to Spark's memory.
   * These single items will be aggregated together into data blocks before
   * being pushed into Spark's memory.
   */
  def store(dataItem: T) {
    supervisor.pushSingle(dataItem)
  }

  /** Store an ArrayBuffer of received data as a data block into Spark's memory. */
  def store(dataBuffer: ArrayBuffer[T]) {
    supervisor.pushArrayBuffer(dataBuffer, None, None)
  }

  /**
   * Store an ArrayBuffer of received data as a data block into Spark's memory.
   * The metadata will be associated with this block of data
   * for being used in the corresponding InputDStream.
   */
  def store(dataBuffer: ArrayBuffer[T], metadata: Any) {
    supervisor.pushArrayBuffer(dataBuffer, Some(metadata), None)
  }

  /** Store an iterator of received data as a data block into Spark's memory. */
  def store(dataIterator: Iterator[T]) {
    supervisor.pushIterator(dataIterator, None, None)
  }

  /**
   * Store an iterator of received data as a data block into Spark's memory.
   * The metadata will be associated with this block of data
   * for being used in the corresponding InputDStream.
   */
  def store(dataIterator: java.util.Iterator[T], metadata: Any) {
    supervisor.pushIterator(dataIterator.asScala, Some(metadata), None)
  }

  /** Store an iterator of received data as a data block into Spark's memory. */
  def store(dataIterator: java.util.Iterator[T]) {
    supervisor.pushIterator(dataIterator.asScala, None, None)
  }

  /**
   * Store an iterator of received data as a data block into Spark's memory.
   * The metadata will be associated with this block of data
   * for being used in the corresponding InputDStream.
   */
  def store(dataIterator: Iterator[T], metadata: Any) {
    supervisor.pushIterator(dataIterator, Some(metadata), None)
  }

  /**
   * Store the bytes of received data as a data block into Spark's memory. Note
   * that the data in the ByteBuffer must be serialized using the same serializer
   * that Spark is configured to use.
   */
  def store(bytes: ByteBuffer) {
    supervisor.pushBytes(bytes, None, None)
  }

  /**
   * Store the bytes of received data as a data block into Spark's memory.
   * The metadata will be associated with this block of data
   * for being used in the corresponding InputDStream.
   */
  def store(bytes: ByteBuffer, metadata: Any) {
    supervisor.pushBytes(bytes, Some(metadata), None)
  }

  /** Report exceptions in receiving data. */
  def reportError(message: String, throwable: Throwable) {
    supervisor.reportError(message, throwable)
  }

  /**
   * Restart the receiver. This method schedules the restart and returns
   * immediately. The stopping and subsequent starting of the receiver
   * (by calling `onStop()` and `onStart()`) is performed asynchronously
   * in a background thread. The delay between the stopping and the starting
   * is defined by the Spark configuration `spark.streaming.receiverRestartDelay`.
   * The `message` will be reported to the driver.
   */
  def restart(message: String) {
    supervisor.restartReceiver(message)
  }

  /**
   * Restart the receiver. This method schedules the restart and returns
   * immediately. The stopping and subsequent starting of the receiver
   * (by calling `onStop()` and `onStart()`) is performed asynchronously
   * in a background thread. The delay between the stopping and the starting
   * is defined by the Spark configuration `spark.streaming.receiverRestartDelay`.
   * The `message` and `exception` will be reported to the driver.
   */
  def restart(message: String, error: Throwable) {
    supervisor.restartReceiver(message, Some(error))
  }

  /**
   * Restart the receiver. This method schedules the restart and returns
   * immediately. The stopping and subsequent starting of the receiver
   * (by calling `onStop()` and `onStart()`) is performed asynchronously
   * in a background thread.
   */
  def restart(message: String, error: Throwable, millisecond: Int) {
    supervisor.restartReceiver(message, Some(error), millisecond)
  }

  /** Stop the receiver completely. */
  def stop(message: String) {
    supervisor.stop(message, None)
  }

  /** Stop the receiver completely due to an exception */
  def stop(message: String, error: Throwable) {
    supervisor.stop(message, Some(error))
  }

  /** Check if the receiver has started or not. */
  def isStarted(): Boolean = {
    supervisor.isReceiverStarted()
  }

  /**
   * Check if receiver has been marked for stopping. Use this to identify when
   * the receiving of data should be stopped.
   */
  def isStopped(): Boolean = {
    supervisor.isReceiverStopped()
  }

  /**
   * Get the unique identifier the receiver input stream that this
   * receiver is associated with.
   */
  def streamId: Int = id

  /*
   * =================
   * Private methods
   * =================
   */

  /** Identifier of the stream this receiver is associated with. */
  private var id: Int = -1

  /** Handler object that runs the receiver. This is instantiated lazily in the worker. */
  @transient private var _supervisor: ReceiverSupervisor = null

  /** Set the ID of the DStream that this receiver is associated with. */
  private[streaming] def setReceiverId(_id: Int) {
    id = _id
  }

  /** Attach Network Receiver executor to this receiver. */
  private[streaming] def attachSupervisor(exec: ReceiverSupervisor) {
    assert(_supervisor == null)
    _supervisor = exec
  }

  /** Get the attached supervisor. */
  private[streaming] def supervisor: ReceiverSupervisor = {
    assert(_supervisor != null,
      "A ReceiverSupervisor has not been attached to the receiver yet. Maybe you are starting " +
        "some computation in the receiver before the Receiver.onStart() has been called.")
    _supervisor
  }
}

