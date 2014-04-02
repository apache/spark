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

import org.apache.spark.storage.StorageLevel

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
 *     // Call reportError(...) if there is an error that you cannot ignore and need
 *     // the receiver to be terminated.
 *   }
 *
 *   def onStop() {
 *     // Cleanup stuff (stop threads, close sockets, etc.) to stop receiving data.
 *   }
 * }
 */
abstract class NetworkReceiver[T](val storageLevel: StorageLevel) extends Serializable {

  /**
   * This method is called by the system when the receiver is started to start receiving data.
   * All threads and resources set up in this method must be cleaned up in onStop().
   * If there are exceptions on other threads such that the receiver must be terminated,
   * then you must call reportError(exception). However, the thread that called onStart() must
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

  /** Store a single item of received data to Spark's memory. */
  def store(dataItem: T) {
    executor.pushSingle(dataItem)
  }

  /** Store a sequence of received data into Spark's memory. */
  def store(dataBuffer: ArrayBuffer[T]) {
    executor.pushArrayBuffer(dataBuffer, None, None)
  }

  /**
   * Store a sequence of received data into Spark's memory.
   * The metadata will be associated with this block of data
   * for being used in the corresponding InputDStream.
   */
  def store(dataBuffer: ArrayBuffer[T], metadata: Any) {
    executor.pushArrayBuffer(dataBuffer, Some(metadata), None)
  }
  /** Store a sequence of received data into Spark's memory. */
  def store(dataIterator: Iterator[T]) {
    executor.pushIterator(dataIterator, None, None)
  }

  /**
   * Store a sequence of received data into Spark's memory.
   * The metadata will be associated with this block of data
   * for being used in the corresponding InputDStream.
   */
  def store(dataIterator: Iterator[T], metadata: Any) {
    executor.pushIterator(dataIterator, Some(metadata), None)
  }
  /** Store the bytes of received data into Spark's memory. */
  def store(bytes: ByteBuffer) {
    executor.pushBytes(bytes, None, None)
  }

  /** Store the bytes of received data into Spark's memory.
   * The metadata will be associated with this block of data
   * for being used in the corresponding InputDStream.
   */
  def store(bytes: ByteBuffer, metadata: Any = null) {
    executor.pushBytes(bytes, Some(metadata), None)
  }
  /** Report exceptions in receiving data. */
  def reportError(message: String, throwable: Throwable) {
    executor.reportError(message, throwable)
  }

  /** Stop the receiver. */
  def stop() {
    executor.stop()
  }

  /** Check if receiver has been marked for stopping. */
  def isStopped(): Boolean = {
    executor.isStopped
  }

  /** Get unique identifier of this receiver. */
  def receiverId = id

  /** Identifier of the stream this receiver is associated with. */
  private var id: Int = -1

  /** Handler object that runs the receiver. This is instantiated lazily in the worker. */
  private[streaming] var executor_ : NetworkReceiverExecutor = null

  /** Set the ID of the DStream that this receiver is associated with. */
  private[streaming] def setReceiverId(id_ : Int) {
    id = id_
  }

  /** Attach Network Receiver executor to this receiver. */
  private[streaming] def attachExecutor(exec: NetworkReceiverExecutor) {
    assert(executor_ == null)
    executor_ = exec
  }

  /** Get the attached executor. */
  private def executor = {
    assert(executor_ != null, "Executor has not been attached to this receiver")
    executor_
  }
}

