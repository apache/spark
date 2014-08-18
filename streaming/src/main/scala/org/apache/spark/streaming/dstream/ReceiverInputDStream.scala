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

import scala.collection.mutable.HashMap
import scala.reflect.ClassTag

import org.apache.spark.rdd.{BlockRDD, RDD}
import org.apache.spark.storage.BlockId
import org.apache.spark.streaming._
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.scheduler.ReceivedBlockInfo

/**
 * Abstract class for defining any [[org.apache.spark.streaming.dstream.InputDStream]]
 * that has to start a receiver on worker nodes to receive external data.
 * Specific implementations of NetworkInputDStream must
 * define `the getReceiver()` function that gets the receiver object of type
 * [[org.apache.spark.streaming.receiver.Receiver]] that will be sent
 * to the workers to receive data.
 * @param ssc_ Streaming context that will execute this input stream
 * @tparam T Class type of the object of this stream
 */
abstract class ReceiverInputDStream[T: ClassTag](@transient ssc_ : StreamingContext)
  extends InputDStream[T](ssc_) {

  /** Keeps all received blocks information */
  private lazy val receivedBlockInfo = new HashMap[Time, Array[ReceivedBlockInfo]]

  /** This is an unique identifier for the network input stream. */
  val id = ssc.getNewReceiverStreamId()

  /**
   * Gets the receiver object that will be sent to the worker nodes
   * to receive data. This method needs to defined by any specific implementation
   * of a NetworkInputDStream.
   */
  def getReceiver(): Receiver[T]

  // Nothing to start or stop as both taken care of by the ReceiverInputTracker.
  def start() {}

  def stop() {}

  /** Ask ReceiverInputTracker for received data blocks and generates RDDs with them. */
  override def compute(validTime: Time): Option[RDD[T]] = {
    // If this is called for any time before the start time of the context,
    // then this returns an empty RDD. This may happen when recovering from a
    // master failure
    if (validTime >= graph.startTime) {
      val blockInfo = ssc.scheduler.receiverTracker.getReceivedBlockInfo(id)
      receivedBlockInfo(validTime) = blockInfo
      val blockIds = blockInfo.map(_.blockId.asInstanceOf[BlockId])
      Some(new BlockRDD[T](ssc.sc, blockIds))
    } else {
      Some(new BlockRDD[T](ssc.sc, Array[BlockId]()))
    }
  }

  /** Get information on received blocks. */
  private[streaming] def getReceivedBlockInfo(time: Time) = {
    receivedBlockInfo.get(time).getOrElse(Array.empty[ReceivedBlockInfo])
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

