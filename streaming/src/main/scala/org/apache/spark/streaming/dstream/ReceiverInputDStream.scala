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

import scala.reflect.ClassTag

import org.apache.spark.rdd.{BlockRDD, RDD}
import org.apache.spark.storage.{BlockId, StorageLevel}
import org.apache.spark.streaming._
import org.apache.spark.streaming.rdd.WriteAheadLogBackedBlockRDD
import org.apache.spark.streaming.receiver.{Receiver, WriteAheadLogBasedStoreResult}
import org.apache.spark.streaming.scheduler.ReceivedBlockInfo

/**
 * Abstract class for defining any [[org.apache.spark.streaming.dstream.InputDStream]]
 * that has to start a receiver on worker nodes to receive external data.
 * Specific implementations of ReceiverInputDStream must
 * define `the getReceiver()` function that gets the receiver object of type
 * [[org.apache.spark.streaming.receiver.Receiver]] that will be sent
 * to the workers to receive data.
 * @param ssc_ Streaming context that will execute this input stream
 * @tparam T Class type of the object of this stream
 */
abstract class ReceiverInputDStream[T: ClassTag](@transient ssc_ : StreamingContext)
  extends InputDStream[T](ssc_) {

  /** This is an unique identifier for the receiver input stream. */
  val id = ssc.getNewReceiverStreamId()

  /**
   * Gets the receiver object that will be sent to the worker nodes
   * to receive data. This method needs to defined by any specific implementation
   * of a ReceiverInputDStream.
   */
  def getReceiver(): Receiver[T]

  // Nothing to start or stop as both taken care of by the ReceiverTracker.
  def start() {}

  def stop() {}

  /**
   * Generates RDDs with blocks received by the receiver of this stream. */
  override def compute(validTime: Time): Option[RDD[T]] = {
    val blockRDD = {

      if (validTime < graph.startTime) {
        // If this is called for any time before the start time of the context,
        // then this returns an empty RDD. This may happen when recovering from a
        // driver failure without any write ahead log to recover pre-failure data.
        new BlockRDD[T](ssc.sc, Array.empty)
      } else {
        // Otherwise, ask the tracker for all the blocks that have been allocated to this stream
        // for this batch
        val blockInfos =
          ssc.scheduler.receiverTracker.getBlocksOfBatch(validTime).get(id).getOrElse(Seq.empty)
        val blockStoreResults = blockInfos.map { _.blockStoreResult }
        val blockIds = blockStoreResults.map { _.blockId.asInstanceOf[BlockId] }.toArray

        // Check whether all the results are of the same type
        val resultTypes = blockStoreResults.map { _.getClass }.distinct
        if (resultTypes.size > 1) {
          logWarning("Multiple result types in block information, WAL information will be ignored.")
        }

        // If all the results are of type WriteAheadLogBasedStoreResult, then create
        // WriteAheadLogBackedBlockRDD else create simple BlockRDD.
        if (resultTypes.size == 1 && resultTypes.head == classOf[WriteAheadLogBasedStoreResult]) {
          val logSegments = blockStoreResults.map {
            _.asInstanceOf[WriteAheadLogBasedStoreResult].segment
          }.toArray
          // Since storeInBlockManager = false, the storage level does not matter.
          new WriteAheadLogBackedBlockRDD[T](ssc.sparkContext,
            blockIds, logSegments, storeInBlockManager = false, StorageLevel.MEMORY_ONLY_SER)
        } else {
          new BlockRDD[T](ssc.sc, blockIds)
        }
      }
    }
    Some(blockRDD)
  }
}
