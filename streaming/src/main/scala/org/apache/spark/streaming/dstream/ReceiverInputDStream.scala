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
import org.apache.spark.storage.BlockId
import org.apache.spark.streaming._
import org.apache.spark.streaming.rdd.WriteAheadLogBackedBlockRDD
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.scheduler.InputInfo
import org.apache.spark.streaming.util.WriteAheadLogUtils

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
        val receiverTracker = ssc.scheduler.receiverTracker
        val blockInfos = receiverTracker.getBlocksOfBatch(validTime).getOrElse(id, Seq.empty)
        val blockIds = blockInfos.map { _.blockId.asInstanceOf[BlockId] }.toArray

        // Register the input blocks information into InputInfoTracker
        val inputInfo = InputInfo(id, blockInfos.flatMap(_.numRecords).sum)
        ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)

        if (blockInfos.nonEmpty) {
          // Are WAL record handles present with all the blocks
          val areWALRecordHandlesPresent = blockInfos.forall { _.walRecordHandleOption.nonEmpty }

          if (areWALRecordHandlesPresent) {
            // If all the blocks have WAL record handle, then create a WALBackedBlockRDD
            val isBlockIdValid = blockInfos.map { _.isBlockIdValid() }.toArray
            val walRecordHandles = blockInfos.map { _.walRecordHandleOption.get }.toArray
            new WriteAheadLogBackedBlockRDD[T](
              ssc.sparkContext, blockIds, walRecordHandles, isBlockIdValid)
          } else {
            // Else, create a BlockRDD. However, if there are some blocks with WAL info but not
            // others then that is unexpected and log a warning accordingly.
            if (blockInfos.find(_.walRecordHandleOption.nonEmpty).nonEmpty) {
              if (WriteAheadLogUtils.enableReceiverLog(ssc.conf)) {
                logError("Some blocks do not have Write Ahead Log information; " +
                  "this is unexpected and data may not be recoverable after driver failures")
              } else {
                logWarning("Some blocks have Write Ahead Log information; this is unexpected")
              }
            }
            new BlockRDD[T](ssc.sc, blockIds)
          }
        } else {
          // If no block is ready now, creating WriteAheadLogBackedBlockRDD or BlockRDD
          // according to the configuration
          if (WriteAheadLogUtils.enableReceiverLog(ssc.conf)) {
            new WriteAheadLogBackedBlockRDD[T](
              ssc.sparkContext, Array.empty, Array.empty, Array.empty)
          } else {
            new BlockRDD[T](ssc.sc, Array.empty)
          }
        }
      }
    }
    Some(blockRDD)
  }
}
