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

package org.apache.spark.streaming.kinesis

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.{BlockId, StorageLevel}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.scheduler.ReceivedBlockInfo
import org.apache.spark.streaming.{Duration, StreamingContext, Time}

private[kinesis] class KinesisInputDStream(
    @transient _ssc: StreamingContext,
    streamName: String,
    endpointUrl: String,
    regionName: String,
    initialPositionInStream: InitialPositionInStream,
    checkpointAppName: String,
    checkpointInterval: Duration,
    storageLevel: StorageLevel,
    awsCredentialsOption: Option[SerializableAWSCredentials]
  ) extends ReceiverInputDStream[Array[Byte]](_ssc) {

  private[streaming]
  override def createBlockRDD(time: Time, blockInfos: Seq[ReceivedBlockInfo]): RDD[Array[Byte]] = {

    // This returns true even for when blockInfos is empty
    val allBlocksHaveRanges = blockInfos.map { _.metadataOption }.forall(_.nonEmpty)

    if (allBlocksHaveRanges) {
      // Create a KinesisBackedBlockRDD, even when there are no blocks
      val blockIds = blockInfos.map { _.blockId.asInstanceOf[BlockId] }.toArray
      val seqNumRanges = blockInfos.map {
        _.metadataOption.get.asInstanceOf[SequenceNumberRanges] }.toArray
      val isBlockIdValid = blockInfos.map { _.isBlockIdValid() }.toArray
      logDebug(s"Creating KinesisBackedBlockRDD for $time with ${seqNumRanges.length} " +
          s"seq number ranges: ${seqNumRanges.mkString(", ")} ")
      new KinesisBackedBlockRDD(
        context.sc, regionName, endpointUrl, blockIds, seqNumRanges,
        isBlockIdValid = isBlockIdValid,
        retryTimeoutMs = ssc.graph.batchDuration.milliseconds.toInt,
        awsCredentialsOption = awsCredentialsOption)
    } else {
      logWarning("Kinesis sequence number information was not present with some block metadata," +
        " it may not be possible to recover from failures")
      super.createBlockRDD(time, blockInfos)
    }
  }

  override def getReceiver(): Receiver[Array[Byte]] = {
    new KinesisReceiver(streamName, endpointUrl, regionName, initialPositionInStream,
      checkpointAppName, checkpointInterval, storageLevel, awsCredentialsOption)
  }
}
