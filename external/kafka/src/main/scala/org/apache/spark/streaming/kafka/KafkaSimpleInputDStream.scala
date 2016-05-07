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

package org.apache.spark.streaming.kafka

import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import kafka.serializer.Decoder

class KafkaSimpleInputDStream[U <: Decoder[_]: Manifest, T <: Decoder[_]: Manifest](
    @transient ssc_ : StreamingContext,
    zkQuorum: String,
    groupId: String,
    topic: String,
    partition: Integer,
    startPositionOffset: Long,
    autoCommitOffset: Boolean,
    maxBatchByteSize: Int = 1024 * 1024,
    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
  ) extends ReceiverInputDStream[(Long, Array[Byte])](ssc_) with Logging {

  def getReceiver(): Receiver[(Long, Array[Byte])] = {
    new KafkaSimpleReceiver[U, T](zkQuorum, groupId, topic, partition, startPositionOffset,
        autoCommitOffset, maxBatchByteSize, storageLevel)
      .asInstanceOf[Receiver[(Long, Array[Byte])]]
  }
}

class KafkaSimpleReceiver[U <: Decoder[_]: Manifest, T <: Decoder[_]: Manifest](
    zkQuorum: String,
    groupId: String,
    topic: String,
    partition: Integer,
    startPositionOffset: Long,
    autoCommitOffset: Boolean,
    maxBatchByteSize: Int = 1024 * 1024,
    storageLevel: StorageLevel
  ) extends Receiver[Any](storageLevel) with Logging {

  var currentOffset = startPositionOffset
  val kac = new KafkaSimpleConsumer(zkQuorum, groupId, topic, partition, maxBatchByteSize)

  def onStop() {
    kac.close
    logInfo("Kafka consumer closed.")
  }

  def onStart() {
    logInfo("Starting Kafka Consumer Stream")
    val firstOffset = kac.getEarliestOffset()
    if (currentOffset < firstOffset) {
      logWarning(s"""at present, the first offset is ${firstOffset}, the messages which is 
        |from ${currentOffset} to ${firstOffset} might been pruned.""".stripMargin)
      currentOffset = firstOffset
    }
    while (true) {
      val messageSet = kac.fetch(currentOffset)

      val itr = messageSet.iterator
      var hasMessage = false
      while (itr.hasNext) {
        val messageAndOffset = itr.next()
        val payload = messageAndOffset.message.payload
        val bytes = new Array[Byte](payload.limit)
        payload.get(bytes)
        currentOffset = messageAndOffset.offset
        store((currentOffset, bytes))
        hasMessage = true
        if (autoCommitOffset) {
          kac.commitOffsetToZookeeper(currentOffset)
        }
      }
      if (hasMessage) {
        currentOffset += 1
      }
      Thread.sleep(1000)
    }
  }
}
