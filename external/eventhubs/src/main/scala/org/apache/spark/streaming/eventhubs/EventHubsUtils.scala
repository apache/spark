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
package org.apache.spark.streaming.eventhubs

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import scala.collection.Map

object EventHubsUtils {
  /**
   * Create a unioned EventHubs stream that receives data from Microsoft Azure Eventhubs
   * The unioned stream will receive message from all partitions of the EventHubs
   *
   * @param ssc Streaming Context object
   * @param eventhubsParams a Map that contains parameters for EventHubs.
   *   Required parameters are:
   *   "eventhubs.policyname": EventHubs policy name
   *   "eventhubs.policykey": EventHubs policy key
   *   "eventhubs.namespace": EventHubs namespace
   *   "eventhubs.name": EventHubs name
   *   "eventhubs.partition.count": Number of partitions
   *   "eventhubs.checkpoint.dir": checkpoint directory on HDFS
   *
   *   Optional parameters are:
   *   "eventhubs.consumergroup": EventHubs consumer group name, default to "\$default"
   *   "eventhubs.filter.offset": Starting offset of EventHubs, default to "-1"
   *   "eventhubs.filter.enqueuetime": Unix time, millisecond since epoch, default to "0"
   *   "eventhubs.default.credits": default AMQP credits, default to -1 (which is 1024)
   *   "eventhubs.checkpoint.interval": checkpoint interval in second, default to 10
   * @param storageLevel Storage level, by default it is MEMORY_ONLY
   * @return ReceiverInputStream
   */
  def createUnionStream (
      ssc: StreamingContext,
      eventhubsParams: Map[String, String],
      storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
    ): DStream[Array[Byte]] = {
    val partitionCount = eventhubsParams("eventhubs.partition.count").toInt
    val streams = (0 to partitionCount-1).map{
      i => createStream(ssc,eventhubsParams, i.toString, storageLevel)}
    ssc.union(streams)
  }

  /**
   * Create a single EventHubs stream that receives data from Microsoft Azure EventHubs
   * A single stream only receives message from one EventHubs partition
   *
   * @param ssc Streaming Context object
   * @param eventhubsParams a Map that contains parameters for EventHubs. Same as above.
   * @param partitionId Partition ID
   * @param storageLevel Storage level
   * @param offsetStore Offset store implementation, defaults to DFSBasedOffsetStore
   * @param receiverClient the EventHubs client implementation, defaults to EventHubsClientWrapper
   * @return ReceiverInputStream
   */
  def createStream (
    ssc: StreamingContext,
    eventhubsParams: Map[String, String],
    partitionId: String,
    storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
    offsetStore: OffsetStore = null,
    receiverClient: EventHubsClientWrapper = new EventHubsClientWrapper
  ): ReceiverInputDStream[Array[Byte]] = {
    ssc.receiverStream(getReceiver(ssc, eventhubsParams, partitionId, storageLevel, offsetStore,
      receiverClient))
  }

  /**
   * A helper function to get EventHubsReceiver or ReliableEventHubsReceiver based on whether
   * Write Ahead Log is enabled or not ("spark.streaming.receiver.writeAheadLog.enable")
   */
  private def getReceiver(
    ssc: StreamingContext,
    eventhubsParams: Map[String, String],
    partitionId: String,
    storageLevel: StorageLevel,
    offsetStore: OffsetStore,
    receiverClient: EventHubsClientWrapper): Receiver[Array[Byte]] = {
    val walEnabled = ssc.conf.getBoolean("spark.streaming.receiver.writeAheadLog.enable", false)
    if (walEnabled) {
      new ReliableEventHubsReceiver(eventhubsParams, partitionId, storageLevel, offsetStore,
        receiverClient)
    }
    else {
      new EventHubsReceiver(eventhubsParams, partitionId, storageLevel, offsetStore,
        receiverClient)
    }
  }
}
