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

import com.microsoft.eventhubs.client._

import scala.collection.Map

/**
 * Wraps a raw EventHubReceiver to make it easier for unit tests
 */
@SerialVersionUID(1L)
class EventHubsClientWrapper extends Serializable {
  var receiver: ResilientEventHubReceiver = null

  def createReceiver(eventhubsParams: Map[String, String],
      partitionId: String,
      offsetStore: OffsetStore): Unit = {
    // Read previously stored offset if exist
    var filter: IEventHubFilter = null
    val offset = offsetStore.read()
    if(offset != "-1" && offset != null) {
      filter = new EventHubOffsetFilter(offset)
    }
    else if (eventhubsParams.contains("eventhubs.filter.offset")) {
      filter = new EventHubOffsetFilter(eventhubsParams("eventhubs.filter.offset"))
    }
    else if (eventhubsParams.contains("eventhubs.filter.enqueuetime")) {
      filter = new EventHubEnqueueTimeFilter(
        eventhubsParams("eventhubs.filter.enqueuetime").toLong)
    }

    // Create EventHubs connection string
    val connectionString = new ConnectionStringBuilder(
      eventhubsParams("eventhubs.policyname"),
      eventhubsParams("eventhubs.policykey"),
      eventhubsParams("eventhubs.namespace")
    ).getConnectionString

    // Set consumer group name if provided by user
    var consumerGroup: String = null
    if(eventhubsParams.contains("eventhubs.consumergroup")) {
      consumerGroup = eventhubsParams("eventhubs.consumergroup")
    }
    val name = eventhubsParams("eventhubs.name")

    createReceiverProxy(connectionString, name, partitionId, consumerGroup, -1, filter)
  }

  private[eventhubs]
  def createReceiverProxy(connectionString: String,
      name: String,
      partitionId: String,
      consumerGroup: String,
      defaultCredits: Int,
      filter: IEventHubFilter): Unit = {
    receiver = new ResilientEventHubReceiver(
      connectionString, name, partitionId, consumerGroup, -1, filter)
    receiver.initialize()
  }

  def receive(): EventHubMessage = {
    EventHubMessage.parseAmqpMessage(receiver.receive(5000))
  }

  def close(): Unit = {
    if(receiver != null) {
      receiver.close()
    }
  }
}
