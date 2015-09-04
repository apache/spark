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
import org.apache.spark.streaming.{StreamingContext, TestSuiteBase}
import org.apache.spark.streaming.receiver.ReceiverSupervisor

import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import com.microsoft.eventhubs.client.EventHubMessage

/**
 * Suite of EventHubs streaming receiver tests
 * This suite of tests are low level unit tests, they directly call EventHubsReceiver with mocks
 */
class EventHubsReceiverSuite extends TestSuiteBase with org.scalatest.Matchers
with MockitoSugar {
  var ehClientWrapperMock: EventHubsClientWrapper = _
  var offsetStoreMock: OffsetStore = _
  var executorMock: ReceiverSupervisor = _
  val ehParams = Map[String, String] (
    "eventhubs.policyname" -> "policyname",
    "eventhubs.policykey" -> "policykey",
    "eventhubs.namespace" -> "namespace",
    "eventhubs.name" -> "name",
    "eventhubs.partition.count" -> "4",
    "eventhubs.checkpoint.dir" -> "checkpointdir",
    "eventhubs.checkpoint.interval" -> "1000"
  )

  override def beforeFunction() = {
    ehClientWrapperMock = mock[EventHubsClientWrapper]
    offsetStoreMock = mock[OffsetStore]
    executorMock = mock[ReceiverSupervisor]
  }

  override def afterFunction(): Unit = {
    super.afterFunction()
    // Since this suite was originally written using EasyMock, add this to preserve the old
    // mocking semantics (see SPARK-5735 for more details)
    verifyNoMoreInteractions(ehClientWrapperMock, offsetStoreMock)
  }

  test("EventHubsUtils API works") {
    val ssc = new StreamingContext(master, framework, batchDuration)
    EventHubsUtils.createStream(ssc, ehParams, "0", StorageLevel.MEMORY_ONLY)
    EventHubsUtils.createUnionStream(ssc, ehParams, StorageLevel.MEMORY_ONLY_2)
    ssc.stop()
  }

  test("EventHubsReceiver can receive message with proper checkpointing") {
    val ehParams2 = collection.mutable.Map[String, String]() ++= ehParams
    ehParams2("eventhubs.checkpoint.interval") = "0"

    // Mock object setup
    val data = Array[Byte](1,2,3,4)
    when(offsetStoreMock.read()).thenReturn("-1")
    when(ehClientWrapperMock.receive())
      .thenReturn(new EventHubMessage("123", 456, 789, data))
      .thenReturn(null)
    val receiver = new EventHubsReceiver(ehParams2, "0", StorageLevel.MEMORY_ONLY,
      offsetStoreMock, ehClientWrapperMock)
    receiver.attachExecutor(executorMock)

    receiver.onStart()
    Thread sleep (100)
    receiver.onStop()

    verify(offsetStoreMock, times(1)).open()
    verify(offsetStoreMock, times(1)).write("123")
    verify(offsetStoreMock, times(1)).close()

    verify(ehClientWrapperMock, times(1)).createReceiver(ehParams2, "0", offsetStoreMock)
    verify(ehClientWrapperMock, atLeastOnce).receive()
    verify(ehClientWrapperMock, times(1)).close()
  }

  test("EventHubsReceiver can restart when exception is thrown") {
    // Mock object setup
    val data = Array[Byte](1,2,3,4)
    val exception = new RuntimeException("error")
    when(offsetStoreMock.read()).thenReturn("-1")
    when(ehClientWrapperMock.receive())
      .thenReturn(new EventHubMessage("123", 456, 789, data)) // return message "123"
      .thenThrow(exception) // then throw

    val receiver = new EventHubsReceiver(ehParams, "0", StorageLevel.MEMORY_ONLY,
      offsetStoreMock, ehClientWrapperMock)
    receiver.attachExecutor(executorMock)

    receiver.onStart()
    Thread sleep (100)
    receiver.onStop()

    // Verify that executor.restartReceiver() has been called
    verify(executorMock, times(1))
      .restartReceiver("Error handling message; restarting receiver", Some(exception))

    verify(offsetStoreMock, times(1)).open()
    verify(offsetStoreMock, times(1)).close()

    verify(ehClientWrapperMock, times(1)).createReceiver(ehParams, "0", offsetStoreMock)
    verify(ehClientWrapperMock, times(2)).receive()
    verify(ehClientWrapperMock, times(1)).close()
  }
}