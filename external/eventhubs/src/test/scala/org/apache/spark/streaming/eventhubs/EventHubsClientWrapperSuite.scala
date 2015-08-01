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

import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.mockito.Matchers
import org.mockito.ArgumentMatcher
import com.microsoft.eventhubs.client.{EventHubEnqueueTimeFilter, IEventHubFilter, EventHubOffsetFilter}

/**
 * Test suite for EventHubsClientWrapper
 */
class EventHubsClientWrapperSuite extends FunSuite with BeforeAndAfter with MockitoSugar {
  var ehClientWrapperMock: EventHubsClientWrapper = _
  var offsetStoreMock: OffsetStore = _
  val ehParams = Map[String, String] (
    "eventhubs.policyname" -> "policyname",
    "eventhubs.policykey" -> "policykey",
    "eventhubs.namespace" -> "namespace",
    "eventhubs.name" -> "name",
    "eventhubs.partition.count" -> "4",
    "eventhubs.checkpoint.dir" -> "checkpointdir",
    "eventhubs.checkpoint.interval" -> "0"
  )

  def beforeFunction(): Unit = {
    ehClientWrapperMock = spy(new EventHubsClientWrapper)
    offsetStoreMock = mock[OffsetStore]
  }

  def afterFunction(): Unit = {
  }

  before(beforeFunction)
  after(afterFunction)

  // Verify that the EventHubOffsetFilter matches the given offset string
  class OffsetFilterEqual(offset: String) extends ArgumentMatcher[EventHubOffsetFilter] {
    override def matches(obj: Object): Boolean = {
      if(obj == null) {
        false
      }
      else {
        obj.asInstanceOf[EventHubOffsetFilter].getFilterValue().equals(offset)
      }
    }
  }

  // Verify that the EventHubEnqueueTimeFilter matches the given enqueuetime string
  class EnqueueTimeFilterEqual(time: String) extends ArgumentMatcher[EventHubEnqueueTimeFilter] {
    override def matches(obj: Object): Boolean = {
      if(obj == null) {
        false
      }
      else {
        obj.asInstanceOf[EventHubEnqueueTimeFilter].getFilterValue().equals(time)
      }
    }
  }

  test("EventHubsClientWrapper converts parameters correctly when offset was previously saved") {
    when(offsetStoreMock.read()).thenReturn("123")
    doNothing().when(ehClientWrapperMock).createReceiverProxy(
      Matchers.anyString, Matchers.anyString, Matchers.anyString, Matchers.anyString,
      Matchers.anyInt, Matchers.any[IEventHubFilter])

    ehClientWrapperMock.createReceiver(ehParams, "0", offsetStoreMock)

    verify(ehClientWrapperMock, times(1)).createReceiverProxy(
      Matchers.eq("amqps://policyname:policykey@namespace.servicebus.windows.net"),
      Matchers.eq("name"),
      Matchers.eq("0"),
      Matchers.eq(null),
      Matchers.eq(-1),
      Matchers.argThat(new OffsetFilterEqual("123")))
  }

  test("EventHubsClientWrapper converts parameters for consumergroup") {
    val ehParams2 = collection.mutable.Map[String, String]() ++= ehParams
    ehParams2("eventhubs.consumergroup") = "$consumergroup"
    when(offsetStoreMock.read()).thenReturn("-1")
    doNothing().when(ehClientWrapperMock).createReceiverProxy(
      Matchers.anyString, Matchers.anyString, Matchers.anyString, Matchers.anyString,
      Matchers.anyInt, Matchers.any[IEventHubFilter])

    ehClientWrapperMock.createReceiver(ehParams2, "0", offsetStoreMock)

    verify(ehClientWrapperMock, times(1)).createReceiverProxy(
      Matchers.eq("amqps://policyname:policykey@namespace.servicebus.windows.net"),
      Matchers.eq("name"),
      Matchers.eq("0"),
      Matchers.eq("$consumergroup"),
      Matchers.eq(-1),
      Matchers.eq(null))
  }

  test("EventHubsClientWrapper converts parameters for enqueuetime filter") {
    val ehParams2 = collection.mutable.Map[String, String]() ++= ehParams
    ehParams2("eventhubs.filter.enqueuetime") = "1433889798"
    when(offsetStoreMock.read()).thenReturn("-1")
    doNothing().when(ehClientWrapperMock).createReceiverProxy(
      Matchers.anyString, Matchers.anyString, Matchers.anyString, Matchers.anyString,
      Matchers.anyInt, Matchers.any[IEventHubFilter])

    ehClientWrapperMock.createReceiver(ehParams2, "0", offsetStoreMock)

    verify(ehClientWrapperMock, times(1)).createReceiverProxy(
      Matchers.eq("amqps://policyname:policykey@namespace.servicebus.windows.net"),
      Matchers.eq("name"),
      Matchers.eq("0"),
      Matchers.eq(null),
      Matchers.eq(-1),
      Matchers.argThat(new EnqueueTimeFilterEqual("1433889798")))
  }
}
