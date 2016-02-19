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

package org.apache.spark.rpc.netty

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import org.mockito.Mockito._

import org.apache.spark.SparkFunSuite
import org.apache.spark.rpc.{RpcEnv, RpcEndpoint, RpcAddress, TestRpcEndpoint}

class InboxSuite extends SparkFunSuite {

  test("post") {
    val endpoint = new TestRpcEndpoint
    val endpointRef = mock(classOf[NettyRpcEndpointRef])
    when(endpointRef.name).thenReturn("hello")

    val dispatcher = mock(classOf[Dispatcher])

    val inbox = new Inbox(endpointRef, endpoint)
    val message = OneWayMessage(null, "hi")
    inbox.post(message)
    inbox.process(dispatcher)
    assert(inbox.isEmpty)

    endpoint.verifySingleReceiveMessage("hi")

    inbox.stop()
    inbox.process(dispatcher)
    assert(inbox.isEmpty)
    endpoint.verifyStarted()
    endpoint.verifyStopped()
  }

  test("post: with reply") {
    val endpoint = new TestRpcEndpoint
    val endpointRef = mock(classOf[NettyRpcEndpointRef])
    val dispatcher = mock(classOf[Dispatcher])

    val inbox = new Inbox(endpointRef, endpoint)
    val message = RpcMessage(null, "hi", null)
    inbox.post(message)
    inbox.process(dispatcher)
    assert(inbox.isEmpty)

    endpoint.verifySingleReceiveAndReplyMessage("hi")
  }

  test("post: multiple threads") {
    val endpoint = new TestRpcEndpoint
    val endpointRef = mock(classOf[NettyRpcEndpointRef])
    when(endpointRef.name).thenReturn("hello")

    val dispatcher = mock(classOf[Dispatcher])

    val numDroppedMessages = new AtomicInteger(0)
    val inbox = new Inbox(endpointRef, endpoint) {
      override def onDrop(message: InboxMessage): Unit = {
        numDroppedMessages.incrementAndGet()
      }
    }

    val exitLatch = new CountDownLatch(10)

    for (_ <- 0 until 10) {
      new Thread {
        override def run(): Unit = {
          for (_ <- 0 until 100) {
            val message = OneWayMessage(null, "hi")
            inbox.post(message)
          }
          exitLatch.countDown()
        }
      }.start()
    }
    // Try to process some messages
    inbox.process(dispatcher)
    inbox.stop()
    // After `stop` is called, further messages will be dropped. However, while `stop` is called,
    // some messages may be post to Inbox, so process them here.
    inbox.process(dispatcher)
    assert(inbox.isEmpty)

    exitLatch.await(30, TimeUnit.SECONDS)

    assert(1000 === endpoint.numReceiveMessages + numDroppedMessages.get)
    endpoint.verifyStarted()
    endpoint.verifyStopped()
  }

  test("post: Associated") {
    val endpoint = new TestRpcEndpoint
    val endpointRef = mock(classOf[NettyRpcEndpointRef])
    val dispatcher = mock(classOf[Dispatcher])

    val remoteAddress = RpcAddress("localhost", 11111)

    val inbox = new Inbox(endpointRef, endpoint)
    inbox.post(RemoteProcessConnected(remoteAddress))
    inbox.process(dispatcher)

    endpoint.verifySingleOnConnectedMessage(remoteAddress)
  }

  test("post: Disassociated") {
    val endpoint = new TestRpcEndpoint
    val endpointRef = mock(classOf[NettyRpcEndpointRef])
    val dispatcher = mock(classOf[Dispatcher])

    val remoteAddress = RpcAddress("localhost", 11111)

    val inbox = new Inbox(endpointRef, endpoint)
    inbox.post(RemoteProcessDisconnected(remoteAddress))
    inbox.process(dispatcher)

    endpoint.verifySingleOnDisconnectedMessage(remoteAddress)
  }

  test("post: AssociationError") {
    val endpoint = new TestRpcEndpoint
    val endpointRef = mock(classOf[NettyRpcEndpointRef])
    val dispatcher = mock(classOf[Dispatcher])

    val remoteAddress = RpcAddress("localhost", 11111)
    val cause = new RuntimeException("Oops")

    val inbox = new Inbox(endpointRef, endpoint)
    inbox.post(RemoteProcessConnectionError(cause, remoteAddress))
    inbox.process(dispatcher)

    endpoint.verifySingleOnNetworkErrorMessage(cause, remoteAddress)
  }
}
