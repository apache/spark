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

import java.util.concurrent.{TimeUnit, CountDownLatch}

import org.mockito.Mockito._
import org.scalatest.FunSuite

import org.apache.spark.rpc.{TestRpcEndpoint, RpcAddress}

class InboxSuite extends FunSuite {

  test("post") {
    val endpoint = new TestRpcEndpoint
    val endpointRef = mock(classOf[NettyRpcEndpointRef])
    when(endpointRef.name).thenReturn("hello")

    val dispatcher = mock(classOf[Dispatcher])

    val inbox = new Inbox(endpointRef, endpoint)
    val message = ContentMessage(null, "hi", false, null)
    inbox.post(message)
    assert(inbox.process(dispatcher) === false)

    endpoint.verifySingleReceiveMessage("hi")

    inbox.stop()
    assert(inbox.process(dispatcher) === true)
    endpoint.verifyStarted()
    endpoint.verifyStopped()
    verify(dispatcher).unregisterRpcEndpoint("hello")
  }

  test("post: with reply") {
    val endpoint = new TestRpcEndpoint
    val endpointRef = mock(classOf[NettyRpcEndpointRef])
    val dispatcher = mock(classOf[Dispatcher])

    val inbox = new Inbox(endpointRef, endpoint)
    val message = ContentMessage(null, "hi", true, null)
    inbox.post(message)
    assert(inbox.process(dispatcher) === false)

    endpoint.verifySingleReceiveAndReplyMessage("hi")
  }

  test("post: multiple threads") {
    val endpoint = new TestRpcEndpoint
    val endpointRef = mock(classOf[NettyRpcEndpointRef])
    when(endpointRef.name).thenReturn("hello")

    val dispatcher = mock(classOf[Dispatcher])

    @volatile var numDroppedMessages = 0
    val inbox = new Inbox(endpointRef, endpoint) {
      override def onDrop(message: Any): Unit = {
        numDroppedMessages += 1
      }
    }

    val exitLatch = new CountDownLatch(10)

    for(_ <- 0 until 10) {
      new Thread {
        override def run(): Unit = {
          for(_ <- 0 until 100) {
            val message = ContentMessage(null, "hi", false, null)
            inbox.post(message)
          }
          exitLatch.countDown()
        }
      }.start()
    }
    assert(inbox.process(dispatcher) === false)
    inbox.stop()
    assert(inbox.process(dispatcher) === true)

    exitLatch.await(30, TimeUnit.SECONDS)

    assert(1000 === endpoint.numReceiveMessages + numDroppedMessages)
    endpoint.verifyStarted()
    endpoint.verifyStopped()
    verify(dispatcher).unregisterRpcEndpoint("hello")
  }

  test("post: Associated") {
    val endpoint = new TestRpcEndpoint
    val endpointRef = mock(classOf[NettyRpcEndpointRef])
    val dispatcher = mock(classOf[Dispatcher])

    val remoteAddress = RpcAddress("localhost", 11111)

    val inbox = new Inbox(endpointRef, endpoint)
    inbox.post(Associated(remoteAddress))
    inbox.process(dispatcher)

    endpoint.verifySingleOnConnectedMessage(remoteAddress)
  }

  test("post: Disassociated") {
    val endpoint = new TestRpcEndpoint
    val endpointRef = mock(classOf[NettyRpcEndpointRef])
    val dispatcher = mock(classOf[Dispatcher])

    val remoteAddress = RpcAddress("localhost", 11111)

    val inbox = new Inbox(endpointRef, endpoint)
    inbox.post(Disassociated(remoteAddress))
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
    inbox.post(AssociationError(cause, remoteAddress))
    inbox.process(dispatcher)

    endpoint.verifySingleOnNetworkErrorMessage(cause, remoteAddress)
  }
}
