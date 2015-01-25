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

package org.apache.spark.rpc

import scala.concurrent.duration._
import scala.language.postfixOps

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.concurrent.Eventually._

/**
 * Common tests for an RpcEnv implementation.
 */
abstract class RpcEnvSuite extends FunSuite with BeforeAndAfterAll {

  var env: RpcEnv = _

  override def beforeAll(): Unit = {
    env = createRpcEnv
  }

  override def afterAll(): Unit = {
    if(env != null) {
      env.stopAll()
    }
  }

  def createRpcEnv: RpcEnv

  test("send a message locally") {
    @volatile var message: String = null
    val rpcEndpointRef = env.setupEndpoint("send_test", new RpcEndpoint {
      override val rpcEnv = env

      override def receive(sender: RpcEndpointRef) = {
        case msg: String => message = msg
      }
    })
    rpcEndpointRef.send("hello")
    Thread.sleep(2000)
    assert("hello" === message)
  }

  test("ask a message locally") {
    val rpcEndpointRef = env.setupEndpoint("ask_test", new RpcEndpoint {
      override val rpcEnv = env

      override def receive(sender: RpcEndpointRef) = {
        case msg: String => sender.send(msg)
      }
    })
    val reply = rpcEndpointRef.askWithReply[String]("hello")
    assert("hello" === reply)
  }

  test("ping pong") {
    case object Start

    case class Ping(id: Int)

    case class Pong(id: Int)

    val pongRef = env.setupEndpoint("pong", new RpcEndpoint {
      override val rpcEnv = env

      override def receive(sender: RpcEndpointRef) = {
        case Ping(id) => sender.send(Pong(id))
      }
    })

    val pingRef = env.setupEndpoint("ping", new RpcEndpoint {
      override val rpcEnv = env

      var requester: RpcEndpointRef = _

      override def receive(sender: RpcEndpointRef) = {
        case Start => {
          requester = sender
          pongRef.send(Ping(1))
        }
        case p @ Pong(id) => {
          if (id < 10) {
            sender.send(Ping(id + 1))
          } else {
            requester.send(p)
          }
        }
      }
    })

    val reply = pingRef.askWithReply[Pong](Start)
    assert(Pong(10) === reply)
  }

  test("register and unregister") {
    val endpoint = new RpcEndpoint {
      override val rpcEnv = env

      override def receive(sender: RpcEndpointRef) = {
        case msg: String => sender.send(msg)
      }
    }
    val rpcEndpointRef = env.setupEndpoint("register_test", endpoint)

    eventually(timeout(5 seconds), interval(200 milliseconds)) {
      assert(rpcEndpointRef eq env.endpointRef(endpoint))
    }
    endpoint.stop()

    val e = intercept[IllegalArgumentException] {
      env.endpointRef(endpoint)
    }
    assert(e.getMessage.contains("Cannot find RpcEndpointRef"))
  }

  test("fault tolerance") {
    case class SetState(state: Int)

    case object Crash

    case object GetState

    val rpcEndpointRef = env.setupEndpoint("fault_tolerance", new RpcEndpoint {
      override val rpcEnv = env

      var state: Int = 0

      override def receive(sender: RpcEndpointRef) = {
        case SetState(state) => this.state = state
        case Crash => throw new RuntimeException("Oops")
        case GetState => sender.send(state)
      }
    })
    assert(0 === rpcEndpointRef.askWithReply[Int](GetState))

    rpcEndpointRef.send(SetState(10))
    assert(10 === rpcEndpointRef.askWithReply[Int](GetState))

    rpcEndpointRef.send(Crash)
    // RpcEndpoint is crashed. Should reset its state.
    assert(0 === rpcEndpointRef.askWithReply[Int](GetState))
  }
}
