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

import org.scalatest.{BeforeAndAfterAll, FunSuite}

abstract class RpcEnvSuite extends FunSuite with BeforeAndAfterAll {

  var env: RpcEnv = _

  override def beforeAll(): Unit = {
    env = createRpcEnv
  }

  override def afterAll(): Unit = {
    if(env != null) {
      destroyRpcEnv(env)
    }
  }

  def createRpcEnv: RpcEnv

  def destroyRpcEnv(rpcEnv: RpcEnv)

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
    assert(rpcEndpointRef eq env.endpointRef(endpoint))
    endpoint.stop()
    assert(null == env.endpointRef(endpoint))
  }
}
