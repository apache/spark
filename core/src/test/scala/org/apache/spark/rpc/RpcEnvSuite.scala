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

import org.apache.spark.Logging
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
    val rpcEndPointRef = env.setupEndPoint("send_test", new RpcEndPoint with Logging {
      override val rpcEnv = env

      override def receive(sender: RpcEndPointRef) = {
        case msg: String => message = msg
      }
    })
    rpcEndPointRef.send("hello")
    Thread.sleep(2000)
    assert("hello" === message)
  }

  test("ask a message locally") {
    val rpcEndPointRef = env.setupEndPoint("ask_test", new RpcEndPoint with Logging {
      override val rpcEnv = env

      override def receive(sender: RpcEndPointRef) = {
        case msg: String => sender.send(msg)
      }
    })
    val reply = rpcEndPointRef.askWithReply[String]("hello")
    assert("hello" === reply)
  }

  test("ping pong") {
    case object Start

    case class Ping(id: Int)

    case class Pong(id: Int)

    val pongRef = env.setupEndPoint("pong", new RpcEndPoint with Logging {
      override val rpcEnv = env

      override def receive(sender: RpcEndPointRef) = {
        case Ping(id) => sender.send(Pong(id))
      }
    })

    val pingRef = env.setupEndPoint("ping", new RpcEndPoint with Logging {
      override val rpcEnv = env

      var requester: RpcEndPointRef = _

      override def receive(sender: RpcEndPointRef) = {
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
    val endPoint = new RpcEndPoint with Logging {
      override val rpcEnv = env

      override def receive(sender: RpcEndPointRef) = {
        case msg: String => sender.send(msg)
      }
    }
    val rpcEndPointRef = env.setupEndPoint("register_test", endPoint)
    assert(rpcEndPointRef eq env.endPointRef(endPoint))
    endPoint.stop()
    assert(null == env.endPointRef(endPoint))
  }
}
