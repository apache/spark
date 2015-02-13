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

import java.util.concurrent.{TimeUnit, CountDownLatch, TimeoutException}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.concurrent.Eventually._

import org.apache.spark.SparkConf

/**
 * Common tests for an RpcEnv implementation.
 */
abstract class RpcEnvSuite extends FunSuite with BeforeAndAfterAll {

  var env: RpcEnv = _

  override def beforeAll(): Unit = {
    val conf = new SparkConf()
    env = createRpcEnv(conf, 12345)
  }

  override def afterAll(): Unit = {
    if(env != null) {
      env.shutdown()
    }
  }

  def createRpcEnv(conf: SparkConf, port: Int): RpcEnv

  test("send a message locally") {
    @volatile var message: String = null
    val rpcEndpointRef = env.setupEndpoint("send-locally", new RpcEndpoint {
      override val rpcEnv = env

      override def receive(sender: RpcEndpointRef) = {
        case msg: String => message = msg
      }
    })
    rpcEndpointRef.send("hello")
    eventually(timeout(5 seconds), interval(10 millis)) {
      assert("hello" === message)
    }
  }

  test("send a message remotely") {
    @volatile var message: String = null
    // Set up a RpcEndpoint using env
    env.setupEndpoint("send-remotely", new RpcEndpoint {
      override val rpcEnv = env

      override def receive(sender: RpcEndpointRef) = {
        case msg: String => message = msg
      }
    })

    val anotherEnv = createRpcEnv(new SparkConf(), 13345)
    // Use anotherEnv to find out the RpcEndpointRef
    val rpcEndpointRef = anotherEnv.setupEndpointRef(env.systemName, env.address, "send-remotely")
    try {
      rpcEndpointRef.send("hello")
      eventually(timeout(5 seconds), interval(10 millis)) {
        assert("hello" === message)
      }
    } finally {
      anotherEnv.shutdown()
    }
  }

  test("send a RpcEndpointRef") {
    val endpoint = new RpcEndpoint {
      override val rpcEnv = env

      override def receive(sender: RpcEndpointRef) = {
        case "Hello" => sender.send(self)
        case "Echo" => sender.send("Echo")
      }
    }
    val rpcEndpointRef = env.setupEndpoint("send-ref", endpoint)

    val newRpcEndpointRef = rpcEndpointRef.askWithReply[RpcEndpointRef]("Hello")
    val reply = newRpcEndpointRef.askWithReply[String]("Echo")
    assert("Echo" === reply)
  }

  test("ask a message locally") {
    val rpcEndpointRef = env.setupEndpoint("ask-locally", new RpcEndpoint {
      override val rpcEnv = env

      override def receive(sender: RpcEndpointRef) = {
        case msg: String => {
          sender.send(msg)
        }
      }
    })
    val reply = rpcEndpointRef.askWithReply[String]("hello")
    assert("hello" === reply)
  }

  test("ask a message remotely") {
    env.setupEndpoint("ask-remotely", new RpcEndpoint {
      override val rpcEnv = env

      override def receive(sender: RpcEndpointRef) = {
        case msg: String => {
          sender.send(msg)
        }
      }
    })

    val anotherEnv = createRpcEnv(new SparkConf(), 13345)
    // Use anotherEnv to find out the RpcEndpointRef
    val rpcEndpointRef = anotherEnv.setupEndpointRef(env.systemName, env.address, "ask-remotely")
    try {
      val reply = rpcEndpointRef.askWithReply[String]("hello")
      assert("hello" === reply)
    } finally {
      anotherEnv.shutdown()
    }
  }

  test("ask a message timeout") {
    env.setupEndpoint("ask-timeout", new RpcEndpoint {
      override val rpcEnv = env

      override def receive(sender: RpcEndpointRef) = {
        case msg: String => {
          Thread.sleep(100)
          sender.send(msg)
        }
      }
    })

    val conf = new SparkConf()
    conf.set("spark.akka.retry.wait", "0")
    conf.set("spark.akka.num.retries", "1")
    val anotherEnv = createRpcEnv(conf, 13345)
    // Use anotherEnv to find out the RpcEndpointRef
    val rpcEndpointRef = anotherEnv.setupEndpointRef(env.systemName, env.address, "ask-timeout")
    try {
      val e = intercept[Exception] {
        rpcEndpointRef.askWithReply[String]("hello", 1 millis)
      }
      assert(e.isInstanceOf[TimeoutException] || e.getCause.isInstanceOf[TimeoutException])
    } finally {
      anotherEnv.shutdown()
    }
  }

  test("ping pong") {
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

  test("onStart and onStop") {
    val stopLatch = new CountDownLatch(1)
    val calledMethods = mutable.ArrayBuffer[String]()

    val endpoint = new RpcEndpoint {
      override val rpcEnv = env

      override def onStart(): Unit = {
        calledMethods += "start"
      }

      override def receive(sender: RpcEndpointRef) = {
        case msg: String =>
      }

      override def onStop(): Unit = {
        calledMethods += "stop"
        stopLatch.countDown()
      }
    }
    val rpcEndpointRef = env.setupEndpoint("start-stop-test", endpoint)
    rpcEndpointRef.send("message")
    env.stop(rpcEndpointRef)
    stopLatch.await(10, TimeUnit.SECONDS)
    assert(List("start", "stop") === calledMethods)
  }

  test("onError: error in onStart") {
    @volatile var e: Throwable = null
    env.setupEndpoint("onError-onStart", new RpcEndpoint {
      override val rpcEnv = env

      override def onStart(): Unit = {
        throw new RuntimeException("Oops!")
      }

      override def receive(sender: RpcEndpointRef) = {
        case m =>
      }

      override def onError(cause: Throwable): Unit = {
        e = cause
      }
    })

    eventually(timeout(5 seconds), interval(10 millis)) {
      assert(e.getMessage === "Oops!")
    }
  }

  test("onError: error in onStop") {
    @volatile var e: Throwable = null
    val endpointRef = env.setupEndpoint("onError-onStop", new RpcEndpoint {
      override val rpcEnv = env

      override def receive(sender: RpcEndpointRef) = {
        case m =>
      }

      override def onError(cause: Throwable): Unit = {
        e = cause
      }

      override def onStop(): Unit = {
        throw new RuntimeException("Oops!")
      }
    })

    env.stop(endpointRef)

    eventually(timeout(5 seconds), interval(10 millis)) {
      assert(e.getMessage === "Oops!")
    }
  }

  test("onError: error in receive") {
    @volatile var e: Throwable = null
    val endpointRef = env.setupEndpoint("onError-receive", new RpcEndpoint {
      override val rpcEnv = env

      override def receive(sender: RpcEndpointRef) = {
        case m =>  throw new RuntimeException("Oops!")
      }

      override def onError(cause: Throwable): Unit = {
        e = cause
      }
    })

    endpointRef.send("Foo")

    eventually(timeout(5 seconds), interval(10 millis)) {
      assert(e.getMessage === "Oops!")
    }
  }

  test("self: call in onStart") {
    @volatile var callSelfSuccessfully = false

    env.setupEndpoint("self-onStart", new RpcEndpoint {
      override val rpcEnv = env

      override def onStart(): Unit = {
        self
        callSelfSuccessfully = true
      }

      override def receive(sender: RpcEndpointRef) = {
        case m =>
      }
    })

    eventually(timeout(5 seconds), interval(10 millis)) {
      // Calling `self` in `onStart` is fine
      assert(callSelfSuccessfully === true)
    }
  }

  test("self: call in receive") {
    @volatile var callSelfSuccessfully = false

    val endpointRef = env.setupEndpoint("self-receive", new RpcEndpoint {
      override val rpcEnv = env

      override def receive(sender: RpcEndpointRef) = {
        case m => {
          self
          callSelfSuccessfully = true
        }
      }
    })

    endpointRef.send("Foo")

    eventually(timeout(5 seconds), interval(10 millis)) {
      // Calling `self` in `receive` is fine
      assert(callSelfSuccessfully === true)
    }
  }

  test("self: call in onStop") {
    @volatile var e: Throwable = null

    val endpointRef = env.setupEndpoint("self-onStop", new RpcEndpoint {
      override val rpcEnv = env

      override def receive(sender: RpcEndpointRef) = {
        case m =>
      }

      override def onStop(): Unit = {
        self
      }

      override def onError(cause: Throwable): Unit = {
        e = cause
      }
    })

    env.stop(endpointRef)

    eventually(timeout(5 seconds), interval(10 millis)) {
      // Calling `self` in `onStop` is invalid
      assert(e != null)
      assert(e.getMessage.contains("Cannot find RpcEndpointRef"))
    }
  }

  test("call receive in sequence") {
    // If a RpcEnv implementation breaks the `receive` contract, hope this test can expose it
    for(i <- 0 until 100) {
      @volatile var result = 0
      val endpointRef = env.setupEndpoint(s"receive-in-sequence-$i", new RpcEndpoint {
        override val rpcEnv = env

        override def receive(sender: RpcEndpointRef) = {
          case m => result += 1
        }

      })

      (0 until 10) foreach { _ =>
        new Thread {
          override def run() {
            (0 until 100) foreach { _ =>
              endpointRef.send("Hello")
            }
          }
        }.start()
      }

      eventually(timeout(5 seconds), interval(5 millis)) {
        assert(result == 1000)
      }

      env.stop(endpointRef)
    }
  }

  test("stop(RpcEndpointRef) reentrant") {
    @volatile var onStopCount = 0
    val endpointRef = env.setupEndpoint("stop-reentrant", new RpcEndpoint {
      override val rpcEnv = env

      override def receive(sender: RpcEndpointRef) = {
        case m =>
      }

      override def onStop(): Unit = {
        onStopCount += 1
      }
    })

    env.stop(endpointRef)
    env.stop(endpointRef)

    eventually(timeout(5 seconds), interval(5 millis)) {
      // Calling stop twice should only trigger onStop once.
      assert(onStopCount == 1)
    }
  }
}

case object Start

case class Ping(id: Int)

case class Pong(id: Int)
