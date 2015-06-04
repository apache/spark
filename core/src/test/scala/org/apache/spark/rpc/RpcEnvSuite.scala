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
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually._

import org.apache.spark.{SparkConf, SparkException, SparkFunSuite}

/**
 * Common tests for an RpcEnv implementation.
 */
abstract class RpcEnvSuite extends SparkFunSuite with BeforeAndAfterAll {

  var env: RpcEnv = _

  override def beforeAll(): Unit = {
    val conf = new SparkConf()
    env = createRpcEnv(conf, "local", 12345)
  }

  override def afterAll(): Unit = {
    if (env != null) {
      env.shutdown()
    }
  }

  def createRpcEnv(conf: SparkConf, name: String, port: Int): RpcEnv

  test("send a message locally") {
    @volatile var message: String = null
    val rpcEndpointRef = env.setupEndpoint("send-locally", new RpcEndpoint {
      override val rpcEnv = env

      override def receive = {
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

      override def receive: PartialFunction[Any, Unit] = {
        case msg: String => message = msg
      }
    })

    val anotherEnv = createRpcEnv(new SparkConf(), "remote", 13345)
    // Use anotherEnv to find out the RpcEndpointRef
    val rpcEndpointRef = anotherEnv.setupEndpointRef("local", env.address, "send-remotely")
    try {
      rpcEndpointRef.send("hello")
      eventually(timeout(5 seconds), interval(10 millis)) {
        assert("hello" === message)
      }
    } finally {
      anotherEnv.shutdown()
      anotherEnv.awaitTermination()
    }
  }

  test("send a RpcEndpointRef") {
    val endpoint = new RpcEndpoint {
      override val rpcEnv = env

      override def receiveAndReply(context: RpcCallContext) = {
        case "Hello" => context.reply(self)
        case "Echo" => context.reply("Echo")
      }
    }
    val rpcEndpointRef = env.setupEndpoint("send-ref", endpoint)

    val newRpcEndpointRef = rpcEndpointRef.askWithRetry[RpcEndpointRef]("Hello")
    val reply = newRpcEndpointRef.askWithRetry[String]("Echo")
    assert("Echo" === reply)
  }

  test("ask a message locally") {
    val rpcEndpointRef = env.setupEndpoint("ask-locally", new RpcEndpoint {
      override val rpcEnv = env

      override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
        case msg: String => {
          context.reply(msg)
        }
      }
    })
    val reply = rpcEndpointRef.askWithRetry[String]("hello")
    assert("hello" === reply)
  }

  test("ask a message remotely") {
    env.setupEndpoint("ask-remotely", new RpcEndpoint {
      override val rpcEnv = env

      override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
        case msg: String => {
          context.reply(msg)
        }
      }
    })

    val anotherEnv = createRpcEnv(new SparkConf(), "remote", 13345)
    // Use anotherEnv to find out the RpcEndpointRef
    val rpcEndpointRef = anotherEnv.setupEndpointRef("local", env.address, "ask-remotely")
    try {
      val reply = rpcEndpointRef.askWithRetry[String]("hello")
      assert("hello" === reply)
    } finally {
      anotherEnv.shutdown()
      anotherEnv.awaitTermination()
    }
  }

  test("ask a message timeout") {
    env.setupEndpoint("ask-timeout", new RpcEndpoint {
      override val rpcEnv = env

      override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
        case msg: String => {
          Thread.sleep(100)
          context.reply(msg)
        }
      }
    })

    val conf = new SparkConf()
    conf.set("spark.rpc.retry.wait", "0")
    conf.set("spark.rpc.numRetries", "1")
    val anotherEnv = createRpcEnv(conf, "remote", 13345)
    // Use anotherEnv to find out the RpcEndpointRef
    val rpcEndpointRef = anotherEnv.setupEndpointRef("local", env.address, "ask-timeout")
    try {
      val e = intercept[Exception] {
        rpcEndpointRef.askWithRetry[String]("hello", 1 millis)
      }
      assert(e.isInstanceOf[TimeoutException] || e.getCause.isInstanceOf[TimeoutException])
    } finally {
      anotherEnv.shutdown()
      anotherEnv.awaitTermination()
    }
  }

  test("onStart and onStop") {
    val stopLatch = new CountDownLatch(1)
    val calledMethods = mutable.ArrayBuffer[String]()

    val endpoint = new RpcEndpoint {
      override val rpcEnv = env

      override def onStart(): Unit = {
        calledMethods += "start"
      }

      override def receive: PartialFunction[Any, Unit] = {
        case msg: String =>
      }

      override def onStop(): Unit = {
        calledMethods += "stop"
        stopLatch.countDown()
      }
    }
    val rpcEndpointRef = env.setupEndpoint("start-stop-test", endpoint)
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

      override def receive: PartialFunction[Any, Unit] = {
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

      override def receive: PartialFunction[Any, Unit] = {
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

      override def receive: PartialFunction[Any, Unit] = {
        case m => throw new RuntimeException("Oops!")
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

      override def receive: PartialFunction[Any, Unit] = {
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

      override def receive: PartialFunction[Any, Unit] = {
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
    @volatile var selfOption: Option[RpcEndpointRef] = null

    val endpointRef = env.setupEndpoint("self-onStop", new RpcEndpoint {
      override val rpcEnv = env

      override def receive: PartialFunction[Any, Unit] = {
        case m =>
      }

      override def onStop(): Unit = {
        selfOption = Option(self)
      }

      override def onError(cause: Throwable): Unit = {
      }
    })

    env.stop(endpointRef)

    eventually(timeout(5 seconds), interval(10 millis)) {
      // Calling `self` in `onStop` will return null, so selfOption will be None
      assert(selfOption == None)
    }
  }

  test("call receive in sequence") {
    // If a RpcEnv implementation breaks the `receive` contract, hope this test can expose it
    for (i <- 0 until 100) {
      @volatile var result = 0
      val endpointRef = env.setupEndpoint(s"receive-in-sequence-$i", new ThreadSafeRpcEndpoint {
        override val rpcEnv = env

        override def receive: PartialFunction[Any, Unit] = {
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

      override def receive: PartialFunction[Any, Unit] = {
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

  test("sendWithReply") {
    val endpointRef = env.setupEndpoint("sendWithReply", new RpcEndpoint {
      override val rpcEnv = env

      override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
        case m => context.reply("ack")
      }
    })

    val f = endpointRef.ask[String]("Hi")
    val ack = Await.result(f, 5 seconds)
    assert("ack" === ack)

    env.stop(endpointRef)
  }

  test("sendWithReply: remotely") {
    env.setupEndpoint("sendWithReply-remotely", new RpcEndpoint {
      override val rpcEnv = env

      override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
        case m => context.reply("ack")
      }
    })

    val anotherEnv = createRpcEnv(new SparkConf(), "remote", 13345)
    // Use anotherEnv to find out the RpcEndpointRef
    val rpcEndpointRef = anotherEnv.setupEndpointRef("local", env.address, "sendWithReply-remotely")
    try {
      val f = rpcEndpointRef.ask[String]("hello")
      val ack = Await.result(f, 5 seconds)
      assert("ack" === ack)
    } finally {
      anotherEnv.shutdown()
      anotherEnv.awaitTermination()
    }
  }

  test("sendWithReply: error") {
    val endpointRef = env.setupEndpoint("sendWithReply-error", new RpcEndpoint {
      override val rpcEnv = env

      override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
        case m => context.sendFailure(new SparkException("Oops"))
      }
    })

    val f = endpointRef.ask[String]("Hi")
    val e = intercept[SparkException] {
      Await.result(f, 5 seconds)
    }
    assert("Oops" === e.getMessage)

    env.stop(endpointRef)
  }

  test("sendWithReply: remotely error") {
    env.setupEndpoint("sendWithReply-remotely-error", new RpcEndpoint {
      override val rpcEnv = env

      override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
        case msg: String => context.sendFailure(new SparkException("Oops"))
      }
    })

    val anotherEnv = createRpcEnv(new SparkConf(), "remote", 13345)
    // Use anotherEnv to find out the RpcEndpointRef
    val rpcEndpointRef = anotherEnv.setupEndpointRef(
      "local", env.address, "sendWithReply-remotely-error")
    try {
      val f = rpcEndpointRef.ask[String]("hello")
      val e = intercept[SparkException] {
        Await.result(f, 5 seconds)
      }
      assert("Oops" === e.getMessage)
    } finally {
      anotherEnv.shutdown()
      anotherEnv.awaitTermination()
    }
  }

  test("network events") {
    val events = new mutable.ArrayBuffer[(Any, Any)] with mutable.SynchronizedBuffer[(Any, Any)]
    env.setupEndpoint("network-events", new ThreadSafeRpcEndpoint {
      override val rpcEnv = env

      override def receive: PartialFunction[Any, Unit] = {
        case "hello" =>
        case m => events += "receive" -> m
      }

      override def onConnected(remoteAddress: RpcAddress): Unit = {
        events += "onConnected" -> remoteAddress
      }

      override def onDisconnected(remoteAddress: RpcAddress): Unit = {
        events += "onDisconnected" -> remoteAddress
      }

      override def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit = {
        events += "onNetworkError" -> remoteAddress
      }

    })

    val anotherEnv = createRpcEnv(new SparkConf(), "remote", 13345)
    // Use anotherEnv to find out the RpcEndpointRef
    val rpcEndpointRef = anotherEnv.setupEndpointRef(
      "local", env.address, "network-events")
    val remoteAddress = anotherEnv.address
    rpcEndpointRef.send("hello")
    eventually(timeout(5 seconds), interval(5 millis)) {
      assert(events === List(("onConnected", remoteAddress)))
    }

    anotherEnv.shutdown()
    anotherEnv.awaitTermination()
    eventually(timeout(5 seconds), interval(5 millis)) {
      assert(events === List(
        ("onConnected", remoteAddress),
        ("onNetworkError", remoteAddress),
        ("onDisconnected", remoteAddress)))
    }
  }

  test("sendWithReply: unserializable error") {
    env.setupEndpoint("sendWithReply-unserializable-error", new RpcEndpoint {
      override val rpcEnv = env

      override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
        case msg: String => context.sendFailure(new UnserializableException)
      }
    })

    val anotherEnv = createRpcEnv(new SparkConf(), "remote", 13345)
    // Use anotherEnv to find out the RpcEndpointRef
    val rpcEndpointRef = anotherEnv.setupEndpointRef(
      "local", env.address, "sendWithReply-unserializable-error")
    try {
      val f = rpcEndpointRef.ask[String]("hello")
      intercept[TimeoutException] {
        Await.result(f, 1 seconds)
      }
    } finally {
      anotherEnv.shutdown()
      anotherEnv.awaitTermination()
    }
  }

}

class UnserializableClass

class UnserializableException extends Exception {
  private val unserializableField = new UnserializableClass
}
