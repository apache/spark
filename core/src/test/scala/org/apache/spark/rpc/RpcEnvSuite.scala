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

import java.io.{File, NotSerializableException}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

import com.google.common.io.Files
import org.mockito.Matchers.any
import org.mockito.Mockito.{mock, never, verify, when}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually._

import org.apache.spark.{SecurityManager, SparkConf, SparkEnv, SparkException, SparkFunSuite}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * Common tests for an RpcEnv implementation.
 */
abstract class RpcEnvSuite extends SparkFunSuite with BeforeAndAfterAll {

  var env: RpcEnv = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val conf = new SparkConf()
    env = createRpcEnv(conf, "local", 0)

    val sparkEnv = mock(classOf[SparkEnv])
    when(sparkEnv.rpcEnv).thenReturn(env)
    SparkEnv.set(sparkEnv)
  }

  override def afterAll(): Unit = {
    try {
      if (env != null) {
        env.shutdown()
      }
      SparkEnv.set(null)
    } finally {
      super.afterAll()
    }
  }

  def createRpcEnv(conf: SparkConf, name: String, port: Int, clientMode: Boolean = false): RpcEnv

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

    val anotherEnv = createRpcEnv(new SparkConf(), "remote", 0, clientMode = true)
    // Use anotherEnv to find out the RpcEndpointRef
    val rpcEndpointRef = anotherEnv.setupEndpointRef(env.address, "send-remotely")
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
    val newRpcEndpointRef = rpcEndpointRef.askSync[RpcEndpointRef]("Hello")
    val reply = newRpcEndpointRef.askSync[String]("Echo")
    assert("Echo" === reply)
  }

  test("ask a message locally") {
    val rpcEndpointRef = env.setupEndpoint("ask-locally", new RpcEndpoint {
      override val rpcEnv = env

      override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
        case msg: String =>
          context.reply(msg)
      }
    })
    val reply = rpcEndpointRef.askSync[String]("hello")
    assert("hello" === reply)
  }

  test("ask a message remotely") {
    env.setupEndpoint("ask-remotely", new RpcEndpoint {
      override val rpcEnv = env

      override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
        case msg: String =>
          context.reply(msg)
      }
    })

    val anotherEnv = createRpcEnv(new SparkConf(), "remote", 0, clientMode = true)
    // Use anotherEnv to find out the RpcEndpointRef
    val rpcEndpointRef = anotherEnv.setupEndpointRef(env.address, "ask-remotely")
    try {
      val reply = rpcEndpointRef.askSync[String]("hello")
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
        case msg: String =>
          Thread.sleep(100)
          context.reply(msg)
      }
    })

    val conf = new SparkConf()
    val shortProp = "spark.rpc.short.timeout"
    conf.set("spark.rpc.retry.wait", "0")
    conf.set("spark.rpc.numRetries", "1")
    val anotherEnv = createRpcEnv(conf, "remote", 0, clientMode = true)
    // Use anotherEnv to find out the RpcEndpointRef
    val rpcEndpointRef = anotherEnv.setupEndpointRef(env.address, "ask-timeout")
    try {
      val e = intercept[RpcTimeoutException] {
        rpcEndpointRef.askSync[String]("hello", new RpcTimeout(1 millis, shortProp))
      }
      // The SparkException cause should be a RpcTimeoutException with message indicating the
      // controlling timeout property
      assert(e.isInstanceOf[RpcTimeoutException])
      assert(e.getMessage.contains(shortProp))
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
        case m =>
          self
          callSelfSuccessfully = true
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
    val ack = ThreadUtils.awaitResult(f, 5 seconds)
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

    val anotherEnv = createRpcEnv(new SparkConf(), "remote", 0, clientMode = true)
    // Use anotherEnv to find out the RpcEndpointRef
    val rpcEndpointRef = anotherEnv.setupEndpointRef(env.address, "sendWithReply-remotely")
    try {
      val f = rpcEndpointRef.ask[String]("hello")
      val ack = ThreadUtils.awaitResult(f, 5 seconds)
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
      ThreadUtils.awaitResult(f, 5 seconds)
    }
    assert("Oops" === e.getCause.getMessage)

    env.stop(endpointRef)
  }

  test("sendWithReply: remotely error") {
    env.setupEndpoint("sendWithReply-remotely-error", new RpcEndpoint {
      override val rpcEnv = env

      override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
        case msg: String => context.sendFailure(new SparkException("Oops"))
      }
    })

    val anotherEnv = createRpcEnv(new SparkConf(), "remote", 0, clientMode = true)
    // Use anotherEnv to find out the RpcEndpointRef
    val rpcEndpointRef = anotherEnv.setupEndpointRef(env.address, "sendWithReply-remotely-error")
    try {
      val f = rpcEndpointRef.ask[String]("hello")
      val e = intercept[SparkException] {
        ThreadUtils.awaitResult(f, 5 seconds)
      }
      assert("Oops" === e.getCause.getMessage)
    } finally {
      anotherEnv.shutdown()
      anotherEnv.awaitTermination()
    }
  }

  /**
   * Setup an [[RpcEndpoint]] to collect all network events.
   *
   * @return the [[RpcEndpointRef]] and a `ConcurrentLinkedQueue` that contains network events.
   */
  private def setupNetworkEndpoint(
      _env: RpcEnv,
      name: String): (RpcEndpointRef, ConcurrentLinkedQueue[(Any, Any)]) = {
    val events = new ConcurrentLinkedQueue[(Any, Any)]
    val ref = _env.setupEndpoint("network-events-non-client", new ThreadSafeRpcEndpoint {
      override val rpcEnv = _env

      override def receive: PartialFunction[Any, Unit] = {
        case "hello" =>
        case m => events.add("receive" -> m)
      }

      override def onConnected(remoteAddress: RpcAddress): Unit = {
        events.add("onConnected" -> remoteAddress)
      }

      override def onDisconnected(remoteAddress: RpcAddress): Unit = {
        events.add("onDisconnected" -> remoteAddress)
      }

      override def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit = {
        events.add("onNetworkError" -> remoteAddress)
      }

    })
    (ref, events)
  }

  test("network events in sever RpcEnv when another RpcEnv is in server mode") {
    val serverEnv1 = createRpcEnv(new SparkConf(), "server1", 0, clientMode = false)
    val serverEnv2 = createRpcEnv(new SparkConf(), "server2", 0, clientMode = false)
    val (_, events) = setupNetworkEndpoint(serverEnv1, "network-events")
    val (serverRef2, _) = setupNetworkEndpoint(serverEnv2, "network-events")
    try {
      val serverRefInServer2 = serverEnv1.setupEndpointRef(serverRef2.address, serverRef2.name)
      // Send a message to set up the connection
      serverRefInServer2.send("hello")

      eventually(timeout(5 seconds), interval(5 millis)) {
        assert(events.contains(("onConnected", serverEnv2.address)))
      }

      serverEnv2.shutdown()
      serverEnv2.awaitTermination()

      eventually(timeout(5 seconds), interval(5 millis)) {
        assert(events.contains(("onConnected", serverEnv2.address)))
        assert(events.contains(("onDisconnected", serverEnv2.address)))
      }
    } finally {
      serverEnv1.shutdown()
      serverEnv2.shutdown()
      serverEnv1.awaitTermination()
      serverEnv2.awaitTermination()
    }
  }

  test("network events in sever RpcEnv when another RpcEnv is in client mode") {
    val serverEnv = createRpcEnv(new SparkConf(), "server", 0, clientMode = false)
    val (serverRef, events) = setupNetworkEndpoint(serverEnv, "network-events")
    val clientEnv = createRpcEnv(new SparkConf(), "client", 0, clientMode = true)
    try {
      val serverRefInClient = clientEnv.setupEndpointRef(serverRef.address, serverRef.name)
      // Send a message to set up the connection
      serverRefInClient.send("hello")

      eventually(timeout(5 seconds), interval(5 millis)) {
        // We don't know the exact client address but at least we can verify the message type
        assert(events.asScala.map(_._1).exists(_ == "onConnected"))
      }

      clientEnv.shutdown()
      clientEnv.awaitTermination()

      eventually(timeout(5 seconds), interval(5 millis)) {
        // We don't know the exact client address but at least we can verify the message type
        assert(events.asScala.map(_._1).exists(_ == "onConnected"))
        assert(events.asScala.map(_._1).exists(_ == "onDisconnected"))
      }
    } finally {
      clientEnv.shutdown()
      serverEnv.shutdown()
      clientEnv.awaitTermination()
      serverEnv.awaitTermination()
    }
  }

  test("network events in client RpcEnv when another RpcEnv is in server mode") {
    val clientEnv = createRpcEnv(new SparkConf(), "client", 0, clientMode = true)
    val serverEnv = createRpcEnv(new SparkConf(), "server", 0, clientMode = false)
    val (_, events) = setupNetworkEndpoint(clientEnv, "network-events")
    val (serverRef, _) = setupNetworkEndpoint(serverEnv, "network-events")
    try {
      val serverRefInClient = clientEnv.setupEndpointRef(serverRef.address, serverRef.name)
      // Send a message to set up the connection
      serverRefInClient.send("hello")

      eventually(timeout(5 seconds), interval(5 millis)) {
        assert(events.contains(("onConnected", serverEnv.address)))
      }

      serverEnv.shutdown()
      serverEnv.awaitTermination()

      eventually(timeout(5 seconds), interval(5 millis)) {
        assert(events.contains(("onConnected", serverEnv.address)))
        assert(events.contains(("onDisconnected", serverEnv.address)))
      }
    } finally {
      clientEnv.shutdown()
      serverEnv.shutdown()
      clientEnv.awaitTermination()
      serverEnv.awaitTermination()
    }
  }

  test("sendWithReply: unserializable error") {
    env.setupEndpoint("sendWithReply-unserializable-error", new RpcEndpoint {
      override val rpcEnv = env

      override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
        case msg: String => context.sendFailure(new UnserializableException)
      }
    })

    val anotherEnv = createRpcEnv(new SparkConf(), "remote", 0, clientMode = true)
    // Use anotherEnv to find out the RpcEndpointRef
    val rpcEndpointRef =
      anotherEnv.setupEndpointRef(env.address, "sendWithReply-unserializable-error")
    try {
      val f = rpcEndpointRef.ask[String]("hello")
      val e = intercept[SparkException] {
        ThreadUtils.awaitResult(f, 1 seconds)
      }
      assert(e.getCause.isInstanceOf[NotSerializableException])
    } finally {
      anotherEnv.shutdown()
      anotherEnv.awaitTermination()
    }
  }

  test("port conflict") {
    val anotherEnv = createRpcEnv(new SparkConf(), "remote", env.address.port)
    assert(anotherEnv.address.port != env.address.port)
  }

  private def testSend(conf: SparkConf): Unit = {
    val localEnv = createRpcEnv(conf, "authentication-local", 0)
    val remoteEnv = createRpcEnv(conf, "authentication-remote", 0, clientMode = true)

    try {
      @volatile var message: String = null
      localEnv.setupEndpoint("send-authentication", new RpcEndpoint {
        override val rpcEnv = localEnv

        override def receive: PartialFunction[Any, Unit] = {
          case msg: String => message = msg
        }
      })
      val rpcEndpointRef = remoteEnv.setupEndpointRef(localEnv.address, "send-authentication")
      rpcEndpointRef.send("hello")
      eventually(timeout(5 seconds), interval(10 millis)) {
        assert("hello" === message)
      }
    } finally {
      localEnv.shutdown()
      localEnv.awaitTermination()
      remoteEnv.shutdown()
      remoteEnv.awaitTermination()
    }
  }

  private def testAsk(conf: SparkConf): Unit = {
    val localEnv = createRpcEnv(conf, "authentication-local", 0)
    val remoteEnv = createRpcEnv(conf, "authentication-remote", 0, clientMode = true)

    try {
      localEnv.setupEndpoint("ask-authentication", new RpcEndpoint {
        override val rpcEnv = localEnv

        override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
          case msg: String =>
            context.reply(msg)
        }
      })
      val rpcEndpointRef = remoteEnv.setupEndpointRef(localEnv.address, "ask-authentication")
      val reply = rpcEndpointRef.askSync[String]("hello")
      assert("hello" === reply)
    } finally {
      localEnv.shutdown()
      localEnv.awaitTermination()
      remoteEnv.shutdown()
      remoteEnv.awaitTermination()
    }
  }

  test("send with authentication") {
    testSend(new SparkConf()
      .set("spark.authenticate", "true")
      .set("spark.authenticate.secret", "good"))
  }

  test("send with SASL encryption") {
    testSend(new SparkConf()
      .set("spark.authenticate", "true")
      .set("spark.authenticate.secret", "good")
      .set("spark.authenticate.enableSaslEncryption", "true"))
  }

  test("send with AES encryption") {
    testSend(new SparkConf()
      .set("spark.authenticate", "true")
      .set("spark.authenticate.secret", "good")
      .set("spark.network.crypto.enabled", "true")
      .set("spark.network.crypto.saslFallback", "false"))
  }

  test("ask with authentication") {
    testAsk(new SparkConf()
      .set("spark.authenticate", "true")
      .set("spark.authenticate.secret", "good"))
  }

  test("ask with SASL encryption") {
    testAsk(new SparkConf()
      .set("spark.authenticate", "true")
      .set("spark.authenticate.secret", "good")
      .set("spark.authenticate.enableSaslEncryption", "true"))
  }

  test("ask with AES encryption") {
    testAsk(new SparkConf()
      .set("spark.authenticate", "true")
      .set("spark.authenticate.secret", "good")
      .set("spark.network.crypto.enabled", "true")
      .set("spark.network.crypto.saslFallback", "false"))
  }

  test("construct RpcTimeout with conf property") {
    val conf = new SparkConf

    val testProp = "spark.ask.test.timeout"
    val testDurationSeconds = 30
    val secondaryProp = "spark.ask.secondary.timeout"

    conf.set(testProp, s"${testDurationSeconds}s")
    conf.set(secondaryProp, "100s")

    // Construct RpcTimeout with a single property
    val rt1 = RpcTimeout(conf, testProp)
    assert( testDurationSeconds === rt1.duration.toSeconds )

    // Construct RpcTimeout with prioritized list of properties
    val rt2 = RpcTimeout(conf, Seq("spark.ask.invalid.timeout", testProp, secondaryProp), "1s")
    assert( testDurationSeconds === rt2.duration.toSeconds )

    // Construct RpcTimeout with default value,
    val defaultProp = "spark.ask.default.timeout"
    val defaultDurationSeconds = 1
    val rt3 = RpcTimeout(conf, Seq(defaultProp), defaultDurationSeconds.toString + "s")
    assert( defaultDurationSeconds === rt3.duration.toSeconds )
    assert( rt3.timeoutProp.contains(defaultProp) )

    // Try to construct RpcTimeout with an unconfigured property
    intercept[NoSuchElementException] {
      RpcTimeout(conf, "spark.ask.invalid.timeout")
    }
  }

  test("ask a message timeout on Future using RpcTimeout") {
    case class NeverReply(msg: String)

    val rpcEndpointRef = env.setupEndpoint("ask-future", new RpcEndpoint {
      override val rpcEnv = env

      override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
        case msg: String => context.reply(msg)
        case _: NeverReply =>
      }
    })

    val longTimeout = new RpcTimeout(1 second, "spark.rpc.long.timeout")
    val shortTimeout = new RpcTimeout(10 millis, "spark.rpc.short.timeout")

    // Ask with immediate response, should complete successfully
    val fut1 = rpcEndpointRef.ask[String]("hello", longTimeout)
    val reply1 = longTimeout.awaitResult(fut1)
    assert("hello" === reply1)

    // Ask with a delayed response and wait for response immediately that should timeout
    val fut2 = rpcEndpointRef.ask[String](NeverReply("doh"), shortTimeout)
    val reply2 =
      intercept[RpcTimeoutException] {
        shortTimeout.awaitResult(fut2)
      }.getMessage

    // RpcTimeout.awaitResult should have added the property to the TimeoutException message
    assert(reply2.contains(shortTimeout.timeoutProp))

    // Ask with delayed response and allow the Future to timeout before ThreadUtils.awaitResult
    val fut3 = rpcEndpointRef.ask[String](NeverReply("goodbye"), shortTimeout)

    // scalastyle:off awaitresult
    // Allow future to complete with failure using plain Await.result, this will return
    // once the future is complete to verify addMessageIfTimeout was invoked
    val reply3 =
      intercept[RpcTimeoutException] {
        Await.result(fut3, 2000 millis)
      }.getMessage
    // scalastyle:on awaitresult

    // When the future timed out, the recover callback should have used
    // RpcTimeout.addMessageIfTimeout to add the property to the TimeoutException message
    assert(reply3.contains(shortTimeout.timeoutProp))

    // Use RpcTimeout.awaitResult to process Future, since it has already failed with
    // RpcTimeoutException, the same RpcTimeoutException should be thrown
    val reply4 =
      intercept[RpcTimeoutException] {
        shortTimeout.awaitResult(fut3)
      }.getMessage

    // Ensure description is not in message twice after addMessageIfTimeout and awaitResult
    assert(shortTimeout.timeoutProp.r.findAllIn(reply4).length === 1)
  }

  test("file server") {
    val conf = new SparkConf()
    val tempDir = Utils.createTempDir()
    val file = new File(tempDir, "file")
    Files.write(UUID.randomUUID().toString(), file, UTF_8)
    val fileWithSpecialChars = new File(tempDir, "file name")
    Files.write(UUID.randomUUID().toString(), fileWithSpecialChars, UTF_8)
    val empty = new File(tempDir, "empty")
    Files.write("", empty, UTF_8);
    val jar = new File(tempDir, "jar")
    Files.write(UUID.randomUUID().toString(), jar, UTF_8)

    val dir1 = new File(tempDir, "dir1")
    assert(dir1.mkdir())
    val subFile1 = new File(dir1, "file1")
    Files.write(UUID.randomUUID().toString(), subFile1, UTF_8)

    val dir2 = new File(tempDir, "dir2")
    assert(dir2.mkdir())
    val subFile2 = new File(dir2, "file2")
    Files.write(UUID.randomUUID().toString(), subFile2, UTF_8)

    val fileUri = env.fileServer.addFile(file)
    val fileWithSpecialCharsUri = env.fileServer.addFile(fileWithSpecialChars)
    val emptyUri = env.fileServer.addFile(empty)
    val jarUri = env.fileServer.addJar(jar)
    val dir1Uri = env.fileServer.addDirectory("/dir1", dir1)
    val dir2Uri = env.fileServer.addDirectory("/dir2", dir2)

    // Try registering directories with invalid names.
    Seq("/files", "/jars").foreach { uri =>
      intercept[IllegalArgumentException] {
        env.fileServer.addDirectory(uri, dir1)
      }
    }

    val destDir = Utils.createTempDir()
    val sm = new SecurityManager(conf)
    val hc = SparkHadoopUtil.get.conf

    val files = Seq(
      (file, fileUri),
      (fileWithSpecialChars, fileWithSpecialCharsUri),
      (empty, emptyUri),
      (jar, jarUri),
      (subFile1, dir1Uri + "/file1"),
      (subFile2, dir2Uri + "/file2"))
    files.foreach { case (f, uri) =>
      val destFile = new File(destDir, f.getName())
      Utils.fetchFile(uri, destDir, conf, sm, hc, 0L, false)
      assert(Files.equal(f, destFile))
    }

    // Try to download files that do not exist.
    Seq("files", "jars", "dir1").foreach { root =>
      intercept[Exception] {
        val uri = env.address.toSparkURL + s"/$root/doesNotExist"
        Utils.fetchFile(uri, destDir, conf, sm, hc, 0L, false)
      }
    }
  }

  test("SPARK-14699: RpcEnv.shutdown should not fire onDisconnected events") {
    env.setupEndpoint("SPARK-14699", new RpcEndpoint {
      override val rpcEnv: RpcEnv = env

      override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
        case m => context.reply(m)
      }
    })

    val anotherEnv = createRpcEnv(new SparkConf(), "remote", 0)
    val endpoint = mock(classOf[RpcEndpoint])
    anotherEnv.setupEndpoint("SPARK-14699", endpoint)

    val ref = anotherEnv.setupEndpointRef(env.address, "SPARK-14699")
    // Make sure the connect is set up
    assert(ref.askSync[String]("hello") === "hello")
    anotherEnv.shutdown()
    anotherEnv.awaitTermination()

    env.stop(ref)

    verify(endpoint).onStop()
    verify(endpoint, never()).onDisconnected(any())
    verify(endpoint, never()).onNetworkError(any(), any())
  }
}

class UnserializableClass

class UnserializableException extends Exception {
  private val unserializableField = new UnserializableClass
}
