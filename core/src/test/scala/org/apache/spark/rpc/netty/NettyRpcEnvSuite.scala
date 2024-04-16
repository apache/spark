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

import java.util.concurrent.ExecutionException

import scala.concurrent.duration._

import org.scalatest.concurrent.{Signaler, ThreadSignaler, TimeLimits}
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark._
import org.apache.spark.network.client.TransportClient
import org.apache.spark.rpc._
import org.apache.spark.util.{SslTestUtils, ThreadUtils}

class NettyRpcEnvSuite extends RpcEnvSuite with MockitoSugar with TimeLimits {

  private implicit val signaler: Signaler = ThreadSignaler

  override def createRpcEnv(
      conf: SparkConf,
      name: String,
      port: Int,
      clientMode: Boolean = false): RpcEnv = {
    val config = RpcEnvConfig(conf, "test", "localhost", "localhost", port,
      new SecurityManager(conf), 0, clientMode)
    new NettyRpcEnvFactory().create(config)
  }

  test("non-existent endpoint") {
    val uri = RpcEndpointAddress(env.address, "nonexist-endpoint").toString
    val e = intercept[SparkException] {
      env.setupEndpointRef(env.address, "nonexist-endpoint")
    }
    assert(e.getCause.isInstanceOf[RpcEndpointNotFoundException])
    assert(e.getCause.getMessage.contains(uri))
  }

  test("advertise address different from bind address") {
    val sparkConf = createSparkConf()
    val config = RpcEnvConfig(sparkConf, "test", "localhost", "example.com", 0,
      new SecurityManager(sparkConf), 0, false)
    val env = new NettyRpcEnvFactory().create(config)
    try {
      assert(env.address.hostPort.startsWith("example.com:"))
    } finally {
      env.shutdown()
    }
  }

  test("RequestMessage serialization") {
    def assertRequestMessageEquals(expected: RequestMessage, actual: RequestMessage): Unit = {
      assert(expected.senderAddress === actual.senderAddress)
      assert(expected.receiver === actual.receiver)
      assert(expected.content === actual.content)
    }

    val nettyEnv = env.asInstanceOf[NettyRpcEnv]
    val client = mock[TransportClient]
    val senderAddress = RpcAddress("localhost", 12345)
    val receiverAddress = RpcEndpointAddress("localhost", 54321, "test")
    val receiver = new NettyRpcEndpointRef(nettyEnv.conf, receiverAddress, nettyEnv)

    val msg = new RequestMessage(senderAddress, receiver, "foo")
    assertRequestMessageEquals(
      msg,
      RequestMessage(nettyEnv, client, msg.serialize(nettyEnv)))

    val msg2 = new RequestMessage(null, receiver, "foo")
    assertRequestMessageEquals(
      msg2,
      RequestMessage(nettyEnv, client, msg2.serialize(nettyEnv)))

    val msg3 = new RequestMessage(senderAddress, receiver, null)
    assertRequestMessageEquals(
      msg3,
      RequestMessage(nettyEnv, client, msg3.serialize(nettyEnv)))
  }

  test("StackOverflowError should be sent back and Dispatcher should survive") {
    val numUsableCores = 2
    val conf = createSparkConf()
    val config = RpcEnvConfig(
      conf,
      "test",
      "localhost",
      "localhost",
      0,
      new SecurityManager(conf),
      numUsableCores,
      clientMode = false)
    val anotherEnv = new NettyRpcEnvFactory().create(config)
    anotherEnv.setupEndpoint("StackOverflowError", new RpcEndpoint {
      override val rpcEnv = anotherEnv

      override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
        // scalastyle:off throwerror
        case msg: String => throw new StackOverflowError
        // scalastyle:on throwerror
        case num: Int => context.reply(num)
      }
    })

    val rpcEndpointRef = env.setupEndpointRef(anotherEnv.address, "StackOverflowError")
    try {
      // Send `numUsableCores` messages to trigger `numUsableCores` `StackOverflowError`s
      for (_ <- 0 until numUsableCores) {
        val e = intercept[SparkException] {
          rpcEndpointRef.askSync[String]("hello")
        }
        // The root cause `e.getCause.getCause` because it is boxed by Scala Promise.
        assert(e.getCause.isInstanceOf[ExecutionException])
        assert(e.getCause.getCause.isInstanceOf[StackOverflowError])
      }
      failAfter(10.seconds) {
        assert(rpcEndpointRef.askSync[Int](100) === 100)
      }
    } finally {
      anotherEnv.shutdown()
      anotherEnv.awaitTermination()
    }
  }


  test("SPARK-31233: ask rpcEndpointRef in client mode timeout") {
    var remoteRef: RpcEndpointRef = null
    env.setupEndpoint("ask-remotely-server", new RpcEndpoint {
      override val rpcEnv = env
      override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
        case Register(ref) =>
          remoteRef = ref
          context.reply("okay")
        case msg: String =>
          context.reply(msg)
      }
    })
    val conf = createSparkConf()
    val anotherEnv = createRpcEnv(conf, "remote", 0, clientMode = true)
    // Use anotherEnv to find out the RpcEndpointRef
    val rpcEndpointRef = anotherEnv.setupEndpointRef(env.address, "ask-remotely-server")
    // Register a rpcEndpointRef in anotherEnv
    val anotherRef = anotherEnv.setupEndpoint("receiver", new RpcEndpoint {
      override val rpcEnv = anotherEnv
      override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
        case _ =>
          Thread.sleep(1200)
          context.reply("okay")
      }
    })
    try {
      val reply = rpcEndpointRef.askSync[String](Register(anotherRef))
      assert("okay" === reply)
      val timeout = "1s"
      val answer = remoteRef.ask[String]("msg",
        RpcTimeout(conf, Seq("spark.rpc.askTimeout", "spark.network.timeout"), timeout))
      val thrown = intercept[RpcTimeoutException] {
        ThreadUtils.awaitResult(answer, Duration(1300, MILLISECONDS))
      }
      val remoteAddr = remoteRef.asInstanceOf[NettyRpcEndpointRef].client.getChannel.remoteAddress
      assert(thrown.getMessage.contains(remoteAddr.toString))
    } finally {
      anotherEnv.shutdown()
      anotherEnv.awaitTermination()
    }
  }
}

class SslNettyRpcEnvSuite extends NettyRpcEnvSuite with MockitoSugar with TimeLimits {
  override def createSparkConf(): SparkConf = {
    SslTestUtils.updateWithSSLConfig(super.createSparkConf())
  }
}
