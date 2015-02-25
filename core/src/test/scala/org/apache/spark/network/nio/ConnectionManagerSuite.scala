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

package org.apache.spark.network.nio

import java.io.IOException
import java.nio._

import scala.concurrent.duration._
import scala.concurrent.{Await, TimeoutException}
import scala.language.postfixOps

import org.scalatest.FunSuite

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.util.Utils

/**
  * Test the ConnectionManager with various security settings.
  */
class ConnectionManagerSuite extends FunSuite {

  test("security default off") {
    val conf = new SparkConf
    val securityManager = new SecurityManager(conf)
    val manager = new ConnectionManager(0, conf, securityManager)
    var receivedMessage = false
    manager.onReceiveMessage((msg: Message, id: ConnectionManagerId) => {
      receivedMessage = true
      None
    })

    val size = 10 * 1024 * 1024
    val buffer = ByteBuffer.allocate(size).put(Array.tabulate[Byte](size)(x => x.toByte))
    buffer.flip

    val bufferMessage = Message.createBufferMessage(buffer.duplicate)
    Await.result(manager.sendMessageReliably(manager.id, bufferMessage), 10 seconds)

    assert(receivedMessage == true)

    manager.stop()
  }

  test("security on same password") {
    val conf = new SparkConf
    conf.set("spark.authenticate", "true")
    conf.set("spark.authenticate.secret", "good")
    conf.set("spark.app.id", "app-id")
    val securityManager = new SecurityManager(conf)
    val manager = new ConnectionManager(0, conf, securityManager)
    var numReceivedMessages = 0

    manager.onReceiveMessage((msg: Message, id: ConnectionManagerId) => {
      numReceivedMessages += 1
      None
    })
    val managerServer = new ConnectionManager(0, conf, securityManager)
    var numReceivedServerMessages = 0
    managerServer.onReceiveMessage((msg: Message, id: ConnectionManagerId) => {
      numReceivedServerMessages += 1
      None
    })

    val size = 10 * 1024 * 1024
    val count = 10
    val buffer = ByteBuffer.allocate(size).put(Array.tabulate[Byte](size)(x => x.toByte))
    buffer.flip

    (0 until count).map(i => {
      val bufferMessage = Message.createBufferMessage(buffer.duplicate)
      Await.result(manager.sendMessageReliably(managerServer.id, bufferMessage), 10 seconds)
    })

    assert(numReceivedServerMessages == 10)
    assert(numReceivedMessages == 0)

    manager.stop()
    managerServer.stop()
  }

  test("security mismatch password") {
    val conf = new SparkConf
    conf.set("spark.authenticate", "true")
    conf.set("spark.app.id", "app-id")
    conf.set("spark.authenticate.secret", "good")
    val securityManager = new SecurityManager(conf)
    val manager = new ConnectionManager(0, conf, securityManager)
    var numReceivedMessages = 0

    manager.onReceiveMessage((msg: Message, id: ConnectionManagerId) => {
      numReceivedMessages += 1
      None
    })

    val badconf = conf.clone.set("spark.authenticate.secret", "bad")
    val badsecurityManager = new SecurityManager(badconf)
    val managerServer = new ConnectionManager(0, badconf, badsecurityManager)
    var numReceivedServerMessages = 0

    managerServer.onReceiveMessage((msg: Message, id: ConnectionManagerId) => {
      numReceivedServerMessages += 1
      None
    })

    val size = 10 * 1024 * 1024
    val buffer = ByteBuffer.allocate(size).put(Array.tabulate[Byte](size)(x => x.toByte))
    buffer.flip
    val bufferMessage = Message.createBufferMessage(buffer.duplicate)
    // Expect managerServer to close connection, which we'll report as an error:
    intercept[IOException] {
      Await.result(manager.sendMessageReliably(managerServer.id, bufferMessage), 10 seconds)
    }

    assert(numReceivedServerMessages == 0)
    assert(numReceivedMessages == 0)

    manager.stop()
    managerServer.stop()
  }

  test("security mismatch auth off") {
    val conf = new SparkConf
    conf.set("spark.authenticate", "false")
    conf.set("spark.authenticate.secret", "good")
    val securityManager = new SecurityManager(conf)
    val manager = new ConnectionManager(0, conf, securityManager)
    var numReceivedMessages = 0

    manager.onReceiveMessage((msg: Message, id: ConnectionManagerId) => {
      numReceivedMessages += 1
      None
    })

    val badconf = new SparkConf
    badconf.set("spark.authenticate", "true")
    badconf.set("spark.authenticate.secret", "good")
    val badsecurityManager = new SecurityManager(badconf)
    val managerServer = new ConnectionManager(0, badconf, badsecurityManager)
    var numReceivedServerMessages = 0
    managerServer.onReceiveMessage((msg: Message, id: ConnectionManagerId) => {
      numReceivedServerMessages += 1
      None
    })

    val size = 10 * 1024 * 1024
    val buffer = ByteBuffer.allocate(size).put(Array.tabulate[Byte](size)(x => x.toByte))
    buffer.flip
    val bufferMessage = Message.createBufferMessage(buffer.duplicate)
    (0 until 1).map(i => {
      val bufferMessage = Message.createBufferMessage(buffer.duplicate)
      manager.sendMessageReliably(managerServer.id, bufferMessage)
    }).foreach(f => {
      try {
        val g = Await.result(f, 1 second)
        assert(false)
      } catch {
        case i: IOException =>
          assert(true)
        case e: TimeoutException => {
          // we should timeout here since the client can't do the negotiation
          assert(true)
        }
      }
    })

    assert(numReceivedServerMessages == 0)
    assert(numReceivedMessages == 0)
    manager.stop()
    managerServer.stop()
  }

  test("security auth off") {
    val conf = new SparkConf
    conf.set("spark.authenticate", "false")
    val securityManager = new SecurityManager(conf)
    val manager = new ConnectionManager(0, conf, securityManager)
    var numReceivedMessages = 0

    manager.onReceiveMessage((msg: Message, id: ConnectionManagerId) => {
      numReceivedMessages += 1
      None
    })

    val badconf = new SparkConf
    badconf.set("spark.authenticate", "false")
    val badsecurityManager = new SecurityManager(badconf)
    val managerServer = new ConnectionManager(0, badconf, badsecurityManager)
    var numReceivedServerMessages = 0

    managerServer.onReceiveMessage((msg: Message, id: ConnectionManagerId) => {
      numReceivedServerMessages += 1
      None
    })

    val size = 10 * 1024 * 1024
    val buffer = ByteBuffer.allocate(size).put(Array.tabulate[Byte](size)(x => x.toByte))
    buffer.flip
    val bufferMessage = Message.createBufferMessage(buffer.duplicate)
    (0 until 10).map(i => {
      val bufferMessage = Message.createBufferMessage(buffer.duplicate)
      manager.sendMessageReliably(managerServer.id, bufferMessage)
    }).foreach(f => {
      try {
        val g = Await.result(f, 1 second)
      } catch {
        case e: Exception => {
          assert(false)
        }
      }
    })
    assert(numReceivedServerMessages == 10)
    assert(numReceivedMessages == 0)

    manager.stop()
    managerServer.stop()
  }

  test("Ack error message") {
    val conf = new SparkConf
    conf.set("spark.authenticate", "false")
    val securityManager = new SecurityManager(conf)
    val manager = new ConnectionManager(0, conf, securityManager)
    val managerServer = new ConnectionManager(0, conf, securityManager)
    managerServer.onReceiveMessage((msg: Message, id: ConnectionManagerId) => {
      throw new Exception("Custom exception text")
    })

    val size = 10 * 1024 * 1024
    val buffer = ByteBuffer.allocate(size).put(Array.tabulate[Byte](size)(x => x.toByte))
    buffer.flip
    val bufferMessage = Message.createBufferMessage(buffer)

    val future = manager.sendMessageReliably(managerServer.id, bufferMessage)

    val exception = intercept[IOException] {
      Await.result(future, 1 second)
    }
    assert(Utils.exceptionString(exception).contains("Custom exception text"))

    manager.stop()
    managerServer.stop()

  }

  test("sendMessageReliably timeout") {
    val clientConf = new SparkConf
    clientConf.set("spark.authenticate", "false")
    val ackTimeout = 30
    clientConf.set("spark.core.connection.ack.wait.timeout", s"${ackTimeout}")

    val clientSecurityManager = new SecurityManager(clientConf)
    val manager = new ConnectionManager(0, clientConf, clientSecurityManager)

    val serverConf = new SparkConf
    serverConf.set("spark.authenticate", "false")
    val serverSecurityManager = new SecurityManager(serverConf)
    val managerServer = new ConnectionManager(0, serverConf, serverSecurityManager)
    managerServer.onReceiveMessage((msg: Message, id: ConnectionManagerId) => {
      // sleep 60 sec > ack timeout for simulating server slow down or hang up
      Thread.sleep(ackTimeout * 3 * 1000)
      None
    })

    val size = 10 * 1024 * 1024
    val buffer = ByteBuffer.allocate(size).put(Array.tabulate[Byte](size)(x => x.toByte))
    buffer.flip
    val bufferMessage = Message.createBufferMessage(buffer.duplicate)

    val future = manager.sendMessageReliably(managerServer.id, bufferMessage)

    // Future should throw IOException in 30 sec.
    // Otherwise TimeoutExcepton is thrown from Await.result.
    // We expect TimeoutException is not thrown.
    intercept[IOException] {
      Await.result(future, (ackTimeout * 2) second)
    }

    manager.stop()
    managerServer.stop()
  }

}

