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

package org.apache.spark.network

import java.nio._

import org.apache.spark.{SecurityManager, SparkConf}
import org.scalatest.FunSuite

import scala.concurrent.{Await, TimeoutException}
import scala.concurrent.duration._
import scala.language.postfixOps

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
    manager.sendMessageReliablySync(manager.id, bufferMessage)

    assert(receivedMessage == true)

    manager.stop()
  }

  test("security on same password") {
    val conf = new SparkConf
    conf.set("spark.authenticate", "true")
    conf.set("spark.authenticate.secret", "good")
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
      manager.sendMessageReliablySync(managerServer.id, bufferMessage)
    })

    assert(numReceivedServerMessages == 10)
    assert(numReceivedMessages == 0)

    manager.stop()
    managerServer.stop()
  }

  test("security mismatch password") {
    val conf = new SparkConf
    conf.set("spark.authenticate", "true")
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
    badconf.set("spark.authenticate.secret", "bad")
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
    manager.sendMessageReliablySync(managerServer.id, bufferMessage)

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
        if (!g.isDefined) assert(false) else assert(true)
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



}

