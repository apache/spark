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

package org.apache.spark.streaming.mqtt

import java.net.{URI, ServerSocket}
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import scala.concurrent.duration._
import scala.language.postfixOps

import org.apache.activemq.broker.{TransportConnector, BrokerService}
import org.apache.commons.lang3.RandomUtils
import org.eclipse.paho.client.mqttv3._
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.concurrent.Eventually

import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.scheduler.StreamingListener
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStarted
import org.apache.spark.SparkConf
import org.apache.spark.util.Utils

class MQTTStreamSuite extends FunSuite with Eventually with BeforeAndAfter {

  private val batchDuration = Milliseconds(500)
  private val master = "local[2]"
  private val framework = this.getClass.getSimpleName
  private val freePort = findFreePort()
  private val brokerUri = "//localhost:" + freePort
  private val topic = "def"
  private val persistenceDir = Utils.createTempDir()

  private var ssc: StreamingContext = _
  private var broker: BrokerService = _
  private var connector: TransportConnector = _

  before {
    ssc = new StreamingContext(master, framework, batchDuration)
    setupMQTT()
  }

  after {
    if (ssc != null) {
      ssc.stop()
      ssc = null
    }
    Utils.deleteRecursively(persistenceDir)
    tearDownMQTT()
  }

  test("mqtt input stream") {
    val sendMessage = "MQTT demo for spark streaming"
    val receiveStream =
      MQTTUtils.createStream(ssc, "tcp:" + brokerUri, topic, StorageLevel.MEMORY_ONLY)
    @volatile var receiveMessage: List[String] = List()
    receiveStream.foreachRDD { rdd =>
      if (rdd.collect.length > 0) {
        receiveMessage = receiveMessage ::: List(rdd.first)
        receiveMessage
      }
    }
    ssc.start()

    // wait for the receiver to start before publishing data, or we risk failing
    // the test nondeterministically. See SPARK-4631
    waitForReceiverToStart()

    publishData(sendMessage)
    eventually(timeout(10000 milliseconds), interval(100 milliseconds)) {
      assert(sendMessage.equals(receiveMessage(0)))
    }
    ssc.stop()
  }

  private def setupMQTT() {
    broker = new BrokerService()
    broker.setDataDirectoryFile(Utils.createTempDir())
    connector = new TransportConnector()
    connector.setName("mqtt")
    connector.setUri(new URI("mqtt:" + brokerUri))
    broker.addConnector(connector)
    broker.start()
  }

  private def tearDownMQTT() {
    if (broker != null) {
      broker.stop()
      broker = null
    }
    if (connector != null) {
      connector.stop()
      connector = null
    }
  }

  private def findFreePort(): Int = {
    val candidatePort = RandomUtils.nextInt(1024, 65536)
    Utils.startServiceOnPort(candidatePort, (trialPort: Int) => {
      val socket = new ServerSocket(trialPort)
      socket.close()
      (null, trialPort)
    }, new SparkConf())._2
  }

  def publishData(data: String): Unit = {
    var client: MqttClient = null
    try {
      val persistence = new MqttDefaultFilePersistence(persistenceDir.getAbsolutePath)
      client = new MqttClient("tcp:" + brokerUri, MqttClient.generateClientId(), persistence)
      client.connect()
      if (client.isConnected) {
        val msgTopic = client.getTopic(topic)
        val message = new MqttMessage(data.getBytes("utf-8"))
        message.setQos(1)
        message.setRetained(true)

        for (i <- 0 to 10) {
          try {
            msgTopic.publish(message)
          } catch {
            case e: MqttException if e.getReasonCode == MqttException.REASON_CODE_MAX_INFLIGHT =>
              Thread.sleep(50) // wait for Spark streaming to consume something from the message queue
          }
        }
      }
    } finally {
      client.disconnect()
      client.close()
      client = null
    }
  }

  /**
   * Block until at least one receiver has started or timeout occurs.
   */
  private def waitForReceiverToStart() = {
    val latch = new CountDownLatch(1)
    ssc.addStreamingListener(new StreamingListener {
      override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted) {
        latch.countDown()
      }
    })

    assert(latch.await(10, TimeUnit.SECONDS), "Timeout waiting for receiver to start.")
  }
}
