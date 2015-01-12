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

import scala.concurrent.duration._
import scala.language.postfixOps

import org.apache.activemq.broker.{TransportConnector, BrokerService}
import org.eclipse.paho.client.mqttv3._
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.concurrent.Eventually

import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.SparkConf
import org.apache.spark.util.Utils

class MQTTStreamSuite extends FunSuite with Eventually with BeforeAndAfter {

  private val batchDuration = Milliseconds(500)
  private val master: String = "local[2]"
  private val framework: String = this.getClass.getSimpleName
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
    val receiveStream: ReceiverInputDStream[String] =
      MQTTUtils.createStream(ssc, "tcp:" + brokerUri, topic, StorageLevel.MEMORY_ONLY)
    var receiveMessage: List[String] = List()
    receiveStream.foreachRDD { rdd =>
      if (rdd.collect.length > 0) {
        receiveMessage = receiveMessage ::: List(rdd.first)
        receiveMessage
      }
    }
    ssc.start()
    publishData(sendMessage)
    eventually(timeout(10000 milliseconds), interval(100 milliseconds)) {
      assert(sendMessage.equals(receiveMessage(0)))
    }
    ssc.stop()
  }

  private def setupMQTT() {
    broker = new BrokerService()
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
    Utils.startServiceOnPort(23456, (trialPort: Int) => {
      val socket = new ServerSocket(trialPort)
      socket.close()
      (null, trialPort)
    }, new SparkConf())._2
  }

  def publishData(data: String): Unit = {
    var client: MqttClient = null
    try {
      val persistence: MqttClientPersistence = new MqttDefaultFilePersistence(persistenceDir.getAbsolutePath)
      client = new MqttClient("tcp:" + brokerUri, MqttClient.generateClientId(), persistence)
      client.connect()
      if (client.isConnected) {
        val msgTopic: MqttTopic = client.getTopic(topic)
        val message: MqttMessage = new MqttMessage(data.getBytes("utf-8"))
        message.setQos(1)
        message.setRetained(true)
        for (i <- 0 to 100) {
          msgTopic.publish(message)
        }
      }
    } finally {
      client.disconnect()
      client.close()
      client = null
    }
  }
}
