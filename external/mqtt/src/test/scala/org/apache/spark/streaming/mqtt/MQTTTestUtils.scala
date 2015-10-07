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

import java.net.{ServerSocket, URI}

import scala.language.postfixOps

import com.google.common.base.Charsets.UTF_8
import org.apache.activemq.broker.{BrokerService, TransportConnector}
import org.apache.commons.lang3.RandomUtils
import org.eclipse.paho.client.mqttv3._
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence

import org.apache.spark.util.Utils
import org.apache.spark.{Logging, SparkConf}

/**
 * Share codes for Scala and Python unit tests
 */
private[mqtt] class MQTTTestUtils extends Logging {

  private val persistenceDir = Utils.createTempDir()
  private val brokerHost = "localhost"
  private val brokerPort = findFreePort()

  private var broker: BrokerService = _
  private var connector: TransportConnector = _

  def brokerUri: String = {
    s"$brokerHost:$brokerPort"
  }

  def setup(): Unit = {
    broker = new BrokerService()
    broker.setDataDirectoryFile(Utils.createTempDir())
    connector = new TransportConnector()
    connector.setName("mqtt")
    connector.setUri(new URI("mqtt://" + brokerUri))
    broker.addConnector(connector)
    broker.start()
  }

  def teardown(): Unit = {
    if (broker != null) {
      broker.stop()
      broker = null
    }
    if (connector != null) {
      connector.stop()
      connector = null
    }
    Utils.deleteRecursively(persistenceDir)
  }

  private def findFreePort(): Int = {
    val candidatePort = RandomUtils.nextInt(1024, 65536)
    Utils.startServiceOnPort(candidatePort, (trialPort: Int) => {
      val socket = new ServerSocket(trialPort)
      socket.close()
      (null, trialPort)
    }, new SparkConf())._2
  }

  def publishData(topic: String, data: String): Unit = {
    var client: MqttClient = null
    try {
      val persistence = new MqttDefaultFilePersistence(persistenceDir.getAbsolutePath)
      client = new MqttClient("tcp://" + brokerUri, MqttClient.generateClientId(), persistence)
      client.connect()
      if (client.isConnected) {
        val msgTopic = client.getTopic(topic)
        val message = new MqttMessage(data.getBytes(UTF_8))
        message.setQos(1)
        message.setRetained(true)

        for (i <- 0 to 10) {
          try {
            msgTopic.publish(message)
          } catch {
            case e: MqttException if e.getReasonCode == MqttException.REASON_CODE_MAX_INFLIGHT =>
              // wait for Spark streaming to consume something from the message queue
              Thread.sleep(50)
          }
        }
      }
    } finally {
      if (client != null) {
        client.disconnect()
        client.close()
        client = null
      }
    }
  }

}
