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

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.concurrent.Eventually
import scala.concurrent.duration._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.eclipse.paho.client.mqttv3._
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence

class MQTTStreamSuite extends FunSuite with Eventually with BeforeAndAfter {

  private val batchDuration = Seconds(1)
  private val master: String = "local[2]"
  private val framework: String = this.getClass.getSimpleName
  private val brokerUrl = "tcp://localhost:1883"
  private val topic = "def"
  private var ssc: StreamingContext = _

  before {
    ssc = new StreamingContext(master, framework, batchDuration)
  }
  after {
    if (ssc != null) {
      ssc.stop()
      ssc = null
    }
  }

  test("mqtt input stream") {
    val sendMessage = "MQTT demo for spark streaming"
    publishData(sendMessage)
    val receiveStream: ReceiverInputDStream[String] =
      MQTTUtils.createStream(ssc, brokerUrl, topic, StorageLevel.MEMORY_AND_DISK_SER_2)
    var receiveMessage: String = ""
    receiveStream.foreachRDD { rdd =>
      receiveMessage = rdd.first
      receiveMessage
    }
    ssc.start()
    eventually(timeout(10000 milliseconds), interval(100 milliseconds)) {
      assert(sendMessage.equals(receiveMessage))
    }
    ssc.stop()
  }

  def publishData(sendMessage: String): Unit = {
    try {
      val persistence: MqttClientPersistence = new MqttDefaultFilePersistence("/tmp")
      val client: MqttClient = new MqttClient(brokerUrl, MqttClient.generateClientId(), persistence)
      client.connect()
      val msgTopic: MqttTopic = client.getTopic(topic)
      val message: MqttMessage = new MqttMessage(String.valueOf(sendMessage).getBytes("utf-8"))
      message.setQos(1)
      message.setRetained(true)
      msgTopic.publish(message)
      println("Published data \ntopic: " + msgTopic.getName() + "\nMessage: " + message)
      client.disconnect()
    } catch {
      case e: MqttException => println("Exception Caught: " + e)
    }
  }
}
