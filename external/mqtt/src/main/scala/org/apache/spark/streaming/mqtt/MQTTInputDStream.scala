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

import scala.collection.Map
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import java.util.Properties
import java.util.concurrent.Executors
import java.io.IOException

import org.eclipse.paho.client.mqttv3.MqttCallback
import org.eclipse.paho.client.mqttv3.MqttClient
import org.eclipse.paho.client.mqttv3.MqttClientPersistence
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken
import org.eclipse.paho.client.mqttv3.MqttException
import org.eclipse.paho.client.mqttv3.MqttMessage
import org.eclipse.paho.client.mqttv3.MqttTopic

import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream._

/**
 * Input stream that subscribe messages from a Mqtt Broker.
 * Uses eclipse paho as MqttClient http://www.eclipse.org/paho/
 * @param brokerUrl Url of remote mqtt publisher
 * @param topic topic name to subscribe to
 * @param storageLevel RDD storage level.
 */

private[streaming] 
class MQTTInputDStream[T: ClassTag](
    @transient ssc_ : StreamingContext,
    brokerUrl: String,
    topic: String,
    storageLevel: StorageLevel
  ) extends NetworkInputDStream[T](ssc_) with Logging {
  
  def getReceiver(): NetworkReceiver[T] = {
    new MQTTReceiver(brokerUrl, topic, storageLevel).asInstanceOf[NetworkReceiver[T]]
  }
}

private[streaming] 
class MQTTReceiver(brokerUrl: String,
  topic: String,
  storageLevel: StorageLevel
  ) extends NetworkReceiver[Any] {
  lazy protected val blockGenerator = new BlockGenerator(storageLevel)
  
  def onStop() {
    blockGenerator.stop()
  }
  
  def onStart() {

    blockGenerator.start()

    // Set up persistence for messages 
    var peristance: MqttClientPersistence = new MemoryPersistence()

    // Initializing Mqtt Client specifying brokerUrl, clientID and MqttClientPersistance
    var client: MqttClient = new MqttClient(brokerUrl, MqttClient.generateClientId(), peristance)

    // Connect to MqttBroker    
    client.connect()

    // Subscribe to Mqtt topic
    client.subscribe(topic)

    // Callback automatically triggers as and when new message arrives on specified topic
    var callback: MqttCallback = new MqttCallback() {

      // Handles Mqtt message 
      override def messageArrived(arg0: String, arg1: MqttMessage) {
        blockGenerator += new String(arg1.getPayload())
      }

      override def deliveryComplete(arg0: IMqttDeliveryToken) {
      }

      override def connectionLost(arg0: Throwable) {
        logInfo("Connection lost " + arg0)
      }
    }

    // Set up callback for MqttClient
    client.setCallback(callback)
  }
}
