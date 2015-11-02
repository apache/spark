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

import scala.concurrent.duration._
import scala.language.postfixOps

import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Eventually

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

class MQTTStreamSuite extends SparkFunSuite with Eventually with BeforeAndAfter {

  private val batchDuration = Milliseconds(500)
  private val master = "local[2]"
  private val framework = this.getClass.getSimpleName
  private val topic = "def"

  private var ssc: StreamingContext = _
  private var mqttTestUtils: MQTTTestUtils = _

  before {
    ssc = new StreamingContext(master, framework, batchDuration)
    mqttTestUtils = new MQTTTestUtils
    mqttTestUtils.setup()
  }

  after {
    if (ssc != null) {
      ssc.stop()
      ssc = null
    }
    if (mqttTestUtils != null) {
      mqttTestUtils.teardown()
      mqttTestUtils = null
    }
  }

  test("mqtt input stream") {
    val sendMessage = "MQTT demo for spark streaming"
    val receiveStream = MQTTUtils.createStream(ssc, "tcp://" + mqttTestUtils.brokerUri, topic,
      StorageLevel.MEMORY_ONLY)

    @volatile var receiveMessage: List[String] = List()
    receiveStream.foreachRDD { rdd =>
      if (rdd.collect.length > 0) {
        receiveMessage = receiveMessage ::: List(rdd.first)
        receiveMessage
      }
    }

    ssc.start()

    // Retry it because we don't know when the receiver will start.
    eventually(timeout(10000 milliseconds), interval(100 milliseconds)) {
      mqttTestUtils.publishData(topic, sendMessage)
      assert(sendMessage.equals(receiveMessage(0)))
    }
    ssc.stop()
  }
}
