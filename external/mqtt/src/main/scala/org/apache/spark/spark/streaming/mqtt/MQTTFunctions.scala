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

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._

/**
 * Extra MQTT input stream functions available on [[org.apache.spark.streaming.StreamingContext]]
 * through implicit conversions. Import org.apache.spark.streaming.mqtt._ to use these functions.
 */
class MQTTFunctions(ssc: StreamingContext) {
  /**
   * Create an input stream that receives messages pushed by a MQTT publisher.
   * @param brokerUrl Url of remote MQTT publisher
   * @param topic topic name to subscribe to
   * @param storageLevel RDD storage level. Defaults to StorageLevel.MEMORY_AND_DISK_SER_2.
   */
  def mqttStream(
      brokerUrl: String,
      topic: String,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    ): DStream[String] = {
    val inputStream = new MQTTInputDStream[String](ssc, brokerUrl, topic, storageLevel)
    ssc.registerInputStream(inputStream)
    inputStream
  }
}
