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

import org.apache.spark.streaming.{StreamingContext, TestSuiteBase}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream

class MQTTStreamSuite extends TestSuiteBase {

  test("mqtt input stream") {
    val ssc = new StreamingContext(master, framework, batchDuration)
    val brokerUrl = "abc"
    val topic = "def"

    // tests the API, does not actually test data receiving
    val test1: ReceiverInputDStream[String] = MQTTUtils.createStream(ssc, brokerUrl, topic)
    val test2: ReceiverInputDStream[String] =
      MQTTUtils.createStream(ssc, brokerUrl, topic, StorageLevel.MEMORY_AND_DISK_SER_2)

    // TODO: Actually test receiving data
    ssc.stop()
  }
}
