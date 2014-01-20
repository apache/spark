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

package org.apache.spark.streaming.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.{StreamingContext, TestSuiteBase}
import org.apache.spark.storage.StorageLevel

class KafkaStreamSuite extends TestSuiteBase {

  test("kafka input stream") {
    val ssc = new StreamingContext(master, framework, batchDuration)
    val topics = Map("my-topic" -> 1)

    // tests the API, does not actually test data receiving
    val test1 = KafkaUtils.createStream(ssc, "localhost:1234", "group", topics)
    val test2 = KafkaUtils.createStream(ssc, "localhost:12345", "group", topics, StorageLevel.MEMORY_AND_DISK_SER_2)
    val kafkaParams = Map("zookeeper.connect"->"localhost:12345","group.id"->"consumer-group")
    val test3 = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics, StorageLevel.MEMORY_AND_DISK_SER_2)

    // TODO: Actually test receiving data
    ssc.stop()
  }
}
