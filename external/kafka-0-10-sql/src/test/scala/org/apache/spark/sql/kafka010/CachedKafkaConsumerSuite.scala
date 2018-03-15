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

package org.apache.spark.sql.kafka010

import java.util

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords}
import org.apache.kafka.common.TopicPartition
import org.scalatest.PrivateMethodTester

import org.apache.spark.sql.test.SharedSQLContext

class CachedKafkaConsumerSuite extends SharedSQLContext with PrivateMethodTester {

  test("SPARK-19886: Report error cause correctly in reportDataLoss") {
    val cause = new Exception("D'oh!")
    val reportDataLoss = PrivateMethod[Unit]('reportDataLoss0)
    val e = intercept[IllegalStateException] {
      CachedKafkaConsumer.invokePrivate(reportDataLoss(true, "message", cause))
    }
    assert(e.getCause === cause)
  }

  test("SPARK: Report error cause correctly in ") {
    val topicPartition = new TopicPartition("testTopic", 0)
    val data = new util.ArrayList[ConsumerRecord[String, String]]
    data.add(new ConsumerRecord("testTopic", 0, 1L, "mykey", "myvalue1"))
    data.add(new ConsumerRecord("testTopic", 0, 2L, "mykey", "myvalue2"))
    data.add(new ConsumerRecord("testTopic", 0, 3L, "mykey", "myvalue3"))
    data.add(new ConsumerRecord("testTopic", 0, 5L, "mykey", "myvalue5"))
    data.add(new ConsumerRecord("testTopic", 0, 6L, "mykey", "myvalue6"))

    var recordsMap = new java.util.HashMap[TopicPartition,
      util.List[ConsumerRecord[String, String]]]
    recordsMap.put(topicPartition, data)
    var consumerRecords = new ConsumerRecords(recordsMap)
    var fetchedData = consumerRecords.iterator

    var map = new java.util.HashMap[String, Object]
    map.put("group.id", "groupid")
    map.put("bootstrap.servers", "http://127.0.0.1:8080")
    map.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    map.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val ru = scala.reflect.runtime.universe
    val m = ru.runtimeMirror(getClass.getClassLoader)

    val consumer = CachedKafkaConsumer(topicPartition, map)
    val im = m.reflect(consumer)
    val methodX = ru.typeOf[CachedKafkaConsumer].decl(ru.TermName("fetchData")).asMethod
    val mm = im.reflectMethod(methodX)

    val fieldX = ru.typeOf[CachedKafkaConsumer].decl(ru.TermName("fetchedData"))
      .asTerm.accessed.asTerm
    val fmX = im.reflectField(fieldX)
    fmX.set(fetchedData)

    consumer.nextOffsetInFetchedData = 0L
    val record = mm(0L, 4L, 100L, true)
    assert(data.get(0) === record)

  }
}
