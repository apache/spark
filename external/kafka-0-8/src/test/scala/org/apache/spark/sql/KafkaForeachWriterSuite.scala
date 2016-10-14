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

package org.apache.spark.sql

import java.util.Properties
import scala.collection.mutable.ArrayBuffer

import kafka.consumer.{ConsumerConfig, Consumer}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.streaming.kafka.KafkaTestUtils

class KafkaForeachWriterSuite
  extends SparkFunSuite
  with BeforeAndAfter
  with BeforeAndAfterAll
  with Eventually
  with SharedSQLContext{
  import testImplicits._

  private var kafkaTestUtils: KafkaTestUtils = _

  override def beforeAll {
    super.beforeAll
    kafkaTestUtils = new KafkaTestUtils
    kafkaTestUtils.setup()
  }

  override def afterAll {
    if (kafkaTestUtils != null) {
      kafkaTestUtils.teardown()
      kafkaTestUtils = null
    }
    super.afterAll
  }

  test("Basic usage: Write kafka without key"){
    val topic = "testWithoutKey"
    val topics = Set(topic)
    kafkaTestUtils.createTopic(topic)
    val topicCountMap = Map(topic -> 1)

    //create streaming query, write results to kafka
    val input = MemoryStream[String]
    val producerProps : Properties = new Properties()
    producerProps.setProperty("metadata.broker.list", kafkaTestUtils.brokerAddress)
    val query = input.toDF().writeStream
      .outputMode("append")
      .foreach(new KafkaForeachWriter[Row](producerProps, topics, input.toDF().schema))
      .start()
    val source = Array("A","B","C","E","F","G")
    input.addData(source)
    query.processAllAvailable()

    //create consumer, and consume data in kafka
    val consumerProps: Properties = new Properties()
    consumerProps.setProperty("zookeeper.connect", kafkaTestUtils.zkAddress)
    consumerProps.setProperty("auto.offset.reset", "smallest")
    consumerProps.setProperty("group.id", "spark")
    val connector = Consumer.create(new ConsumerConfig(consumerProps))
    val stream = connector.createMessageStreams(topicCountMap)
    val result = ArrayBuffer[String]()
    for (message <- stream.get(topic)) {
      message.map(kafkaStream => {
        val it = kafkaStream.iterator()
        while(result.size < source.length) {
          result += new String(it.next.message())
        }
      })
    }
    assert(result.synchronized { source === result.sorted.toArray })
  }


  test("Basic usage: Write kafka with key"){
    val topic = "testWithKey"
    kafkaTestUtils.createTopic(topic)
    val topics = Set(topic)
    val topicCountMap = Map(topic -> 1)

    //create streaming query, write results to kafka
    val input = MemoryStream[String]
    val producerProps : Properties = new Properties()
    producerProps.setProperty("metadata.broker.list", kafkaTestUtils.brokerAddress)
    val data = input.toDS()
      .map( word => (word,word))
      .as[(String,String)].toDF("key","value")
    val query = data.writeStream
      .outputMode("append")
      .foreach(new KafkaForeachWriter[Row](producerProps, topics, data.schema))
      .start()
    val source = Array("A","B","C")
    input.addData(source)
    query.processAllAvailable()

    //create consumer, and consume data in kafka
    val consumerProps: Properties = new Properties()
    consumerProps.setProperty("zookeeper.connect", kafkaTestUtils.zkAddress)
    consumerProps.setProperty("auto.offset.reset", "smallest")
    consumerProps.setProperty("group.id", "spark")
    val connector = Consumer.create(new ConsumerConfig(consumerProps))
    val stream = connector.createMessageStreams(topicCountMap)
    val result = ArrayBuffer[String]()
    for (message <- stream.get(topic)) {
      message.map(kafkaStream => {
        val it = kafkaStream.iterator()
        while(result.size < source.length) {
          result += new String(it.next.message())
        }
      })
    }
    assert(result.synchronized { source === result.sorted.toArray })
  }

}
