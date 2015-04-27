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

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

import kafka.serializer.StringDecoder
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.concurrent.Eventually

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

class KafkaStreamSuite extends FunSuite with Eventually with BeforeAndAfterAll {
  private var ssc: StreamingContext = _
  private var kafkaTestUtils: KafkaTestUtils = _

  override def beforeAll(): Unit = {
    kafkaTestUtils = new KafkaTestUtils
    kafkaTestUtils.setup()
  }

  override def afterAll(): Unit = {
    if (ssc != null) {
      ssc.stop()
      ssc = null
    }

    if (kafkaTestUtils != null) {
      kafkaTestUtils.teardown()
      kafkaTestUtils = null
    }
  }

  test("Kafka input stream") {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName(this.getClass.getSimpleName)
    ssc = new StreamingContext(sparkConf, Milliseconds(500))
    val topic = "topic1"
    val sent = Map("a" -> 5, "b" -> 3, "c" -> 10)
    kafkaTestUtils.createTopic(topic)
    kafkaTestUtils.sendMessages(topic, sent)

    val kafkaParams = Map("zookeeper.connect" -> kafkaTestUtils.zkAddress,
      "group.id" -> s"test-consumer-${Random.nextInt(10000)}",
      "auto.offset.reset" -> "smallest")

    val stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Map(topic -> 1), StorageLevel.MEMORY_ONLY)
    val result = new mutable.HashMap[String, Long]()
    stream.map(_._2).countByValue().foreachRDD { r =>
      val ret = r.collect()
      ret.toMap.foreach { kv =>
        val count = result.getOrElseUpdate(kv._1, 0) + kv._2
        result.put(kv._1, count)
      }
    }

    ssc.start()

    eventually(timeout(10000 milliseconds), interval(100 milliseconds)) {
      assert(sent.size === result.size)
      sent.keys.foreach { k =>
        assert(sent(k) === result(k).toInt)
      }
    }
  }
}
