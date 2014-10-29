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

import kafka.serializer.StringDecoder
import kafka.utils.{ZkUtils, ZKGroupTopicDirs}

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext

class ReliableKafkaStreamSuite extends KafkaStreamSuite {
  import KafkaTestUtils._

  test("Reliable Kafka input stream") {
    val sparkConf = new SparkConf()
      .setMaster(master)
      .setAppName(framework)
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")
    val ssc = new StreamingContext(sparkConf, batchDuration)
    val topic = "test"
    val sent = Map("a" -> 1, "b" -> 1, "c" -> 1)
    createTopic(topic)
    produceAndSendMessage(topic, sent)

    val kafkaParams = Map("zookeeper.connect" -> s"$zkHost:$zkPort",
      "group.id" -> s"test-consumer-${random.nextInt(10000)}",
      "auto.offset.reset" -> "smallest")

    val stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      Map(topic -> 1),
      StorageLevel.MEMORY_ONLY)
    val result = new mutable.HashMap[String, Long]()
    stream.map { case (k, v) => v }
      .foreachRDD { r =>
        val ret = r.collect()
        ret.foreach { v =>
          val count = result.getOrElseUpdate(v, 0) + 1
          result.put(v, count)
        }
      }
    ssc.start()
    ssc.awaitTermination(3000)

    assert(sent.size === result.size)
    sent.keys.foreach { k => assert(sent(k) === result(k).toInt) }

    ssc.stop()
  }

  test("Verify the offset commit") {
    val sparkConf = new SparkConf()
      .setMaster(master)
      .setAppName(framework)
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")
    val ssc = new StreamingContext(sparkConf, batchDuration)
    val topic = "test"
    val sent = Map("a" -> 10, "b" -> 10, "c" -> 10)
    createTopic(topic)
    produceAndSendMessage(topic, sent)

    val groupId = s"test-consumer-${random.nextInt(10000)}"

    val kafkaParams = Map("zookeeper.connect" -> s"$zkHost:$zkPort",
      "group.id" -> groupId,
      "auto.offset.reset" -> "smallest")

    assert(getCommitOffset(groupId, topic, 0) === 0L)

    val stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      Map(topic -> 1),
      StorageLevel.MEMORY_ONLY)
    stream.foreachRDD(_ => Unit)
    ssc.start()
    ssc.awaitTermination(3000)
    ssc.stop()

    assert(getCommitOffset(groupId, topic, 0) === 29L)
  }

  test("Verify multiple topics offset commit") {
    val sparkConf = new SparkConf()
      .setMaster(master)
      .setAppName(framework)
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")
    val ssc = new StreamingContext(sparkConf, batchDuration)
    val topics = Map("topic1" -> 1, "topic2" -> 1, "topic3" -> 1)
    val sent = Map("a" -> 10, "b" -> 10, "c" -> 10)
    topics.foreach { case (t, _) =>
      createTopic(t)
      produceAndSendMessage(t, sent)
    }

    val groupId = s"test-consumer-${random.nextInt(10000)}"

    val kafkaParams = Map("zookeeper.connect" -> s"$zkHost:$zkPort",
      "group.id" -> groupId,
      "auto.offset.reset" -> "smallest")

    topics.foreach { case (t, _) => assert(getCommitOffset(groupId, t, 0) === 0L) }

    val stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      topics,
      StorageLevel.MEMORY_ONLY)
    stream.foreachRDD(_ => Unit)
    ssc.start()
    ssc.awaitTermination(3000)
    ssc.stop()

    topics.foreach { case (t, _) => assert(getCommitOffset(groupId, t, 0) === 29L) }
  }

  test("Verify offset commit when exception is met") {
    val sparkConf = new SparkConf()
      .setMaster(master)
      .setAppName(framework)
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")
    var ssc = new StreamingContext(
      sparkConf.clone.set("spark.streaming.blockInterval", "4000"),
      batchDuration)
    val topics = Map("topic1" -> 1, "topic2" -> 1, "topic3" -> 1)
    val sent = Map("a" -> 10, "b" -> 10, "c" -> 10)
    topics.foreach { case (t, _) =>
      createTopic(t)
      produceAndSendMessage(t, sent)
    }

    val groupId = s"test-consumer-${random.nextInt(10000)}"

    val kafkaParams = Map("zookeeper.connect" -> s"$zkHost:$zkPort",
      "group.id" -> groupId,
      "auto.offset.reset" -> "smallest")

    KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      topics,
      StorageLevel.MEMORY_ONLY).foreachRDD(_ => throw new Exception)
    try {
      ssc.start()
      ssc.awaitTermination(1000)
    } catch {
      case e: Exception =>
        if (ssc != null) {
          ssc.stop()
          ssc = null
        }
    }
    // Failed before putting to BM, so offset is not updated.
    topics.foreach { case (t, _) => assert(getCommitOffset(groupId, t, 0) === 0L) }

    // Restart to see if data is consumed from last checkpoint.
    ssc = new StreamingContext(sparkConf, batchDuration)
    KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      topics,
      StorageLevel.MEMORY_ONLY).foreachRDD(_ => Unit)
    ssc.start()
    ssc.awaitTermination(3000)
    ssc.stop()

    topics.foreach { case (t, _) => assert(getCommitOffset(groupId, t, 0) === 29L) }
  }

  private def getCommitOffset(groupId: String, topic: String, partition: Int): Long = {
    assert(zkClient != null, "Zookeeper client is not initialized")

    val topicDirs = new ZKGroupTopicDirs(groupId, topic)
    val zkPath = s"${topicDirs.consumerOffsetDir}/$partition"

    ZkUtils.readDataMaybeNull(zkClient, zkPath)._1.map(_.toLong).getOrElse(0L)
  }
}
