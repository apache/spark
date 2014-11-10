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

import java.io.File

import scala.collection.mutable

import kafka.serializer.StringDecoder
import kafka.utils.{ZkUtils, ZKGroupTopicDirs}

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.util.Utils

class ReliableKafkaStreamSuite extends KafkaStreamSuite {
  import KafkaTestUtils._

  test("Reliable Kafka input stream") {
    val sparkConf = new SparkConf()
      .setMaster(master)
      .setAppName(framework)
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")
    val ssc = new StreamingContext(sparkConf, batchDuration)
    val checkpointDir = s"${System.getProperty("java.io.tmpdir", "/tmp")}/" +
      s"test-checkpoint${random.nextInt(10000)}"
    Utils.registerShutdownDeleteDir(new File(checkpointDir))
    ssc.checkpoint(checkpointDir)

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

    // A basic process verification for ReliableKafkaReceiver.
    // Verify whether received message number is equal to the sent message number.
    assert(sent.size === result.size)
    // Verify whether each message is the same as the data to be verified.
    sent.keys.foreach { k => assert(sent(k) === result(k).toInt) }

    ssc.stop()
  }

  test("Verify the offset commit") {
    // Verify the corretness of offset commit mechanism.
    val sparkConf = new SparkConf()
      .setMaster(master)
      .setAppName(framework)
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")
    val ssc = new StreamingContext(sparkConf, batchDuration)
    val checkpointDir = s"${System.getProperty("java.io.tmpdir", "/tmp")}/" +
      s"test-checkpoint${random.nextInt(10000)}"
    Utils.registerShutdownDeleteDir(new File(checkpointDir))
    ssc.checkpoint(checkpointDir)

    val topic = "test"
    val sent = Map("a" -> 10, "b" -> 10, "c" -> 10)
    createTopic(topic)
    produceAndSendMessage(topic, sent)

    val groupId = s"test-consumer-${random.nextInt(10000)}"

    val kafkaParams = Map("zookeeper.connect" -> s"$zkHost:$zkPort",
      "group.id" -> groupId,
      "auto.offset.reset" -> "smallest")

    // Verify whether the offset of this group/topic/partition is 0 before starting.
    assert(getCommitOffset(groupId, topic, 0) === 0L)

    // Do this to consume all the message of this group/topic.
    val stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      Map(topic -> 1),
      StorageLevel.MEMORY_ONLY)
    stream.foreachRDD(_ => Unit)
    ssc.start()
    ssc.awaitTermination(3000)
    ssc.stop()

    // Verify the offset number whether it is equal to the total message number.
    assert(getCommitOffset(groupId, topic, 0) === 29L)
  }

  test("Verify multiple topics offset commit") {
    val sparkConf = new SparkConf()
      .setMaster(master)
      .setAppName(framework)
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")
    val ssc = new StreamingContext(sparkConf, batchDuration)
    val checkpointDir = s"${System.getProperty("java.io.tmpdir", "/tmp")}/" +
      s"test-checkpoint${random.nextInt(10000)}"
    Utils.registerShutdownDeleteDir(new File(checkpointDir))
    ssc.checkpoint(checkpointDir)

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

    // Before started, verify all the group/topic/partition offsets are 0.
    topics.foreach { case (t, _) => assert(getCommitOffset(groupId, t, 0) === 0L) }

    // Consuming all the data sent to the broker which will potential commit the offsets internally.
    val stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      topics,
      StorageLevel.MEMORY_ONLY)
    stream.foreachRDD(_ => Unit)
    ssc.start()
    ssc.awaitTermination(3000)
    ssc.stop()

    // Verify the offset for each group/topic to see whether they are equal to the expected one.
    topics.foreach { case (t, _) => assert(getCommitOffset(groupId, t, 0) === 29L) }
  }

  /** Getting partition offset from Zookeeper. */
  private def getCommitOffset(groupId: String, topic: String, partition: Int): Long = {
    assert(zkClient != null, "Zookeeper client is not initialized")

    val topicDirs = new ZKGroupTopicDirs(groupId, topic)
    val zkPath = s"${topicDirs.consumerOffsetDir}/$partition"

    ZkUtils.readDataMaybeNull(zkClient, zkPath)._1.map(_.toLong).getOrElse(0L)
  }
}
