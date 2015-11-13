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
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.concurrent.Eventually

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.util.Utils

class ReliableKafkaStreamSuite extends SparkFunSuite
    with BeforeAndAfterAll with BeforeAndAfter with Eventually {

  private val sparkConf = new SparkConf()
    .setMaster("local[4]")
    .setAppName(this.getClass.getSimpleName)
    .set("spark.streaming.receiver.writeAheadLog.enable", "true")
  private val data = Map("a" -> 10, "b" -> 10, "c" -> 10)

  private var kafkaTestUtils: KafkaTestUtils = _

  private var groupId: String = _
  private var kafkaParams: Map[String, String] = _
  private var ssc: StreamingContext = _
  private var tempDirectory: File = null

  override def beforeAll() : Unit = {
    kafkaTestUtils = new KafkaTestUtils
    kafkaTestUtils.setup()

    groupId = s"test-consumer-${Random.nextInt(10000)}"
    kafkaParams = Map(
      "zookeeper.connect" -> kafkaTestUtils.zkAddress,
      "group.id" -> groupId,
      "auto.offset.reset" -> "smallest"
    )

    tempDirectory = Utils.createTempDir()
  }

  override def afterAll(): Unit = {
    Utils.deleteRecursively(tempDirectory)

    if (kafkaTestUtils != null) {
      kafkaTestUtils.teardown()
      kafkaTestUtils = null
    }
  }

  before {
    ssc = new StreamingContext(sparkConf, Milliseconds(500))
    ssc.checkpoint(tempDirectory.getAbsolutePath)
  }

  after {
    if (ssc != null) {
      ssc.stop()
      ssc = null
    }
  }

  test("Reliable Kafka input stream with single topic") {
    val topic = "test-topic"
    kafkaTestUtils.createTopic(topic)
    kafkaTestUtils.sendMessages(topic, data)

    // Verify whether the offset of this group/topic/partition is 0 before starting.
    assert(getCommitOffset(groupId, topic, 0) === None)

    val stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Map(topic -> 1), StorageLevel.MEMORY_ONLY)
    val result = new mutable.HashMap[String, Long]()
    stream.map { case (k, v) => v }.foreachRDD { r =>
        val ret = r.collect()
        ret.foreach { v =>
          val count = result.getOrElseUpdate(v, 0) + 1
          result.put(v, count)
        }
      }
    ssc.start()

    eventually(timeout(20000 milliseconds), interval(200 milliseconds)) {
      // A basic process verification for ReliableKafkaReceiver.
      // Verify whether received message number is equal to the sent message number.
      assert(data.size === result.size)
      // Verify whether each message is the same as the data to be verified.
      data.keys.foreach { k => assert(data(k) === result(k).toInt) }
      // Verify the offset number whether it is equal to the total message number.
      assert(getCommitOffset(groupId, topic, 0) === Some(29L))
    }
  }

  test("Reliable Kafka input stream with multiple topics") {
    val topics = Map("topic1" -> 1, "topic2" -> 1, "topic3" -> 1)
    topics.foreach { case (t, _) =>
      kafkaTestUtils.createTopic(t)
      kafkaTestUtils.sendMessages(t, data)
    }

    // Before started, verify all the group/topic/partition offsets are 0.
    topics.foreach { case (t, _) => assert(getCommitOffset(groupId, t, 0) === None) }

    // Consuming all the data sent to the broker which will potential commit the offsets internally.
    val stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics, StorageLevel.MEMORY_ONLY)
    stream.foreachRDD(_ => Unit)
    ssc.start()

    eventually(timeout(20000 milliseconds), interval(100 milliseconds)) {
      // Verify the offset for each group/topic to see whether they are equal to the expected one.
      topics.foreach { case (t, _) => assert(getCommitOffset(groupId, t, 0) === Some(29L)) }
    }
  }


  /** Getting partition offset from Zookeeper. */
  private def getCommitOffset(groupId: String, topic: String, partition: Int): Option[Long] = {
    val topicDirs = new ZKGroupTopicDirs(groupId, topic)
    val zkPath = s"${topicDirs.consumerOffsetDir}/$partition"
    ZkUtils.readDataMaybeNull(kafkaTestUtils.zookeeperClient, zkPath)._1.map(_.toLong)
  }
}
