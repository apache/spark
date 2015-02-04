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

import scala.util.Random
import scala.concurrent.duration._

import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Eventually

import kafka.serializer.StringDecoder

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

class KafkaDirectStreamSuite extends KafkaStreamSuiteBase with BeforeAndAfter with Eventually {
  val sparkConf = new SparkConf()
    .setMaster("local[4]")
    .setAppName(this.getClass.getSimpleName)

  val brokerHost = "localhost"

  val kafkaParams = Map(
    "metadata.broker.list" -> s"$brokerHost:$brokerPort",
    "auto.offset.reset" -> "smallest"
  )

  var ssc: StreamingContext = _

  before {
    setupKafka()

    ssc = new StreamingContext(sparkConf, Milliseconds(500))
  }

  after {
    if (ssc != null) {
      ssc.stop()
    }
    tearDownKafka()
  }

  test("multi topic stream") {
    val topics = Set("newA", "newB")
    val data = Map("a" -> 7, "b" -> 9)
    topics.foreach { t =>
      createTopic(t)
      produceAndSendMessage(t, data)
    }
    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)
    var total = 0L;

    stream.foreachRDD { rdd =>
      val offsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val collected = rdd.mapPartitionsWithIndex { (i, iter) =>
        val off = offsets(i)
        val all = iter.toSeq
        val partSize = all.size
        val rangeSize = off.untilOffset - off.fromOffset
        all.map { _ =>
          (partSize, rangeSize)
        }.toIterator
      }.collect
      collected.foreach { case (partSize, rangeSize) =>
          assert(partSize === rangeSize, "offset ranges are wrong")
      }
      total += collected.size
    }
    ssc.start()
    eventually(timeout(20000.milliseconds), interval(200.milliseconds)) {
      assert(total === data.values.sum * topics.size, "didn't get all messages")
    }
    ssc.stop()
  }
}
