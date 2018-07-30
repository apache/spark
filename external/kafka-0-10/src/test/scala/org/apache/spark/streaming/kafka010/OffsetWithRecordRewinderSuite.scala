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

package org.apache.spark.streaming.kafka010

import java.{util => ju}

import scala.collection.JavaConverters._

import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord, ConsumerRecords}
import org.apache.kafka.common.TopicPartition

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging

class OffsetWithRecordRewinderSuite
  extends SparkFunSuite
    with Logging {

  class OffsetWithRecordRewinderMock[K, V](records: Map[Long, ConsumerRecords[K, V]])
    extends OffsetWithRecordRewinder[K, V]( 10,
      Map[String, Object]("isolation.level" -> "read_committed").asJava) {

    override protected def seekAndPoll(c: Consumer[K, V], tp: TopicPartition, offset: Long) = {
      records(offset)
    }

  }

  val emptyConsumerRecords = new ConsumerRecords[String, String](ju.Collections.emptyMap())
  val tp = new TopicPartition("topic", 0)

  test("Rewinder construction should fail if isolation level isn't set to committed") {
    intercept[IllegalStateException] {
      new OffsetWithRecordRewinder[String, String](10,
        Map[String, Object]().asJava)
    }
    intercept[IllegalStateException] {
      new OffsetWithRecordRewinder[String, String](10,
        Map[String, Object]("isolation.level" -> "read_uncommitted").asJava)
    }
  }

  test("Rewind should find the last offset containing data. " +
    "records are empty, should always rewind to 0") {
    var rewinder = new OffsetWithRecordRewinderMock[String, String](
      Map(0L -> emptyConsumerRecords))
    assert(0 === rewinder.rewindUntilDataExist(Map(tp -> 0), tp, 10, iteration = 1, 10, null))

    rewinder = new OffsetWithRecordRewinderMock[String, String](Map(0L -> emptyConsumerRecords))
    assert(0 === rewinder.rewindUntilDataExist(Map(tp -> 0), tp, 8, iteration = 1, 10, null))

    rewinder = new OffsetWithRecordRewinderMock[String, String](
      Map(0L -> emptyConsumerRecords, 2L -> emptyConsumerRecords, 12L -> emptyConsumerRecords))
    assert(0 === rewinder.rewindUntilDataExist(Map(tp -> 0), tp, 22, iteration = 1, 10, null))

    rewinder = new OffsetWithRecordRewinderMock[String, String](
      Map(0L -> emptyConsumerRecords, 2L -> emptyConsumerRecords, 22L -> emptyConsumerRecords))
    assert(0 === rewinder.rewindUntilDataExist(Map(tp -> 0), tp, 32, iteration = 1, 10, null))

    rewinder = new OffsetWithRecordRewinderMock[String, String](
      Map(0L -> emptyConsumerRecords, 12L -> emptyConsumerRecords, 32L -> emptyConsumerRecords))
    assert(0 === rewinder.rewindUntilDataExist(Map(tp -> 0), tp, 42, iteration = 1, 10, null))
  }

  test("Rewind should not do anything if start = last") {
    var rewinder = new OffsetWithRecordRewinderMock[String, String](Map())
    assert(10 === rewinder.rewindUntilDataExist(Map(tp -> 10), tp, 10, iteration = 1, 10, null))
  }

  test("Rewind should find the first existing records") {
    var records = List(new ConsumerRecord("topic", 0, 35, "k", "v"),
      new ConsumerRecord("topic", 0, 36, "k", "v")).asJava
    var rewinder = new OffsetWithRecordRewinderMock[String, String](
      Map(32L -> buildConsumerRecords(records)))
    assert(36 === rewinder.rewindUntilDataExist(Map(tp -> 0), tp, 42, iteration = 1, 10, null))

    records = List(new ConsumerRecord("topic", 0, 35, "k", "v")).asJava
    rewinder = new OffsetWithRecordRewinderMock[String, String](
      Map(32L -> buildConsumerRecords(records)))
    assert(35 === rewinder.rewindUntilDataExist(Map(tp -> 0), tp, 42, iteration = 1, 10, null))
  }

  test("Rewind should find the first existing records and retry") {
    val records = List(new ConsumerRecord("topic", 0, 34, "k", "v"),
      new ConsumerRecord("topic", 0, 35, "k", "v")).asJava
    var rewinder = new OffsetWithRecordRewinderMock[String, String](
      Map(32L -> buildConsumerRecords(records),
        132L -> emptyConsumerRecords))
    assert(35 === rewinder.rewindUntilDataExist(Map(tp -> 0), tp, 142, iteration = 1, 10, null))
  }

  test("Rewind should find the first existing records even the offset if higher") {
    val records = List(new ConsumerRecord("topic", 0, 144, "k", "v"),
      new ConsumerRecord("topic", 0, 145, "k", "v")).asJava
    var rewinder = new OffsetWithRecordRewinderMock[String, String](
      Map(132L -> buildConsumerRecords(records)))
    assert(144 === rewinder.rewindUntilDataExist(Map(tp -> 0), tp, 142, iteration = 1, 10, null))
  }

  private def buildConsumerRecords(records: ju.List[ConsumerRecord[String, String]]) = {
    new ConsumerRecords[String, String](Map(tp -> records).asJava)
  }
}
