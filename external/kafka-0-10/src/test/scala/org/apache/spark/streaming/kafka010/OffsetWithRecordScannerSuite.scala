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

class OffsetWithRecordScannerSuite
  extends SparkFunSuite
    with Logging {

  class OffsetWithRecordScannerMock[K, V](records: List[Option[ConsumerRecord[K, V]]])
    extends OffsetWithRecordScanner[K, V](
      Map[String, Object]("isolation.level" -> "read_committed").asJava, 1, 1, 0.75F, true) {
    var i = -1
    override protected def getNext(c: KafkaDataConsumer[K, V]): Option[ConsumerRecord[K, V]] = {
      i = i + 1
      records(i)
    }

  }

  val emptyConsumerRecords = new ConsumerRecords[String, String](ju.Collections.emptyMap())
  val tp = new TopicPartition("topic", 0)

  test("Rewinder construction should fail if isolation level isn set to read_committed") {
    intercept[IllegalStateException] {
      new OffsetWithRecordScanner[String, String](
        Map[String, Object]("isolation.level" -> "read_uncommitted").asJava, 1, 1, 0.75F, true)
    }
  }

  test("Rewinder construction shouldn't fail if isolation level isn't set") {
      assert(new OffsetWithRecordScanner[String, String](
        Map[String, Object]().asJava, 1, 1, 0.75F, true) != null)
  }

  test("Rewinder construction should fail if isolation level isn't set to committed") {
    intercept[IllegalStateException] {
      new OffsetWithRecordScanner[String, String](
        Map[String, Object]("isolation.level" -> "read_uncommitted").asJava, 1, 1, 0.75F, true)
    }
  }

  test("Rewind should return the proper count.") {
    var scanner = new OffsetWithRecordScannerMock[String, String](
      records(Some(0), Some(1), Some(2), Some(3)))
    val (offset, size) = scanner.iterateUntilLastOrEmpty(0, 0, null, 2)
    assert(offset === 2)
    assert(size === 2)
  }

  test("Rewind should return the proper count with gap") {
    var scanner = new OffsetWithRecordScannerMock[String, String](
      records(Some(0), Some(1), Some(3), Some(4), Some(5)))
    val (offset, size) = scanner.iterateUntilLastOrEmpty(0, 0, null, 3)
    assert(offset === 4)
    assert(size === 3)
  }

  test("Rewind should return the proper count for the end of the iterator") {
    var scanner = new OffsetWithRecordScannerMock[String, String](
      records(Some(0), Some(1), Some(2), None))
    val (offset, size) = scanner.iterateUntilLastOrEmpty(0, 0, null, 3)
    assert(offset === 3)
    assert(size === 3)
  }

  test("Rewind should return the proper count missing data") {
    var scanner = new OffsetWithRecordScannerMock[String, String](
      records(Some(0), None))
    val (offset, size) = scanner.iterateUntilLastOrEmpty(0, 0, null, 2)
    assert(offset === 1)
    assert(size === 1)
  }

  test("Rewind should return the proper count without data") {
    var scanner = new OffsetWithRecordScannerMock[String, String](
      records(None))
    val (offset, size) = scanner.iterateUntilLastOrEmpty(0, 0, null, 2)
    assert(offset === 0)
    assert(size === 0)
  }

  private def records(offsets: Option[Long]*) = {
    offsets.map(o => o.map(new ConsumerRecord("topic", 0, _, "k", "v"))).toList
  }
}
