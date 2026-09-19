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

import org.apache.kafka.common.TopicPartition

import org.apache.spark.SparkFunSuite

class KafkaOffsetRangeLimitSuite extends SparkFunSuite {

  test("resolving topic-level offsets against the assigned partitions") {
    val limit = SpecificOffsetRangeLimit(
      Map(new TopicPartition("topicB", 0) -> 23L),
      Map("topicA" -> KafkaOffsetRangeLimit.EARLIEST))
    val partitions = Set(
      new TopicPartition("topicA", 0),
      new TopicPartition("topicA", 1),
      new TopicPartition("topicB", 0))

    assert(limit.resolve(partitions) === Map(
      new TopicPartition("topicA", 0) -> KafkaOffsetRangeLimit.EARLIEST,
      new TopicPartition("topicA", 1) -> KafkaOffsetRangeLimit.EARLIEST,
      new TopicPartition("topicB", 0) -> 23L))
  }

  test("resolving fully enumerated offsets doesn't need the assigned partitions") {
    val limit = SpecificOffsetRangeLimit(Map(new TopicPartition("topicB", 0) -> 23L))
    assert(limit.resolve(fail("the partitions should not have been fetched")) ===
      Map(new TopicPartition("topicB", 0) -> 23L))
  }

  test("resolving a topic-level offset of a topic without assigned partitions fails") {
    val limit = SpecificOffsetRangeLimit(
      Map.empty,
      Map("topicA" -> KafkaOffsetRangeLimit.EARLIEST,
        "topicC" -> KafkaOffsetRangeLimit.EARLIEST))
    val ex = intercept[KafkaIllegalStateException] {
      limit.resolve(Set(new TopicPartition("topicA", 0)))
    }
    assert(ex.getCondition === "KAFKA_TOPIC_OFFSET_DOES_NOT_MATCH_ASSIGNED")
    assert(ex.getMessage.contains("topicC"))
  }
}
