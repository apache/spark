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

class JsonUtilsSuite extends SparkFunSuite {

  test("parsing partitions") {
    val parsed = JsonUtils.partitions("""{"topicA":[0,1],"topicB":[4,6]}""")
    val expected = Array(
      new TopicPartition("topicA", 0),
      new TopicPartition("topicA", 1),
      new TopicPartition("topicB", 4),
      new TopicPartition("topicB", 6)
    )
    assert(parsed.toSeq === expected.toSeq)
  }

  test("parsing partitionOffsets") {
    val parsed = JsonUtils.partitionOffsets(
      """{"topicA":{"0":23,"1":-1},"topicB":{"0":-2}}""")

    assert(parsed(new TopicPartition("topicA", 0)) === 23)
    assert(parsed(new TopicPartition("topicA", 1)) === -1)
    assert(parsed(new TopicPartition("topicB", 0)) === -2)
  }

  test("parsing specificOffsets") {
    val parsed = JsonUtils.specificOffsets(
      """{"topicA":{"0":23,"1":-1},"topicB":{"0":-2}}""")

    assert(parsed.partitionOffsets === Map(
      new TopicPartition("topicA", 0) -> 23L,
      new TopicPartition("topicA", 1) -> -1L,
      new TopicPartition("topicB", 0) -> -2L))
    assert(parsed.topicOffsets.isEmpty)
  }

  test("parsing specificOffsets with topic-level earliest/latest") {
    val parsed = JsonUtils.specificOffsets(
      """{"topicA":"earliest","topicB":"latest"}""")

    assert(parsed.partitionOffsets.isEmpty)
    assert(parsed.topicOffsets === Map(
      "topicA" -> KafkaOffsetRangeLimit.EARLIEST,
      "topicB" -> KafkaOffsetRangeLimit.LATEST))
  }

  test("parsing specificOffsets mixing topic-level and partition-level values") {
    val parsed = JsonUtils.specificOffsets(
      """{"topicA":"earliest","topicB":{"0":23,"1":-1},"topicC":"LATEST"}""")

    assert(parsed.partitionOffsets === Map(
      new TopicPartition("topicB", 0) -> 23L,
      new TopicPartition("topicB", 1) -> -1L))
    assert(parsed.topicOffsets === Map(
      "topicA" -> KafkaOffsetRangeLimit.EARLIEST,
      "topicC" -> KafkaOffsetRangeLimit.LATEST))
  }

  test("parsing specificOffsets rejects a topic given in both forms") {
    val ex = intercept[IllegalArgumentException] {
      JsonUtils.specificOffsets("""{"topicA":"earliest","topicA":{"0":23}}""")
    }
    assert(ex.getMessage.contains("topicA"))
    assert(ex.getMessage.contains("both a topic-level offset"))
  }

  test("parsing specificOffsets rejects malformed json") {
    Seq(
      """{"topicA":"first"}""",
      """{"topicA":[0,1]}""",
      """{"topicA":{"0":"earliest"}}""",
      """"earliest"""",
      "not json").foreach { json =>
      val ex = intercept[IllegalArgumentException] {
        JsonUtils.specificOffsets(json)
      }
      assert(ex.getMessage.contains(json))
    }
  }
}
