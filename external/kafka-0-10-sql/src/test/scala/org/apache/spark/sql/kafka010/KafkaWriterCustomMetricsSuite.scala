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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage

class KafkaWriterCustomMetricsSuite extends SparkFunSuite {

  test("collate messages") {
    val minOffset1 = KafkaSourceOffset(("topic1", 1, 2), ("topic1", 2, 3))
    val maxOffset1 = KafkaSourceOffset(("topic1", 1, 2), ("topic1", 2, 5))
    val minOffset2 = KafkaSourceOffset(("topic1", 1, 0), ("topic1", 2, 3))
    val maxOffset2 = KafkaSourceOffset(("topic1", 1, 0), ("topic1", 2, 7))
    val messages: Array[WriterCommitMessage] = Array(
      KafkaWriterCommitMessage(minOffset1, maxOffset1),
      KafkaWriterCommitMessage(minOffset2, maxOffset2))
    val metrics = KafkaWriterCustomMetrics(messages)
    assert(metrics.minOffset === KafkaSourceOffset(("topic1", 1, 0), ("topic1", 2, 3)))
    assert(metrics.maxOffset === KafkaSourceOffset(("topic1", 1, 2), ("topic1", 2, 7)))
  }
}
