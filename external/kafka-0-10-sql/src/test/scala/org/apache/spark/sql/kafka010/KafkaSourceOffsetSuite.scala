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

import java.io.File

import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.streaming.OffsetSuite
import org.apache.spark.sql.test.SharedSQLContext

class KafkaSourceOffsetSuite extends OffsetSuite with SharedSQLContext {

  compare(
    one = KafkaSourceOffset(("t", 0, 1L)),
    two = KafkaSourceOffset(("t", 0, 2L)))

  compare(
    one = KafkaSourceOffset(("t", 0, 1L), ("t", 1, 0L)),
    two = KafkaSourceOffset(("t", 0, 2L), ("t", 1, 1L)))

  compare(
    one = KafkaSourceOffset(("t", 0, 1L), ("T", 0, 0L)),
    two = KafkaSourceOffset(("t", 0, 2L), ("T", 0, 1L)))

  compare(
    one = KafkaSourceOffset(("t", 0, 1L)),
    two = KafkaSourceOffset(("t", 0, 2L), ("t", 1, 1L)))


  val kso1 = KafkaSourceOffset(("t", 0, 1L))
  val kso2 = KafkaSourceOffset(("t", 0, 2L), ("t", 1, 3L))
  val kso3 = KafkaSourceOffset(("t", 0, 2L), ("t", 1, 3L), ("t", 1, 4L))

  compare(KafkaSourceOffset(SerializedOffset(kso1.json)),
    KafkaSourceOffset(SerializedOffset(kso2.json)))

  test("basic serialization - deserialization") {
    assert(KafkaSourceOffset.getPartitionOffsets(kso1) ==
      KafkaSourceOffset.getPartitionOffsets(SerializedOffset(kso1.json)))
  }


  testWithUninterruptibleThread("OffsetSeqLog serialization - deserialization") {
    withTempDir { temp =>
      // use non-existent directory to test whether log make the dir
      val dir = new File(temp, "dir")
      val metadataLog = new OffsetSeqLog(spark, dir.getAbsolutePath)
      val batch0 = OffsetSeq.fill(kso1)
      val batch1 = OffsetSeq.fill(kso2, kso3)

      val batch0Serialized = OffsetSeq.fill(batch0.offsets.flatMap(_.map(o =>
        SerializedOffset(o.json))): _*)

      val batch1Serialized = OffsetSeq.fill(batch1.offsets.flatMap(_.map(o =>
        SerializedOffset(o.json))): _*)

      assert(metadataLog.add(0, batch0))
      assert(metadataLog.getLatest() === Some(0 -> batch0Serialized))
      assert(metadataLog.get(0) === Some(batch0Serialized))

      assert(metadataLog.add(1, batch1))
      assert(metadataLog.get(0) === Some(batch0Serialized))
      assert(metadataLog.get(1) === Some(batch1Serialized))
      assert(metadataLog.getLatest() === Some(1 -> batch1Serialized))
      assert(metadataLog.get(None, Some(1)) ===
        Array(0 -> batch0Serialized, 1 -> batch1Serialized))

      // Adding the same batch does nothing
      metadataLog.add(1, OffsetSeq.fill(LongOffset(3)))
      assert(metadataLog.get(0) === Some(batch0Serialized))
      assert(metadataLog.get(1) === Some(batch1Serialized))
      assert(metadataLog.getLatest() === Some(1 -> batch1Serialized))
      assert(metadataLog.get(None, Some(1)) ===
        Array(0 -> batch0Serialized, 1 -> batch1Serialized))
    }
  }
}
