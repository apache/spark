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

package org.apache.spark.sql.execution.streaming

import java.io.File

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSQLContext

class OffsetSeqLogSuite extends SparkFunSuite with SharedSQLContext {

  /** test string offset type */
  case class StringOffset(override val json: String) extends Offset

  testWithUninterruptibleThread("serialization - deserialization") {
    withTempDir { temp =>
      val dir = new File(temp, "dir") // use non-existent directory to test whether log make the dir
    val metadataLog = new OffsetSeqLog(spark, dir.getAbsolutePath)
      val batch0 = OffsetSeq.fill(LongOffset(0), LongOffset(1), LongOffset(2))
      val batch1 = OffsetSeq.fill(StringOffset("one"), StringOffset("two"), StringOffset("three"))

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

  test("read Spark 2.1.0 log format") {
    val (batchId, offsetSeq) = readFromResource("offset-log-version-2.1.0")
    assert(batchId === 16)
    assert(offsetSeq ===
      OffsetSeq(
        offsets = Seq(Some(SerializedOffset(
          """
            |{"kafka-topic":{"23":0,"8":1,"17":1,"11":1,"20":0,"2":6,"5":2,"14":0,"4":4,"13":1,
            |"22":1,"7":1,"16":0,"25":0,"10":0,"1":6,"19":0,"9":0,"18":1,"3":3,"21":0,"12":0,
            |"15":0,"24":0,"6":0,"0":4},"kafka-topic8":{"8":0,"11":0,"2":2,"5":2,"4":0,"7":0,
            |"10":0,"1":0,"9":0,"12":0,"3":0,"6":1,"0":2},"kafka-topic16":{"8":0,"2":0,"5":0,
            |"4":0,"7":0,"1":0,"3":0,"6":0,"0":0},"kafka-topic10":{"8":1,"2":1,"5":1,"4":1,"7":0,
            |"1":0,"3":0,"6":0,"0":0},"kafka-topic1":{"8":3,"17":0,"11":3,"20":0,"2":5,"5":2,"14":2,
            |"13":0,"4":1,"7":0,"16":0,"1":3,"10":0,"19":0,"9":0,"18":0,"21":0,"12":1,"3":0,"15":0,
            |"6":1,"0":5},"kafka-topic4":{"23":0,"8":0,"17":0,"11":0,"20":0,"2":0,"5":0,"14":0,
            |"4":0,"13":0,"22":0,"16":0,"7":0,"25":0,"10":0,"1":0,"19":0,"9":0,"18":0,"3":0,"12":0,
            |"21":0,"15":0,"6":0,"24":0,"0":9},"kafka-topic3":{"23":0,"17":0,"8":0,"26":0,"11":0,
            |"2":4,"20":0,"5":1,"14":1,"13":2,"4":1,"22":1,"16":1,"7":2,"25":0,"10":0,"1":4,"19":0,
            |"9":1,"18":0,"27":0,"3":4,"12":0,"21":0,"15":0,"6":1,"24":0,"0":2},
            |"kafka-topic18":{"1":0,"0":0},"kafka-topic6":{"8":0,"11":0,"2":0,"5":0,"14":0,"13":0,
            |"4":0,"7":0,"1":0,"10":0,"9":0,"12":0,"3":0,"15":0,"6":0,"0":0},"kafka-topic12":{"8":0,
            |"2":0,"5":0,"4":0,"7":0,"1":0,"3":0,"6":0,"0":0},"kafka-topic14":{"2":0,"5":0,"4":0,
            |"1":0,"3":0,"0":0},"kafka-topic2":{"23":0,"17":1,"8":1,"26":0,"11":1,"2":4,"20":1,
            |"5":3,"14":1,"4":1,"13":1,"22":0,"16":0,"7":1,"25":0,"10":1,"1":3,"19":0,"9":1,
            |"18":0,"12":0,"21":0,"3":2,"15":0,"6":0,"24":0,"0":3}}
          """.stripMargin.trim.split("\n").mkString))),
        metadata = Some("""{"batchWatermarkMs":0,"batchTimestampMs":1480732570644}""")))
  }

  private def readFromResource(dir: String): (Long, OffsetSeq) = {
    val input = getClass.getResource(s"/structured-streaming/$dir")
    val log = new OffsetSeqLog(spark, input.toString)
    log.getLatest().get
  }
}
