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

package org.apache.spark.sql.streaming

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.internal.SQLConf

class StreamingDeduplicationAvroSuite extends StreamingDeduplicationSuite {

  import testImplicits._

  override protected def test(testName: String, testTags: Tag*)(testBody: => Any)
                             (implicit pos: Position): Unit = {
    super.test(s"$testName (encoding = Avro)", testTags: _*) {
      withSQLConf(SQLConf.STREAMING_STATE_STORE_ENCODING_FORMAT.key -> "avro") {
        testBody
      }
    }
  }

  test("deduplicate with add fields schema evolution") {
    withTempDir { chkptDir =>
      val dirPath = chkptDir.getCanonicalPath
      val inputData = MemoryStream[(String, String, Int)]
      val result1 = inputData.toDS().dropDuplicates("_1")
      testStream(result1, OutputMode.Append())(
        StartStream(checkpointLocation = dirPath),
        AddData(inputData, ("a", "key1", 1)),
        CheckLastBatch(("a", "key1", 1)),
        assertNumStateRows(total = 1, updated = 1),
        AddData(inputData, ("a", "key2", 1)), // Dropped
        CheckLastBatch(),
        assertNumStateRows(total = 1, updated = 0),
        AddData(inputData, ("a", "key1", 2)), // Dropped
        CheckLastBatch(),
        assertNumStateRows(total = 1, updated = 0),
        StopStream
      )

      val result2 = inputData.toDS().dropDuplicates("_1", "_2")
      testStream(result1, OutputMode.Append())(
        StartStream(checkpointLocation = dirPath),
        AddData(inputData, ("a", "key3", 1)),
        CheckLastBatch(("a", "key3", 1)),
        assertNumStateRows(total = 2, updated = 1),
        AddData(inputData, ("a", "key4", 1)), // Would not drop
        CheckLastBatch(("a", "key4", 1)),
        assertNumStateRows(total = 3, updated = 1),
        AddData(inputData, ("a", "key4", 2)), // Dropped
        CheckLastBatch(),
        assertNumStateRows(total = 3, updated = 0),
        AddData(inputData, ("a", null, 1)), // Dropped
        CheckLastBatch(),
        assertNumStateRows(total = 3, updated = 0),
      )
    }
  }

  test("deduplicate with remove fields schema evolution") {

  }

  test("deduplicate with reorder fields schema evolution") {

  }

  test("deduplicate with upcast fields schema evolution") {

  }

  test("deduplicate with downcast fields would fail") {

  }
}
