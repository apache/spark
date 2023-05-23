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

import java.io.File

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.state.{RocksDBConf, RocksDBStateStoreProvider}
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.OutputMode.Update
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.util.Utils


trait RocksDBStateStoreTest extends SQLTestUtils {
  override protected def test(testName: String, testTags: Tag*)(testBody: => Any)
                             (implicit pos: Position): Unit = {
    super.test(testName + " (RocksDBStateStore)", testTags: _*) {
      withSQLConf(rocksdbChangelogCheckpointingConfKey -> "false",
        SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName) {
        testBody
      }
      // in case tests have any code that needs to execute after every test
      super.afterEach()
    }

    super.test(testName + " (RocksDBStateStore with changelog checkpointing)", testTags: _*) {
      // in case tests have any code that needs to execute before every test
      super.beforeEach()
      withSQLConf(rocksdbChangelogCheckpointingConfKey -> "true",
        SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName) {
        testBody
      }
    }
  }


  def rocksdbChangelogCheckpointingConfKey: String = RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX +
    ".changelogCheckpointing.enabled"

  def isChangelogCheckpointingEnabled: Boolean =
    SQLConf.get.getConfString(rocksdbChangelogCheckpointingConfKey) == "true"
}

class RocksDBStateStoreStreamingAggregationSuite
  extends StreamingAggregationSuite with RocksDBStateStoreTest  {
  import testImplicits._
  def snapshotVersionsPresent(dir: File): Seq[Long] = {
    dir.listFiles.filter(_.getName.endsWith(".zip"))
      .map(_.getName.stripSuffix(".zip"))
      .map(_.toLong)
      .sorted
  }
  def changelogVersionsPresent(dir: File): Seq[Long] = {
    dir.listFiles.filter(_.getName.endsWith(".changelog"))
      .map(_.getName.stripSuffix(".changelog"))
      .map(_.toLong)
      .sorted
  }
  test("Streaming aggregation RocksDB State Store backward compatibility.") {
    val checkpointDir = Utils.createTempDir().getCanonicalFile
    checkpointDir.delete()

    val rocksDBStateDir = new File(checkpointDir.getAbsolutePath, "/state/0/0")
    val inputData = MemoryStream[Int]
    val aggregated =
      inputData.toDF()
        .groupBy($"value")
        .agg(count("*"))
        .as[(Int, Long)]

    // Run the stream with changelog checkpointing disabled.
    testStream(aggregated, Update)(
      StartStream(checkpointLocation = checkpointDir.getAbsolutePath,
        additionalConfs = Map(rocksdbChangelogCheckpointingConfKey -> "false")),
      AddData(inputData, 3),
      CheckLastBatch((3, 1)),
      AddData(inputData, 3, 2),
      CheckLastBatch((3, 2), (2, 1)),
      StopStream
    )
    assert(changelogVersionsPresent(rocksDBStateDir).isEmpty)
    assert(snapshotVersionsPresent(rocksDBStateDir) == List(1L, 2L))

    // Run the stream with changelog checkpointing enabled.
    testStream(aggregated, Update)(
      StartStream(checkpointLocation = checkpointDir.getAbsolutePath,
        additionalConfs = Map(rocksdbChangelogCheckpointingConfKey -> "true")),
      AddData(inputData, 3, 2, 1),
      CheckLastBatch((3, 3), (2, 2), (1, 1)),
      // By default we run in new tuple mode.
      AddData(inputData, 4, 4, 4, 4),
      CheckLastBatch((4, 4))
    )
    assert(changelogVersionsPresent(rocksDBStateDir) == List(3L, 4L))
  }
}
/*
class RocksDBStateStoreStreamingDeduplicationSuite
  extends StreamingDeduplicationSuite with RocksDBStateStoreTest  {}

class RocksDBStateStoreStreamingDeduplicationWithinWatermarkSuite
  extends StreamingDeduplicationWithinWatermarkSuite with RocksDBStateStoreTest  {}

class RocksDBStateStoreStreamingInnerJoinSuite
  extends StreamingInnerJoinSuite with RocksDBStateStoreTest  {}

class RocksDBStateStoreStreamingOuterJoinSuite
  extends StreamingOuterJoinSuite with RocksDBStateStoreTest  {}

class RocksDBStateStoreStreamingFullOuterJoinSuite
  extends StreamingFullOuterJoinSuite with RocksDBStateStoreTest  {}

class RocksDBStateStoreStreamingLeftSemiJoinSuite
  extends StreamingLeftSemiJoinSuite with RocksDBStateStoreTest  {}

class RocksDBStateStoreFlatMapGroupsWithStateSuite
  extends FlatMapGroupsWithStateSuite with RocksDBStateStoreTest {}
*/