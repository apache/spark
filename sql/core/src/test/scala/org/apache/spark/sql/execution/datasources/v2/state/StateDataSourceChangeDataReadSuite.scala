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

package org.apache.spark.sql.execution.datasources.v2.state

import java.io.File
import java.sql.Timestamp
import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.scalatest.Assertions

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.streaming.checkpointing.{CommitLog, CommitMetadata}
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamExecution}
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.functions.{col, window}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

class HDFSBackedStateDataSourceChangeDataReaderSuite extends StateDataSourceChangeDataReaderSuite {
  override protected def newStateStoreProvider(): HDFSBackedStateStoreProvider =
    new HDFSBackedStateStoreProvider
}

class RocksDBWithChangelogCheckpointStateDataSourceChangeDataReaderSuite extends
  StateDataSourceChangeDataReaderSuite {
  override protected def newStateStoreProvider(): RocksDBStateStoreProvider =
    new RocksDBStateStoreProvider

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled",
      "true")
  }
}

class RocksDBWithCheckpointV2StateDataSourceChangeDataReaderSuite extends
  RocksDBWithChangelogCheckpointStateDataSourceChangeDataReaderSuite {
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set("spark.sql.streaming.stateStore.checkpointFormatVersion", "2")
  }
}

abstract class StateDataSourceChangeDataReaderSuite extends StateDataSourceTestBase
  with Assertions {

  import testImplicits._
  import StateStoreTestsHelper._

  protected val keySchema: StructType = StateStoreTestsHelper.keySchema
  protected val valueSchema: StructType = StateStoreTestsHelper.valueSchema

  protected def newStateStoreProvider(): StateStoreProvider

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key, false)
    spark.conf.set(SQLConf.STATE_STORE_PROVIDER_CLASS.key, newStateStoreProvider().getClass.getName)
  }

  /**
   * Calls the overridable [[newStateStoreProvider]] to create the state store provider instance.
   * Initialize it with the configuration set by child classes.
   *
   * @param checkpointDir path to store state information
   * @return instance of class extending [[StateStoreProvider]]
   */
  private def getNewStateStoreProvider(checkpointDir: String): StateStoreProvider = {
    val provider = newStateStoreProvider()
    val conf = new Configuration
    conf.set(StreamExecution.RUN_ID_KEY, UUID.randomUUID().toString)
    provider.init(
      StateStoreId(checkpointDir, 0, 0),
      keySchema,
      valueSchema,
      NoPrefixKeyStateEncoderSpec(keySchema),
      useColumnFamilies = false,
      StateStoreConf(spark.sessionState.conf),
      conf)
    provider
  }

  test("ERROR: specify changeStartBatchId in normal mode") {
    withTempDir { tempDir =>
      val exc = intercept[StateDataSourceInvalidOptionValue] {
        spark.read.format("statestore")
          .option(StateSourceOptions.CHANGE_START_BATCH_ID, 0)
          .option(StateSourceOptions.CHANGE_END_BATCH_ID, 2)
          .load(tempDir.getAbsolutePath)
      }
      assert(exc.getCondition === "STDS_INVALID_OPTION_VALUE.WITH_MESSAGE")
    }
  }

  test("ERROR: changeStartBatchId is set to negative") {
    withTempDir { tempDir =>
      val exc = intercept[StateDataSourceInvalidOptionValueIsNegative] {
        spark.read.format("statestore")
          .option(StateSourceOptions.READ_CHANGE_FEED, value = true)
          .option(StateSourceOptions.CHANGE_START_BATCH_ID, -1)
          .option(StateSourceOptions.CHANGE_END_BATCH_ID, 0)
          .load(tempDir.getAbsolutePath)
      }
      assert(exc.getCondition === "STDS_INVALID_OPTION_VALUE.IS_NEGATIVE")
    }
  }

  test("ERROR: changeEndBatchId is set to less than changeStartBatchId") {
    withTempDir { tempDir =>
      val exc = intercept[StateDataSourceInvalidOptionValue] {
        spark.read.format("statestore")
          .option(StateSourceOptions.READ_CHANGE_FEED, value = true)
          .option(StateSourceOptions.CHANGE_START_BATCH_ID, 1)
          .option(StateSourceOptions.CHANGE_END_BATCH_ID, 0)
          .load(tempDir.getAbsolutePath)
      }
      assert(exc.getCondition === "STDS_INVALID_OPTION_VALUE.WITH_MESSAGE")
    }
  }

  test("ERROR: mixed checkpoint format versions not supported") {
    withTempDir { tempDir =>
      val commitLog = new CommitLog(spark,
        new File(tempDir.getAbsolutePath, "commits").getAbsolutePath)

      // Start version: treated as v1 (no operator unique ids)
      val startMetadata = CommitMetadata(0, None)
      assert(commitLog.add(0, startMetadata))

      // End version: treated as v2 (operator 0 has unique ids)
      val endMetadata = CommitMetadata(0,
        Some(Map[Long, Array[Array[String]]](0L -> Array(Array("uid")))))
      assert(commitLog.add(1, endMetadata))

      val exc = intercept[StateDataSourceMixedCheckpointFormatVersionsNotSupported] {
        spark.read.format("statestore")
          .option(StateSourceOptions.PATH, tempDir.getAbsolutePath)
          .option(StateSourceOptions.READ_CHANGE_FEED, true)
          .option(StateSourceOptions.CHANGE_START_BATCH_ID, 0)
          .option(StateSourceOptions.CHANGE_END_BATCH_ID, 1)
          .load()
      }

      checkError(exc, "STDS_MIXED_CHECKPOINT_FORMAT_VERSIONS_NOT_SUPPORTED", "KD002",
        Map(
          "startBatchId" -> "0",
          "endBatchId" -> "1",
          "startFormatVersion" -> "1",
          "endFormatVersion" -> "2"
        ))
    }
  }

  test("ERROR: joinSide option is used together with readChangeFeed") {
    withTempDir { tempDir =>
      val exc = intercept[StateDataSourceConflictOptions] {
        spark.read.format("statestore")
          .option(StateSourceOptions.READ_CHANGE_FEED, value = true)
          .option(StateSourceOptions.JOIN_SIDE, "left")
          .option(StateSourceOptions.CHANGE_START_BATCH_ID, 0)
          .option(StateSourceOptions.CHANGE_END_BATCH_ID, 0)
          .load(tempDir.getAbsolutePath)
      }
      assert(exc.getCondition === "STDS_CONFLICT_OPTIONS")
    }
  }

  test("getChangeDataReader of state store provider") {
    val versionToCkptId = scala.collection.mutable.Map[Long, Option[String]]()

    def withNewStateStore(provider: StateStoreProvider, version: Int)(f: StateStore => Unit):
      Unit = {
      val stateStore = provider.getStore(version, versionToCkptId.getOrElse(version, None))
      f(stateStore)
      stateStore.commit()

      val ssInfo = stateStore.getStateStoreCheckpointInfo()
      versionToCkptId(ssInfo.batchVersion) = ssInfo.stateStoreCkptId
    }

    withTempDir { tempDir =>
      val provider = getNewStateStoreProvider(tempDir.getAbsolutePath)
      withNewStateStore(provider, 0) { stateStore =>
        put(stateStore, "a", 1, 1) }
      withNewStateStore(provider, 1) { stateStore =>
        put(stateStore, "b", 2, 2) }
      withNewStateStore(provider, 2) { stateStore =>
        stateStore.remove(dataToKeyRow("a", 1)) }
      withNewStateStore(provider, 3) { stateStore =>
        stateStore.remove(dataToKeyRow("b", 2)) }

      val reader =
        provider.asInstanceOf[SupportsFineGrainedReplay]
          .getStateStoreChangeDataReader(1, 4, None, versionToCkptId.getOrElse(4, None))

      assert(reader.next() === (RecordType.PUT_RECORD, dataToKeyRow("a", 1), dataToValueRow(1), 0L))
      assert(reader.next() === (RecordType.PUT_RECORD, dataToKeyRow("b", 2), dataToValueRow(2), 1L))
      assert(reader.next() ===
        (RecordType.DELETE_RECORD, dataToKeyRow("a", 1), null, 2L))
      assert(reader.next() ===
        (RecordType.DELETE_RECORD, dataToKeyRow("b", 2), null, 3L))
    }
  }

  test("read global streaming limit state change feed") {
    withTempDir { tempDir =>
      val inputData = MemoryStream[Int]
      val df = inputData.toDF().limit(10)
      testStream(df)(
        StartStream(checkpointLocation = tempDir.getAbsolutePath),
        AddData(inputData, 1, 2, 3, 4),
        ProcessAllAvailable(),
        AddData(inputData, 5, 6, 7, 8),
        ProcessAllAvailable(),
        AddData(inputData, 9, 10, 11, 12),
        ProcessAllAvailable()
      )

      val stateDf = spark.read.format("statestore")
        .option(StateSourceOptions.READ_CHANGE_FEED, value = true)
        .option(StateSourceOptions.CHANGE_START_BATCH_ID, 0)
        .option(StateSourceOptions.CHANGE_END_BATCH_ID, 2)
        .load(tempDir.getAbsolutePath)

      val expectedDf = Seq(
        Row(0L, "update", Row(null), Row(4), 0),
        Row(1L, "update", Row(null), Row(8), 0),
        Row(2L, "update", Row(null), Row(10), 0)
      )

      checkAnswer(stateDf, expectedDf)
    }
  }

  test("read streaming aggregate state change feed") {
    withTempDir { tempDir =>
      val inputData = MemoryStream[Int]
      val df = inputData.toDF().groupBy("value").count()
      testStream(df, OutputMode.Update)(
        StartStream(checkpointLocation = tempDir.getAbsolutePath),
        AddData(inputData, 1, 2, 3, 4),
        ProcessAllAvailable(),
        AddData(inputData, 2, 3, 4, 5),
        ProcessAllAvailable(),
        AddData(inputData, 3, 4, 5, 6),
        ProcessAllAvailable()
      )

      val stateDf = spark.read.format("statestore")
        .option(StateSourceOptions.READ_CHANGE_FEED, value = true)
        .option(StateSourceOptions.CHANGE_START_BATCH_ID, 0)
        .option(StateSourceOptions.CHANGE_END_BATCH_ID, 2)
        .load(tempDir.getAbsolutePath)

      val expectedDf = Seq(
        Row(0L, "update", Row(3), Row(1), 1),
        Row(1L, "update", Row(3), Row(2), 1),
        Row(1L, "update", Row(5), Row(1), 1),
        Row(2L, "update", Row(3), Row(3), 1),
        Row(2L, "update", Row(5), Row(2), 1),
        Row(0L, "update", Row(4), Row(1), 2),
        Row(1L, "update", Row(4), Row(2), 2),
        Row(2L, "update", Row(4), Row(3), 2),
        Row(0L, "update", Row(1), Row(1), 3),
        Row(0L, "update", Row(2), Row(1), 4),
        Row(1L, "update", Row(2), Row(2), 4),
        Row(2L, "update", Row(6), Row(1), 4)
      )

      checkAnswer(stateDf, expectedDf)
    }
  }

  test("read streaming deduplication state change feed") {
    withTempDir { tempDir =>
      val inputData = MemoryStream[Int]
      val df = inputData.toDF().dropDuplicates("value")
      testStream(df, OutputMode.Update)(
        StartStream(checkpointLocation = tempDir.getAbsolutePath),
        AddData(inputData, 1, 2, 3, 4),
        ProcessAllAvailable(),
        AddData(inputData, 2, 3, 4, 5),
        ProcessAllAvailable(),
        AddData(inputData, 3, 4, 5, 6),
        ProcessAllAvailable()
      )

      val stateDf = spark.read.format("statestore")
        .option(StateSourceOptions.READ_CHANGE_FEED, value = true)
        .option(StateSourceOptions.CHANGE_START_BATCH_ID, 0)
        .option(StateSourceOptions.CHANGE_END_BATCH_ID, 2)
        .load(tempDir.getAbsolutePath)

      val expectedDf = Seq(
        Row(0L, "update", Row(1), Row(null), 3),
        Row(0L, "update", Row(2), Row(null), 4),
        Row(0L, "update", Row(3), Row(null), 1),
        Row(0L, "update", Row(4), Row(null), 2),
        Row(1L, "update", Row(5), Row(null), 1),
        Row(2L, "update", Row(6), Row(null), 4)
      )

      checkAnswer(stateDf, expectedDf)
    }
  }

  test("read stream-stream join state change feed") {
    withTempDir { tempDir =>
      val inputData = MemoryStream[(Int, Long)]
      val leftDf =
        inputData.toDF().select(col("_1").as("leftKey"), col("_2").as("leftValue"))
      val rightDf =
        inputData.toDF().select((col("_1") * 2).as("rightKey"), col("_2").as("rightValue"))
      val df = leftDf.join(rightDf).where("leftKey == rightKey")

      testStream(df)(
        StartStream(checkpointLocation = tempDir.getAbsolutePath),
        AddData(inputData, (1, 1L), (2, 2L)),
        ProcessAllAvailable(),
        AddData(inputData, (3, 3L), (4, 4L)),
        ProcessAllAvailable()
      )

      val keyWithIndexToValueDf = spark.read.format("statestore")
        .option(StateSourceOptions.STORE_NAME, "left-keyWithIndexToValue")
        .option(StateSourceOptions.READ_CHANGE_FEED, value = true)
        .option(StateSourceOptions.CHANGE_START_BATCH_ID, 0)
        .option(StateSourceOptions.CHANGE_END_BATCH_ID, 1)
        .load(tempDir.getAbsolutePath)

      val keyWithIndexToValueExpectedDf = Seq(
        Row(1L, "update", Row(3, 0L), Row(3, 3L, false), 1),
        Row(1L, "update", Row(4, 0L), Row(4, 4L, true), 2),
        Row(0L, "update", Row(1, 0L), Row(1, 1L, false), 3),
        Row(0L, "update", Row(2, 0L), Row(2, 2L, false), 4),
        Row(0L, "update", Row(2, 0L), Row(2, 2L, true), 4)
      )

      checkAnswer(keyWithIndexToValueDf, keyWithIndexToValueExpectedDf)

      val keyToNumValuesDf = spark.read.format("statestore")
        .option(StateSourceOptions.STORE_NAME, "left-keyToNumValues")
        .option(StateSourceOptions.READ_CHANGE_FEED, value = true)
        .option(StateSourceOptions.CHANGE_START_BATCH_ID, 0)
        .option(StateSourceOptions.CHANGE_END_BATCH_ID, 1)
        .load(tempDir.getAbsolutePath)

      val keyToNumValuesDfExpectedDf = Seq(
        Row(1L, "update", Row(3), Row(1L), 1),
        Row(1L, "update", Row(4), Row(1L), 2),
        Row(0L, "update", Row(1), Row(1L), 3),
        Row(0L, "update", Row(2), Row(1L), 4)
      )

      checkAnswer(keyToNumValuesDf, keyToNumValuesDfExpectedDf)
    }
  }

  test("read change feed past multiple snapshots") {
    withSQLConf("spark.sql.streaming.stateStore.minDeltasForSnapshot" -> "2") {
      withTempDir { tempDir =>
        val inputData = MemoryStream[Int]
        val df = inputData.toDF().groupBy("value").count()
        testStream(df, OutputMode.Update)(
          StartStream(checkpointLocation = tempDir.getAbsolutePath),
          AddData(inputData, 1, 2, 3, 4, 1),
          ProcessAllAvailable(),
          AddData(inputData, 2, 3, 4, 5),
          ProcessAllAvailable(),
          AddData(inputData, 3, 4, 5, 6),
          ProcessAllAvailable(),
          AddData(inputData, 1, 1),
          ProcessAllAvailable(),
          AddData(inputData, 1, 1),
          ProcessAllAvailable(),
          AddData(inputData, 1, 1),
          ProcessAllAvailable()
        )

        val stateDf = spark.read.format("statestore")
          .option(StateSourceOptions.READ_CHANGE_FEED, value = true)
          .option(StateSourceOptions.CHANGE_START_BATCH_ID, 0)
          .option(StateSourceOptions.CHANGE_END_BATCH_ID, 5)
          .load(tempDir.getAbsolutePath)

        val expectedDf = Seq(
          Row(0L, "update", Row(3), Row(1), 1),
          Row(1L, "update", Row(3), Row(2), 1),
          Row(1L, "update", Row(5), Row(1), 1),
          Row(2L, "update", Row(3), Row(3), 1),
          Row(2L, "update", Row(5), Row(2), 1),
          Row(0L, "update", Row(4), Row(1), 2),
          Row(1L, "update", Row(4), Row(2), 2),
          Row(2L, "update", Row(4), Row(3), 2),
          Row(0L, "update", Row(1), Row(2), 3),
          Row(3L, "update", Row(1), Row(4), 3),
          Row(4L, "update", Row(1), Row(6), 3),
          Row(5L, "update", Row(1), Row(8), 3),
          Row(0L, "update", Row(2), Row(1), 4),
          Row(1L, "update", Row(2), Row(2), 4),
          Row(2L, "update", Row(6), Row(1), 4)
        )

        checkAnswer(stateDf, expectedDf)

        val stateDf2 = spark.read.format("statestore")
          .option(StateSourceOptions.READ_CHANGE_FEED, value = true)
          .option(StateSourceOptions.CHANGE_START_BATCH_ID, 1)
          .option(StateSourceOptions.CHANGE_END_BATCH_ID, 3)
          .load(tempDir.getAbsolutePath)

        val expectedDf2 = Seq(
          Row(1L, "update", Row(3), Row(2), 1),
          Row(1L, "update", Row(5), Row(1), 1),
          Row(2L, "update", Row(3), Row(3), 1),
          Row(2L, "update", Row(5), Row(2), 1),
          Row(1L, "update", Row(4), Row(2), 2),
          Row(2L, "update", Row(4), Row(3), 2),
          Row(3L, "update", Row(1), Row(4), 3),
          Row(1L, "update", Row(2), Row(2), 4),
          Row(2L, "update", Row(6), Row(1), 4)
        )

        checkAnswer(stateDf2, expectedDf2)

        val stateDf3 = spark.read.format("statestore")
          .option(StateSourceOptions.READ_CHANGE_FEED, value = true)
          .option(StateSourceOptions.CHANGE_START_BATCH_ID, 2)
          .option(StateSourceOptions.CHANGE_END_BATCH_ID, 4)
          .load(tempDir.getAbsolutePath)

        val expectedDf3 = Seq(
          Row(2L, "update", Row(3), Row(3), 1),
          Row(2L, "update", Row(5), Row(2), 1),
          Row(2L, "update", Row(4), Row(3), 2),
          Row(3L, "update", Row(1), Row(4), 3),
          Row(4L, "update", Row(1), Row(6), 3),
          Row(2L, "update", Row(6), Row(1), 4)
        )

        checkAnswer(stateDf3, expectedDf3)
      }
    }
  }

  test("read change feed with delete entries") {
    withTempDir { tempDir =>
      val inputData = MemoryStream[(Int, Timestamp)]
      val df = inputData.toDF()
        .selectExpr("_1 as key", "_2 as ts")
        .withWatermark("ts", "1 second")
        .groupBy(window(col("ts"), "1 second"))
        .count()

      val ts0 = Timestamp.valueOf("2025-01-01 00:00:00")
      val ts1 = Timestamp.valueOf("2025-01-01 00:00:01")
      val ts2 = Timestamp.valueOf("2025-01-01 00:00:02")
      val ts3 = Timestamp.valueOf("2025-01-01 00:00:03")
      val ts4 = Timestamp.valueOf("2025-01-01 00:00:04")

      testStream(df, OutputMode.Append)(
        StartStream(checkpointLocation = tempDir.getAbsolutePath),
        AddData(inputData, (1, ts0), (2, ts0)),
        ProcessAllAvailable(),
        AddData(inputData, (3, ts2)),
        ProcessAllAvailable(),
        AddData(inputData, (4, ts3)),
        ProcessAllAvailable(),
        StopStream
      )

      val stateDf = spark.read.format("statestore")
        .option(StateSourceOptions.READ_CHANGE_FEED, value = true)
        .option(StateSourceOptions.CHANGE_START_BATCH_ID, 0)
        .load(tempDir.getAbsolutePath)

      val expectedDf = Seq(
        Row(0L, "update", Row(Row(ts0, ts1)), Row(2), 4),
        Row(1L, "update", Row(Row(ts2, ts3)), Row(1), 1),
        Row(2L, "delete", Row(Row(ts0, ts1)), null, 4),
        Row(2L, "update", Row(Row(ts3, ts4)), Row(1), 4)
      )

      checkAnswer(stateDf, expectedDf)
    }
  }
}
