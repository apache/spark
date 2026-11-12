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

import org.apache.hadoop.fs.Path
import org.scalatest.Tag

import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.v2.state.StateSourceOptions
import org.apache.spark.sql.execution.streaming.checkpointing.CheckpointFileManager
import org.apache.spark.sql.execution.streaming.operators.stateful.join.StreamingSymmetricHashJoinExec
import org.apache.spark.sql.execution.streaming.operators.stateful.join.StreamingSymmetricHashJoinHelper.{JoinStateKeyWatermarkPredicate, JoinStateValueWatermarkPredicate, LeftSide, RightSide}
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamExecution}
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.tags.SlowSQLTest

/**
 * Trait that overrides test execution to run with state format version 4.
 * V4 uses timestamp-based indexing with a secondary index and requires
 * RocksDB with virtual column families. The innermost withSQLConf wins,
 * so wrapping the test body overrides the V3 setting from the parent trait.
 */
trait TestWithV4StateFormat extends StreamingJoinSuite {
  override protected def testMode: Mode = Mode.WithVCF

  override def testWithVirtualColumnFamilyJoins(
      testName: String, testTags: Tag*)(testBody: => Any): Unit = {
    super.testWithVirtualColumnFamilyJoins(testName, testTags: _*) {
      withSQLConf(
        SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> "4"
      ) {
        testBody
      }
    }
  }
}

@SlowSQLTest
class StreamingInnerJoinV4Suite
  extends StreamingInnerJoinBase
  with TestWithV4StateFormat {

  import testImplicits._

  test("SPARK-55628: V4 state format is active in execution plan") {
    val input1 = MemoryStream[Int]
    val input2 = MemoryStream[Int]

    val df1 = input1.toDF()
      .select($"value" as "key", timestamp_seconds($"value") as "ts",
        ($"value" * 2) as "leftValue")
      .withWatermark("ts", "10 seconds")
    val df2 = input2.toDF()
      .select($"value" as "key", timestamp_seconds($"value") as "ts",
        ($"value" * 3) as "rightValue")
      .withWatermark("ts", "10 seconds")

    val joined = df1.join(df2, Seq("key"), "inner")

    testStream(joined)(
      AddData(input1, 1),
      CheckAnswer(),
      Execute { q =>
        val joinNodes = q.lastExecution.executedPlan.collect {
          case j: StreamingSymmetricHashJoinExec => j
        }
        assert(joinNodes.length == 1)
        assert(joinNodes.head.stateFormatVersion == 4)
      },
      StopStream
    )
  }

  test("state format version 4 promotes RocksDB merge operator version 1 for new joins") {
    withTempDir { checkpointDir =>
      withSQLConf(SQLConf.STATE_STORE_ROCKSDB_MERGE_OPERATOR_VERSION.key -> "1") {
        val input1 = MemoryStream[Int]
        val input2 = MemoryStream[Int]
        val df1 = input1.toDF().select($"value" as "key", ($"value" * 2) as "leftValue")
        val df2 = input2.toDF().select($"value" as "key", ($"value" * 3) as "rightValue")
        val joined = df1.join(df2, "key")

        def checkMergeOperatorVersion(query: StreamExecution): Unit = {
          val join = query.lastExecution.executedPlan.collectFirst {
            case join: StreamingSymmetricHashJoinExec => join
          }.getOrElse(fail("StreamingSymmetricHashJoinExec not found"))
          assert(join.stateFormatVersion == 4)
          assert(SQLConf.get.getConf(SQLConf.STATE_STORE_ROCKSDB_MERGE_OPERATOR_VERSION) == 1,
            "the guardrail must not mutate the caller's session")
        }

        testStream(joined)(
          StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
          AddData(input1, 1),
          AddData(input2, 1),
          CheckAnswer((1, 2, 3)),
          Execute { query =>
            checkMergeOperatorVersion(query)
            assert(query.offsetLog.getLatest().flatMap(_._2.metadataOpt)
              .flatMap(_.conf.get(SQLConf.STATE_STORE_ROCKSDB_MERGE_OPERATOR_VERSION.key))
              .contains("2"))
          },
          StopStream,
          StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
          AddData(input1, 2),
          AddData(input2, 2),
          CheckNewAnswer((2, 4, 6)),
          Execute(checkMergeOperatorVersion),
          StopStream
        )
      }
    }
  }

  test("state format version 4 rejects an incompatible merge operator in a physical plan") {
    val input1 = MemoryStream[Int]
    val input2 = MemoryStream[Int]
    val joined = input1.toDF().join(input2.toDF(), "value")

    testStream(joined)(
      AddData(input1, 1),
      CheckAnswer(),
      Execute { query =>
        val join = query.lastExecution.executedPlan.collectFirst {
          case join: StreamingSymmetricHashJoinExec => join
        }.getOrElse(fail("StreamingSymmetricHashJoinExec not found"))
        withSQLConf(SQLConf.STATE_STORE_ROCKSDB_MERGE_OPERATOR_VERSION.key -> "1") {
          // New streams promote v1 to v2 before planning. copy() rebuilds this join with the
          // caller's v1 setting so the incompatible-plan validation remains covered.
          val error = intercept[SparkIllegalArgumentException] {
            join.copy()
          }
          checkError(
            exception = error,
            condition = "STREAM_STREAM_JOIN_INCOMPATIBLE_STATE_STORE_CONFIGS",
            sqlState = Some("42K06"),
            parameters = Map(
              "stateFormatVersionConfig" -> SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key,
              "stateFormatVersion" -> "4",
              "mergeOperatorVersionConfig" ->
                SQLConf.STATE_STORE_ROCKSDB_MERGE_OPERATOR_VERSION.key,
              "mergeOperatorVersion" -> "1",
              "requiredMergeOperatorVersion" -> "2"))
        }
      },
      StopStream
    )
  }

  // V4 uses different column families (keyWithTsToValues, tsWithKey)
  // with timestamp-based key encoder specs instead of V3's
  // keyToNumValues/keyWithIndexToValue.
  testWithVirtualColumnFamilyJoins(
    "SPARK-55628: verify V4 state schema writes correct key and " +
      "value schemas for join operator") {
    withTempDir { checkpointDir =>
      val input1 = MemoryStream[Int]
      val input2 = MemoryStream[Int]

      val df1 = input1.toDF()
        .select($"value" as "key", ($"value" * 2) as "leftValue")
      val df2 = input2.toDF()
        .select($"value" as "key", ($"value" * 3) as "rightValue")
      val joined = df1.join(df2, "key")

      val metadataPathPostfix = "state/0/_stateSchema/default"
      val stateSchemaPath =
        new Path(checkpointDir.toString, s"$metadataPathPostfix")
      val hadoopConf = spark.sessionState.newHadoopConf()
      val fm =
        CheckpointFileManager.create(stateSchemaPath, hadoopConf)

      val keySchemaWithTimestamp = new StructType()
        .add("field0", IntegerType)
        .add("__event_time", LongType)

      val leftValueSchema: StructType = new StructType()
        .add("key", IntegerType)
        .add("leftValue", IntegerType)
        .add("matched", BooleanType)
      val rightValueSchema: StructType = new StructType()
        .add("key", IntegerType)
        .add("rightValue", IntegerType)
        .add("matched", BooleanType)

      val dummyValueSchema =
        StructType(Array(StructField("__dummy__", NullType)))

      val schemaLeftPrimary = StateStoreColFamilySchema(
        "left-keyWithTsToValues", 0,
        keySchemaWithTimestamp, 0, leftValueSchema,
        Some(TimestampAsPostfixKeyStateEncoderSpec(
          keySchemaWithTimestamp)),
        None
      )
      val schemaLeftSecondary = StateStoreColFamilySchema(
        "left-tsWithKey", 0,
        keySchemaWithTimestamp, 0, dummyValueSchema,
        Some(TimestampAsPrefixKeyStateEncoderSpec(
          keySchemaWithTimestamp)),
        None
      )
      val schemaRightPrimary = StateStoreColFamilySchema(
        "right-keyWithTsToValues", 0,
        keySchemaWithTimestamp, 0, rightValueSchema,
        Some(TimestampAsPostfixKeyStateEncoderSpec(
          keySchemaWithTimestamp)),
        None
      )
      val schemaRightSecondary = StateStoreColFamilySchema(
        "right-tsWithKey", 0,
        keySchemaWithTimestamp, 0, dummyValueSchema,
        Some(TimestampAsPrefixKeyStateEncoderSpec(
          keySchemaWithTimestamp)),
        None
      )

      testStream(joined)(
        StartStream(
          checkpointLocation = checkpointDir.getCanonicalPath),
        AddData(input1, 1),
        CheckAnswer(),
        AddData(input2, 1, 10),
        CheckNewAnswer((1, 2, 3)),
        Execute { q =>
          val schemaFilePath =
            fm.list(stateSchemaPath).toSeq.head.getPath
          val providerId = StateStoreProviderId(
            StateStoreId(checkpointDir.getCanonicalPath, 0, 0),
            q.lastProgress.runId
          )
          val checker = new StateSchemaCompatibilityChecker(
            providerId,
            hadoopConf,
            List(schemaFilePath)
          )
          val colFamilySeq = checker.readSchemaFile()
          assert(colFamilySeq.length == 4)
          assert(colFamilySeq.map(_.toString).toSet == Set(
            schemaLeftPrimary, schemaLeftSecondary,
            schemaRightPrimary, schemaRightSecondary
          ).map(_.toString))
        },
        StopStream
      )
    }
  }

  private def readStateStore(checkpointLoc: String, storeName: String): Long = {
    spark.read.format("statestore")
      .option(StateSourceOptions.PATH, checkpointLoc)
      .option(StateSourceOptions.STORE_NAME, storeName)
      .load()
      .count()
  }

  testWithVirtualColumnFamilyJoins(
    "SPARK-56406: secondary index is not populated for join without event time") {
    withTempDir { checkpointDir =>
      val input1 = MemoryStream[Int]
      val input2 = MemoryStream[Int]

      val df1 = input1.toDF()
        .select($"value" as "key", ($"value" * 2) as "leftValue")
      val df2 = input2.toDF()
        .select($"value" as "key", ($"value" * 3) as "rightValue")
      val joined = df1.join(df2, "key")

      testStream(joined)(
        StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
        AddData(input1, 1, 2, 3),
        CheckAnswer(),
        AddData(input2, 1, 2),
        CheckNewAnswer((1, 2, 3), (2, 4, 6)),
        Execute { _ =>
          val checkpointLoc = checkpointDir.getCanonicalPath

          assert(readStateStore(checkpointLoc, "left-keyWithTsToValues") > 0,
            "left primary store should have rows")
          assert(readStateStore(checkpointLoc, "right-keyWithTsToValues") > 0,
            "right primary store should have rows")

          assert(readStateStore(checkpointLoc, "left-tsWithKey") === 0,
            "left secondary index should be empty without event time")
          assert(readStateStore(checkpointLoc, "right-tsWithKey") === 0,
            "right secondary index should be empty without event time")
        },
        StopStream
      )
    }
  }

  testWithVirtualColumnFamilyJoins(
    "SPARK-56406: secondary index populated on both sides when watermark is on join key") {
    withTempDir { checkpointDir =>
      val input1 = MemoryStream[(Int, Int)]
      val input2 = MemoryStream[(Int, Int)]

      val df1 = input1.toDF().toDF("key", "time")
        .select($"key", timestamp_seconds($"time") as "ts", ($"key" * 2) as "leftValue")
        .withWatermark("ts", "10 seconds")
      val df2 = input2.toDF().toDF("key", "time")
        .select($"key", timestamp_seconds($"time") as "ts", ($"key" * 3) as "rightValue")
      // Only the left input has a watermark, on the join key ts. It bounds the right state,
      // which is evicted by key on ts. The left state is never evicted, because the right input
      // has no watermark, but both state managers store and look up rows at the ts key.

      val joined = df1.join(df2, Seq("key", "ts"))
        .select($"key", $"ts".cast("long"), $"leftValue", $"rightValue")

      testStream(joined)(
        StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
        // withWatermark("ts", "10 seconds") causes batch 0 to advance the watermark to
        // max(ts) - 10s = 10s. Because watermark-based cleanup is enabled,
        // MicroBatchExecution fires a no-data batch (shouldRunAnotherBatch) after
        // batch 0. It evicts right rows with ts <= 10 (inclusive) and keeps every left row.
        AddData(input1, (1, 20), (2, 10)),
        CheckAnswer(),
        AddData(input2, (1, 20)),
        CheckNewAnswer((1, 20, 2, 3)),
        Execute { _ =>
          val checkpointLoc = checkpointDir.getCanonicalPath

          assert(readStateStore(checkpointLoc, "left-keyWithTsToValues") > 0,
            "left primary store should have rows")
          assert(readStateStore(checkpointLoc, "right-keyWithTsToValues") > 0,
            "right primary store should have rows")

          // Both secondary indexes are populated: each side indexes its rows at the ts key,
          // although only the right side is evicted through it.
          assert(readStateStore(checkpointLoc, "left-tsWithKey") > 0,
            "left secondary index should be populated when watermark is on join key")
          assert(readStateStore(checkpointLoc, "right-tsWithKey") > 0,
            "right secondary index should be populated when watermark is on join key")
        },
        StopStream
      )
    }
  }

  test("one-sided watermark on an event-time key matches rows stored before a restart") {
    // Only leftTime has a watermark, so the left state is never evicted. A right row looks up
    // the left rows at its own key timestamp, including rows stored before the restart.
    val (leftInput, rightInput, joined) =
      setupJoinWithRangeCondition("inner", rightWatermark = false, joinOnTime = true)
    withSQLConf(SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false") {
      withTempDir { checkpointDir =>
        val startStream = StartStream(checkpointLocation = checkpointDir.getCanonicalPath)
        testStream(joined)(
          startStream,
          AddData(leftInput, (1, 5), (2, 6)),
          CheckNewAnswer(),
          StopStream,
          startStream,
          AddData(rightInput, (1, 5), (2, 7)),
          CheckNewAnswer(Row(1, 1, 5, 5)),
          AddData(leftInput, (3, 100)),
          CheckNewAnswer(),
          AddData(rightInput, (2, 6)),
          CheckNewAnswer(Row(2, 2, 6, 6)),
          StopStream)
      }
    }
  }

  testWithVirtualColumnFamilyJoins(
    "SPARK-56406: secondary index supports one-sided watermark time interval joins") {
    withTempDir { checkpointDir =>
      val leftInput = MemoryStream[(Int, Int)]
      val rightInput = MemoryStream[(Int, Int)]

      val df1 = leftInput.toDF().toDF("leftKey", "time")
        .select($"leftKey", timestamp_seconds($"time") as "leftTime",
          ($"leftKey" * 2) as "leftValue")
        .withWatermark("leftTime", "10 seconds")
      val df2 = rightInput.toDF().toDF("rightKey", "time")
        .select($"rightKey", timestamp_seconds($"time") as "rightTime",
          ($"rightKey" * 3) as "rightValue")
      // The left watermark and range condition permit right-state eviction. The right
      // secondary index must be populated even though rightTime has no watermark metadata.

      val joined = df1.join(df2,
        expr("leftKey = rightKey AND " +
          "leftTime BETWEEN rightTime - interval 5 seconds AND rightTime + interval 5 seconds"))
        .select($"leftKey", $"leftTime".cast("int"), $"rightTime".cast("int"))

      testStream(joined)(
        StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
        AddData(leftInput, (1, 10), (2, 20)),
        CheckAnswer(),
        AddData(rightInput, (1, 12)),
        CheckNewAnswer((1, 10, 12)),
        Execute { _ =>
          val checkpointLoc = checkpointDir.getCanonicalPath

          assert(readStateStore(checkpointLoc, "left-keyWithTsToValues") > 0,
            "left primary store should have rows")
          assert(readStateStore(checkpointLoc, "right-keyWithTsToValues") > 0,
            "right primary store should have rows")

          // Left has watermark on a value column -> hasTimestampForEviction = true,
          // secondary index populated.
          assert(readStateStore(checkpointLoc, "left-tsWithKey") > 0,
            "left secondary index should be populated (watermark on left value column)")
          assert(readStateStore(checkpointLoc, "right-tsWithKey") > 0,
            "right secondary index should be populated for range eviction")
        },
        StopStream
      )
    }
  }

  test("prevStateWatermark must be None in the first batch") {
    // Regression test for the IncrementalExecution guard: in the first batch
    // prevOffsetSeqMetadata is None, so eventTimeWatermarkForLateEvents must NOT
    // be passed to getStateWatermarkPredicates.  Without the guard the watermark
    // propagation framework yields Some(0) even in batch 0, which would cause
    // scanEvictedKeys to skip state entries at timestamp 0.
    val input1 = MemoryStream[(Int, Int)]
    val input2 = MemoryStream[(Int, Int)]

    val df1 = input1.toDF().toDF("key", "time")
      .select($"key", timestamp_seconds($"time") as "leftTime",
        ($"key" * 2) as "leftValue")
      .withWatermark("leftTime", "10 seconds")
    val df2 = input2.toDF().toDF("key", "time")
      .select($"key", timestamp_seconds($"time") as "rightTime",
        ($"key" * 3) as "rightValue")
      .withWatermark("rightTime", "10 seconds")

    val joined = df1.join(df2,
      df1("key") === df2("key") &&
        expr("leftTime >= rightTime - interval 5 seconds " +
          "AND leftTime <= rightTime + interval 5 seconds"),
      "inner")
      .select(df1("key"), $"leftTime".cast("long"), $"leftValue", $"rightValue")

    def extractPrevWatermarks(q: StreamExecution): (Option[Long], Option[Long]) = {
      val joinExec = q.lastExecution.executedPlan.collect {
        case j: StreamingSymmetricHashJoinExec => j
      }.head
      val leftPrev = joinExec.stateWatermarkPredicates.left.flatMap {
        case p: JoinStateKeyWatermarkPredicate => p.prevStateWatermark
        case p: JoinStateValueWatermarkPredicate => p.prevStateWatermark
      }
      val rightPrev = joinExec.stateWatermarkPredicates.right.flatMap {
        case p: JoinStateKeyWatermarkPredicate => p.prevStateWatermark
        case p: JoinStateValueWatermarkPredicate => p.prevStateWatermark
      }
      (leftPrev, rightPrev)
    }

    testStream(joined)(
      // First batch: prevStateWatermark must be None on both sides.
      MultiAddData(input1, (1, 5))(input2, (1, 5)),
      CheckNewAnswer((1, 5, 2, 3)),
      Execute { q =>
        val (leftPrev, rightPrev) = extractPrevWatermarks(q)
        assert(leftPrev.isEmpty,
          s"Left prevStateWatermark should be None in the first batch, got $leftPrev")
        assert(rightPrev.isEmpty,
          s"Right prevStateWatermark should be None in the first batch, got $rightPrev")
      },

      // Second batch: after watermark advances, prevStateWatermark should be set.
      MultiAddData(input1, (2, 30))(input2, (2, 30)),
      CheckNewAnswer((2, 30, 4, 6)),
      Execute { q =>
        val (leftPrev, rightPrev) = extractPrevWatermarks(q)
        assert(leftPrev.isDefined,
          "Left prevStateWatermark should be defined after the first batch")
        assert(rightPrev.isDefined,
          "Right prevStateWatermark should be defined after the first batch")
      },
      StopStream
    )
  }

  test("SPARK-56402: prevStateWatermark must be None under legacy single-watermark propagator " +
    "(STATEFUL_OPERATOR_ALLOW_MULTIPLE = false)") {
    // Guards against the propagator-type bug: in legacy single-watermark mode
    // (STATEFUL_OPERATOR_ALLOW_MULTIPLE = false), lateEvents == eviction for the
    // same batch. If we naively thread `eventTimeWatermarkForLateEvents` as
    // `prevStateWatermark`, the eviction scan range collapses to (wm, wm] = empty
    // from batch 2 onward, silently skipping every eligible eviction.
    // IncrementalExecution must fall back to None in legacy mode.
    withSQLConf(SQLConf.STATEFUL_OPERATOR_ALLOW_MULTIPLE.key -> "false") {
      val input1 = MemoryStream[(Int, Int)]
      val input2 = MemoryStream[(Int, Int)]

      val df1 = input1.toDF().toDF("key", "time")
        .select($"key", timestamp_seconds($"time") as "leftTime",
          ($"key" * 2) as "leftValue")
        .withWatermark("leftTime", "10 seconds")
      val df2 = input2.toDF().toDF("key", "time")
        .select($"key", timestamp_seconds($"time") as "rightTime",
          ($"key" * 3) as "rightValue")
        .withWatermark("rightTime", "10 seconds")

      val joined = df1.join(df2,
        df1("key") === df2("key") &&
          expr("leftTime >= rightTime - interval 5 seconds " +
            "AND leftTime <= rightTime + interval 5 seconds"),
        "inner")
        .select(df1("key"), $"leftTime".cast("long"), $"leftValue", $"rightValue")

      def extractPrevWatermarks(q: StreamExecution): (Option[Long], Option[Long]) = {
        val joinExec = q.lastExecution.executedPlan.collect {
          case j: StreamingSymmetricHashJoinExec => j
        }.head
        val leftPrev = joinExec.stateWatermarkPredicates.left.flatMap {
          case p: JoinStateKeyWatermarkPredicate => p.prevStateWatermark
          case p: JoinStateValueWatermarkPredicate => p.prevStateWatermark
        }
        val rightPrev = joinExec.stateWatermarkPredicates.right.flatMap {
          case p: JoinStateKeyWatermarkPredicate => p.prevStateWatermark
          case p: JoinStateValueWatermarkPredicate => p.prevStateWatermark
        }
        (leftPrev, rightPrev)
      }

      testStream(joined)(
        MultiAddData(input1, (1, 5))(input2, (1, 5)),
        CheckNewAnswer((1, 5, 2, 3)),

        // Batch 2+: even though prevOffsetSeqMetadata is now defined, legacy mode
        // must keep prevStateWatermark = None to avoid collapsing the eviction scan
        // range.
        MultiAddData(input1, (2, 30))(input2, (2, 30)),
        CheckNewAnswer((2, 30, 4, 6)),
        Execute { q =>
          val (leftPrev, rightPrev) = extractPrevWatermarks(q)
          assert(leftPrev.isEmpty,
            s"Left prevStateWatermark must be None under legacy propagator, got $leftPrev")
          assert(rightPrev.isEmpty,
            s"Right prevStateWatermark must be None under legacy propagator, got $rightPrev")
        },
        StopStream
      )
    }
  }

  private def joinExec(query: StreamExecution): StreamingSymmetricHashJoinExec =
    query.lastExecution.executedPlan.collectFirst {
      case join: StreamingSymmetricHashJoinExec => join
    }.getOrElse(fail("StreamingSymmetricHashJoinExec not found"))

  test("matching scans read the column each side's state is ordered by") {
    // The shape of Nexmark q9: a bid matches an auction between its start and its expiry.
    val auctions = MemoryStream[(Int, Long, Long)]
    val bids = MemoryStream[(Int, Long, Long)]
    val left = auctions.toDF().toDF("id", "start", "end")
      .select($"id", timestamp_millis($"start").as("dateTime"),
        timestamp_millis($"end").as("expires"))
      .withWatermark("dateTime", "10 seconds")
    val right = bids.toDF().toDF("auction", "time", "price")
      .select($"auction", timestamp_millis($"time").as("bidTime"), $"price")
      .withWatermark("bidTime", "10 seconds")
    val joined = left.join(right, expr("id = auction AND bidTime BETWEEN dateTime AND expires"))
      .select($"id", $"price")

    testStream(joined)(
      AddData(auctions, (1, 100000L, 200000L)),
      CheckNewAnswer(),
      AddData(bids, (1, 99999L, 1L), (1, 100000L, 2L), (1, 150000L, 3L), (1, 200000L, 4L),
        (1, 200001L, 5L)),
      CheckNewAnswer((1, 2L), (1, 3L), (1, 4L)),
      AddData(auctions, (1, 150000L, 160000L)),
      CheckNewAnswer((1, 3L)),
      Execute { query =>
        val join = joinExec(query)
        // The left side evicts on expires and the right side on bidTime, so a left row
        // (id, dateTime, expires) reads bids from dateTime to expires, and a right row
        // (auction, bidTime, price) reads auctions that expire at or after bidTime.
        assert(join.matchingScanWindow(LeftSide).flatMap(
          _.rangeFor(InternalRow(1, 100000000L, 200000000L))) === Some((100000000L, 200000000L)))
        assert(join.matchingScanWindow(RightSide).flatMap(
          _.rangeFor(InternalRow(1, 150000000L, 3L))) === Some((150000000L, Long.MaxValue)))
      })
  }

  test("matching scans open at one end for a one-sided range") {
    val leftInput = MemoryStream[(Int, Long)]
    val rightInput = MemoryStream[(Int, Long)]
    val left = leftInput.toDF().toDF("leftKey", "time")
      .select($"leftKey", timestamp_millis($"time").as("leftTime"))
      .withWatermark("leftTime", "1 hour")
    val right = rightInput.toDF().toDF("rightKey", "time")
      .select($"rightKey", timestamp_millis($"time").as("rightTime"))
      .withWatermark("rightTime", "1 hour")
    val joined = left.join(right,
      expr("leftKey = rightKey AND rightTime <= leftTime + INTERVAL 10 SECONDS"))
      .select(unix_millis($"leftTime"), unix_millis($"rightTime"))

    testStream(joined)(
      AddData(leftInput, (1, 100000L), (1, 80000L)),
      CheckNewAnswer(),
      // A right row reads left rows from 10 seconds before it, open at the top.
      AddData(rightInput, (1, 90000L), (1, 110000L), (1, 110001L)),
      CheckNewAnswer((100000L, 90000L), (80000L, 90000L), (100000L, 110000L)),
      // A left row reads right rows up to 10 seconds after it, open at the bottom.
      AddData(leftInput, (1, 100001L)),
      CheckNewAnswer((100001L, 90000L), (100001L, 110000L), (100001L, 110001L)),
      Execute { query =>
        val join = joinExec(query)
        assert(join.matchingScanWindow(LeftSide).flatMap(_.rangeFor(InternalRow(1, 0L))) ===
          Some((Long.MinValue, 10000000L)))
        assert(join.matchingScanWindow(RightSide).flatMap(_.rangeFor(InternalRow(1, 0L))) ===
          Some((-10000000L, Long.MaxValue)))
      })
  }

  test("matching scans read every row of a key ordered by an equality key") {
    val leftInput = MemoryStream[(Int, Long)]
    val rightInput = MemoryStream[(Int, Long)]
    val left = leftInput.toDF().toDF("leftKey", "time")
      .select($"leftKey", timestamp_millis($"time").as("leftTime"))
      .withWatermark("leftTime", "10 seconds")
    val right = rightInput.toDF().toDF("rightKey", "time")
      .select($"rightKey", timestamp_millis($"time").as("rightTime"))
      .withWatermark("rightTime", "10 seconds")
    val joined = left.join(right, expr("leftKey = rightKey AND leftTime = rightTime"))
      .select($"leftKey")

    testStream(joined)(
      MultiAddData(leftInput, (1, 1000L))(rightInput, (1, 1000L)),
      CheckNewAnswer(1),
      Execute { query =>
        // Both sides are ordered by the event-time key, whose lookups read one timestamp.
        val join = joinExec(query)
        assert(join.matchingScanWindow(LeftSide).isEmpty)
        assert(join.matchingScanWindow(RightSide).isEmpty)
      })
  }
}

@SlowSQLTest
class StreamingOuterJoinV4Suite
  extends StreamingOuterJoinBase
  with TestWithV4StateFormat {

  import testImplicits._

  for (history <- Seq("continuous", "restart")) {
    test(s"V4 eviction includes newly admitted rows below the previous bound, $history") {
      val leftInput = MemoryStream[(Int, Int)]
      val rightInput = MemoryStream[(Int, Int)]
      val left = leftInput.toDF().toDF("leftKey", "time")
        .select($"leftKey", timestamp_seconds($"time").as("leftTime"))
        .withWatermark("leftTime", "0 seconds")
      val right = rightInput.toDF().toDF("rightKey", "time")
        .select($"rightKey", timestamp_seconds($"time").as("rightTime"))
        .withWatermark("rightTime", "0 seconds")
      val joined = left.join(right,
        expr("leftKey = rightKey AND leftTime > rightTime + INTERVAL 5 SECONDS"),
        "left_outer")
        .select($"leftKey", $"rightKey", $"leftTime".cast("long"), $"rightTime".cast("long"))

      withSQLConf(SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false") {
        withTempDir { checkpointDir =>
          val start = StartStream(checkpointLocation = checkpointDir.getCanonicalPath)
          // All watermarks below are in seconds. inputW is the minimum of the two sides'
          // maximum event times before the batch and drives eviction. Late input uses the
          // previous batch's inputW. The condition gives leftStateW = inputW + 5, but no
          // rightStateW: old right rows can still match future left rows.
          val setup = Seq(start,
            // Batch 1: inputW = 0, leftStateW = 5, rightStateW = None.
            MultiAddData(leftInput, (9, 20))(rightInput, (9, 10)),
            CheckNewAnswer(Row(9, 9, 20, 10)),
            // Batch 2: inputW = 10, leftStateW = 15, rightStateW = None.
            AddData(rightInput, (2, 20)), CheckNewAnswer()) ++
            (if (history == "restart") Seq(StopStream, start) else Nil)
          // Batch 3: inputW = 20, leftStateW = 25, rightStateW = None.
          // Late input uses inputW = 10 from batch 2, so the 12-second row is accepted.
          // The old scan started above batch 2's leftStateW = 15, skipping this row.
          testStream(joined)((setup ++ Seq(
            MultiAddData(leftInput, (1, 12))(rightInput, (2, 30)),
            CheckNewAnswer(Row(1, null, 12, null)),
            // Batch 4: inputW = 20, leftStateW = 25, rightStateW = None. No duplicate output.
            AddData(rightInput, (2, 50)), CheckNewAnswer(), StopStream)): _*)
        }
      }
    }
  }

  test("right outer join with one-sided watermark on an event-time key matches rows " +
      "stored before a restart") {
    // Only leftTime has a watermark. It bounds the right state, and the left state is never
    // evicted. A right row looks up the left rows at its own key timestamp.
    val (leftInput, rightInput, joined) =
      setupJoinWithRangeCondition("right_outer", rightWatermark = false, joinOnTime = true)
    withSQLConf(SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false") {
      withTempDir { checkpointDir =>
        val startStream = StartStream(checkpointLocation = checkpointDir.getCanonicalPath)
        testStream(joined)(
          startStream,
          AddData(leftInput, (1, 5), (2, 6)),
          CheckNewAnswer(),
          StopStream,
          startStream,
          AddData(rightInput, (1, 5), (2, 7)),
          CheckNewAnswer(Row(1, 1, 5, 5)),
          // The watermark advances to 90.
          AddData(leftInput, (3, 100)),
          CheckNewAnswer(),
          AddData(rightInput, (2, 6)),
          CheckNewAnswer(Row(2, 2, 6, 6), Row(null, 2, null, 7)),
          StopStream)
      }
    }
  }
}

@SlowSQLTest
class StreamingFullOuterJoinV4Suite
  extends StreamingFullOuterJoinBase
  with TestWithV4StateFormat {

  import testImplicits._

  Seq("left", "right").foreach { nullableWindowSide =>
    test(s"full outer join emits null $nullableWindowSide window keys on arrival") {
      val nullWindowOnLeft = nullableWindowSide == "left"
      val nullInput = MemoryStream[(Int, Int)]
      val boundedInput = MemoryStream[(Int, Int)]
      val windowType = new StructType()
        .add("start", TimestampType)
        .add("end", TimestampType)

      def nullableWindowPlan(input: MemoryStream[(Int, Int)], timeName: String,
          windowName: String): DataFrame = {
        input.toDF().toDF("a", "b")
          .select(timestamp_seconds($"a").as(timeName),
            when($"b" < 0, lit(null).cast(windowType)).otherwise(
              struct(
                timestamp_seconds($"b").as("start"),
                timestamp_seconds($"b" + 10).as("end"))).as(windowName))
          .withWatermark(timeName, "10 seconds")
      }

      def boundedWindowPlan(input: MemoryStream[(Int, Int)], timeName: String,
          windowName: String): DataFrame = {
        input.toDF().toDF("a", "b")
          .select(timestamp_seconds($"a").as(timeName), timestamp_seconds($"b").as("t"))
          .withWatermark("t", "10 seconds")
          .select(col(timeName), window($"t", "10 seconds").as(windowName))
      }

      val (leftInput, rightInput, left, right) = if (nullWindowOnLeft) {
        val left = nullableWindowPlan(nullInput, "la", "lb")
        val right = boundedWindowPlan(boundedInput, "ra", "rb")
        (nullInput, boundedInput, left, right)
      } else {
        val left = boundedWindowPlan(boundedInput, "la", "lb")
        val right = nullableWindowPlan(nullInput, "ra", "rb")
        (boundedInput, nullInput, left, right)
      }

      val joined = left.join(right, expr("la = ra AND lb = rb"), "full_outer")
        .select(
          $"la".cast("long"),
          $"lb.end".cast("long"),
          $"ra".cast("long"),
          $"rb.end".cast("long"))

      withSQLConf(SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false") {
        withTempDir { checkpointDir =>
          // A NULL window key can never match, so it is emitted at once and never stored.
          val (nullWindowRow, oldBoundedWindowRow) = if (nullWindowOnLeft) {
            (Row(100, null, null, null), Row(110, 120, null, null))
          } else {
            (Row(null, null, 100, null), Row(null, null, 110, 120))
          }
          val laterBoundedWindowRows = Seq(
            Row(1000, 1010, null, null),
            Row(null, null, 2000, 2010))
          val startStream = StartStream(checkpointLocation = checkpointDir.getCanonicalPath)

          testStream(joined)(
            startStream,
            AddData(nullInput, (100, -1), (110, 110)),
            CheckNewAnswer(nullWindowRow),
            StopStream,
            startStream,
            MultiAddData(leftInput, (1000, 1000))(rightInput, (2000, 2000)),
            CheckNewAnswer(),
            AddData(leftInput),
            CheckNewAnswer(oldBoundedWindowRow),
            assertNumStateRows(total = 2, updated = 2),
            StopStream,
            startStream,
            MultiAddData(leftInput, (3000, 3000))(rightInput, (4000, 4000)),
            CheckNewAnswer(),
            AddData(leftInput),
            CheckNewAnswer(laterBoundedWindowRows: _*),
            assertNumStateRows(total = 2, updated = 2),
            StopStream)
        }
      }
    }
  }
}

@SlowSQLTest
class StreamingLeftSemiJoinV4Suite
  extends StreamingLeftSemiJoinBase
    with TestWithV4StateFormat
