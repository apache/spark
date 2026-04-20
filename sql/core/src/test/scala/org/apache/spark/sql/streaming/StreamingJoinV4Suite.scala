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

import org.apache.spark.sql.execution.datasources.v2.state.StateSourceOptions
import org.apache.spark.sql.execution.streaming.checkpointing.CheckpointFileManager
import org.apache.spark.sql.execution.streaming.operators.stateful.join.StreamingSymmetricHashJoinExec
import org.apache.spark.sql.execution.streaming.operators.stateful.join.StreamingSymmetricHashJoinHelper.{JoinStateKeyWatermarkPredicate, JoinStateValueWatermarkPredicate}
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
        .add("field0", IntegerType, nullable = false)
        .add("__event_time", LongType, nullable = false)

      val leftValueSchema: StructType = new StructType()
        .add("key", IntegerType, nullable = false)
        .add("leftValue", IntegerType, nullable = false)
        .add("matched", BooleanType)
      val rightValueSchema: StructType = new StructType()
        .add("key", IntegerType, nullable = false)
        .add("rightValue", IntegerType, nullable = false)
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
      // Only left side has watermark; ts is part of the join key, so
      // joinKeyOrdinalForWatermark is defined -> hasEventTime = true for both sides.

      val joined = df1.join(df2, Seq("key", "ts"))
        .select($"key", $"ts".cast("long"), $"leftValue", $"rightValue")

      testStream(joined)(
        StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
        // Use ts=20 for the row we expect to join against input2.
        // withWatermark("ts", "10 seconds") causes batch 0 to advance the watermark to
        // max(ts) - 10s = 10s. Because watermark-based cleanup is enabled,
        // MicroBatchExecution fires a no-data batch (shouldRunAnotherBatch) after
        // batch 0 that evicts any state rows with ts <= 10 (inclusive). Keeping
        // ts=20 > 10 ensures the row survives that eviction so the input2 row in
        // the following batch can match it.
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

          // Both secondary indexes should be populated because joinKeyOrdinalForWatermark
          // is defined (watermark on join key applies to both sides).
          assert(readStateStore(checkpointLoc, "left-tsWithKey") > 0,
            "left secondary index should be populated when watermark is on join key")
          assert(readStateStore(checkpointLoc, "right-tsWithKey") > 0,
            "right secondary index should be populated when watermark is on join key")
        },
        StopStream
      )
    }
  }

  testWithVirtualColumnFamilyJoins(
    "SPARK-56406: secondary index only populated on watermarked side for time interval join") {
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
      // Only left side has watermark; watermark is on a value column, not the join key.
      // joinKeyOrdinalForWatermark is None -> only left has hasEventTime = true.
      // Neither side can actually evict: the left state watermark is derived from the right
      // side's watermark via the join condition, which is absent here. The left secondary
      // index is populated but never used for eviction.

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

          // Left has watermark on a value column -> hasEventTime = true, secondary index populated.
          assert(readStateStore(checkpointLoc, "left-tsWithKey") > 0,
            "left secondary index should be populated (watermark on left value column)")
          // Right has no watermark -> hasEventTime = false, secondary index empty.
          assert(readStateStore(checkpointLoc, "right-tsWithKey") === 0,
            "right secondary index should be empty (no watermark on right side)")
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
}

@SlowSQLTest
class StreamingOuterJoinV4Suite
  extends StreamingOuterJoinBase
  with TestWithV4StateFormat

@SlowSQLTest
class StreamingFullOuterJoinV4Suite
  extends StreamingFullOuterJoinBase
  with TestWithV4StateFormat

@SlowSQLTest
class StreamingLeftSemiJoinV4Suite
  extends StreamingLeftSemiJoinBase
    with TestWithV4StateFormat
