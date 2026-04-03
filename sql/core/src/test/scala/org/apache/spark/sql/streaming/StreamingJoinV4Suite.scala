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

import org.apache.spark.sql.execution.streaming.checkpointing.CheckpointFileManager
import org.apache.spark.sql.execution.streaming.operators.stateful.join.StreamingSymmetricHashJoinExec
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
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
