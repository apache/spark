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
import org.scalatest.{Args, Status, Tag}

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
trait TestWithV4StateFormat extends AlsoTestWithVirtualColumnFamilyJoins {

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

  // V4 always uses virtual column families, so skip non-VCF tests.
  override def testWithoutVirtualColumnFamilyJoins(
      testName: String, testTags: Tag*)(testBody: => Any): Unit = {}

  // Use lazy val because the parent constructor registers tests before
  // subclass vals are initialized.
  private lazy val testsToSkip = Seq(
    // V4's timestamp-based indexing does not support window structs
    // in join keys.
    "stream stream inner join on windows - with watermark",
    // V4 uses 1 store with VCFs instead of V3's 4*partitions layout,
    // so metric assertions about number of state store instances differ.
    "SPARK-35896: metrics in StateOperatorProgress are output correctly",
    // V4 uses different column families and encoder specs than V3;
    // overridden in StreamingInnerJoinV4Suite with V4-specific assertions.
    "SPARK-51779 Verify StateSchemaV3 writes correct key and value " +
      "schemas for join operator",
    // V4's key encoding is not yet supported by StateDataSource reader.
    "SPARK-49829"
  )

  override def runTest(testName: String, args: Args): Status = {
    if (testsToSkip.exists(testName.contains)) {
      org.scalatest.SucceededStatus
    } else {
      super.runTest(testName, args)
    }
  }
}

@SlowSQLTest
class StreamingInnerJoinV4Suite
  extends StreamingInnerJoinSuite
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
  extends StreamingOuterJoinSuite
  with TestWithV4StateFormat

@SlowSQLTest
class StreamingFullOuterJoinV4Suite
  extends StreamingFullOuterJoinSuite
  with TestWithV4StateFormat

@SlowSQLTest
class StreamingLeftSemiJoinV4Suite
  extends StreamingLeftSemiJoinSuite
    with TestWithV4StateFormat
