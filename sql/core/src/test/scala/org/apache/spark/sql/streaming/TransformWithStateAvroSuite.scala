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

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.v2.state.StateSourceOptions
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.state.{RocksDBStateStoreProvider, StateStoreInvalidValueSchemaEvolution, StateStoreValueSchemaEvolutionThresholdExceeded}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.util.StreamManualClock

class TransformWithStateAvroSuite extends TransformWithStateSuite {

  import testImplicits._

  override protected def test(testName: String, testTags: Tag*)(testBody: => Any)
                             (implicit pos: Position): Unit = {
    super.test(s"$testName (encoding = Avro)", testTags: _*) {
      withSQLConf(SQLConf.STREAMING_STATE_STORE_ENCODING_FORMAT.key -> "avro") {
        testBody
      }
    }
  }

  test("transformWithState - incompatible schema evolution should fail") {
    withSQLConf(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
      SQLConf.STREAMING_STATE_STORE_ENCODING_FORMAT.key -> "avro",
      SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      withTempDir { dir =>
        val inputData = MemoryStream[String]

        // First run with String field
        val result1 = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new ProcessorV1(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result1, OutputMode.Update())(
          StartStream(checkpointLocation = dir.getCanonicalPath),
          AddData(inputData, "test1"),
          CheckNewAnswer("test1"),
          StopStream
        )

        // Second run with Long field
        val result2 = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new ProcessorV2(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result2, OutputMode.Update())(
          StartStream(checkpointLocation = dir.getCanonicalPath),
          AddData(inputData, "test2"),
          CheckNewAnswer("test2"),
          StopStream
        )

        // Third run with Int field - should fail
        val result3 = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new ProcessorV3(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result3, OutputMode.Update())(
          StartStream(checkpointLocation = dir.getCanonicalPath),
          AddData(inputData, "test3"),
          ExpectFailure[StateStoreInvalidValueSchemaEvolution] { e =>
            checkError(
              e.asInstanceOf[SparkUnsupportedOperationException],
              condition = "STATE_STORE_INVALID_VALUE_SCHEMA_EVOLUTION",
              parameters = Map(
                "oldValueSchema" -> "StructType(StructField(value1,StringType,true))",
                "newValueSchema" -> "StructType(StructField(value1,IntegerType,true))")
            )
          }
        )
      }
    }
  }

  test("transformWithState - value schema threshold exceeded") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString,
      SQLConf.STREAMING_VALUE_STATE_SCHEMA_EVOLUTION_THRESHOLD.key -> "0") {
      withTempDir { chkptDir =>
        val dirPath = chkptDir.getCanonicalPath
        val inputData = MemoryStream[String]
        val result1 = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountStatefulProcessorInt(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result1, OutputMode.Update())(
          StartStream(checkpointLocation = dirPath),
          AddData(inputData, "a"),
          CheckNewAnswer(("a", "1")),
          Execute { q =>
            assert(q.lastProgress.stateOperators(0).customMetrics.get("numValueStateVars") > 0)
            assert(q.lastProgress.stateOperators(0).customMetrics.get("numRegisteredTimers") == 0)
            assert(q.lastProgress.stateOperators(0).numRowsUpdated === 1)
          },
          AddData(inputData, "a", "b"),
          CheckNewAnswer(("a", "2"), ("b", "1")),
          StopStream,
          StartStream(checkpointLocation = dirPath),
          AddData(inputData, "a", "b"), // should remove state for "a" and not return anything for a
          CheckNewAnswer(("b", "2")),
          StopStream,
          Execute { q =>
            assert(q.lastProgress.stateOperators(0).numRowsUpdated === 1)
            assert(q.lastProgress.stateOperators(0).numRowsRemoved === 1)
          },
          StartStream(checkpointLocation = dirPath),
          AddData(inputData, "a", "c"), // should recreate state for "a" and return count as 1 and
          CheckNewAnswer(("a", "1"), ("c", "1"))
        )

        val result2 = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountStatefulProcessor(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result2, OutputMode.Update())(
          StartStream(checkpointLocation = dirPath),
          AddData(inputData, "a"),
          ExpectFailure[StateStoreValueSchemaEvolutionThresholdExceeded] { t =>
            checkError(
              t.asInstanceOf[StateStoreValueSchemaEvolutionThresholdExceeded],
              condition = "STATE_STORE_VALUE_SCHEMA_EVOLUTION_THRESHOLD_EXCEEDED",
              parameters = Map(
                "numSchemaEvolutions" -> "1",
                "maxSchemaEvolutions" -> "0",
                "colFamilyName" -> "countState"
              )
            )
          }
        )
      }
    }
  }

  test("transformWithState - upcasting should succeed") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      withTempDir { chkptDir =>
        val dirPath = chkptDir.getCanonicalPath
        val inputData = MemoryStream[String]
        val result1 = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountStatefulProcessorInt(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result1, OutputMode.Update())(
          StartStream(checkpointLocation = dirPath),
          AddData(inputData, "a"),
          CheckNewAnswer(("a", "1")),
          Execute { q =>
            assert(q.lastProgress.stateOperators(0).customMetrics.get("numValueStateVars") > 0)
            assert(q.lastProgress.stateOperators(0).customMetrics.get("numRegisteredTimers") == 0)
            assert(q.lastProgress.stateOperators(0).numRowsUpdated === 1)
          },
          AddData(inputData, "a", "b"),
          CheckNewAnswer(("a", "2"), ("b", "1")),
          StopStream,
          StartStream(checkpointLocation = dirPath),
          AddData(inputData, "a", "b"), // should remove state for "a" and not return anything for a
          CheckNewAnswer(("b", "2")),
          StopStream,
          Execute { q =>
            assert(q.lastProgress.stateOperators(0).numRowsUpdated === 1)
            assert(q.lastProgress.stateOperators(0).numRowsRemoved === 1)
          },
          StartStream(checkpointLocation = dirPath),
          AddData(inputData, "a", "c"), // should recreate state for "a" and return count as 1 and
          CheckNewAnswer(("a", "1"), ("c", "1"))
        )

        val result2 = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountStatefulProcessor(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result2, OutputMode.Update())(
          StartStream(checkpointLocation = dirPath),
          AddData(inputData, "a"),
          CheckNewAnswer(("a", "2")),
          AddData(inputData, "d"),
          CheckNewAnswer(("d", "1")),
          StopStream
        )
      }
    }
  }

  test("transformWithState - reordering fields should succeed") {
    withSQLConf(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
        classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      withTempDir { chkptDir =>
        val dirPath = chkptDir.getCanonicalPath
        val inputData = MemoryStream[String]

        // First run with initial field order
        val result1 = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountStatefulProcessorInitialOrder(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result1, OutputMode.Update())(
          StartStream(checkpointLocation = dirPath),
          AddData(inputData, "a"),
          CheckNewAnswer(("a", "1")),
          StopStream
        )

        // Second run with reordered fields
        val result2 = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountStatefulProcessorReorderedFields(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result2, OutputMode.Update())(
          StartStream(checkpointLocation = dirPath),
          AddData(inputData, "a"),
          CheckNewAnswer(("a", "2")), // Should continue counting from previous state
          StopStream
        )
      }
    }
  }

  test("transformWithState - adding field should succeed") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      withTempDir { chkptDir =>
        val dirPath = chkptDir.getCanonicalPath
        val inputData = MemoryStream[String]
        val result1 = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountStatefulProcessor(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result1, OutputMode.Update())(
          StartStream(checkpointLocation = dirPath),
          AddData(inputData, "a"),
          CheckNewAnswer(("a", "1")),
          Execute { q =>
            assert(q.lastProgress.stateOperators(0).customMetrics.get("numValueStateVars") > 0)
            assert(q.lastProgress.stateOperators(0).customMetrics.get("numRegisteredTimers") == 0)
            assert(q.lastProgress.stateOperators(0).numRowsUpdated === 1)
          },
          AddData(inputData, "a", "b"),
          CheckNewAnswer(("a", "2"), ("b", "1")),
          StopStream,
          StartStream(checkpointLocation = dirPath),
          AddData(inputData, "a", "b"), // should remove state for "a" and not return anything for a
          CheckNewAnswer(("b", "2")),
          StopStream,
          Execute { q =>
            assert(q.lastProgress.stateOperators(0).numRowsUpdated === 1)
            assert(q.lastProgress.stateOperators(0).numRowsRemoved === 1)
          },
          StartStream(checkpointLocation = dirPath),
          AddData(inputData, "a", "c"), // should recreate state for "a" and return count as 1 and
          CheckNewAnswer(("a", "1"), ("c", "1"))
        )

        val result2 = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountStatefulProcessorNestedLongs(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result2, OutputMode.Update())(
          StartStream(checkpointLocation = dirPath),
          AddData(inputData, "a"),
          CheckNewAnswer(("a", "2")),
          StopStream
        )
      }
    }
  }

  test("transformWithState - add and remove field between runs") {
    withSQLConf(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      withTempDir { dir =>
        val inputData = MemoryStream[String]

        // First run with original field names
        val result1 = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountStatefulProcessorInitialOrder(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result1, OutputMode.Update())(
          StartStream(checkpointLocation = dir.getCanonicalPath),
          AddData(inputData, "test1"),
          CheckNewAnswer(("test1", "1")),
          StopStream
        )

        // Second run with renamed field
        val result2 = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RenameEvolvedProcessor(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result2, OutputMode.Update())(
          StartStream(checkpointLocation = dir.getCanonicalPath),
          // Uses default value, does not factor previous value1 into this
          AddData(inputData, "test1"),
          CheckNewAnswer(("test1", "1")),
          // Verify we can write state with new field name
          AddData(inputData, "test2"),
          CheckNewAnswer(("test2", "1")),
          StopStream
        )
      }
    }
  }

  test("state data source - schema evolution with time travel support") {
    withSQLConf(
      rocksdbChangelogCheckpointingConfKey -> "true",
      SQLConf.STREAMING_MAINTENANCE_INTERVAL.key -> "100",
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key -> "1",
      SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key -> "1") {

      withTempDir { chkptDir =>
        val dirPath = chkptDir.getCanonicalPath
        val inputData = MemoryStream[String]

        val result1 = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountStatefulProcessorTwoLongs(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result1, OutputMode.Update())(
          StartStream(checkpointLocation = dirPath),
          AddData(inputData, "a"),
          CheckNewAnswer(("a", "1")),
          AddData(inputData, "b"),
          CheckNewAnswer(("b", "1")),
          ProcessAllAvailable(),
          Execute { _ => Thread.sleep(5000) },
          StopStream
        )

        val result2 = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RenameEvolvedProcessor(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result2, OutputMode.Update())(
          StartStream(checkpointLocation = dirPath),
          AddData(inputData, "c"),
          CheckNewAnswer(("c", "1")),
          AddData(inputData, "d"),
          CheckNewAnswer(("d", "1")),
          ProcessAllAvailable(),
          Execute { _ => Thread.sleep(5000) },
          StopStream
        )

        val oldStateDf = spark.read
          .format("statestore")
          .option("snapshotStartBatchId", 0)
          .option("batchId", 1)
          .option("snapshotPartitionId", 0)
          .option(StateSourceOptions.STATE_VAR_NAME, "countState")
          .load(dirPath)

        checkAnswer(
          oldStateDf.selectExpr(
            "key.value AS groupingKey",
            "value.value1 AS count"),
          Seq(Row("a", 1), Row("b", 1))
        )

        val evolvedStateDf1 = spark.read
          .format("statestore")
          .option("snapshotStartBatchId", 0)
          .option("batchId", 3)
          .option("snapshotPartitionId", 0)
          .option(StateSourceOptions.STATE_VAR_NAME, "countState")
          .load(dirPath)

        checkAnswer(
          evolvedStateDf1.selectExpr(
            "key.value AS groupingKey",
            "value.value4 AS count"),
          Seq(
            Row("a", null),
            Row("b", null),
            Row("c", 1),
            Row("d", 1)
          )
        )

        val evolvedStateDf = spark.read
          .format("statestore")
          .option("snapshotStartBatchId", 3)
          .option("snapshotPartitionId", 0)
          .option(StateSourceOptions.STATE_VAR_NAME, "countState")
          .load(dirPath)

        checkAnswer(
          evolvedStateDf.selectExpr(
            "key.value AS groupingKey",
            "value.value4 AS count"),
          Seq(
            Row("a", null),
            Row("b", null),
            Row("c", 1),
            Row("d", 1)
          )
        )
      }
    }
  }

  test("transformWithState - verify default values during schema evolution") {
    withSQLConf(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      withTempDir { dir =>
        val inputData = MemoryStream[String]

        // First run with basic schema
        val result1 = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new DefaultValueInitialProcessor(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result1, OutputMode.Update())(
          StartStream(checkpointLocation = dir.getCanonicalPath),
          AddData(inputData, "test1"),
          CheckNewAnswer(("test1", BasicState("test1".hashCode, "test1"))),
          StopStream
        )

        // Second run with evolved schema to check defaults
        val result2 = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new DefaultValueEvolvedProcessor(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result2, OutputMode.Update())(
          StartStream(checkpointLocation = dir.getCanonicalPath),

          // Check existing state - new fields should get default values
          AddData(inputData, "test1"),
          CheckNewAnswer(
            ("test1", EvolvedState(
              id = "test1".hashCode,
              name = "test1",
              count = 0L,
              active = false,
              score = 0.0
            ))
          ),

          // New state should get initialized values, not defaults
          AddData(inputData, "test2"),
          CheckNewAnswer(
            ("test2", EvolvedState(
              id = "test2".hashCode,
              name = "test2",
              count = 100L,
              active = true,
              score = 99.9
            ))
          ),
          StopStream
        )
      }
    }
  }

  test("transformWithState - removing field should succeed") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      withTempDir { chkptDir =>
        val dirPath = chkptDir.getCanonicalPath
        val inputData = MemoryStream[String]

        val result2 = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountStatefulProcessorTwoLongs(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result2, OutputMode.Update())(
          StartStream(checkpointLocation = dirPath),
          AddData(inputData, "a"),
          CheckNewAnswer(("a", "1")),
          StopStream
        )

        val result1 = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountStatefulProcessor(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result1, OutputMode.Update())(
          StartStream(checkpointLocation = dirPath),
          AddData(inputData, "a"),
          CheckNewAnswer(("a", "1")),
          StopStream
        )
      }
    }
  }

  test("test that invalid schema evolution " +
    "fails query for column family") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      withTempDir { checkpointDir =>
        val inputData = MemoryStream[String]
        val result1 = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountStatefulProcessor(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result1, OutputMode.Update())(
          StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
          AddData(inputData, "a"),
          CheckNewAnswer(("a", "1")),
          StopStream
        )
        val result2 = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountStatefulProcessorInt(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result2, OutputMode.Update())(
          StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
          AddData(inputData, "a"),
          ExpectFailure[StateStoreInvalidValueSchemaEvolution] { e =>
            checkError(
              e.asInstanceOf[SparkUnsupportedOperationException],
              condition = "STATE_STORE_INVALID_VALUE_SCHEMA_EVOLUTION",
              parameters = Map(
                "oldValueSchema" -> "StructType(StructField(value,LongType,true))",
                "newValueSchema" -> "StructType(StructField(value,IntegerType,true))")
            )
          }
        )
      }
    }
  }

  test("test that introducing TTL after restart fails query") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      withTempDir { checkpointDir =>
        val inputData = MemoryStream[String]
        val clock = new StreamManualClock
        val result = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountStatefulProcessor(),
            TimeMode.ProcessingTime(),
            OutputMode.Update())

        testStream(result, OutputMode.Update())(
          StartStream(
            trigger = Trigger.ProcessingTime("1 second"),
            checkpointLocation = checkpointDir.getCanonicalPath,
            triggerClock = clock),
          AddData(inputData, "a"),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(("a", "1")),
          AdvanceManualClock(1 * 1000),
          StopStream
        )
        val result2 = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountStatefulProcessorWithTTL(),
            TimeMode.ProcessingTime(),
            OutputMode.Update())
        testStream(result2, OutputMode.Update())(
          StartStream(
            trigger = Trigger.ProcessingTime("1 second"),
            checkpointLocation = checkpointDir.getCanonicalPath,
            triggerClock = clock),
          AddData(inputData, "a"),
          AdvanceManualClock(1 * 1000),
          ExpectFailure[StateStoreInvalidValueSchemaEvolution] { e =>
            checkError(
              e.asInstanceOf[SparkUnsupportedOperationException],
              condition = "STATE_STORE_INVALID_VALUE_SCHEMA_EVOLUTION",
              parameters = Map(
                "newValueSchema" -> ("StructType(StructField(value,StructType(StructField(" +
                  "value,LongType,true)),true),StructField(ttlExpirationMs,LongType,true))"),
                "oldValueSchema" -> "StructType(StructField(value,LongType,true))")
            )
          }
        )
      }
    }
  }
}
