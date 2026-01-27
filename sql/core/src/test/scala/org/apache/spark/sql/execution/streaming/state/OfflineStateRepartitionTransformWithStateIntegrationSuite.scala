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
package org.apache.spark.sql.execution.streaming.state

import java.sql.Timestamp
import java.time.Duration

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.functions.{col, timestamp_seconds}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{InputEvent, ListStateTTLProcessor, MapInputEvent, MapOutputEvent, MapStateTTLProcessor, OutputEvent, OutputMode, RunningCountStatefulProcessorWithProcTimeTimer, TimeMode, Trigger, TTLConfig, ValueStateTTLProcessor}
import org.apache.spark.sql.streaming.util.{EventTimeTimerProcessor, MultiStateVarProcessor, MultiStateVarProcessorTestUtils, TimerTestUtils, TTLProcessorUtils}

/**
 * Integration test suite for transformWithState operator repartitioning.
 */
class OfflineStateRepartitionTransformWithStateIntegrationSuite
  extends OfflineStateRepartitionIntegrationSuiteBase {

  import testImplicits._

  def testWithDifferentEncodingType(testNamePrefix: String)
      (testFun: Int => Unit): Unit = {
    Seq("unsaferow", "avro").foreach { encodingFormat =>
      testWithAllRepartitionOperations(
        s"$testNamePrefix (encoding = $encodingFormat)") { newPartitions =>
        withSQLConf(SQLConf.STREAMING_STATE_STORE_ENCODING_FORMAT.key -> encodingFormat) {
          testFun(newPartitions)
        }
      }
    }
  }

  testWithDifferentEncodingType(
    "transformWithState with multiple column families") {
    newPartitions =>
      val allColFamilyNames = MultiStateVarProcessorTestUtils.ALL_COLUMN_FAMILIES.toList
      val stateSourceOptions = buildStateSourceOptionsForTWS(
        allColFamilyNames,
        listStateName = Some(MultiStateVarProcessorTestUtils.ITEMS_LIST))
      val selectExprs = MultiStateVarProcessorTestUtils.getColumnFamilyToSelectExprs()

      def buildQuery(inputData: MemoryStream[String]): Dataset[(String, String)] = {
        inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new MultiStateVarProcessor(),
            TimeMode.None(),
            OutputMode.Update())
      }

      testRepartitionWorkflow[String](
        newPartitions = newPartitions,
        setupInitialState = (inputData, checkpointDir, _) => {
          val query = buildQuery(inputData)
          testStream(query)(
            StartStream(checkpointLocation = checkpointDir),
            // Batch 1: Creates state in all column families
            AddData(inputData, "a", "b", "c"),
            CheckNewAnswer(("a", "1"), ("b", "1"), ("c", "1")),
            // Batch 2: Adds more state
            AddData(inputData, "a", "b", "d"),
            CheckNewAnswer(("a", "2"), ("b", "2"), ("d", "1")),
            StopStream
          )
        },
        verifyResumedQuery = (inputData, checkpointDir, _) => {
          val query = buildQuery(inputData)
          testStream(query)(
            StartStream(checkpointLocation = checkpointDir),
            // Batch 3: Resume with new data after repartition
            AddData(inputData, "a", "c", "e"),
            CheckNewAnswer(("a", "3"), ("c", "2"), ("e", "1"))
          )
        },
        storeToColumnFamilyToStateSourceOptions = Map(
          StateStoreId.DEFAULT_STORE_NAME -> stateSourceOptions
        ),
        storeToColumnFamilyToSelectExprs = Map(
          StateStoreId.DEFAULT_STORE_NAME -> selectExprs
        )
      )
  }

  testWithDifferentEncodingType("transformWithState with eventTime timers") {
    newPartitions =>
      val columnFamilies = TimerTestUtils
        .getTimerConfigsForCountState(TimeMode.EventTime()).keys.toSeq
        .filterNot(_ == StateStore.DEFAULT_COL_FAMILY_NAME)
      val stateSourceOptions = buildStateSourceOptionsForTWS(
        columnFamilies, timeMode = Some(TimeMode.EventTime()))
      val selectExprs = TimerTestUtils.getTimerColumnFamilyToSelectExprs(TimeMode.EventTime())

      def buildQuery(inputData: MemoryStream[(String, Long)]): Dataset[(String, String)] = {
        inputData.toDS()
          .select(col("_1").as("key"), timestamp_seconds(col("_2")).as("eventTime"))
          .withWatermark("eventTime", "10 seconds")
          .as[(String, Timestamp)]
          .groupByKey(_._1)
          .transformWithState(
            new EventTimeTimerProcessor(),
            TimeMode.EventTime(),
            OutputMode.Update())
      }

      testRepartitionWorkflow[(String, Long)](
        newPartitions = newPartitions,
        setupInitialState = (inputData, checkpointDir, _) => {
          val query = buildQuery(inputData)
          testStream(query, OutputMode.Update())(
            StartStream(checkpointLocation = checkpointDir),
            // Batch 1: Creates state with event time timers
            AddData(inputData, ("a", 1L), ("b", 2L), ("c", 3L)),
            CheckNewAnswer(("a", "1"), ("b", "1"), ("c", "1")),
            // Batch 2: More data
            AddData(inputData, ("a", 5L), ("d", 6L)),
            CheckNewAnswer(("a", "2"), ("d", "1")),
            StopStream
          )
        },
        verifyResumedQuery = (inputData, checkpointDir, _) => {
          val query = buildQuery(inputData)
          testStream(query, OutputMode.Update())(
            StartStream(checkpointLocation = checkpointDir),
            // Batch 3: Resume with new data after repartition
            AddData(inputData, ("a", 10L), ("e", 11L)),
            // Simply maintaining a count for each key
            CheckNewAnswer(("a", "3"), ("e", "1"))
          )
        },
        storeToColumnFamilyToStateSourceOptions = Map(
          StateStoreId.DEFAULT_STORE_NAME -> stateSourceOptions
        ),
        storeToColumnFamilyToSelectExprs = Map(
          StateStoreId.DEFAULT_STORE_NAME -> selectExprs
        )
      )
  }

  testWithDifferentEncodingType("transformWithState with processing time timers") {
    newPartitions =>
      val schemas = TimerTestUtils.getTimerConfigsForCountState(TimeMode.ProcessingTime())
      val columnFamilies = schemas.keys.toSeq.filterNot(_ == StateStore.DEFAULT_COL_FAMILY_NAME)
      val stateSourceOptions = buildStateSourceOptionsForTWS(
        columnFamilies,
        timeMode = Some(TimeMode.ProcessingTime()))
      val selectExprs = TimerTestUtils.getTimerColumnFamilyToSelectExprs(TimeMode.ProcessingTime())

      def buildQuery(inputData: MemoryStream[String]): Dataset[(String, String)] = {
        inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountStatefulProcessorWithProcTimeTimer(),
            TimeMode.ProcessingTime(),
            OutputMode.Update())
      }

      testRepartitionWorkflow[String](
        newPartitions = newPartitions,
        setupInitialState = (inputData, checkpointDir, clockOpt) => {
          val clock = clockOpt.get
          val query = buildQuery(inputData)
          testStream(query, OutputMode.Update())(
            StartStream(checkpointLocation = checkpointDir,
              trigger = Trigger.ProcessingTime("1 second"),
              triggerClock = clock),
            AddData(inputData, "a", "b"),
            AdvanceManualClock(1000),
            CheckNewAnswer(("a", "1"), ("b", "1")),
            AddData(inputData, "a", "c"),
            AdvanceManualClock(1000),
            CheckNewAnswer(("a", "2"), ("c", "1")),
            StopStream
          )
        },
        verifyResumedQuery = (inputData, checkpointDir, clockOpt) => {
          val clock = clockOpt.get
          val query = buildQuery(inputData)
          testStream(query, OutputMode.Update())(
            StartStream(checkpointLocation = checkpointDir,
              trigger = Trigger.ProcessingTime("1 second"),
              triggerClock = clock),
            AddData(inputData, "c", "d"),
            AdvanceManualClock(5 * 1000),
            // "a" and "c" are expired, and processor fires eventTime with "-1"
            CheckNewAnswer(("a", "-1"), ("c", "-1"), ("c", "2"), ("d", "1")),
            AddData(inputData, "c"),
            AdvanceManualClock(1000),
            // "c" is cleared after timer went off, so recount from 1
            CheckNewAnswer(("c", "1"))
          )
        },
        useManualClock = true,
        storeToColumnFamilyToStateSourceOptions = Map(
          StateStoreId.DEFAULT_STORE_NAME -> stateSourceOptions
        ),
        storeToColumnFamilyToSelectExprs = Map(
          StateStoreId.DEFAULT_STORE_NAME -> selectExprs
        )
      )
  }

  testWithDifferentEncodingType("transformWithState with list and TTL") {
    newPartitions =>
      val schemas = TTLProcessorUtils.getListStateTTLSchemasWithMetadata()
      val columnFamilies = schemas.keys.toSeq.filterNot(_ == StateStore.DEFAULT_COL_FAMILY_NAME)
      val stateSourceOptions = buildStateSourceOptionsForTWS(
        columnFamilies,
        listStateName = Some(TTLProcessorUtils.LIST_STATE))
      val selectExprs = TTLProcessorUtils.getTTLSelectExpressions(columnFamilies)

      def buildQuery(inputData: MemoryStream[InputEvent]): Dataset[OutputEvent] = {
        val ttlConfig = TTLConfig(ttlDuration = Duration.ofMinutes(1))
        inputData.toDS()
          .groupByKey(x => x.key)
          .transformWithState(new ListStateTTLProcessor(ttlConfig),
            TimeMode.ProcessingTime(),
            OutputMode.Update())
      }

      testRepartitionWorkflow[InputEvent](
        newPartitions = newPartitions,
        setupInitialState = (inputData, checkpointDir, clockOpt) => {
          val clock = clockOpt.get
          val query = buildQuery(inputData)
          testStream(query, OutputMode.Update())(
            StartStream(checkpointLocation = checkpointDir,
              trigger = Trigger.ProcessingTime("1 second"),
              triggerClock = clock),
            // Batch 1: Clock advances to 1000ms, TTL = 1000 + 60000 = 61000ms
            AddData(inputData, InputEvent("k1", "put", 1),
              InputEvent("k1", "get_ttl_value_from_state", 0)),
            AdvanceManualClock(1 * 1000),
            CheckNewAnswer(OutputEvent("k1", 1, true, 61000)),
            // Batch 2: Clock advances to 2000ms, TTL = 2000 + 60000 = 62000ms
            AddData(inputData, InputEvent("k2", "put", 2),
              InputEvent("k2", "get_ttl_value_from_state", 0)),
            AdvanceManualClock(1 * 1000),
            CheckNewAnswer(OutputEvent("k2", 2, true, 62000)),
            StopStream
          )
        },
        verifyResumedQuery = (inputData, checkpointDir, clockOpt) => {
          val clock = clockOpt.get
          val query = buildQuery(inputData)
          testStream(query, OutputMode.Update())(
            StartStream(checkpointLocation = checkpointDir,
              trigger = Trigger.ProcessingTime("1 second"),
              triggerClock = clock),
            // Batch 3: Clock advances to 3000ms
            // Value 1 has TTL from batch 1 (61000ms), value 3 gets TTL = 3000 + 60000 = 63000ms
            AddData(inputData, InputEvent("k1", "append", 3),
              InputEvent("k1", "get_ttl_value_from_state", 0)),
            AdvanceManualClock(1 * 1000),
            CheckNewAnswer(OutputEvent("k1", 1, true, 61000), OutputEvent("k1", 3, true, 63000))
          )
        },
        useManualClock = true,
        storeToColumnFamilyToStateSourceOptions = Map(
          StateStoreId.DEFAULT_STORE_NAME -> stateSourceOptions
        ),
        storeToColumnFamilyToSelectExprs = Map(
          StateStoreId.DEFAULT_STORE_NAME -> selectExprs
        )
      )
  }

  testWithDifferentEncodingType("transformWithState with map and TTL") {
    newPartitions =>
      val schemas = TTLProcessorUtils.getMapStateTTLSchemasWithMetadata()
      val columnFamilies = schemas.keys.toSeq.filterNot(_ == StateStore.DEFAULT_COL_FAMILY_NAME)
      val stateSourceOptions = buildStateSourceOptionsForTWS(columnFamilies)
      val selectExprs = TTLProcessorUtils.getTTLSelectExpressions(columnFamilies)

      def buildQuery(inputData: MemoryStream[MapInputEvent]): Dataset[MapOutputEvent] = {
        val ttlConfig = TTLConfig(ttlDuration = Duration.ofMinutes(1))
        inputData.toDS()
          .groupByKey(x => x.key)
          .transformWithState(new MapStateTTLProcessor(ttlConfig),
            TimeMode.ProcessingTime(),
            OutputMode.Update())
      }

      testRepartitionWorkflow[MapInputEvent](
        newPartitions = newPartitions,
        setupInitialState = (inputData, checkpointDir, clockOpt) => {
          val clock = clockOpt.get
          val query = buildQuery(inputData)
          testStream(query)(
            StartStream(checkpointLocation = checkpointDir,
              trigger = Trigger.ProcessingTime("1 second"),
              triggerClock = clock),
            // Batch 1: Clock advances to 1000ms, TTL = 1000 + 60000 = 61000ms
            AddData(inputData, MapInputEvent("a", "key1", "put", 1),
              MapInputEvent("a", "key1", "get_ttl_value_from_state", 0)),
            AdvanceManualClock(1 * 1000),
            CheckNewAnswer(MapOutputEvent("a", "key1", 1, true, 61000)),
            // Batch 2: Clock advances to 2000ms, TTL = 2000 + 60000 = 62000ms
            AddData(inputData, MapInputEvent("b", "key2", "put", 2),
              MapInputEvent("b", "key2", "get_ttl_value_from_state", 0)),
            AdvanceManualClock(1 * 1000),
            CheckNewAnswer(MapOutputEvent("b", "key2", 2, true, 62000)),
            StopStream
          )
        },
        verifyResumedQuery = (inputData, checkpointDir, clockOpt) => {
          val clock = clockOpt.get
          val query = buildQuery(inputData)
          testStream(query)(
            StartStream(checkpointLocation = checkpointDir,
              trigger = Trigger.ProcessingTime("1 second"),
              triggerClock = clock),
            // Batch 3: Clock advances to 3000ms
            // key1 has TTL from batch 1 (61000ms), key3 gets TTL = 3000 + 60000 = 63000ms
            AddData(inputData, MapInputEvent("a", "key3", "put", 3),
              MapInputEvent("a", "key1", "get_ttl_value_from_state", 0),
              MapInputEvent("a", "key3", "get_ttl_value_from_state", 0)),
            AdvanceManualClock(1 * 1000),
            CheckNewAnswer(MapOutputEvent("a", "key1", 1, true, 61000),
              MapOutputEvent("a", "key3", 3, true, 63000))
          )
        },
        useManualClock = true,
        storeToColumnFamilyToStateSourceOptions = Map(
          StateStoreId.DEFAULT_STORE_NAME -> stateSourceOptions
        ),
        storeToColumnFamilyToSelectExprs = Map(
          StateStoreId.DEFAULT_STORE_NAME -> selectExprs
        )
      )
  }

  testWithDifferentEncodingType("transformWithState with value and TTL") {
    newPartitions =>
      val schemas = TTLProcessorUtils.getValueStateTTLSchemasWithMetadata()
      val stateSourceOptions = buildStateSourceOptionsForTWS(
        schemas.keys.toSeq.filterNot(_ == StateStore.DEFAULT_COL_FAMILY_NAME))

      def buildQuery(inputData: MemoryStream[InputEvent]): Dataset[OutputEvent] = {
        val ttlConfig = TTLConfig(ttlDuration = Duration.ofMinutes(1))
        inputData.toDS()
          .groupByKey(x => x.key)
          .transformWithState(new ValueStateTTLProcessor(ttlConfig),
            TimeMode.ProcessingTime(),
            OutputMode.Update())
      }

      testRepartitionWorkflow[InputEvent](
        newPartitions = newPartitions,
        setupInitialState = (inputData, checkpointDir, clockOpt) => {
          val clock = clockOpt.get
          val query = buildQuery(inputData)
          testStream(query)(
            StartStream(checkpointLocation = checkpointDir,
              trigger = Trigger.ProcessingTime("1 second"),
              triggerClock = clock),
            // Batch 1: Clock advances to 1000ms, TTL = 1000 + 60000 = 61000ms
            AddData(inputData, InputEvent("k1", "put", 1),
              InputEvent("k1", "get_ttl_value_from_state", 0)),
            AdvanceManualClock(1 * 1000),
            CheckNewAnswer(OutputEvent("k1", 1, true, 61000)),
            // Batch 2: Clock is at 2000ms, TTL = 2000 + 60000 = 62000ms
            AddData(inputData, InputEvent("k2", "put", 2),
              InputEvent("k2", "get_ttl_value_from_state", 0)),
            AdvanceManualClock(1 * 1000),
            CheckNewAnswer(OutputEvent("k2", 2, true, 62000)),
            StopStream
          )
        },
        verifyResumedQuery = (inputData, checkpointDir, clockOpt) => {
          val clock = clockOpt.get
          val query = buildQuery(inputData)
          testStream(query)(
            StartStream(checkpointLocation = checkpointDir,
              trigger = Trigger.ProcessingTime("1 second"),
              triggerClock = clock),
            // k2 is still in the state
            AddData(inputData, InputEvent("k2", "get_ttl_value_from_state", 0)),
            AdvanceManualClock(1 * 1000),
            CheckNewAnswer(OutputEvent("k2", 2, true, 62000))
          )
        },
        useManualClock = true,
        storeToColumnFamilyToStateSourceOptions = Map(
          StateStoreId.DEFAULT_STORE_NAME -> stateSourceOptions
        )
      )
  }
}
