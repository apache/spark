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
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.state.{RocksDBStateStoreProvider, StateStoreValueSchemaNotCompatible}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.util.StreamManualClock

class TransformWithStateUnsafeRowSuite extends TransformWithStateSuite {

  import testImplicits._

  override protected def test(testName: String, testTags: Tag*)(testBody: => Any)
                             (implicit pos: Position): Unit = {
    super.test(s"$testName (encoding = UnsafeRow)", testTags: _*) {
      withSQLConf(SQLConf.STREAMING_STATE_STORE_ENCODING_FORMAT.key -> "unsaferow") {
        testBody
      }
    }
  }

  test("test that invalid schema evolution fails query for column family") {
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
          ExpectFailure[StateStoreValueSchemaNotCompatible] {
            (t: Throwable) => {
              assert(t.getMessage.contains("Please check number and type of fields."))
            }
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
          ExpectFailure[StateStoreValueSchemaNotCompatible] { t =>
            checkError(
              t.asInstanceOf[SparkUnsupportedOperationException],
              condition = "STATE_STORE_VALUE_SCHEMA_NOT_COMPATIBLE",
              parameters = Map(
                "storedValueSchema" -> "StructType(StructField(value,LongType,false))",
                "newValueSchema" ->
                  ("StructType(StructField(value,StructType(StructField(value,LongType,false))," +
                    "true),StructField(ttlExpirationMs,LongType,true))")
              )
            )
          }
        )
      }
    }
  }
}
