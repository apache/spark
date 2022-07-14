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

import org.apache.spark.sql.IntegratedUDFTestUtils._
import org.apache.spark.sql.catalyst.expressions.PythonUDF
import org.apache.spark.sql.catalyst.plans.logical.{NoTimeout, ProcessingTimeTimeout}
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes.{Complete, Update}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.timestamp_seconds
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.types._

class FlatMapGroupsInPandasWithStateSuite extends StateStoreMetricsTest {

  import testImplicits._

  test("flatMapGroupsWithState - streaming") {
    assume(shouldTestPandasUDFs)

    // Function to maintain running count up to 2, and then remove the count
    // Returns the data and the count if state is defined, otherwise does not return anything
    val pythonScript =
      """
        |import pandas as pd
        |from pyspark.sql.types import StructType, StructField, StringType
        |
        |tpe = StructType([
        |    StructField("key", StringType()),
        |    StructField("countAsString", StringType())])
        |
        |def func(key, pdf, state):
        |    assert state.getCurrentProcessingTimeMs() >= 0
        |    try:
        |        state.getCurrentWatermarkMs()
        |        assert False
        |    except RuntimeError as e:
        |        assert "watermark" in str(e)
        |
        |    count = state.getOption
        |    if count is None:
        |        count = 0
        |    else:
        |        count = count[0]
        |    count += len(pdf)
        |    if count == 3:
        |        state.remove()
        |        return pd.DataFrame()
        |    else:
        |        state.update((count,))
        |        return pd.DataFrame({'key': [key[0]], 'countAsString': [str(count)]})
        |""".stripMargin
    val pythonFunc = TestGroupedMapPandasUDFWithState(
      name = "pandas_grouped_map_with_state", pythonScript = pythonScript)

    val inputData = MemoryStream[String]
    val outputStructType = StructType(
      Seq(
        StructField("key", StringType),
        StructField("countAsString", StringType)))
    val stateStructType = StructType(Seq(StructField("count", LongType)))
    val inputDataDS = inputData.toDS()
    val result =
      inputDataDS
        .groupBy("value")
        .applyInPandasWithState(
          pythonFunc(inputDataDS("value")).expr.asInstanceOf[PythonUDF],
          outputStructType,
          stateStructType,
          "Update",
          "NoTimeout")

    testStream(result, Update)(
      AddData(inputData, "a"),
      CheckNewAnswer(("a", "1")),
      assertNumStateRows(total = 1, updated = 1),
      AddData(inputData, "a", "b"),
      CheckNewAnswer(("a", "2"), ("b", "1")),
      assertNumStateRows(total = 2, updated = 2),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "b"), // should remove state for "a" and not return anything for a
      CheckNewAnswer(("b", "2")),
      assertNumStateRows(
        total = Seq(1), updated = Seq(1), droppedByWatermark = Seq(0), removed = Some(Seq(1))),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "c"), // should recreate state for "a" and return count as 1 and
      CheckNewAnswer(("a", "1"), ("c", "1")),
      assertNumStateRows(total = 3, updated = 2)
    )
  }

  test("flatMapGroupsWithState - streaming + aggregation") {
    assume(shouldTestPandasUDFs)

    // Function to maintain running count up to 2, and then remove the count
    // Returns the data and the count (-1 if count reached beyond 2 and state was just removed)
    val pythonScript =
      """
        |import pandas as pd
        |from pyspark.sql.types import StructType, StructField, StringType
        |
        |tpe = StructType([
        |    StructField("key", StringType()),
        |    StructField("countAsString", StringType())])
        |
        |def func(key, pdf, state):
        |    count = state.getOption
        |    if count is None:
        |        count = 0
        |    else:
        |        count = count[0]
        |    count += len(pdf)
        |    if count == 3:
        |        state.remove()
        |        return pd.DataFrame({'key': [key[0]], 'countAsString': [str(-1)]})
        |    else:
        |        state.update((count,))
        |        return pd.DataFrame({'key': [key[0]], 'countAsString': [str(count)]})
        |""".stripMargin
    val pythonFunc = TestGroupedMapPandasUDFWithState(
      name = "pandas_grouped_map_with_state", pythonScript = pythonScript)

    val inputData = MemoryStream[String]
    val inputDataDS = inputData.toDS
    val outputStructType = StructType(
      Seq(
        StructField("key", StringType),
        StructField("countAsString", StringType)))
    val stateStructType = StructType(Seq(StructField("count", LongType)))
    val result =
      inputDataDS
        .groupBy("value")
        .applyInPandasWithState(
          pythonFunc(inputDataDS("value")).expr.asInstanceOf[PythonUDF],
          outputStructType,
          stateStructType,
          "Append",
          "NoTimeout")
        .groupBy("key")
        .count()

    testStream(result, Complete)(
      AddData(inputData, "a"),
      CheckNewAnswer(("a", 1)),
      AddData(inputData, "a", "b"),
      // mapGroups generates ("a", "2"), ("b", "1"); so increases counts of a and b by 1
      CheckNewAnswer(("a", 2), ("b", 1)),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "b"),
      // mapGroups should remove state for "a" and generate ("a", "-1"), ("b", "2") ;
      // so increment a and b by 1
      CheckNewAnswer(("a", 3), ("b", 2)),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "c"),
      // mapGroups should recreate state for "a" and generate ("a", "1"), ("c", "1") ;
      // so increment a and c by 1
      CheckNewAnswer(("a", 4), ("b", 2), ("c", 1))
    )
  }

  test("flatMapGroupsWithState - streaming with processing time timeout") {
    assume(shouldTestPandasUDFs)

    // Function to maintain the count as state and set the proc. time timeout delay of 10 seconds.
    // It returns the count if changed, or -1 if the state was removed by timeout.
    val pythonScript =
      """
        |import pandas as pd
        |from pyspark.sql.types import StructType, StructField, StringType
        |
        |tpe = StructType([
        |    StructField("key", StringType()),
        |    StructField("countAsString", StringType())])
        |
        |def func(key, pdf, state):
        |    assert state.getCurrentProcessingTimeMs() >= 0
        |    try:
        |        state.getCurrentWatermarkMs()
        |        assert False
        |    except RuntimeError as e:
        |        assert "watermark" in str(e)
        |
        |    if state.hasTimedOut:
        |        state.remove()
        |        return pd.DataFrame({'key': [key[0]], 'countAsString': [str(-1)]})
        |    else:
        |        count = state.getOption
        |        if count is None:
        |            count = 0
        |        else:
        |            count = count[0]
        |        count += len(pdf)
        |        state.update((count,))
        |        state.setTimeoutDuration(10000)
        |        return pd.DataFrame({'key': [key[0]], 'countAsString': [str(count)]})
        |""".stripMargin
    val pythonFunc = TestGroupedMapPandasUDFWithState(
      name = "pandas_grouped_map_with_state", pythonScript = pythonScript)

    val clock = new StreamManualClock
    val inputData = MemoryStream[String]
    val inputDataDS = inputData.toDS
    val outputStructType = StructType(
      Seq(
        StructField("key", StringType),
        StructField("countAsString", StringType)))
    val stateStructType = StructType(Seq(StructField("count", LongType)))
    val result =
      inputDataDS
        .groupBy("value")
        .applyInPandasWithState(
          pythonFunc(inputDataDS("value")).expr.asInstanceOf[PythonUDF],
          outputStructType,
          stateStructType,
          "Update",
          "ProcessingTimeTimeout")

    testStream(result, Update)(
      StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
      AddData(inputData, "a"),
      AdvanceManualClock(1 * 1000),
      CheckNewAnswer(("a", "1")),
      assertNumStateRows(total = 1, updated = 1),

      AddData(inputData, "b"),
      AdvanceManualClock(1 * 1000),
      CheckNewAnswer(("b", "1")),
      assertNumStateRows(total = 2, updated = 1),

      AddData(inputData, "b"),
      AdvanceManualClock(10 * 1000),
      CheckNewAnswer(("a", "-1"), ("b", "2")),
      assertNumStateRows(
        total = Seq(1), updated = Seq(1), droppedByWatermark = Seq(0), removed = Some(Seq(1))),

      StopStream,
      StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),

      AddData(inputData, "c"),
      AdvanceManualClock(11 * 1000),
      CheckNewAnswer(("b", "-1"), ("c", "1")),
      assertNumStateRows(
        total = Seq(1), updated = Seq(1), droppedByWatermark = Seq(0), removed = Some(Seq(1))),

      AdvanceManualClock(12 * 1000),
      AssertOnQuery { _ => clock.getTimeMillis() == 35000 },
      Execute { q =>
        failAfter(streamingTimeout) {
          while (q.lastProgress.timestamp != "1970-01-01T00:00:35.000Z") {
            Thread.sleep(1)
          }
        }
      },
      CheckNewAnswer(("c", "-1")),
      assertNumStateRows(
        total = Seq(0), updated = Seq(0), droppedByWatermark = Seq(0), removed = Some(Seq(1)))
    )
  }

  test("flatMapGroupsWithState - streaming w/ event time timeout + watermark") {
    assume(shouldTestPandasUDFs)

    // timestamp_seconds assumes the base timezone is UTC. However, the provided function
    // localizes it. Therefore, this test assumes the timezone is in UTC
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      val pythonScript =
        """
          |import calendar
          |import os
          |import datetime
          |import pandas as pd
          |from pyspark.sql.types import StructType, StringType, StructField, IntegerType
          |
          |tpe = StructType([
          |    StructField("key", StringType()),
          |    StructField("maxEventTimeSec", IntegerType())])
          |
          |def func(key, pdf, state):
          |    assert state.getCurrentProcessingTimeMs() >= 0
          |    assert state.getCurrentWatermarkMs() >= -1
          |
          |    timeout_delay_sec = 5
          |    if state.hasTimedOut:
          |        state.remove()
          |        return pd.DataFrame({'key': [key[0]], 'maxEventTimeSec': [-1]})
          |    else:
          |        m = state.getOption
          |        if m is None:
          |            m = 0
          |        else:
          |            m = m[0]
          |
          |        pser = pdf.eventTime.apply(
          |                lambda dt: (int(calendar.timegm(dt.utctimetuple()) + dt.microsecond)))
          |        max_event_time_sec = int(max(pser.max(), m))
          |        timeout_timestamp_sec = max_event_time_sec + timeout_delay_sec
          |        state.update((max_event_time_sec,))
          |        state.setTimeoutTimestamp(timeout_timestamp_sec * 1000)
          |        return pd.DataFrame({'key': [key[0]], 'maxEventTimeSec': [max_event_time_sec]})
          |""".stripMargin
      val pythonFunc = TestGroupedMapPandasUDFWithState(
        name = "pandas_grouped_map_with_state", pythonScript = pythonScript)

      val inputData = MemoryStream[(String, Int)]
      val inputDataDF =
        inputData.toDF.select($"_1".as("key"), timestamp_seconds($"_2").as("eventTime"))
      val outputStructType = StructType(
        Seq(
          StructField("key", StringType),
          StructField("maxEventTimeSec", IntegerType)))
      val stateStructType = StructType(Seq(StructField("maxEventTimeSec", LongType)))
      val result =
        inputDataDF
          .withWatermark("eventTime", "10 seconds")
          .groupBy("key")
          .applyInPandasWithState(
            pythonFunc(inputDataDF("key"), inputDataDF("eventTime")).expr.asInstanceOf[PythonUDF],
            outputStructType,
            stateStructType,
            "Update",
            "EventTimeTimeout")

      testStream(result, Update)(
        StartStream(),

        AddData(inputData, ("a", 11), ("a", 13), ("a", 15)),
        // Max event time = 15. Timeout timestamp for "a" = 15 + 5 = 20. Watermark = 15 - 10 = 5.
        CheckNewAnswer(("a", 15)), // Output = max event time of a

        AddData(inputData, ("a", 4)), // Add data older than watermark for "a"
        CheckNewAnswer(), // No output as data should get filtered by watermark

        AddData(inputData, ("a", 10)), // Add data newer than watermark for "a"
        CheckNewAnswer(("a", 15)), // Max event time is still the same
        // Timeout timestamp for "a" is still 20 as max event time for "a" is still 15.
        // Watermark is still 5 as max event time for all data is still 15.

        AddData(inputData, ("b", 31)), // Add data newer than watermark for "b", not "a"
        // Watermark = 31 - 10 = 21, so "a" should be timed out as timeout timestamp for "a" is 20.
        CheckNewAnswer(("a", -1), ("b", 31)) // State for "a" should timeout and emit -1
      )
    }
  }

  def testWithTimeout(timeoutConf: GroupStateTimeout): Unit = {
    test("SPARK-20714: watermark does not fail query when timeout = " + timeoutConf) {
      assume(shouldTestPandasUDFs)

      // timestamp_seconds assumes the base timezone is UTC. However, the provided function
      // localizes it. Therefore, this test assumes the timezone is in UTC
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
        // Function to maintain running count up to 2, and then remove the count
        // Returns the data and the count (-1 if count reached beyond 2 and state was just removed)
        // String, (String, Long), RunningCount(Long)
        val pythonScript =
          """
            |import pandas as pd
            |from pyspark.sql.types import StructType, StructField, StringType
            |
            |tpe = StructType([
            |    StructField("key", StringType()),
            |    StructField("countAsString", StringType())])
            |
            |def func(key, pdf, state):
            |    if state.hasTimedOut:
            |        state.remove()
            |        return pd.DataFrame({'key': [key[0]], 'countAsString': [str(-1)]})
            |    else:
            |        count = state.getOption
            |        if count is None:
            |            count = 0
            |        else:
            |            count = count[0]
            |        count += len(pdf)
            |        state.update((count,))
            |        state.setTimeoutDuration(10000)
            |        return pd.DataFrame({'key': [key[0]], 'countAsString': [str(count)]})
            |""".stripMargin
        val pythonFunc = TestGroupedMapPandasUDFWithState(
          name = "pandas_grouped_map_with_state", pythonScript = pythonScript)

        val clock = new StreamManualClock
        val inputData = MemoryStream[(String, Long)]
        val inputDataDF = inputData
          .toDF.toDF("key", "time")
          .selectExpr("key", "timestamp_seconds(time) as timestamp")
        val outputStructType = StructType(
          Seq(
            StructField("key", StringType),
            StructField("countAsString", StringType)))
        val stateStructType = StructType(Seq(StructField("count", LongType)))
        val result =
          inputDataDF
            .withWatermark("timestamp", "10 second")
            .groupBy("key")
            .applyInPandasWithState(
              pythonFunc(inputDataDF("key"), inputDataDF("timestamp")).expr.asInstanceOf[PythonUDF],
              outputStructType,
              stateStructType,
              "Update",
              "ProcessingTimeTimeout")

        testStream(result, Update)(
          StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
          AddData(inputData, ("a", 1L)),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(("a", "1"))
        )
      }
    }
  }
  testWithTimeout(NoTimeout)
  testWithTimeout(ProcessingTimeTimeout)
}
