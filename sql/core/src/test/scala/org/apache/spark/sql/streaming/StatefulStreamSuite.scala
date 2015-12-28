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

import org.apache.spark.sql.{Row, QueryTest}
import org.apache.spark.sql.functions._

import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.test.SharedSQLContext

object FailureCounter {
  var shouldFail: Boolean = true
}

class StatefulStreamSuite extends QueryTest with SharedSQLContext {

  import testImplicits._

  test("windowed aggregation") {
    val inputData = MemoryStream[Int]
    val tenSecondCounts =
      inputData.toDF.toDF("eventTime")
        .window($"eventTime", step = 10, closingTriggerDelay = 20)
        .groupBy($"eventTime" % 2)
        .agg(count("*"))

    testStream(tenSecondCounts)(
      AddData(inputData, 1, 2, 3),
      CheckAnswer(),
      AddData(inputData, 11, 12),
      CheckAnswer(),
      AddData(inputData, 20),
      CheckAnswer(),
      AddData(inputData, 30),
      AwaitEventTime(30),
      CheckAnswer((10, 0, 1), (10, 1, 2)))
  }

  test("windowed aggregation with failures") {
    val inputData = MemoryStream[Int]

    val tenSecondCounts =
      inputData.toDS()
        .map { i =>
          // Fail part way through processing one partition to ensure that some partial
          // state updates have already been done and are rolled back properly.
          if (FailureCounter.shouldFail && i == 3) {
            FailureCounter.shouldFail = false
            sys.error("injected failure")
          }

          i
        }
        .toDF() // Bug
        .select('value as 'eventTime)
        .window($"eventTime", step = 10, closingTriggerDelay = 20)
        .groupBy($"eventTime" % 2)
        .agg(count("*"))

    testStream(tenSecondCounts)(
      AddData(inputData, 1, 2, 3),
      ExpectFailure,
      StartStream,
      CheckAnswer(),
      AddData(inputData, 11, 12),
      CheckAnswer(),
      AddData(inputData, 30), // Trigger closing of first window (20s delay)
      AwaitEventTime(30),
      CheckAnswer((10, 0, 1), (10, 1, 2)),
      StopStream,     // Kill the stream
      DropBatches(2), // Simulate data loss due to sink buffering.
      StartStream,
      AwaitEventTime(30),
      CheckAnswer((10, 0, 1), (10, 1, 2)),
      AddData(inputData, 40),  // Close the second window
      AwaitEventTime(40),
      CheckAnswer(
        (10, 0, 1), (10, 1, 2),
        (20, 0, 1), (20, 1, 1)),
      StopStream,               // Stop the stream while still adding data.
      AddData(inputData, 10),   // Late data should be dropped (by the current policy)
      AddData(inputData, 31, 32, 33, 34),
      AddData(inputData, 1000), // Advance time a lot while stream is stopped.
      StartStream,
      AwaitEventTime(41),
      CheckAnswer(
        (10, 0, 1), (10, 1, 2),
        (20, 0, 1), (20, 1, 1),
        (30, 0, 1),
        (40, 0, 3), (40, 1, 2)))
  }
}