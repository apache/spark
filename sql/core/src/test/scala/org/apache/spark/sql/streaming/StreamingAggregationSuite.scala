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

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.InternalOutputModes._
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.state.StateStore
import org.apache.spark.sql.expressions.scalalang.typed
import org.apache.spark.sql.functions._

object FailureSinglton {
  var firstTime = true
}

class StreamingAggregationSuite extends StreamTest with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    super.afterAll()
    StateStore.stop()
  }

  import testImplicits._

  test("simple count, update mode") {
    val inputData = MemoryStream[Int]

    val aggregated =
      inputData.toDF()
        .groupBy($"value")
        .agg(count("*"))
        .as[(Int, Long)]

    testStream(aggregated, Update)(
      AddData(inputData, 3),
      CheckLastBatch((3, 1)),
      AddData(inputData, 3, 2),
      CheckLastBatch((3, 2), (2, 1)),
      StopStream,
      StartStream(),
      AddData(inputData, 3, 2, 1),
      CheckLastBatch((3, 3), (2, 2), (1, 1)),
      // By default we run in new tuple mode.
      AddData(inputData, 4, 4, 4, 4),
      CheckLastBatch((4, 4))
    )
  }

  test("simple count, complete mode") {
    val inputData = MemoryStream[Int]

    val aggregated =
      inputData.toDF()
        .groupBy($"value")
        .agg(count("*"))
        .as[(Int, Long)]

    testStream(aggregated, Complete)(
      AddData(inputData, 3),
      CheckLastBatch((3, 1)),
      AddData(inputData, 2),
      CheckLastBatch((3, 1), (2, 1)),
      StopStream,
      StartStream(),
      AddData(inputData, 3, 2, 1),
      CheckLastBatch((3, 2), (2, 2), (1, 1)),
      AddData(inputData, 4, 4, 4, 4),
      CheckLastBatch((4, 4), (3, 2), (2, 2), (1, 1))
    )
  }

  test("simple count, append mode") {
    val inputData = MemoryStream[Int]

    val aggregated =
      inputData.toDF()
        .groupBy($"value")
        .agg(count("*"))
        .as[(Int, Long)]

    val e = intercept[AnalysisException] {
      testStream(aggregated, Append)()
    }
    Seq("append", "not supported").foreach { m =>
      assert(e.getMessage.toLowerCase.contains(m.toLowerCase))
    }
  }

  test("sort after aggregate in complete mode") {
    val inputData = MemoryStream[Int]

    val aggregated =
      inputData.toDF()
        .groupBy($"value")
        .agg(count("*"))
        .toDF("value", "count")
        .orderBy($"count".desc)
        .as[(Int, Long)]

    testStream(aggregated, Complete)(
      AddData(inputData, 3),
      CheckLastBatch(isSorted = true, (3, 1)),
      AddData(inputData, 2, 3),
      CheckLastBatch(isSorted = true, (3, 2), (2, 1)),
      StopStream,
      StartStream(),
      AddData(inputData, 3, 2, 1),
      CheckLastBatch(isSorted = true, (3, 3), (2, 2), (1, 1)),
      AddData(inputData, 4, 4, 4, 4),
      CheckLastBatch(isSorted = true, (4, 4), (3, 3), (2, 2), (1, 1))
    )
  }

  test("multiple keys") {
    val inputData = MemoryStream[Int]

    val aggregated =
      inputData.toDF()
        .groupBy($"value", $"value" + 1)
        .agg(count("*"))
        .as[(Int, Int, Long)]

    testStream(aggregated, Update)(
      AddData(inputData, 1, 2),
      CheckLastBatch((1, 2, 1), (2, 3, 1)),
      AddData(inputData, 1, 2),
      CheckLastBatch((1, 2, 2), (2, 3, 2))
    )
  }

  testQuietly("midbatch failure") {
    val inputData = MemoryStream[Int]
    FailureSinglton.firstTime = true
    val aggregated =
      inputData.toDS()
          .map { i =>
            if (i == 4 && FailureSinglton.firstTime) {
              FailureSinglton.firstTime = false
              sys.error("injected failure")
            }

            i
          }
          .groupBy($"value")
          .agg(count("*"))
          .as[(Int, Long)]

    testStream(aggregated, Update)(
      StartStream(),
      AddData(inputData, 1, 2, 3, 4),
      ExpectFailure[SparkException](),
      StartStream(),
      CheckLastBatch((1, 1), (2, 1), (3, 1), (4, 1))
    )
  }

  test("typed aggregators") {
    val inputData = MemoryStream[(String, Int)]
    val aggregated = inputData.toDS().groupByKey(_._1).agg(typed.sumLong(_._2))

    testStream(aggregated, Update)(
      AddData(inputData, ("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)),
      CheckLastBatch(("a", 30), ("b", 3), ("c", 1))
    )
  }
}
