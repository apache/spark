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

import org.apache.spark.sql.IntegratedUDFTestUtils.{shouldTestPandasUDFs, TestGroupedMapPandasUDFWithState}
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes.Update
import org.apache.spark.sql.execution.python.streaming.FlatMapGroupsInPandasWithStateExec
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.util.{StatefulOpClusteredDistributionTestHelper, StreamManualClock}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.tags.SlowSQLTest

@SlowSQLTest
class FlatMapGroupsInPandasWithStateDistributionSuite extends StreamTest
  with StatefulOpClusteredDistributionTestHelper {

  import testImplicits._

  test("applyInPandasWithState should require StatefulOpClusteredDistribution " +
    "from children - without initial state") {
    assume(shouldTestPandasUDFs)

    // Function to maintain running count up to 2, and then remove the count
    // Returns the data and the count if state is defined, otherwise does not return anything
    val pythonScript =
    """
      |import pandas as pd
      |from pyspark.sql.types import StructType, StructField, StringType, IntegerType
      |
      |tpe = StructType([
      |    StructField("key1", StringType()),
      |    StructField("key2", StringType()),
      |    StructField("count", IntegerType())])
      |
      |def func(key, pdf_iter, state):
      |    count = state.getOption
      |    if count is None:
      |        count = 0
      |    else:
      |        count = count[0]
      |
      |    for pdf in pdf_iter:
      |        count += len(pdf)
      |        state.update((count,))
      |
      |    if count >= 3:
      |        state.remove()
      |        yield pd.DataFrame()
      |    else:
      |        yield pd.DataFrame({'key1': [key[0]], 'key2': [key[1]], 'count': [count]})
      |""".stripMargin
    val pythonFunc = TestGroupedMapPandasUDFWithState(
      name = "pandas_grouped_map_with_state", pythonScript = pythonScript)

    val inputData = MemoryStream[(String, String, Long)]
    val outputStructType = StructType(
      Seq(
        StructField("key1", StringType),
        StructField("key2", StringType),
        StructField("count", IntegerType)))
    val stateStructType = StructType(Seq(StructField("count", LongType)))
    val inputDataDS = inputData.toDS().toDF("key1", "key2", "time")
      .selectExpr("key1", "key2", "timestamp_seconds(time) as timestamp")
    val result =
      inputDataDS
        .withWatermark("timestamp", "10 second")
        .repartition($"key1")
        .groupBy($"key1", $"key2")
        .applyInPandasWithState(
          pythonFunc(inputDataDS("key1"), inputDataDS("key2"), inputDataDS("timestamp")),
          outputStructType,
          stateStructType,
          "Update",
          "NoTimeout")
        .select("key1", "key2", "count")

    val clock = new StreamManualClock
    testStream(result, Update)(
      StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
      AddData(inputData, ("a", "a", 1L)),
      AdvanceManualClock(1 * 1000), // a is processed here for the first time.
      CheckNewAnswer(("a", "a", 1)),
      Execute { query =>
        val numPartitions = query.lastExecution.numStateStores

        val flatMapGroupsInPandasWithStateExecs = query.lastExecution.executedPlan.collect {
          case f: FlatMapGroupsInPandasWithStateExec => f
        }

        assert(flatMapGroupsInPandasWithStateExecs.length === 1)
        assert(requireStatefulOpClusteredDistribution(
          flatMapGroupsInPandasWithStateExecs.head, Seq(Seq("key1", "key2")), numPartitions))
        assert(hasDesiredHashPartitioningInChildren(
          flatMapGroupsInPandasWithStateExecs.head, Seq(Seq("key1", "key2")), numPartitions))
      }
    )
  }
}
