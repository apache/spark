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

import org.scalatest.Assertions

import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.state.StreamingAggregationStateManager
import org.apache.spark.sql.expressions.scalalang.typed
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.OutputMode._

@deprecated("This test suite will be removed.", "3.0.0")
class DeprecatedStreamingAggregationSuite extends StateStoreMetricsTest with Assertions {

  import testImplicits._

  def executeFuncWithStateVersionSQLConf(
      stateVersion: Int,
      confPairs: Seq[(String, String)],
      func: => Any): Unit = {
    withSQLConf(confPairs ++
      Seq(SQLConf.STREAMING_AGGREGATION_STATE_FORMAT_VERSION.key -> stateVersion.toString): _*) {
      func
    }
  }

  def testWithAllStateVersions(name: String, confPairs: (String, String)*)
                              (func: => Any): Unit = {
    for (version <- StreamingAggregationStateManager.supportedVersions) {
      test(s"$name - state format version $version") {
        executeFuncWithStateVersionSQLConf(version, confPairs, func)
      }
    }
  }


  testWithAllStateVersions("typed aggregators") {
    val inputData = MemoryStream[(String, Int)]
    val aggregated = inputData.toDS().groupByKey(_._1).agg(typed.sumLong(_._2))

    testStream(aggregated, Update)(
      AddData(inputData, ("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)),
      CheckLastBatch(("a", 30), ("b", 3), ("c", 1))
    )
  }
}
