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

package org.apache.spark.sql.streaming.continuous

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.execution.streaming.sources.ContinuousMemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

class ContinuousAggregationSuite extends ContinuousSuiteBase {
  import testImplicits._

  test("not enabled") {
    val ex = intercept[AnalysisException] {
      val input = ContinuousMemoryStream.singlePartition[Int]
      testStream(input.toDF().agg(max('value)), OutputMode.Complete)()
    }

    assert(ex.getMessage.contains("Continuous processing does not support Aggregate operations"))
  }

  test("basic") {
    withSQLConf(("spark.sql.streaming.unsupportedOperationCheck", "false")) {
      val input = ContinuousMemoryStream.singlePartition[Int]

      testStream(input.toDF().agg(max('value)), OutputMode.Complete)(
        AddData(input, 0, 1, 2),
        CheckAnswer(2),
        StopStream,
        AddData(input, 3, 4, 5),
        StartStream(),
        CheckAnswer(5),
        AddData(input, -1, -2, -3),
        CheckAnswer(5))
    }
  }

  test("repeated restart") {
    withSQLConf(("spark.sql.streaming.unsupportedOperationCheck", "false")) {
      val input = ContinuousMemoryStream.singlePartition[Int]

      testStream(input.toDF().agg(max('value)), OutputMode.Complete)(
        AddData(input, 0, 1, 2),
        CheckAnswer(2),
        StopStream,
        StartStream(),
        StopStream,
        StartStream(),
        StopStream,
        StartStream(),
        AddData(input, 0),
        CheckAnswer(2),
        AddData(input, 5),
        CheckAnswer(5))
    }
  }
}
