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

class StatefulStreamSuite extends QueryTest with SharedSQLContext {

  import testImplicits._

  test("windowed aggregation") {
    val inputData = MemoryStream[Int]
    val tenSecondCounts =
      inputData.toDF("eventTime")
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
      CheckAnswer((0, 0, 1), (0, 1, 2)))
  }
}