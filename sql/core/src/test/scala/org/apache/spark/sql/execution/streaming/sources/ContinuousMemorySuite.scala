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


package org.apache.spark.sql.execution.streaming.sources

import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.streaming.{OutputMode, StreamTest}

class ContinuousMemorySuite extends StreamTest {
  import testImplicits._

  test("basic functionality") {
    val inputData = MemoryStream[Int]
    val mapped = inputData.toDS().map(_ + 1)

    testStream(mapped, OutputMode.Append, Map.empty, new ContinuousMemorySink())(
      AddData(inputData, 1, 2, 3),
      StartStream(),
      CheckAnswer(2, 3, 4),
      AddData(inputData, 4, 5, 6),
      CheckAnswer(2, 3, 4, 5, 6, 7),
      AddData(inputData, 7),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8),
      StopStream,
      AddData(inputData, 10, 11),
      StartStream(),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8, 11, 12))
  }
}
