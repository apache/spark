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

import org.apache.spark.sql.execution.streaming.StreamExecution
import org.apache.spark.sql.execution.streaming.sources.ContinuousMemoryStream
import org.apache.spark.sql.streaming.Trigger

class ContinuousQueryStatusAndProgressSuite extends ContinuousSuiteBase {
  test("StreamingQueryStatus - ContinuousExecution isDataAvailable and isTriggerActive " +
      "should be false") {
    import testImplicits._

    val input = ContinuousMemoryStream[Int]

    def assertStatus(stream: StreamExecution): Unit = {
      assert(stream.status.isDataAvailable === false)
      assert(stream.status.isTriggerActive === false)
    }

    val trigger = Trigger.Continuous(100)
    testStream(input.toDF())(
      StartStream(trigger),
      Execute(assertStatus),
      AddData(input, 0, 1, 2),
      Execute(assertStatus),
      CheckAnswer(0, 1, 2),
      Execute(assertStatus),
      StopStream,
      Execute(assertStatus),
      AddData(input, 3, 4, 5),
      Execute(assertStatus),
      StartStream(trigger),
      Execute(assertStatus),
      CheckAnswer(0, 1, 2, 3, 4, 5),
      Execute(assertStatus),
      StopStream,
      Execute(assertStatus))
  }
}
