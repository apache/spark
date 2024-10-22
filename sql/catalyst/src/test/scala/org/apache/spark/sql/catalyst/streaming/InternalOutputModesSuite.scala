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

package org.apache.spark.sql.catalyst.streaming

import org.apache.spark.{SparkFunSuite, SparkIllegalArgumentException}
import org.apache.spark.sql.streaming.OutputMode

class InternalOutputModesSuite extends SparkFunSuite {

  test("supported strings") {
    def testMode(outputMode: String, expected: OutputMode): Unit = {
      assert(InternalOutputModes(outputMode) === expected)
    }

    testMode("append", OutputMode.Append)
    testMode("Append", OutputMode.Append)
    testMode("complete", OutputMode.Complete)
    testMode("Complete", OutputMode.Complete)
    testMode("update", OutputMode.Update)
    testMode("Update", OutputMode.Update)
  }

  test("unsupported strings") {
    val outputMode = "Xyz"
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        InternalOutputModes(outputMode)
      },
      condition = "STREAMING_OUTPUT_MODE.INVALID",
      parameters = Map("outputMode" -> outputMode))
  }
}
