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

package org.apache.spark.sql.catalyst.util

import org.apache.datasketches.tuple.adouble.DoubleSummary
import org.apache.datasketches.tuple.aninteger.IntegerSummary

import org.apache.spark.{SparkFunSuite, SparkRuntimeException}
import org.apache.spark.sql.catalyst.plans.SQLHelper

class TupleSketchUtilsSuite extends SparkFunSuite with SQLHelper {

  test("TupleSummaryMode.fromString: accepts valid modes") {
    val validModes = Seq("sum", "min", "max", "alwaysone")
    validModes.foreach { mode =>
      // Should not throw any exception
      val result = TupleSummaryMode.fromString(mode, "test_function")
      assert(result != null)
      assert(result.toString == mode)
    }
  }

  test("TupleSummaryMode.fromString: case insensitive") {
    assert(TupleSummaryMode.fromString("SUM", "test_function") == TupleSummaryMode.Sum)
    assert(TupleSummaryMode.fromString("Min", "test_function") == TupleSummaryMode.Min)
    assert(TupleSummaryMode.fromString("MAX", "test_function") == TupleSummaryMode.Max)
    assert(TupleSummaryMode.fromString("AlwaysOne", "test_function") == TupleSummaryMode.AlwaysOne)
  }

  test("TupleSummaryMode.fromString: throws exception for invalid modes") {
    val invalidModes = Seq("invalid", "average", "count", "multiply", "")
    invalidModes.foreach { mode =>
      checkError(
        exception = intercept[SparkRuntimeException] {
          TupleSummaryMode.fromString(mode, "test_function")
        },
        condition = "TUPLE_INVALID_SKETCH_MODE",
        parameters = Map(
          "function" -> "`test_function`",
          "mode" -> mode,
          "validModes" -> TupleSummaryMode.validModeStrings.mkString(", ")))
    }
  }

  test("TupleSummaryMode: converts to DoubleSummary.Mode correctly") {
    assert(TupleSummaryMode.Sum.toDoubleSummaryMode == DoubleSummary.Mode.Sum)
    assert(TupleSummaryMode.Min.toDoubleSummaryMode == DoubleSummary.Mode.Min)
    assert(TupleSummaryMode.Max.toDoubleSummaryMode == DoubleSummary.Mode.Max)
    assert(TupleSummaryMode.AlwaysOne.toDoubleSummaryMode == DoubleSummary.Mode.AlwaysOne)
  }

  test("TupleSummaryMode: converts to IntegerSummary.Mode correctly") {
    assert(TupleSummaryMode.Sum.toIntegerSummaryMode == IntegerSummary.Mode.Sum)
    assert(TupleSummaryMode.Min.toIntegerSummaryMode == IntegerSummary.Mode.Min)
    assert(TupleSummaryMode.Max.toIntegerSummaryMode == IntegerSummary.Mode.Max)
    assert(TupleSummaryMode.AlwaysOne.toIntegerSummaryMode == IntegerSummary.Mode.AlwaysOne)
  }
}
