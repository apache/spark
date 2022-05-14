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

package org.apache.spark.partial

import org.apache.spark.{SparkFunSuite, SparkUnsupportedOperationException}

class PartialResultSuite extends SparkFunSuite {

  test("UNSUPPORTED_OPERATION: call onComplete twice") {
    val evaluator = new SumEvaluator(10, 0.95)
    val partialResult = new PartialResult[BoundedDouble](evaluator.currentResult(), true)
    partialResult.onComplete(_ => {})

    val e = intercept[SparkUnsupportedOperationException](
      partialResult.onComplete(_ => {})
    )
    assert(e.getErrorClass === "UNSUPPORTED_OPERATION")
    assert(e.getMessage === "[UNSUPPORTED_OPERATION] " +
      "The operation is not supported: onComplete cannot be called twice")
  }

  test("UNSUPPORTED_OPERATION: call onFail twice") {
    val evaluator = new SumEvaluator(10, 0.95)
    val partialResult = new PartialResult[BoundedDouble](evaluator.currentResult(), false)
    partialResult.onFail(_ => {})
    val e = intercept[SparkUnsupportedOperationException](
      partialResult.onFail(_ => {})
    )
    assert(e.getErrorClass === "UNSUPPORTED_OPERATION")
    assert(e.getMessage === "[UNSUPPORTED_OPERATION] " +
      "The operation is not supported: onFail cannot be called twice")
  }

  test("UNSUPPORTED_OPERATION: call setFinalValue twice") {
    val evaluator = new SumEvaluator(10, 0.95)
    val partialResult = new PartialResult[BoundedDouble](evaluator.currentResult(), false)
    partialResult.setFinalValue(new BoundedDouble(20.0, 0.95,
      Double.NegativeInfinity, Double.PositiveInfinity))
    val e = intercept[SparkUnsupportedOperationException](
      partialResult.setFinalValue(new BoundedDouble(20.0, 0.95,
        Double.NegativeInfinity, Double.PositiveInfinity))
    )
    assert(e.getErrorClass === "UNSUPPORTED_OPERATION")
    assert(e.getMessage === "[UNSUPPORTED_OPERATION] " +
      "The operation is not supported: setFinalValue called twice on a PartialResult")
  }

  test("UNSUPPORTED_OPERATION: call setFailure twice") {
    val evaluator = new SumEvaluator(10, 0.95)
    val partialResult = new PartialResult[BoundedDouble](evaluator.currentResult(), false)
    partialResult.setFailure(new Exception("setFailure failed"))
    val e = intercept[SparkUnsupportedOperationException](
      partialResult.setFailure(new Exception("setFailure failed"))
    )
    assert(e.getErrorClass === "UNSUPPORTED_OPERATION")
    assert(e.getMessage === "[UNSUPPORTED_OPERATION] " +
      "The operation is not supported: setFailure called twice on a PartialResult")
  }
}
