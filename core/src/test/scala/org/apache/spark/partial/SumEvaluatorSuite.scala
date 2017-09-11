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

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.StatCounter

class SumEvaluatorSuite extends SparkFunSuite {

  test("correct handling of count 1") {
    // sanity check:
    assert(new BoundedDouble(2.0, 0.95, 1.1, 1.2) == new BoundedDouble(2.0, 0.95, 1.1, 1.2))

    // count of 10 because it's larger than 1,
    // and 0.95 because that's the default
    val evaluator = new SumEvaluator(10, 0.95)
    // arbitrarily assign id 1
    evaluator.merge(1, new StatCounter(Seq(2.0)))
    assert(new BoundedDouble(20.0, 0.95, Double.NegativeInfinity, Double.PositiveInfinity) ==
      evaluator.currentResult())
  }

  test("correct handling of count 0") {
    val evaluator = new SumEvaluator(10, 0.95)
    evaluator.merge(1, new StatCounter())
    assert(new BoundedDouble(0, 0.0, Double.NegativeInfinity, Double.PositiveInfinity) ==
      evaluator.currentResult())
  }

  test("correct handling of NaN") {
    val evaluator = new SumEvaluator(10, 0.95)
    evaluator.merge(1, new StatCounter(Seq(1, Double.NaN, 2)))
    val res = evaluator.currentResult()
    // assert - note semantics of == in face of NaN
    assert(res.mean.isNaN)
    assert(res.confidence == 0.95)
    assert(res.low == Double.NegativeInfinity)
    assert(res.high == Double.PositiveInfinity)
  }

  test("correct handling of > 1 values") {
    val evaluator = new SumEvaluator(10, 0.95)
    evaluator.merge(1, new StatCounter(Seq(1.0, 3.0, 2.0)))
    val res = evaluator.currentResult()
    assert(new BoundedDouble(60.0, 0.95, -101.7362525347778, 221.7362525347778) ==
      evaluator.currentResult())
  }

  test("test count > 1") {
    val evaluator = new SumEvaluator(10, 0.95)
    evaluator.merge(1, new StatCounter().merge(1.0))
    evaluator.merge(1, new StatCounter().merge(3.0))
    assert(new BoundedDouble(20.0, 0.95, -186.4513905077019, 226.4513905077019) ==
      evaluator.currentResult())
    evaluator.merge(1, new StatCounter().merge(8.0))
    assert(new BoundedDouble(40.0, 0.95, -72.75723361226733, 152.75723361226733) ==
      evaluator.currentResult())
    (4 to 10).foreach(_ => evaluator.merge(1, new StatCounter().merge(9.0)))
    assert(new BoundedDouble(75.0, 1.0, 75.0, 75.0) == evaluator.currentResult())
  }

}
