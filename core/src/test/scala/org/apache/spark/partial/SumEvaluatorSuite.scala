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

import org.apache.spark._
import org.apache.spark.util.StatCounter

class SumEvaluatorSuite extends SparkFunSuite with SharedSparkContext {

  test("correct handling of count 1") {

    // setup
    val counter = new StatCounter(List(2.0))
    // count of 10 because it's larger than 1,
    // and 0.95 because that's the default
    val evaluator = new SumEvaluator(10, 0.95)
    // arbitrarily assign id 1
    evaluator.merge(1, counter)

    // execute
    val res = evaluator.currentResult()
    // 38.0 - 7.1E-15 because that's how the maths shakes out
    val targetMean = 38.0 - 7.1E-15

    // Sanity check that equality works on BoundedDouble
    assert(new BoundedDouble(2.0, 0.95, 1.1, 1.2) == new BoundedDouble(2.0, 0.95, 1.1, 1.2))

    // actual test
    assert(res ==
      new BoundedDouble(targetMean, 0.950, Double.NegativeInfinity, Double.PositiveInfinity))
  }

  test("correct handling of count 0") {

    // setup
    val counter = new StatCounter(List())
    // count of 10 because it's larger than 0,
    // and 0.95 because that's the default
    val evaluator = new SumEvaluator(10, 0.95)
    // arbitrarily assign id 1
    evaluator.merge(1, counter)

    // execute
    val res = evaluator.currentResult()
    // assert
    assert(res == new BoundedDouble(0, 0.0, Double.NegativeInfinity, Double.PositiveInfinity))
  }

  test("correct handling of NaN") {

    // setup
    val counter = new StatCounter(List(1, Double.NaN, 2))
    // count of 10 because it's larger than 0,
    // and 0.95 because that's the default
    val evaluator = new SumEvaluator(10, 0.95)
    // arbitrarily assign id 1
    evaluator.merge(1, counter)

    // execute
    val res = evaluator.currentResult()
    // assert - note semantics of == in face of NaN
    assert(res.mean.isNaN)
    assert(res.confidence == 0.95)
    assert(res.low == Double.NegativeInfinity)
    assert(res.high == Double.PositiveInfinity)
  }

  test("correct handling of > 1 values") {

    // setup
    val counter = new StatCounter(List(1, 3, 2))
    // count of 10 because it's larger than 0,
    // and 0.95 because that's the default
    val evaluator = new SumEvaluator(10, 0.95)
    // arbitrarily assign id 1
    evaluator.merge(1, counter)

    // execute
    val res = evaluator.currentResult()

    // These vals because that's how the maths shakes out
    val targetMean = 78.0
    val targetLow = -117.617 + 2.732357258139473E-5
    val targetHigh = 273.617 - 2.7323572624027292E-5
    val target = new BoundedDouble(targetMean, 0.95, targetLow, targetHigh)


    // check that values are within expected tolerance of expectation
    assert(res == target)
  }

}
