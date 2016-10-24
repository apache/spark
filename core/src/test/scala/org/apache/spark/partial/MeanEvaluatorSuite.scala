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

class MeanEvaluatorSuite extends SparkFunSuite {

  test("test count 0") {
    val evaluator = new MeanEvaluator(10, 0.95)
    assert(new BoundedDouble(0.0, 0.0, Double.NegativeInfinity, Double.PositiveInfinity) ==
      evaluator.currentResult())
    evaluator.merge(1, new StatCounter())
    assert(new BoundedDouble(0.0, 0.0, Double.NegativeInfinity, Double.PositiveInfinity) ==
      evaluator.currentResult())
    evaluator.merge(1, new StatCounter(Seq(0.0)))
    assert(new BoundedDouble(0.0, 0.95, Double.NegativeInfinity, Double.PositiveInfinity) ==
      evaluator.currentResult())
  }

  test("test count 1") {
    val evaluator = new MeanEvaluator(10, 0.95)
    evaluator.merge(1, new StatCounter(Seq(1.0)))
    assert(new BoundedDouble(1.0, 0.95, Double.NegativeInfinity, Double.PositiveInfinity) ==
      evaluator.currentResult())
  }

  test("test count > 1") {
    val evaluator = new MeanEvaluator(10, 0.95)
    evaluator.merge(1, new StatCounter(Seq(1.0)))
    evaluator.merge(1, new StatCounter(Seq(3.0)))
    assert(new BoundedDouble(2.0, 0.95, -10.706204736174746, 14.706204736174746) ==
      evaluator.currentResult())
    evaluator.merge(1, new StatCounter(Seq(8.0)))
    assert(new BoundedDouble(4.0, 0.95, -4.9566858949231225, 12.956685894923123) ==
      evaluator.currentResult())
    (4 to 10).foreach(_ => evaluator.merge(1, new StatCounter(Seq(9.0))))
    assert(new BoundedDouble(7.5, 1.0, 7.5, 7.5) == evaluator.currentResult())
  }

}
