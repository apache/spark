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

class CountEvaluatorSuite extends SparkFunSuite {

  test("test count 0") {
    val evaluator = new CountEvaluator(10, 0.95)
    assert(new BoundedDouble(0.0, 0.0, 0.0, Double.PositiveInfinity) == evaluator.currentResult())
    evaluator.merge(1, 0)
    assert(new BoundedDouble(0.0, 0.0, 0.0, Double.PositiveInfinity) == evaluator.currentResult())
  }

  test("test count >= 1") {
    val evaluator = new CountEvaluator(10, 0.95)
    evaluator.merge(1, 1)
    assert(new BoundedDouble(10.0, 0.95, 1.0, 36.0) == evaluator.currentResult())
    evaluator.merge(1, 3)
    assert(new BoundedDouble(20.0, 0.95, 7.0, 41.0) == evaluator.currentResult())
    evaluator.merge(1, 8)
    assert(new BoundedDouble(40.0, 0.95, 24.0, 61.0) == evaluator.currentResult())
    (4 to 10).foreach(_ => evaluator.merge(1, 10))
    assert(new BoundedDouble(82.0, 1.0, 82.0, 82.0) == evaluator.currentResult())
  }

}
