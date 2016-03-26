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

    //setup
    val counter = new StatCounter(List(2.0))
    // count of 10 because it's larger than 1,
    // and 0.95 because that's the default
    val evaluator = new SumEvaluator(10, 0.95)
    // arbitrarily assign id 1
    evaluator.merge(1, counter)

    //execute
    val res = evaluator.currentResult()
    // Build version with known precisions for equality check
    val round_res = new BoundedDouble(res.mean.round.toDouble, res.confidence, res.low, res.high)

    //Sanity check that equality works on BoundedDouble
    assert(new BoundedDouble(2.0, 0.95, 1.1, 1.2) == new BoundedDouble(2.0, 0.95, 1.1, 1.2))
    // actual test
    
    // 38.0 because that's how the maths shakes out
    assert(round_res == new BoundedDouble(38.0, 0.950, Double.NegativeInfinity, Double.PositiveInfinity))
  }

}
