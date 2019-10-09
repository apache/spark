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

package org.apache.spark.mllib.stat.test

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLlibTestSparkContext


class ChiSqTestSuite extends SparkFunSuite with MLlibTestSparkContext with Logging {
  /*
   Generate test data for e.g. TWO categories

   group | population | good_sample | bad_sample
   A     | 10000      | 1000        | 5000
   B     | 50000      | 5000        | 2000

   */
  val expected: Vector = Vectors.dense(Array[Double](10000, 50000))
  val observedGood: Vector = Vectors.dense(Array[Double](1000, 5000))
  val observedBad: Vector = Vectors.dense(Array[Double](5000, 2000))

  test("theoretical chi2 test") {
    val passResult: ChiSqTestResult =
      ChiSqTest.chiSquared(observedGood, expected)
    assert(passResult.degreesOfFreedom === 1, "degreesOfFreedom calculated correctly")
    assert(passResult.pValue > 0.05, "pValue indicates that test passed")

    val failResult: ChiSqTestResult =
      ChiSqTest.chiSquared(observedBad, expected)
    assert(failResult.degreesOfFreedom === 1, "degreesOfFreedom calculated correctly")
    assert(failResult.pValue < 0.05, "pValue indicates that test failed")
  }

  test("simulated/empirical chi2 test") {
    val passResult: ChiSqTestResult =
      ChiSqTest.chiSquared(observedGood, expected, simulatePValue = true, numDraw = 5000)
    assert(passResult.degreesOfFreedom === 1, "degreesOfFreedom calculated correctly")
    assert(passResult.pValue > 0.05, "pValue indicates that test passed")

    val failResult: ChiSqTestResult =
      ChiSqTest.chiSquared(observedBad, expected, simulatePValue = true, numDraw = 5000)
    assert(failResult.degreesOfFreedom === 1, "degreesOfFreedom calculated correctly")
    assert(failResult.pValue < 0.05, "pValue indicates that test failed")
  }
}
