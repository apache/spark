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

package org.apache.spark.mllib.stat

import org.scalatest.FunSuite

import org.apache.spark.SparkException
import org.apache.spark.mllib.linalg.{Matrices, DenseVector, Vectors}
import org.apache.spark.mllib.stat.test.ChiSquaredTest
import org.apache.spark.mllib.util.LocalSparkContext
import org.apache.spark.mllib.util.TestingUtils._

class HypothesisTestSuite extends FunSuite with LocalSparkContext {

  test("chi squared pearson goodness of fit") {

    val observed = new DenseVector(Array[Double](4, 6, 5))
    val pearson = Statistics.chiSqTest(observed)

    // Results validated against the R command `chisq.test(c(4, 6, 5), p=c(1/3, 1/3, 1/3))`
    assert(pearson.statistic === 0.4)
    assert(pearson.degreesOfFreedom === Array(2))
    assert(pearson.pValue ~= 0.8187 absTol 1e-3)
    assert(pearson.method === ChiSquaredTest.PEARSON)
    assert(pearson.nullHypothesis === ChiSquaredTest.NullHypothesis.goodnessOfFit.toString)

    // different expected and observed sum
    val observed1 = new DenseVector(Array[Double](21, 38, 43, 80))
    val expected1 = new DenseVector(Array[Double](3, 5, 7, 20))
    val c1 = Statistics.chiSqTest(observed1, expected1)

    // Results validated against the R command
    // `chisq.test(c(21, 38, 43, 80), p=c(3/35, 1/7, 1/5, 4/7))`
    assert(c1.statistic ~= 14.1429 absTol 1e-3)
    assert(c1.degreesOfFreedom === Array(3))
    assert(c1.pValue ~= 0.002717 absTol 1e-6)
    assert(c1.method === ChiSquaredTest.PEARSON)
    assert(c1.nullHypothesis === ChiSquaredTest.NullHypothesis.goodnessOfFit.toString)

    // Vectors with different sizes
    val observed3 = new DenseVector(Array(1.0, 2.0, 3.0))
    val expected3 = new DenseVector(Array(1.0, 2.0, 3.0, 4.0))
    intercept[IllegalArgumentException](Statistics.chiSqTest(observed3, expected3))

    // negative counts in observed
    val negObs = new DenseVector(Array(1.0, 2.0, 3.0, -4.0))
    intercept[IllegalArgumentException](Statistics.chiSqTest(negObs, expected1))

    // count = 0.0 in expected
    val zeroExpected = new DenseVector(Array(1.0, 0.0, 3.0))
    intercept[IllegalArgumentException](Statistics.chiSqTest(observed, zeroExpected))
  }

  test("chi squared pearson independence") {

    val data = Array(
      40.0, 56.0, 31.0, 30.0,
      24.0, 32.0, 10.0, 15.0,
      29.0, 42.0, 0.0, 12.0)
    val chi = Statistics.chiSqTest(Matrices.dense(3, 4, data))
    assert(chi.statistic ~= 21.9958 absTol 1e-3)
    assert(chi.degreesOfFreedom === Array(6))
    assert(chi.pValue ~= 0.001213 absTol 1e-6)
    assert(chi.method === ChiSquaredTest.PEARSON)
    assert(chi.nullHypothesis === ChiSquaredTest.NullHypothesis.independence.toString)

    // Negative counts
    val negCounts = Array(
      4.0, 5.0, 3.0, 3.0,
      0.0, -3.0, 0.0, 5.0,
      9.0, 0.0, 0.0, 1.0)
    intercept[SparkException](Statistics.chiSqTest(Matrices.dense(3, 4, negCounts)))

    // Row sum = 0.0
    val rowZero = Array(
      4.0, 5.0, 3.0, 3.0,
      0.0, 0.0, 0.0, 0.0,
      9.0, 0.0, 0.0, 1.0)
    intercept[SparkException](Statistics.chiSqTest(Matrices.dense(3, 4, rowZero)))

    // Column sum  = 0.0
    val colZero = Array(
      1.0, 0.0, 0.0, 2.0,
      4.0, 5.0, 0.0, 3.0,
      9.0, 0.0, 0.0, 1.0)
    // IllegalArgumentException thrown here since it's thrown on driver, not inside a task
    intercept[IllegalArgumentException](Statistics.chiSqTest(Matrices.dense(3, 4, colZero)))
  }

  test("chi squared pearson features") {

  }

}
