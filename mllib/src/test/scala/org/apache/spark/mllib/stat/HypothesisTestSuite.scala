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
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.test.ChiSquaredTest
import org.apache.spark.mllib.util.LocalSparkContext
import org.apache.spark.mllib.util.TestingUtils._

class HypothesisTestSuite extends FunSuite with LocalSparkContext {

  test("chi squared pearson goodness of fit") {

    // check that number of partitions does not affect results
    for (numParts <- List(1, 2, 3, 4, 5)) {
      val observed = sc.parallelize(Array[Double](4, 6, 5), numParts)
      val expected = sc.parallelize(Array[Double](5, 5, 5), numParts)
      val default = Statistics.chiSquared(observed, expected)
      val pearson = Statistics.chiSquared(observed, expected, "pearson")

      // Results validated against the R command `chisq.test(c(4, 6, 5), p=c(1/3, 1/3, 1/3))`
      assert(default.statistic === 0.4)
      assert(default.degreesOfFreedom === Array(2))
      assert(default.pValue ~= 0.8187 absTol 1e-3)
      assert(default.method === ChiSquaredTest.PEARSON)
      assert(default.nullHypothesis === ChiSquaredTest.NullHypothesis.goodnessOfFit.toString)
      assert(pearson.statistic === 0.4)
      assert(pearson.degreesOfFreedom === Array(2))
      assert(pearson.pValue ~= 0.8187 absTol 1e-3)
      assert(pearson.method === ChiSquaredTest.PEARSON)
      assert(pearson.nullHypothesis === ChiSquaredTest.NullHypothesis.goodnessOfFit.toString)

      // different expected and observed sum
      val observed1 = sc.parallelize(Array[Double](21, 38, 43, 80), numParts)
      val expected1 = sc.parallelize(Array[Double](3, 5, 7, 20), numParts)
      val c1 = Statistics.chiSquared(observed1, expected1)

      // Results validated against the R command
      // `chisq.test(c(21, 38, 43, 80), p=c(3/35, 1/7, 1/5, 4/7))`
      assert(c1.statistic ~= 14.1429 absTol 1e-3)
      assert(c1.degreesOfFreedom === Array(3))
      assert(c1.pValue ~= 0.002717 absTol 1e-6)
      assert(c1.method === ChiSquaredTest.PEARSON)
      assert(c1.nullHypothesis === ChiSquaredTest.NullHypothesis.goodnessOfFit.toString)
    }

    // different sized RDDs
    val observed = sc.parallelize(Array(1.0, 2.0, 3.0))
    val expected = sc.parallelize(Array(1.0, 2.0, 3.0, 4.0))
    intercept[IllegalArgumentException](Statistics.chiSquared(observed, expected))

    // negative counts in observed
    val negObs = sc.parallelize(Array(1.0, 2.0, 3.0, -4.0))
    intercept[IllegalArgumentException](Statistics.chiSquared(negObs, expected))

    // count = 0.0 in expected
    val zeroExpected = sc.parallelize(Array(1.0, 0.0, 3.0))
    intercept[IllegalArgumentException](Statistics.chiSquared(observed, zeroExpected))
  }

  test("chi squared pearson independence") {

    val data = Seq(
      Vectors.dense(40.0, 56.0, 31.0, 30.0),
      Vectors.dense(24.0, 32.0, 10.0, 15.0),
      Vectors.dense(29.0, 42.0, 0.0, 12.0)
    )
    val chi = Statistics.chiSquared(sc.parallelize(data))
    assert(chi.statistic ~= 21.9958 absTol 1e-3)
    assert(chi.degreesOfFreedom === Array(6))
    assert(chi.pValue ~= 0.001213 absTol 1e-6)
    assert(chi.method === ChiSquaredTest.PEARSON)
    assert(chi.nullHypothesis === ChiSquaredTest.NullHypothesis.independence.toString)

    // Negative counts
    val negCounts = Seq(
      Vectors.dense(4.0, 5.0, 3.0, 3.0),
      Vectors.dense(0.0, -3.0, 0.0, 5.0),
      Vectors.dense(9.0, 0.0, 0.0, 1.0)
    )
    intercept[SparkException](Statistics.chiSquared(sc.parallelize(negCounts)))

    // Row sum = 0.0
    val rowZero = Seq(
      Vectors.dense(4.0, 5.0, 3.0, 3.0),
      Vectors.dense(0.0, 0.0, 0.0, 0.0),
      Vectors.dense(9.0, 0.0, 0.0, 1.0)
    )
    intercept[SparkException](Statistics.chiSquared(sc.parallelize(rowZero)))

    // Column sum  = 0.0
    val colZero = Seq(
      Vectors.dense(1.0, 0.0, 0.0, 2.0),
      Vectors.dense(4.0, 5.0, 0.0, 3.0),
      Vectors.dense(9.0, 0.0, 0.0, 1.0)
    )
    // IllegalArgumentException thrown here since it's thrown on driver, not inside a task
    intercept[IllegalArgumentException](Statistics.chiSquared(sc.parallelize(colZero)))
  }
}
