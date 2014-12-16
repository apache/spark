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

import java.util.Random

import org.scalatest.FunSuite

import org.apache.spark.SparkException
import org.apache.spark.mllib.linalg.{DenseVector, Matrices, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.test.ChiSqTest
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._

class HypothesisTestSuite extends FunSuite with MLlibTestSparkContext {

  test("chi squared pearson goodness of fit") {

    val observed = new DenseVector(Array[Double](4, 6, 5))
    val pearson = Statistics.chiSqTest(observed)

    // Results validated against the R command `chisq.test(c(4, 6, 5), p=c(1/3, 1/3, 1/3))`
    assert(pearson.statistic === 0.4)
    assert(pearson.degreesOfFreedom === 2)
    assert(pearson.pValue ~== 0.8187 relTol 1e-4)
    assert(pearson.method === ChiSqTest.PEARSON.name)
    assert(pearson.nullHypothesis === ChiSqTest.NullHypothesis.goodnessOfFit.toString)

    // different expected and observed sum
    val observed1 = new DenseVector(Array[Double](21, 38, 43, 80))
    val expected1 = new DenseVector(Array[Double](3, 5, 7, 20))
    val pearson1 = Statistics.chiSqTest(observed1, expected1)

    // Results validated against the R command
    // `chisq.test(c(21, 38, 43, 80), p=c(3/35, 1/7, 1/5, 4/7))`
    assert(pearson1.statistic ~== 14.1429 relTol 1e-4)
    assert(pearson1.degreesOfFreedom === 3)
    assert(pearson1.pValue ~== 0.002717 relTol 1e-4)
    assert(pearson1.method === ChiSqTest.PEARSON.name)
    assert(pearson1.nullHypothesis === ChiSqTest.NullHypothesis.goodnessOfFit.toString)

    // Vectors with different sizes
    val observed3 = new DenseVector(Array(1.0, 2.0, 3.0))
    val expected3 = new DenseVector(Array(1.0, 2.0, 3.0, 4.0))
    intercept[IllegalArgumentException](Statistics.chiSqTest(observed3, expected3))

    // negative counts in observed
    val negObs = new DenseVector(Array(1.0, 2.0, 3.0, -4.0))
    intercept[IllegalArgumentException](Statistics.chiSqTest(negObs, expected1))

    // count = 0.0 in expected but not observed
    val zeroExpected = new DenseVector(Array(1.0, 0.0, 3.0))
    val inf = Statistics.chiSqTest(observed, zeroExpected)
    assert(inf.statistic === Double.PositiveInfinity)
    assert(inf.degreesOfFreedom === 2)
    assert(inf.pValue === 0.0)
    assert(inf.method === ChiSqTest.PEARSON.name)
    assert(inf.nullHypothesis === ChiSqTest.NullHypothesis.goodnessOfFit.toString)

    // 0.0 in expected and observed simultaneously
    val zeroObserved = new DenseVector(Array(2.0, 0.0, 1.0))
    intercept[IllegalArgumentException](Statistics.chiSqTest(zeroObserved, zeroExpected))
  }

  test("chi squared pearson matrix independence") {
    val data = Array(40.0, 24.0, 29.0, 56.0, 32.0, 42.0, 31.0, 10.0, 0.0, 30.0, 15.0, 12.0)
    // [[40.0, 56.0, 31.0, 30.0],
    //  [24.0, 32.0, 10.0, 15.0],
    //  [29.0, 42.0, 0.0,  12.0]]
    val chi = Statistics.chiSqTest(Matrices.dense(3, 4, data))
    // Results validated against R command
    // `chisq.test(rbind(c(40, 56, 31, 30),c(24, 32, 10, 15), c(29, 42, 0, 12)))`
    assert(chi.statistic ~== 21.9958 relTol 1e-4)
    assert(chi.degreesOfFreedom === 6)
    assert(chi.pValue ~== 0.001213 relTol 1e-4)
    assert(chi.method === ChiSqTest.PEARSON.name)
    assert(chi.nullHypothesis === ChiSqTest.NullHypothesis.independence.toString)

    // Negative counts
    val negCounts = Array(4.0, 5.0, 3.0, -3.0)
    intercept[IllegalArgumentException](Statistics.chiSqTest(Matrices.dense(2, 2, negCounts)))

    // Row sum = 0.0
    val rowZero = Array(0.0, 1.0, 0.0, 2.0)
    intercept[IllegalArgumentException](Statistics.chiSqTest(Matrices.dense(2, 2, rowZero)))

    // Column sum  = 0.0
    val colZero = Array(0.0, 0.0, 2.0, 2.0)
    // IllegalArgumentException thrown here since it's thrown on driver, not inside a task
    intercept[IllegalArgumentException](Statistics.chiSqTest(Matrices.dense(2, 2, colZero)))
  }

  test("chi squared pearson RDD[LabeledPoint]") {
    // labels: 1.0 (2 / 6), 0.0 (4 / 6)
    // feature1: 0.5 (1 / 6), 1.5 (2 / 6), 3.5 (3 / 6)
    // feature2: 10.0 (1 / 6), 20.0 (1 / 6), 30.0 (2 / 6), 40.0 (2 / 6)
    val data = Seq(
      LabeledPoint(0.0, Vectors.dense(0.5, 10.0)),
      LabeledPoint(0.0, Vectors.dense(1.5, 20.0)),
      LabeledPoint(1.0, Vectors.dense(1.5, 30.0)),
      LabeledPoint(0.0, Vectors.dense(3.5, 30.0)),
      LabeledPoint(0.0, Vectors.dense(3.5, 40.0)),
      LabeledPoint(1.0, Vectors.dense(3.5, 40.0)))
    for (numParts <- List(2, 4, 6, 8)) {
      val chi = Statistics.chiSqTest(sc.parallelize(data, numParts))
      val feature1 = chi(0)
      assert(feature1.statistic === 0.75)
      assert(feature1.degreesOfFreedom === 2)
      assert(feature1.pValue ~== 0.6873 relTol 1e-4)
      assert(feature1.method === ChiSqTest.PEARSON.name)
      assert(feature1.nullHypothesis === ChiSqTest.NullHypothesis.independence.toString)
      val feature2 = chi(1)
      assert(feature2.statistic === 1.5)
      assert(feature2.degreesOfFreedom === 3)
      assert(feature2.pValue ~== 0.6823 relTol 1e-4)
      assert(feature2.method === ChiSqTest.PEARSON.name)
      assert(feature2.nullHypothesis === ChiSqTest.NullHypothesis.independence.toString)
    }

    // Test that the right number of results is returned
    val numCols = 1001
    val sparseData = Array(
      new LabeledPoint(0.0, Vectors.sparse(numCols, Seq((100, 2.0)))),
      new LabeledPoint(0.1, Vectors.sparse(numCols, Seq((200, 1.0)))))
    val chi = Statistics.chiSqTest(sc.parallelize(sparseData))
    assert(chi.size === numCols)
    assert(chi(1000) != null) // SPARK-3087

    // Detect continous features or labels
    val random = new Random(11L)
    val continuousLabel =
      Seq.fill(100000)(LabeledPoint(random.nextDouble(), Vectors.dense(random.nextInt(2))))
    intercept[SparkException] {
      Statistics.chiSqTest(sc.parallelize(continuousLabel, 2))
    }
    val continuousFeature =
      Seq.fill(100000)(LabeledPoint(random.nextInt(2), Vectors.dense(random.nextDouble())))
    intercept[SparkException] {
      Statistics.chiSqTest(sc.parallelize(continuousFeature, 2))
    }
  }
}
