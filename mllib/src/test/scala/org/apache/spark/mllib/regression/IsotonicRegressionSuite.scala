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

package org.apache.spark.mllib.regression

import org.scalatest.Matchers

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.util.Utils

class IsotonicRegressionSuite extends SparkFunSuite with MLlibTestSparkContext with Matchers {

  private def round(d: Double) = {
    math.round(d * 100).toDouble / 100
  }

  private def generateIsotonicInput(labels: Seq[Double]): Seq[(Double, Double, Double)] = {
    Seq.tabulate(labels.size)(i => (labels(i), i.toDouble, 1d))
  }

  private def generateIsotonicInput(
      labels: Seq[Double],
      weights: Seq[Double]): Seq[(Double, Double, Double)] = {
    Seq.tabulate(labels.size)(i => (labels(i), i.toDouble, weights(i)))
  }

  private def runIsotonicRegression(
      labels: Seq[Double],
      weights: Seq[Double],
      isotonic: Boolean): IsotonicRegressionModel = {
    val trainRDD = sc.parallelize(generateIsotonicInput(labels, weights)).cache()
    new IsotonicRegression().setIsotonic(isotonic).run(trainRDD)
  }

  private def runIsotonicRegression(
      labels: Seq[Double],
      isotonic: Boolean): IsotonicRegressionModel = {
    runIsotonicRegression(labels, Array.fill(labels.size)(1d), isotonic)
  }

  test("increasing isotonic regression") {
    /*
     The following result could be re-produced with sklearn.

     > from sklearn.isotonic import IsotonicRegression
     > x = range(9)
     > y = [1, 2, 3, 1, 6, 17, 16, 17, 18]
     > ir = IsotonicRegression(x, y)
     > print ir.predict(x)

     array([  1. ,   2. ,   2. ,   2. ,   6. ,  16.5,  16.5,  17. ,  18. ])
     */
    val model = runIsotonicRegression(Seq(1, 2, 3, 1, 6, 17, 16, 17, 18), true)

    assert(Array.tabulate(9)(x => model.predict(x)) === Array(1, 2, 2, 2, 6, 16.5, 16.5, 17, 18))

    assert(model.boundaries === Array(0, 1, 3, 4, 5, 6, 7, 8))
    assert(model.predictions === Array(1, 2, 2, 6, 16.5, 16.5, 17.0, 18.0))
    assert(model.isotonic)
  }

  test("model save/load") {
    val boundaries = Array(0.0, 1.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0)
    val predictions = Array(1, 2, 2, 6, 16.5, 16.5, 17.0, 18.0)
    val model = new IsotonicRegressionModel(boundaries, predictions, true)

    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString

    // Save model, load it back, and compare.
    try {
      model.save(sc, path)
      val sameModel = IsotonicRegressionModel.load(sc, path)
      assert(model.boundaries === sameModel.boundaries)
      assert(model.predictions === sameModel.predictions)
      assert(model.isotonic === model.isotonic)
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }

  test("isotonic regression with size 0") {
    val model = runIsotonicRegression(Seq(), true)

    assert(model.predictions === Array())
  }

  test("isotonic regression with size 1") {
    val model = runIsotonicRegression(Seq(1), true)

    assert(model.predictions === Array(1.0))
  }

  test("isotonic regression strictly increasing sequence") {
    val model = runIsotonicRegression(Seq(1, 2, 3, 4, 5), true)

    assert(model.predictions === Array(1, 2, 3, 4, 5))
  }

  test("isotonic regression strictly decreasing sequence") {
    val model = runIsotonicRegression(Seq(5, 4, 3, 2, 1), true)

    assert(model.boundaries === Array(0, 4))
    assert(model.predictions === Array(3, 3))
  }

  test("isotonic regression with last element violating monotonicity") {
    val model = runIsotonicRegression(Seq(1, 2, 3, 4, 2), true)

    assert(model.boundaries === Array(0, 1, 2, 4))
    assert(model.predictions === Array(1, 2, 3, 3))
  }

  test("isotonic regression with first element violating monotonicity") {
    val model = runIsotonicRegression(Seq(4, 2, 3, 4, 5), true)

    assert(model.boundaries === Array(0, 2, 3, 4))
    assert(model.predictions === Array(3, 3, 4, 5))
  }

  test("isotonic regression with negative labels") {
    val model = runIsotonicRegression(Seq(-1, -2, 0, 1, -1), true)

    assert(model.boundaries === Array(0, 1, 2, 4))
    assert(model.predictions === Array(-1.5, -1.5, 0, 0))
  }

  test("isotonic regression with unordered input") {
    val trainRDD = sc.parallelize(generateIsotonicInput(Seq(1, 2, 3, 4, 5)).reverse, 2).cache()

    val model = new IsotonicRegression().run(trainRDD)
    assert(model.predictions === Array(1, 2, 3, 4, 5))
  }

  test("weighted isotonic regression") {
    val model = runIsotonicRegression(Seq(1, 2, 3, 4, 2), Seq(1, 1, 1, 1, 2), true)

    assert(model.boundaries === Array(0, 1, 2, 4))
    assert(model.predictions === Array(1, 2, 2.75, 2.75))
  }

  test("weighted isotonic regression with weights lower than 1") {
    val model = runIsotonicRegression(Seq(1, 2, 3, 2, 1), Seq(1, 1, 1, 0.1, 0.1), true)

    assert(model.boundaries === Array(0, 1, 2, 4))
    assert(model.predictions.map(round) === Array(1, 2, 3.3/1.2, 3.3/1.2))
  }

  test("weighted isotonic regression with negative weights") {
    val ex = intercept[SparkException] {
      runIsotonicRegression(Seq(1, 2, 3, 2, 1), Seq(-1, 1, -3, 1, -5), true)
    }
    assert(ex.getCause.isInstanceOf[IllegalArgumentException])
  }

  test("weighted isotonic regression with zero weights") {
    val model = runIsotonicRegression(Seq(1, 2, 3, 2, 1, 0), Seq(0, 0, 0, 1, 1, 0), true)
    assert(model.boundaries === Array(3, 4))
    assert(model.predictions === Array(1.5, 1.5))
  }

  test("SPARK-16426 isotonic regression with duplicate features that produce NaNs") {
    val trainRDD = sc.parallelize(Seq[(Double, Double, Double)]((2, 1, 1), (1, 1, 1), (0, 2, 1),
                                                                (1, 2, 1), (0.5, 3, 1), (0, 3, 1)),
                                  2)

    val model = new IsotonicRegression().run(trainRDD)

    assert(model.boundaries === Array(1.0, 3.0))
    assert(model.predictions === Array(0.75, 0.75))
  }

  test("isotonic regression prediction") {
    val model = runIsotonicRegression(Seq(1, 2, 7, 1, 2), true)

    assert(model.predict(-2) === 1)
    assert(model.predict(-1) === 1)
    assert(model.predict(0.5) === 1.5)
    assert(model.predict(0.75) === 1.75)
    assert(model.predict(1) === 2)
    assert(model.predict(2) === 10d/3)
    assert(model.predict(9) === 10d/3)
  }

  test("isotonic regression prediction with duplicate features") {
    val trainRDD = sc.parallelize(
      Seq[(Double, Double, Double)](
        (2, 1, 1), (1, 1, 1), (4, 2, 1), (2, 2, 1), (6, 3, 1), (5, 3, 1)), 2).cache()
    val model = new IsotonicRegression().run(trainRDD)

    assert(model.predict(0) === 1)
    assert(model.predict(1.5) === 2)
    assert(model.predict(2.5) === 4.5)
    assert(model.predict(4) === 6)
  }

  test("antitonic regression prediction with duplicate features") {
    val trainRDD = sc.parallelize(
      Seq[(Double, Double, Double)](
        (5, 1, 1), (6, 1, 1), (2, 2, 1), (4, 2, 1), (1, 3, 1), (2, 3, 1)), 2).cache()
    val model = new IsotonicRegression().setIsotonic(false).run(trainRDD)

    assert(model.predict(0) === 6)
    assert(model.predict(1.5) === 4.5)
    assert(model.predict(2.5) === 2)
    assert(model.predict(4) === 1)
  }

  test("isotonic regression RDD prediction") {
    val model = runIsotonicRegression(Seq(1, 2, 7, 1, 2), true)

    val testRDD = sc.parallelize(List(-2.0, -1.0, 0.5, 0.75, 1.0, 2.0, 9.0), 2).cache()
    val predictions = testRDD.map(x => (x, model.predict(x))).collect().sortBy(_._1).map(_._2)
    assert(predictions === Array(1, 1, 1.5, 1.75, 2, 10.0/3, 10.0/3))
  }

  test("antitonic regression prediction") {
    val model = runIsotonicRegression(Seq(7, 5, 3, 5, 1), false)

    assert(model.predict(-2) === 7)
    assert(model.predict(-1) === 7)
    assert(model.predict(0.5) === 6)
    assert(model.predict(0.75) === 5.5)
    assert(model.predict(1) === 5)
    assert(model.predict(2) === 4)
    assert(model.predict(9) === 1)
  }

  test("model construction") {
    val model = new IsotonicRegressionModel(Array(0.0, 1.0), Array(1.0, 2.0), isotonic = true)
    assert(model.predict(-0.5) === 1.0)
    assert(model.predict(0.0) === 1.0)
    assert(model.predict(0.5) ~== 1.5 absTol 1e-14)
    assert(model.predict(1.0) === 2.0)
    assert(model.predict(1.5) === 2.0)

    intercept[IllegalArgumentException] {
      // different array sizes.
      new IsotonicRegressionModel(Array(0.0, 1.0), Array(1.0), isotonic = true)
    }

    intercept[IllegalArgumentException] {
      // unordered boundaries
      new IsotonicRegressionModel(Array(1.0, 0.0), Array(1.0, 2.0), isotonic = true)
    }

    intercept[IllegalArgumentException] {
      // unordered predictions (isotonic)
      new IsotonicRegressionModel(Array(0.0, 1.0), Array(2.0, 1.0), isotonic = true)
    }

    intercept[IllegalArgumentException] {
      // unordered predictions (antitonic)
      new IsotonicRegressionModel(Array(0.0, 1.0), Array(1.0, 2.0), isotonic = false)
    }
  }
}
