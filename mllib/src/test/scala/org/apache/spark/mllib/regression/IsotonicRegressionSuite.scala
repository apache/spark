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

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.{LocalClusterSparkContext, MLlibTestSparkContext}
import org.scalatest.{Matchers, FunSuite}
import scala.util.Random
import org.apache.spark.mllib.util.IsotonicDataGenerator._

class IsotonicRegressionSuite
  extends FunSuite
  with MLlibTestSparkContext
  with Matchers {

  private def round(d: Double): Double =
    Math.round(d * 100).toDouble / 100

  test("increasing isotonic regression") {
    val trainRDD = sc.parallelize(generateIsotonicInput(1, 2, 3, 3, 1, 6, 7, 8, 11, 9, 10, 12, 14, 15, 17, 16, 17, 18, 19, 20)).cache()

    val alg = new PoolAdjacentViolators
    val model = alg.run(trainRDD, true)

    model.predictions should be(generateIsotonicInput(1, 2, 7d/3, 7d/3, 7d/3, 6, 7, 8, 10, 10, 10, 12, 14, 15, 16.5, 16.5, 17, 18, 19, 20))
  }

  test("increasing isotonic regression using api") {
    val trainRDD = sc.parallelize(generateIsotonicInput(1, 2, 3, 3, 1, 6, 7, 8, 11, 9, 10, 12, 14, 15, 17, 16, 17, 18, 19, 20)).cache()

    val model = IsotonicRegression.train(trainRDD, true)

    model.predictions should be(generateIsotonicInput(1, 2, 7d/3, 7d/3, 7d/3, 6, 7, 8, 10, 10, 10, 12, 14, 15, 16.5, 16.5, 17, 18, 19, 20))
  }

  test("isotonic regression with size 0") {
    val trainRDD = sc.parallelize(List[(Double, Double, Double)]()).cache()

    val alg = new PoolAdjacentViolators
    val model = alg.run(trainRDD, true)

    model.predictions should be(List())
  }

  test("isotonic regression with size 1") {
    val trainRDD = sc.parallelize(generateIsotonicInput(1)).cache()

    val alg = new PoolAdjacentViolators
    val model = alg.run(trainRDD, true)

    model.predictions should be(generateIsotonicInput(1))
  }

  test("isotonic regression strictly increasing sequence") {
    val trainRDD = sc.parallelize(generateIsotonicInput(1, 2, 3, 4, 5)).cache()

    val alg = new PoolAdjacentViolators
    val model = alg.run(trainRDD, true)

    model.predictions should be(generateIsotonicInput(1, 2, 3, 4, 5))
  }

  test("isotonic regression strictly decreasing sequence") {
    val trainRDD = sc.parallelize(generateIsotonicInput(5, 4, 3, 2, 1)).cache()

    val alg = new PoolAdjacentViolators
    val model = alg.run(trainRDD, true)

    model.predictions should be(generateIsotonicInput(3, 3, 3, 3, 3))
  }

  test("isotonic regression with last element violating monotonicity") {
    val trainRDD = sc.parallelize(generateIsotonicInput(1, 2, 3, 4, 2)).cache()

    val alg = new PoolAdjacentViolators
    val model = alg.run(trainRDD, true)

    model.predictions should be(generateIsotonicInput(1, 2, 3, 3, 3))
  }

  test("isotonic regression with first element violating monotonicity") {
    val trainRDD = sc.parallelize(generateIsotonicInput(4, 2, 3, 4, 5)).cache()

    val alg = new PoolAdjacentViolators
    val model = alg.run(trainRDD, true)

    model.predictions should be(generateIsotonicInput(3, 3, 3, 4, 5))
  }

  test("isotonic regression with negative labels") {
    val trainRDD = sc.parallelize(generateIsotonicInput(-1, -2, 0, 1, -1)).cache()

    val alg = new PoolAdjacentViolators
    val model = alg.run(trainRDD, true)

    model.predictions should be(generateIsotonicInput(-1.5, -1.5, 0, 0, 0))
  }

  test("isotonic regression with unordered input") {
    val trainRDD = sc.parallelize(generateIsotonicInput(1, 2, 3, 4, 5).reverse).cache()

    val alg = new PoolAdjacentViolators
    val model = alg.run(trainRDD, true)

    model.predictions should be(generateIsotonicInput(1, 2, 3, 4, 5))
  }

  test("weighted isotonic regression") {
    val trainRDD = sc.parallelize(generateWeightedIsotonicInput(Seq(1, 2, 3, 4, 2), Seq(1, 1, 1, 1, 2))).cache()

    val alg = new PoolAdjacentViolators
    val model = alg.run(trainRDD, true)

    model.predictions should be(generateWeightedIsotonicInput(Seq(1, 2, 2.75, 2.75,2.75), Seq(1, 1, 1, 1, 2)))
  }

  test("weighted isotonic regression with weights lower than 1") {
    val trainRDD = sc.parallelize(generateWeightedIsotonicInput(Seq(1, 2, 3, 2, 1), Seq(1, 1, 1, 0.1, 0.1))).cache()

    val alg = new PoolAdjacentViolators
    val model = alg.run(trainRDD, true)

    model.predictions.map(p => p.copy(_1 = round(p._1))) should be
      (generateWeightedIsotonicInput(Seq(1, 2, 3.3/1.2, 3.3/1.2, 3.3/1.2), Seq(1, 1, 1, 0.1, 0.1)))
  }

  test("weighted isotonic regression with negative weights") {
    val trainRDD = sc.parallelize(generateWeightedIsotonicInput(Seq(1, 2, 3, 2, 1), Seq(-1, 1, -3, 1, -5))).cache()

    val alg = new PoolAdjacentViolators
    val model = alg.run(trainRDD, true)

    model.predictions.map(p => p.copy(_1 = round(p._1))) should be
      (generateWeightedIsotonicInput(Seq(1, 10/6, 10/6, 10/6, 10/6), Seq(-1, 1, -3, 1, -5)))
  }

  test("weighted isotonic regression with zero weights") {
    val trainRDD = sc.parallelize(generateWeightedIsotonicInput(Seq(1, 2, 3, 2, 1), Seq(0, 0, 0, 1, 0))).cache()

    val alg = new PoolAdjacentViolators
    val model = alg.run(trainRDD, true)

    model.predictions should be(generateWeightedIsotonicInput(Seq(1, 2, 2, 2, 2), Seq(0, 0, 0, 1, 0)))
  }

  test("isotonic regression prediction") {
    val trainRDD = sc.parallelize(generateIsotonicInput(1, 2, 7, 1, 2)).cache()

    val alg = new PoolAdjacentViolators
    val model = alg.run(trainRDD, true)

    model.predict(0) should be(1)
    model.predict(2) should be(2)
    model.predict(3) should be(10d/3)
    model.predict(10) should be(10d/3)
  }

  test("isotonic regression RDD prediction") {
    val trainRDD = sc.parallelize(generateIsotonicInput(1, 2, 7, 1, 2)).cache()
    val testRDD = sc.parallelize(List(0d, 2d, 3d, 10d)).cache()

    val alg = new PoolAdjacentViolators
    val model = alg.run(trainRDD, true)

    model.predict(testRDD).collect() should be(Array(1, 2, 10d/3, 10d/3))
  }

  test("antitonic regression prediction") {
    val trainRDD = sc.parallelize(generateIsotonicInput(7, 5, 3, 5, 1)).cache()

    val alg = new PoolAdjacentViolators
    val model = alg.run(trainRDD, false)

    model.predict(0) should be(7)
    model.predict(2) should be(5)
    model.predict(3) should be(4)
    model.predict(10) should be(1)
  }
}

class IsotonicRegressionClusterSuite
  extends FunSuite
  with LocalClusterSparkContext {

  //TODO: FIX
  test("task size should be small in both training and prediction") {
    val n = 135000

    val trainData = (0 to n).map(i => (i.toDouble, i.toDouble, 1d))
    val points = sc.parallelize(trainData, 1)

    // If we serialize data directly in the task closure, the size of the serialized task would be
    // greater than 1MB and hence Spark would throw an error.
    val model = IsotonicRegression.train(points, true)
    val predictions = model.predict(points.map(_._2))
  }
}