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

class IsotonicRegressionSuite
  extends FunSuite
  with MLlibTestSparkContext
  with Matchers {

  def generateDataPoints(labels: Double*): Seq[LabeledPoint] =
    labels.zip((1 to labels.size)).map(point => LabeledPoint(point._1, Vectors.dense(point._2)))

  test("increasing isotonic regression") {
    val testRDD = sc.parallelize(generateDataPoints(1, 2, 3, 3, 1, 6, 7, 8, 11, 9, 10, 12, 14, 15, 17, 16, 17, 18, 19, 20)).cache()

    val alg = new PoolAdjacentViolators
    val model = alg.run(testRDD, Isotonic)

    model.predictions should be(generateDataPoints(1, 2, 7d/3, 7d/3, 7d/3, 6, 7, 8, 10, 10, 10, 12, 14, 15, 16.5, 16.5, 17, 18, 19, 20))
  }

  test("isotonic regression with size 0") {
    val testRDD = sc.parallelize(List[LabeledPoint]()).cache()

    val alg = new PoolAdjacentViolators
    val model = alg.run(testRDD, Isotonic)

    model.predictions should be(List())
  }

  test("isotonic regression with size 1") {
    val testRDD = sc.parallelize(generateDataPoints(1)).cache()

    val alg = new PoolAdjacentViolators
    val model = alg.run(testRDD, Isotonic)

    model.predictions should be(generateDataPoints(1))
  }

  test("isotonic regression strictly increasing sequence") {
    val testRDD = sc.parallelize(generateDataPoints(1, 2, 3, 4, 5)).cache()

    val alg = new PoolAdjacentViolators
    val model = alg.run(testRDD, Isotonic)

    model.predictions should be(generateDataPoints(1, 2, 3, 4, 5))
  }

  test("isotonic regression strictly decreasing sequence") {
    val testRDD = sc.parallelize(generateDataPoints(5, 4, 3, 2, 1)).cache()

    val alg = new PoolAdjacentViolators
    val model = alg.run(testRDD, Isotonic)

    model.predictions should be(generateDataPoints(3, 3, 3, 3, 3))
  }

  test("isotonic regression with last element violating monotonicity") {
    val testRDD = sc.parallelize(generateDataPoints(1, 2, 3, 4, 2)).cache()

    val alg = new PoolAdjacentViolators
    val model = alg.run(testRDD, Isotonic)

    model.predictions should be(generateDataPoints(1, 2, 3, 3, 3))
  }

  test("isotonic regression with first element violating monotonicity") {
    val testRDD = sc.parallelize(generateDataPoints(4, 2, 3, 4, 5)).cache()

    val alg = new PoolAdjacentViolators
    val model = alg.run(testRDD, Isotonic)

    model.predictions should be(generateDataPoints(3, 3, 3, 4, 5))
  }

  test("isotonic regression with unordered input") {
    val testRDD = sc.parallelize(List[LabeledPoint]()).cache()

    val alg = new PoolAdjacentViolators
    val model = alg.run(testRDD, Isotonic)

    model.predictions should be(List())
  }

  test("isotonic regression prediction") {
    val testRDD = sc.parallelize(generateDataPoints(1, 2, 7, 1, 2)).cache()

    val alg = new PoolAdjacentViolators
    val model = alg.run(testRDD, Isotonic)

    model.predict(Vectors.dense(0)) should be(1)
    model.predict(Vectors.dense(2)) should be(2)
    model.predict(Vectors.dense(3)) should be(10d/3)
    model.predict(Vectors.dense(10)) should be(10d/3)
  }

  test("antitonic regression prediction") {
    val testRDD = sc.parallelize(generateDataPoints(7, 5, 3, 5, 1)).cache()

    val alg = new PoolAdjacentViolators
    val model = alg.run(testRDD, Antitonic)

    model.predict(Vectors.dense(0)) should be(7)
    model.predict(Vectors.dense(2)) should be(5)
    model.predict(Vectors.dense(3)) should be(4)
    model.predict(Vectors.dense(10)) should be(1)
  }
}

class IsotonicRegressionClusterSuite extends FunSuite with LocalClusterSparkContext {

  test("task size should be small in both training and prediction") {
    val m = 4
    val n = 200000
    val points = sc.parallelize(0 until m, 2).mapPartitionsWithIndex { (idx, iter) =>
      val random = new Random(idx)
      iter.map(i => LabeledPoint(1.0, Vectors.dense(Array.fill(n)(random.nextDouble()))))
    }.cache()

    // If we serialize data directly in the task closure, the size of the serialized task would be
    // greater than 1MB and hence Spark would throw an error.
    val model = IsotonicRegression.train(points, Isotonic)
    val predictions = model.predict(points.map(_.features))
  }
}