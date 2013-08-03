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

package spark.mllib.regression

import scala.util.Random

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite

import spark.SparkContext
import spark.mllib.optimization._

import org.jblas.DoubleMatrix


class LassoSuite extends FunSuite with BeforeAndAfterAll {
  val sc = new SparkContext("local", "test")

  override def afterAll() {
    sc.stop()
    System.clearProperty("spark.driver.port")
  }

  // Generate noisy input of the form Y = x.dot(weights) + intercept + noise
  def generateLassoInput(
    intercept: Double,
    weights: Array[Double],
    nPoints: Int,
    seed: Int): Seq[(Double, Array[Double])] = {
    val rnd = new Random(seed)
    val weightsMat = new DoubleMatrix(1, weights.length, weights:_*)
    val x = Array.fill[Array[Double]](nPoints)(
      Array.fill[Double](weights.length)(rnd.nextGaussian()))
    val y = x.map(xi =>
      (new DoubleMatrix(1, xi.length, xi:_*)).dot(weightsMat) + intercept + 0.1 * rnd.nextGaussian()
    )
    y zip x
  }

  def validatePrediction(predictions: Seq[Double], input: Seq[(Double, Array[Double])]) {
    val numOffPredictions = predictions.zip(input).filter { case (prediction, (expected, _)) =>
      // A prediction is off if the prediction is more than 0.5 away from expected value.
      math.abs(prediction - expected) > 0.5
    }.size
    // At least 80% of the predictions should be on.
    assert(numOffPredictions < input.length / 5)
  }

  test("Lasso local random SGD") {
    val nPoints = 10000

    val A = 2.0
    val B = -1.5
    val C = 1.0e-2

    val testData = generateLassoInput(A, Array[Double](B,C), nPoints, 42)

    val testRDD = sc.parallelize(testData, 2)
    testRDD.cache()

    val ls = new LassoWithSGD().setStepSize(1.0).setRegParam(0.01).setNumIterations(20)

    val model = ls.train(testRDD)

    val weight0 = model.weights(0)
    val weight1 = model.weights(1)
    assert(model.intercept >= 1.9 && model.intercept <= 2.1, model.intercept + " not in [1.9, 2.1]")
    assert(weight0 >= -1.60 && weight0 <= -1.40, weight0 + " not in [-1.6, -1.4]")
    assert(weight1 >= -1.0e-3 && weight1 <= 1.0e-3, weight1 + " not in [-0.001, 0.001]")

    val validationData = generateLassoInput(A, Array[Double](B,C), nPoints, 17)
    val validationRDD  = sc.parallelize(validationData,2)

    // Test prediction on RDD.
    validatePrediction(model.predict(validationRDD.map(_._2)).collect(), validationData)

    // Test prediction on Array.
    validatePrediction(validationData.map(row => model.predict(row._2)), validationData)
  }

  test("Lasso local random SGD with initial weights") {
    val nPoints = 10000

    val A = 2.0
    val B = -1.5
    val C = 1.0e-2

    val testData = generateLassoInput(A, Array[Double](B,C), nPoints, 42)

    val initialB = -1.0
    val initialC = -1.0
    val initialWeights = Array(initialB,initialC)

    val testRDD = sc.parallelize(testData, 2)
    testRDD.cache()

    val ls = new LassoWithSGD().setStepSize(1.0).setRegParam(0.01).setNumIterations(20)

    val model = ls.train(testRDD, initialWeights)

    val weight0 = model.weights(0)
    val weight1 = model.weights(1)
    assert(model.intercept >= 1.9 && model.intercept <= 2.1, model.intercept + " not in [1.9, 2.1]")
    assert(weight0 >= -1.60 && weight0 <= -1.40, weight0 + " not in [-1.6, -1.4]")
    assert(weight1 >= -1.0e-3 && weight1 <= 1.0e-3, weight1 + " not in [-0.001, 0.001]")

    val validationData = generateLassoInput(A, Array[Double](B,C), nPoints, 17)
    val validationRDD  = sc.parallelize(validationData,2)

    // Test prediction on RDD.
    validatePrediction(model.predict(validationRDD.map(_._2)).collect(), validationData)

    // Test prediction on Array.
    validatePrediction(validationData.map(row => model.predict(row._2)), validationData)
  }
}
