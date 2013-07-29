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

package spark.mllib.classification

import scala.util.Random
import scala.math.signum

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite

import spark.SparkContext

class SVMSuite extends FunSuite with BeforeAndAfterAll {
  val sc = new SparkContext("local", "test")

  override def afterAll() {
    sc.stop()
    System.clearProperty("spark.driver.port")
  }

  // Generate noisy input of the form Y = signum(x.dot(weights) + intercept + noise)
  def generateSVMInput(
    intercept: Double,
    weights: Array[Double],
    nPoints: Int,
    seed: Int): Seq[(Double, Array[Double])] = {
    val rnd = new Random(seed)
    val x = Array.fill[Array[Double]](nPoints)(Array.fill[Double](weights.length)(rnd.nextGaussian()))
    val y = x.map(xi =>
      signum((xi zip weights).map(xw => xw._1*xw._2).reduce(_+_) + intercept + 0.1 * rnd.nextGaussian())
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

  test("SVMLocalRandomSGD") {
    val nPoints = 10000

    val A = 2.0
    val B = -1.5
    val C = 1.0

    val testData = generateSVMInput(A, Array[Double](B,C), nPoints, 42)

    val testRDD = sc.parallelize(testData, 2)
    testRDD.cache()

    val svm = new SVMLocalRandomSGD().setStepSize(1.0).setRegParam(1.0).setNumIterations(100)

    val model = svm.train(testRDD)

    val validationData = generateSVMInput(A, Array[Double](B,C), nPoints, 17)
    val validationRDD  = sc.parallelize(validationData,2)

    // Test prediction on RDD.
    validatePrediction(model.predict(validationRDD.map(_._2)).collect(), validationData)

    // Test prediction on Array.
    validatePrediction(validationData.map(row => model.predict(row._2)), validationData)
  }

  test("SVMLocalRandomSGD with initial weights") {
    val nPoints = 10000

    val A = 2.0
    val B = -1.5
    val C = 1.0

    val testData = generateSVMInput(A, Array[Double](B,C), nPoints, 42)

    val initialB = -1.0
    val initialC = -1.0
    val initialWeights = Array(initialB,initialC)

    val testRDD = sc.parallelize(testData, 2)
    testRDD.cache()

    val svm = new SVMLocalRandomSGD().setStepSize(1.0).setRegParam(1.0).setNumIterations(100)

    val model = svm.train(testRDD, initialWeights)

    val validationData = generateSVMInput(A, Array[Double](B,C), nPoints, 17)
    val validationRDD  = sc.parallelize(validationData,2)

    // Test prediction on RDD.
    validatePrediction(model.predict(validationRDD.map(_._2)).collect(), validationData)

    // Test prediction on Array.
    validatePrediction(validationData.map(row => model.predict(row._2)), validationData)
  }
}
