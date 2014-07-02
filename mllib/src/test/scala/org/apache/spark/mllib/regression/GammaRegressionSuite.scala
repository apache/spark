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

import org.scalatest.FunSuite
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.{LinearDataGenerator, LocalSparkContext}
import scala.util.Random
import breeze.linalg.DenseVector

class GammaRegressionSuite extends FunSuite with LocalSparkContext {

  def validatePrediction(predictions: Seq[Double], input: Seq[LabeledPoint]) {
    val numOffPredictions = predictions.zip(input).count { case (prediction, expected) =>
      // A prediction is off if the prediction is more than 0.5 away from expected value.
      math.abs(prediction - expected.label) > 1.0
    }
    // At least 80% of the predictions should be on.
    assert(numOffPredictions < input.length / 5)
  }

  def generateGammaInput(
      offset: Double,
      scale: Double,
      nPoints: Int,
      seed: Int): Seq[LabeledPoint]  = {
    val rnd = new Random(seed)
    val x1 = Array.fill[Double](nPoints)(rnd.nextDouble()*5)

    val y = (0 until nPoints).map { i =>
       math.exp(x1(i)*scale)
    }

    val testData = (0 until nPoints).map(i => LabeledPoint(y(i), Vectors.dense(Array(x1(i)))))
    testData
  }

  test("Gamma regression with the SGD optimizer") {
    val nPoints = 100

    val testData = generateGammaInput(0.0, 0.9, nPoints, 42)
    val testRDD = sc.parallelize(testData, 2).cache()
    val pr = new GammaRegressionWithSGD()

    pr.optimizer.setStepSize(0.05).setNumIterations(400).setMiniBatchFraction(1.0)
    val model = pr.run(testRDD)

    val weight1 = model.weights(0)

    assert(weight1 >= 0.8 && weight1 <= 1.0, weight1 + " not in [0.8, 1.0]")

    val validationData = generateGammaInput(0.0, 0.9, nPoints, 42)
    val validationRDD  = sc.parallelize(validationData, 2)

    // Test prediction on RDD.
    validatePrediction(model.predict(validationRDD.map(_.features)).collect(), validationData)

    // Test prediction on Array.
    validatePrediction(validationData.map(row => model.predict(row.features)), validationData)
  }

  test("Gamma regression with the L-BFGS optimizer") {
    val nPoints = 100

    val testData = generateGammaInput(0.0, 0.9, nPoints, 42)
    val testRDD = sc.parallelize(testData, 2).cache()
    val pr = new GammaRegressionWithLBFGS()

    val model = pr.run(testRDD)

    val weight1 = model.weights(0)

    assert(weight1 >= 0.8 && weight1 <= 1.0, weight1 + " not in [0.8, 1.0]")

    val validationData = generateGammaInput(0.0, 0.9, nPoints, 42)
    val validationRDD  = sc.parallelize(validationData, 2)

    // Test prediction on RDD.
    validatePrediction(model.predict(validationRDD.map(_.features)).collect(), validationData)

    // Test prediction on Array.
    validatePrediction(validationData.map(row => model.predict(row.features)), validationData)
  }

}
