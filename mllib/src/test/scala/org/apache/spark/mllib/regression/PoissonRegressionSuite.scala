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

class PoissonRegressionSuite extends FunSuite with LocalSparkContext {

  def validatePrediction(predictions: Seq[Double], input: Seq[LabeledPoint]) {
    val numOffPredictions = predictions.zip(input).count { case (prediction, expected) =>
      // A prediction is off if the prediction is more than 1 away from expected value.
      math.abs(prediction - expected.label) > 1
    }
    // At least 80% of the predictions should be on.
    assert(numOffPredictions < input.length / 5)
  }

  // Ideally, y should be integer. We use double instead since we do not have no Gamma distribution generator
  def generatePoissonInput(
      offset: Double,
      scale: Double,
      nPoints: Int,
      seed: Int): Seq[LabeledPoint]  = {
    val rnd = new Random(seed)
    val x1 = Array.fill[Double](nPoints)(rnd.nextDouble()*5)
    val y = (0 until nPoints).map { i =>
       math.exp(x1(i)*scale+offset)
    }

    val testData = (0 until nPoints).map(i => LabeledPoint(y(i), Vectors.dense(Array(x1(i)))))
    testData
  }

  test("poisson regression") {
    val nPoints = 10

    val testData = generatePoissonInput(0.0, 0.3, nPoints, 42)
    val testRDD = sc.parallelize(testData, 2).cache()

    val lr = new LinearRegressionWithSGD()
    val pr = new PoissonRegressionWithSGD()
    val logTestRDD =  testRDD.map(p => LabeledPoint(math.log(p.label+0.5), p.features))
    lr.optimizer.setStepSize(0.1).setNumIterations(100)
    val LRmodel = lr.run(logTestRDD)
    val initialWeights = LRmodel.weights

    // Poisson Regression with default initialWeights
    pr.optimizer.setStepSize(0.2).setNumIterations(200)
    // Poisson Regression with initialWeights from linear regression on log data
    val model = pr.run(testRDD, initialWeights)
    val weight1 = model.weights(0)
    println(initialWeights)

    println(weight1)
    assert(weight1 >= 0.2 && weight1 <= 0.4, weight1 + " not in [0.2, 0.4]")

    pr.optimizer.setStepSize(0.2).setNumIterations(200)
    val modelDefault = pr.run(testRDD)
    val weight1Default = modelDefault.weights(0)
    assert(weight1Default >= 0.2 && weight1Default <= 0.4, weight1Default + " not in [0.2, 0.4]")

    val validationData = generatePoissonInput(0.0, 0.3, nPoints, 42)
    val validationRDD  = sc.parallelize(validationData, 2)

    // Test prediction on RDD.
    validatePrediction(model.predict(validationRDD.map(_.features)).collect(), validationData)

    // Test prediction on Array.
    validatePrediction(validationData.map(row => model.predict(row.features)), validationData)
  }
}
