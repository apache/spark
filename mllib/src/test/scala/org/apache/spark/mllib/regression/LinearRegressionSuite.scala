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

import scala.util.Random

import org.scalatest.FunSuite

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.{LocalClusterSparkContext, LinearDataGenerator,
  MLlibTestSparkContext}
import org.apache.spark.util.Utils

private object LinearRegressionSuite {

  /** 3 features */
  val model = new LinearRegressionModel(weights = Vectors.dense(0.1, 0.2, 0.3), intercept = 0.5)
}

class LinearRegressionSuite extends FunSuite with MLlibTestSparkContext {

  def validatePrediction(predictions: Seq[Double], input: Seq[LabeledPoint]) {
    val numOffPredictions = predictions.zip(input).count { case (prediction, expected) =>
      // A prediction is off if the prediction is more than 0.5 away from expected value.
      math.abs(prediction - expected.label) > 0.5
    }
    // At least 80% of the predictions should be on.
    assert(numOffPredictions < input.length / 5)
  }

  // Test if we can correctly learn Y = 3 + 10*X1 + 10*X2
  test("linear regression") {
    val testRDD = sc.parallelize(LinearDataGenerator.generateLinearInput(
      3.0, Array(10.0, 10.0), 100, 42), 2).cache()
    val linReg = new LinearRegressionWithSGD().setIntercept(true)
    linReg.optimizer.setNumIterations(1000).setStepSize(1.0)

    val model = linReg.run(testRDD)
    assert(model.intercept >= 2.5 && model.intercept <= 3.5)

    val weights = model.weights
    assert(weights.size === 2)
    assert(weights(0) >= 9.0 && weights(0) <= 11.0)
    assert(weights(1) >= 9.0 && weights(1) <= 11.0)

    val validationData = LinearDataGenerator.generateLinearInput(
      3.0, Array(10.0, 10.0), 100, 17)
    val validationRDD = sc.parallelize(validationData, 2).cache()

    // Test prediction on RDD.
    validatePrediction(model.predict(validationRDD.map(_.features)).collect(), validationData)

    // Test prediction on Array.
    validatePrediction(validationData.map(row => model.predict(row.features)), validationData)
  }

  // Test if we can correctly learn Y = 10*X1 + 10*X2
  test("linear regression without intercept") {
    val testRDD = sc.parallelize(LinearDataGenerator.generateLinearInput(
      0.0, Array(10.0, 10.0), 100, 42), 2).cache()
    val linReg = new LinearRegressionWithSGD().setIntercept(false)
    linReg.optimizer.setNumIterations(1000).setStepSize(1.0)

    val model = linReg.run(testRDD)

    assert(model.intercept === 0.0)

    val weights = model.weights
    assert(weights.size === 2)
    assert(weights(0) >= 9.0 && weights(0) <= 11.0)
    assert(weights(1) >= 9.0 && weights(1) <= 11.0)

    val validationData = LinearDataGenerator.generateLinearInput(
      0.0, Array(10.0, 10.0), 100, 17)
    val validationRDD = sc.parallelize(validationData, 2).cache()

    // Test prediction on RDD.
    validatePrediction(model.predict(validationRDD.map(_.features)).collect(), validationData)

    // Test prediction on Array.
    validatePrediction(validationData.map(row => model.predict(row.features)), validationData)
  }

  // Test if we can correctly learn Y = 10*X1 + 10*X10000
  test("sparse linear regression without intercept") {
    val denseRDD = sc.parallelize(
      LinearDataGenerator.generateLinearInput(0.0, Array(10.0, 10.0), 100, 42), 2)
    val sparseRDD = denseRDD.map { case LabeledPoint(label, v) =>
      val sv = Vectors.sparse(10000, Seq((0, v(0)), (9999, v(1))))
      LabeledPoint(label, sv)
    }.cache()
    val linReg = new LinearRegressionWithSGD().setIntercept(false)
    linReg.optimizer.setNumIterations(1000).setStepSize(1.0)

    val model = linReg.run(sparseRDD)

    assert(model.intercept === 0.0)

    val weights = model.weights
    assert(weights.size === 10000)
    assert(weights(0) >= 9.0 && weights(0) <= 11.0)
    assert(weights(9999) >= 9.0 && weights(9999) <= 11.0)

    val validationData = LinearDataGenerator.generateLinearInput(0.0, Array(10.0, 10.0), 100, 17)
    val sparseValidationData = validationData.map { case LabeledPoint(label, v) =>
      val sv = Vectors.sparse(10000, Seq((0, v(0)), (9999, v(1))))
      LabeledPoint(label, sv)
    }
    val sparseValidationRDD = sc.parallelize(sparseValidationData, 2)

      // Test prediction on RDD.
    validatePrediction(
      model.predict(sparseValidationRDD.map(_.features)).collect(), sparseValidationData)

    // Test prediction on Array.
    validatePrediction(
      sparseValidationData.map(row => model.predict(row.features)), sparseValidationData)
  }

  test("model save/load") {
    val model = LinearRegressionSuite.model

    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString

    // Save model, load it back, and compare.
    try {
      model.save(sc, path)
      val sameModel = LinearRegressionModel.load(sc, path)
      assert(model.weights == sameModel.weights)
      assert(model.intercept == sameModel.intercept)
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }
}

class LinearRegressionClusterSuite extends FunSuite with LocalClusterSparkContext {

  test("task size should be small in both training and prediction") {
    val m = 4
    val n = 200000
    val points = sc.parallelize(0 until m, 2).mapPartitionsWithIndex { (idx, iter) =>
      val random = new Random(idx)
      iter.map(i => LabeledPoint(1.0, Vectors.dense(Array.fill(n)(random.nextDouble()))))
    }.cache()
    // If we serialize data directly in the task closure, the size of the serialized task would be
    // greater than 1MB and hence Spark would throw an error.
    val model = LinearRegressionWithSGD.train(points, 2)
    val predictions = model.predict(points.map(_.features))
  }
}
