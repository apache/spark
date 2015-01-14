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

import org.jblas.DoubleMatrix
import org.scalatest.FunSuite

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.{LocalClusterSparkContext, LinearDataGenerator,
  MLlibTestSparkContext}

class RidgeRegressionSuite extends FunSuite with MLlibTestSparkContext {

  def predictionError(predictions: Seq[Double], input: Seq[LabeledPoint]) = {
    predictions.zip(input).map { case (prediction, expected) =>
      (prediction - expected.label) * (prediction - expected.label)
    }.reduceLeft(_ + _) / predictions.size
  }

  test("ridge regression can help avoid overfitting") {

    // For small number of examples and large variance of error distribution,
    // ridge regression should give smaller generalization error that linear regression.

    val numExamples = 50
    val numFeatures = 20

    org.jblas.util.Random.seed(42)
    // Pick weights as random values distributed uniformly in [-0.5, 0.5]
    val w = DoubleMatrix.rand(numFeatures, 1).subi(0.5)

    // Use half of data for training and other half for validation
    val data = LinearDataGenerator.generateLinearInput(3.0, w.toArray, 2 * numExamples, 42, 10.0)
    val testData = data.take(numExamples)
    val validationData = data.takeRight(numExamples)

    val testRDD = sc.parallelize(testData, 2).cache()
    val validationRDD = sc.parallelize(validationData, 2).cache()

    // First run without regularization.
    val linearReg = new LinearRegressionWithSGD()
    linearReg.optimizer.setNumIterations(200)
                       .setStepSize(1.0)

    val linearModel = linearReg.run(testRDD)
    val linearErr = predictionError(
        linearModel.predict(validationRDD.map(_.features)).collect(), validationData)

    val ridgeReg = new RidgeRegressionWithSGD()
    ridgeReg.optimizer.setNumIterations(200)
                      .setRegParam(0.1)
                      .setStepSize(1.0)
    val ridgeModel = ridgeReg.run(testRDD)
    val ridgeErr = predictionError(
        ridgeModel.predict(validationRDD.map(_.features)).collect(), validationData)

    // Ridge validation error should be lower than linear regression.
    assert(ridgeErr < linearErr,
      "ridgeError (" + ridgeErr + ") was not less than linearError(" + linearErr + ")")
  }
}

class RidgeRegressionClusterSuite extends FunSuite with LocalClusterSparkContext {

  test("task size should be small in both training and prediction") {
    val m = 4
    val n = 200000
    val points = sc.parallelize(0 until m, 2).mapPartitionsWithIndex { (idx, iter) =>
      val random = new Random(idx)
      iter.map(i => LabeledPoint(1.0, Vectors.dense(Array.fill(n)(random.nextDouble()))))
    }.cache()
    // If we serialize data directly in the task closure, the size of the serialized task would be
    // greater than 1MB and hence Spark would throw an error.
    val model = RidgeRegressionWithSGD.train(points, 2)
    val predictions = model.predict(points.map(_.features))
  }
}
