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


import org.jblas.DoubleMatrix
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite

import org.apache.spark.SparkContext
import org.apache.spark.mllib.util.LinearDataGenerator

class RidgeRegressionSuite extends FunSuite with BeforeAndAfterAll {
  @transient private var sc: SparkContext = _

  override def beforeAll() {
    sc = new SparkContext("local", "test")
  }

  override def afterAll() {
    sc.stop()
    System.clearProperty("spark.driver.port")
  }

  def predictionError(predictions: Seq[Double], input: Seq[LabeledPoint]) = {
    predictions.zip(input).map { case (prediction, expected) =>
      (prediction - expected.label) * (prediction - expected.label)
    }.reduceLeft(_ + _) / predictions.size
  }

  test("regularization with skewed weights") {
    val nexamples = 200
    val nfeatures = 20
    val eps = 10

    org.jblas.util.Random.seed(42)
    // Pick weights as random values distributed uniformly in [-0.5, 0.5]
    val w = DoubleMatrix.rand(nfeatures, 1).subi(0.5)
    // Set first two weights to eps
    w.put(0, 0, eps)
    w.put(1, 0, eps)

    // Use half of data for training and other half for validation
    val data = LinearDataGenerator.generateLinearInput(3.0, w.toArray, 2*nexamples, 42, eps)
    val testData = data.take(nexamples)
    val validationData = data.takeRight(nexamples)

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

    // Ridge CV-error should be lower than linear regression
    assert(ridgeErr < linearErr,
      "ridgeError (" + ridgeErr + ") was not less than linearError(" + linearErr + ")")
  }
}
