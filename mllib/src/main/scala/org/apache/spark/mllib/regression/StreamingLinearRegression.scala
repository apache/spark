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

import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg.Vectors

/**
 * Train or predict a linear regression model on streaming data. Training uses
 * Stochastic Gradient Descent to update the model based on each new batch of
 * incoming data from a DStream (see LinearRegressionWithSGD for model equation)
 *
 * Each batch of data is assumed to be an RDD of LabeledPoints.
 * The number of data points per batch can vary, but the number
 * of features must be constant.
 */
@Experimental
class StreamingLinearRegressionWithSGD private (
    private var stepSize: Double,
    private var numIterations: Int,
    private var miniBatchFraction: Double,
    private var numFeatures: Int)
  extends StreamingRegression[LinearRegressionModel, LinearRegressionWithSGD] with Serializable {

  val algorithm = new LinearRegressionWithSGD(stepSize, numIterations, miniBatchFraction)

  var model = algorithm.createModel(Vectors.dense(new Array[Double](numFeatures)), 0.0)

}

/**
 * Top-level methods for calling StreamingLinearRegressionWithSGD.
 */
@Experimental
object StreamingLinearRegressionWithSGD {

  /**
   * Start a streaming Linear Regression model by setting optimization parameters.
   *
   * @param numIterations Number of iterations of gradient descent to run.
   * @param stepSize Step size to be used for each iteration of gradient descent.
   * @param miniBatchFraction Fraction of data to be used per iteration.
   * @param numFeatures Number of features per record, must be constant for all batches of data.
   */
  def start(
      stepSize: Double,
      numIterations: Int,
      miniBatchFraction: Double,
      numFeatures: Int): StreamingLinearRegressionWithSGD = {
    new StreamingLinearRegressionWithSGD(stepSize, numIterations, miniBatchFraction, numFeatures)
  }

  /**
   * Start a streaming Linear Regression model by setting optimization parameters.
   *
   * @param numIterations Number of iterations of gradient descent to run.
   * @param stepSize Step size to be used for each iteration of gradient descent.
   * @param numFeatures Number of features per record, must be constant for all batches of data.
   */
  def start(
      numIterations: Int,
      stepSize: Double,
      numFeatures: Int): StreamingLinearRegressionWithSGD = {
    start(stepSize, numIterations, 1.0, numFeatures)
  }

  /**
   * Start a streaming Linear Regression model by setting optimization parameters.
   *
   * @param numIterations Number of iterations of gradient descent to run.
   * @param numFeatures Number of features per record, must be constant for all batches of data.
   */
  def start(
      numIterations: Int,
      numFeatures: Int): StreamingLinearRegressionWithSGD = {
    start(0.1, numIterations, 1.0, numFeatures)
  }

  /**
   * Start a streaming Linear Regression model by setting optimization parameters.
   *
   * @param numFeatures Number of features per record, must be constant for all batches of data.
   */
  def start(
      numFeatures: Int): StreamingLinearRegressionWithSGD = {
    start(0.1, 100, 1.0, numFeatures)
  }

}
