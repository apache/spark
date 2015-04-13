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
import org.apache.spark.mllib.linalg.Vector

/**
 * :: Experimental ::
 * Train or predict a linear regression model on streaming data. Training uses
 * Stochastic Gradient Descent to update the model based on each new batch of
 * incoming data from a DStream (see `LinearRegressionWithSGD` for model equation)
 *
 * Each batch of data is assumed to be an RDD of LabeledPoints.
 * The number of data points per batch can vary, but the number
 * of features must be constant. An initial weight
 * vector must be provided.
 *
 * Use a builder pattern to construct a streaming linear regression
 * analysis in an application, like:
 *
 *  val model = new StreamingLinearRegressionWithSGD()
 *    .setStepSize(0.5)
 *    .setNumIterations(10)
 *    .setInitialWeights(Vectors.dense(...))
 *    .trainOn(DStream)
 *
 */
@Experimental
class StreamingLinearRegressionWithSGD private[mllib] (
    private var stepSize: Double,
    private var numIterations: Int,
    private var miniBatchFraction: Double)
  extends StreamingLinearAlgorithm[LinearRegressionModel, LinearRegressionWithSGD]
  with Serializable {

  /**
   * Construct a StreamingLinearRegression object with default parameters:
   * {stepSize: 0.1, numIterations: 50, miniBatchFraction: 1.0}.
   * Initial weights must be set before using trainOn or predictOn
   * (see `StreamingLinearAlgorithm`)
   */
  def this() = this(0.1, 50, 1.0)

  val algorithm = new LinearRegressionWithSGD(stepSize, numIterations, miniBatchFraction)

  protected var model: Option[LinearRegressionModel] = None

  /** Set the step size for gradient descent. Default: 0.1. */
  def setStepSize(stepSize: Double): this.type = {
    this.algorithm.optimizer.setStepSize(stepSize)
    this
  }

  /** Set the number of iterations of gradient descent to run per update. Default: 50. */
  def setNumIterations(numIterations: Int): this.type = {
    this.algorithm.optimizer.setNumIterations(numIterations)
    this
  }

  /** Set the fraction of each batch to use for updates. Default: 1.0. */
  def setMiniBatchFraction(miniBatchFraction: Double): this.type = {
    this.algorithm.optimizer.setMiniBatchFraction(miniBatchFraction)
    this
  }

  /** Set the initial weights. Default: [0.0, 0.0]. */
  def setInitialWeights(initialWeights: Vector): this.type = {
    this.model = Some(algorithm.createModel(initialWeights, 0.0))
    this
  }

}
