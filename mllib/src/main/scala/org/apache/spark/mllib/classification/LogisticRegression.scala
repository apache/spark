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

package org.apache.spark.mllib.classification

import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.optimization._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.util.DataValidators
import org.apache.spark.rdd.RDD

/**
 * Classification model trained using Logistic Regression.
 *
 * @param weights Weights computed for every feature.
 * @param intercept Intercept computed for this model.
 */
class LogisticRegressionModel (
    override val weights: Vector,
    override val intercept: Double)
  extends GeneralizedLinearModel(weights, intercept) with ClassificationModel with Serializable {

  private var threshold: Option[Double] = Some(0.5)

  /**
   * :: Experimental ::
   * Sets the threshold that separates positive predictions from negative predictions. An example
   * with prediction score greater than or equal to this threshold is identified as an positive,
   * and negative otherwise. The default value is 0.5.
   */
  @Experimental
  def setThreshold(threshold: Double): this.type = {
    this.threshold = Some(threshold)
    this
  }

  /**
   * :: Experimental ::
   * Clears the threshold so that `predict` will output raw prediction scores.
   */
  @Experimental
  def clearThreshold(): this.type = {
    threshold = None
    this
  }

  override protected def predictPoint(dataMatrix: Vector, weightMatrix: Vector,
      intercept: Double) = {
    val margin = weightMatrix.toBreeze.dot(dataMatrix.toBreeze) + intercept
    val score = 1.0 / (1.0 + math.exp(-margin))
    threshold match {
      case Some(t) => if (score < t) 0.0 else 1.0
      case None => score
    }
  }
}

/**
 * Train a classification model for Logistic Regression using Stochastic Gradient Descent.
 * NOTE: Labels used in Logistic Regression should be {0, 1}
 *
 * Using [[LogisticRegressionWithLBFGS]] is recommended over this.
 */
class LogisticRegressionWithSGD private (
    private var stepSize: Double,
    private var numIterations: Int,
    private var regParam: Double,
    private var miniBatchFraction: Double)
  extends GeneralizedLinearAlgorithm[LogisticRegressionModel] with Serializable {

  private val gradient = new LogisticGradient()
  private val updater = new SquaredL2Updater()
  override val optimizer = new GradientDescent(gradient, updater)
    .setStepSize(stepSize)
    .setNumIterations(numIterations)
    .setRegParam(regParam)
    .setMiniBatchFraction(miniBatchFraction)
  override protected val validators = List(DataValidators.binaryLabelValidator)

  /**
   * Construct a LogisticRegression object with default parameters
   */
  def this() = this(1.0, 100, 0.0, 1.0)

  override protected def createModel(weights: Vector, intercept: Double) = {
    new LogisticRegressionModel(weights, intercept)
  }
}

/**
 * Top-level methods for calling Logistic Regression using Stochastic Gradient Descent.
 * NOTE: Labels used in Logistic Regression should be {0, 1}
 */
object LogisticRegressionWithSGD {
  // NOTE(shivaram): We use multiple train methods instead of default arguments to support
  // Java programs.

  /**
   * Train a logistic regression model given an RDD of (label, features) pairs. We run a fixed
   * number of iterations of gradient descent using the specified step size. Each iteration uses
   * `miniBatchFraction` fraction of the data to calculate the gradient. The weights used in
   * gradient descent are initialized using the initial weights provided.
   * NOTE: Labels used in Logistic Regression should be {0, 1}
   *
   * @param input RDD of (label, array of features) pairs.
   * @param numIterations Number of iterations of gradient descent to run.
   * @param stepSize Step size to be used for each iteration of gradient descent.
   * @param miniBatchFraction Fraction of data to be used per iteration.
   * @param initialWeights Initial set of weights to be used. Array should be equal in size to
   *        the number of features in the data.
   */
  def train(
      input: RDD[LabeledPoint],
      numIterations: Int,
      stepSize: Double,
      miniBatchFraction: Double,
      initialWeights: Vector): LogisticRegressionModel = {
    new LogisticRegressionWithSGD(stepSize, numIterations, 0.0, miniBatchFraction)
      .run(input, initialWeights)
  }

  /**
   * Train a logistic regression model given an RDD of (label, features) pairs. We run a fixed
   * number of iterations of gradient descent using the specified step size. Each iteration uses
   * `miniBatchFraction` fraction of the data to calculate the gradient.
   * NOTE: Labels used in Logistic Regression should be {0, 1}
   *
   * @param input RDD of (label, array of features) pairs.
   * @param numIterations Number of iterations of gradient descent to run.
   * @param stepSize Step size to be used for each iteration of gradient descent.

   * @param miniBatchFraction Fraction of data to be used per iteration.
   */
  def train(
      input: RDD[LabeledPoint],
      numIterations: Int,
      stepSize: Double,
      miniBatchFraction: Double): LogisticRegressionModel = {
    new LogisticRegressionWithSGD(stepSize, numIterations, 0.0, miniBatchFraction)
      .run(input)
  }

  /**
   * Train a logistic regression model given an RDD of (label, features) pairs. We run a fixed
   * number of iterations of gradient descent using the specified step size. We use the entire data
   * set to update the gradient in each iteration.
   * NOTE: Labels used in Logistic Regression should be {0, 1}
   *
   * @param input RDD of (label, array of features) pairs.
   * @param stepSize Step size to be used for each iteration of Gradient Descent.

   * @param numIterations Number of iterations of gradient descent to run.
   * @return a LogisticRegressionModel which has the weights and offset from training.
   */
  def train(
      input: RDD[LabeledPoint],
      numIterations: Int,
      stepSize: Double): LogisticRegressionModel = {
    train(input, numIterations, stepSize, 1.0)
  }

  /**
   * Train a logistic regression model given an RDD of (label, features) pairs. We run a fixed
   * number of iterations of gradient descent using a step size of 1.0. We use the entire data set
   * to update the gradient in each iteration.
   * NOTE: Labels used in Logistic Regression should be {0, 1}
   *
   * @param input RDD of (label, array of features) pairs.
   * @param numIterations Number of iterations of gradient descent to run.
   * @return a LogisticRegressionModel which has the weights and offset from training.
   */
  def train(
      input: RDD[LabeledPoint],
      numIterations: Int): LogisticRegressionModel = {
    train(input, numIterations, 1.0, 1.0)
  }
}

/**
 * Train a classification model for Logistic Regression using Limited-memory BFGS.
 * Standard feature scaling and L2 regularization are used by default.
 * NOTE: Labels used in Logistic Regression should be {0, 1}
 */
class LogisticRegressionWithLBFGS
  extends GeneralizedLinearAlgorithm[LogisticRegressionModel] with Serializable {

  this.setFeatureScaling(true)

  override val optimizer = new LBFGS(new LogisticGradient, new SquaredL2Updater)

  override protected val validators = List(DataValidators.binaryLabelValidator)

  override protected def createModel(weights: Vector, intercept: Double) = {
    new LogisticRegressionModel(weights, intercept)
  }
}
