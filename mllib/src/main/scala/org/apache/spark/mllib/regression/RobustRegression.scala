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

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.optimization._
import org.apache.spark.annotation.Experimental

/**
 * Regression model trained using Huber M-Estimation RobustRegression.
 *
 * @param weights Weights computed for every feature.
 * @param intercept Intercept computed for this model.
 */
class HuberRobustRegressionModel (
    override val weights: Vector,
    override val intercept: Double)
  extends GeneralizedLinearModel(weights, intercept) with RegressionModel with Serializable {

  override protected def predictPoint(
      dataMatrix: Vector,
      weightMatrix: Vector,
      intercept: Double): Double = {
    weightMatrix.toBreeze.dot(dataMatrix.toBreeze) + intercept
  }
}

/**
 * Train a Huber Robust regression model with no regularization using Stochastic Gradient Descent.
 * This solves the Huber objective function
 *              f(weights) = 1/2 ||A weights-y||^2       if |A weights-y| <= k
 *              f(weights) = k |A weights-y| - 1/2 K^2   if |A weights-y| > k
 * where k = 1.345 which produce 95% efficiency when the errors are normal and
 * substantial resistance to outliers otherwise.
 * Here the data matrix has n rows, and the input RDD holds the set of rows of A, each with
 * its corresponding right hand side label y.
 * See also the documentation for the precise formulation.
 */
class HuberRobustRegressionWithSGD private[mllib] (
    private var stepSize: Double,
    private var numIterations: Int,
    private var miniBatchFraction: Double)
  extends GeneralizedLinearAlgorithm[HuberRobustRegressionModel] with Serializable {

  private val gradient = new HuberRobustGradient()
  private val updater = new SimpleUpdater()
  override val optimizer = new GradientDescent(gradient, updater)
    .setStepSize(stepSize)
    .setNumIterations(numIterations)
    .setMiniBatchFraction(miniBatchFraction)

  /**
   * Construct a Huber M-Estimation RobustRegression object with default parameters: {stepSize: 1.0,
   * numIterations: 100, miniBatchFraction: 1.0}.
   */
  def this() = this(1.0, 100, 1.0)

  override protected def createModel(weights: Vector, intercept: Double) = {
    new HuberRobustRegressionModel(weights, intercept)
  }
}

/**
 * Top-level methods for calling HuberRobustRegression.
 */
object HuberRobustRegressionWithSGD {

  /**
   * Train a HuberRobust Regression model given an RDD of (label, features) pairs. We run a fixed
   * number of iterations of gradient descent using the specified step size. Each iteration uses
   * `miniBatchFraction` fraction of the data to calculate a stochastic gradient. The weights used
   * in gradient descent are initialized using the initial weights provided.
   *
   * @param input RDD of (label, array of features) pairs. Each pair describes a row of the data
   *              matrix A as well as the corresponding right hand side label y
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
      initialWeights: Vector): HuberRobustRegressionModel = {
    new HuberRobustRegressionWithSGD(stepSize, numIterations, miniBatchFraction)
      .run(input, initialWeights)
  }

  /**
   * Train a HuberRobustRegression model given an RDD of (label, features) pairs. We run a fixed
   * number of iterations of gradient descent using the specified step size. Each iteration uses
   * `miniBatchFraction` fraction of the data to calculate a stochastic gradient.
   *
   * @param input RDD of (label, array of features) pairs. Each pair describes a row of the data
   *              matrix A as well as the corresponding right hand side label y
   * @param numIterations Number of iterations of gradient descent to run.
   * @param stepSize Step size to be used for each iteration of gradient descent.
   * @param miniBatchFraction Fraction of data to be used per iteration.
   */
  def train(
      input: RDD[LabeledPoint],
      numIterations: Int,
      stepSize: Double,
      miniBatchFraction: Double): HuberRobustRegressionModel = {
    new HuberRobustRegressionWithSGD(stepSize, numIterations, miniBatchFraction).run(input)
  }

  /**
   * Train a HuberRobustRegression model given an RDD of (label, features) pairs. We run a fixed
   * number of iterations of gradient descent using the specified step size. We use the entire
   * data set to compute the true gradient in each iteration.
   *
   * @param input RDD of (label, array of features) pairs. Each pair describes a row of the data
   *              matrix A as well as the corresponding right hand side label y
   * @param stepSize Step size to be used for each iteration of Gradient Descent.
   * @param numIterations Number of iterations of gradient descent to run.
   * @return a HuberRobustRegressionModel which has the weights and offset from training.
   */
  def train(
      input: RDD[LabeledPoint],
      numIterations: Int,
      stepSize: Double): HuberRobustRegressionModel = {
    train(input, numIterations, stepSize, 1.0)
  }

  /**
   * Train a HuberRobustRegression model given an RDD of (label, features) pairs. We run a fixed
   * number of iterations of gradient descent using a step size of 1.0. We use the entire data
   * set to compute the true gradient in each iteration.
   *
   * @param input RDD of (label, array of features) pairs. Each pair describes a row of the data
   *              matrix A as well as the corresponding right hand side label y
   * @param numIterations Number of iterations of gradient descent to run.
   * @return a HuberRobustRegressionModel which has the weights and offset from training.
   */
  def train(
      input: RDD[LabeledPoint],
      numIterations: Int): HuberRobustRegressionModel = {
    train(input, numIterations, 1.0, 1.0)
  }
}

/**
 * Regression model trained using Tukey bisquare (Biweight) M-Estimation RobustRegression.
 *
 * @param weights Weights computed for every feature.
 * @param intercept Intercept computed for this model.
 */
class BiweightRobustRegressionModel (
                                   override val weights: Vector,
                                   override val intercept: Double)
  extends GeneralizedLinearModel(weights, intercept) with RegressionModel with Serializable {

  override protected def predictPoint(
                                       dataMatrix: Vector,
                                       weightMatrix: Vector,
                                       intercept: Double): Double = {
    weightMatrix.toBreeze.dot(dataMatrix.toBreeze) + intercept
  }
}

/**
 * Train a Biweight Robust regression model with no regularization using SGD.
 * This solves the Biweight objective function
 *              L = k^2 / 6 * (1 - (1 - ||A weights-y||^2 / k^2)^3)     if |A weights-y| <= k
 *              L = k^2 / 6                                             if |A weights-y| > k
 * where k = 4.685 which produce 95% efficiency when the errors are normal and
 * substantial resistance to outliers otherwise.
 * Here the data matrix has n rows, and the input RDD holds the set of rows of A, each with
 * its corresponding right hand side label y.
 * See also the documentation for the precise formulation.
 */
class BiweightRobustRegressionWithSGD private[mllib] (
                                                    private var stepSize: Double,
                                                    private var numIterations: Int,
                                                    private var miniBatchFraction: Double)
  extends GeneralizedLinearAlgorithm[BiweightRobustRegressionModel] with Serializable {

  private val gradient = new BiweightRobustGradient()
  private val updater = new SimpleUpdater()
  override val optimizer = new GradientDescent(gradient, updater)
    .setStepSize(stepSize)
    .setNumIterations(numIterations)
    .setMiniBatchFraction(miniBatchFraction)

  /**
   * Construct a Biweight M-Estimation RobustRegression object with default parameters:
   * {stepSize: 1.0, numIterations: 5000, miniBatchFraction: 1.0}.
   */
  def this() = this(1.0, 5000, 1.0)

  override protected def createModel(weights: Vector, intercept: Double) = {
    new BiweightRobustRegressionModel(weights, intercept)
  }
}

/**
 * Top-level methods for calling BiweightRobustRegression.
 */
object BiweightRobustRegressionWithSGD {

  /**
   * Train a BiweightRobust Regression model given an RDD of (label, features) pairs. We run a
   * fixed number of iterations of gradient descent using the specified step size. Each iteration
   * uses `miniBatchFraction` fraction of the data to calculate a stochastic gradient. The weights
   * used in gradient descent are initialized using the initial weights provided.
   *
   * @param input RDD of (label, array of features) pairs. Each pair describes a row of the data
   *              matrix A as well as the corresponding right hand side label y
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
             initialWeights: Vector): BiweightRobustRegressionModel = {
    new BiweightRobustRegressionWithSGD(stepSize, numIterations, miniBatchFraction)
      .run(input, initialWeights)
  }

  /**
   * Train a BiweightRobustRegression model given an RDD of (label,features) pairs. We run a fixed
   * number of iterations of gradient descent using the specified step size. Each iteration uses
   * `miniBatchFraction` fraction of the data to calculate a stochastic gradient.
   *
   * @param input RDD of (label, array of features) pairs. Each pair describes a row of the data
   *              matrix A as well as the corresponding right hand side label y
   * @param numIterations Number of iterations of gradient descent to run.
   * @param stepSize Step size to be used for each iteration of gradient descent.
   * @param miniBatchFraction Fraction of data to be used per iteration.
   */
  def train(
             input: RDD[LabeledPoint],
             numIterations: Int,
             stepSize: Double,
             miniBatchFraction: Double): BiweightRobustRegressionModel = {
    new BiweightRobustRegressionWithSGD(stepSize, numIterations, miniBatchFraction).run(input)
  }

  /**
   * Train a BiweightRobustRegression model given an RDD of (label,features) pairs. We run a fixed
   * number of iterations of gradient descent using the specified step size. We use the entire
   * data set to compute the true gradient in each iteration.
   *
   * @param input RDD of (label, array of features) pairs. Each pair describes a row of the data
   *              matrix A as well as the corresponding right hand side label y
   * @param stepSize Step size to be used for each iteration of Gradient Descent.
   * @param numIterations Number of iterations of gradient descent to run.
   * @return a BiweightRobustRegressionModel which has the weights and offset from training.
   */
  def train(
             input: RDD[LabeledPoint],
             numIterations: Int,
             stepSize: Double): BiweightRobustRegressionModel = {
    train(input, numIterations, stepSize, 1.0)
  }

  /**
   * Train a BiweightRobustRegression model given an RDD of (label,features) pairs. We run a fixed
   * number of iterations of gradient descent using a step size of 1.0. We use the entire data
   * set to compute the true gradient in each iteration.
   *
   * @param input RDD of (label, array of features) pairs. Each pair describes a row of the data
   *              matrix A as well as the corresponding right hand side label y
   * @param numIterations Number of iterations of gradient descent to run.
   * @return a BiweightRobustRegressionModel which has the weights and offset from training.
   */
  def train(
             input: RDD[LabeledPoint],
             numIterations: Int): BiweightRobustRegressionModel = {
    train(input, numIterations, 1.0, 1.0)
  }
}
