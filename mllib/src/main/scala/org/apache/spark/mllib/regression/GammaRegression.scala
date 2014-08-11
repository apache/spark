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

/**
 * Regression model trained using GammaRegression.
 *
 * @param weights Weights computed for every feature.
 * @param intercept Intercept computed for this model.
 */
class GammaRegressionModel private[mllib] (
    override val weights: Vector,
    override val intercept: Double)
  extends GeneralizedLinearModel(weights, intercept) with RegressionModel with Serializable {

  override protected def predictPoint(
      dataMatrix: Vector,
      weightMatrix: Vector,
      intercept: Double): Double = {
    math.exp(weightMatrix.toBreeze.dot(dataMatrix.toBreeze) + intercept)
  }
}

/**
 * Train a Gamma regression model with log link using Stochastic Gradient Descent.
 *
 * Here the data matrix has n rows, and the input RDD holds the set of rows of X, each with
 * its corresponding right hand side label y.
 */
class GammaRegressionWithSGD private (
    private var stepSize: Double,
    private var numIterations: Int,
    private var miniBatchFraction: Double)
  extends GeneralizedLinearAlgorithm[GammaRegressionModel] with Serializable {

  private val gradient = new GammaLogGradient()
  private val updater = new SquaredL2Updater()
  override val optimizer = new GradientDescent(gradient, updater)
    .setStepSize(stepSize)
    .setNumIterations(numIterations)
    .setMiniBatchFraction(miniBatchFraction)

  /**
   * Construct a GammaRegression object with default parameters: {stepSize: 1.0,
   * numIterations: 100, miniBatchFraction: 1.0}.
   */
  def this() = this(1.0, 100, 1.0)

  override protected def createModel(weights: Vector, intercept: Double) = {
    new GammaRegressionModel(weights, intercept)
  }
}

/**
 * Top-level methods for calling GammaRegression.
 */
object GammaRegressionWithSGD {

  /**
   * Train a Gamma Regression model given an RDD of (label, features) pairs. We run a fixed number
   * of iterations of gradient descent using the specified step size. Each iteration uses
   * `miniBatchFraction` fraction of the data to calculate a stochastic gradient. The weights used
   * in gradient descent are initialized using the initial weights provided.
   *
   * @param input RDD of (label, array of features) pairs. Each pair describes a row of the data
   *              matrix X as well as the corresponding right hand side label y
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
      initialWeights: Vector): GammaRegressionModel = {
    new GammaRegressionWithSGD(stepSize, numIterations, miniBatchFraction)
      .run(input, initialWeights)
  }

  /**
   * Train a GammaRegression model given an RDD of (label, features) pairs. We run a fixed number
   * of iterations of gradient descent using the specified step size. Each iteration uses
   * `miniBatchFraction` fraction of the data to calculate a stochastic gradient.
   *
   * @param input RDD of (label, array of features) pairs. Each pair describes a row of the data
   *              matrix X as well as the corresponding right hand side label y
   * @param numIterations Number of iterations of gradient descent to run.
   * @param stepSize Step size to be used for each iteration of gradient descent.
   * @param miniBatchFraction Fraction of data to be used per iteration.
   */
  def train(
      input: RDD[LabeledPoint],
      numIterations: Int,
      stepSize: Double,
      miniBatchFraction: Double): GammaRegressionModel = {
    new GammaRegressionWithSGD(stepSize, numIterations, miniBatchFraction).run(input)
  }

  /**
   * Train a GammaRegression model given an RDD of (label, features) pairs. We run a fixed number
   * of iterations of gradient descent using the specified step size. We use the entire data set to
   * compute the true gradient in each iteration.
   *
   * @param input RDD of (label, array of features) pairs. Each pair describes a row of the data
   *              matrix X as well as the corresponding right hand side label y
   * @param stepSize Step size to be used for each iteration of Gradient Descent.
   * @param numIterations Number of iterations of gradient descent to run.
   * @return a GammaRegressionModel which has the weights and offset from training.
   */
  def train(
      input: RDD[LabeledPoint],
      numIterations: Int,
      stepSize: Double): GammaRegressionModel = {
    train(input, numIterations, stepSize, 1.0)
  }

  /**
   * Train a GammaRegression model given an RDD of (label, features) pairs. We run a fixed number
   * of iterations of gradient descent using a step size of 1.0. We use the entire data set to
   * compute the true gradient in each iteration.
   *
   * @param input RDD of (label, array of features) pairs. Each pair describes a row of the data
   *              matrix X as well as the corresponding right hand side label y
   * @param numIterations Number of iterations of gradient descent to run.
   * @return a GammaRegressionModel which has the weights and offset from training.
   */
  def train(
      input: RDD[LabeledPoint],
      numIterations: Int): GammaRegressionModel = {
    train(input, numIterations, 1.0, 1.0)
  }
}

/**
 * Train a Gamma regression model with log link using L-BFGS.
 *
 * Here the data matrix has n rows, and the input RDD holds the set of rows of X, each with
 * its corresponding right hand side label y.
 */
class GammaRegressionWithLBFGS private (
    private var numCorrections: Int,
    private var numIters: Int,
    private var convergenceTol: Double,
    private var regParam: Double)
  extends GeneralizedLinearAlgorithm[GammaRegressionModel] with Serializable {

  private val gradient = new GammaLogGradient()
  private val updater = new SquaredL2Updater()
  override val optimizer = new LBFGS(gradient, updater)
    .setNumCorrections(numCorrections)
    .setConvergenceTol(convergenceTol)
    .setMaxNumIterations(numIters)
    .setRegParam(regParam)

  def this() = this(10, 100, 1e-4, 0.0)

  override protected def createModel(weights: Vector, intercept: Double) = {
    new GammaRegressionModel(weights, intercept)
  }
}

object GammaRegressionWithLBFGS {
  
  /**
   * Train a GammaRegression model given an RDD of (label, features) pairs. We run a L-BFGS to
   * estimate the weights. The weights are initialized using the initial weights provided.
   *
   * @param input RDD of (label, array of features) pairs. Each pair describes a row of the data
   *              matrix A as well as the corresponding right hand side label y
   * @param numIterations The maximum number of iterations carried out by L-BFGS.
   * @param numCorrections Specific parameter for LBFGS.
   * @param convergTol The convergence tolerance of iterations for L-BFGS.
   * @param regParam The regularization parameter for L-BFGS.
   * @param initialWeights Initial set of weights to be used. Array should be equal in size to
   *        the number of features in the data.
   */
  def train(
      input: RDD[LabeledPoint],
      numIterations: Int,
      numCorrections: Int,
      convergTol: Double,
      regParam: Double,
      initialWeights: Vector): GammaRegressionModel = {
    new GammaRegressionWithLBFGS(
        numCorrections,
        numIterations,
        convergTol,
        regParam)
      .setIntercept(true)
      .run(input, initialWeights)
  }
  
   /**
   * Train a GammaRegression model given an RDD of (label, features) pairs. We run a L-BFGS to
   * estimate the weights.
   *
   * @param input RDD of (label, array of features) pairs. Each pair describes a row of the data
   *              matrix A as well as the corresponding right hand side label y
   * @param numIterations The maximum number of iterations carried out by L-BFGS.
   * @param numCorrections Specific parameter for LBFGS.
   * @param convergTol The convergence tolerance of iterations for L-BFGS.
   * @param regParam The regularization parameter for L-BFGS.
   */
  def train(
      input: RDD[LabeledPoint],
      numIterations: Int,
      numCorrections: Int,
      convergTol: Double,
      regParam: Double): GammaRegressionModel = {
    new GammaRegressionWithLBFGS(
        numCorrections,
        numIterations,
        convergTol,
        regParam)
      .setIntercept(true)
      .run(input)
  }
  
  /**
   * Train a GammaRegression model given an RDD of (label, features) pairs. We run a L-BFGS to
   * estimate the weights.
   *
   * @param input RDD of (label, array of features) pairs. Each pair describes a row of the data
   *              matrix A as well as the corresponding right hand side label y
   * @param numIterations The maximum number of iterations carried out by L-BFGS.
   * @param numCorrections Specific parameter for LBFGS.
   * @param convergTol The convergence tolerance of iterations for L-BFGS.
   */
  def train(
      input: RDD[LabeledPoint],
      numIterations: Int,
      numCorrections: Int,
      convergTol: Double): GammaRegressionModel = {
    train(input, numIterations, numCorrections, convergTol, 0.0)
  }
  
  /**
   * Train a GammaRegression model given an RDD of (label, features) pairs. We run a L-BFGS to
   * estimate the weights.
   *
   * @param input RDD of (label, array of features) pairs. Each pair describes a row of the data
   *              matrix A as well as the corresponding right hand side label y
   * @param numIterations The maximum number of iterations carried out by L-BFGS.
   * @param numCorrections Specific parameter for LBFGS.
   */
  def train(
      input: RDD[LabeledPoint],
      numIterations: Int,
      numCorrections: Int): GammaRegressionModel = {
    train(input, numIterations, numCorrections, 1e-4, 0.0)
  }
  
  /**
   * Train a GammaRegression model given an RDD of (label, features) pairs. We run a L-BFGS to
   * estimate the weights.
   *
   * @param input RDD of (label, array of features) pairs. Each pair describes a row of the data
   *              matrix A as well as the corresponding right hand side label y
   * @param numIterations The maximum number of iterations carried out by L-BFGS.
   */
  def train(
      input: RDD[LabeledPoint],
      numIterations: Int): GammaRegressionModel = {
    train(input, numIterations, 10, 1e-4, 0.0)
  }
}

