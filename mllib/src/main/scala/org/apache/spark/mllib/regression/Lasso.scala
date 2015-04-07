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

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.optimization._
import org.apache.spark.mllib.regression.impl.GLMRegressionModel
import org.apache.spark.mllib.util.{Saveable, Loader}
import org.apache.spark.rdd.RDD

/**
 * Regression model trained using Lasso.
 *
 * @param weights Weights computed for every feature.
 * @param intercept Intercept computed for this model.
 */
class LassoModel (
    override val weights: Vector,
    override val intercept: Double)
  extends GeneralizedLinearModel(weights, intercept)
  with RegressionModel with Serializable with Saveable {

  override protected def predictPoint(
      dataMatrix: Vector,
      weightMatrix: Vector,
      intercept: Double): Double = {
    weightMatrix.toBreeze.dot(dataMatrix.toBreeze) + intercept
  }

  override def save(sc: SparkContext, path: String): Unit = {
    GLMRegressionModel.SaveLoadV1_0.save(sc, path, this.getClass.getName, weights, intercept)
  }

  override protected def formatVersion: String = "1.0"
}

object LassoModel extends Loader[LassoModel] {

  override def load(sc: SparkContext, path: String): LassoModel = {
    val (loadedClassName, version, metadata) = Loader.loadMetadata(sc, path)
    // Hard-code class name string in case it changes in the future
    val classNameV1_0 = "org.apache.spark.mllib.regression.LassoModel"
    (loadedClassName, version) match {
      case (className, "1.0") if className == classNameV1_0 =>
        val numFeatures = RegressionModel.getNumFeatures(metadata)
        val data = GLMRegressionModel.SaveLoadV1_0.loadData(sc, path, classNameV1_0, numFeatures)
        new LassoModel(data.weights, data.intercept)
      case _ => throw new Exception(
        s"LassoModel.load did not recognize model with (className, format version):" +
        s"($loadedClassName, $version).  Supported:\n" +
        s"  ($classNameV1_0, 1.0)")
    }
  }
}

/**
 * Train a regression model with L1-regularization using Stochastic Gradient Descent.
 * This solves the l1-regularized least squares regression formulation
 *          f(weights) = 1/2n ||A weights-y||^2^  + regParam ||weights||_1
 * Here the data matrix has n rows, and the input RDD holds the set of rows of A, each with
 * its corresponding right hand side label y.
 * See also the documentation for the precise formulation.
 */
class LassoWithSGD private (
    private var stepSize: Double,
    private var numIterations: Int,
    private var regParam: Double,
    private var miniBatchFraction: Double)
  extends GeneralizedLinearAlgorithm[LassoModel] with Serializable {

  private val gradient = new LeastSquaresGradient()
  private val updater = new L1Updater()
  override val optimizer = new GradientDescent(gradient, updater)
    .setStepSize(stepSize)
    .setNumIterations(numIterations)
    .setRegParam(regParam)
    .setMiniBatchFraction(miniBatchFraction)

  /**
   * Construct a Lasso object with default parameters: {stepSize: 1.0, numIterations: 100,
   * regParam: 0.01, miniBatchFraction: 1.0}.
   */
  def this() = this(1.0, 100, 0.01, 1.0)

  override protected def createModel(weights: Vector, intercept: Double) = {
    new LassoModel(weights, intercept)
  }
}

/**
 * Top-level methods for calling Lasso.
 */
object LassoWithSGD {

  /**
   * Train a Lasso model given an RDD of (label, features) pairs. We run a fixed number
   * of iterations of gradient descent using the specified step size. Each iteration uses
   * `miniBatchFraction` fraction of the data to calculate a stochastic gradient. The weights used
   * in gradient descent are initialized using the initial weights provided.
   *
   * @param input RDD of (label, array of features) pairs. Each pair describes a row of the data
   *              matrix A as well as the corresponding right hand side label y
   * @param numIterations Number of iterations of gradient descent to run.
   * @param stepSize Step size scaling to be used for the iterations of gradient descent.
   * @param regParam Regularization parameter.
   * @param miniBatchFraction Fraction of data to be used per iteration.
   * @param initialWeights Initial set of weights to be used. Array should be equal in size to
   *        the number of features in the data.
   */
  def train(
      input: RDD[LabeledPoint],
      numIterations: Int,
      stepSize: Double,
      regParam: Double,
      miniBatchFraction: Double,
      initialWeights: Vector): LassoModel = {
    new LassoWithSGD(stepSize, numIterations, regParam, miniBatchFraction)
      .run(input, initialWeights)
  }

  /**
   * Train a Lasso model given an RDD of (label, features) pairs. We run a fixed number
   * of iterations of gradient descent using the specified step size. Each iteration uses
   * `miniBatchFraction` fraction of the data to calculate a stochastic gradient.
   *
   * @param input RDD of (label, array of features) pairs. Each pair describes a row of the data
   *              matrix A as well as the corresponding right hand side label y
   * @param numIterations Number of iterations of gradient descent to run.
   * @param stepSize Step size to be used for each iteration of gradient descent.
   * @param regParam Regularization parameter.
   * @param miniBatchFraction Fraction of data to be used per iteration.
   */
  def train(
      input: RDD[LabeledPoint],
      numIterations: Int,
      stepSize: Double,
      regParam: Double,
      miniBatchFraction: Double): LassoModel = {
    new LassoWithSGD(stepSize, numIterations, regParam, miniBatchFraction).run(input)
  }

  /**
   * Train a Lasso model given an RDD of (label, features) pairs. We run a fixed number
   * of iterations of gradient descent using the specified step size. We use the entire data set to
   * update the true gradient in each iteration.
   *
   * @param input RDD of (label, array of features) pairs. Each pair describes a row of the data
   *              matrix A as well as the corresponding right hand side label y
   * @param stepSize Step size to be used for each iteration of Gradient Descent.
   * @param regParam Regularization parameter.
   * @param numIterations Number of iterations of gradient descent to run.
   * @return a LassoModel which has the weights and offset from training.
   */
  def train(
      input: RDD[LabeledPoint],
      numIterations: Int,
      stepSize: Double,
      regParam: Double): LassoModel = {
    train(input, numIterations, stepSize, regParam, 1.0)
  }

  /**
   * Train a Lasso model given an RDD of (label, features) pairs. We run a fixed number
   * of iterations of gradient descent using a step size of 1.0. We use the entire data set to
   * compute the true gradient in each iteration.
   *
   * @param input RDD of (label, array of features) pairs. Each pair describes a row of the data
   *              matrix A as well as the corresponding right hand side label y
   * @param numIterations Number of iterations of gradient descent to run.
   * @return a LassoModel which has the weights and offset from training.
   */
  def train(
      input: RDD[LabeledPoint],
      numIterations: Int): LassoModel = {
    train(input, numIterations, 1.0, 0.01, 1.0)
  }
}
