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

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.optimization._
import org.apache.spark.mllib.util.MLUtils

import org.jblas.DoubleMatrix

/**
 * Regression model trained using Lasso.
 *
 * @param weights Weights computed for every feature.
 * @param intercept Intercept computed for this model.
 */
class LassoModel(
    override val weights: Array[Double],
    override val intercept: Double)
  extends GeneralizedLinearModel(weights, intercept)
  with RegressionModel with Serializable {

  override def predictPoint(dataMatrix: DoubleMatrix, weightMatrix: DoubleMatrix,
      intercept: Double) = {
    dataMatrix.dot(weightMatrix) + intercept
  }
}

/**
 * Train a regression model with L1-regularization using Stochastic Gradient Descent.
 * This solves the l1-regularized least squares regression formulation
 *          f(weights) = 1/n ||A weights-y||^2  + regParam ||weights||_1
 * Here the data matrix has n rows, and the input RDD holds the set of rows of A, each with
 * its corresponding right hand side label y.
 * See also the documentation for the precise formulation.
 */
class LassoWithSGD private (
    var stepSize: Double,
    var numIterations: Int,
    var regParam: Double,
    var miniBatchFraction: Double)
  extends GeneralizedLinearAlgorithm[LassoModel]
  with Serializable {

  val gradient = new LeastSquaresGradient()
  val updater = new L1Updater()
  @transient val optimizer = new GradientDescent(gradient, updater).setStepSize(stepSize)
    .setNumIterations(numIterations)
    .setRegParam(regParam)
    .setMiniBatchFraction(miniBatchFraction)

  // We don't want to penalize the intercept, so set this to false.
  setIntercept(false)

  var yMean = 0.0
  var xColMean: DoubleMatrix = _
  var xColSd: DoubleMatrix = _

  /**
   * Construct a Lasso object with default parameters
   */
  def this() = this(1.0, 100, 1.0, 1.0)

  def createModel(weights: Array[Double], intercept: Double) = {
    val weightsMat = new DoubleMatrix(weights.length + 1, 1, (Array(intercept) ++ weights):_*)
    val weightsScaled = weightsMat.div(xColSd)
    val interceptScaled = yMean - (weightsMat.transpose().mmul(xColMean.div(xColSd)).get(0))

    new LassoModel(weightsScaled.data, interceptScaled)
  }

  override def run(
      input: RDD[LabeledPoint],
      initialWeights: Array[Double])
    : LassoModel =
  {
    val nfeatures: Int = input.first.features.length
    val nexamples: Long = input.count()

    // To avoid penalizing the intercept, we center and scale the data.
    val stats = MLUtils.computeStats(input, nfeatures, nexamples)
    yMean = stats._1
    xColMean = stats._2
    xColSd = stats._3

    val normalizedData = input.map { point =>
      val yNormalized = point.label - yMean
      val featuresMat = new DoubleMatrix(nfeatures, 1, point.features:_*)
      val featuresNormalized = featuresMat.sub(xColMean).divi(xColSd)
      LabeledPoint(yNormalized, featuresNormalized.toArray)
    }

    super.run(normalizedData, initialWeights)
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
      initialWeights: Array[Double])
    : LassoModel =
  {
    new LassoWithSGD(stepSize, numIterations, regParam, miniBatchFraction).run(input,
        initialWeights)
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
      miniBatchFraction: Double)
    : LassoModel =
  {
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
      regParam: Double)
    : LassoModel =
  {
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
      numIterations: Int)
    : LassoModel =
  {
    train(input, numIterations, 1.0, 1.0, 1.0)
  }

  def main(args: Array[String]) {
    if (args.length != 5) {
      println("Usage: Lasso <master> <input_dir> <step_size> <regularization_parameter> <niters>")
      System.exit(1)
    }
    val sc = new SparkContext(args(0), "Lasso")
    val data = MLUtils.loadLabeledData(sc, args(1))
    val model = LassoWithSGD.train(data, args(4).toInt, args(2).toDouble, args(3).toDouble)
    println("Weights: " + model.weights.mkString("[", ", ", "]"))
    println("Intercept: " + model.intercept)

    sc.stop()
  }
}
