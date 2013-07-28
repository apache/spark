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

package spark.mllib.classification

import scala.math.signum
import spark.{Logging, RDD, SparkContext}
import spark.mllib.optimization._
import spark.mllib.util.MLUtils

import org.jblas.DoubleMatrix

/**
 * SVM using Stochastic Gradient Descent.
 */
class SVMModel(
  val weights: Array[Double],
  val intercept: Double,
  val stochasticLosses: Array[Double]) extends ClassificationModel {

  // Create a column vector that can be used for predictions
  private val weightsMatrix = new DoubleMatrix(weights.length, 1, weights:_*)

  override def predict(testData: spark.RDD[Array[Double]]) = {
    // A small optimization to avoid serializing the entire model. Only the weightsMatrix
    // and intercept is needed.
    val localWeights = weightsMatrix
    val localIntercept = intercept
    testData.map { x => 
      signum(new DoubleMatrix(1, x.length, x:_*).dot(localWeights) + localIntercept)
    }
  }

  override def predict(testData: Array[Double]): Double = {
    val dataMat = new DoubleMatrix(1, testData.length, testData:_*)
    signum(dataMat.dot(weightsMatrix) + this.intercept)
  }
}



class SVMLocalRandomSGD private (var stepSize: Double, var regParam: Double, var miniBatchFraction: Double,
    var numIters: Int)
  extends Logging {

  /**
   * Construct a SVM object with default parameters
   */
  def this() = this(1.0, 1.0, 1.0, 100)

  /**
   * Set the step size per-iteration of SGD. Default 1.0.
   */
  def setStepSize(step: Double) = {
    this.stepSize = step
    this
  }

  /**
   * Set the regularization parameter. Default 1.0.
   */
  def setRegParam(param: Double) = {
    this.regParam = param
    this
  }

  /**
   * Set fraction of data to be used for each SGD iteration. Default 1.0.
   */
  def setMiniBatchFraction(fraction: Double) = {
    this.miniBatchFraction = fraction
    this
  }

  /**
   * Set the number of iterations for SGD. Default 100.
   */
  def setNumIterations(iters: Int) = {
    this.numIters = iters
    this
  }

  def train(input: RDD[(Double, Array[Double])]): SVMModel = {
    val nfeatures: Int = input.take(1)(0)._2.length
    val initialWeights = Array.fill(nfeatures)(1.0)
    train(input, initialWeights)
  }

  def train(
    input: RDD[(Double, Array[Double])],
    initialWeights: Array[Double]): SVMModel = {

    // Add a extra variable consisting of all 1.0's for the intercept.
    val data = input.map { case (y, features) =>
      (y, Array(1.0, features:_*))
    }

    val initalWeightsWithIntercept = Array(1.0, initialWeights:_*)

    val (weights, stochasticLosses) = GradientDescent.runMiniBatchSGD(
      data,
      new HingeGradient(),
      new SquaredL2Updater(),
      stepSize,
      numIters,
      regParam,
      initalWeightsWithIntercept,
      miniBatchFraction)

    val intercept = weights(0)
    val weightsScaled = weights.tail

    val model = new SVMModel(weightsScaled, intercept, stochasticLosses)

    logInfo("Final model weights " + model.weights.mkString(","))
    logInfo("Final model intercept " + model.intercept)
    logInfo("Last 10 stochasticLosses " + model.stochasticLosses.takeRight(10).mkString(", "))
    model
  }
}

/**
 * Top-level methods for calling SVM.


 */
object SVMLocalRandomSGD {

  /**
   * Train a SVM model given an RDD of (label, features) pairs. We run a fixed number
   * of iterations of gradient descent using the specified step size. Each iteration uses
   * `miniBatchFraction` fraction of the data to calculate the gradient. The weights used in
   * gradient descent are initialized using the initial weights provided.
   *
   * @param input RDD of (label, array of features) pairs.
   * @param numIterations Number of iterations of gradient descent to run.
   * @param stepSize Step size to be used for each iteration of gradient descent.
   * @param regParam Regularization parameter.
   * @param miniBatchFraction Fraction of data to be used per iteration.
   * @param initialWeights Initial set of weights to be used. Array should be equal in size to 
   *        the number of features in the data.
   */
  def train(
      input: RDD[(Double, Array[Double])],
      numIterations: Int,
      stepSize: Double,
      regParam: Double,
      miniBatchFraction: Double,
      initialWeights: Array[Double])
    : SVMModel =
  {
    new SVMLocalRandomSGD(stepSize, regParam, miniBatchFraction, numIterations).train(input, initialWeights)
  }

  /**
   * Train a SVM model given an RDD of (label, features) pairs. We run a fixed number
   * of iterations of gradient descent using the specified step size. Each iteration uses
   * `miniBatchFraction` fraction of the data to calculate the gradient.
   *
   * @param input RDD of (label, array of features) pairs.
   * @param numIterations Number of iterations of gradient descent to run.
   * @param stepSize Step size to be used for each iteration of gradient descent.
   * @param regParam Regularization parameter.
   * @param miniBatchFraction Fraction of data to be used per iteration.
   */
  def train(
      input: RDD[(Double, Array[Double])],
      numIterations: Int,
      stepSize: Double,
      regParam: Double,
      miniBatchFraction: Double)
    : SVMModel =
  {
    new SVMLocalRandomSGD(stepSize, regParam, miniBatchFraction, numIterations).train(input)
  }

  /**
   * Train a SVM model given an RDD of (label, features) pairs. We run a fixed number
   * of iterations of gradient descent using the specified step size. We use the entire data set to update
   * the gradient in each iteration.
   *
   * @param input RDD of (label, array of features) pairs.
   * @param stepSize Step size to be used for each iteration of Gradient Descent.
   * @param regParam Regularization parameter.
   * @param numIterations Number of iterations of gradient descent to run.
   * @return a SVMModel which has the weights and offset from training.
   */
  def train(
      input: RDD[(Double, Array[Double])],
      numIterations: Int,
      stepSize: Double,
      regParam: Double)
    : SVMModel =
  {
    train(input, numIterations, stepSize, regParam, 1.0)
  }

  /**
   * Train a SVM model given an RDD of (label, features) pairs. We run a fixed number
   * of iterations of gradient descent using a step size of 1.0. We use the entire data set to update
   * the gradient in each iteration.
   *
   * @param input RDD of (label, array of features) pairs.
   * @param numIterations Number of iterations of gradient descent to run.
   * @return a SVMModel which has the weights and offset from training.
   */
  def train(
      input: RDD[(Double, Array[Double])],
      numIterations: Int)
    : SVMModel =
  {
    train(input, numIterations, 1.0, 1.0, 1.0)
  }

  def main(args: Array[String]) {
    if (args.length != 5) {
      println("Usage: SVM <master> <input_dir> <step_size> <regularization_parameter> <niters>")
      System.exit(1)
    }
    val sc = new SparkContext(args(0), "SVM")
    val data = MLUtils.loadLabeledData(sc, args(1))
    val model = SVMLocalRandomSGD.train(data, args(4).toInt, args(2).toDouble, args(3).toDouble)

    sc.stop()
  }
}
