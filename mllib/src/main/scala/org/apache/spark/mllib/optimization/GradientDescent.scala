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

package org.apache.spark.mllib.optimization

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD

import org.jblas.DoubleMatrix

import scala.collection.mutable.ArrayBuffer

/**
 * Class used to solve an optimization problem using Gradient Descent.
 * @param gradient Gradient function to be used.
 * @param updater Updater to be used to update weights after every iteration.
 */
class GradientDescent(var gradient: Gradient, var updater: Updater)
  extends Optimizer with Logging
{
  private var stepSize: Double = 1.0
  private var numIterations: Int = 100
  private var regParam: Double = 0.0
  private var miniBatchFraction: Double = 1.0

  /**
   * Set the initial step size of SGD for the first step. Default 1.0.
   * In subsequent steps, the step size will decrease with stepSize/sqrt(t)
   */
  def setStepSize(step: Double): this.type = {
    this.stepSize = step
    this
  }

  /**
   * Set fraction of data to be used for each SGD iteration.
   * Default 1.0 (corresponding to deterministic/classical gradient descent)
   */
  def setMiniBatchFraction(fraction: Double): this.type = {
    this.miniBatchFraction = fraction
    this
  }

  /**
   * Set the number of iterations for SGD. Default 100.
   */
  def setNumIterations(iters: Int): this.type = {
    this.numIterations = iters
    this
  }

  /**
   * Set the regularization parameter. Default 0.0.
   */
  def setRegParam(regParam: Double): this.type = {
    this.regParam = regParam
    this
  }

  /**
   * Set the gradient function (of the loss function of one single data example)
   * to be used for SGD.
   */
  def setGradient(gradient: Gradient): this.type = {
    this.gradient = gradient
    this
  }


  /**
   * Set the updater function to actually perform a gradient step in a given direction.
   * The updater is responsible to perform the update from the regularization term as well,
   * and therefore determines what kind or regularization is used, if any.
   */
  def setUpdater(updater: Updater): this.type = {
    this.updater = updater
    this
  }

  def optimize(data: RDD[(Double, Array[Double])], initialWeights: Array[Double])
    : Array[Double] = {

    val (weights, stochasticLossHistory) = GradientDescent.runMiniBatchSGD(
        data,
        gradient,
        updater,
        stepSize,
        numIterations,
        regParam,
        miniBatchFraction,
        initialWeights)
    weights
  }

}

// Top-level method to run gradient descent.
object GradientDescent extends Logging {
  /**
   * Run stochastic gradient descent (SGD) in parallel using mini batches.
   * In each iteration, we sample a subset (fraction miniBatchFraction) of the total data
   * in order to compute a gradient estimate.
   * Sampling, and averaging the subgradients over this subset is performed using one standard
   * spark map-reduce in each iteration.
   *
   * @param data - Input data for SGD. RDD of the set of data examples, each of
   *               the form (label, [feature values]).
   * @param gradient - Gradient object (used to compute the gradient of the loss function of
   *                   one single data example)
   * @param updater - Updater function to actually perform a gradient step in a given direction.
   * @param stepSize - initial step size for the first step
   * @param numIterations - number of iterations that SGD should be run.
   * @param regParam - regularization parameter
   * @param miniBatchFraction - fraction of the input data set that should be used for
   *                            one iteration of SGD. Default value 1.0.
   *
   * @return A tuple containing two elements. The first element is a column matrix containing
   *         weights for every feature, and the second element is an array containing the
   *         stochastic loss computed for every iteration.
   */
  def runMiniBatchSGD(
    data: RDD[(Double, Array[Double])],
    gradient: Gradient,
    updater: Updater,
    stepSize: Double,
    numIterations: Int,
    regParam: Double,
    miniBatchFraction: Double,
    initialWeights: Array[Double]) : (Array[Double], Array[Double]) = {

    val stochasticLossHistory = new ArrayBuffer[Double](numIterations)

    val nexamples: Long = data.count()
    val miniBatchSize = nexamples * miniBatchFraction

    // Initialize weights as a column vector
    var weights = new DoubleMatrix(initialWeights.length, 1, initialWeights:_*)
    var regVal = 0.0

    for (i <- 1 to numIterations) {
      // Sample a subset (fraction miniBatchFraction) of the total data
      // compute and sum up the subgradients on this subset (this is one map-reduce)
      val (gradientSum, lossSum) = data.sample(false, miniBatchFraction, 42 + i).map {
        case (y, features) =>
          val featuresCol = new DoubleMatrix(features.length, 1, features:_*)
          val (grad, loss) = gradient.compute(featuresCol, y, weights)
          (grad, loss)
      }.reduce((a, b) => (a._1.addi(b._1), a._2 + b._2))

      /**
       * NOTE(Xinghao): lossSum is computed using the weights from the previous iteration
       * and regVal is the regularization value computed in the previous iteration as well.
       */
      stochasticLossHistory.append(lossSum / miniBatchSize + regVal)
      val update = updater.compute(
        weights, gradientSum.div(miniBatchSize), stepSize, i, regParam)
      weights = update._1
      regVal = update._2
    }

    logInfo("GradientDescent.runMiniBatchSGD finished. Last 10 stochastic losses %s".format(
      stochasticLossHistory.takeRight(10).mkString(", ")))

    (weights.toArray, stochasticLossHistory.toArray)
  }
}
