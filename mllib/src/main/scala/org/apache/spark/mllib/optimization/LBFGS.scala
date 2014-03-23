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
import scala.collection.mutable.ArrayBuffer
import org.jblas.DoubleMatrix
import scala.util.control.Breaks._
import scala.Array

/**
 * Class used to solve an optimization problem using Limited-memory BFGS.
 * @param gradient Gradient function to be used.
 * @param updater Updater to be used to update weights after every iteration.
 */
class LBFGS(var gradient: Gradient, var updater: Updater)
  extends Optimizer with Logging
{
  private var numCorrections: Int = 10
  private var lineSearchTolerance: Double = 0.9
  private var convTolerance: Double = 1E-4
  private var maxNumIterations: Int = 100
  private var regParam: Double = 0.0
  private var miniBatchFraction: Double = 1.0

  /**
   * Set the number of corrections used in the LBFGS update. Default 10.
   * Values of m less than 3 are not recommended; large values of m
   * will result in excessive computing time. 3 < m < 7 is recommended.
   * Restriction: m > 0
   */
  def setNumCorrections(corrections: Int): this.type = {
    this.numCorrections = corrections
    this
  }

  /**
   * Set the tolerance to control the accuracy of the line search in mcsrch step. Default 0.9.
   * If the function and gradient evaluations are inexpensive with respect to the cost of
   * the iteration (which is sometimes the case when solving very large problems) it may
   * be advantageous to set to a small value. A typical small value is 0.1.
   * Restriction: should be greater than 1e-4.
   */
  def setLineSearchTolerance(tolerance: Double): this.type = {
    this.lineSearchTolerance = tolerance
    this
  }

  /**
   * Set fraction of data to be used for each LBFGS iteration. Default 1.0.
   */
  def setMiniBatchFraction(fraction: Double): this.type = {
    this.miniBatchFraction = fraction
    this
  }

  /**
   * Set the convergence tolerance of iterations for LBFGS. Default 1E-4.
   * Smaller value will lead to higher accuracy with the cost of more iterations.
   */
  def setConvTolerance(tolerance: Int): this.type = {
    this.convTolerance = tolerance
    this
  }

  /**
   * Set the maximal number of iterations for LBFGS. Default 100.
   */
  def setMaxNumIterations(iters: Int): this.type = {
    this.maxNumIterations = iters
    this
  }

  /**
   * Set the regularization parameter used for LBFGS. Default 0.0.
   */
  def setRegParam(regParam: Double): this.type = {
    this.regParam = regParam
    this
  }

  /**
   * Set the gradient function to be used for LBFGS.
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

    val (weights, lossHistory) = LBFGS.runMiniBatchLBFGS(
        data,
        gradient,
        updater,
        numCorrections,
        lineSearchTolerance,
        convTolerance,
        maxNumIterations,
        regParam,
        miniBatchFraction,
        initialWeights)
    weights
  }
}

// Top-level method to run LBFGS.
object LBFGS extends Logging {
  /**
   * Run gradient descent in parallel using mini batches.
   *
   * @param data - Input data for LBFGS. RDD of form (label, [feature values]).
   * @param gradient - Gradient object that will be used to compute the gradient.
   * @param updater - Updater function to actually perform a gradient step in a given direction.
   * @param numCorrections - The number of corrections used in the LBFGS update.
   * @param lineSearchTolerance - The tolerance to control the accuracy of the line search.
   * @param convTolerance - The convergence tolerance of iterations for LBFGS
   * @param maxNumIterations - Maximal number of iterations that LBFGS should be run.
   * @param regParam - Regularization parameter
   * @param miniBatchFraction - Fraction of the input data set that should be used for
   *                          one iteration of LBFGS. Default value 1.0.
   *
   * @return A tuple containing two elements. The first element is a column matrix containing
   *         weights for every feature, and the second element is an array containing the loss
   *         computed for every iteration.
   */
  def runMiniBatchLBFGS(
    data: RDD[(Double, Array[Double])],
    gradient: Gradient,
    updater: Updater,
    numCorrections: Int,
    lineSearchTolerance: Double,
    convTolerance: Double,
    maxNumIterations: Int,
    regParam: Double,
    miniBatchFraction: Double,
    initialWeights: Array[Double]): (Array[Double], Array[Double]) = {

    val lossHistory = new ArrayBuffer[Double](maxNumIterations)

    val nexamples: Long = data.count()
    val miniBatchSize = nexamples * miniBatchFraction

    /**
     * Clone the initialWeights to weightsArray to avoid changing it.
     * Since LBFGS Java implementation takes primitive double array, and gradient.compute only
     * takes column vector, we create weights column vector which points the data pointer
     * to weightsArray. Because they are actually in the same memory, we don't need to copy over
     * for each iteration.
     */
    val weightsArray = initialWeights.clone()
    // Initialize weights as a column vector
    val weights = new DoubleMatrix(initialWeights.length, 1, weightsArray:_*)


    var i = 0
    var diff = 1.0
    var isConverged = false

    // Initialize the java LBFGS implementation configuration and object.
    val iprint = Array.ofDim[Int](2)
    iprint(0) = -1
    iprint(1) = 3

    val iflag = Array.ofDim[Int](1)
    iflag(0) = 0

    val diag = Array.ofDim[Double](initialWeights.length)
    val lbfgs = new org.riso.numerical.LBFGS

    while (i < maxNumIterations && !isConverged) {
      val (gradientSum, lossSum) = data.sample(false, miniBatchFraction, 42 + i).map {
        case (y, features) =>
          val featuresCol = new DoubleMatrix(features.length, 1, features: _*)
          val (grad, loss) = gradient.compute(featuresCol, y, weights)
          (grad, loss)
      }.reduce((a, b) => (a._1.addi(b._1), a._2 + b._2))

      /**
       * It will return the regVal given the weights and regParam using updater. We use
       * the same way to initialize the regVal in GradientDescent for first iteration.
       * Since updater is designed for SGD with adaptive training rate in mind, it seems to be
       * a little bit hacky; however, with the right input parameters in updater,
       * we still can get the correct regVal. Need to re-factorize it later.
       */
      val regVal = updater.compute(
        weights,
        new DoubleMatrix(initialWeights.length, 1), 0, 1, regParam)._2

      val loss = lossSum / miniBatchSize + regVal

      /**
       * Again, this is very hacky, but it returns the correct gradient of regularization part
       * using updater. Let's review how updater works when returning newWeights given
       * the input parameters.
       *
       * w' = w - thisIterStepSize * (gradient + regGradient(w))
       * Note that regGradient is function of w!
       *
       * If we set gradient = 0, thisIterStepSize = 1, then
       *
       * regGradient(w) = w - w'
       */
      val regGradient = weights.sub(
        updater.compute(weights, new DoubleMatrix(initialWeights.length, 1), 1, 1, regParam)._1)

      val gradientTotal = gradientSum.div(miniBatchSize).add(regGradient).toArray

      lossHistory.append(loss)

      lbfgs.lbfgs(
        gradientTotal.length, numCorrections, weightsArray, loss,
        gradientTotal, false, diag, iprint, 0.0, 10e-16, iflag)

      if (iflag(0) < 1) {
        print("Something i s wrong...lol")
        break
      }

      diff = lossHistory match {
        case x if x.length > 1 => Math.abs({
          val losses = x.takeRight(2)
          (losses(0) - losses(1)) / (losses(0) + 1e-6)})
        case _ => 1.0}

      if (diff < convTolerance) isConverged = true

      /**
       * NOTE: lossSum / loss is computed using the weights from the previous iteration
       * and regVal is the regularization value computed in the previous iteration as well.
       * diff are the loss difference between the previous iteration and the iteration
       * before the previous iteration. As a result, we print out (i - 1) as the current iteration.
       */
      println("Iteration " + (i - 1) + ": loss " + lossHistory.last + ", diff " + diff)
      i += 1
    }

    println("LBFGS finished with %s iterations.".format(i.toString))

    (weights.toArray, lossHistory.toArray)
  }
}
