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

import scala.collection.mutable.ArrayBuffer

import breeze.linalg.{DenseVector => BDV, norm}

import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

/**
 * :: DeveloperApi ::
 * This class optimizes a vector of weights via accelerated (proximal) gradient descent.
 * The implementation is based on TFOCS [[http://cvxr.com/tfocs]], described in Becker, Candes, and
 * Grant 2010.
 * @param gradient Delegate that computes the loss function value and gradient for a vector of
 *                 weights.
 * @param updater Delegate that updates weights in the direction of a gradient.
 */
@DeveloperApi
class AcceleratedGradientDescent (private var gradient: Gradient, private var updater: Updater)
  extends Optimizer {

  private var stepSize: Double = 1.0
  private var convergenceTol: Double = 1e-4
  private var numIterations: Int = 100
  private var regParam: Double = 0.0

  /**
   * Set the initial step size, used for the first step. Default 1.0.
   * On subsequent steps, the step size will be adjusted by the acceleration algorithm.
   */
  def setStepSize(step: Double): this.type = {
    this.stepSize = step
    this
  }

  /**
   * Set the optimization convergence tolerance. Default 1e-4.
   * Smaller values will increase accuracy but require additional iterations.
   */
  def setConvergenceTol(tol: Double): this.type = {
    this.convergenceTol = tol
    this
  }

  /**
   * Set the maximum number of iterations. Default 100.
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
   * Set a Gradient delegate for computing the loss function value and gradient.
   */
  def setGradient(gradient: Gradient): this.type = {
    this.gradient = gradient
    this
  }

  /**
   * Set an Updater delegate for updating weights in the direction of a gradient.
   * If regularization is used, the Updater will implement the regularization term's proximity
   * operator. Thus the type of regularization penalty is configured by providing a corresponding
   * Updater implementation.
   */
  def setUpdater(updater: Updater): this.type = {
    this.updater = updater
    this
  }

  /**
   * Run accelerated gradient descent on the provided training data.
   * @param data training data
   * @param initialWeights initial weights
   * @return solution vector
   */
  def optimize(data: RDD[(Double, Vector)], initialWeights: Vector): Vector = {
    val (weights, _) = AcceleratedGradientDescent.run(
      data,
      gradient,
      updater,
      stepSize,
      convergenceTol,
      numIterations,
      regParam,
      initialWeights)
    weights
  }
}

/**
 * :: DeveloperApi ::
 * Top-level method to run accelerated (proximal) gradient descent.
 */
@DeveloperApi
object AcceleratedGradientDescent extends Logging {
  /**
   * Run accelerated proximal gradient descent.
   * The implementation is based on TFOCS [[http://cvxr.com/tfocs]], described in Becker, Candes,
   * and Grant 2010. A limited but useful subset of the TFOCS feature set is implemented, including
   * support for composite loss functions, the Auslender and Teboulle acceleration method, and
   * automatic restart using the gradient test. A global Lipschitz bound is supported in preference
   * to local Lipschitz estimation via backtracking. On each iteration, the loss function and
   * gradient are caclculated from the full training dataset, requiring one Spark map reduce.
   *
   * @param data Input data. RDD containing data examples of the form (label, [feature values]).
   * @param gradient Delegate that computes the loss function value and gradient for a vector of
                     weights (for one single data example).
   * @param updater Delegate that updates weights in the direction of a gradient.
   * @param stepSize Initial step size for the first step.
   * @param convergenceTol Tolerance for convergence of the optimization algorithm. When the norm of
   *                       the change in weight vectors between successive iterations falls below
   *                       this relative tolerance, optimization is complete.
   * @param numIterations Maximum number of iterations to run the algorithm.
   * @param regParam The regularization parameter.
   * @param initialWeights The initial weight values.
   *
   * @return A tuple containing two elements. The first element is a Vector containing the optimized
   *         weight for each feature, and the second element is an array containing the approximate
   *         loss computed on each iteration.
   */
  def run(
      data: RDD[(Double, Vector)],
      gradient: Gradient,
      updater: Updater,
      stepSize: Double,
      convergenceTol: Double,
      numIterations: Int,
      regParam: Double,
      initialWeights: Vector): (Vector, Array[Double]) = {

    /** Returns the loss function and gradient for the provided weights 'x'. */
    def applySmooth(x: BDV[Double]): (Double, BDV[Double]) = {
      val bcX = data.context.broadcast(Vectors.fromBreeze(x))

      // Sum the loss function and gradient computed for each training example.
      val (loss, grad, count) = data.treeAggregate((0.0, BDV.zeros[Double](x.size), 0L))(
        seqOp = (c, v) => (c, v) match { case ((loss, grad, count), (label, features)) =>
          val l = gradient.compute(features, label, bcX.value, Vectors.fromBreeze(grad))
          (loss + l, grad, count + 1)
        },
        combOp = (c1, c2) => (c1, c2) match {
          case ((loss1, grad1, count1), (loss2, grad2, count2)) =>
            (loss1 + loss2, grad1 += grad2, count1 + count2)
        })

      // Divide the summed loss and gradient by the number of training examples.
      (loss / count, grad / (count: Double))
    }

    /**
     * Returns the regularization loss and updates weights according to the gradient and the
     * proximity operator.
     */
    def applyProjector(x: BDV[Double], g: BDV[Double], step: Double): (Double, BDV[Double]) = {
      val (weights, regularization) = updater.compute(Vectors.fromBreeze(x),
                                                      Vectors.fromBreeze(g),
                                                      step,
                                                      iter = 1, // Passing 1 avoids step size
                                                                // rescaling within the updater.
                                                      regParam)
      (regularization, BDV[Double](weights.toArray))
    }

    var x = BDV[Double](initialWeights.toArray)
    var z = x
    val L = 1.0 / stepSize // Infer a (global) Lipshitz bound from the provided stepSize.
    var theta = Double.PositiveInfinity
    var hasConverged = false
    val lossHistory = new ArrayBuffer[Double](numIterations)

    for (i <- 1 to numIterations if !hasConverged) {

      // Auslender and Teboulle's accelerated method.
      val (x_old, z_old) = (x, z)
      theta = 2.0 / (1.0 + math.sqrt(1.0 + 4.0 / (theta * theta)))
      val y = x_old * (1.0 - theta) + z_old * theta
      val (f_y, g_y) = applySmooth(y)
      val step = 1.0 / (theta * L)
      z = applyProjector(z_old, g_y, step)._2
      x = x_old * (1.0 - theta) + z * theta
      val d_x = x - x_old

      // Track loss history using the loss function at y, since f_y is already avaialble and
      // computing f_x would require another (distributed) call to applySmooth. Start by finding
      // c_y, the regularization component of the loss function at y.
      val (c_y, _) = applyProjector(y, g_y, 0.0)
      lossHistory.append(f_y + c_y)

      // Restart acceleration if indicated by the gradient test from O'Donoghue and Candes 2013.
      if (g_y.dot(d_x) > 0.0) {
        z = x
        theta = Double.PositiveInfinity
      }

      // Check convergence.
      hasConverged = norm(d_x) match {
        case 0.0 => i > 1
        case norm_dx => norm_dx < convergenceTol * math.max(norm(x), 1.0)
      }
    }

    logInfo("AcceleratedGradientDescent.run finished. Last 10 approximate losses %s".format(
      lossHistory.takeRight(10).mkString(", ")))

    (Vectors.fromBreeze(x), lossHistory.toArray)
  }
}
