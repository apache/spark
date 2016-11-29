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

import scala.math._

import breeze.linalg.{axpy => brzAxpy, norm => brzNorm, Vector => BV}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mllib.linalg.{Vector, Vectors}

/**
 * :: DeveloperApi ::
 * Class used to perform steps (weight update) using Gradient Descent methods.
 *
 * For general minimization problems, or for regularized problems of the form
 *         min  L(w) + regParam * R(w),
 * the compute function performs the actual update step, when given some
 * (e.g. stochastic) gradient direction for the loss L(w),
 * and a desired step-size (learning rate).
 *
 * The updater is responsible to also perform the update coming from the
 * regularization term R(w) (if any regularization is used).
 */
@DeveloperApi
abstract class Updater extends Serializable {
  /**
   * Compute an updated value for weights given the gradient, stepSize, iteration number and
   * regularization parameter. Also returns the regularization value regParam * R(w)
   * computed using the *updated* weights.
   *
   * @param weightsOld - Column matrix of size dx1 where d is the number of features.
   * @param gradient - Column matrix of size dx1 where d is the number of features.
   * @param stepSize - step size across iterations
   * @param iter - Iteration number
   * @param regParam - Regularization parameter
   *
   * @return A tuple of 2 elements. The first element is a column matrix containing updated weights,
   *         and the second element is the regularization value computed using updated weights.
   */
  def compute(
      weightsOld: Vector,
      gradient: Vector,
      stepSize: Double,
      iter: Int,
      regParam: Double): (Vector, Double)
}

/**
 * :: DeveloperApi ::
 * A simple updater for gradient descent *without* any regularization.
 * Uses a step-size decreasing with the square root of the number of iterations.
 */
@DeveloperApi
class SimpleUpdater extends Updater {
  override def compute(
      weightsOld: Vector,
      gradient: Vector,
      stepSize: Double,
      iter: Int,
      regParam: Double): (Vector, Double) = {
    val thisIterStepSize = stepSize / math.sqrt(iter)
    val brzWeights: BV[Double] = weightsOld.asBreeze.toDenseVector
    brzAxpy(-thisIterStepSize, gradient.asBreeze, brzWeights)

    (Vectors.fromBreeze(brzWeights), 0)
  }
}

/**
 * :: DeveloperApi ::
 * Updater for L1 regularized problems.
 *          R(w) = ||w||_1
 * Uses a step-size decreasing with the square root of the number of iterations.
 *
 * Instead of subgradient of the regularizer, the proximal operator for the
 * L1 regularization is applied after the gradient step. This is known to
 * result in better sparsity of the intermediate solution.
 *
 * The corresponding proximal operator for the L1 norm is the soft-thresholding
 * function. That is, each weight component is shrunk towards 0 by shrinkageVal.
 *
 * If w is greater than shrinkageVal, set weight component to w-shrinkageVal.
 * If w is less than -shrinkageVal, set weight component to w+shrinkageVal.
 * If w is (-shrinkageVal, shrinkageVal), set weight component to 0.
 *
 * Equivalently, set weight component to signum(w) * max(0.0, abs(w) - shrinkageVal)
 */
@DeveloperApi
class L1Updater extends Updater {
  override def compute(
      weightsOld: Vector,
      gradient: Vector,
      stepSize: Double,
      iter: Int,
      regParam: Double): (Vector, Double) = {
    val thisIterStepSize = stepSize / math.sqrt(iter)
    // Take gradient step
    val brzWeights: BV[Double] = weightsOld.asBreeze.toDenseVector
    brzAxpy(-thisIterStepSize, gradient.asBreeze, brzWeights)
    // Apply proximal operator (soft thresholding)
    val shrinkageVal = regParam * thisIterStepSize
    var i = 0
    val len = brzWeights.length
    while (i < len) {
      val wi = brzWeights(i)
      brzWeights(i) = signum(wi) * max(0.0, abs(wi) - shrinkageVal)
      i += 1
    }

    (Vectors.fromBreeze(brzWeights), brzNorm(brzWeights, 1.0) * regParam)
  }
}

/**
 * :: DeveloperApi ::
 * Updater for L2 regularized problems.
 *          R(w) = 1/2 ||w||^2
 * Uses a step-size decreasing with the square root of the number of iterations.
 */
@DeveloperApi
class SquaredL2Updater extends Updater {
  override def compute(
      weightsOld: Vector,
      gradient: Vector,
      stepSize: Double,
      iter: Int,
      regParam: Double): (Vector, Double) = {
    // add up both updates from the gradient of the loss (= step) as well as
    // the gradient of the regularizer (= regParam * weightsOld)
    // w' = w - thisIterStepSize * (gradient + regParam * w)
    // w' = (1 - thisIterStepSize * regParam) * w - thisIterStepSize * gradient
    val thisIterStepSize = stepSize / math.sqrt(iter)
    val brzWeights: BV[Double] = weightsOld.asBreeze.toDenseVector
    brzWeights :*= (1.0 - thisIterStepSize * regParam)
    brzAxpy(-thisIterStepSize, gradient.asBreeze, brzWeights)
    val norm = brzNorm(brzWeights, 2.0)

    (Vectors.fromBreeze(brzWeights), 0.5 * regParam * norm * norm)
  }
}

