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

import breeze.linalg.{norm => brzNorm, axpy => brzAxpy, Vector => BV}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mllib.linalg.{Vectors, Vector}

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
    val brzWeights: BV[Double] = weightsOld.toBreeze.toDenseVector
    brzAxpy(-thisIterStepSize, gradient.toBreeze, brzWeights)

    (Vectors.fromBreeze(brzWeights), 0)
  }
}

/**
 * :: DeveloperApi ::
 * Updater for L1 regularized problems.
 *          R(w) = ||w||_1
 * Uses a step-size decreasing with the square root of the number of iterations.

 * Instead of subgradient of the regularizer, the proximal operator for the
 * L1 regularization is applied after the gradient step. This is known to
 * result in better sparsity of the intermediate solution.
 *
 * The corresponding proximal operator for the L1 norm is the soft-thresholding
 * function. That is, each weight component is shrunk towards 0 by shrinkageVal.
 *
 * If w >  shrinkageVal, set weight component to w-shrinkageVal.
 * If w < -shrinkageVal, set weight component to w+shrinkageVal.
 * If -shrinkageVal < w < shrinkageVal, set weight component to 0.
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
    val brzWeights: BV[Double] = weightsOld.toBreeze.toDenseVector
    brzAxpy(-thisIterStepSize, gradient.toBreeze, brzWeights)
    // Apply proximal operator (soft thresholding)
    val shrinkageVal = regParam * thisIterStepSize
    for (i <- 0 until brzWeights.length) {
      val wi = brzWeights(i)
      brzWeights(i) = signum(wi) * max(0.0, abs(wi) - shrinkageVal)
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
    val brzWeights: BV[Double] = weightsOld.toBreeze.toDenseVector
    brzWeights :*= (1.0 - thisIterStepSize * regParam)
    brzAxpy(-thisIterStepSize, gradient.toBreeze, brzWeights)
    val norm = brzNorm(brzWeights, 2.0)

    (Vectors.fromBreeze(brzWeights), 0.5 * regParam * norm * norm)
  }
}

/**
 * :: DeveloperApi ::
 * Class used to perform steps (weight update) using Gradient Descent methods lazily.
 *
 * This class updates the weights based on given gradient and desired step-size. It
 * avoids updating the weights due to the regularization term. Instead, for L2
 * regularization, `weightShrinkage` is updated, similarly `weightTruncation` is
 * updated for L1 regularization. The main motivation for lazy update is to keep
 * the computation efficient for sparse gradient, otherwise the regularization would
 * update every weight element.
 */
@DeveloperApi
abstract class LazyUpdater extends Updater with Cloneable {
  /**
   * Compute the regularization penalty
   * @param weights - Column matrix of size dx1 where d is the number of features.
   * @param regParam - Regularization parameter
   * @return the regularization value computed using the given weights.
   */
  def computeRegularizationPenalty(weights: Vector, regParam: Double): Double = 0.0

  /**
   * Copy this object
   */
  override def clone(): LazyUpdater = null

  /**
   * Return the weight scaling coefficient accumulated during lazy L2 regularization.
   */
  def weightShrinkage = 1.0

  /**
   * Return the weight soft threshold coefficient accumulated during lazy L1 regularization.
   */
  def weightTruncation = 0.0

  /**
   * Apply weight scaling and thresholding to the given weight, and return the updated
   * weights. weightScale and weightThreshold are reset.
   * @param weights - Column matrix of size dx1 where d is the number of features.
   * @return - new weights after scaling and thresholding have been applied.
   */
  def applyLazyRegularization(weights:  Vector) = weights

}

/**
 * :: DeveloperApi ::
 * Lazy updater for L1 regularized problems.
 *          R(w) = ||w||_1
 *
 * The difference with L1Updater is that, this class does not soft threshold weights,
 * it accumulates the soft threshold for L1 regularization into softThreshold. Thresholding
 * is applied dynamically during predication for a data instance.
 *
 * There are two advantages: 1. This makes the updater more efficient for sparse data,
 * as it does not need to soft threshold every weight. 2. For features that has many small
 * updates, lazy update gives them chances to accumulate.
 */
@DeveloperApi
class LazyL1Updater extends LazyUpdater {
  var softThreshold = 0.0

  override def compute(
                        weightsOld: Vector,
                        gradient: Vector,
                        stepSize: Double,
                        iter: Int,
                        regParam: Double): (Vector, Double) = {
    val thisIterStepSize = stepSize / math.sqrt(iter)
    // Take gradient step
    val brzWeights: BV[Double] = weightsOld.toBreeze.toDenseVector
    brzAxpy(-thisIterStepSize, gradient.toBreeze, brzWeights)
    // Accumulate truncation
    softThreshold += regParam * thisIterStepSize

    // return 0 for the reg penalty
    (Vectors.fromBreeze(brzWeights), 0.0)
  }

  override def computeRegularizationPenalty(weights: Vector, regParam: Double): Double = {
    val brzWeights: BV[Double] = weights.toBreeze.toDenseVector
    val norm = brzNorm(brzWeights, 1.0)
    regParam * norm
  }

  override def clone() = {
    val n = new LazyL1Updater()
    n.softThreshold = softThreshold
    n
  }

  override def weightTruncation = softThreshold

  override def applyLazyRegularization(weights: Vector) = {
    val brzWeights = weights.toBreeze.toDenseVector
    // apply shrinkage
    for (i <- 0 until brzWeights.length) {
      val wi = brzWeights(i)
      brzWeights(i) = signum(wi) * max(0.0, abs(wi) - softThreshold)
    }
    softThreshold = 0.0
    Vectors.fromBreeze(brzWeights)
  }
}

object LazySquaredL2Updater {
  // avoid underflow
  val MinimumScale = 1e-14
}

/**
 * :: DeveloperApi ::
 * Lazy updater for L2 regularized problems.
 *          R(w) = 1/2 ||w||^2
 *
 * The difference with L2Updater is that, this class does not scale weights,
 * it accumulates the scaling for L2 regularization into scale. Scaling
 * is applied dynamically during predication for a data instance. The results
 * should be the same between lazy L2 and regular L2. The main advantage is
 * lazy L2 update is more efficient for sparse data, as it does not need to
 * scale every weight.
 */
@DeveloperApi
class LazySquaredL2Updater extends LazyUpdater {
  var scale: Double = 1.0

  override def compute(
      weightsOld: Vector,
      gradient: Vector,
      stepSize: Double,
      iter: Int,
      regParam: Double): (Vector, Double) = {
    // scale *= (1 - thisIterStepSize * regParam)
    // w' = w - thisIterStepSize * gradient / scale
    val thisIterStepSize = stepSize / math.sqrt(iter)
    val discount = 1.0 - thisIterStepSize * regParam
    val decayedWeights = weightDecay(discount, weightsOld)
    val brzWeights: BV[Double] = decayedWeights.toBreeze.toDenseVector

    brzAxpy(-thisIterStepSize / scale, gradient.toBreeze, brzWeights)

    (Vectors.fromBreeze(brzWeights), 0.0)
  }

  override def computeRegularizationPenalty(weights: Vector, regParam: Double): Double = {
    val brzWeights: BV[Double] = weights.toBreeze.toDenseVector
    val norm = brzNorm(brzWeights, 2.0)
    0.5 * regParam * norm * norm
  }

  override def clone() = {
    val n = new LazySquaredL2Updater()
    n.scale = scale
    n
  }

  override def weightShrinkage = scale

  override def applyLazyRegularization(weights: Vector) = {
    val brzWeights = weights.toBreeze.toDenseVector
    brzWeights :*= scale
    scale = 1.0
    Vectors.fromBreeze(brzWeights)
  }

  /** *
    * Accumulate L2 regularization shrinkage. In case scale is already too small,
    * update the weights and reset scale to 1
    */
  private def weightDecay(discount: Double, weights: Vector) = {
    if (discount <= 0.0) {
      weights
    } else {
      val decayed = if (scale < LazySquaredL2Updater.MinimumScale) {
        applyLazyRegularization(weights)
      } else {
        weights
      }
      scale *= discount
      decayed
    }
  }
}
