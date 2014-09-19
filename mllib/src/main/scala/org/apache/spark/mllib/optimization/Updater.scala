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

import breeze.linalg.{norm => brzNorm}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.BLAS.{axpy, gemm, scal}

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
  def compute(
      weightsOld: Vector,
      gradient: Vector,
      stepSize: Double,
      iter: Int,
      regParam: Double): (Vector, Double) = {
    val thisIterStepSize = stepSize / math.sqrt(iter)

    axpy(-thisIterStepSize, gradient, weightsOld)

    (weightsOld, 0)
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
  def compute(
      weightsOld: Vector,
      gradient: Vector,
      stepSize: Double,
      iter: Int,
      regParam: Double): (Vector, Double) = {
    val thisIterStepSize = stepSize / math.sqrt(iter)
    // Take gradient step
    //println(s"\n$iter:")
    //println(s"\n$gradient\n")
    axpy(-thisIterStepSize, gradient, weightsOld)
    //println(s"\n$weightsOld\n")
    // Apply proximal operator (soft thresholding)
    val shrinkageVal = regParam * thisIterStepSize
    //println(s"\n$shrinkageVal\n")
    var i = 0
    val weightValues = weightsOld.toArray
    while (i < weightsOld.size) {
      val wi = weightValues(i)
      weightValues(i) = signum(wi) * max(0.0, abs(wi) - shrinkageVal)
      i += 1
    }
    //println(s"\n$weightsOld\n")
    (weightsOld, brzNorm(weightsOld.toBreeze, 1.0) * regParam)
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
  def compute(
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
    scal(1.0 - thisIterStepSize * regParam, weightsOld)
    axpy(-thisIterStepSize, gradient, weightsOld)
    val norm = brzNorm(weightsOld.toBreeze, 2.0)

    (weightsOld, 0.5 * regParam * norm * norm)
  }
}

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
abstract class MultiModelUpdater extends Serializable {
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
      weightsOld: DenseMatrix,
      gradient: DenseMatrix,
      stepSize: DenseMatrix,
      iter: Int,
      regParam: Matrix): (DenseMatrix, Matrix)
}

/**
 * :: DeveloperApi ::
 * A simple updater for gradient descent *without* any regularization.
 * Uses a step-size decreasing with the square root of the number of iterations.
 */
@DeveloperApi
class MultiModelSimpleUpdater extends MultiModelUpdater {
  def compute(
     weightsOld: DenseMatrix,
     gradient: DenseMatrix,
     stepSize: DenseMatrix,
     iter: Int,
     regParam: Matrix): (DenseMatrix, Matrix) = {
    val thisIterStepSize =
      SparseMatrix.diag(Vectors.dense(stepSize.map(-_ / sqrt(iter)).toArray))

    gemm(1.0, gradient,thisIterStepSize, 1.0, weightsOld)

    (weightsOld, Matrices.zeros(1, weightsOld.numCols))
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
class MultiModelL1Updater extends MultiModelUpdater {
  def compute(
                        weightsOld: DenseMatrix,
                        gradient: DenseMatrix,
                        stepSize: DenseMatrix,
                        iter: Int,
                        regParam: Matrix): (DenseMatrix, Matrix) = {
    val thisIterStepSize =
       SparseMatrix.diag(Vectors.dense(stepSize.map(-_ / sqrt(iter)).toArray))

    // Take gradient step
    gemm(1.0, gradient,thisIterStepSize, 1.0, weightsOld)
    // Apply proximal operator (soft thresholding)
    val shrinkageVal = regParam.elementWiseOperate(_ * _, thisIterStepSize)

    var j = 0
    while (j < weightsOld.numCols){
      var i = 0
      while (i < weightsOld.numRows) {
        val wi = weightsOld(i, j)
        weightsOld.update(i, j, signum(wi) * max(0.0, abs(wi) + shrinkageVal(j, j)))
        i += 1
      }
      j += 1
    }

    (weightsOld, weightsOld.colNorms(1.0) multiplyInPlace regParam)
  }
}
/**
 * :: DeveloperApi ::
 * Updater for L2 regularized problems.
 *          R(w) = 1/2 ||w||^2
 * Uses a step-size decreasing with the square root of the number of iterations.
 */
@DeveloperApi
class MultiModelSquaredL2Updater extends MultiModelUpdater {
  def compute(
                        weightsOld: DenseMatrix,
                        gradient: DenseMatrix,
                        stepSize: DenseMatrix,
                        iter: Int,
                        regParam: Matrix): (DenseMatrix, Matrix) = {
    // add up both updates from the gradient of the loss (= step) as well as
    // the gradient of the regularizer (= regParam * weightsOld)
    // w' = w - thisIterStepSize * (gradient + regParam * w)
    // w' = (1 - thisIterStepSize * regParam) * w - thisIterStepSize * gradient
    val thisIterStepSize =
      SparseMatrix.diag(Vectors.dense(stepSize.map(-_ / sqrt(iter)).toArray))

    weightsOld multiplyInPlace thisIterStepSize.elementWiseOperate(_ * _, regParam).
      elementWiseOperateInPlace(_ + _, Matrices.speye(thisIterStepSize.numRows))

    gemm(1.0, gradient,thisIterStepSize, 1.0, weightsOld)

    val norm = weightsOld.colNorms(2.0)

    (weightsOld, (norm.elementWiseOperate(_ * _, norm) multiplyInPlace regParam).
      elementWiseOperateScalarInPlace(_ * _, 0.5))
  }
}