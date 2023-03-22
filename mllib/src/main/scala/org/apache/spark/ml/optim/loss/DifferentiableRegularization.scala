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
package org.apache.spark.ml.optim.loss

import breeze.optimize.DiffFunction

import org.apache.spark.ml.linalg._

/**
 * A Breeze diff function which represents a cost function for differentiable regularization
 * of parameters. e.g. L2 regularization: 1 / 2 regParam * beta dot beta
 *
 * @tparam T The type of the coefficients being regularized.
 */
private[ml] trait DifferentiableRegularization[T] extends DiffFunction[T] {

  /** Magnitude of the regularization penalty. */
  def regParam: Double

}

/**
 * A Breeze diff function for computing the L2 regularized loss and gradient of a vector of
 * coefficients.
 *
 * @param regParam The magnitude of the regularization.
 * @param shouldApply A function (Int => Boolean) indicating whether a given index should have
 *                    regularization applied to it. Usually we don't apply regularization to
 *                    the intercept.
 * @param applyFeaturesStd Option for a function which maps coefficient index (column major) to the
 *                         feature standard deviation. Since we always standardize the data during
 *                         training, if `standardization` is false, we have to reverse
 *                         standardization by penalizing each component differently by this param.
 *                         If `standardization` is true, this should be `None`.
 */
private[ml] class L2Regularization(
    override val regParam: Double,
    shouldApply: Int => Boolean,
    applyFeaturesStd: Option[Int => Double]) extends DifferentiableRegularization[Vector] {

  override def calculate(coefficients: Vector): (Double, Vector) = {
    coefficients match {
      case dv: DenseVector =>
        var sum = 0.0
        val gradient = new Array[Double](dv.size)
        dv.values.indices.filter(shouldApply).foreach { j =>
          val coef = coefficients(j)
          applyFeaturesStd match {
            case Some(getStd) =>
              // If `standardization` is false, we still standardize the data
              // to improve the rate of convergence; as a result, we have to
              // perform this reverse standardization by penalizing each component
              // differently to get effectively the same objective function when
              // the training dataset is not standardized.
              val std = getStd(j)
              if (std != 0.0) {
                val temp = coef / (std * std)
                sum += coef * temp
                gradient(j) = regParam * temp
              } else {
                0.0
              }
            case None =>
              // If `standardization` is true, compute L2 regularization normally.
              sum += coef * coef
              gradient(j) = coef * regParam
          }
        }
        (0.5 * sum * regParam, Vectors.dense(gradient))
      case _: SparseVector =>
        throw new IllegalArgumentException("Sparse coefficients are not currently supported.")
    }
  }
}
