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

  def getReg: Int => Double = (x: Int) => regParam

}

/**
 * A Breeze diff function for computing the L2 regularized loss and gradient of an array of
 * coefficients.
 *
 * @param regParam The magnitude of the regularization.
 * @param shouldApply A function (Int => Boolean) indicating whether a given index should have
 *                    regularization applied to it.
 * @param featuresStd Option indicating whether the regularization should be scaled by the standard
 *                    deviation of the features.
 */
private[ml] class L2Regularization(
    val regParam: Double,
    shouldApply: Int => Boolean,
    featuresStd: Option[Int => Double]) extends DifferentiableRegularization[Array[Double]] {

  override def calculate(coefficients: Array[Double]): (Double, Array[Double]) = {
    var sum = 0.0
    val gradient = new Array[Double](coefficients.length)
    coefficients.indices.filter(shouldApply).foreach { j =>
      val coef = coefficients(j)
      featuresStd match {
        case Some(getStd) =>
          val std = getStd(j)
          if (std != 0.0) {
            val temp = coef / (std * std)
            sum += coef * temp
            gradient(j) = regParam * temp
          } else {
            0.0
          }
        case None =>
          sum += coef * coef
          gradient(j) = coef * regParam
      }
    }
    (0.5 * sum * regParam, gradient)
  }
}

//class StandardizedRegularization(val getStd: Int => Double)
//  extends DifferentiableRegularization[Vector] {
//  val regParam = 0.0
//
//  override def calculate(coefficients: Vector): (Double, Vector) = {
//
//
//  }
//}
//
//class Regularization(override val getReg: Int => Double)
//  extends DifferentiableRegularization[Vector] {
//  val regParam = 0.0
//
//  override def calculate(coefficients: Vector): (Double, Vector) = {
//    var sum = 0.0
//    // TODO: handle sparse?
//    val gradient = new Array[Double](coefficients.size)
//    coefficients.foreachActive { (i, v) =>
//      val reg = getReg(i)
//      sum += 0.5 * v * v * reg
//      gradient(i) = v * reg
//    }
//    (sum, Vectors.dense(gradient))
//  }
//}
