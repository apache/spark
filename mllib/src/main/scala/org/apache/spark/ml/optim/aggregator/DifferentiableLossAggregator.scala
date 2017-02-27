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
package org.apache.spark.ml.optim.aggregator

import org.apache.spark.ml.linalg._

private[ml] trait DifferentiableLossAggregator[
    Datum,
    Agg <: DifferentiableLossAggregator[Datum, Agg]] extends Serializable {

  self: Agg =>

  protected var weightSum: Double = 0.0
  protected var lossSum: Double = 0.0

  /** The dimension of the gradient array. */
  protected val dim: Int

  /** Array of gradient values that are mutated when new instances are added to the aggregator. */
  protected lazy val gradientSumArray: Array[Double] = Array.ofDim[Double](dim)

  /** Add a single data point to this aggregator. */
  def add(instance: Datum): Agg

  def merge(other: Agg): Agg = {
    require(dim == other.dim, s"Dimensions mismatch when merging with another " +
      s"LeastSquaresAggregator. Expecting $dim but got ${other.dim}.")

    if (other.weightSum != 0) {
      weightSum += other.weightSum
      lossSum += other.lossSum

      var i = 0
      val localThisGradientSumArray = this.gradientSumArray
      val localOtherGradientSumArray = other.gradientSumArray
      while (i < dim) {
        localThisGradientSumArray(i) += localOtherGradientSumArray(i)
        i += 1
      }
    }
    this
  }

  /** The current weighted averaged gradient. */
  def gradient: Vector = {
    require(weightSum > 0.0, s"The effective number of instances should be " +
      s"greater than 0.0, but was $weightSum.")
    val result = Vectors.dense(gradientSumArray.clone())
    BLAS.scal(1.0 / weightSum, result)
    result
  }

  /** Weighted count of instances in this aggregator. */
  def weight: Double = weightSum

  /** The current loss value of this aggregator. */
  def loss: Double = {
    require(weightSum > 0.0, s"The effective number of instances should be " +
      s"greater than 0.0, but $weightSum.")
    lossSum / weightSum
  }

}

