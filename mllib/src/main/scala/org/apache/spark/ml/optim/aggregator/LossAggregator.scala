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
import org.apache.spark.broadcast.Broadcast


private[ml] trait LossAggregator[Datum, Agg <: LossAggregator[Datum, Agg]] extends Serializable {

  protected var weightSum: Double = 0.0

  protected var lossSum: Double = 0.0

  def add(instance: Datum): this.type

  def merge(other: Agg): this.type = {
    if (other.weightSum != 0) {
      weightSum += other.weightSum
      lossSum += other.lossSum
    }
    this
  }

  def loss: Double = {
    require(weightSum > 0.0, s"The effective number of instances should be " +
      s"greater than 0.0, but $weightSum.")
    lossSum / weightSum
  }
}

private[ml] trait DifferentiableLossAggregator[Datum, Agg <: DifferentiableLossAggregator[Datum, Agg]]
  extends LossAggregator[Datum, Agg] {

  protected val dim: Int

  protected lazy val gradientSumArray: Array[Double] = Array.ofDim[Double](dim)

  override def merge(other: Agg): this.type = {
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

  def gradient: Vector = {
    require(weightSum > 0.0, s"The effective number of instances should be " +
      s"greater than 0.0, but was $weightSum.")
    val result = Vectors.dense(gradientSumArray.clone())
    BLAS.scal(1.0 / weightSum, result)
    result
  }

  def create(coeff: Broadcast[Vector]): Agg
}

