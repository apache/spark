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

package org.apache.spark.mllib.linalg.distance

import breeze.linalg.{sum, DenseVector => DBV, Vector => BV}
import breeze.numerics.abs
import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.mllib.linalg.Vector

/**
 * this abstract class is used for a weighted distance measure
 *
 * @param weights weight vector
 */
@Experimental
@DeveloperApi
abstract class WeightedDistanceMeasure(val weights: BV[Double]) extends DistanceMeasure {
  private val EPSILON = 1.0E-10

  /**
   * A weights is required to satisfy the following conditions:
   * 1. All element is greater than and equal to zero
   * 2. The summation of all element is required to be 1.0
   */
  require(weights.forall(_ >= 0))
  // if the difference is less than EPSILON, the condition is satisfied
  require(abs(1.0 - sum(weights)) < EPSILON)
}


/**
 * A weighted Cosined distance measure implementation
 *
 * (sum w[i]*u[i]*v[i]) / sqrt[(sum w[i]*u[i]^2)*(sum w[i]*v[i]^2)].
 *
 * @param weights weight BV[Double]
 */
@Experimental
@DeveloperApi
sealed private[mllib]
class WeightedCosineDistanceMeasure private[mllib] (weights: BV[Double])
    extends WeightedDistanceMeasure(weights) {

  override def apply(v1: BV[Double], v2: BV[Double]): Double = {
    val wbv = weights
    val bv1 = v1
    val bv2 = v2

    val dotProduct = sum(wbv :* (bv1 :* bv2))
    var denominator = Math.sqrt(sum(wbv :* bv1 :* bv1) * sum(wbv :* bv2 :* bv2))

    // correct for floating-point rounding errors
    if(denominator < dotProduct) {
      denominator = dotProduct
    }

    // correct for zero-vector corner case
    if(denominator == 0 && dotProduct == 0) {
      return 0.0
    }
    1.0 - (dotProduct / denominator)
  }
}

@Experimental
object WeightedCosineDistanceMeasure {

  def apply(weights: Vector)(v1: Vector, v2: Vector): Double =
    new WeightedCosineDistanceMeasure(weights.toBreeze).apply(v1.toBreeze, v2.toBreeze)
}
