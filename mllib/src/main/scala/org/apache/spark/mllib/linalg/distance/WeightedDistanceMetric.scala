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

import breeze.linalg.{max, sum, DenseVector => DBV, Vector => BV}
import breeze.numerics.abs
import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.mllib.linalg.Vector

/**
 * this abstract class is used for a weighted distance metric
 *
 * @param weights weight vector
 */
@Experimental
abstract class WeightedDistanceMetric(weights: BV[Double]) extends DistanceMetric {
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
 * :: Experimental ::
 * A weighted Euclidean distance metric implementation
 * this metric is calculated by summing the square root of the squared differences
 * between each coordinate, optionally adding weights.
 */
@Experimental
@DeveloperApi
final private[mllib]
class WeightedEuclideanDistanceMetric private[mllib] (weights: BV[Double])
  extends WeightedDistanceMetric(weights) {

  override def apply(v1: BV[Double], v2: BV[Double]): Double = {
    val d = v1 - v2
    Math.sqrt(d dot (weights :* d))
  }
}

@Experimental
object WeightedEuclideanDistanceMetric {

  def apply(weights: Vector)(v1: Vector, v2: Vector): Double =
    new WeightedEuclideanDistanceMetric(weights.toBreeze).apply(v1.toBreeze, v2.toBreeze)
}


/**
 * :: Experimental ::
 * A weighted Chebyshev distance implementation
 */
@Experimental
@DeveloperApi
final private[mllib]
class WeightedChebyshevDistanceMetric private[mllib] (weights: BV[Double])
  extends WeightedDistanceMetric(weights) {

  /**
   * Calculates a weighted Chebyshev distance metric
   *
   * d(a, b) := max{w(i) * |a(i) - b(i)|} for all i
   * where w is a weighted vector
   *
   * @param v1 a vector defining a multidimensional point in some feature space
   * @param v2 a vector defining a multidimensional point in some feature space
   * @return Double a distance
   */
  override def apply(v1: BV[Double], v2: BV[Double]): Double = {
    val diff = (v1 - v2).map(abs(_)).:*(weights)
    max(diff)
  }
}

@Experimental
object WeightedChebyshevDistanceMetric {

  def apply(weights: Vector)(v1: Vector, v2: Vector): Double =
    new WeightedChebyshevDistanceMetric(weights.toBreeze).apply(v1.toBreeze, v2.toBreeze)
}

/**
 * :: Experimental ::
 * A weighted Manhattan distance metric implementation
 * this metric is calculated by summing the absolute values of the difference
 * between each coordinate, optionally with weights.
 */
@Experimental
@DeveloperApi
final private[mllib]
class WeightedManhattanDistanceMetric private[mllib] (weights: BV[Double])
  extends WeightedDistanceMetric(weights) {

  override def apply(v1: BV[Double], v2: BV[Double]): Double = {
    weights dot ((v1 - v2).map(abs(_)))
  }
}


@Experimental
object WeightedManhattanDistanceMetric {

  def apply(weights: Vector)(v1: Vector, v2: Vector): Double =
    new WeightedManhattanDistanceMetric(weights.toBreeze).apply(v1.toBreeze, v2.toBreeze)
}
