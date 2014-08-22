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

import breeze.linalg.{max, DenseVector => DBV, Vector => BV}
import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg.Vector

/**
 * :: Experimental ::
 * Chebyshev distance implementation
 *
 * @see http://en.wikipedia.org/wiki/Chebyshev_distance
 */
@Experimental
class ChebyshevDistanceMetric extends DistanceMetric {

  /**
   * Calculates a Chebyshev distance metric
   *
   * d(a, b) := max{|a(i) - b(i)|} for all i
   *
   * @param v1 a Vector defining a multidimensional point in some feature space
   * @param v2 a Vector defining a multidimensional point in some feature space
   * @return Double a distance
   */
  override def apply(v1: BV[Double], v2: BV[Double]): Double = {
    val diff = (v1 - v2).map(Math.abs)
    max(diff)
  }
}

/**
 * :: Experimental ::
 * A weighted Chebyshev distance implementation
 */
@Experimental
class WeightedChebyshevDistanceMetric(val weights: BV[Double]) extends DistanceMetric {

  def this(v: Vector) = this(v.toBreeze)

  /**
   * Calculates a weighted Chebyshev distance metric
   *
   * d(a, b) := max{w(i) * |a(i) - b(i)|} for all i
   * where w is a weighted vector
   *
   * @param v1 a Vector defining a multidimensional point in some feature space
   * @param v2 a Vector defining a multidimensional point in some feature space
   * @return Double a distance
   */
  override def apply(v1: BV[Double], v2: BV[Double]): Double = {
    val diff = (v1 - v2).map(Math.abs).:*(weights)
    max(diff)
  }
}
