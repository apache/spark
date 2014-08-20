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

import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg.Vector

/**
 * :: Experimental ::
 * Tanimoto distance implementation
 *
 * @see http://en.wikipedia.org/wiki/Jaccard_index
 */
@Experimental
class TanimotoDistanceMeasure extends DistanceMetric {

  /**
   * Calculates the tanimoto distance between 2 points
   *
   * The coefficient (a measure of similarity) is: Td(a, b) = a.b / (|a|^2 + |b|^2 - a.b)
   * The distance d(a,b) = 1 - T(a,b)
   *
   * @param v1 a Vector defining a multidimensional point in some feature space
   * @param v2 a Vector defining a multidimensional point in some feature space
   * @return 0 for perfect match, > 0 for greater distance
   */
  override def apply(v1: Vector, v2: Vector): Double = {
    validate(v1, v2)

    val calcSquaredSum = (vector: Vector) => vector.toBreeze.map(x => x * x).reduce(_ + _).apply(0)
    val dotProduct = v1.toBreeze.dot(v2.toBreeze)
    var denominator = (calcSquaredSum(v1) + calcSquaredSum(v2) - dotProduct)

    // correct for floating-point round-off: distance >= 0
    if(denominator < dotProduct) {
      denominator = dotProduct
    }

    // denominator == 0 only when dot(a,a) == dot(b,b) == dot(a,b) == 0
    val distance = if(denominator > 0) {
      1 - dotProduct / denominator
    }
    else {
      0.0
    }
    distance
  }
}
