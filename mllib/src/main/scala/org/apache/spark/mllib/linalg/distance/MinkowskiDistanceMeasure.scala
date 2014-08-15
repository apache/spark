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
 * Minkowski distance Implementation
 * The Minkowski distance is a metric on Euclidean space
 * which can be considered as a generalization of both
 * the Euclidean distance and the Manhattan distance.
 *
 * @see http://en.wikipedia.org/wiki/Minkowski_distance
 */
@Experimental
class MinkowskiDistanceMeasure(val exponent: Double) extends DistanceMeasure{

  // the default value for exponent
  def this() = this(3.0)

  /**
   * Calculates the distance metric between 2 points
   *
   * @param v1 a Vector defining a multidimensional point in some feature space
   * @param v2 a Vector defining a multidimensional point in some feature space
   * @return a scalar doubles of the distance
   */
  override def distance(v1: Vector, v2: Vector): Double = {
    validate(v1, v2)

    val sum = v1.toArray.zip(v2.toArray).map {
      case(elm1: Double, elm2: Double) => Math.pow(Math.abs(elm1 - elm2), exponent)
    }.sum
    Math.pow(sum, 1 / exponent)
  }
}
