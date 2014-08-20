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
 * Euclidean distance implementation
 */
@Experimental
class EuclideanDistanceMeasure extends DistanceMeasure {

  /**
   * Calculates the euclidean distance (L2 distance) between 2 points
   *
   * D(x, y) = sqrt(sum((x1-y1)^2 + (x2-y2)^2 + ... + (xn-yn)^2))
   * or
   * D(x, y) = sqrt((x-y) dot (x-y))
   *
   * @param v1 a Vector defining a multidimensional point in some feature space
   * @param v2 a Vector defining a multidimensional point in some feature space
   * @return a scalar doubles of the distance
   */
  override def apply(v1: Vector, v2: Vector): Double = {
    validate(v1, v2)

    val diffVector = (v1.toBreeze - v2.toBreeze)
    val squaredDistance = diffVector.dot(diffVector)
    Math.sqrt(squaredDistance)
  }
}
