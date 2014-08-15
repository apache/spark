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
import org.apache.spark.mllib.linalg

/**
 * :: Experimental ::
 * Cosine distance implementation
 *
 * @see http://en.wikipedia.org/wiki/Cosine_similarity
 */
@Experimental
class CosineDistanceMeasure extends DistanceMeasure {

  /**
   * Calculates the cosine distance between 2 points
   *
   * @param v1 a Vector defining a multidimensional point in some feature space
   * @param v2 a Vector defining a multidimensional point in some feature space
   * @return a scalar doubles of the distance
   */
  override def distance(v1: linalg.Vector, v2: linalg.Vector): Double = {
    validate(v1, v2)

    val dotProduct = v1.toBreeze.dot(v2.toBreeze)
    var denominator = v1.toBreeze.norm(2) * v2.toBreeze.norm(2)

    // correct for floating-point rounding errors
    if (denominator < dotProduct) {
      denominator = dotProduct
    }

    // correct for zero-vector corner case
    if (denominator == 0 && dotProduct == 0) {
      return 0.0
    }
    1.0 - (dotProduct / denominator)
  }
}
