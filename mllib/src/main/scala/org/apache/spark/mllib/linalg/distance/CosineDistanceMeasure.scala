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

import breeze.linalg.{Vector => BV, sum}
import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg.Vector

/**
 * :: Experimental ::
 * Cosine distance implementation
 *
 * @see http://en.wikipedia.org/wiki/Cosine_similarity
 */
@Experimental
class CosineDistanceMeasure extends DistanceMeasure {

  override def mixVectors(v1: Vector, v2: Vector): breeze.linalg.Vector[Double] = {
    var denominator = v1.toBreeze.norm(2) * v2.toBreeze.norm(2)
    val dotProductElements = v1.toBreeze.:*(v2.toBreeze)
    val dotProduct = sum(dotProductElements)

    // correct for floating-point rounding errors
    if (denominator < dotProduct) {
      denominator = dotProduct
    }

    // correct for zero-vector corner case
    if (denominator == 0 && dotProduct == 0) {
      // convert all element into Double.MinValue
      return dotProductElements.map(elm => Double.MinValue)
    }
    dotProductElements.map(elm => elm / denominator)
  }

  override def vectorToDistance(breezeVector: BV[Double]): Double = {
    if (breezeVector.forall(elm => elm == Double.MinValue)) {
      return 0.0
    }
    1.0 - sum(breezeVector)
  }
}
