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

import breeze.linalg.{Vector => BV}
import breeze.linalg.max
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

  override def mixVectors(v1: Vector, v2: Vector): BV[Double] = {
    (v1.toBreeze - v2.toBreeze).map(Math.abs)
  }

  override def vectorToDistance(breezeVector: BV[Double]): Double = {
    max(breezeVector)
  }
}

/**
 * :: Experimental ::
 * A weighted Chebyshev distance implementation
 */
@Experimental
class WeightedChebyshevDistanceMetric(val weights: Vector)
    extends ChebyshevDistanceMetric with Weighted
