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

import breeze.linalg.{DenseVector => DBV, Vector => BV}
import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg.Vector

/**
 * :: Experimental ::
 * Euclidean distance implementation
 */
@Experimental
class EuclideanDistanceMetric extends DistanceMetric {

  /**
   * Calculates the euclidean distance (L2 distance) between 2 points
   *
   * D(x, y) = sqrt(sum((x1-y1)^2 + (x2-y2)^2 + ... + (xn-yn)^2))
   * or
   * D(x, y) = sqrt((x-y) dot (x-y))
   *
   * @param v1
   * @param v2
   * @return
   */
  override def apply(v1: BV[Double], v2: BV[Double]): Double = {
    val d = v1 - v2
    Math.sqrt(d dot d)
  }
}

/**
 * :: Experimental ::
 * A weighted Euclidean distance metric implementation
 * this metric is calculated by summing the square root of the squared differences
 * between each coordinate, optionally adding weights.
 */
@Experimental
class WeightedEuclideanDistanceMetric(val weights: BV[Double]) extends DistanceMetric {

  def this(v: Vector) = this(v.toBreeze)

  override def apply(v1: BV[Double], v2: BV[Double]): Double = {
    val d = v1 - v2
    Math.sqrt(d dot (weights :* d))
  }
}
