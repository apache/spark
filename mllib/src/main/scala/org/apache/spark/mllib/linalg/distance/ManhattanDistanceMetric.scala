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
import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg.Vector

/**
 * :: Experimental ::
 * Manhattan distance (L1 distance) implementation
 *
 * @see http://en.wikipedia.org/wiki/Manhattan_distance
 */
@Experimental
class ManhattanDistanceMetric extends DistanceMetric {

  override def apply(v1: BV[Double], v2: BV[Double]): Double = {
    sum((v1 - v2).map(Math.abs))
  }
}

/**
 * :: Experimental ::
 * A weighted Manhattan distance metric implementation
 * this metric is calculated by summing the absolute values of the difference
 * between each coordinate, optionally with weights.
 */
@Experimental
class WeightedManhattanDistanceMetric(val weights: BV[Double]) extends DistanceMetric {

  def this(v: Vector) = this(v.toBreeze)

  override def apply(v1: BV[Double], v2: BV[Double]): Double = {
    weights dot ((v1 - v2).map(Math.abs))
  }
}
