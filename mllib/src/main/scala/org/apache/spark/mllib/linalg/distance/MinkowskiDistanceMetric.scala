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

import breeze.linalg.{sum, Vector => BV}
import org.apache.spark.annotation.Experimental

/**
 * Minkowski distance Implementation
 * The Minkowski distance is a metric on Euclidean space
 * which can be considered as a generalization of both
 * the Euclidean distance and the Manhattan distance.
 *
 * @see http://en.wikipedia.org/wiki/Minkowski_distance
 */
@Experimental
class MinkowskiDistanceMetric(val exponent: Double) extends DistanceMetric {

  // the default value for exponent
  def this() = this(3.0)

  override def apply(v1: BV[Double], v2: BV[Double]): Double = {
    val d = (v1 - v2).map(diff => Math.pow(Math.abs(diff), exponent))
    Math.pow(sum(d), 1 / exponent)
  }
}
