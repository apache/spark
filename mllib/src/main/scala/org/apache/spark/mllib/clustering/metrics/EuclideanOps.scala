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

package org.apache.spark.mllib.clustering.metrics

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
import org.apache.spark.mllib.base.{Centroid, FPoint, PointOps, Infinity, Zero}
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector}


/**
 * Euclidean distance measure
 *
 * This is the slow implementation of the squared Euclidean distance function,
 * shown here simply for clarity.
 */
class EuclideanOps extends PointOps[FPoint, FPoint] with Serializable {

  type C = FPoint
  type P = FPoint

  val epsilon = 1e-4

  def distance(p: P, c: C, upperBound: Double = Infinity): Double = {
    val d = p.inh.zip(c.inh).foldLeft(Zero) {
      case (d: Double, (a: Double, b: Double)) => d + (a - b) * (a - b)
    }
    if( d < Zero) Zero else d
  }

  def arrayToPoint(raw: Array[Double]) = new FPoint(BDV(raw), 1)

  def vectorToPoint(v: Vector) = {
    v match {
      case x: DenseVector => new FPoint(new BDV[Double](x.toArray), 1)
      case x: SparseVector => new FPoint(new BSV[Double](x.indices, x.values, x.size), 1)
    }
  }

  def centerToPoint(v: C) = new P(v.raw, v.weight)

  def centroidToPoint(v: Centroid) = new P(v.raw, v.weight)

  def pointToCenter(v: P) = new C(v.raw, v.weight)

  def centroidToCenter(v: Centroid) = new C(v.raw, v.weight)

  def centerToVector(c: C) = new DenseVector(c.inh)

  def centerMoved(v: FPoint, w: FPoint): Boolean = distance(v, w) > epsilon * epsilon

}

