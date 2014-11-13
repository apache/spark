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

import breeze.linalg.{ DenseVector => BDV, SparseVector => BSV, Vector => BV }

import org.apache.spark.mllib.base._
import org.apache.spark.mllib.linalg.{ SparseVector, DenseVector, Vector }
import org.apache.spark.mllib.base.{ Centroid, FPoint, PointOps, Infinity, Zero }

class FastEUPoint(raw: BV[Double], weight: Double) extends FPoint(raw, weight) {
  val norm = if (weight == Zero) Zero else raw.dot(raw) / (weight * weight)
}

/**
 * Euclidean distance measure, expedited by pre-computing vector norms
 */
class FastEuclideanOps extends PointOps[FastEUPoint, FastEUPoint] with Serializable {

  type C = FastEUPoint
  type P = FastEUPoint

  val epsilon = 1e-4

  /* compute a lower bound on the euclidean distance distance */

  def distance(p: P, c: C, upperBound: Double): Double = {
    val d = if (p.weight == Zero || c.weight == Zero) {
      p.norm + c.norm
    } else {
      val x = p.raw.dot(c.raw) / (p.weight * c.weight)
      p.norm + c.norm - 2.0 * x
    }
    if (d < upperBound) {
      if (d < Zero) Zero else d
    } else {
      upperBound
    }
  }

  def arrayToPoint(raw: Array[Double]) = new P(new BDV[Double](raw), One)

  def vectorToPoint(v: Vector) = {
    v match {
      case x: DenseVector => new P(new BDV[Double](x.toArray), 1)
      case x: SparseVector => new P(new BSV[Double](x.indices, x.values, x.size), 1)
    }
  }

  def centerToPoint(v: C) = new P(v.raw, v.weight)

  def centroidToPoint(v: Centroid) = new P(v.raw, v.weight)

  def pointToCenter(v: P) = new C(v.raw, v.weight)

  def centroidToCenter(v: Centroid) = new C(v.raw, v.weight)

  def centerToVector(c: C) = new DenseVector(c.inh)

  def centerMoved(v: P, w: C): Boolean = distance(v, w, Infinity) > epsilon * epsilon

}
