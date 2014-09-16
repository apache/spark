package org.apache.spark.mllib.clustering.metrics

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
import org.apache.spark.mllib.base._
import org.apache.spark.mllib.linalg.{SparseVector, DenseVector, Vector}


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
      val b = p.norm + c.norm - 2.0 * x
      if (b < upperBound) b else upperBound
    }
    if (d < Zero) Zero else d
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

