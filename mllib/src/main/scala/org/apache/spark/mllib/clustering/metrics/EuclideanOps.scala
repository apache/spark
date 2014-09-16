package org.apache.spark.mllib.clustering.metrics

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
import org.apache.spark.mllib.base.{Centroid, FPoint, PointOps, Infinity, Zero}
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector}


/**
 * Euclidean distance measure
 */
class EuclideanOps extends PointOps[FPoint, FPoint] with Serializable {

  type C = FPoint
  type P = FPoint

  val epsilon = 1e-4

  def distance(p: P, c: C, upperBound: Double = Infinity): Double = {
    val d = p.inh.zip(c.inh).foldLeft(Zero) { case (d: Double, (a: Double, b: Double)) => d + (a - b) * (a - b)}
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

