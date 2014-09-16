package org.apache.spark.mllib

import org.apache.spark.mllib.linalg.Vector
import breeze.linalg.{Vector => BV}
import org.apache.spark.rdd.RDD

package object base {

  val Zero = 0.0
  val One = 1.0
  val Infinity = Double.MaxValue
  val Unknown = -1.0

  private[mllib] trait FP extends Serializable {
    val weight: Double
    val raw: BV[Double]
  }

  private[mllib] class FPoint(val raw: BV[Double], val weight: Double) extends FP {
    override def toString: String = weight + "," + (raw.toArray mkString ",")
    lazy val inh = (raw :*  (1.0 / weight)).toArray
  }

  /**
   * A mutable point in homogeneous coordinates
   */
  private[mllib] class Centroid extends Serializable {
    override def toString: String = weight + "," + (raw.toArray mkString ",")

    def isEmpty = weight == Zero

    var raw: BV[Double] = null

    var weight: Double = Zero

    def add(p: Centroid): this.type = add(p.raw, p.weight)

    def add(p: FP): this.type = add(p.raw, p.weight)

    def sub(p: Centroid): this.type = sub(p.raw, p.weight)

    def sub(p: FP): this.type = sub(p.raw, p.weight)

    def sub(r: BV[Double], w: Double): this.type = {
      if (r != null) {
        if (raw == null) {
          raw = r.toVector :*= -1.0
          weight = w * -1
        } else {
          raw -= r
          weight = weight - w
        }
      }
      this
    }

    def add(r: BV[Double], w: Double) : this.type = {
      if (r != null) {
        if (raw == null) {
          raw = r.toVector
          weight = w
        } else {
          raw += r
          weight = weight + w
        }
      }
      this
    }
  }

  private[mllib] trait PointOps[P <: FP, C <: FP] {
    def distance(p: P, c: C, upperBound: Double): Double

    def arrayToPoint(v: Array[Double]): P

    def vectorToPoint(v: Vector): P

    def centerToPoint(v: C): P

    def pointToCenter(v: P): C

    def centroidToCenter(v: Centroid): C

    def centroidToPoint(v: Centroid): P

    def centerMoved(v: P, w: C): Boolean

    def centerToVector(c: C) : Vector

    /**
     * Return the index of the closest point in `centers` to `point`, as well as its distance.
     */
    def findClosest(centers: Array[C], point: P): (Int, Double) = {
      var bestDistance = Infinity
      var bestIndex = 0
      var i = 0
      val end = centers.length
      while (i < end && bestDistance > 0.0) {
        val d = distance(point, centers(i), bestDistance)
        if (d < bestDistance) {
          bestIndex = i
          bestDistance = d
        }
        i = i + 1
      }
      (bestIndex, bestDistance)
    }

    def distortion(data: RDD[P], centers: Array[C]) = {
      data.mapPartitions{
        points => Array(points.foldLeft(Zero){ case (total, p) => total + findClosest(centers, p)._2}).iterator
      }.reduce( _ + _ )
    }

    /**
     * Return the K-means cost of a given point against the given cluster centers.
     */
    def pointCost(centers: Array[C], point: P): Double = findClosest(centers, point)._2

  }

}
