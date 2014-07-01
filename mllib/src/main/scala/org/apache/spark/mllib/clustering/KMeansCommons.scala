package org.apache.spark.mllib.clustering

import breeze.linalg.{DenseVector => BDV, Vector => BV, norm => breezeNorm}

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.util.MLUtils

trait KMeansObjectCommons {
  
    /**
   * Returns the index of the closest center to the given point, as well as the squared distance.
   */
  private[mllib] def findClosest(
      centers: TraversableOnce[BreezeVectorWithNorm],
      point: BreezeVectorWithNorm): (Int, Double) = {
    var bestDistance = Double.PositiveInfinity
    var bestIndex = 0
    var i = 0
    centers.foreach { center =>
      // Since `\|a - b\| \geq |\|a\| - \|b\||`, we can use this lower bound to avoid unnecessary
      // distance computation.
      var lowerBoundOfSqDist = center.norm - point.norm
      lowerBoundOfSqDist = lowerBoundOfSqDist * lowerBoundOfSqDist
      if (lowerBoundOfSqDist < bestDistance) {
        val distance: Double = fastSquaredDistance(center, point)
        if (distance < bestDistance) {
          bestDistance = distance
          bestIndex = i
        }
      }
      i += 1
    }
    (bestIndex, bestDistance)
  }

  /**
   * Returns the K-means cost of a given point against the given cluster centers.
   */
  private[mllib] def pointCost(
      centers: TraversableOnce[BreezeVectorWithNorm],
      point: BreezeVectorWithNorm): Double =
    findClosest(centers, point)._2

  /**
   * Returns the squared Euclidean distance between two vectors computed by
   * [[org.apache.spark.mllib.util.MLUtils#fastSquaredDistance]].
   */
  private[clustering] def fastSquaredDistance(
      v1: BreezeVectorWithNorm,
      v2: BreezeVectorWithNorm): Double = {
    MLUtils.fastSquaredDistance(v1.vector, v1.norm, v2.vector, v2.norm)
  }
  
}

/**
 * A breeze vector with its norm for fast distance computation.
 *
 * @see [[org.apache.spark.mllib.clustering.KMeans#fastSquaredDistance]]
 */
private[clustering]
class BreezeVectorWithNorm(val vector: BV[Double], val norm: Double) extends Serializable {

  def this(vector: BV[Double]) = this(vector, breezeNorm(vector, 2.0))

  def this(array: Array[Double]) = this(new BDV[Double](array))

  def this(v: Vector) = this(v.toBreeze)

  /** Converts the vector to a dense vector. */
  def toDense = new BreezeVectorWithNorm(vector.toDenseVector, norm)
}