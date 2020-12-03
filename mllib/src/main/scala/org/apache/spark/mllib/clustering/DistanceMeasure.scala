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

package org.apache.spark.mllib.clustering

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Since
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.impl.Utils.indexUpperTriangular
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.BLAS.{axpy, dot, scal}
import org.apache.spark.mllib.util.MLUtils

private[spark] abstract class DistanceMeasure extends Serializable {

  /**
   * Statistics used in triangle inequality to obtain useful bounds to find closest centers.
   * @param distance distance between two centers
   */
  def computeStatistics(distance: Double): Double

  /**
   * Statistics used in triangle inequality to obtain useful bounds to find closest centers.
   *
   * @return The packed upper triangular part of a symmetric matrix containing statistics,
   *         matrix(i,j) represents:
   *         1, if i != j: a bound r = matrix(i,j) to help avoiding unnecessary distance
   *         computation. Given point x, let i be current closest center, and d be current best
   *         distance, if d < f(r), then we no longer need to compute the distance to center j;
   *         2, if i == j: a bound r = matrix(i,i) = min_k{matrix(i,k)|k!=i}. If distance
   *         between point x and center i is less than f(r), then center i is the closest center
   *         to point x.
   */
  def computeStatistics(centers: Array[VectorWithNorm]): Array[Double] = {
    val k = centers.length
    if (k == 1) return Array(Double.NaN)

    val packedValues = Array.ofDim[Double](k * (k + 1) / 2)
    val diagValues = Array.fill(k)(Double.PositiveInfinity)
    var i = 0
    while (i < k) {
      var j = i + 1
      while (j < k) {
        val d = distance(centers(i), centers(j))
        val s = computeStatistics(d)
        val index = indexUpperTriangular(k, i, j)
        packedValues(index) = s
        if (s < diagValues(i)) diagValues(i) = s
        if (s < diagValues(j)) diagValues(j) = s
        j += 1
      }
      i += 1
    }

    i = 0
    while (i < k) {
      val index = indexUpperTriangular(k, i, i)
      packedValues(index) = diagValues(i)
      i += 1
    }
    packedValues
  }

  /**
   * Compute distance between centers in a distributed way.
   */
  def computeStatisticsDistributedly(
      sc: SparkContext,
      bcCenters: Broadcast[Array[VectorWithNorm]]): Array[Double] = {
    val k = bcCenters.value.length
    if (k == 1) return Array(Double.NaN)

    val packedValues = Array.ofDim[Double](k * (k + 1) / 2)
    val diagValues = Array.fill(k)(Double.PositiveInfinity)

    val numParts = math.min(k, 1024)
    sc.range(0, numParts, 1, numParts)
      .mapPartitionsWithIndex { case (pid, _) =>
        val centers = bcCenters.value
        Iterator.range(0, k).flatMap { i =>
          Iterator.range(i + 1, k).flatMap { j =>
            val hash = (i, j).hashCode.abs
            if (hash % numParts == pid) {
              val d = distance(centers(i), centers(j))
              val s = computeStatistics(d)
              Iterator.single((i, j, s))
            } else Iterator.empty
          }
        }
      }.collect.foreach { case (i, j, s) =>
        val index = indexUpperTriangular(k, i, j)
        packedValues(index) = s
        if (s < diagValues(i)) diagValues(i) = s
        if (s < diagValues(j)) diagValues(j) = s
      }

    var i = 0
    while (i < k) {
      val index = indexUpperTriangular(k, i, i)
      packedValues(index) = diagValues(i)
      i += 1
    }
    packedValues
  }

  /**
   * @return the index of the closest center to the given point, as well as the cost.
   */
  def findClosest(
      centers: Array[VectorWithNorm],
      statistics: Array[Double],
      point: VectorWithNorm): (Int, Double)

  /**
   * @return the index of the closest center to the given point, as well as the cost.
   */
  def findClosest(
      centers: Array[VectorWithNorm],
      point: VectorWithNorm): (Int, Double) = {
    var bestDistance = Double.PositiveInfinity
    var bestIndex = 0
    var i = 0
    while (i < centers.length) {
      val center = centers(i)
      val currentDistance = distance(center, point)
      if (currentDistance < bestDistance) {
        bestDistance = currentDistance
        bestIndex = i
      }
      i += 1
    }
    (bestIndex, bestDistance)
  }

  /**
   * @return the K-means cost of a given point against the given cluster centers.
   */
  def pointCost(
      centers: Array[VectorWithNorm],
      point: VectorWithNorm): Double = {
    findClosest(centers, point)._2
  }

  /**
   * @return whether a center converged or not, given the epsilon parameter.
   */
  def isCenterConverged(
      oldCenter: VectorWithNorm,
      newCenter: VectorWithNorm,
      epsilon: Double): Boolean = {
    distance(oldCenter, newCenter) <= epsilon
  }

  /**
   * @return the distance between two points.
   */
  def distance(
      v1: VectorWithNorm,
      v2: VectorWithNorm): Double

  /**
   * @return the total cost of the cluster from its aggregated properties
   */
  def clusterCost(
      centroid: VectorWithNorm,
      pointsSum: VectorWithNorm,
      weightSum: Double,
      pointsSquaredNorm: Double): Double

  /**
   * Updates the value of `sum` adding the `point` vector.
   * @param point a `VectorWithNorm` to be added to `sum` of a cluster
   * @param sum the `sum` for a cluster to be updated
   */
  def updateClusterSum(point: VectorWithNorm, sum: Vector): Unit = {
    axpy(point.weight, point.vector, sum)
  }

  /**
   * Returns a centroid for a cluster given its `sum` vector and the weightSum of points.
   *
   * @param sum   the `sum` for a cluster
   * @param weightSum the weightSum of points in the cluster
   * @return the centroid of the cluster
   */
  def centroid(sum: Vector, weightSum: Double): VectorWithNorm = {
    scal(1.0 / weightSum, sum)
    new VectorWithNorm(sum)
  }

  /**
   * Returns two new centroids symmetric to the specified centroid applying `noise` with the
   * with the specified `level`.
   *
   * @param level the level of `noise` to apply to the given centroid.
   * @param noise a noise vector
   * @param centroid the parent centroid
   * @return a left and right centroid symmetric to `centroid`
   */
  def symmetricCentroids(
      level: Double,
      noise: Vector,
      centroid: Vector): (VectorWithNorm, VectorWithNorm) = {
    val left = centroid.copy
    axpy(-level, noise, left)
    val right = centroid.copy
    axpy(level, noise, right)
    (new VectorWithNorm(left), new VectorWithNorm(right))
  }

  /**
   * @return the cost of a point to be assigned to the cluster centroid
   */
  def cost(
      point: VectorWithNorm,
      centroid: VectorWithNorm): Double = distance(point, centroid)
}

@Since("2.4.0")
object DistanceMeasure {

  @Since("2.4.0")
  val EUCLIDEAN = "euclidean"
  @Since("2.4.0")
  val COSINE = "cosine"

  private[spark] def decodeFromString(distanceMeasure: String): DistanceMeasure =
    distanceMeasure match {
      case EUCLIDEAN => new EuclideanDistanceMeasure
      case COSINE => new CosineDistanceMeasure
      case _ => throw new IllegalArgumentException(s"distanceMeasure must be one of: " +
        s"$EUCLIDEAN, $COSINE. $distanceMeasure provided.")
    }

  private[spark] def validateDistanceMeasure(distanceMeasure: String): Boolean = {
    distanceMeasure match {
      case DistanceMeasure.EUCLIDEAN => true
      case DistanceMeasure.COSINE => true
      case _ => false
    }
  }
}

private[spark] class EuclideanDistanceMeasure extends DistanceMeasure {

  /**
   * Statistics used in triangle inequality to obtain useful bounds to find closest centers.
   * @see <a href="https://www.aaai.org/Papers/ICML/2003/ICML03-022.pdf">Charles Elkan,
   *      Using the Triangle Inequality to Accelerate k-Means</a>
   *
   * @return One element used in statistics matrix to make matrix(i,j) represents:
   *         1, if i != j: a bound r = matrix(i,j) to help avoiding unnecessary distance
   *         computation. Given point x, let i be current closest center, and d be current best
   *         squared distance, if d < r, then we no longer need to compute the distance to center
   *         j. matrix(i,j) equals to squared of half of Euclidean distance between centers i
   *         and j;
   *         2, if i == j: a bound r = matrix(i,i) = min_k{matrix(i,k)|k!=i}. If squared
   *         distance between point x and center i is less than r, then center i is the closest
   *         center to point x.
   */
  override def computeStatistics(distance: Double): Double = {
    0.25 * distance * distance
  }

  /**
   * @return the index of the closest center to the given point, as well as the cost.
   */
  override def findClosest(
      centers: Array[VectorWithNorm],
      statistics: Array[Double],
      point: VectorWithNorm): (Int, Double) = {
    var bestDistance = EuclideanDistanceMeasure.fastSquaredDistance(centers(0), point)
    if (bestDistance < statistics(0)) return (0, bestDistance)

    val k = centers.length
    var bestIndex = 0
    var i = 1
    while (i < k) {
      val center = centers(i)
      // Since `\|a - b\| \geq |\|a\| - \|b\||`, we can use this lower bound to avoid unnecessary
      // distance computation.
      val normDiff = center.norm - point.norm
      val lowerBound = normDiff * normDiff
      if (lowerBound < bestDistance) {
        val index1 = indexUpperTriangular(k, i, bestIndex)
        if (statistics(index1) < bestDistance) {
          val d = EuclideanDistanceMeasure.fastSquaredDistance(center, point)
          val index2 = indexUpperTriangular(k, i, i)
          if (d < statistics(index2)) return (i, d)
          if (d < bestDistance) {
            bestDistance = d
            bestIndex = i
          }
        }
      }
      i += 1
    }
    (bestIndex, bestDistance)
  }

  /**
   * @return the index of the closest center to the given point, as well as the squared distance.
   */
  override def findClosest(
      centers: Array[VectorWithNorm],
      point: VectorWithNorm): (Int, Double) = {
    var bestDistance = Double.PositiveInfinity
    var bestIndex = 0
    var i = 0
    while (i < centers.length) {
      val center = centers(i)
      // Since `\|a - b\| \geq |\|a\| - \|b\||`, we can use this lower bound to avoid unnecessary
      // distance computation.
      var lowerBoundOfSqDist = center.norm - point.norm
      lowerBoundOfSqDist = lowerBoundOfSqDist * lowerBoundOfSqDist
      if (lowerBoundOfSqDist < bestDistance) {
        val distance = EuclideanDistanceMeasure.fastSquaredDistance(center, point)
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
   * @return whether a center converged or not, given the epsilon parameter.
   */
  override def isCenterConverged(
      oldCenter: VectorWithNorm,
      newCenter: VectorWithNorm,
      epsilon: Double): Boolean = {
    EuclideanDistanceMeasure.fastSquaredDistance(newCenter, oldCenter) <= epsilon * epsilon
  }

  /**
   * @param v1: first vector
   * @param v2: second vector
   * @return the Euclidean distance between the two input vectors
   */
  override def distance(v1: VectorWithNorm, v2: VectorWithNorm): Double = {
    Math.sqrt(EuclideanDistanceMeasure.fastSquaredDistance(v1, v2))
  }

  /**
   * @return the total cost of the cluster from its aggregated properties
   */
  override def clusterCost(
      centroid: VectorWithNorm,
      pointsSum: VectorWithNorm,
      weightSum: Double,
      pointsSquaredNorm: Double): Double = {
    math.max(pointsSquaredNorm - weightSum * centroid.norm * centroid.norm, 0.0)
  }

  /**
   * @return the cost of a point to be assigned to the cluster centroid
   */
  override def cost(
      point: VectorWithNorm,
      centroid: VectorWithNorm): Double = {
    EuclideanDistanceMeasure.fastSquaredDistance(point, centroid)
  }
}


private[spark] object EuclideanDistanceMeasure {
  /**
   * @return the squared Euclidean distance between two vectors computed by
   * [[org.apache.spark.mllib.util.MLUtils#fastSquaredDistance]].
   */
  private[clustering] def fastSquaredDistance(
      v1: VectorWithNorm,
      v2: VectorWithNorm): Double = {
    MLUtils.fastSquaredDistance(v1.vector, v1.norm, v2.vector, v2.norm)
  }
}

private[spark] class CosineDistanceMeasure extends DistanceMeasure {

  /**
   * Statistics used in triangle inequality to obtain useful bounds to find closest centers.
   *
   * @return One element used in statistics matrix to make matrix(i,j) represents:
   *         1, if i != j: a bound r = matrix(i,j) to help avoiding unnecessary distance
   *         computation. Given point x, let i be current closest center, and d be current best
   *         squared distance, if d < r, then we no longer need to compute the distance to center
   *         j. For Cosine distance, it is similar to Euclidean distance. However, radian/angle
   *         is used instead of Cosine distance to compute matrix(i,j): for centers i and j,
   *         compute the radian/angle between them, halving it, and converting it back to Cosine
   *         distance at the end;
   *         2, if i == j: a bound r = matrix(i,i) = min_k{matrix(i,k)|k!=i}. If Cosine
   *         distance between point x and center i is less than r, then center i is the closest
   *         center to point x.
   */
  override def computeStatistics(distance: Double): Double = {
    // d = 1 - cos(x)
    // r = 1 - cos(x/2) = 1 - sqrt((cos(x) + 1) / 2) = 1 - sqrt(1 - d/2)
    1 - math.sqrt(1 - distance / 2)
  }

  /**
   * @return the index of the closest center to the given point, as well as the cost.
   */
  def findClosest(
      centers: Array[VectorWithNorm],
      statistics: Array[Double],
      point: VectorWithNorm): (Int, Double) = {
    var bestDistance = distance(centers(0), point)
    if (bestDistance < statistics(0)) return (0, bestDistance)

    val k = centers.length
    var bestIndex = 0
    var i = 1
    while (i < k) {
      val index1 = indexUpperTriangular(k, i, bestIndex)
      if (statistics(index1) < bestDistance) {
        val center = centers(i)
        val d = distance(center, point)
        val index2 = indexUpperTriangular(k, i, i)
        if (d < statistics(index2)) return (i, d)
        if (d < bestDistance) {
          bestDistance = d
          bestIndex = i
        }
      }
      i += 1
    }
    (bestIndex, bestDistance)
  }

  /**
   * @param v1: first vector
   * @param v2: second vector
   * @return the cosine distance between the two input vectors
   */
  override def distance(v1: VectorWithNorm, v2: VectorWithNorm): Double = {
    assert(v1.norm > 0 && v2.norm > 0, "Cosine distance is not defined for zero-length vectors.")
    1 - dot(v1.vector, v2.vector) / v1.norm / v2.norm
  }

  /**
   * Updates the value of `sum` adding the `point` vector.
   * @param point a `VectorWithNorm` to be added to `sum` of a cluster
   * @param sum the `sum` for a cluster to be updated
   */
  override def updateClusterSum(point: VectorWithNorm, sum: Vector): Unit = {
    assert(point.norm > 0, "Cosine distance is not defined for zero-length vectors.")
    axpy(point.weight / point.norm, point.vector, sum)
  }

  /**
   * Returns a centroid for a cluster given its `sum` vector and its `count` of points.
   *
   * @param sum   the `sum` for a cluster
   * @param weightSum the sum of weight in the cluster
   * @return the centroid of the cluster
   */
  override def centroid(sum: Vector, weightSum: Double): VectorWithNorm = {
    scal(1.0 / weightSum, sum)
    val norm = Vectors.norm(sum, 2)
    scal(1.0 / norm, sum)
    new VectorWithNorm(sum, 1)
  }

  /**
   * @return the total cost of the cluster from its aggregated properties
   */
  override def clusterCost(
      centroid: VectorWithNorm,
      pointsSum: VectorWithNorm,
      weightSum: Double,
      pointsSquaredNorm: Double): Double = {
    val costVector = pointsSum.vector.copy
    math.max(weightSum - dot(centroid.vector, costVector) / centroid.norm, 0.0)
  }

  /**
   * Returns two new centroids symmetric to the specified centroid applying `noise` with the
   * with the specified `level`.
   *
   * @param level the level of `noise` to apply to the given centroid.
   * @param noise a noise vector
   * @param centroid the parent centroid
   * @return a left and right centroid symmetric to `centroid`
   */
  override def symmetricCentroids(
      level: Double,
      noise: Vector,
      centroid: Vector): (VectorWithNorm, VectorWithNorm) = {
    val (left, right) = super.symmetricCentroids(level, noise, centroid)
    val leftVector = left.vector
    val rightVector = right.vector
    scal(1.0 / left.norm, leftVector)
    scal(1.0 / right.norm, rightVector)
    (new VectorWithNorm(leftVector, 1.0), new VectorWithNorm(rightVector, 1.0))
  }
}
