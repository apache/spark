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

import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.BLAS.{axpy, dot, scal}
import org.apache.spark.mllib.util.MLUtils

private[spark] abstract class DistanceMeasure extends Serializable {

  /**
   * @return the index of the closest center to the given point, as well as the cost.
   */
  def findClosest(
      centers: TraversableOnce[VectorWithNorm],
      point: VectorWithNorm): (Int, Double) = {
    var bestDistance = Double.PositiveInfinity
    var bestIndex = 0
    var i = 0
    centers.foreach { center =>
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
      centers: TraversableOnce[VectorWithNorm],
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
      numberOfPoints: Long,
      pointsSquaredNorm: Double): Double

  /**
   * Updates the value of `sum` adding the `point` vector.
   * @param point a `VectorWithNorm` to be added to `sum` of a cluster
   * @param sum the `sum` for a cluster to be updated
   */
  def updateClusterSum(point: VectorWithNorm, sum: Vector): Unit = {
    axpy(1.0, point.vector, sum)
  }

  /**
   * Returns a centroid for a cluster given its `sum` vector and its `count` of points.
   *
   * @param sum   the `sum` for a cluster
   * @param count the number of points in the cluster
   * @return the centroid of the cluster
   */
  def centroid(sum: Vector, count: Long): VectorWithNorm = {
    scal(1.0 / count, sum)
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
   * @return the index of the closest center to the given point, as well as the squared distance.
   */
  override def findClosest(
      centers: TraversableOnce[VectorWithNorm],
      point: VectorWithNorm): (Int, Double) = {
    var bestDistance = Double.PositiveInfinity
    var bestIndex = 0
    var i = 0
    centers.foreach { center =>
      // Since `\|a - b\| \geq |\|a\| - \|b\||`, we can use this lower bound to avoid unnecessary
      // distance computation.
      var lowerBoundOfSqDist = center.norm - point.norm
      lowerBoundOfSqDist = lowerBoundOfSqDist * lowerBoundOfSqDist
      if (lowerBoundOfSqDist < bestDistance) {
        val distance: Double = EuclideanDistanceMeasure.fastSquaredDistance(center, point)
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
      numberOfPoints: Long,
      pointsSquaredNorm: Double): Double = {
    math.max(pointsSquaredNorm - numberOfPoints * centroid.norm * centroid.norm, 0.0)
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
    axpy(1.0 / point.norm, point.vector, sum)
  }

  /**
   * Returns a centroid for a cluster given its `sum` vector and its `count` of points.
   *
   * @param sum   the `sum` for a cluster
   * @param count the number of points in the cluster
   * @return the centroid of the cluster
   */
  override def centroid(sum: Vector, count: Long): VectorWithNorm = {
    scal(1.0 / count, sum)
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
      numberOfPoints: Long,
      pointsSquaredNorm: Double): Double = {
    val costVector = pointsSum.vector.copy
    math.max(numberOfPoints - dot(centroid.vector, costVector) / centroid.norm, 0.0)
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
