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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.Logging
import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.BLAS.{axpy, scal}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * :: Experimental ::
 *
 * The Dirichlet process (DP) is a popular non-parametric Bayesian mixture
 * model that allows for flexible clustering of data without having to
 * determine the number of clusters in advance.
 *
 * Given a set of data points, this class performs cluster creation process,
 * based on DP means algorithm, iterating until the maximum number of iterations
 * is reached or the convergence criteria is satisfied. With the current
 * global set of centers, it locally creates a new cluster centered at `x`
 * whenever it encounters an uncovered data point `x`. In a similar manner,
 * a local cluster center is promoted to a global center whenever an uncovered
 * local cluster center is found. A data point is said to be "covered" by
 * a cluster `c` if the distance from the point to the cluster center of `c`
 * is less than a given lambda value.
 *
 * The original paper is "MLbase: Distributed Machine Learning Made Easy" by
 * Xinghao Pan, Evan R. Sparks, Andre Wibisono
 *
 * @param lambda The distance threshold value that controls cluster creation.
 * @param convergenceTol The threshold value at which convergence is considered to have occurred.
 * @param maxIterations The maximum number of iterations to perform.
 */

@Experimental
class DpMeans private (
    private var lambda: Double,
    private var convergenceTol: Double,
    private var maxIterations: Int) extends Serializable with Logging {

  /**
   * Constructs a default instance.The default parameters are {lambda: 1, convergenceTol: 0.01,
   * maxIterations: 20}.
   */
  def this() = this(1, 0.01, 20)

  /** Set the distance threshold that controls cluster creation. Default: 1 */
  def getLambda(): Double = lambda

  /** Return the lambda. */
  def setLambda(lambda: Double): this.type = {
    this.lambda = lambda
    this
  }

  /** Set the threshold value at which convergence is considered to have occurred. Default: 0.01 */
  def setConvergenceTol(convergenceTol: Double): this.type = {
    this.convergenceTol = convergenceTol
    this
  }

  /** Return the threshold value at which convergence is considered to have occurred. */
  def getConvergenceTol: Double = convergenceTol

  /** Set the maximum number of iterations. Default: 20 */
  def setMaxIterations(maxIterations: Int): this.type = {
    this.maxIterations = maxIterations
    this
  }

  /** Return the maximum number of iterations. */
  def getMaxIterations: Int = maxIterations

  /**
   * Perform DP means clustering
   */
  def run(data: RDD[Vector]): DpMeansModel = {
    if (data.getStorageLevel == StorageLevel.NONE) {
      logWarning("The input data is not directly cached, which may hurt performance if its"
        + " parent RDDs are also uncached.")
    }

    // Compute squared norms and cache them.
    val norms = data.map(Vectors.norm(_, 2.0))
    norms.persist()
    val zippedData = data.zip(norms).map {
      case (v, norm) => new VectorWithNorm(v, norm)
    }

    // Implementation of DP means algorithm.
    var iteration = 0
    var covered = false
    var converged = false
    var localCenters = Array.empty[VectorWithNorm]
    val globalCenters = ArrayBuffer.empty[VectorWithNorm]

    // Execute clustering until the maximum number of iterations is reached
    // or the cluster centers have converged.
    while (iteration < maxIterations && !converged) {
      type WeightedPoint = (Vector, Long)
      def mergeClusters(x: WeightedPoint, y: WeightedPoint): WeightedPoint = {
        axpy(1.0, x._1, y._1)
        (y._1, x._2 + y._2)
      }

      // Loop until all data points are covered by some cluster center
      do {
        localCenters = zippedData.mapPartitions(h => DpMeans.cover(h, globalCenters, lambda))
          .collect()
        if (localCenters.isEmpty) {
          covered = true
        }
        // Promote a local cluster center to a global center
        else {
          var newGlobalCenters = DpMeans.cover(localCenters.iterator,
            ArrayBuffer.empty[VectorWithNorm], lambda)
          globalCenters ++= newGlobalCenters
        }
      } while (covered == false)

      // Find the sum and count of points belonging to each cluster
      val clusterStat = zippedData.mapPartitions { points =>
        val activeCenters = globalCenters
        val k = activeCenters.length
        val dims = activeCenters(0).vector.size

        val sums = Array.fill(k)(Vectors.zeros(dims))
        val counts = Array.fill(k)(0L)
        val totalCost = Array.fill(k)(0.0D)

        points.foreach { point =>
          val (currentCenter, cost) = DpMeans.assignCluster(activeCenters, point)
          totalCost(currentCenter) += cost
          val currentSum = sums(currentCenter)
          axpy(1.0, point.vector, currentSum)
          counts(currentCenter) +=1
        }

        val result = for (i <- 0 until k) yield {
          (i, (sums(i), counts(i)))
        }
        result.iterator
      }.reduceByKey(mergeClusters).collectAsMap()

      // Update the cluster centers
      var changed = false
      var j = 0
      val currentK = clusterStat.size
      while (j < currentK) {
        val (sumOfPoints, count) = clusterStat(j)
        if (count != 0) {
          scal(1.0 / count, sumOfPoints)
          val newCenter = new VectorWithNorm(sumOfPoints)
          // Check for convergence
          globalCenters.length match {
            case currentK => if (DpMeans.squaredDistance(newCenter, globalCenters(j)) >
              convergenceTol * convergenceTol) {
              changed = true
              }
            case _ => changed = true
          }
          globalCenters(j) = newCenter
        }
        j += 1
      }
      if (!changed) {
        converged = true
        logInfo("DpMeans clustering finished in " + (iteration + 1) + " iterations")
      }
      iteration += 1
      norms.unpersist()
    }

    if (iteration == maxIterations) {
      logInfo(s"DPMeans reached the max number of iterations: $maxIterations.")
    } else {
      logInfo(s"DPMeans converged in $iteration iterations")
    }
    new DpMeansModel(globalCenters.toArray.map(_.vector))
  }
}
/**
 * Core methods of  DP means clustering.
 */
private object DpMeans {
  /**
   * A data point is said to be "covered" by a cluster `c` if the distance from the point
   * to the cluster center of `c` is less than a given lambda value.
   */
  def cover(
      points: Iterator[VectorWithNorm],
      centers: ArrayBuffer[VectorWithNorm],
      lambda: Double): Iterator[VectorWithNorm] = {
    var newCenters = ArrayBuffer.empty[VectorWithNorm]
    if(!points.isEmpty){
      if (centers.length == 0) newCenters += points.next
      points.foreach { z =>
        val dist = newCenters.union(centers).map { center => squaredDistance(z, center) }
        if (dist.min > lambda) newCenters += z
      }
    }
    newCenters.iterator
  }

  /**
   * Each data point is assigned to the nearest cluster. This method returns
   * the corresponding cluster label and the distance from the center to the
   * cluster center.
   */
    def assignCluster(
        centers: ArrayBuffer[VectorWithNorm],
        x: VectorWithNorm): (Int, Double) = {
      val dist = centers.map { c => squaredDistance(x, c) }
      (dist.indexOf(dist.min), dist.min)
    }

  /**
   * Returns the squared Euclidean distance between two vectors computed by
   * [[org.apache.spark.mllib.util.MLUtils#fastSquaredDistance]].
   */
  def squaredDistance(
      v1: VectorWithNorm,
      v2: VectorWithNorm): Double = {
    MLUtils.fastSquaredDistance(v1.vector, v1.norm, v2.vector, v2.norm)
  }

}
