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
 * based on DP-means algorithm, iterating until the maximum number of iterations
 * is reached or the convergence criteria is satisfied. With the current
 * global set of centers, it locally creates a new cluster centered at `x`
 * whenever it encounters an uncovered data point `x`. In a similar manner,
 * a local cluster center is promoted to a global center whenever an uncovered
 * local cluster center is found. A data point is said to be "covered" by
 * a cluster `c` if the distance from the point to the cluster center of `c`
 * is less than a given lambda value.
 *
 * The original paper is "Revisiting k-means: New Algorithms via Bayesian Nonparametrics"
 * by Brian Kulis, Michael I. Jordan. This implementation is based on "MLbase: Distributed
 * Machine Learning Made Easy" by Xinghao Pan, Evan R. Sparks, Andre Wibisono
 *
 * @param lambdaValue distance value that controls cluster creation.
 * @param convergenceTol The threshold value at which convergence is considered to have occurred.
 * @param maxIterations The maximum number of iterations to perform.
 * // TODO
 * @param maxClusterCount The maximum expected number of clusters.
 */

@Experimental
class DpMeans private (
    private var lambdaValue: Double,
    private var convergenceTol: Double,
    private var maxIterations: Int,
    private var maxClusterCount: Int) extends Serializable with Logging {

  /**
   * Constructs a default instance with default parameters: {lambdaValue: 1,
   * convergenceTol: 0.01, maxIterations: 20, maxClusterCount: 1000}.
   */
  def this() = this(1, 0.01, 20, 1000)

  /** Sets the value for the lambda parameter, which controls the cluster creation. Default: 1 */
  def setLambdaValue(lambdaValue: Double): this.type = {
    this.lambdaValue = lambdaValue
    this
  }

  /** Returns the lambda value */
  def getLambdaValue: Double = lambdaValue

  /** Sets the threshold value at which convergence is considered to have occurred. Default: 0.01 */
  def setConvergenceTol(convergenceTol: Double): this.type = {
    this.convergenceTol = convergenceTol
    this
  }

  /** Returns the convergence threshold value . */
  def getConvergenceTol: Double = convergenceTol

  /** Sets the maximum number of iterations. Default: 20 */
  def setMaxIterations(maxIterations: Int): this.type = {
    this.maxIterations = maxIterations
    this
  }

  /** Returns the maximum number of iterations. */
  def getMaxIterations: Int = maxIterations

  /** Sets the maximum number of clusters expected. Default: 1000 */
  def setMaxClusterCount(maxClusterCount: Int): this.type = {
    this.maxClusterCount = maxClusterCount
    this
  }

  /** Returns the maximum number of clusters expected. */
  def getMaxClusterCount: Int = maxClusterCount

  /**
   * Perform DP-means clustering
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

    // Implementation of DP-means algorithm.
    var iteration = 0
    var previousK = 1
    var covered = false
    var converged = false
    var localCenters = Array.empty[VectorWithNorm]
    val globalCenters = ArrayBuffer.empty[VectorWithNorm]

    // Execute clustering until the maximum number of iterations is reached
    // or the cluster centers have converged.
    while (iteration < maxIterations && !converged) {

      // This loop simulates the cluster assignment step by iterating through the data
      // points until no new clusters are generated. A new cluster is formed whenever
      // the distance from a data point to all the existing cluster centroids is greater
      // than lambdaValue. The cluster centers returned by generateNewCenters(), called
      // localCenters, are sent to the master. The cluster creation process is performed
      // locally on these localCenters and the resulting cluster centers are added to
      // the existing set of global centers.
      while (!covered) {
        val findLocalCenters = zippedData
          .mapPartitions(h =>
              DpMeans.generateNewCenters(h, globalCenters, lambdaValue)
          ).persist()
        val localCentersCount = findLocalCenters.count()
        logInfo(" Local centers count :: " + localCentersCount)
        if (localCentersCount > getMaxClusterCount) {
          logInfo(" Local centers count " + localCentersCount + " exceeds user expectation")
          sys.exit()
        }
        localCenters = findLocalCenters.collect()
        if (localCenters.isEmpty) {
          covered = true
        }
        // Promote a local cluster center to a global center
        else {
          val newGlobalCenters = DpMeans.generateNewCenters(localCenters.iterator,
            ArrayBuffer.empty[VectorWithNorm], lambdaValue)
          globalCenters ++= newGlobalCenters
          logInfo(" Number of global centers :: " + globalCenters.length)
        }
      }

      // Find the sum and count of points belonging to each cluster
      case class WeightedPoint(vector: Vector, count: Long)
      val mergeClusters = (x: WeightedPoint, y: WeightedPoint) => {
        axpy(1.0, y.vector, x.vector)
        WeightedPoint(x.vector, x.count + y.count)
      }

      val dims = globalCenters(0).vector.size
      val clusterStat = zippedData.mapPartitions { points =>
        val activeCenters = globalCenters
        val k = activeCenters.length

        val sums = Array.fill(k)(Vectors.zeros(dims))
        val counts = Array.fill(k)(0L)
        val totalCost = Array.fill(k)(0.0D)
        points.foreach { point =>
          val (currentCenter, cost) = DpMeans.assignCluster(activeCenters, point)
          totalCost(currentCenter) += cost
          val currentSum = sums(currentCenter)
          axpy(1.0, point.vector, currentSum)
          counts(currentCenter) += 1
        }
        val result = Iterator.tabulate(k) { i => (i, WeightedPoint(sums(i), counts(i))) }
        result
      }.aggregateByKey(WeightedPoint(Vectors.zeros(dims), 0L))(mergeClusters, mergeClusters)
        .collectAsMap()

      // Update the cluster centers and convergence check
      var changed = false
      var j = 0
      val currentK = clusterStat.size

      while (j < currentK) {
        val (sumOfPoints, count) = (clusterStat(j).vector, clusterStat(j).count)
        if (count != 0) {
          scal(1.0 / count, sumOfPoints)
          val newCenter = new VectorWithNorm(sumOfPoints)
          // Check for convergence
          if (previousK == currentK) {
            if (DpMeans.squaredDistance(newCenter, globalCenters(j))
              > convergenceTol * convergenceTol) {
              changed = true
            }
          }
          else {
            changed = true
          }
          globalCenters(j) = newCenter
        }
        previousK = currentK
        j += 1
      }
      if (!changed) {
        converged = true
        logInfo("DpMeans clustering finished in " + (iteration + 1) + " iterations")
      }
      previousK = currentK
      iteration += 1
    }

    if (iteration == maxIterations) {
      logInfo(s"DPMeans reached the max number of iterations: $maxIterations.")
    } else {
      logInfo(s"DPMeans converged in $iteration iterations")
    }
    new DpMeansModel(globalCenters.toArray.map(_.vector), lambdaValue)
  }
}

/**
 * Core methods of DP-means clustering.
 */
private object DpMeans {

  /**
   * A new cluster is formed whenever the distance from a data point to
   * all the existing cluster centroids is greater than lambdaValue.
   * @param points an iterator to the input data points
   * @param centers current centers
   * @param lambdaValue threshold value which decides the cluster creation
   * @return an iterator to the computed new centers
   */
  def generateNewCenters(
      points: Iterator[VectorWithNorm],
      centers: ArrayBuffer[VectorWithNorm],
      lambdaValue: Double): Iterator[VectorWithNorm] = {
    var newCenters = ArrayBuffer.empty[VectorWithNorm]
    if (!points.isEmpty) {
      if (centers.length == 0) newCenters += points.next
      points.foreach { z =>
        val dist = newCenters.union(centers).map { center => squaredDistance(z, center) }
        if (dist.min > lambdaValue) newCenters += z
      }
    }
    newCenters.iterator
  }

  /**
   * Returns the nearest cluster center computed by
   * [[org.apache.spark.mllib.clustering.KMeans#findClosest]]
   * @return (cluster label, distance from the point to the cluster center)
   */
  def assignCluster(centers: ArrayBuffer[VectorWithNorm], point: VectorWithNorm): (Int, Double) = {
    KMeans.findClosest(centers, point)
  }

  /**
   * Returns the squared Euclidean distance between two vectors computed by
   * [[org.apache.spark.mllib.util.MLUtils#fastSquaredDistance]].
   */
  def squaredDistance(v1: VectorWithNorm, v2: VectorWithNorm): Double = {
    MLUtils.fastSquaredDistance(v1.vector, v1.norm, v2.vector, v2.norm)
  }

}
