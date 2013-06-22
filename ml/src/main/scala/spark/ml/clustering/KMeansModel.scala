package spark.ml.clustering

import spark.RDD
import spark.SparkContext._
import spark.ml.util.MLUtils


/**
 * A clustering model for K-means. Each point belongs to the cluster with the closest center.
 */
class KMeansModel(val clusterCenters: Array[Array[Double]]) extends Serializable {
  /** Total number of clusters. */
  def k: Int = clusterCenters.length

  /** Return the cluster index that a given point belongs to. */
  def predict(point: Array[Double]): Int = {
    var bestDist = Double.PositiveInfinity
    var bestIndex = -1
    for (i <- 0 until k) {
      val dist = MLUtils.squaredDistance(clusterCenters(i), point)
      if (dist < bestDist) {
        bestDist = dist
        bestIndex = i
      }
    }
    bestIndex
  }

  /**
   * Return the K-means cost (sum of squared distances of points to their nearest center) for this
   * model on the given data.
   */
  def computeCost(data: RDD[Array[Double]]): Double = {
    data.map(p => KMeans.pointCost(clusterCenters, p)).sum
  }
}
