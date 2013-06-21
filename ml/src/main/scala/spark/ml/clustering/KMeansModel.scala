package spark.ml.clustering

import spark.ml.util.MLUtils

/**
 * A clustering model for K-means. Each point belongs to the cluster with the closest center.
 */
class KMeansModel(val clusterCenters: Array[Array[Double]]) {
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
}
