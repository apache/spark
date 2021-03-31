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

package org.apache.spark.ml.evaluation

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Since
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.linalg.{BLAS, DenseVector, Vector, Vectors}
import org.apache.spark.ml.util.MetadataUtils
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType


/**
 * Metrics for clustering, which expects two input columns: prediction and label.
 */
@Since("3.1.0")
class ClusteringMetrics private[spark](dataset: Dataset[_]) {

  private var distanceMeasure: String = "squaredEuclidean"

  def getDistanceMeasure: String = distanceMeasure

  def setDistanceMeasure(value: String) : this.type = {
    require(value.equalsIgnoreCase("squaredEuclidean") ||
      value.equalsIgnoreCase("cosine"))
    distanceMeasure = value
    this
  }

  /**
   * Returns the silhouette score
   */
  @Since("3.1.0")
  def silhouette(): Double = {
    val columns = dataset.columns.toSeq
    if (distanceMeasure.equalsIgnoreCase("squaredEuclidean")) {
      SquaredEuclideanSilhouette.computeSilhouetteScore(
        dataset, columns(0), columns(1), columns(2))
    } else {
      CosineSilhouette.computeSilhouetteScore(dataset, columns(0), columns(1), columns(2))
    }
  }
}


private[evaluation] abstract class Silhouette {

  /**
   * It computes the Silhouette coefficient for a point.
   */
  def pointSilhouetteCoefficient(
      clusterIds: Set[Double],
      pointClusterId: Double,
      weightSum: Double,
      weight: Double,
      averageDistanceToCluster: (Double) => Double): Double = {
    if (weightSum == weight) {
      // Single-element clusters have silhouette 0
      0.0
    } else {
      // Here we compute the average dissimilarity of the current point to any cluster of which the
      // point is not a member.
      // The cluster with the lowest average dissimilarity - i.e. the nearest cluster to the current
      // point - is said to be the "neighboring cluster".
      val otherClusterIds = clusterIds.filter(_ != pointClusterId)
      val neighboringClusterDissimilarity = otherClusterIds.map(averageDistanceToCluster).min
      // adjustment for excluding the node itself from the computation of the average dissimilarity
      val currentClusterDissimilarity =
      averageDistanceToCluster(pointClusterId) * weightSum /
        (weightSum - weight)
      if (currentClusterDissimilarity < neighboringClusterDissimilarity) {
        1 - (currentClusterDissimilarity / neighboringClusterDissimilarity)
      } else if (currentClusterDissimilarity > neighboringClusterDissimilarity) {
        (neighboringClusterDissimilarity / currentClusterDissimilarity) - 1
      } else {
        0.0
      }
    }
  }

  /**
   * Compute the mean Silhouette values of all samples.
   */
  def overallScore(df: DataFrame, scoreColumn: Column, weightColumn: Column): Double = {
    df.select(sum(scoreColumn * weightColumn) / sum(weightColumn)).collect()(0).getDouble(0)
  }
}

/**
 * SquaredEuclideanSilhouette computes the average of the
 * Silhouette over all the data of the dataset, which is
 * a measure of how appropriately the data have been clustered.
 *
 * The Silhouette for each point `i` is defined as:
 *
 * <blockquote>
 *   $$
 *   s_{i} = \frac{b_{i}-a_{i}}{max\{a_{i},b_{i}\}}
 *   $$
 * </blockquote>
 *
 * which can be rewritten as
 *
 * <blockquote>
 *   $$
 *   s_{i}= \begin{cases}
 *   1-\frac{a_{i}}{b_{i}} & \text{if } a_{i} \leq b_{i} \\
 *   \frac{b_{i}}{a_{i}}-1 & \text{if } a_{i} \gt b_{i} \end{cases}
 *   $$
 * </blockquote>
 *
 * where `$a_{i}$` is the average dissimilarity of `i` with all other data
 * within the same cluster, `$b_{i}$` is the lowest average dissimilarity
 * of `i` to any other cluster, of which `i` is not a member.
 * `$a_{i}$` can be interpreted as how well `i` is assigned to its cluster
 * (the smaller the value, the better the assignment), while `$b_{i}$` is
 * a measure of how well `i` has not been assigned to its "neighboring cluster",
 * i.e. the nearest cluster to `i`.
 *
 * Unfortunately, the naive implementation of the algorithm requires to compute
 * the distance of each couple of points in the dataset. Since the computation of
 * the distance measure takes `D` operations - if `D` is the number of dimensions
 * of each point, the computational complexity of the algorithm is `O(N^2^*D)`, where
 * `N` is the cardinality of the dataset. Of course this is not scalable in `N`,
 * which is the critical number in a Big Data context.
 *
 * The algorithm which is implemented in this object, instead, is an efficient
 * and parallel implementation of the Silhouette using the squared Euclidean
 * distance measure.
 *
 * With this assumption, the total distance of the point `X`
 * to the points `$C_{i}$` belonging to the cluster `$\Gamma$` is:
 *
 * <blockquote>
 *   $$
 *   \sum\limits_{i=1}^N d(X, C_{i} ) =
 *   \sum\limits_{i=1}^N \Big( \sum\limits_{j=1}^D (x_{j}-c_{ij})^2 \Big)
 *   = \sum\limits_{i=1}^N \Big( \sum\limits_{j=1}^D x_{j}^2 +
 *   \sum\limits_{j=1}^D c_{ij}^2 -2\sum\limits_{j=1}^D x_{j}c_{ij} \Big)
 *   = \sum\limits_{i=1}^N \sum\limits_{j=1}^D x_{j}^2 +
 *   \sum\limits_{i=1}^N \sum\limits_{j=1}^D c_{ij}^2
 *   -2 \sum\limits_{i=1}^N \sum\limits_{j=1}^D x_{j}c_{ij}
 *   $$
 * </blockquote>
 *
 * where `$x_{j}$` is the `j`-th dimension of the point `X` and
 * `$c_{ij}$` is the `j`-th dimension of the `i`-th point in cluster `$\Gamma$`.
 *
 * Then, the first term of the equation can be rewritten as:
 *
 * <blockquote>
 *   $$
 *   \sum\limits_{i=1}^N \sum\limits_{j=1}^D x_{j}^2 = N \xi_{X} \text{ ,
 *   with } \xi_{X} = \sum\limits_{j=1}^D x_{j}^2
 *   $$
 * </blockquote>
 *
 * where `$\xi_{X}$` is fixed for each point and it can be precomputed.
 *
 * Moreover, the second term is fixed for each cluster too,
 * thus we can name it `$\Psi_{\Gamma}$`
 *
 * <blockquote>
 *   $$
 *   \sum\limits_{i=1}^N \sum\limits_{j=1}^D c_{ij}^2 =
 *   \sum\limits_{i=1}^N \xi_{C_{i}} = \Psi_{\Gamma}
 *   $$
 * </blockquote>
 *
 * Last, the third element becomes
 *
 * <blockquote>
 *   $$
 *   \sum\limits_{i=1}^N \sum\limits_{j=1}^D x_{j}c_{ij} =
 *   \sum\limits_{j=1}^D \Big(\sum\limits_{i=1}^N c_{ij} \Big) x_{j}
 *   $$
 * </blockquote>
 *
 * thus defining the vector
 *
 * <blockquote>
 *   $$
 *   Y_{\Gamma}:Y_{\Gamma j} = \sum\limits_{i=1}^N c_{ij} , j=0, ..., D
 *   $$
 * </blockquote>
 *
 * which is fixed for each cluster `$\Gamma$`, we have
 *
 * <blockquote>
 *   $$
 *   \sum\limits_{j=1}^D \Big(\sum\limits_{i=1}^N c_{ij} \Big) x_{j} =
 *   \sum\limits_{j=1}^D Y_{\Gamma j} x_{j}
 *   $$
 * </blockquote>
 *
 * In this way, the previous equation becomes
 *
 * <blockquote>
 *   $$
 *   N\xi_{X} + \Psi_{\Gamma} - 2 \sum\limits_{j=1}^D Y_{\Gamma j} x_{j}
 *   $$
 * </blockquote>
 *
 * and the average distance of a point to a cluster can be computed as
 *
 * <blockquote>
 *   $$
 *   \frac{\sum\limits_{i=1}^N d(X, C_{i} )}{N} =
 *   \frac{N\xi_{X} + \Psi_{\Gamma} - 2 \sum\limits_{j=1}^D Y_{\Gamma j} x_{j}}{N} =
 *   \xi_{X} + \frac{\Psi_{\Gamma} }{N} - 2 \frac{\sum\limits_{j=1}^D Y_{\Gamma j} x_{j}}{N}
 *   $$
 * </blockquote>
 *
 * Thus, it is enough to precompute: the constant `$\xi_{X}$` for each point `X`; the
 * constants `$\Psi_{\Gamma}$`, `N` and the vector `$Y_{\Gamma}$` for
 * each cluster `$\Gamma$`.
 *
 * In the implementation, the precomputed values for the clusters
 * are distributed among the worker nodes via broadcasted variables,
 * because we can assume that the clusters are limited in number and
 * anyway they are much fewer than the points.
 *
 * The main strengths of this algorithm are the low computational complexity
 * and the intrinsic parallelism. The precomputed information for each point
 * and for each cluster can be computed with a computational complexity
 * which is `O(N/W)`, where `N` is the number of points in the dataset and
 * `W` is the number of worker nodes. After that, every point can be
 * analyzed independently of the others.
 *
 * For every point we need to compute the average distance to all the clusters.
 * Since the formula above requires `O(D)` operations, this phase has a
 * computational complexity which is `O(C*D*N/W)` where `C` is the number of
 * clusters (which we assume quite low), `D` is the number of dimensions,
 * `N` is the number of points in the dataset and `W` is the number
 * of worker nodes.
 */
private[evaluation] object SquaredEuclideanSilhouette extends Silhouette {

  private[this] var kryoRegistrationPerformed: Boolean = false

  /**
   * This method registers the class
   * [[org.apache.spark.ml.evaluation.SquaredEuclideanSilhouette.ClusterStats]]
   * for kryo serialization.
   *
   * @param sc `SparkContext` to be used
   */
  def registerKryoClasses(sc: SparkContext): Unit = {
    if (!kryoRegistrationPerformed) {
      sc.getConf.registerKryoClasses(
        Array(
          classOf[SquaredEuclideanSilhouette.ClusterStats]
        )
      )
      kryoRegistrationPerformed = true
    }
  }

  case class ClusterStats(featureSum: Vector, squaredNormSum: Double, weightSum: Double)

  /**
   * The method takes the input dataset and computes the aggregated values
   * about a cluster which are needed by the algorithm.
   *
   * @param df The DataFrame which contains the input data
   * @param predictionCol The name of the column which contains the predicted cluster id
   *                      for the point.
   * @param featuresCol The name of the column which contains the feature vector of the point.
   * @param weightCol The name of the column which contains the instance weight.
   * @return A [[scala.collection.immutable.Map]] which associates each cluster id
   *         to a [[ClusterStats]] object (which contains the precomputed values `N`,
   *         `$\Psi_{\Gamma}$` and `$Y_{\Gamma}$` for a cluster).
   */
  def computeClusterStats(
      df: DataFrame,
      predictionCol: String,
      featuresCol: String,
      weightCol: String): Map[Double, ClusterStats] = {
    val numFeatures = MetadataUtils.getNumFeatures(df, featuresCol)
    val clustersStatsRDD = df.select(
      col(predictionCol).cast(DoubleType), col(featuresCol), col("squaredNorm"), col(weightCol))
      .rdd
      .map { row => (row.getDouble(0), (row.getAs[Vector](1), row.getDouble(2), row.getDouble(3))) }
      .aggregateByKey
      [(DenseVector, Double, Double)]((Vectors.zeros(numFeatures).toDense, 0.0, 0.0))(
        seqOp = {
          case (
            (featureSum: DenseVector, squaredNormSum: Double, weightSum: Double),
            (features, squaredNorm, weight)
            ) =>
            BLAS.axpy(weight, features, featureSum)
            (featureSum, squaredNormSum + squaredNorm * weight, weightSum + weight)
        },
        combOp = {
          case (
            (featureSum1, squaredNormSum1, weightSum1),
            (featureSum2, squaredNormSum2, weightSum2)
            ) =>
            BLAS.axpy(1.0, featureSum2, featureSum1)
            (featureSum1, squaredNormSum1 + squaredNormSum2, weightSum1 + weightSum2)
        }
      )

    clustersStatsRDD
      .collectAsMap()
      .mapValues {
        case (featureSum: DenseVector, squaredNormSum: Double, weightSum: Double) =>
          SquaredEuclideanSilhouette.ClusterStats(featureSum, squaredNormSum, weightSum)
      }
      .toMap
  }

  /**
   * It computes the Silhouette coefficient for a point.
   *
   * @param broadcastedClustersMap A map of the precomputed values for each cluster.
   * @param point The [[org.apache.spark.ml.linalg.Vector]] representing the current point.
   * @param clusterId The id of the cluster the current point belongs to.
   * @param weight The instance weight of the current point.
   * @param squaredNorm The `$\Xi_{X}$` (which is the squared norm) precomputed for the point.
   * @return The Silhouette for the point.
   */
  def computeSilhouetteCoefficient(
      broadcastedClustersMap: Broadcast[Map[Double, ClusterStats]],
      point: Vector,
      clusterId: Double,
      weight: Double,
      squaredNorm: Double): Double = {

    def compute(targetClusterId: Double): Double = {
      val clusterStats = broadcastedClustersMap.value(targetClusterId)
      val pointDotClusterFeaturesSum = BLAS.dot(point, clusterStats.featureSum)

      squaredNorm +
        clusterStats.squaredNormSum / clusterStats.weightSum -
        2 * pointDotClusterFeaturesSum / clusterStats.weightSum
    }

    pointSilhouetteCoefficient(broadcastedClustersMap.value.keySet,
      clusterId,
      broadcastedClustersMap.value(clusterId).weightSum,
      weight,
      compute)
  }

  /**
   * Compute the Silhouette score of the dataset using squared Euclidean distance measure.
   *
   * @param dataset The input dataset (previously clustered) on which compute the Silhouette.
   * @param predictionCol The name of the column which contains the predicted cluster id
   *                      for the point.
   * @param featuresCol The name of the column which contains the feature vector of the point.
   * @param weightCol The name of the column which contains instance weight.
   * @return The average of the Silhouette values of the clustered data.
   */
  def computeSilhouetteScore(
      dataset: Dataset[_],
      predictionCol: String,
      featuresCol: String,
      weightCol: String): Double = {
    SquaredEuclideanSilhouette.registerKryoClasses(dataset.sparkSession.sparkContext)

    val squaredNormUDF = udf {
      features: Vector => math.pow(Vectors.norm(features, 2.0), 2.0)
    }
    val dfWithSquaredNorm = dataset.withColumn("squaredNorm", squaredNormUDF(col(featuresCol)))

    // compute aggregate values for clusters needed by the algorithm
    val clustersStatsMap = SquaredEuclideanSilhouette
      .computeClusterStats(dfWithSquaredNorm, predictionCol, featuresCol, weightCol)

    // Silhouette is reasonable only when the number of clusters is greater then 1
    assert(clustersStatsMap.size > 1, "Number of clusters must be greater than one.")

    val bClustersStatsMap = dataset.sparkSession.sparkContext.broadcast(clustersStatsMap)

    val computeSilhouetteCoefficientUDF = udf {
      computeSilhouetteCoefficient(bClustersStatsMap, _: Vector, _: Double, _: Double, _: Double)
    }

    val silhouetteScore = overallScore(dfWithSquaredNorm,
      computeSilhouetteCoefficientUDF(col(featuresCol), col(predictionCol).cast(DoubleType),
        col(weightCol), col("squaredNorm")), col(weightCol))

    bClustersStatsMap.destroy()

    silhouetteScore
  }
}


/**
 * The algorithm which is implemented in this object, instead, is an efficient and parallel
 * implementation of the Silhouette using the cosine distance measure. The cosine distance
 * measure is defined as `1 - s` where `s` is the cosine similarity between two points.
 *
 * The total distance of the point `X` to the points `$C_{i}$` belonging to the cluster `$\Gamma$`
 * is:
 *
 * <blockquote>
 *   $$
 *   \sum\limits_{i=1}^N d(X, C_{i} ) =
 *   \sum\limits_{i=1}^N \Big( 1 - \frac{\sum\limits_{j=1}^D x_{j}c_{ij} }{ \|X\|\|C_{i}\|} \Big)
 *   = \sum\limits_{i=1}^N 1 - \sum\limits_{i=1}^N \sum\limits_{j=1}^D \frac{x_{j}}{\|X\|}
 *   \frac{c_{ij}}{\|C_{i}\|}
 *   = N - \sum\limits_{j=1}^D \frac{x_{j}}{\|X\|} \Big( \sum\limits_{i=1}^N
 *   \frac{c_{ij}}{\|C_{i}\|} \Big)
 *   $$
 * </blockquote>
 *
 * where `$x_{j}$` is the `j`-th dimension of the point `X` and `$c_{ij}$` is the `j`-th dimension
 * of the `i`-th point in cluster `$\Gamma$`.
 *
 * Then, we can define the vector:
 *
 * <blockquote>
 *   $$
 *   \xi_{X} : \xi_{X i} = \frac{x_{i}}{\|X\|}, i = 1, ..., D
 *   $$
 * </blockquote>
 *
 * which can be precomputed for each point and the vector
 *
 * <blockquote>
 *   $$
 *   \Omega_{\Gamma} : \Omega_{\Gamma i} = \sum\limits_{j=1}^N \xi_{C_{j}i}, i = 1, ..., D
 *   $$
 * </blockquote>
 *
 * which can be precomputed too for each cluster `$\Gamma$` by its points `$C_{i}$`.
 *
 * With these definitions, the numerator becomes:
 *
 * <blockquote>
 *   $$
 *   N - \sum\limits_{j=1}^D \xi_{X j} \Omega_{\Gamma j}
 *   $$
 * </blockquote>
 *
 * Thus the average distance of a point `X` to the points of the cluster `$\Gamma$` is:
 *
 * <blockquote>
 *   $$
 *   1 - \frac{\sum\limits_{j=1}^D \xi_{X j} \Omega_{\Gamma j}}{N}
 *   $$
 * </blockquote>
 *
 * In the implementation, the precomputed values for the clusters are distributed among the worker
 * nodes via broadcasted variables, because we can assume that the clusters are limited in number.
 *
 * The main strengths of this algorithm are the low computational complexity and the intrinsic
 * parallelism. The precomputed information for each point and for each cluster can be computed
 * with a computational complexity which is `O(N/W)`, where `N` is the number of points in the
 * dataset and `W` is the number of worker nodes. After that, every point can be analyzed
 * independently from the others.
 *
 * For every point we need to compute the average distance to all the clusters. Since the formula
 * above requires `O(D)` operations, this phase has a computational complexity which is
 * `O(C*D*N/W)` where `C` is the number of clusters (which we assume quite low), `D` is the number
 * of dimensions, `N` is the number of points in the dataset and `W` is the number of worker
 * nodes.
 */
private[evaluation] object CosineSilhouette extends Silhouette {

  private[this] val normalizedFeaturesColName = "normalizedFeatures"

  /**
   * The method takes the input dataset and computes the aggregated values
   * about a cluster which are needed by the algorithm.
   *
   * @param df The DataFrame which contains the input data
   * @param featuresCol The name of the column which contains the feature vector of the point.
   * @param predictionCol The name of the column which contains the predicted cluster id
   *                      for the point.
   * @param weightCol The name of the column which contains the instance weight.
   * @return A [[scala.collection.immutable.Map]] which associates each cluster id to a
   *         its statistics (i.e. the precomputed values `N` and `$\Omega_{\Gamma}$`).
   */
  def computeClusterStats(
      df: DataFrame,
      featuresCol: String,
      predictionCol: String,
      weightCol: String): Map[Double, (Vector, Double)] = {
    val numFeatures = MetadataUtils.getNumFeatures(df, featuresCol)
    val clustersStatsRDD = df.select(
      col(predictionCol).cast(DoubleType), col(normalizedFeaturesColName), col(weightCol))
      .rdd
      .map { row => (row.getDouble(0), (row.getAs[Vector](1), row.getDouble(2))) }
      .aggregateByKey[(DenseVector, Double)]((Vectors.zeros(numFeatures).toDense, 0.0))(
      seqOp = {
        case ((normalizedFeaturesSum: DenseVector, weightSum: Double),
        (normalizedFeatures, weight)) =>
          BLAS.axpy(weight, normalizedFeatures, normalizedFeaturesSum)
          (normalizedFeaturesSum, weightSum + weight)
      },
      combOp = {
        case ((normalizedFeaturesSum1, weightSum1), (normalizedFeaturesSum2, weightSum2)) =>
          BLAS.axpy(1.0, normalizedFeaturesSum2, normalizedFeaturesSum1)
          (normalizedFeaturesSum1, weightSum1 + weightSum2)
      }
    )

    clustersStatsRDD
      .collectAsMap()
      .toMap
  }

  /**
   * It computes the Silhouette coefficient for a point.
   *
   * @param broadcastedClustersMap A map of the precomputed values for each cluster.
   * @param normalizedFeatures The [[org.apache.spark.ml.linalg.Vector]] representing the
   *                           normalized features of the current point.
   * @param clusterId The id of the cluster the current point belongs to.
   * @param weight The instance weight of the current point.
   */
  def computeSilhouetteCoefficient(
      broadcastedClustersMap: Broadcast[Map[Double, (Vector, Double)]],
      normalizedFeatures: Vector,
      clusterId: Double,
      weight: Double): Double = {

    def compute(targetClusterId: Double): Double = {
      val (normalizedFeatureSum, numOfPoints) = broadcastedClustersMap.value(targetClusterId)
      1 - BLAS.dot(normalizedFeatures, normalizedFeatureSum) / numOfPoints
    }

    pointSilhouetteCoefficient(broadcastedClustersMap.value.keySet,
      clusterId,
      broadcastedClustersMap.value(clusterId)._2,
      weight,
      compute)
  }

  /**
   * Compute the Silhouette score of the dataset using the cosine distance measure.
   *
   * @param dataset The input dataset (previously clustered) on which compute the Silhouette.
   * @param predictionCol The name of the column which contains the predicted cluster id
   *                      for the point.
   * @param featuresCol The name of the column which contains the feature vector of the point.
   * @param weightCol The name of the column which contains the instance weight.
   * @return The average of the Silhouette values of the clustered data.
   */
  def computeSilhouetteScore(
      dataset: Dataset[_],
      predictionCol: String,
      featuresCol: String,
      weightCol: String): Double = {
    val normalizeFeatureUDF = udf {
      features: Vector => {
        val norm = Vectors.norm(features, 2.0)
        BLAS.scal(1.0 / norm, features)
        features
      }
    }
    val dfWithNormalizedFeatures = dataset.withColumn(normalizedFeaturesColName,
      normalizeFeatureUDF(col(featuresCol)))

    // compute aggregate values for clusters needed by the algorithm
    val clustersStatsMap = computeClusterStats(dfWithNormalizedFeatures, featuresCol,
      predictionCol, weightCol)

    // Silhouette is reasonable only when the number of clusters is greater then 1
    assert(clustersStatsMap.size > 1, "Number of clusters must be greater than one.")

    val bClustersStatsMap = dataset.sparkSession.sparkContext.broadcast(clustersStatsMap)

    val computeSilhouetteCoefficientUDF = udf {
      computeSilhouetteCoefficient(bClustersStatsMap, _: Vector, _: Double, _: Double)
    }

    val silhouetteScore = overallScore(dfWithNormalizedFeatures,
      computeSilhouetteCoefficientUDF(col(normalizedFeaturesColName),
        col(predictionCol).cast(DoubleType), col(weightCol)), col(weightCol))

    bClustersStatsMap.destroy()

    silhouetteScore
  }
}
