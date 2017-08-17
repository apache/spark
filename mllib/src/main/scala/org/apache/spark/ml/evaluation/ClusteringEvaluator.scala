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
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.linalg.{BLAS, DenseVector, Vector, Vectors, VectorUDT}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasFeaturesCol, HasPredictionCol}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable, SchemaUtils}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{avg, col, udf}
import org.apache.spark.sql.types.IntegerType

/**
 * Evaluator for clustering results.
 * The metric computes the silhouette measure
 * using the squared Euclidean distance.
 * The implementation follows the proposal explained
 * <a href="https://drive.google.com/file/d/0B0Hyo%5f%5fbG%5f3fdkNvSVNYX2E3ZU0/view">
 * in this document</a>.
 */
@Experimental
@Since("2.3.0")
class ClusteringEvaluator (val uid: String)
  extends Evaluator with HasPredictionCol with HasFeaturesCol with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("cluEval"))

  override def copy(pMap: ParamMap): ClusteringEvaluator = this.defaultCopy(pMap)

  override def isLargerBetter: Boolean = true

  /** @group setParam */
  @Since("2.3.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  @Since("2.3.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  @Since("2.3.0")
  override def evaluate(dataset: Dataset[_]): Double = {
    SchemaUtils.checkColumnType(dataset.schema, $(featuresCol), new VectorUDT)
    SchemaUtils.checkColumnType(dataset.schema, $(predictionCol), IntegerType)

    SquaredEuclideanSilhouette.computeSilhouetteScore(
      dataset,
      $(predictionCol),
      $(featuresCol)
    )
  }
}


object ClusteringEvaluator
  extends DefaultParamsReadable[ClusteringEvaluator] {

  override def load(path: String): ClusteringEvaluator = super.load(path)

}

private[evaluation] object SquaredEuclideanSilhouette {

  private[this] var kryoRegistrationPerformed: Boolean = false

  /**
   * This method registers the class
   * [[org.apache.spark.ml.evaluation.SquaredEuclideanSilhouette.ClusterStats]]
   * for kryo serialization.
   *
   * @param sc `SparkContext` to be used
   */
  def registerKryoClasses(sc: SparkContext): Unit = {
    if (! kryoRegistrationPerformed) {
      sc.getConf.registerKryoClasses(
        Array(
          classOf[SquaredEuclideanSilhouette.ClusterStats]
        )
      )
      kryoRegistrationPerformed = true
    }
  }

  case class ClusterStats(featureSum: Vector, squaredNormSum: Double, numOfPoints: Long)

  def computeClusterStats(
    df: DataFrame,
    predictionCol: String,
    featuresCol: String): Map[Int, ClusterStats] = {
    val numFeatures = df.select(col(featuresCol)).first().getAs[Vector](0).size
    val clustersStatsRDD = df.select(col(predictionCol), col(featuresCol), col("squaredNorm"))
      .rdd
      .map { row => (row.getInt(0), (row.getAs[Vector](1), row.getDouble(2))) }
      .aggregateByKey[(DenseVector, Double, Long)]((Vectors.zeros(numFeatures).toDense, 0.0, 0L))(
        seqOp = {
          case (
              (featureSum: DenseVector, squaredNormSum: Double, numOfPoints: Long),
              (features, squaredNorm)
            ) =>
            BLAS.axpy(1.0, features, featureSum)
            (featureSum, squaredNormSum + squaredNorm, numOfPoints + 1)
        },
        combOp = {
          case (
              (featureSum1, squaredNormSum1, numOfPoints1),
              (featureSum2, squaredNormSum2, numOfPoints2)
            ) =>
            BLAS.axpy(1.0, featureSum2, featureSum1)
            (featureSum1, squaredNormSum1 + squaredNormSum2, numOfPoints1 + numOfPoints2)
        }
      )

    clustersStatsRDD
      .collectAsMap()
      .mapValues {
        case (featureSum: DenseVector, squaredNormSum: Double, numOfPoints: Long) =>
          SquaredEuclideanSilhouette.ClusterStats(featureSum, squaredNormSum, numOfPoints)
      }
      .toMap
  }

  def computeSilhouetteCoefficient(
     broadcastedClustersMap: Broadcast[Map[Int, ClusterStats]],
     features: Vector,
     clusterId: Int,
     squaredNorm: Double): Double = {

    def compute(squaredNorm: Double, point: Vector, clusterStats: ClusterStats): Double = {
      val pointDotClusterFeaturesSum = BLAS.dot(point, clusterStats.featureSum)

      squaredNorm +
        clusterStats.squaredNormSum / clusterStats.numOfPoints -
        2 * pointDotClusterFeaturesSum / clusterStats.numOfPoints
    }

    // Here we compute the average dissimilarity of the
    // current point to any cluster of which the point
    // is not a member.
    // The cluster with the lowest average dissimilarity
    // - i.e. the nearest cluster to the current point -
    // is said to be the "neighboring cluster".
    var neighboringClusterDissimilarity = Double.MaxValue
    broadcastedClustersMap.value.keySet.foreach {
      c =>
        if (c != clusterId) {
          val dissimilarity = compute(squaredNorm, features, broadcastedClustersMap.value(c))
          if(dissimilarity < neighboringClusterDissimilarity) {
            neighboringClusterDissimilarity = dissimilarity
          }
        }
    }
    val currentCluster = broadcastedClustersMap.value(clusterId)
    // adjustment for excluding the node itself from
    // the computation of the average dissimilarity
    val currentClusterDissimilarity = if (currentCluster.numOfPoints == 1) {
      0
    } else {
      compute(squaredNorm, features, currentCluster) * currentCluster.numOfPoints /
        (currentCluster.numOfPoints - 1)
    }

    (currentClusterDissimilarity compare neighboringClusterDissimilarity).signum match {
      case -1 => 1 - (currentClusterDissimilarity / neighboringClusterDissimilarity)
      case 1 => (neighboringClusterDissimilarity / currentClusterDissimilarity) - 1
      case 0 => 0.0
    }
  }

  /**
   * Compute the mean Silhouette Coefficient of all samples.
   */
  def computeSilhouetteScore(dataset: Dataset[_],
      predictionCol: String,
      featuresCol: String): Double = {
    SquaredEuclideanSilhouette.registerKryoClasses(dataset.sparkSession.sparkContext)

    val squaredNormUDF = udf {
      features: Vector => math.pow(Vectors.norm(features, 2.0), 2.0)
    }
    val dfWithSquaredNorm = dataset.withColumn("squaredNorm", squaredNormUDF(col(featuresCol)))

    // compute aggregate values for clusters
    // needed by the algorithm
    val clustersStatsMap = SquaredEuclideanSilhouette
      .computeClusterStats(dfWithSquaredNorm, predictionCol, featuresCol)

    val bClustersStatsMap = dataset.sparkSession.sparkContext.broadcast(clustersStatsMap)

    val computeSilhouetteCoefficientUDF = udf {
      computeSilhouetteCoefficient(bClustersStatsMap, _: Vector, _: Int, _: Double)
    }

    dfWithSquaredNorm
      .select(avg(
        computeSilhouetteCoefficientUDF(col(featuresCol), col(predictionCol), col("squaredNorm"))
      ))
      .collect()(0)
      .getDouble(0)
  }
}
