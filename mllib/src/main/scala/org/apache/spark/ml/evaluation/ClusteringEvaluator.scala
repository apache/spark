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
import org.apache.spark.annotation.Experimental
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.linalg.{BLAS, DenseVector, Vector, Vectors, VectorUDT}
import org.apache.spark.ml.param.{Param, ParamMap, ParamValidators}
import org.apache.spark.ml.param.shared.{HasFeaturesCol, HasPredictionCol}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable, SchemaUtils}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{avg, col, udf}
import org.apache.spark.sql.types.IntegerType

/**
 * Evaluator for clustering results.
 * At the moment, the supported metrics are:
 *  squaredSilhouette: silhouette measure using the squared Euclidean distance;
 *  cosineSilhouette: silhouette measure using the cosine distance.
 *  The implementation follows the proposal explained
 * <a href="https://drive.google.com/file/d/0B0Hyo%5f%5fbG%5f3fdkNvSVNYX2E3ZU0/view">
 *   in this document</a>.
 */
@Experimental
class ClusteringEvaluator (val uid: String)
  extends Evaluator with HasPredictionCol with HasFeaturesCol with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("SquaredEuclideanSilhouette"))

  override def copy(pMap: ParamMap): ClusteringEvaluator = this.defaultCopy(pMap)

  override def isLargerBetter: Boolean = true

  /** @group setParam */
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /**
   * param for metric name in evaluation
   * (supports `"squaredSilhouette"` (default))
   * @group param
   */
  val metricName: Param[String] = {
    val allowedParams = ParamValidators.inArray(Array("squaredSilhouette"))
    new Param(
      this,
      "metricName",
      "metric name in evaluation (squaredSilhouette)",
      allowedParams
    )
  }

  /** @group getParam */
  def getMetricName: String = $(metricName)

  /** @group setParam */
  def setMetricName(value: String): this.type = set(metricName, value)

  setDefault(metricName -> "squaredSilhouette")

  override def evaluate(dataset: Dataset[_]): Double = {
    SchemaUtils.checkColumnType(dataset.schema, $(featuresCol), new VectorUDT)
    SchemaUtils.checkColumnType(dataset.schema, $(predictionCol), IntegerType)

    val metric: Double = $(metricName) match {
      case "squaredSilhouette" =>
        SquaredEuclideanSilhouette.computeSquaredSilhouette(
          dataset,
          $(predictionCol),
          $(featuresCol)
        )
    }
    metric
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

  def computeSquaredSilhouetteCoefficient(
     broadcastedClustersMap: Broadcast[Map[Int, ClusterStats]],
     vector: Vector,
     clusterId: Int,
     squaredNorm: Double): Double = {

    def compute(squaredNorm: Double, point: Vector, clusterStats: ClusterStats): Double = {
      val pointDotClusterFeaturesSum = BLAS.dot(point, clusterStats.featureSum)

      squaredNorm +
        clusterStats.squaredNormSum / clusterStats.numOfPoints -
        2 * pointDotClusterFeaturesSum / clusterStats.numOfPoints
    }

    var minOther = Double.MaxValue
    for(c <- broadcastedClustersMap.value.keySet) {
      if (c != clusterId) {
        val sil = compute(squaredNorm, vector, broadcastedClustersMap.value(c))
        if(sil < minOther) {
          minOther = sil
        }
      }
    }
    val clusterCurrentPoint = broadcastedClustersMap.value(clusterId)
    // adjustment for excluding the node itself from
    // the computation of the average dissimilarity
    val clusterSil = if (clusterCurrentPoint.numOfPoints == 1) {
      0
    } else {
      compute(squaredNorm, vector, clusterCurrentPoint) * clusterCurrentPoint.numOfPoints /
        (clusterCurrentPoint.numOfPoints - 1)
    }

    var silhouetteCoeff = 0.0
    if (clusterSil < minOther) {
      silhouetteCoeff = 1 - (clusterSil / minOther)
    } else {
      if (clusterSil > minOther) {
        silhouetteCoeff = (minOther / clusterSil) - 1
      }
    }
    silhouetteCoeff

  }

  def computeSquaredSilhouette(dataset: Dataset[_],
    predictionCol: String,
    featuresCol: String): Double = {
    SquaredEuclideanSilhouette.registerKryoClasses(dataset.sparkSession.sparkContext)

    val squaredNorm = udf {
      features: Vector =>
        math.pow(Vectors.norm(features, 2.0), 2.0)
    }
    val dfWithSquaredNorm = dataset.withColumn("squaredNorm", squaredNorm(col(featuresCol)))

    // compute aggregate values for clusters
    // needed by the algorithm
    val clustersStatsMap = SquaredEuclideanSilhouette
      .computeClusterStats(dfWithSquaredNorm, predictionCol, featuresCol)

    val bClustersStatsMap = dataset.sparkSession.sparkContext.broadcast(clustersStatsMap)

    val computeSilhouette = dataset.sparkSession.udf.register("computeSilhouette",
      computeSquaredSilhouetteCoefficient(bClustersStatsMap, _: Vector, _: Int, _: Double)
    )

    val squaredSilhouetteDF = dfWithSquaredNorm
      .withColumn("silhouetteCoefficient",
        computeSilhouette(col(featuresCol), col(predictionCol), col("squaredNorm"))
      )
      .agg(avg(col("silhouetteCoefficient")))

    squaredSilhouetteDF.collect()(0).getDouble(0)
  }

}
