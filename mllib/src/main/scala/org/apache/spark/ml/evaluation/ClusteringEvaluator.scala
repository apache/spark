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
import org.apache.spark.ml.linalg.{Vector, VectorElementWiseSum, VectorUDT}
import org.apache.spark.ml.param.{Param, ParamMap, ParamValidators}
import org.apache.spark.ml.param.shared.{HasFeaturesCol, HasPredictionCol}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable, SchemaUtils}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{avg, col, count, sum}
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

  override def copy(pMap: ParamMap): Evaluator = this.defaultCopy(pMap)

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
        computeSquaredSilhouette(dataset)
    }
    metric
  }

  private[this] def computeSquaredSilhouette(dataset: Dataset[_]): Double = {
    SquaredEuclideanSilhouette.registerKryoClasses(dataset.sparkSession.sparkContext)

    val computeCsi = dataset.sparkSession.udf.register("computeCsi",
      SquaredEuclideanSilhouette.computeCsi _ )
    val dfWithCsi = dataset.withColumn("csi", computeCsi(col($(featuresCol))))

    // compute aggregate values for clusters
    // needed by the algorithm
    val clustersAggregateValues = SquaredEuclideanSilhouette
      .computeYVectorPsiAndCount(dfWithCsi, $(predictionCol), $(featuresCol))

    val clustersMap = clustersAggregateValues.collect().map(row => {
      row.getAs[Int]($(predictionCol)) ->
        SquaredEuclideanSilhouette.ClusterStats(
          row.getAs[Vector]("y"),
          row.getAs[Double]("psi"),
          row.getAs[Long]("count")
        )
    }).toMap

    val broadcastedClustersMap = dataset.sparkSession.sparkContext.broadcast(clustersMap)

    val computeSilhouette = dataset.sparkSession.udf.register("computeSilhouette",
      SquaredEuclideanSilhouette
        .computeSquaredSilhouetteCoefficient(broadcastedClustersMap, _: Vector, _: Int, _: Double)
    )

    val squaredSilhouetteDF = dfWithCsi
      .withColumn("silhouetteCoefficient",
        computeSilhouette(col($(featuresCol)), col($(predictionCol)), col("csi"))
      )
      .agg(avg(col("silhouetteCoefficient")))

    squaredSilhouetteDF.collect()(0).getDouble(0)
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

  case class ClusterStats(Y: Vector, psi: Double, count: Long)

  def computeCsi(vector: Vector): Double = {
    var sumOfSquares = 0.0
    vector.foreachActive((_, v) => {
      sumOfSquares += v * v
    })
    sumOfSquares
  }

  def computeYVectorPsiAndCount(
                                 df: DataFrame,
                                 predictionCol: String,
                                 featuresCol: String): DataFrame = {
    val Yudaf = new VectorElementWiseSum()
    df.groupBy(predictionCol)
      .agg(
        count("*").alias("count"),
        sum("csi").alias("psi"),
        Yudaf(col(featuresCol)).alias("y")
      )
  }

  def computeSquaredSilhouetteCoefficient(
     broadcastedClustersMap: Broadcast[Map[Int, ClusterStats]],
     vector: Vector,
     clusterId: Int,
     csi: Double): Double = {

    def compute(csi: Double, point: Vector, clusterStats: ClusterStats): Double = {
      var YmultiplyPoint = 0.0
      point.foreachActive((idx, v) => {
        YmultiplyPoint += clusterStats.Y(idx) * v
      })

      csi + clusterStats.psi / clusterStats.count - 2 * YmultiplyPoint / clusterStats.count
    }

    var minOther = Double.MaxValue
    for(c <- broadcastedClustersMap.value.keySet) {
      if (c != clusterId) {
        val sil = compute(csi, vector, broadcastedClustersMap.value(c))
        if(sil < minOther) {
          minOther = sil
        }
      }
    }
    val clusterCurrentPoint = broadcastedClustersMap.value(clusterId)
    // adjustment for excluding the node itself from
    // the computation of the average dissimilarity
    val clusterSil = if (clusterCurrentPoint.count == 1) {
      0
    } else {
      compute(csi, vector, clusterCurrentPoint) * clusterCurrentPoint.count /
        (clusterCurrentPoint.count - 1)
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

}
