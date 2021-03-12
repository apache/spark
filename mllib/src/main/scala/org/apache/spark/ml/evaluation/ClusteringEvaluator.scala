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

import org.apache.spark.annotation.Since
import org.apache.spark.ml.functions.checkNonNegativeWeight
import org.apache.spark.ml.param.{Param, ParamMap, ParamValidators}
import org.apache.spark.ml.param.shared.{HasFeaturesCol, HasPredictionCol, HasWeightCol}
import org.apache.spark.ml.util._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

/**
 * Evaluator for clustering results.
 * The metric computes the Silhouette measure using the specified distance measure.
 *
 * The Silhouette is a measure for the validation of the consistency within clusters. It ranges
 * between 1 and -1, where a value close to 1 means that the points in a cluster are close to the
 * other points in the same cluster and far from the points of the other clusters.
 */
@Since("2.3.0")
class ClusteringEvaluator @Since("2.3.0") (@Since("2.3.0") override val uid: String)
  extends Evaluator with HasPredictionCol with HasFeaturesCol with HasWeightCol
    with DefaultParamsWritable {

  @Since("2.3.0")
  def this() = this(Identifiable.randomUID("cluEval"))

  @Since("2.3.0")
  override def copy(pMap: ParamMap): ClusteringEvaluator = this.defaultCopy(pMap)

  @Since("2.3.0")
  override def isLargerBetter: Boolean = true

  /** @group setParam */
  @Since("2.3.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  @Since("2.3.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("3.1.0")
  def setWeightCol(value: String): this.type = set(weightCol, value)

  /**
   * param for metric name in evaluation
   * (supports `"silhouette"` (default))
   * @group param
   */
  @Since("2.3.0")
  val metricName: Param[String] = {
    val allowedParams = ParamValidators.inArray(Array("silhouette"))
    new Param(
      this, "metricName", "metric name in evaluation (silhouette)", allowedParams)
  }

  /** @group getParam */
  @Since("2.3.0")
  def getMetricName: String = $(metricName)

  /** @group setParam */
  @Since("2.3.0")
  def setMetricName(value: String): this.type = set(metricName, value)

  /**
   * param for distance measure to be used in evaluation
   * (supports `"squaredEuclidean"` (default), `"cosine"`)
   * @group param
   */
  @Since("2.4.0")
  val distanceMeasure: Param[String] = {
    val availableValues = Array("squaredEuclidean", "cosine")
    val allowedParams = ParamValidators.inArray(availableValues)
    new Param(this, "distanceMeasure", "distance measure in evaluation. Supported options: " +
      availableValues.mkString("'", "', '", "'"), allowedParams)
  }

  /** @group getParam */
  @Since("2.4.0")
  def getDistanceMeasure: String = $(distanceMeasure)

  /** @group setParam */
  @Since("2.4.0")
  def setDistanceMeasure(value: String): this.type = set(distanceMeasure, value)

  setDefault(metricName -> "silhouette", distanceMeasure -> "squaredEuclidean")

  @Since("2.3.0")
  override def evaluate(dataset: Dataset[_]): Double = {
    val metrics = getMetrics(dataset)

    $(metricName) match {
      case ("silhouette") => metrics.silhouette
      case (other) =>
        throw new IllegalArgumentException(s"No support for metric $other")
    }
  }

  /**
   * Get a ClusteringMetrics, which can be used to get clustering metrics such as
   * silhouette score.
   *
   * @param dataset a dataset that contains labels/observations and predictions.
   * @return ClusteringMetrics
   */
  @Since("3.1.0")
  def getMetrics(dataset: Dataset[_]): ClusteringMetrics = {
    val schema = dataset.schema
    SchemaUtils.validateVectorCompatibleColumn(schema, $(featuresCol))
    SchemaUtils.checkNumericType(schema, $(predictionCol))
    if (isDefined(weightCol)) {
      SchemaUtils.checkNumericType(schema, $(weightCol))
    }

    val weightColName = if (!isDefined(weightCol)) "weightCol" else $(weightCol)

    val vectorCol = DatasetUtils.columnToVector(dataset, $(featuresCol))
    val df = if (!isDefined(weightCol) || $(weightCol).isEmpty) {
      dataset.select(col($(predictionCol)),
        vectorCol.as($(featuresCol), dataset.schema($(featuresCol)).metadata),
        lit(1.0).as(weightColName))
    } else {
      dataset.select(col($(predictionCol)),
        vectorCol.as($(featuresCol), dataset.schema($(featuresCol)).metadata),
        checkNonNegativeWeight(col(weightColName).cast(DoubleType)))
    }

    val metrics = new ClusteringMetrics(df)
    metrics.setDistanceMeasure($(distanceMeasure))
    metrics
  }

  @Since("3.0.0")
  override def toString: String = {
    s"ClusteringEvaluator: uid=$uid, metricName=${$(metricName)}, " +
      s"distanceMeasure=${$(distanceMeasure)}"
  }
}


@Since("2.3.0")
object ClusteringEvaluator
  extends DefaultParamsReadable[ClusteringEvaluator] {

  @Since("2.3.0")
  override def load(path: String): ClusteringEvaluator = super.load(path)

}
