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

import scala.reflect.ClassTag

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.internal.Logging
import org.apache.spark.ml.param.{IntParam, Param, ParamMap, ParamValidators}
import org.apache.spark.ml.param.shared.{HasLabelCol, HasPredictionCol}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.mllib.evaluation.RankingMetrics
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}
import org.apache.spark.sql.functions._

/**
 * :: Experimental ::
 * Evaluator for ranking, which expects two input columns: prediction and label.
 * Both prediction and label columns need to be instances of Array[T] where T is the ClassTag.
 */
@Since("2.0.0")
@Experimental
final class RankingEvaluator[T: ClassTag] @Since("2.0.0") (@Since("2.0.0") override val uid: String)
  extends Evaluator with HasPredictionCol with HasLabelCol with DefaultParamsWritable with Logging {

  @Since("2.0.0")
  def this() = this(Identifiable.randomUID("rankingEval"))

  @Since("2.0.0")
  final val k = new IntParam(this, "k", "Top-K cutoff", (x: Int) => x > 0)

  /** @group getParam */
  @Since("2.0.0")
  def getK: Int = $(k)

  /** @group setParam */
  @Since("2.0.0")
  def setK(value: Int): this.type = set(k, value)

  setDefault(k -> 1)

  /**
   * Param for metric name in evaluation. Supports:
   *  - `"map"` (default): Mean Average Precision
   *  - `"mapk"`: Mean Average Precision@K
   *  - `"ndcg"`: Normalized Discounted Cumulative Gain
   *  - `"mrr"`: Mean Reciprocal Rank
   *
   * @group param
   */
  @Since("2.0.0")
  val metricName: Param[String] = {
    val allowedParams = ParamValidators.inArray(Array("map", "mapk", "ndcg", "mrr"))
    new Param(this, "metricName", "metric name in evaluation (map|mapk|ndcg|mrr)", allowedParams)
  }

  /** @group getParam */
  @Since("2.0.0")
  def getMetricName: String = $(metricName)

  /** @group setParam */
  @Since("2.0.0")
  def setMetricName(value: String): this.type = set(metricName, value)

  /** @group setParam */
  @Since("2.0.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  @Since("2.0.0")
  def setLabelCol(value: String): this.type = set(labelCol, value)

  setDefault(metricName -> "map")

  @Since("2.0.0")
  override def evaluate(dataset: Dataset[_]): Double = {
    val schema = dataset.schema
    val predictionColName = $(predictionCol)
    val predictionType = schema($(predictionCol)).dataType
    val labelColName = $(labelCol)
    val labelType = schema($(labelCol)).dataType
    require(predictionType == labelType,
      s"Prediction column $predictionColName and Label column $labelColName " +
        s"must be of the same type, but Prediction column $predictionColName is $predictionType " +
        s"and Label column $labelColName is $labelType")

    val predictionAndLabels = dataset
      .select(col($(predictionCol)).cast(predictionType), col($(labelCol)).cast(labelType))
      .rdd.
      map { case Row(prediction: Seq[T], label: Seq[T]) => (prediction.toArray, label.toArray) }

    val metrics = new RankingMetrics[T](predictionAndLabels)
    val metric = $(metricName) match {
      case "map" => metrics.meanAveragePrecision
      case "ndcg" => metrics.ndcgAt($(k))
      case "mapk" => metrics.precisionAt($(k))
      case "mrr" => metrics.meanReciprocalRank
    }
    metric
  }

  @Since("2.0.0")
  override def isLargerBetter: Boolean = $(metricName) match {
    case "map" => false
    case "ndcg" => false
    case "mapk" => false
    case "mrr" => false
  }

  @Since("2.0.0")
  override def copy(extra: ParamMap): RankingEvaluator[T] = defaultCopy(extra)
}

@Since("2.0.0")
object RankingEvaluator extends DefaultParamsReadable[RankingEvaluator[_]] {

  @Since("2.0.0")
  override def load(path: String): RankingEvaluator[_] = super.load(path)
}
