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

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.evaluation.RankingMetrics
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * :: Experimental ::
 * Evaluator for ranking, which expects two input columns: prediction and label.
 */
@Experimental
@Since("3.0.0")
class RankingEvaluator @Since("3.0.0") (@Since("3.0.0") override val uid: String)
  extends Evaluator with HasPredictionCol with HasLabelCol with DefaultParamsWritable {

  import RankingEvaluator.supportedMetricNames

  @Since("3.0.0")
  def this() = this(Identifiable.randomUID("rankEval"))

  /**
   * param for metric name in evaluation (supports `"meanAveragePrecision"` (default),
   * `"meanAveragePrecisionAtK"`, `"precisionAtK"`, `"ndcgAtK"`, `"recallAtK"`)
   * @group param
   */
  @Since("3.0.0")
  final val metricName: Param[String] = {
    val allowedParams = ParamValidators.inArray(supportedMetricNames)
    new Param(this, "metricName", "metric name in evaluation " +
      s"${supportedMetricNames.mkString("(", "|", ")")}", allowedParams)
  }

  /** @group getParam */
  @Since("3.0.0")
  def getMetricName: String = $(metricName)

  /** @group setParam */
  @Since("3.0.0")
  def setMetricName(value: String): this.type = set(metricName, value)

  /**
   * param for ranking position value used in `"meanAveragePrecisionAtK"`, `"precisionAtK"`,
   * `"ndcgAtK"`, `"recallAtK"`. Must be &gt; 0. The default value is 10.
   * @group param
   */
  @Since("3.0.0")
  final val k = new IntParam(this, "k",
    "The ranking position value used in " +
      s"${supportedMetricNames.filter(_.endsWith("AtK")).mkString("(", "|", ")")}  " +
      "Must be > 0. The default value is 10.",
    ParamValidators.gt(0))

  /** @group getParam */
  @Since("3.0.0")
  def getK: Int = $(k)

  /** @group setParam */
  @Since("3.0.0")
  def setK(value: Int): this.type = set(k, value)

  /** @group setParam */
  @Since("3.0.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  @Since("3.0.0")
  def setLabelCol(value: String): this.type = set(labelCol, value)

  setDefault(k -> 10, metricName -> "meanAveragePrecision")

  @Since("3.0.0")
  override def evaluate(dataset: Dataset[_]): Double = {
    val metrics = getMetrics(dataset)
    $(metricName) match {
      case "meanAveragePrecision" => metrics.meanAveragePrecision
      case "meanAveragePrecisionAtK" => metrics.meanAveragePrecisionAt($(k))
      case "precisionAtK" => metrics.precisionAt($(k))
      case "ndcgAtK" => metrics.ndcgAt($(k))
      case "recallAtK" => metrics.recallAt($(k))
    }
  }

  /**
   * Get a RankingMetrics, which can be used to get ranking metrics
   * such as meanAveragePrecision, meanAveragePrecisionAtK, etc.
   *
   * @param dataset a dataset that contains labels/observations and predictions.
   * @return RankingMetrics
   */
  @Since("3.1.0")
  def getMetrics(dataset: Dataset[_]): RankingMetrics[Double] = {
    val schema = dataset.schema
    SchemaUtils.checkColumnTypes(schema, $(predictionCol),
      Seq(ArrayType(DoubleType, false), ArrayType(DoubleType, true)))
    SchemaUtils.checkColumnTypes(schema, $(labelCol),
      Seq(ArrayType(DoubleType, false), ArrayType(DoubleType, true)))

    val predictionAndLabels =
      dataset.select(col($(predictionCol)), col($(labelCol)))
        .rdd.map { row =>
        (row.getSeq[Double](0).toArray, row.getSeq[Double](1).toArray)
      }
    new RankingMetrics[Double](predictionAndLabels)
  }

  @Since("3.0.0")
  override def isLargerBetter: Boolean = true

  @Since("3.0.0")
  override def copy(extra: ParamMap): RankingEvaluator = defaultCopy(extra)

  @Since("3.0.0")
  override def toString: String = {
    s"RankingEvaluator: uid=$uid, metricName=${$(metricName)}, k=${$(k)}"
  }
}


@Since("3.0.0")
object RankingEvaluator extends DefaultParamsReadable[RankingEvaluator] {

  private val supportedMetricNames = Array("meanAveragePrecision",
    "meanAveragePrecisionAtK", "precisionAtK", "ndcgAtK", "recallAtK")

  @Since("3.0.0")
  override def load(path: String): RankingEvaluator = super.load(path)
}
