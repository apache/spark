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
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

/**
 * Evaluator for multiclass classification, which expects input columns: prediction, label,
 * weight (optional) and probability (only for logLoss).
 */
@Since("1.5.0")
class MulticlassClassificationEvaluator @Since("1.5.0") (@Since("1.5.0") override val uid: String)
  extends Evaluator with HasPredictionCol with HasLabelCol with HasWeightCol
    with HasProbabilityCol with DefaultParamsWritable {

  import MulticlassClassificationEvaluator.supportedMetricNames

  @Since("1.5.0")
  def this() = this(Identifiable.randomUID("mcEval"))

  /**
   * param for metric name in evaluation (supports `"f1"` (default), `"accuracy"`,
   * `"weightedPrecision"`, `"weightedRecall"`, `"weightedTruePositiveRate"`,
   * `"weightedFalsePositiveRate"`, `"weightedFMeasure"`, `"truePositiveRateByLabel"`,
   * `"falsePositiveRateByLabel"`, `"precisionByLabel"`, `"recallByLabel"`,
   * `"fMeasureByLabel"`, `"logLoss"`, `"hammingLoss"`)
   *
   * @group param
   */
  @Since("1.5.0")
  val metricName: Param[String] = new Param(this, "metricName",
    s"metric name in evaluation ${supportedMetricNames.mkString("(", "|", ")")}",
    ParamValidators.inArray(supportedMetricNames))

  /** @group getParam */
  @Since("1.5.0")
  def getMetricName: String = $(metricName)

  /** @group setParam */
  @Since("1.5.0")
  def setMetricName(value: String): this.type = set(metricName, value)

  /** @group setParam */
  @Since("1.5.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setLabelCol(value: String): this.type = set(labelCol, value)

  /** @group setParam */
  @Since("3.0.0")
  def setWeightCol(value: String): this.type = set(weightCol, value)

  /** @group setParam */
  @Since("3.0.0")
  def setProbabilityCol(value: String): this.type = set(probabilityCol, value)

  /**
   * The class whose metric will be computed in `"truePositiveRateByLabel"`,
   * `"falsePositiveRateByLabel"`, `"precisionByLabel"`, `"recallByLabel"`,
   * `"fMeasureByLabel"`.
   * Must be greater than or equal to 0. The default value is 0.
   *
   * @group param
   */
  @Since("3.0.0")
  final val metricLabel: DoubleParam = new DoubleParam(this, "metricLabel",
    "The class whose metric will be computed in " +
      s"${supportedMetricNames.filter(_.endsWith("ByLabel")).mkString("(", "|", ")")}. " +
      "Must be >= 0. The default value is 0.",
    ParamValidators.gtEq(0.0))

  /** @group getParam */
  @Since("3.0.0")
  def getMetricLabel: Double = $(metricLabel)

  /** @group setParam */
  @Since("3.0.0")
  def setMetricLabel(value: Double): this.type = set(metricLabel, value)

  /**
   * The beta value, which controls precision vs recall weighting,
   * used in `"weightedFMeasure"`, `"fMeasureByLabel"`.
   * Must be greater than 0. The default value is 1.
   *
   * @group param
   */
  @Since("3.0.0")
  final val beta: DoubleParam = new DoubleParam(this, "beta",
    "The beta value, which controls precision vs recall weighting, " +
      "used in (weightedFMeasure|fMeasureByLabel). Must be > 0. The default value is 1.",
    ParamValidators.gt(0.0))

  /** @group getParam */
  @Since("3.0.0")
  def getBeta: Double = $(beta)

  /** @group setParam */
  @Since("3.0.0")
  def setBeta(value: Double): this.type = set(beta, value)

  /**
   * param for eps. log-loss is undefined for p=0 or p=1, so probabilities are clipped to
   * max(eps, min(1 - eps, p)). Must be in range (0, 0.5). The default value is 1e-15.
   *
   * @group param
   */
  @Since("3.0.0")
  final val eps: DoubleParam = new DoubleParam(this, "eps",
    "log-loss is undefined for p=0 or p=1, so probabilities are clipped to " +
      "max(eps, min(1 - eps, p)).",
    ParamValidators.inRange(0, 0.5, false, false))

  /** @group getParam */
  @Since("3.0.0")
  def getEps: Double = $(eps)

  /** @group setParam */
  @Since("3.0.0")
  def setEps(value: Double): this.type = set(eps, value)

  setDefault(metricName -> "f1", eps -> 1e-15, metricLabel -> 0.0, beta -> 1.0)

  @Since("2.0.0")
  override def evaluate(dataset: Dataset[_]): Double = {
    val metrics = getMetrics(dataset)
    $(metricName) match {
      case "f1" => metrics.weightedFMeasure
      case "accuracy" => metrics.accuracy
      case "weightedPrecision" => metrics.weightedPrecision
      case "weightedRecall" => metrics.weightedRecall
      case "weightedTruePositiveRate" => metrics.weightedTruePositiveRate
      case "weightedFalsePositiveRate" => metrics.weightedFalsePositiveRate
      case "weightedFMeasure" => metrics.weightedFMeasure($(beta))
      case "truePositiveRateByLabel" => metrics.truePositiveRate($(metricLabel))
      case "falsePositiveRateByLabel" => metrics.falsePositiveRate($(metricLabel))
      case "precisionByLabel" => metrics.precision($(metricLabel))
      case "recallByLabel" => metrics.recall($(metricLabel))
      case "fMeasureByLabel" => metrics.fMeasure($(metricLabel), $(beta))
      case "hammingLoss" => metrics.hammingLoss
      case "logLoss" => metrics.logLoss($(eps))
    }
  }

  /**
   * Get a MulticlassMetrics, which can be used to get multiclass classification
   * metrics such as accuracy, weightedPrecision, etc.
   *
   * @param dataset a dataset that contains labels/observations and predictions.
   * @return MulticlassMetrics
   */
  @Since("3.1.0")
  def getMetrics(dataset: Dataset[_]): MulticlassMetrics = {
    val schema = dataset.schema
    SchemaUtils.checkColumnType(schema, $(predictionCol), DoubleType)
    SchemaUtils.checkNumericType(schema, $(labelCol))

    if ($(metricName) == "logLoss") {
      // probabilityCol is only needed to compute logloss
      require(schema.fieldNames.contains($(probabilityCol)),
        "probabilityCol is needed to compute logloss")
    }

    val w = DatasetUtils.checkNonNegativeWeights(get(weightCol))
    val rdd = if (schema.fieldNames.contains($(probabilityCol))) {
      val p = DatasetUtils.columnToVector(dataset, $(probabilityCol))
      dataset.select(col($(predictionCol)), col($(labelCol)).cast(DoubleType), w, p)
        .rdd.map {
        case Row(prediction: Double, label: Double, weight: Double, probability: Vector) =>
          (prediction, label, weight, probability.toArray)
      }
    } else {
      dataset.select(col($(predictionCol)), col($(labelCol)).cast(DoubleType), w)
        .rdd.map {
        case Row(prediction: Double, label: Double, weight: Double) => (prediction, label, weight)
      }
    }

    new MulticlassMetrics(rdd)
  }

  @Since("1.5.0")
  override def isLargerBetter: Boolean = $(metricName) match {
    case "weightedFalsePositiveRate" | "falsePositiveRateByLabel" | "logLoss" | "hammingLoss" =>
      false
    case _ => true
  }

  @Since("1.5.0")
  override def copy(extra: ParamMap): MulticlassClassificationEvaluator = defaultCopy(extra)

  @Since("3.0.0")
  override def toString: String = {
    s"MulticlassClassificationEvaluator: uid=$uid, metricName=${$(metricName)}, " +
      s"metricLabel=${$(metricLabel)}, beta=${$(beta)}, eps=${$(eps)}"
  }
}

@Since("1.6.0")
object MulticlassClassificationEvaluator
  extends DefaultParamsReadable[MulticlassClassificationEvaluator] {

  private val supportedMetricNames = Array("f1", "accuracy", "weightedPrecision", "weightedRecall",
    "weightedTruePositiveRate", "weightedFalsePositiveRate", "weightedFMeasure",
    "truePositiveRateByLabel", "falsePositiveRateByLabel", "precisionByLabel", "recallByLabel",
    "fMeasureByLabel", "logLoss", "hammingLoss")

  @Since("1.6.0")
  override def load(path: String): MulticlassClassificationEvaluator = super.load(path)
}
