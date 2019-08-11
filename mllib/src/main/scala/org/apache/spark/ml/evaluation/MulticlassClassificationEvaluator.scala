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
import org.apache.spark.ml.param.{DoubleParam, Param, ParamMap, ParamValidators}
import org.apache.spark.ml.param.shared.{HasLabelCol, HasPredictionCol, HasWeightCol}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable, SchemaUtils}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

/**
 * :: Experimental ::
 * Evaluator for multiclass classification, which expects two input columns: prediction and label.
 */
@Since("1.5.0")
@Experimental
class MulticlassClassificationEvaluator @Since("1.5.0") (@Since("1.5.0") override val uid: String)
  extends Evaluator with HasPredictionCol with HasLabelCol
    with HasWeightCol with DefaultParamsWritable {

  import MulticlassClassificationEvaluator.supportedMetricNames

  @Since("1.5.0")
  def this() = this(Identifiable.randomUID("mcEval"))

  /**
   * param for metric name in evaluation (supports `"f1"` (default), `"weightedPrecision"`,
   * `"weightedRecall"`, `"accuracy"`)
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

  setDefault(metricName -> "f1")

  /** @group setParam */
  @Since("1.5.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setLabelCol(value: String): this.type = set(labelCol, value)

  /** @group setParam */
  @Since("3.0.0")
  def setWeightCol(value: String): this.type = set(weightCol, value)

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

  setDefault(metricLabel -> 0.0)

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

  setDefault(beta -> 1.0)


  @Since("2.0.0")
  override def evaluate(dataset: Dataset[_]): Double = {
    val schema = dataset.schema
    SchemaUtils.checkColumnType(schema, $(predictionCol), DoubleType)
    SchemaUtils.checkNumericType(schema, $(labelCol))

    val predictionAndLabelsWithWeights =
      dataset.select(col($(predictionCol)), col($(labelCol)).cast(DoubleType),
        if (!isDefined(weightCol) || $(weightCol).isEmpty) lit(1.0) else col($(weightCol)))
        .rdd.map {
        case Row(prediction: Double, label: Double, weight: Double) => (prediction, label, weight)
      }
    val metrics = new MulticlassMetrics(predictionAndLabelsWithWeights)
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
    }
  }

  @Since("1.5.0")
  override def isLargerBetter: Boolean = {
    $(metricName) match {
      case "weightedFalsePositiveRate" => false
      case "falsePositiveRateByLabel" => false
      case _ => true
    }
  }

  @Since("1.5.0")
  override def copy(extra: ParamMap): MulticlassClassificationEvaluator = defaultCopy(extra)
}

@Since("1.6.0")
object MulticlassClassificationEvaluator
  extends DefaultParamsReadable[MulticlassClassificationEvaluator] {

  private val supportedMetricNames = Array("f1", "accuracy", "weightedPrecision", "weightedRecall",
    "weightedTruePositiveRate", "weightedFalsePositiveRate", "weightedFMeasure",
    "truePositiveRateByLabel", "falsePositiveRateByLabel", "precisionByLabel", "recallByLabel",
    "fMeasureByLabel")

  @Since("1.6.0")
  override def load(path: String): MulticlassClassificationEvaluator = super.load(path)
}
