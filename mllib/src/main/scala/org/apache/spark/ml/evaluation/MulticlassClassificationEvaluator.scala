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
import org.apache.spark.ml.param.shared.{HasLabelCol, HasPredictionCol}
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
  extends Evaluator with HasPredictionCol with HasLabelCol with DefaultParamsWritable {

  @Since("1.5.0")
  def this() = this(Identifiable.randomUID("mcEval"))

  /**
   * param for specific label value for metric to be used in evaluation (supports
   *  Double values for metric `"f1"`, `"precision"`, `"recall"`, `"tpr"`, `"fpr"`)
   * @group param
   */
  val labelValue: Param[Double] = {
    new DoubleParam(this, "labelValue", "specific labelValue for metric to be used in evaluation")
  }

  /** @group getParam */
  def getLabelValue: Double = $(labelValue)

  /** @group setParam */
  def setLabelValue(labelClass: Double): this.type = set(labelValue, labelClass)

  /**
   * param for metric name in evaluation (supports `"f1"` (default), `"weightedPrecision"`,
   * `"weightedRecall"`, `"accuracy"` and with labelValue `"f1"`, `"precision"`, `"recall"`,
   * `"tpr"`, `"fpr"`)
   * @group param
   */
  @Since("1.5.0")
  val metricName: Param[String] = {

    val allowedParams: String => Boolean = { (p: String) =>
      if (isSet(labelValue)) {
        ParamValidators.inArray(MulticlassClassificationEvaluator.labelOptions)(p)
      } else ParamValidators.inArray(MulticlassClassificationEvaluator.weightedOptions)(p)
    }

    new Param(this, "metricName", "metric name in evaluation without label value " +
    "(f1|weightedPrecision|weightedRecall|accuracy) and with label value " +
    " (f1|precision|recall|tpr|fpr)", allowedParams)
  }

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

  setDefault(metricName -> "f1")

  @Since("2.0.0")
  override def evaluate(dataset: Dataset[_]): Double = {
    val schema = dataset.schema
    SchemaUtils.checkColumnType(schema, $(predictionCol), DoubleType)
    SchemaUtils.checkNumericType(schema, $(labelCol))

    val predictionAndLabels =
      dataset.select(col($(predictionCol)), col($(labelCol)).cast(DoubleType)).rdd.map {
        case Row(prediction: Double, label: Double) => (prediction, label)
      }
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val metric = if (isSet(labelValue)) {
      $(metricName) match {
        case "f1" => metrics.fMeasure(getLabelValue)
        case "precision" => metrics.precision(getLabelValue)
        case "recall" => metrics.recall(getLabelValue)
        case "tpr" => metrics.truePositiveRate(getLabelValue)
        case "fpr" => metrics.falsePositiveRate(getLabelValue)
        case weightedMetric
          if (MulticlassClassificationEvaluator.weightedOptions.contains(weightedMetric)) =>
          throw new IllegalArgumentException(
            s"metricName $weightedMetric cannot be specified when label value is set.")
        case _ => throw new IllegalArgumentException(
          s"metricName parameter given invalid value $metricName.")
      }
    } else {
      $(metricName) match {
        case "f1" => metrics.weightedFMeasure
        case "weightedPrecision" => metrics.weightedPrecision
        case "weightedRecall" => metrics.weightedRecall
        case "accuracy" => metrics.accuracy
        case _ => throw new IllegalArgumentException(
          s"metricName parameter given invalid value $metricName.")
      }
    }
    metric
  }

  @Since("1.5.0")
  override def isLargerBetter: Boolean = {
    // Lower False Positive Rate is better for evaluation
    if (isSet(labelValue) && getMetricName.equals("fpr")) {
      false
    } else {
      true
    }
  }

  @Since("1.5.0")
  override def copy(extra: ParamMap): MulticlassClassificationEvaluator = defaultCopy(extra)
}

@Since("1.6.0")
object MulticlassClassificationEvaluator
  extends DefaultParamsReadable[MulticlassClassificationEvaluator] {

  val weightedOptions: Array[String] = Array("f1", "weightedPrecision",
    "weightedRecall", "accuracy")

  val labelOptions: Array[String] = Array("f1", "precision", "recall", "tpr", "fpr")

  @Since("1.6.0")
  override def load(path: String): MulticlassClassificationEvaluator = super.load(path)
}
