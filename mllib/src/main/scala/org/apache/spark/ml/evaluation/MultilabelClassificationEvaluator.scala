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
import org.apache.spark.mllib.evaluation.MultilabelMetrics
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


/**
 * :: Experimental ::
 * Evaluator for multi-label classification, which expects two input
 * columns: prediction and label.
 */
@Since("3.0.0")
@Experimental
class MultilabelClassificationEvaluator @Since("3.0.0") (@Since("3.0.0") override val uid: String)
  extends Evaluator with HasPredictionCol with HasLabelCol
    with DefaultParamsWritable {

  import MultilabelClassificationEvaluator.supportedMetricNames

  @Since("3.0.0")
  def this() = this(Identifiable.randomUID("mlcEval"))

  /**
   * param for metric name in evaluation (supports `"f1Measure"` (default), `"subsetAccuracy"`,
   * `"accuracy"`, `"hammingLoss"`, `"precision"`, `"recall"`, `"precisionByLabel"`,
   * `"recallByLabel"`, `"f1MeasureByLabel"`, `"microPrecision"`, `"microRecall"`,
   * `"microF1Measure"`)
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
   * param for the class whose metric will be computed in `"precisionByLabel"`, `"recallByLabel"`,
   * `"f1MeasureByLabel"`.
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
  def setMetricLabel(value: Double): this.type = set(metricLabel, value)

  /** @group setParam */
  @Since("3.0.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  @Since("3.0.0")
  def setLabelCol(value: String): this.type = set(labelCol, value)

  setDefault(metricLabel -> 0.0, metricName -> "f1Measure")

  @Since("3.0.0")
  override def evaluate(dataset: Dataset[_]): Double = {
    val metrics = getMetrics(dataset)
    $(metricName) match {
      case "subsetAccuracy" => metrics.subsetAccuracy
      case "accuracy" => metrics.accuracy
      case "hammingLoss" => metrics.hammingLoss
      case "precision" => metrics.precision
      case "recall" => metrics.recall
      case "f1Measure" => metrics.f1Measure
      case "precisionByLabel" => metrics.precision($(metricLabel))
      case "recallByLabel" => metrics.recall($(metricLabel))
      case "f1MeasureByLabel" => metrics.f1Measure($(metricLabel))
      case "microPrecision" => metrics.microPrecision
      case "microRecall" => metrics.microRecall
      case "microF1Measure" => metrics.microF1Measure
    }
  }

  /**
   * Get a MultilabelMetrics, which can be used to get multilabel classification
   * metrics such as accuracy, precision, precisionByLabel, etc.
   *
   * @param dataset a dataset that contains labels/observations and predictions.
   * @return MultilabelMetrics
   */
  @Since("3.1.0")
  def getMetrics(dataset: Dataset[_]): MultilabelMetrics = {
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
    new MultilabelMetrics(predictionAndLabels)
  }

  @Since("3.0.0")
  override def isLargerBetter: Boolean = {
    $(metricName) match {
      case "hammingLoss" => false
      case _ => true
    }
  }

  @Since("3.0.0")
  override def copy(extra: ParamMap): MultilabelClassificationEvaluator = defaultCopy(extra)

  @Since("3.0.0")
  override def toString: String = {
    s"MultilabelClassificationEvaluator: uid=$uid, metricName=${$(metricName)}, " +
      s"metricLabel=${$(metricLabel)}"
  }
}


@Since("3.0.0")
object MultilabelClassificationEvaluator
  extends DefaultParamsReadable[MultilabelClassificationEvaluator] {

  private val supportedMetricNames: Array[String] = Array("subsetAccuracy",
    "accuracy", "hammingLoss", "precision", "recall", "f1Measure",
    "precisionByLabel", "recallByLabel", "f1MeasureByLabel",
    "microPrecision", "microRecall", "microF1Measure")

  @Since("3.0.0")
  override def load(path: String): MultilabelClassificationEvaluator = super.load(path)
}
