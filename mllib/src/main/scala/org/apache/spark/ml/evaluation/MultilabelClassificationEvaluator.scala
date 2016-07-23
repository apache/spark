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
import org.apache.spark.ml.param.{Param, ParamMap, ParamValidators}
import org.apache.spark.ml.param.shared.{HasLabelCol, HasPredictionCol}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.mllib.evaluation.MultilabelMetrics
import org.apache.spark.sql.{DataFrame, Row}

/**
  * :: Experimental ::
  * Evaluator for Multilabel classification, which expects two input columns: score and label.
  */

@Since("1.5.0")
@Experimental
class MultilabelClassificationEvaluator @Since("1.5.0") (@Since("1.5.0") override val uid: String)
  extends HasPredictionCol with HasLabelCol with DefaultParamsWritable {
  @Since("1.5.0")
  def this() = this(Identifiable.randomUID("mlcEval"))

  /**
    * param for metric name in evaluation "f1Measure", "precision",
      "recall", "accuracy", "microF1Measure", "microPrecision", "microRecall",
      "hammingLoss", "fScoreByLabel", "precisionByLabel", "recallByLabel")
    *
    * @group param
    */
  @Since("1.5.0")
  val metricName: Param[String] = {
    val allowedParams = ParamValidators.inArray(Array("f1Measure", "precision",
      "recall", "accuracy", "microF1Measure", "microPrecision", "microRecall",
      "hammingLoss", "fScoreByLabel", "precisionByLabel", "recallByLabel"))
    new Param(this, "metricName", "metric name in evaluation " +
      "(f1Measure|precision|recall|accuracy|microF1Measure|microPrecision|microRecall" +
      "|hammingLoss|fScoreByLabel|precisionByLabel|recallByLabel)", allowedParams)}

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

  setDefault(metricName -> "f1Measure")

  @Since("1.5.0")
  def evaluate(dataset: DataFrame, label: Double = Double.NegativeInfinity): Double = {
    val predictionAndLabels = dataset.select($(predictionCol), $(labelCol)).rdd.map {
      case Row(prediction: Array[Double], labelData: Array[Double]) =>
        (prediction, labelData)
    }
    val metrics = new MultilabelMetrics(predictionAndLabels)
    var metric = 0.0
    metric = $( metricName ) match {
      case "f1Measure" => metrics.f1Measure
      case "precision" => metrics.precision
      case "recall" => metrics.recall
      case "accuracy" => metrics.accuracy
      case "microPrecision" => metrics.microPrecision
      case "microF1Measure" => metrics.microF1Measure
      case "hammingLoss" => metrics.hammingLoss
    }
    if(label!=Double.NegativeInfinity) (
       metric = $( metricName ) match {
         case "fScoreByLabel" => metrics.f1Measure(label)
         case "precisionByLabel" => metrics.precision(label)
         case "recallByLabel" => metrics.recall(label)
         }
      )
    metric
  }

  @Since("1.5.0")
  def isLargerBetter: Boolean = $(metricName) match {
    case "f1Measure" => true
    case "precision" => true
    case "recall" => true
    case "accuracy" => true
    case "microPrecision" => true
    case "microF1Measure" => true
    case "hammingLoss" => false
    case "fScoreByLabel" => true
    case "precisionByLabel" => true
    case "recallByLabel" => true

  }

  @Since("1.5.0")
   def copy(extra: ParamMap): MultilabelClassificationEvaluator = defaultCopy(extra)
}

@Since("1.6.0")
object MultilabelClassificationEvaluator
  extends DefaultParamsReadable[MultilabelClassificationEvaluator] {

  @Since("1.6.0")
  override def load(path: String): MultilabelClassificationEvaluator = super.load(path)
}
