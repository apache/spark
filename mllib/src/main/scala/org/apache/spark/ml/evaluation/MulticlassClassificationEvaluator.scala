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

import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.param.{ParamMap, ParamValidators, Param}
import org.apache.spark.ml.param.shared.{HasLabelCol, HasPredictionCol}
import org.apache.spark.ml.util.{SchemaUtils, Identifiable}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.types.DoubleType

/**
 * :: Experimental ::
 * Evaluator for multiclass classification, which expects two input columns: score and label.
 */
@Experimental
class MulticlassClassificationEvaluator (override val uid: String)
  extends Evaluator with HasPredictionCol with HasLabelCol {

  def this() = this(Identifiable.randomUID("mcEval"))

  /**
   * param for metric name in evaluation (supports `"f1"` (default), `"precision"`, `"recall"`,
   * `"weightedPrecision"`, `"weightedRecall"`)
   * @group param
   */
  val metricName: Param[String] = {
    val allowedParams = ParamValidators.inArray(Array("f1", "precision",
      "recall", "weightedPrecision", "weightedRecall"))
    new Param(this, "metricName", "metric name in evaluation " +
      "(f1|precision|recall|weightedPrecision|weightedRecall)", allowedParams)
  }

  /** @group getParam */
  def getMetricName: String = $(metricName)

  /** @group setParam */
  def setMetricName(value: String): this.type = set(metricName, value)

  /** @group setParam */
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  def setLabelCol(value: String): this.type = set(labelCol, value)

  setDefault(metricName -> "f1")

  override def evaluate(dataset: DataFrame): Double = {
    val schema = dataset.schema
    SchemaUtils.checkColumnType(schema, $(predictionCol), DoubleType)
    SchemaUtils.checkColumnType(schema, $(labelCol), DoubleType)

    val predictionAndLabels = dataset.select($(predictionCol), $(labelCol))
      .map { case Row(prediction: Double, label: Double) =>
      (prediction, label)
    }
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val metric = $(metricName) match {
      case "f1" => metrics.weightedFMeasure
      case "precision" => metrics.precision
      case "recall" => metrics.recall
      case "weightedPrecision" => metrics.weightedPrecision
      case "weightedRecall" => metrics.weightedRecall
    }
    metric
  }

  override def isLargerBetter: Boolean = $(metricName) match {
    case "f1" => true
    case "precision" => true
    case "recall" => true
    case "weightedPrecision" => true
    case "weightedRecall" => true
  }

  override def copy(extra: ParamMap): MulticlassClassificationEvaluator = defaultCopy(extra)
}
