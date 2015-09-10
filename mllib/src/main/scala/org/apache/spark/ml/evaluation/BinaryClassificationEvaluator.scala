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
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.{Identifiable, SchemaUtils}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.{Vector, VectorUDT}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.DoubleType

/**
 * :: Experimental ::
 * Evaluator for binary classification, which expects two input columns: rawPrediction and label.
 */
@Experimental
class BinaryClassificationEvaluator(override val uid: String)
  extends Evaluator with HasRawPredictionCol with HasLabelCol {

  def this() = this(Identifiable.randomUID("binEval"))

  /**
   * param for metric name in evaluation
   * Default: areaUnderROC
   * @group param
   */
  val metricName: Param[String] = {
    val allowedParams = ParamValidators.inArray(Array("areaUnderROC", "areaUnderPR"))
    new Param(
      this, "metricName", "metric name in evaluation (areaUnderROC|areaUnderPR)", allowedParams)
  }

  /** @group getParam */
  def getMetricName: String = $(metricName)

  /** @group setParam */
  def setMetricName(value: String): this.type = set(metricName, value)

  /** @group setParam */
  def setRawPredictionCol(value: String): this.type = set(rawPredictionCol, value)

  /**
   * @group setParam
   * @deprecated use [[setRawPredictionCol()]] instead
   */
  @deprecated("use setRawPredictionCol instead", "1.5.0")
  def setScoreCol(value: String): this.type = set(rawPredictionCol, value)

  /** @group setParam */
  def setLabelCol(value: String): this.type = set(labelCol, value)

  setDefault(metricName -> "areaUnderROC")

  override def evaluate(dataset: DataFrame): Double = {
    val schema = dataset.schema
    SchemaUtils.checkColumnType(schema, $(rawPredictionCol), new VectorUDT)
    SchemaUtils.checkColumnType(schema, $(labelCol), DoubleType)

    // TODO: When dataset metadata has been implemented, check rawPredictionCol vector length = 2.
    val scoreAndLabels = dataset.select($(rawPredictionCol), $(labelCol))
      .map { case Row(rawPrediction: Vector, label: Double) =>
        (rawPrediction(1), label)
      }
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val metric = $(metricName) match {
      case "areaUnderROC" => metrics.areaUnderROC()
      case "areaUnderPR" => metrics.areaUnderPR()
    }
    metrics.unpersist()
    metric
  }

  override def isLargerBetter: Boolean = $(metricName) match {
    case "areaUnderROC" => true
    case "areaUnderPR" => true
  }

  override def copy(extra: ParamMap): BinaryClassificationEvaluator = defaultCopy(extra)
}
