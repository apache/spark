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
import org.apache.spark.ml.param.{BooleanParam, Param, ParamMap, ParamValidators}
import org.apache.spark.ml.param.shared.{HasLabelCol, HasPredictionCol, HasWeightCol}
import org.apache.spark.ml.util._
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, FloatType}

/**
 * Evaluator for regression, which expects input columns prediction, label and
 * an optional weight column.
 */
@Since("1.4.0")
final class RegressionEvaluator @Since("1.4.0") (@Since("1.4.0") override val uid: String)
  extends Evaluator with HasPredictionCol with HasLabelCol
    with HasWeightCol with DefaultParamsWritable {

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("regEval"))

  /**
   * Param for metric name in evaluation. Supports:
   *  - `"rmse"` (default): root mean squared error
   *  - `"mse"`: mean squared error
   *  - `"r2"`: R^2^ metric
   *  - `"mae"`: mean absolute error
   *  - `"var"`: explained variance
   *
   * @group param
   */
  @Since("1.4.0")
  val metricName: Param[String] = {
    val allowedParams = ParamValidators.inArray(Array("mse", "rmse", "r2", "mae", "var"))
    new Param(this, "metricName", "metric name in evaluation (mse|rmse|r2|mae|var)", allowedParams)
  }

  /** @group getParam */
  @Since("1.4.0")
  def getMetricName: String = $(metricName)

  /** @group setParam */
  @Since("1.4.0")
  def setMetricName(value: String): this.type = set(metricName, value)

  /**
   * param for whether the regression is through the origin.
   * Default: false.
   * @group expertParam
   */
  @Since("3.0.0")
  val throughOrigin: BooleanParam = new BooleanParam(this, "throughOrigin",
    "Whether the regression is through the origin.")

  /** @group expertGetParam */
  @Since("3.0.0")
  def getThroughOrigin: Boolean = $(throughOrigin)

  /** @group expertSetParam */
  @Since("3.0.0")
  def setThroughOrigin(value: Boolean): this.type = set(throughOrigin, value)

  /** @group setParam */
  @Since("1.4.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  @Since("1.4.0")
  def setLabelCol(value: String): this.type = set(labelCol, value)

  /** @group setParam */
  @Since("3.0.0")
  def setWeightCol(value: String): this.type = set(weightCol, value)

  setDefault(metricName -> "rmse", throughOrigin -> false)

  @Since("2.0.0")
  override def evaluate(dataset: Dataset[_]): Double = {
    val metrics = getMetrics(dataset)
    $(metricName) match {
      case "rmse" => metrics.rootMeanSquaredError
      case "mse" => metrics.meanSquaredError
      case "r2" => metrics.r2
      case "mae" => metrics.meanAbsoluteError
      case "var" => metrics.explainedVariance
    }
  }

  /**
   * Get a RegressionMetrics, which can be used to get regression
   * metrics such as rootMeanSquaredError, meanSquaredError, etc.
   *
   * @param dataset a dataset that contains labels/observations and predictions.
   * @return RegressionMetrics
   */
  @Since("3.1.0")
  def getMetrics(dataset: Dataset[_]): RegressionMetrics = {
    val schema = dataset.schema
    SchemaUtils.checkColumnTypes(schema, $(predictionCol), Seq(DoubleType, FloatType))
    SchemaUtils.checkNumericType(schema, $(labelCol))

    val predictionAndLabelsWithWeights = dataset
      .select(
        col($(predictionCol)).cast(DoubleType),
        col($(labelCol)).cast(DoubleType),
        DatasetUtils.checkNonNegativeWeights(get(weightCol))
      ).rdd.map { case Row(prediction: Double, label: Double, weight: Double) =>
        (prediction, label, weight)
      }
    new RegressionMetrics(predictionAndLabelsWithWeights, $(throughOrigin))
  }

  @Since("1.4.0")
  override def isLargerBetter: Boolean = $(metricName) match {
    case "r2" | "var" => true
    case _ => false
  }

  @Since("1.5.0")
  override def copy(extra: ParamMap): RegressionEvaluator = defaultCopy(extra)

  @Since("3.0.0")
  override def toString: String = {
    s"RegressionEvaluator: uid=$uid, metricName=${$(metricName)}, " +
      s"throughOrigin=${$(throughOrigin)}"
  }
}

@Since("1.6.0")
object RegressionEvaluator extends DefaultParamsReadable[RegressionEvaluator] {

  @Since("1.6.0")
  override def load(path: String): RegressionEvaluator = super.load(path)
}
