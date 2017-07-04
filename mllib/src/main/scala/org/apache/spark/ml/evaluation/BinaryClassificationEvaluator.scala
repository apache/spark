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
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable, SchemaUtils}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

/**
 * :: Experimental ::
 * Evaluator for binary classification, which expects two input columns: rawPrediction and label.
 * The rawPrediction column can be of type double (binary 0/1 prediction, or probability of label 1)
 * or of type vector (length-2 vector of raw predictions, scores, or label probabilities).
 */
@Since("1.2.0")
@Experimental
class BinaryClassificationEvaluator @Since("1.4.0") (@Since("1.4.0") override val uid: String)
  extends Evaluator with HasRawPredictionCol with HasLabelCol with DefaultParamsWritable {

  @Since("1.2.0")
  def this() = this(Identifiable.randomUID("binEval"))

  /**
   * param for metric name in evaluation (supports `"areaUnderROC"` (default), `"areaUnderPR"`)
   * @group param
   */
  @Since("1.2.0")
  val metricName: Param[String] = {
    val allowedParams = ParamValidators.inArray(Array("areaUnderROC", "areaUnderPR"))
    new Param(
      this, "metricName", "metric name in evaluation (areaUnderROC|areaUnderPR)", allowedParams)
  }

  /** @group getParam */
  @Since("1.2.0")
  def getMetricName: String = $(metricName)

  /** @group setParam */
  @Since("1.2.0")
  def setMetricName(value: String): this.type = set(metricName, value)

  /** @group setParam */
  @Since("1.5.0")
  def setRawPredictionCol(value: String): this.type = set(rawPredictionCol, value)

  /** @group setParam */
  @Since("1.2.0")
  def setLabelCol(value: String): this.type = set(labelCol, value)

  setDefault(metricName -> "areaUnderROC")

  @Since("2.0.0")
  override def evaluate(dataset: Dataset[_]): Double = {
    val schema = dataset.schema
    SchemaUtils.checkColumnTypes(schema, $(rawPredictionCol), Seq(DoubleType, new VectorUDT))
    SchemaUtils.checkNumericType(schema, $(labelCol))

    // TODO: When dataset metadata has been implemented, check rawPredictionCol vector length = 2.
    val scoreAndLabels =
      dataset.select(col($(rawPredictionCol)), col($(labelCol)).cast(DoubleType)).rdd.map {
        case Row(rawPrediction: Vector, label: Double) => (rawPrediction(1), label)
        case Row(rawPrediction: Double, label: Double) => (rawPrediction, label)
      }
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val metric = $(metricName) match {
      case "areaUnderROC" => metrics.areaUnderROC()
      case "areaUnderPR" => metrics.areaUnderPR()
    }
    metrics.unpersist()
    metric
  }

  @Since("1.5.0")
  override def isLargerBetter: Boolean = $(metricName) match {
    case "areaUnderROC" => true
    case "areaUnderPR" => true
  }

  @Since("1.4.1")
  override def copy(extra: ParamMap): BinaryClassificationEvaluator = defaultCopy(extra)
}

@Since("1.6.0")
object BinaryClassificationEvaluator extends DefaultParamsReadable[BinaryClassificationEvaluator] {

  @Since("1.6.0")
  override def load(path: String): BinaryClassificationEvaluator = super.load(path)
}
