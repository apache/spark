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
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

/**
 * Evaluator for binary classification, which expects input columns rawPrediction, label and
 *  an optional weight column.
 * The rawPrediction column can be of type double (binary 0/1 prediction, or probability of label 1)
 * or of type vector (length-2 vector of raw predictions, scores, or label probabilities).
 */
@Since("1.2.0")
class BinaryClassificationEvaluator @Since("1.4.0") (@Since("1.4.0") override val uid: String)
  extends Evaluator with HasRawPredictionCol with HasLabelCol
    with HasWeightCol with DefaultParamsWritable {

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

  /**
   * param for number of bins to down-sample the curves (ROC curve, PR curve) in area
   * computation. If 0, no down-sampling will occur.
   * Default: 1000.
   * @group expertParam
   */
  @Since("3.0.0")
  val numBins: IntParam = new IntParam(this, "numBins", "Number of bins to down-sample " +
    "the curves (ROC curve, PR curve) in area computation. If 0, no down-sampling will occur. " +
    "Must be >= 0.",
    ParamValidators.gtEq(0))

  /** @group expertGetParam */
  @Since("3.0.0")
  def getNumBins: Int = $(numBins)

  /** @group expertSetParam */
  @Since("3.0.0")
  def setNumBins(value: Int): this.type = set(numBins, value)

  /** @group setParam */
  @Since("1.5.0")
  def setRawPredictionCol(value: String): this.type = set(rawPredictionCol, value)

  /** @group setParam */
  @Since("1.2.0")
  def setLabelCol(value: String): this.type = set(labelCol, value)

  /** @group setParam */
  @Since("3.0.0")
  def setWeightCol(value: String): this.type = set(weightCol, value)

  setDefault(metricName -> "areaUnderROC", numBins -> 1000)

  @Since("2.0.0")
  override def evaluate(dataset: Dataset[_]): Double = {
    val metrics = getMetrics(dataset)
    val metric = $(metricName) match {
      case "areaUnderROC" => metrics.areaUnderROC()
      case "areaUnderPR" => metrics.areaUnderPR()
    }
    metrics.unpersist()
    metric
  }

  /**
   * Get a BinaryClassificationMetrics, which can be used to get binary classification
   * metrics such as areaUnderROC and areaUnderPR.
   *
   * @param dataset a dataset that contains labels/observations and predictions.
   * @return BinaryClassificationMetrics
   */
  @Since("3.1.0")
  def getMetrics(dataset: Dataset[_]): BinaryClassificationMetrics = {
    val schema = dataset.schema
    SchemaUtils.checkColumnTypes(schema, $(rawPredictionCol), Seq(DoubleType, new VectorUDT))
    SchemaUtils.checkNumericType(schema, $(labelCol))
    if (isDefined(weightCol)) {
      SchemaUtils.checkNumericType(schema, $(weightCol))
    }

    MetadataUtils.getNumFeatures(schema($(rawPredictionCol)))
      .foreach(n => require(n == 2, s"rawPredictionCol vectors must have length=2, but got $n"))

    val scoreAndLabelsWithWeights =
      dataset.select(
        col($(rawPredictionCol)),
        col($(labelCol)).cast(DoubleType),
        DatasetUtils.checkNonNegativeWeights(get(weightCol))
      ).rdd.map {
        case Row(rawPrediction: Vector, label: Double, weight: Double) =>
          (rawPrediction(1), label, weight)
        case Row(rawPrediction: Double, label: Double, weight: Double) =>
          (rawPrediction, label, weight)
      }
    new BinaryClassificationMetrics(scoreAndLabelsWithWeights, $(numBins))
  }

  @Since("1.5.0")
  override def isLargerBetter: Boolean = true

  @Since("1.4.1")
  override def copy(extra: ParamMap): BinaryClassificationEvaluator = defaultCopy(extra)

  @Since("3.0.0")
  override def toString: String = {
    s"BinaryClassificationEvaluator: uid=$uid, metricName=${$(metricName)}, " +
      s"numBins=${$(numBins)}"
  }
}

@Since("1.6.0")
object BinaryClassificationEvaluator extends DefaultParamsReadable[BinaryClassificationEvaluator] {

  @Since("1.6.0")
  override def load(path: String): BinaryClassificationEvaluator = super.load(path)
}
