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

import scala.reflect.ClassTag

import org.apache.spark.SparkContext
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.param.{IntParam, Param, ParamMap, ParamValidators}
import org.apache.spark.ml.param.shared.{HasLabelCol, HasPredictionCol}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}
import org.apache.spark.sql.functions._

/**
 * :: Experimental ::
 * Evaluator for ranking, which expects two input columns: prediction and label.
 */
@Since("2.0.0")
@Experimental
final class RankingEvaluator[T: ClassTag] @Since("2.0.0") (@Since("2.0.0") override val uid: String)
  extends Evaluator with HasPredictionCol with HasLabelCol with DefaultParamsWritable {

  @Since("2.0.0")
  def this() = this(Identifiable.randomUID("rankingEval"))

  @Since("2.0.0")
  final val k = new IntParam(this, "k", "Top-K cutoff", (x: Int) => x > 0)

  /** @group getParam */
  @Since("2.0.0")
  def getK: Int = $(k)

  /** @group setParam */
  @Since("2.0.0")
  def setK(value: Int): this.type = set(k, value)

  setDefault(k -> 1)

  /**
   * Param for metric name in evaluation. Supports:
   *  - `"map"` (default): Mean Average Precision
   *  - `"mapk"`: Mean Average Precision@K
   *  - `"ndcg"`: Normalized Discounted Cumulative Gain
   *  - `"mrr"`: Mean Reciprocal Rank
   *
   * @group param
   */
  @Since("2.0.0")
  val metricName: Param[String] = {
    val allowedParams = ParamValidators.inArray(Array("map", "mapk", "ndcg", "mrr"))
    new Param(this, "metricName", "metric name in evaluation (map|mapk|ndcg||mrr)", allowedParams)
  }

  /** @group getParam */
  @Since("2.0.0")
  def getMetricName: String = $(metricName)

  /** @group setParam */
  @Since("2.0.0")
  def setMetricName(value: String): this.type = set(metricName, value)

  /** @group setParam */
  @Since("2.0.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  @Since("2.0.0")
  def setLabelCol(value: String): this.type = set(labelCol, value)

  setDefault(metricName -> "map")

  @Since("2.0.0")
  override def evaluate(dataset: Dataset[_]): Double = {
    val schema = dataset.schema
    val predictionColName = $(predictionCol)
    val predictionType = schema($(predictionCol)).dataType
    val labelColName = $(labelCol)
    val labelType = schema($(labelCol)).dataType
    require(predictionType == labelType,
      s"Prediction column $predictionColName and Label column $labelColName " +
        s"must be of the same type, but Prediction column $predictionColName is $predictionType " +
        s"and Label column $labelColName is $labelType")

    val metric = $(metricName) match {
      case "map" => meanAveragePrecision(dataset)
      case "ndcg" => normalizedDiscountedCumulativeGain(dataset)
      case "mapk" => meanAveragePrecisionAtK(dataset)
      case "mrr" => meanReciprocalRank(dataset)
    }
    metric
  }

  /**
   * Returns the mean average precision (MAP) of all the queries.
   * If a query has an empty ground truth set, the average precision will be zero and a log
   * warning is generated.
   */
  private def meanAveragePrecision(dataset: Dataset[_]): Double = {
    val sc = SparkContext.getOrCreate()
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    dataset.map{ case (prediction: Array[T], label: Array[T]) =>
      val labSet = label.toSet

      if (labSet.nonEmpty) {
        var i = 0
        var cnt = 0
        var precSum = 0.0
        val n = prediction.length
        while (i < n) {
          if (labSet.contains(prediction(i))) {
            cnt += 1
            precSum += cnt.toDouble / (i + 1)
          }
          i += 1
        }
        precSum / labSet.size
      } else {
        0.0
      }
    }.reduce{ (a, b) => a + b } / dataset.count
  }

  /**
   * Compute the average NDCG value of all the queries, truncated at ranking position k.
   * The discounted cumulative gain at position k is computed as:
   *    sum,,i=1,,^k^ (2^{relevance of ''i''th item}^ - 1) / log(i + 1),
   * and the NDCG is obtained by dividing the DCG value on the ground truth set. In the current
   * implementation, the relevance value is binary.

   * If a query has an empty ground truth set, zero will be used as ndcg together with
   * a log warning.
   *
   * See the following paper for detail:
   *
   * IR evaluation methods for retrieving highly relevant documents. K. Jarvelin and J. Kekalainen
   *
   * @param k the position to compute the truncated ndcg, must be positive
   * @return the average ndcg at the first k ranking positions
   */
  private def normalizedDiscountedCumulativeGain(dataset: Dataset[_]): Double = {
    val sc = SparkContext.getOrCreate()
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    dataset.map{ case (prediction: Array[T], label: Array[T]) =>
      val labSet = label.toSet

      if (labSet.nonEmpty) {
        val labSetSize = labSet.size
        val n = math.min(math.max(prediction.length, labSetSize), $(k))
        var maxDcg = 0.0
        var dcg = 0.0
        var i = 0
        while (i < n) {
          val gain = 1.0 / math.log(i + 2)
          if (labSet.contains(prediction(i))) {
            dcg += gain
          }
          if (i < labSetSize) {
            maxDcg += gain
          }
          i += 1
        }
        dcg / maxDcg
      } else {
        0.0
      }
    }.reduce{ (a, b) => a + b } / dataset.count
  }

  /**
   * Compute the average precision of all the queries, truncated at ranking position k.
   *
   * If for a query, the ranking algorithm returns n (n < k) results, the precision value will be
   * computed as #(relevant items retrieved) / k. This formula also applies when the size of the
   * ground truth set is less than k.
   *
   * If a query has an empty ground truth set, zero will be used as precision together with
   * a log warning.
   *
   * See the following paper for detail:
   *
   * IR evaluation methods for retrieving highly relevant documents. K. Jarvelin and J. Kekalainen
   *
   * @param k the position to compute the truncated precision, must be positive
   * @return the average precision at the first k ranking positions
   */
  private def meanAveragePrecisionAtK(dataset: Dataset[_]): Double = {
    val sc = SparkContext.getOrCreate()
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    dataset.map{ case (prediction: Array[T], label: Array[T]) =>
      val labSet = label.toSet

      if (labSet.nonEmpty) {
        val n = math.min(prediction.length, $(k))
        var i = 0
        var cnt = 0
        while (i < n) {
          if (labSet.contains(prediction(i))) {
            cnt += 1
          }
          i += 1
        }
        cnt.toDouble / $(k)
      } else {
        0.0
      }
    }.reduce{ (a, b) => a + b } / dataset.count
  }

  private def meanReciprocalRank(dataset: Dataset[_]): Double = {
    val sc = SparkContext.getOrCreate()
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    dataset.map{ case (prediction: Array[T], label: Array[T]) =>
      val labSet = label.toSet

      if (labSet.nonEmpty) {
        var i = 0
        var reciprocalRank = 0.0
        while (i < prediction.length && reciprocalRank == 0.0) {
          if (labSet.contains(prediction(i))) {
            reciprocalRank = 1.0 / (i + 1)
          }
          i += 1
        }
        reciprocalRank
      } else {
        0.0
      }
    }.reduce{ (a, b) => a + b } / dataset.count
  }

  @Since("2.0.0")
  override def isLargerBetter: Boolean = $(metricName) match {
    case "map" => false
    case "ndcg" => false
    case "mapk" => false
    case "mrr" => false
  }

  @Since("2.0.0")
  override def copy(extra: ParamMap): RankingEvaluator[T] = defaultCopy(extra)
}

@Since("2.0.0")
object RankingEvaluator extends DefaultParamsReadable[RankingEvaluator[_]] {

  @Since("2.0.0")
  override def load(path: String): RankingEvaluator[_] = super.load(path)
}
