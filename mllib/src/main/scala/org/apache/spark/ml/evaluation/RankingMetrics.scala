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
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{mean, sum}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.DoubleType

@Since("2.2.0")
class RankingMetrics(
  predictionAndObservations: DataFrame, predictionCol: String, labelCol: String)
  extends Logging with Serializable {

  /**
   * Compute the Mean Percentile Rank (MPR) of all the queries.
   *
   * See the following paper for detail ("Expected percentile rank" in the paper):
   * Hu, Y., Y. Koren, and C. Volinsky. “Collaborative Filtering for Implicit Feedback Datasets.”
   * In 2008 Eighth IEEE International Conference on Data Mining, 263–72, 2008.
   * doi:10.1109/ICDM.2008.22.
   *
   * @return the mean percentile rank
   */
  lazy val meanPercentileRank: Double = {

    def rank = udf((predicted: Seq[Any], actual: Any) => {
      val l_i = predicted.indexOf(actual)

      if (l_i == -1) {
        1
      } else {
        l_i.toDouble / predicted.size
      }
    }, DoubleType)

    val R_prime = predictionAndObservations.count()
    val predictionColumn: Column = predictionAndObservations.col(predictionCol)
    val labelColumn: Column = predictionAndObservations.col(labelCol)

    val rankSum: Double = predictionAndObservations
      .withColumn("rank", rank(predictionColumn, labelColumn))
      .agg(sum("rank")).first().getDouble(0)

    rankSum / R_prime
  }

  /**
   * Compute the average precision of all the queries, truncated at ranking position k.
   *
   * If for a query, the ranking algorithm returns n (n is less than k) results, the precision
   * value will be computed as #(relevant items retrieved) / k. This formula also applies when
   * the size of the ground truth set is less than k.
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
  @Since("2.2.0")
  def precisionAt(k: Int): Double = {
    require(k > 0, "ranking position k should be positive")

    def precisionAtK = udf((predicted: Seq[Any], actual: Seq[Any]) => {
      val actualSet = actual.toSet
      if (actualSet.nonEmpty) {
        val n = math.min(predicted.length, k)
        var i = 0
        var cnt = 0
        while (i < n) {
          if (actualSet.contains(predicted(i))) {
            cnt += 1
          }
          i += 1
        }
        cnt.toDouble / k
      } else {
        logWarning("Empty ground truth set, check input data")
        0.0
      }
    }, DoubleType)

    val predictionColumn: Column = predictionAndObservations.col(predictionCol)
    val labelColumn: Column = predictionAndObservations.col(labelCol)

    predictionAndObservations
      .withColumn("predictionAtK", precisionAtK(predictionColumn, labelColumn))
      .agg(mean("predictionAtK")).first().getDouble(0)
  }

  /**
   * Returns the mean average precision (MAP) of all the queries.
   * If a query has an empty ground truth set, the average precision will be zero and a log
   * warning is generated.
   */
  lazy val meanAveragePrecision: Double = {

    def map = udf((predicted: Seq[Any], actual: Seq[Any]) => {
      val actualSet = actual.toSet
      if (actualSet.nonEmpty) {
        var i = 0
        var cnt = 0
        var precSum = 0.0
        val n = predicted.length
        while (i < n) {
          if (actualSet.contains(predicted(i))) {
            cnt += 1
            precSum += cnt.toDouble / (i + 1)
          }
          i += 1
        }
        precSum / actualSet.size
      } else {
        logWarning("Empty ground truth set, check input data")
        0.0
      }
    }, DoubleType)

    val predictionColumn: Column = predictionAndObservations.col(predictionCol)
    val labelColumn: Column = predictionAndObservations.col(labelCol)

    predictionAndObservations
      .withColumn("MAP", map(predictionColumn, labelColumn))
      .agg(mean("MAP")).first().getDouble(0)
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
  @Since("2.2.0")
  def ndcgAt(k: Int): Double = {
    require(k > 0, "ranking position k should be positive")

    def ndcgAtK = udf((predicted: Seq[Any], actual: Seq[Any]) => {
      val actualSet = actual.toSet

      if (actualSet.nonEmpty) {
        val labSetSize = actualSet.size
        val n = math.min(math.max(predicted.length, labSetSize), k)
        var maxDcg = 0.0
        var dcg = 0.0
        var i = 0
        while (i < n) {
          val gain = 1.0 / math.log(i + 2)
          if (i < predicted.length && actualSet.contains(predicted(i))) {
            dcg += gain
          }
          if (i < labSetSize) {
            maxDcg += gain
          }
          i += 1
        }
        dcg / maxDcg
      } else {
        logWarning("Empty ground truth set, check input data")
        0.0
      }
    }, DoubleType)

    val predictionColumn: Column = predictionAndObservations.col(predictionCol)
    val labelColumn: Column = predictionAndObservations.col(labelCol)

    predictionAndObservations
      .withColumn("ndcgAtK", ndcgAtK(predictionColumn, labelColumn))
      .agg(mean("ndcgAtK")).first().getDouble(0)
  }
}
