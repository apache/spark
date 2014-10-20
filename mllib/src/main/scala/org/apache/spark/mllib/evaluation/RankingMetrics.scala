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

package org.apache.spark.mllib.evaluation

import scala.reflect.ClassTag

import org.apache.spark.SparkContext._
import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD

/**
 * ::Experimental::
 * Evaluator for ranking algorithms.
 *
 * @param predictionAndLabels an RDD of (predicted ranking, ground truth set) pairs.
 */
@Experimental
class RankingMetrics[T: ClassTag](predictionAndLabels: RDD[(Array[T], Array[T])]) {

  /**
   * Compute the average precision of all the queries, truncated at ranking position k.
   *
   * If for a query, the ranking algorithm returns n (n < k) results,
   * the precision value will be computed as #(relevant items retrived) / k.
   * This formula also applies when the size of the ground truth set is less than k.
   *
   * If a query has an empty ground truth set, one will be returned.
   * See the following paper for detail:
   *
   * IR evaluation methods for retrieving highly relevant documents.
   *    K. Jarvelin and J. Kekalainen
   *
   * @param k the position to compute the truncated precision, must be positive
   * @return the average precision at the first k ranking positions
   */
  def precisionAt(k: Int): Double = {
    require (k > 0,"ranking position k should be positive")
    predictionAndLabels.map { case (pred, lab) =>
      val labSet = lab.toSet
      val n = math.min(pred.length, k)
      var i = 0
      var cnt = 0

      while (i < n) {
        if (labSet.contains(pred(i))) {
          cnt += 1
        }
        i += 1
      }
      if (labSet.size == 0) 1.0 else cnt.toDouble / k
    }.mean
  }

  /**
   * Returns the mean average precision (MAP) of all the queries.
   * If a query has an empty ground truth set, the average precision will be 1.0
   */
  lazy val meanAveragePrecision: Double = {
    predictionAndLabels.map { case (pred, lab) =>
      val labSet = lab.toSet
      val labSetSize = labSet.size
      var i = 0
      var cnt = 0
      var precSum = 0.0
      val n = pred.length

      while (i < n) {
        if (labSet.contains(pred(i))) {
          cnt += 1
          precSum += cnt.toDouble / (i + 1)
        }
        i += 1
      }
      if (labSetSize == 0) 1.0 else precSum / labSet.size
    }.mean
  }

  /**
   * Compute the average NDCG value of all the queries, truncated at ranking position k.
   * The discounted cumulative gain at position k is computed as:
   *    \sum_{i=1}^k (2^{relevance of ith item} - 1) / log(i + 1),
   * and the NDCG is obtained by dividing the DCG value on the ground truth set. In the
   * current implementation, the relevance value is binary.
   *
   * If for a query, the ranking algorithm returns n (n < k) results, the NDCG value at
   * at position n will be used. If the ground truth set contains n (n < k) results,
   * the first n items will be used to compute the DCG value on the ground truth set.
   *
   * If a query has an empty ground truth set, zero will be returned.
   *
   * See the following paper for detail:
   *
   * IR evaluation methods for retrieving highly relevant documents.
   *    K. Jarvelin and J. Kekalainen
   *
   * @param k the position to compute the truncated ndcg, must be positive
   * @return the average ndcg at the first k ranking positions
   */
  def ndcgAt(k: Int): Double = {
    require (k > 0,"ranking position k should be positive")
    predictionAndLabels.map { case (pred, lab) =>
      val labSet = lab.toSet
      val labSetSize = labSet.size
      val n = math.min(math.max(pred.length, labSetSize), k)
      var maxDcg = 0.0
      var dcg = 0.0
      var i = 0

      while (i < n) {
        val gain = 1.0 / math.log(i + 2)
        if (labSet.contains(pred(i))) {
          dcg += gain
        }
        if (i < labSetSize) {
          maxDcg += gain
        }
        i += 1
      }
      if (labSetSize == 0) 0.0 else dcg / maxDcg
    }.mean
  }

}
