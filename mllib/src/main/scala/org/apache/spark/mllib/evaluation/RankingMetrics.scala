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
   * Returns the precsion@k for each query
   */
  lazy val precAtK: RDD[Array[Double]] = predictionAndLabels.map {case (pred, lab)=>
    val labSet = lab.toSet
    val n = pred.length
    val topKPrec = Array.fill[Double](n)(0.0)
    var (i, cnt) = (0, 0)

    while (i < n) {
      if (labSet.contains(pred(i))) {
        cnt += 1
      }
      topKPrec(i) = cnt.toDouble / (i + 1)
      i += 1
    }
    topKPrec
  }

  /**
   * @param k the position to compute the truncated precision
   * @return the average precision at the first k ranking positions
   */
  def precision(k: Int): Double = precAtK.map {topKPrec =>
    val n = topKPrec.length
    if (k <= n) {
      topKPrec(k - 1)
    } else {
      topKPrec(n - 1) * n / k
    }
  }.mean

  /**
   * Returns the average precision for each query
   */
  lazy val avePrec: RDD[Double] = predictionAndLabels.map {case (pred, lab) =>
    val labSet = lab.toSet
    var (i, cnt, precSum) = (0, 0, 0.0)
    val n = pred.length

    while (i < n) {
      if (labSet.contains(pred(i))) {
        cnt += 1
        precSum += cnt.toDouble / (i + 1)
      }
      i += 1
    }
    precSum / labSet.size
  }

  /**
   * Returns the mean average precision (MAP) of all the queries
   */
  lazy val meanAvePrec: Double = avePrec.mean

  /**
   * Returns the normalized discounted cumulative gain for each query
   */
  lazy val ndcgAtK: RDD[Array[Double]] = predictionAndLabels.map {case (pred, lab) =>
    val labSet = lab.toSet
    val labSetSize = labSet.size
    val n = math.max(pred.length, labSetSize)
    val topKNdcg = Array.fill[Double](n)(0.0)
    var (maxDcg, dcg, i) = (0.0, 0.0, 0)
    while (i < n) {
      /* Calculate 1/log2(i + 2) */
      val gain = math.log(2) / math.log(i + 2)
      if (labSet.contains(pred(i))) {
        dcg += gain
      }
      if (i < labSetSize) {
        maxDcg += gain
      }
      topKNdcg(i) = dcg / maxDcg
      i += 1
    }
    topKNdcg
  }

  /**
   * @param k the position to compute the truncated ndcg
   * @return the average ndcg at the first k ranking positions
   */
  def ndcg(k: Int): Double = ndcgAtK.map {topKNdcg =>
    val pos = math.min(k, topKNdcg.length) - 1
    topKNdcg(pos)
  }.mean

}
