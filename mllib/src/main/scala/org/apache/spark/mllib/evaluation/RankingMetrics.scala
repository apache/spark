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
class RankingMetrics(predictionAndLabels: RDD[(Array[Double], Array[Double])]) {

  /**
   * Returns the precsion@k for each query
   */
  lazy val precAtK: RDD[Array[Double]] = predictionAndLabels.map {case (pred, lab)=>
    val labSet : Set[Double] = lab.toSet
    val n = pred.length
    val topkPrec = Array.fill[Double](n)(.0)
    var (i, cnt) = (0, 0)

    while (i < n) {
      if (labSet.contains(pred(i))) {
        cnt += 1
      }
      topkPrec(i) = cnt.toDouble / (i + 1)
      i += 1
    }
    topkPrec
  }

  /**
   * Returns the average precision for each query
   */
  lazy val avePrec: RDD[Double] = predictionAndLabels.map {case (pred, lab) =>
    val labSet: Set[Double] = lab.toSet
    var (i, cnt, precSum) = (0, 0, .0)
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
  lazy val meanAvePrec: Double = computeMean(avePrec)

  /**
   * Returns the normalized discounted cumulative gain for each query
   */
  lazy val ndcg: RDD[Double] = predictionAndLabels.map {case (pred, lab) =>
    val labSet = lab.toSet
    val n = math.min(pred.length, labSet.size)
    var (maxDcg, dcg, i) = (.0, .0, 0)
    while (i < n) {
      /* Calculate 1/log2(i + 2) */
      val gain = 1.0 / (math.log(i + 2) / math.log(2))
      if (labSet.contains(pred(i))) {
        dcg += gain
      }
      maxDcg += gain
      i += 1
    }
    dcg / maxDcg
  }

  /**
   * Returns the mean NDCG of all the queries
   */
  lazy val meanNdcg: Double = computeMean(ndcg)

  private def computeMean(data: RDD[Double]): Double = {
    val stat = data.aggregate((.0, 0))(
      seqOp = (c, v) => (c, v) match {case ((sum, cnt), a) => (sum + a, cnt + 1)},
      combOp = (c1, c2) => (c1, c2) match {case (x, y) => (x._1 + y._1, x._2 + y._2)}
    )
    stat._1 / stat._2
  }
}
