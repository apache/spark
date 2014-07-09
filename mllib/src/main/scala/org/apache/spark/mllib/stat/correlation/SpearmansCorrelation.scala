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

package org.apache.spark.mllib.stat.correlation

import org.apache.spark.SparkException
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD

/**
 * Compute Spearman's correlation for two RDDs of the type RDD[Double] or the correlation matrix
 * for an RDD of the type RDD[Vector].
 *
 * Definition of Spearman's correlation can be found at
 * http://en.wikipedia.org/wiki/Spearman's_rank_correlation_coefficient
 */
object SpearmansCorrelation extends Correlation {

  /**
   * Compute Spearman's correlation for two datasets.
   */
  override def computeCorrelation(x: RDD[Double], y: RDD[Double]): Double = {
    val rankPairs = makeRankPairs(getRanks(x), getRanks(y))
    computeCorrelationFromRanks(rankPairs)
  }

  /**
   * Compute Spearman's correlation matrix S, for the input matrix.
   *
   * S(i, j) = computeCorrelationMatrix(columnI, columnJ),
   * where columnI and columnJ are the ith and jth column in the input matrix.
   *
   * TODO support for sparse column vectors
   */
  override def computeCorrelationMatrix(X: RDD[Vector]): Matrix = {
    val indexed = X.zipWithIndex()
    // Attempt to checkpoint the RDD before splitting it into numCols RDD[Double]s to avoid
    // computing the lineage prefix multiple times.
    // If checkpoint directory not set, cache the RDD instead.
    try {
      indexed.checkpoint()
    } catch {
      case e: Exception => indexed.cache()
    }

    val numCols = X.first.size

    val ranks = new Array[RDD[Double]](numCols)
    var i = 0
    while (i < numCols) {
      val column = indexed.map {case(vector, index) => (vector(i), index)}
      ranks(i) = getRanksWithIndex(column)
      i += 1
    }

    // only compute for upper triangular since the correlation matrix is symmetric and
    // each pairwise computation is expensive
    val triuSize = numCols * (numCols + 1) / 2
    val correlations = new Array[Double](triuSize)
    var j = 0
    var idx = 0
    while (j < numCols) {
      i = 0
      while (i <= j) {
        correlations(idx) = if (i == j) {
          1.0
        }  else {
          val rankPairs = makeRankPairs(ranks(i), ranks(j))
          computeCorrelationFromRanks(rankPairs)
        }
        idx += 1
        i += 1
      }
      j += 1
    }
    Matrices.fromBreeze(RowMatrix.triuToFull(triuSize, correlations).toBreeze)
  }

  /**
   * Compute the ranks for elements in the input RDD, using the average method for ties.
   *
   * With the average method, elements with the same value receive the same rank that's computed
   * by taking the average of their positions in the sorted list.
   * e.g. ranks([2, 1, 0, 2]) = [2.5, 1.0, 0.0, 2.5]
   */
  private def getRanksWithIndex(indexed: RDD[(Double, Long)]): RDD[Double] = {
    // Get elements' positions in the sorted list for computing average rank for duplicate values
    val sorted = indexed.sortByKey().zipWithIndex()
    val groupedByValue = sorted.groupBy(_._1._1)
    val ranks = groupedByValue.flatMap[(Long, Double)] { item =>
      val duplicates = item._2
      if (duplicates.size > 1) {
        val averageRank = duplicates.foldLeft(0L) {_ + _._2} / duplicates.size.toDouble
        duplicates.map(entry => (entry._1._2, averageRank)).toSeq
      } else {
        duplicates.map(entry => (entry._1._2, entry._2.toDouble)).toSeq
      }
    }
    ranks.sortByKey().values
  }

  private def getRanks(input: RDD[Double]): RDD[Double] = getRanksWithIndex(input.zipWithIndex())

  /**
   * Compute Spearman's rank correlation, rho, given ranks.
   *
   * rho = 1 - 6 * sum(di) / (n * (n * n - 1)), where di = xi - yi for xi in ranks1 & yi in ranks2
   *
   * The size n and sum(di) are computed in the same pass over the rank pairs.
   * We check that the rank RDDs have the same size while making the rank pairs with zip.
   */
  private def computeCorrelationFromRanks(rankPairs: RDD[(Double, Double)]): Double = {
    val results = rankPairs.mapPartitions(it => {
      val results = it.foldLeft((0L, 0.0)) {(r, item) =>
        (r._1 + 1, r._2 + math.pow(item._1 - item._2, 2.0))}
      Iterator(results)
    }, preservesPartitioning = true).reduce((a, b) => (a._1 + b._1, a._2 + b._2))

    val n = results._1
    val D = results._2
    1.0 - 6.0 * D / (n * (n * n - 1.0))
  }

  private def makeRankPairs(ranks1: RDD[Double], ranks2: RDD[Double]): RDD[(Double, Double)] = {
    try {
      ranks1.zip(ranks2)
    } catch {
      case se: SparkException => throw new IllegalArgumentException("Cannot compute correlation"
        + "for RDDs of different sizes.")
    }
  }
}
