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

import org.apache.spark.Partitioner
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.{DenseVector, Matrix, Vector}
import org.apache.spark.rdd.{CoGroupedRDD, RDD}

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
    computeCorrelationWithMatrixImpl(x, y)
  }

  /**
   * Compute Spearman's correlation matrix S, for the input matrix, where S(i, j) is the
   * correlation between column i and j.
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
    val ranks = new Array[RDD[(Long, Double)]](numCols)

    // Note: we use a for loop here instead of a while loop with a single index variable
    // to avoid race condition caused by closure serialization
    for (k <- 0 until numCols) {
      val column = indexed.map {case(vector, index) => {
        (vector(k), index)}
      }
      ranks(k) = getRanks(column)
    }

    val ranksMat: RDD[Vector] = makeRankMatrix(ranks)
    PearsonCorrelation.computeCorrelationMatrix(ranksMat)
  }

  /**
   * Compute the ranks for elements in the input RDD, using the average method for ties.
   *
   * With the average method, elements with the same value receive the same rank that's computed
   * by taking the average of their positions in the sorted list.
   * e.g. ranks([2, 1, 0, 2]) = [3.5, 2.0, 1.0, 3.5]
   */
  private def getRanks(indexed: RDD[(Double, Long)]): RDD[(Long, Double)] = {
    // Get elements' positions in the sorted list for computing average rank for duplicate values
    val sorted = indexed.sortByKey().zipWithIndex()
    val groupedByValue = sorted.groupBy(_._1._1)
    val ranks = groupedByValue.flatMap[(Long, Double)] { item =>
      val duplicates = item._2
      if (duplicates.size > 1) {
        val averageRank = duplicates.foldLeft(0L) {_ + _._2 + 1} / duplicates.size.toDouble
        duplicates.map(entry => (entry._1._2, averageRank)).toSeq
      } else {
        duplicates.map(entry => (entry._1._2, entry._2.toDouble + 1)).toSeq
      }
    }
    ranks.sortByKey()
  }

  private def makeRankMatrix(ranks: Array[RDD[(Long, Double)]]): RDD[Vector] = {
    val partitioner = Partitioner.defaultPartitioner(ranks(0), ranks.tail: _*)
    val cogrouped = new CoGroupedRDD[Long](ranks, partitioner)
    cogrouped.mapPartitions({ iter =>
      iter.map {case (index, values:Seq[Seq[Double]]) => new DenseVector(values.flatten.toArray)}
    })
  }
}
