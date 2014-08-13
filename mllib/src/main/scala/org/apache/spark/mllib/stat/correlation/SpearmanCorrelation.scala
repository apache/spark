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

import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{Logging, HashPartitioner}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.{Vectors, DenseVector, Matrix, Vector}
import org.apache.spark.rdd.{CoGroupedRDD, RDD}

/**
 * Compute Spearman's correlation for two RDDs of the type RDD[Double] or the correlation matrix
 * for an RDD of the type RDD[Vector].
 *
 * Definition of Spearman's correlation can be found at
 * http://en.wikipedia.org/wiki/Spearman's_rank_correlation_coefficient
 */
private[stat] object SpearmanCorrelation extends Correlation with Logging {

  /**
   * Compute Spearman's correlation for two datasets.
   */
  override def computeCorrelation(x: RDD[Double], y: RDD[Double]): Double = {
    computeCorrelationWithMatrixImpl(x, y)
  }

  /**
   * Compute Spearman's correlation matrix S, for the input matrix, where S(i, j) is the
   * correlation between column i and j.
   *
   * Input RDD[Vector] should be cached or checkpointed if possible since it would be split into
   * numCol RDD[Double]s, each of which sorted, and the joined back into a single RDD[Vector].
   */
  override def computeCorrelationMatrix(X: RDD[Vector]): Matrix = {
    val transposed = X.zipWithUniqueId().flatMap { case (vec, uid) =>
      vec.toArray.view.zipWithIndex.map { case (v, j) =>
        ((j, v), uid)
      }
    }.persist(StorageLevel.MEMORY_AND_DISK)
    val sorted = transposed.sortByKey().persist(StorageLevel.MEMORY_AND_DISK)
    val ranked = sorted.zipWithIndex().mapPartitions { iter =>
      var preCol = -1
      var preVal = Double.NaN
      var startRank = -1.0
      var cachedIds = ArrayBuffer.empty[Long]
      def flush(): Iterable[(Long, (Int, Double))] = {
        val averageRank = startRank + (cachedIds.size - 1) / 2.0
        val output = cachedIds.map { i =>
          (i, (preCol, averageRank))
        }
        cachedIds.clear()
        output
      }
      iter.flatMap { case (((j, v), uid), rank) =>
        if (j != preCol || v != preVal) {
          val output = flush()
          preCol = j
          preVal = v
          startRank = rank
          cachedIds += uid
          output
        } else {
          cachedIds += uid
          Iterator.empty
        }
      } ++ {
        flush()
      }
    }
    val ranks = tied.groupByKey().map { case (uid, iter) =>
      val values = iter.toSeq.sortBy(_._1).map(_._2).toArray
      println(values.toSeq)
      Vectors.dense(values)
    }
    val corrMatrix = PearsonCorrelation.computeCorrelationMatrix(ranks)

    transposed.unpersist(blocking = false)
    sorted.unpersist(blocking = false)

    corrMatrix
  }
}

