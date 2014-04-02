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
package org.apache.spark.mllib.rdd

import breeze.linalg.{axpy, Vector => BV}

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

/**
 * Case class of the summary statistics, including mean, variance, count, max, min, and non-zero
 * elements count.
 */
case class VectorRDDStatisticalAggregator(
    mean: BV[Double],
    statCounter: BV[Double],
    totalCount: Double,
    numNonZeros: BV[Double],
    max: BV[Double],
    min: BV[Double])

/**
 * Extra functions available on RDDs of [[org.apache.spark.mllib.linalg.Vector Vector]] through an
 * implicit conversion. Import `org.apache.spark.MLContext._` at the top of your program to use
 * these functions.
 */
class VectorRDDFunctions(self: RDD[Vector]) extends Serializable {

  /**
   * Aggregate function used for aggregating elements in a worker together.
   */
  private def seqOp(
      aggregator: VectorRDDStatisticalAggregator,
      currData: BV[Double]): VectorRDDStatisticalAggregator = {
    aggregator match {
      case VectorRDDStatisticalAggregator(prevMean, prevM2n, cnt, nnzVec, maxVec, minVec) =>
        currData.activeIterator.foreach {
          case (id, 0.0) =>
          case (id, value) =>
            if (maxVec(id) < value) maxVec(id) = value
            if (minVec(id) > value) minVec(id) = value

            val tmpPrevMean = prevMean(id)
            prevMean(id) = (prevMean(id) * cnt + value) / (cnt + 1.0)
            prevM2n(id) += (value - prevMean(id)) * (value - tmpPrevMean)

            nnzVec(id) += 1.0
        }

        VectorRDDStatisticalAggregator(
          prevMean,
          prevM2n,
          cnt + 1.0,
          nnzVec,
          maxVec,
          minVec)
    }
  }

  /**
   * Combine function used for combining intermediate results together from every worker.
   */
  private def combOp(
      statistics1: VectorRDDStatisticalAggregator,
      statistics2: VectorRDDStatisticalAggregator): VectorRDDStatisticalAggregator = {
    (statistics1, statistics2) match {
      case (VectorRDDStatisticalAggregator(mean1, m2n1, cnt1, nnz1, max1, min1),
            VectorRDDStatisticalAggregator(mean2, m2n2, cnt2, nnz2, max2, min2)) =>
        val totalCnt = cnt1 + cnt2
        val deltaMean = mean2 - mean1

        mean2.activeIterator.foreach {
          case (id, 0.0) =>
          case (id, value) =>
            mean1(id) = (mean1(id) * nnz1(id) + mean2(id) * nnz2(id)) / (nnz1(id) + nnz2(id))
        }

        m2n2.activeIterator.foreach {
          case (id, 0.0) =>
          case (id, value) =>
            m2n1(id) +=
              value + deltaMean(id) * deltaMean(id) * nnz1(id) * nnz2(id) / (nnz1(id)+nnz2(id))
        }

        max2.activeIterator.foreach {
          case (id, value) =>
            if (max1(id) < value) max1(id) = value
        }

        min2.activeIterator.foreach {
          case (id, value) =>
            if (min1(id) > value) min1(id) = value
        }

        axpy(1.0, nnz2, nnz1)
        VectorRDDStatisticalAggregator(mean1, m2n1, totalCnt, nnz1, max1, min1)
    }
  }

  /**
   * Compute full column-wise statistics for the RDD with the size of Vector as input parameter.
   */
  def summarizeStatistics(): VectorRDDStatisticalAggregator = {
    val size = self.take(1).head.size
    val zeroValue = VectorRDDStatisticalAggregator(
      BV.zeros[Double](size),
      BV.zeros[Double](size),
      0.0,
      BV.zeros[Double](size),
      BV.fill(size)(Double.MinValue),
      BV.fill(size)(Double.MaxValue))

    val VectorRDDStatisticalAggregator(currMean, currM2n, totalCnt, nnz, currMax, currMin) =
      self.map(_.toBreeze).aggregate(zeroValue)(seqOp, combOp)

    // solve real mean
    val realMean = currMean :* nnz :/ totalCnt

    // solve real m2n
    val deltaMean = currMean
    val realM2n = currM2n - ((deltaMean :* deltaMean) :* (nnz :* (nnz :- totalCnt)) :/ totalCnt)

    // remove the initial value in max and min, i.e. the Double.MaxValue or Double.MinValue.
    nnz.activeIterator.foreach {
      case (id, 0.0) =>
        currMax(id) = 0.0
        currMin(id) = 0.0
      case _ =>
    }

    // get variance
    realM2n :/= totalCnt

    VectorRDDStatisticalAggregator(
      realMean,
      realM2n,
      totalCnt,
      nnz,
      currMax,
      currMin)
  }
}