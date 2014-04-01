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

import breeze.linalg.{Vector => BV}

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import breeze.linalg.axpy

case class VectorRDDStatisticalSummary(
    mean: Vector,
    variance: Vector,
    count: Long,
    max: Vector,
    min: Vector,
    nonZeroCnt: Vector) extends Serializable

/**
 * Extra functions available on RDDs of [[org.apache.spark.mllib.linalg.Vector Vector]] through an
 * implicit conversion. Import `org.apache.spark.MLContext._` at the top of your program to use
 * these functions.
 */
class VectorRDDFunctions(self: RDD[Vector]) extends Serializable {

  /**
   * Compute full column-wise statistics for the RDD, including
   * {{{
   *   Mean:              Vector,
   *   Variance:          Vector,
   *   Count:             Double,
   *   Non-zero count:    Vector,
   *   Maximum elements:  Vector,
   *   Minimum elements:  Vector.
   * }}},
   * with the size of Vector as input parameter.
   */
  def summarizeStatistics(size: Int): VectorRDDStatisticalSummary = {
    val results = self.map(_.toBreeze).aggregate((
      BV.zeros[Double](size),
      BV.zeros[Double](size),
      0.0,
      BV.zeros[Double](size),
      BV.fill(size){Double.MinValue},
      BV.fill(size){Double.MaxValue}))(
      seqOp = (c, v) => (c, v) match {
        case ((prevMean, prevM2n, cnt, nnzVec, maxVec, minVec), currData) =>
          val currMean = prevMean :* (cnt / (cnt + 1.0))
          axpy(1.0/(cnt+1.0), currData, currMean)
          axpy(-1.0, currData, prevMean)
          prevMean :*= (currMean - currData)
          axpy(1.0, prevMean, prevM2n)
          axpy(1.0,
            Vectors.sparse(size, currData.activeKeysIterator.toSeq.map(x => (x, 1.0))).toBreeze,
            nnzVec)
          currData.activeIterator.foreach { case (id, value) =>
            if (maxVec(id) < value) maxVec(id) = value
            if (minVec(id) > value) minVec(id) = value
          }
          (currMean,
            prevM2n,
            cnt + 1.0,
            nnzVec,
            maxVec,
            minVec)
      },
      combOp = (c, v) => (c, v) match {
        case (
          (mean1, m2n1, cnt1, nnz1, max1, min1),
          (mean2, m2n2, cnt2, nnz2, max2, min2)) =>
            val totalCnt = cnt1 + cnt2
            val deltaMean = mean2 - mean1
            mean1 :*= (cnt1 / totalCnt)
            axpy(cnt2/totalCnt, mean2, mean1)
            val totalMean = mean1
            deltaMean :*= deltaMean
            axpy(cnt1*cnt2/totalCnt, deltaMean, m2n1)
            axpy(1.0, m2n2, m2n1)
            val totalM2n = m2n1
            max2.activeIterator.foreach { case (id, value) =>
              if (max1(id) < value) max1(id) = value
            }
            min2.activeIterator.foreach { case (id, value) =>
              if (min1(id) > value) min1(id) = value
            }
            axpy(1.0, nnz2, nnz1)
            (totalMean, totalM2n, totalCnt, nnz1, max1, min1)
      }
    )

    results._2 :/= results._3

    VectorRDDStatisticalSummary(
      Vectors.fromBreeze(results._1),
      Vectors.fromBreeze(results._2),
      results._3.toLong,
      Vectors.fromBreeze(results._4),
      Vectors.fromBreeze(results._5),
      Vectors.fromBreeze(results._6))
  }
}
