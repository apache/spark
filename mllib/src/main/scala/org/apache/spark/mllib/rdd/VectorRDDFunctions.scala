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
    val (fakeMean, fakeM2n, totalCnt, nnz, max, min) = self.map(_.toBreeze).aggregate((
      BV.zeros[Double](size),
      BV.zeros[Double](size),
      0.0,
      BV.zeros[Double](size),
      BV.fill(size){Double.MinValue},
      BV.fill(size){Double.MaxValue}))(
      seqOp = (c, v) => (c, v) match {
        case ((prevMean, prevM2n, cnt, nnzVec, maxVec, minVec), currData) =>
          currData.activeIterator.map{ case (id, value) =>
            val tmpPrevMean = prevMean(id)
            prevMean(id)  = (prevMean(id) * cnt + value) / (cnt + 1.0)
            if (maxVec(id) < value) maxVec(id) = value
            if (minVec(id) > value) minVec(id) = value
            nnzVec(id) += 1.0
            prevM2n(id) += (value - prevMean(id)) * (value - tmpPrevMean)
          }

          (prevMean,
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
            val totalMean = ((mean1 :* nnz1) + (mean2 :* nnz2)) :/ (nnz1 + nnz2)
            val totalM2n = m2n1 + m2n2 + ((deltaMean :* deltaMean) :* (nnz1 :* nnz2) :/ (nnz1 + nnz2))
            max2.activeIterator.foreach { case (id, value) =>
              if (max1(id) < value) max1(id) = value
            }
            min2.activeIterator.foreach { case (id, value) =>
              if (min1(id) > value) min1(id) = value
            }
            (totalMean, totalM2n, totalCnt, nnz1 + nnz2, max1, min1)
      }
    )

    // solve real mean
    val realMean = fakeMean :* nnz :/ totalCnt
    // solve real variance
    val deltaMean = fakeMean :- 0.0
    val realVar = fakeM2n - ((deltaMean :* deltaMean) :* (nnz :* (nnz :- totalCnt)) :/ totalCnt)
    max :+= 0.0
    min :+= 0.0

    realVar :/= totalCnt

    VectorRDDStatisticalSummary(
      Vectors.fromBreeze(realMean),
      Vectors.fromBreeze(realVar),
      totalCnt.toLong,
      Vectors.fromBreeze(nnz),
      Vectors.fromBreeze(max),
      Vectors.fromBreeze(min))
  }
}
