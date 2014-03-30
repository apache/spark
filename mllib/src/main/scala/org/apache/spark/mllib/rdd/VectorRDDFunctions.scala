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
  def statistics(size: Int): (Vector, Vector, Double, Vector, Vector, Vector) = {
    val results = self.map(_.toBreeze).aggregate((
      BV.zeros[Double](size),
      BV.zeros[Double](size),
      0.0,
      BV.zeros[Double](size),
      BV.fill(size){Double.MinValue},
      BV.fill(size){Double.MaxValue}))(
      seqOp = (c, v) => (c, v) match {
        case ((prevMean, prevM2n, cnt, nnzVec, maxVec, minVec), currData) =>
          val currMean = ((prevMean :* cnt) + currData) :/ (cnt + 1.0)
          val nonZeroCnt = Vectors
            .sparse(size, currData.activeKeysIterator.toSeq.map(x => (x, 1.0))).toBreeze
          currData.activeIterator.foreach { case (id, value) =>
            if (maxVec(id) < value) maxVec(id) = value
            if (minVec(id) > value) minVec(id) = value
          }
          (currMean,
            prevM2n + ((currData - prevMean) :* (currData - currMean)),
            cnt + 1.0,
            nnzVec + nonZeroCnt,
            maxVec,
            minVec)
      },
      combOp = (lhs, rhs) => (lhs, rhs) match {
        case (
          (lhsMean, lhsM2n, lhsCnt, lhsNNZ, lhsMax, lhsMin),
          (rhsMean, rhsM2n, rhsCnt, rhsNNZ, rhsMax, rhsMin)) =>
            val totalCnt = lhsCnt + rhsCnt
            val totalMean = (lhsMean :* lhsCnt) + (rhsMean :* rhsCnt) :/ totalCnt
            val deltaMean = rhsMean - lhsMean
            val totalM2n =
              lhsM2n + rhsM2n + (((deltaMean :* deltaMean) :* (lhsCnt * rhsCnt)) :/ totalCnt)
            rhsMax.activeIterator.foreach { case (id, value) =>
              if (lhsMax(id) < value) lhsMax(id) = value
            }
            rhsMin.activeIterator.foreach { case (id, value) =>
              if (lhsMin(id) > value) lhsMin(id) = value
            }
            (totalMean, totalM2n, totalCnt, lhsNNZ + rhsNNZ, lhsMax, lhsMin)
      }
    )

    (Vectors.fromBreeze(results._1),
      Vectors.fromBreeze(results._2 :/ results._3),
      results._3,
      Vectors.fromBreeze(results._4),
      Vectors.fromBreeze(results._5),
      Vectors.fromBreeze(results._6))
  }
}
