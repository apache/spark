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

package org.apache.spark.mllib.stat.test

import cern.jet.stat.Probability.chiSquare

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{DenseVector, Vector}

private[stat] object ChiSquaredTest {

  val PEARSON = "pearson"

  def chiSquared(expected: RDD[Double],
      observed: RDD[Double],
      method: String = PEARSON): ChiSquaredTestResult = {
    method match {
      case PEARSON => chiSquaredPearson(expected, observed)
      case _ => throw new IllegalArgumentException("Unrecognized method for Chi squared test.")
    }
  }

  def chiSquaredMatrix(counts: RDD[Vector], method: String = PEARSON): ChiSquaredTestResult = {
    method match {
      case PEARSON => chiSquaredPearson(counts)
      case _ => throw new IllegalArgumentException("Unrecognized method for Chi squared test.")
    }
  }

  private def chiSquaredPearson(x: RDD[Double],
      y: RDD[Double]): ChiSquaredTestResult = {
    val mat: RDD[Vector] = x.zip(y).map { case (xi, yi) => new DenseVector(Array(xi, yi)) }
    chiSquaredPearson(mat)
  }

  // Makes two passes over the RDD total
  private def chiSquaredPearson(counts: RDD[Vector]): ChiSquaredTestResult = {
    val numCols = counts.first.size

    // first pass for collecting column sums
    val result = counts.aggregate((new Array[Double](numCols), 0))(
      (sums, vector) => {
        val arr = vector.toArray
        // check that the counts are all non-negative and finite in this pass
        if (!arr.forall( i => !i.isNaN && !i.isInfinite && i >= 0.0)) {
          throw new IllegalArgumentException("All input entries must be nonnegative and finite.")
        }
        ((sums._1, arr).zipped.map(_ + _), sums._2 + 1)
      },  //seqOp
      (sums1, sums2) => ((sums1._1, sums2._1).zipped.map(_ + _), sums1._2 + sums2._2)) // combOp

    val colSums = result._1
    val total = colSums.sum

    // Second pass to compute chi-squared statistic
    val statistic = counts.aggregate(0.0)(rowStatistic(colSums, total), _ + _)
    val df = (numCols - 1) * (result._2 - 1)
    val pValue = chiSquare(statistic, df)

    new ChiSquaredTestResult(pValue, Array(df), statistic, PEARSON)
  }

  // curried function to be used as seqOp in the aggregate operation to collect statistic
  private def rowStatistic(colSums: Array[Double], total: Double) = {
    (statistic: Double, vector: Vector) => {
      val arr = vector.toArray
      val rowSum = arr.sum
      (arr, colSums).zipped.foldLeft(statistic) { case (stat, (observed, colSum)) =>
        val expected = rowSum * colSum / total
        val r = stat + (observed - expected) * (observed - expected) / expected
        r
      }
    }
  }
}
