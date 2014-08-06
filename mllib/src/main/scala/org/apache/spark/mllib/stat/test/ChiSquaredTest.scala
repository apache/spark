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

import cern.jet.stat.Probability.chiSquareComplemented

import org.apache.spark.Logging
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

/**
 * Conduct the Chi-squared test for the input RDDs using the specified method.
 * Goodness-of-fit test is conducted on two RDD[Double]s, whereas test of independence is conducted
 * on an input of type RDD[Vector] in which independence between columns is assessed.
 *
 * Supported methods for goodness of fit: `pearson` (default)
 * Supported methods for independence: `pearson` (default)
 *
 * More information on Chi-squared test: http://en.wikipedia.org/wiki/Chi-squared_test
 * More information on Pearson's chi-squared test:
 *   http://en.wikipedia.org/wiki/Pearson%27s_chi-squared_test
 *
 */
private[stat] object ChiSquaredTest extends Logging {

  val PEARSON = "pearson"

  object NullHypothesis extends Enumeration {
    type NullHypothesis = Value
    val goodnessOfFit = Value("observed follows the same distribution as expected.")
    val independence = Value("observations in each column are statistically independent.")
  }

  val zeroExpectedError = new IllegalArgumentException("Chi square statistic cannot be computed"
    + " for input RDD due to nonpositive entries in the expected contingency table.")

  // delegator method for goodness of fit test
  def chiSquared(observed: RDD[Double],
      expected: RDD[Double],
      method: String = PEARSON): ChiSquaredTestResult = {
    method match {
      case PEARSON => chiSquaredPearson(observed, expected)
      case _ => throw new IllegalArgumentException("Unrecognized method for Chi squared test.")
    }
  }

  // delegator method for independence test
  def chiSquaredMatrix(counts: RDD[Vector], method: String = PEARSON): ChiSquaredTestResult = {
    method match {
      // Yates' correction doesn't really apply here
      case PEARSON => chiSquaredPearson(counts)
      case _ => throw new IllegalArgumentException("Unrecognized method for Chi squared test.")
    }
  }

  // Equation for computing Pearson's chi-squared statistic
  private def pearson = (observed: Double, expected: Double) => {
    val dev = observed - expected
    dev * dev / expected
  }

  /*
   * Pearon's goodness of fit test. This can be easily made abstract to support other methods.
   * Makes two passes over both input RDDs.
   */
  private def chiSquaredPearson(observed: RDD[Double],
      expected: RDD[Double]): ChiSquaredTestResult = {

    // compute the scaling factor and count for the input RDDs and check positivity in one pass
    val observedStats = observed.stats()
    if (observedStats.min < 0.0) {
      throw new IllegalArgumentException("Values in observed must be nonnegative.")
    }
    val expectedStats = expected.stats()
    if (expectedStats.min <= 0.0) {
      throw new IllegalArgumentException("Values in expected must be positive.")
    }
    if (observedStats.count != expectedStats.count) {
      throw new IllegalArgumentException("observed and expected must be of the same size.")
    }

    val expScaled = if (math.abs(observedStats.sum - expectedStats.sum) < 1e-7) {
      // No scaling needed since both RDDs have the same total
      expected
    } else {
      expected.map(_ * observedStats.sum / expectedStats.sum)
    }

    // Second pass to compute chi-squared statistic
    val statistic = observed.zip(expScaled).aggregate(0.0)({ case (sum, (obs, exp)) => {
      sum + pearson(obs, exp)
    }}, _ + _)
    val df = observedStats.count - 1
    val pValue = chiSquareComplemented(df, statistic)
    new ChiSquaredTestResult(pValue, Array(df), statistic, PEARSON,
      NullHypothesis.goodnessOfFit.toString)
  }

  /*
   * Pearon's independence test. This can be easily made abstract to support other methods.
   * Makes two passes over the input RDD.
   */
  private def chiSquaredPearson(counts: RDD[Vector]): ChiSquaredTestResult = {

    val numCols = counts.first.size

    // first pass for collecting column sums
    case class SumNCount(colSums: Array[Double], numRows: Long)

    val result = counts.aggregate(new SumNCount(new Array[Double](numCols), 0L))(
      (sumNCount, vector) => {
        val arr = vector.toArray
        // check that the counts are all non-negative and finite in this pass
        if (!arr.forall(i => !i.isNaN && !i.isInfinite && i >= 0.0)) {
          throw new IllegalArgumentException("Values in the input RDD must be nonnegative.")
        }
        new SumNCount((sumNCount.colSums, arr).zipped.map(_ + _), sumNCount.numRows + 1)
      }, (sums1, sums2) => {
        new SumNCount((sums1.colSums, sums2.colSums).zipped.map(_ + _),
          sums1.numRows + sums2.numRows)
      })

    val colSums = result.colSums
    if (!colSums.forall(_ > 0.0)) {
      throw zeroExpectedError
    }
    val total = colSums.sum

    // Second pass to compute chi-squared statistic
    val statistic = counts.aggregate(0.0)(rowStatistic(colSums, total, pearson), _ + _)
    val df = (numCols - 1) * (result.numRows - 1)
    val pValue = chiSquareComplemented(df, statistic)
    new ChiSquaredTestResult(pValue, Array(df), statistic, PEARSON,
      NullHypothesis.independence.toString)
  }

  // returns function to be used as seqOp in the aggregate operation to collect statistic
  private def rowStatistic(colSums: Array[Double],
      total: Double,
      chiSquared: (Double, Double) => Double) = {
    (statistic: Double, vector: Vector) => {
      val arr = vector.toArray
      val rowSum = arr.sum
      if (rowSum == 0.0) { // rowSum >= 0.0 as ensured by the nonnegative check
        throw zeroExpectedError
      }
      (arr, colSums).zipped.foldLeft(statistic) { case (stat, (observed, colSum)) =>
        val expected = rowSum * colSum / total
        val r = stat + chiSquared(observed, expected)
        r
      }
    }
  }
}
