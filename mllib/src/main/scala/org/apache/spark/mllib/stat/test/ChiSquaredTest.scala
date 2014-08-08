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

import breeze.linalg.{DenseMatrix => BDM}
import cern.jet.stat.Probability.chiSquareComplemented

import org.apache.spark.Logging
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
 * Conduct the chi-squared test for the input RDDs using the specified method.
 * Goodness-of-fit test is conducted on two `Vectors`, whereas test of independence is conducted
 * on an input of type `Matrix` in which independence between columns is assessed.
 * We also provide a method for computing the chi-squared statistic between each feature and the
 * label for an input `RDD[LabeledPoint]`, return an `Array[ChiSquaredTestResult]` of size =
 * number of features in the inpuy RDD.
 *
 * Supported methods for goodness of fit: `pearson` (default)
 * Supported methods for independence: `pearson` (default)
 *
 * More information on Chi-squared test: http://en.wikipedia.org/wiki/Chi-squared_test
 */
private[stat] object ChiSqTest extends Logging {

  /**
   * @param name String name for the method.
   * @param chiSqFunc Function for computing the statistic given the observed and expected counts.
   */
  case class Method(name: String, chiSqFunc: (Double, Double) => Double)

  // Pearson's chi-squared test: http://en.wikipedia.org/wiki/Pearson%27s_chi-squared_test
  val PEARSON = new Method("pearson", (observed: Double, expected: Double) => {
    val dev = observed - expected
    dev * dev / expected
  })

  // Null hypothesis for the two different types of chi-squared tests to be included in the result.
  object NullHypothesis extends Enumeration {
    type NullHypothesis = Value
    val goodnessOfFit = Value("observed follows the same distribution as expected.")
    val independence = Value("observations in each column are statistically independent.")
  }

  // Method identification based on input methodName string
  private def methodFromString(methodName: String): Method = {
    methodName match {
      case PEARSON.name => PEARSON
      case _ => throw new IllegalArgumentException("Unrecognized method for Chi squared test.")
    }
  }

  /**
   * Conduct Pearson's independence test for each feature against the label across the input RDD.
   * The contingency table is constructed from the raw (feature, label) pairs and used to conduct
   * the independence test.
   * Returns an array containing the ChiSquaredTestResult for every feature against the label.
   */
  def chiSquaredFeatures(data: RDD[LabeledPoint],
      methodName: String = PEARSON.name): Array[ChiSquaredTestResult] = {
    val numCols = data.first().features.size
    val results = new Array[ChiSquaredTestResult](numCols)
    var labels = Array[Double]()
    // At most 100 columns at a time
    val batchSize = 100
    var batch = 0
    while (batch * batchSize < numCols) {
      // The following block of code can be cleaned up and made public as
      // chiSquared(data: RDD[(V1, V2)])
      val startCol = batch * batchSize
      val endCol = startCol + math.min(batchSize, numCols - startCol)
      val pairCounts = data.flatMap { p =>
        // assume dense vectors
        p.features.toArray.slice(startCol, endCol).zipWithIndex.map { case (feature, col) =>
          (col, feature, p.label)
        }
      }.countByValue()

      if (labels.size == 0) {
        // Do this only once for the first column since labels are invariant across features.
        labels = pairCounts.keys.filter(_._1 == startCol).map(_._3).toArray.distinct
      }
      val numLabels = labels.size
      pairCounts.keys.groupBy(_._1).map { case (col, keys) =>
        val features = keys.map(_._2).toArray.distinct
        val numRows = features.size
        val contingency = new BDM(numRows, numLabels, new Array[Double](numRows * numLabels))
        keys.foreach { case (_, feature, label) =>
          val i = features.indexOf(feature)
          val j = labels.indexOf(label)
          contingency(i, j) += pairCounts((col, feature, label))
        }
        results(col) = chiSquaredMatrix(Matrices.fromBreeze(contingency), methodName)
      }
      batch += 1
    }
    results
  }

  /*
   * Pearon's goodness of fit test on the input observed and expected counts/relative frequencies.
   * Uniform distribution is assumed when `expected` is not passed in.
   */
  def chiSquared(observed: Vector,
      expected: Vector = Vectors.dense(Array[Double]()),
      methodName: String = PEARSON.name): ChiSquaredTestResult = {

    // Validate input arguments
    val method = methodFromString(methodName)
    if (expected.size != 0 && observed.size != expected.size) {
      throw new IllegalArgumentException("observed and expected must be of the same size.")
    }
    val size = observed.size
    // Avoid calling toArray on input vectors to avoid memory blow up
    // (esp if size = Int.MaxValue for a SparseVector).
    // Check positivity and collect sums
    var obsSum = 0.0
    var expSum = if (expected.size == 0.0) 1.0 else 0.0
    var i = 0
    while (i < size) {
      val obs = observed(i)
      if (obs < 0.0) {
        throw new IllegalArgumentException("Values in observed must be nonnegative.")
      }
      obsSum += obs
      if (expected.size > 0) {
        val exp = expected(i)
        if (exp <= 0.0) {
          throw new IllegalArgumentException("Values in expected must be positive.")
        }
        expSum += exp
      }
      i += 1
    }

    // Determine the scaling factor for expected
    val scale = if (math.abs(obsSum - expSum) < 1e-7) 1.0 else  obsSum / expSum
    val getExpected: (Int) => Double = if (expected.size == 0) {
      // Assume uniform distribution
      if (scale == 1.0) _ => 1.0 / size else _ => scale / size
    } else {
      if (scale == 1.0) (i: Int) => expected(i) else (i: Int) => scale * expected(i)
    }

    // compute chi-squared statistic
    var statistic = 0.0
    var j = 0
    while (j < observed.size) {
      val obs = observed(j)
      if (obs != 0.0) {
        statistic += method.chiSqFunc(obs, getExpected(j))
      }
      j += 1
    }
    val df = size - 1
    val pValue = chiSquareComplemented(df, statistic)
    new ChiSquaredTestResult(pValue, df, statistic, PEARSON.name,
      NullHypothesis.goodnessOfFit.toString)
  }

  /*
   * Pearon's independence test on the input contingency matrix.
   * TODO: optimize for SparseMatrix when it becomes supported.
   */
  def chiSquaredMatrix(counts: Matrix, methodName:String = PEARSON.name): ChiSquaredTestResult = {
    val method = methodFromString(methodName)
    val numRows = counts.numRows
    val numCols = counts.numCols

    // get row and column sums
    val colSums = new Array[Double](numCols)
    val rowSums = new Array[Double](numRows)
    val colMajorArr = counts.toArray
    var i = 0
    while (i < colMajorArr.size) {
      val elem = colMajorArr(i)
      if (elem < 0.0) {
        throw new IllegalArgumentException("Contingency table cannot contain negative entries.")
      }
      colSums(i / numRows) += elem
      rowSums(i % numRows) += elem
      i += 1
    }
    if (!colSums.forall(_ > 0.0) || !rowSums.forall(_ > 0.0)) {
      throw new IllegalArgumentException("Chi square statistic cannot be computed for input matrix "
        + "due to 0.0 entries in the expected contingency table.")
    }
    val total = colSums.sum

    // second pass to collect statistic
    var statistic = 0.0
    var j = 0
    while (j < colMajorArr.size) {
      val expected = colSums(j / numRows) * rowSums(j % numRows) / total
      statistic += method.chiSqFunc(colMajorArr(j), expected)
      j += 1
    }

    // Second pass to compute chi-squared statistic
    val df = (numCols - 1) * (numRows - 1)
    val pValue = chiSquareComplemented(df, statistic)
    new ChiSquaredTestResult(pValue, df, statistic, methodName,
      NullHypothesis.independence.toString)
  }
}
