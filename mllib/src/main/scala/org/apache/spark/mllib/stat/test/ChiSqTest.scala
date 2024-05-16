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

import org.apache.commons.math3.distribution.ChiSquaredDistribution

import org.apache.spark.SparkException
import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.{OpenHashMap, Utils}

/**
 * Conduct the chi-squared test for the input RDDs using the specified method.
 * Goodness-of-fit test is conducted on two `Vectors`, whereas test of independence is conducted
 * on an input of type `Matrix` in which independence between columns is assessed.
 * We also provide a method for computing the chi-squared statistic between each feature and the
 * label for an input `RDD[LabeledPoint]`, return an `Array[ChiSquaredTestResult]` of size =
 * number of features in the input RDD.
 *
 * Supported methods for goodness of fit: `pearson` (default)
 * Supported methods for independence: `pearson` (default)
 *
 * More information on Chi-squared test: http://en.wikipedia.org/wiki/Chi-squared_test
 */
private[spark] object ChiSqTest extends Logging {

  /**
   * @param name String name for the method.
   * @param chiSqFunc Function for computing the statistic given the observed and expected counts.
   */
  case class Method(name: String, chiSqFunc: (Double, Double) => Double)

  // Pearson's chi-squared test: http://en.wikipedia.org/wiki/Pearson%27s_chi-squared_test
  val PEARSON = Method("pearson", (observed: Double, expected: Double) => {
    val dev = observed - expected
    dev * dev / expected
  })

  // Null hypothesis for the two different types of chi-squared tests to be included in the result.
  object NullHypothesis extends Enumeration {
    type NullHypothesis = Value
    val goodnessOfFit = Value("observed follows the same distribution as expected.")
    val independence = Value("the occurrence of the outcomes is statistically independent.")
  }

  // Method identification based on input methodName string
  private def methodFromString(methodName: String): Method = {
    methodName match {
      case PEARSON.name => PEARSON
      case _ => throw new IllegalArgumentException("Unrecognized method for Chi squared test.")
    }
  }

  /**
   * Max number of categories when indexing labels and features
   */
  private[spark] val maxCategories: Int = 10000

  /**
   * Conduct Pearson's independence test for each feature against the label across the input RDD.
   * The contingency table is constructed from the raw (feature, label) pairs and used to conduct
   * the independence test.
   * Returns an array containing the ChiSquaredTestResult for every feature against the label.
   */
  def chiSquaredFeatures(
      data: RDD[LabeledPoint],
      methodName: String = PEARSON.name): Array[ChiSqTestResult] = {
    computeChiSquared(data.map(l => (l.label, l.features)), methodName)
      .collect().sortBy(_._1)
      .map { case (_, pValue, degreesOfFreedom, statistic, nullHypothesis) =>
        new ChiSqTestResult(pValue, degreesOfFreedom, statistic, methodName, nullHypothesis)
      }
  }

  private[spark] def computeChiSquared(
      data: RDD[(Double, Vector)],
      methodName: String = PEARSON.name): RDD[(Int, Double, Int, Double, String)] = {
    data.first()._2 match {
      case dv: DenseVector =>
        chiSquaredDenseFeatures(data, dv.size, methodName)
      case sv: SparseVector =>
        chiSquaredSparseFeatures(data, sv.size, methodName)
    }
  }

  private def chiSquaredDenseFeatures(
      data: RDD[(Double, Vector)],
      numFeatures: Int,
      methodName: String = PEARSON.name): RDD[(Int, Double, Int, Double, String)] = {
    data.flatMap { case (label, features) =>
      require(features.size == numFeatures,
        s"Number of features must be $numFeatures but got ${features.size}")
      features.iterator.map { case (col, value) => (col, (label, value)) }
    }.aggregateByKey(new OpenHashMap[(Double, Double), Long])(
      seqOp = { case (counts, t) =>
        counts.changeValue(t, 1L, _ + 1L)
        counts
      },
      combOp = { case (counts1, counts2) =>
        counts2.foreach { case (t, c) => counts1.changeValue(t, c, _ + c) }
        counts1
      }
    ).map { case (col, counts) =>
      val result = computeChiSq(counts.toMap, methodName, col)
      (col, result.pValue, result.degreesOfFreedom, result.statistic, result.nullHypothesis)
    }
  }

  private def chiSquaredSparseFeatures(
      data: RDD[(Double, Vector)],
      numFeatures: Int,
      methodName: String = PEARSON.name): RDD[(Int, Double, Int, Double, String)] = {
    val labelCounts = data.map(_._1).countByValue().toMap
    val numInstances = labelCounts.valuesIterator.sum
    val numLabels = labelCounts.size
    if (numLabels > maxCategories) {
      throw new SparkException(s"Chi-square test expect factors (categorical values) but "
        + s"found more than $maxCategories distinct label values.")
    }

    val numParts = data.getNumPartitions
    data.mapPartitionsWithIndex { case (pid, iter) =>
      iter.flatMap { case (label, features) =>
        require(features.size == numFeatures,
          s"Number of features must be $numFeatures but got ${features.size}")
        features.nonZeroIterator.map { case (col, value) => (col, (label, value)) }
      } ++ {
        // append this to make sure that all columns are taken into account
        Iterator.range(pid, numFeatures, numParts).map(col => (col, null))
      }
    }.aggregateByKey(new OpenHashMap[(Double, Double), Long])(
      seqOp = { case (counts, labelAndValue) =>
        if (labelAndValue != null) counts.changeValue(labelAndValue, 1L, _ + 1L)
        counts
      },
      combOp = { case (counts1, counts2) =>
        counts2.foreach { case (t, c) => counts1.changeValue(t, c, _ + c) }
        counts1
      }
    ).map { case (col, counts) =>
      val nnz = counts.iterator.map(_._2).sum
      require(numInstances >= nnz)
      if (numInstances > nnz) {
        val labelNNZ = counts.iterator
          .map { case ((label, _), c) => (label, c) }
          .toArray
          .groupBy(_._1)
          .transform((_, v) => v.map(_._2).sum)
        labelCounts.foreach { case (label, countByLabel) =>
          val nnzByLabel = labelNNZ.getOrElse(label, 0L)
          val nzByLabel = countByLabel - nnzByLabel
          if (nzByLabel > 0) {
            counts.update((label, 0.0), nzByLabel)
          }
        }
      }

      val result = computeChiSq(counts.toMap, methodName, col)
      (col, result.pValue, result.degreesOfFreedom, result.statistic, result.nullHypothesis)
    }
  }

  private def computeChiSq(
      counts: Map[(Double, Double), Long],
      methodName: String,
      col: Int): ChiSqTestResult = {
    val label2Index = Utils.toMapWithIndex(counts.iterator.map(_._1._1).toArray.distinct.sorted)
    val numLabels = label2Index.size
    if (numLabels > maxCategories) {
      throw new SparkException(s"Chi-square test expect factors (categorical values) but "
        + s"found more than $maxCategories distinct label values.")
    }

    val value2Index = Utils.toMapWithIndex(counts.iterator.map(_._1._2).toArray.distinct.sorted)
    val numValues = value2Index.size
    if (numValues > maxCategories) {
      throw new SparkException(s"Chi-square test expect factors (categorical values) but "
        + s"found more than $maxCategories distinct values in column $col.")
    }

    val contingency = new DenseMatrix(numValues, numLabels,
      Array.ofDim[Double](numValues * numLabels))
    counts.foreach { case ((label, value), c) =>
      val i = value2Index(value)
      val j = label2Index(label)
      contingency.update(i, j, c.toDouble)
    }

    ChiSqTest.chiSquaredMatrix(contingency, methodName)
  }

  /*
   * Pearson's goodness of fit test on the input observed and expected counts/relative frequencies.
   * Uniform distribution is assumed when `expected` is not passed in.
   */
  def chiSquared(observed: Vector,
      expected: Vector = Vectors.dense(Array.emptyDoubleArray),
      methodName: String = PEARSON.name): ChiSqTestResult = {

    // Validate input arguments
    val method = methodFromString(methodName)
    if (expected.size != 0 && observed.size != expected.size) {
      throw new IllegalArgumentException("observed and expected must be of the same size.")
    }
    val size = observed.size
    if (size > 1000) {
      logWarning(log"Chi-squared approximation may not be accurate due to low expected " +
        log"frequencies as a result of a large number of categories: " +
        log"${MDC(LogKeys.NUM_CATEGORIES, size)}.")
    }
    val obsArr = observed.toArray
    val expArr = if (expected.size == 0) Array.tabulate(size)(_ => 1.0 / size) else expected.toArray
    if (!obsArr.forall(_ >= 0.0)) {
      throw new IllegalArgumentException("Negative entries disallowed in the observed vector.")
    }
    if (expected.size != 0 && ! expArr.forall(_ >= 0.0)) {
      throw new IllegalArgumentException("Negative entries disallowed in the expected vector.")
    }

    // Determine the scaling factor for expected
    val obsSum = obsArr.sum
    val expSum = if (expected.size == 0.0) 1.0 else expArr.sum
    val scale = if (math.abs(obsSum - expSum) < 1e-7) 1.0 else obsSum / expSum

    // compute chi-squared statistic
    val statistic = obsArr.zip(expArr).foldLeft(0.0) { case (stat, (obs, exp)) =>
      if (exp == 0.0) {
        if (obs == 0.0) {
          throw new IllegalArgumentException("Chi-squared statistic undefined for input vectors due"
            + " to 0.0 values in both observed and expected.")
        } else {
          return new ChiSqTestResult(0.0, size - 1, Double.PositiveInfinity, PEARSON.name,
            NullHypothesis.goodnessOfFit.toString)
        }
      }
      if (scale == 1.0) {
        stat + method.chiSqFunc(obs, exp)
      } else {
        stat + method.chiSqFunc(obs, exp * scale)
      }
    }
    val df = size - 1
    val pValue = 1.0 - new ChiSquaredDistribution(df).cumulativeProbability(statistic)
    new ChiSqTestResult(pValue, df, statistic, PEARSON.name, NullHypothesis.goodnessOfFit.toString)
  }

  /*
   * Pearson's independence test on the input contingency matrix.
   * TODO: optimize for SparseMatrix when it becomes supported.
   */
  def chiSquaredMatrix(counts: Matrix, methodName: String = PEARSON.name): ChiSqTestResult = {
    val method = methodFromString(methodName)
    val numRows = counts.numRows
    val numCols = counts.numCols

    // get row and column sums
    val colSums = new Array[Double](numCols)
    val rowSums = new Array[Double](numRows)
    val colMajorArr = counts.toArray
    val colMajorArrLen = colMajorArr.length

    var i = 0
    while (i < colMajorArrLen) {
      val elem = colMajorArr(i)
      if (elem < 0.0) {
        throw new IllegalArgumentException("Contingency table cannot contain negative entries.")
      }
      colSums(i / numRows) += elem
      rowSums(i % numRows) += elem
      i += 1
    }
    val total = colSums.sum

    // second pass to collect statistic
    var statistic = 0.0
    var j = 0
    while (j < colMajorArrLen) {
      val col = j / numRows
      val colSum = colSums(col)
      if (colSum == 0.0) {
        throw new IllegalArgumentException("Chi-squared statistic undefined for input matrix due to"
          + s"0 sum in column [$col].")
      }
      val row = j % numRows
      val rowSum = rowSums(row)
      if (rowSum == 0.0) {
        throw new IllegalArgumentException("Chi-squared statistic undefined for input matrix due to"
          + s"0 sum in row [$row].")
      }
      val expected = colSum * rowSum / total
      statistic += method.chiSqFunc(colMajorArr(j), expected)
      j += 1
    }
    val df = (numCols - 1) * (numRows - 1)
    if (df == 0) {
      // 1 column or 1 row. Constant distribution is independent of anything.
      // pValue = 1.0 and statistic = 0.0 in this case.
      new ChiSqTestResult(1.0, 0, 0.0, methodName, NullHypothesis.independence.toString)
    } else {
      val pValue = 1.0 - new ChiSquaredDistribution(df).cumulativeProbability(statistic)
      new ChiSqTestResult(pValue, df, statistic, methodName, NullHypothesis.independence.toString)
    }
  }
}
