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

package org.apache.spark.ml.stat

import org.apache.commons.math3.distribution.ChiSquaredDistribution

import org.apache.spark.SparkException
import org.apache.spark.annotation.Since
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.util.collection.OpenHashMap


/**
 * Chi-square hypothesis testing for categorical data.
 *
 * See <a href="http://en.wikipedia.org/wiki/Chi-squared_test">Wikipedia</a> for more information
 * on the Chi-squared test.
 */
@Since("2.2.0")
object ChiSquareTest {

  /** Used to construct output schema of tests */
  private case class ChiSquareResult(
      pValues: Vector,
      degreesOfFreedom: Array[Int],
      statistics: Vector)

  private[ml] val maxCategories: Int = 10000

  /**
   * Conduct Pearson's independence test for every feature against the label. For each feature, the
   * (feature, label) pairs are converted into a contingency matrix for which the Chi-squared
   * statistic is computed. All label and feature values must be categorical.
   *
   * The null hypothesis is that the occurrence of the outcomes is statistically independent.
   *
   * @param dataset  DataFrame of categorical labels and categorical features.
   *                 Real-valued features will be treated as categorical for each distinct value.
   * @param featuresCol  Name of features column in dataset, of type `Vector` (`VectorUDT`)
   * @param labelCol  Name of label column in dataset, of any numerical type
   * @return DataFrame containing the test result for every feature against the label.
   *         This DataFrame will contain a single Row with the following fields:
   *          - `pValues: Vector`
   *          - `degreesOfFreedom: Array[Int]`
   *          - `statistics: Vector`
   *         Each of these fields has one value per feature.
   */
  @Since("2.2.0")
  def test(
      dataset: DataFrame,
      featuresCol: String,
      labelCol: String): DataFrame = {
    SchemaUtils.checkColumnType(dataset.schema, featuresCol, new VectorUDT)
    SchemaUtils.checkNumericType(dataset.schema, labelCol)

    val spark = dataset.sparkSession
    val results = testChiSquare(dataset, featuresCol, labelCol)
    val pValues = Vectors.dense(results.map(_.pValue))
    val degreesOfFreedom = results.map(_.degreesOfFreedom.toInt)
    val statistics = Vectors.dense(results.map(_.statistic))
    spark.createDataFrame(Seq(ChiSquareResult(pValues, degreesOfFreedom, statistics)))
  }

  /**
   * @param dataset  DataFrame of categorical labels and categorical features.
   *                 Real-valued features will be treated as categorical for each distinct value.
   * @param featuresCol  Name of features column in dataset, of type `Vector` (`VectorUDT`)
   * @param labelCol  Name of label column in dataset, of any numerical type
   * @return Array containing the SelectionTestResult for every feature against the label.
   */
  @Since("3.1.0")
  def testChiSquare(
      dataset: Dataset[_],
      featuresCol: String,
      labelCol: String): Array[SelectionTestResult] = {
    SchemaUtils.checkColumnType(dataset.schema, featuresCol, new VectorUDT)
    SchemaUtils.checkNumericType(dataset.schema, labelCol)

    val points = dataset.select(col(labelCol).cast(DoubleType), col(featuresCol)).rdd
      .map { case Row(label: Double, features: Vector) => (label, features) }
    chiSquared(points)
  }

  private[spark] def chiSquared(points: RDD[(Double, Vector)]): Array[SelectionTestResult] = {
    val resultRDD = points.first()._2 match {
      case dv: DenseVector =>
        chiSquaredDenseFeatures(points, dv.size)
      case sv: SparseVector =>
        chiSquaredSparseFeatures(points, sv.size)
    }

    resultRDD.collect().sortBy(_._1).map {
      case (_, pValue, degreesOfFreedom, statistic) =>
        new ChiSqTestResult(pValue, degreesOfFreedom, statistic)
    }
  }

  private def chiSquaredDenseFeatures(
      points: RDD[(Double, Vector)],
      numFeatures: Int): RDD[(Int, Double, Long, Double)] = {
    points.flatMap { case (label, features) =>
      require(features.size == numFeatures,
        s"Number of features must be $numFeatures but got ${features.size}")
      features.iterator.map { case (col, value) => (col, (label, value)) }
    }.aggregateByKey(new OpenHashMap[(Double, Double), Long])(
      seqOp = { case (counts, labelAndValue) =>
        counts.changeValue(labelAndValue, 1L, _ + 1L)
        counts
      },
      combOp = { case (counts1, counts2) =>
        counts2.foreach { case (t, c) => counts1.changeValue(t, c, _ + c) }
        counts1
      }
    ).map { case (col, counts) =>
      val result = computeChiSq(counts.toMap, col)
      (col, result.pValue, result.degreesOfFreedom, result.statistic)
    }
  }

  private def chiSquaredSparseFeatures(
      points: RDD[(Double, Vector)],
      numFeatures: Int): RDD[(Int, Double, Long, Double)] = {
    val labelCounts = points.map(_._1).countByValue().toMap
    val numInstances = labelCounts.valuesIterator.sum
    val numLabels = labelCounts.size
    if (numLabels > maxCategories) {
      throw new SparkException(s"Chi-square test expect factors (categorical values) but "
        + s"found more than $maxCategories distinct label values.")
    }

    val numParts = points.getNumPartitions
    points.mapPartitionsWithIndex { case (pid, iter) =>
      iter.flatMap { case (label, features) =>
        require(features.size == numFeatures,
          s"Number of features must be $numFeatures but got ${features.size}")
        features.nonZeroIterator.map { case (col, value) => (col, (label, value)) }
      } ++ {
        // append this to make sure that all columns are taken into account
        Iterator.range(pid, numFeatures, numParts)
          .map(col => (col, null))
      }
    }.aggregateByKey(new OpenHashMap[(Double, Double), Long])(
      seqOp = { case (counts, labelAndValue) =>
        if (labelAndValue != null) {
          counts.changeValue(labelAndValue, 1L, _ + 1L)
        }
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
          .mapValues(_.map(_._2).sum)
        labelCounts.foreach { case (label, countByLabel) =>
          val nnzByLabel = labelNNZ.getOrElse(label, 0L)
          val nzByLabel = countByLabel - nnzByLabel
          if (nzByLabel > 0) {
            counts.update((label, 0.0), nzByLabel)
          }
        }
      }

      val result = computeChiSq(counts.toMap, col)
      (col, result.pValue, result.degreesOfFreedom, result.statistic)
    }
  }

  private def computeChiSq(
      counts: Map[(Double, Double), Long],
      col: Int): ChiSqTestResult = {
    val label2Index = counts.iterator.map(_._1._1).toArray.distinct.sorted.zipWithIndex.toMap
    val numLabels = label2Index.size
    if (numLabels > maxCategories) {
      throw new SparkException(s"Chi-square test expect factors (categorical values) but "
        + s"found more than $maxCategories distinct label values.")
    }

    val value2Index = counts.iterator.map(_._1._2).toArray.distinct.sorted.zipWithIndex.toMap
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
      contingency.update(i, j, c)
    }

    chiSquaredMatrix(contingency)
  }

  /*
   * Pearson's independence test on the input contingency matrix.
   */
  private def chiSquaredMatrix(counts: Matrix): ChiSqTestResult = {
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
      statistic += chiSqFunc(colMajorArr(j), expected)
      j += 1
    }
    val df = (numCols - 1) * (numRows - 1)
    if (df == 0) {
      // 1 column or 1 row. Constant distribution is independent of anything.
      // pValue = 1.0 and statistic = 0.0 in this case.
      new ChiSqTestResult(1.0, 0, 0.0)
    } else {
      val pValue = 1.0 - new ChiSquaredDistribution(df).cumulativeProbability(statistic)
      new ChiSqTestResult(pValue, df, statistic)
    }
  }

  private def chiSqFunc(observed: Double, expected: Double): Double = {
    val dev = observed - expected
    dev * dev / expected
  }
}
