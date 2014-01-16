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

package org.apache.spark.mllib.util

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import org.jblas.DoubleMatrix
import org.apache.spark.mllib.regression.LabeledPoint

/**
 * Helper methods to load, save and pre-process data used in ML Lib.
 */
object MLUtils {

  /**
   * Load labeled data from a file. The data format used here is
   * <L>, <f1> <f2> ...
   * where <f1>, <f2> are feature values in Double and <L> is the corresponding label as Double.
   *
   * @param sc SparkContext
   * @param dir Directory to the input data files.
   * @return An RDD of LabeledPoint. Each labeled point has two elements: the first element is
   *         the label, and the second element represents the feature values (an array of Double).
   */
  def loadLabeledData(sc: SparkContext, dir: String): RDD[LabeledPoint] = {
    sc.textFile(dir).map { line =>
      val parts = line.split(',')
      val label = parts(0).toDouble
      val features = parts(1).trim().split(' ').map(_.toDouble)
      LabeledPoint(label, features)
    }
  }

  /**
   * Save labeled data to a file. The data format used here is
   * <L>, <f1> <f2> ...
   * where <f1>, <f2> are feature values in Double and <L> is the corresponding label as Double.
   *
   * @param data An RDD of LabeledPoints containing data to be saved.
   * @param dir Directory to save the data.
   */
  def saveLabeledData(data: RDD[LabeledPoint], dir: String) {
    val dataStr = data.map(x => x.label + "," + x.features.mkString(" "))
    dataStr.saveAsTextFile(dir)
  }

  /**
   * Utility function to compute mean and standard deviation on a given dataset.
   *
   * @param data - input data set whose statistics are computed
   * @param nfeatures - number of features
   * @param nexamples - number of examples in input dataset
   *
   * @return (yMean, xColMean, xColSd) - Tuple consisting of
   *     yMean - mean of the labels
   *     xColMean - Row vector with mean for every column (or feature) of the input data
   *     xColSd - Row vector standard deviation for every column (or feature) of the input data.
   */
  def computeStats(data: RDD[LabeledPoint], nfeatures: Int, nexamples: Long):
      (Double, DoubleMatrix, DoubleMatrix) = {
    val yMean: Double = data.map { labeledPoint => labeledPoint.label }.reduce(_ + _) / nexamples

    // NOTE: We shuffle X by column here to compute column sum and sum of squares.
    val xColSumSq: RDD[(Int, (Double, Double))] = data.flatMap { labeledPoint =>
      val nCols = labeledPoint.features.length
      // Traverse over every column and emit (col, value, value^2)
      Iterator.tabulate(nCols) { i =>
        (i, (labeledPoint.features(i), labeledPoint.features(i)*labeledPoint.features(i)))
      }
    }.reduceByKey { case(x1, x2) =>
      (x1._1 + x2._1, x1._2 + x2._2)
    }
    val xColSumsMap = xColSumSq.collectAsMap()

    val xColMean = DoubleMatrix.zeros(nfeatures, 1)
    val xColSd = DoubleMatrix.zeros(nfeatures, 1)

    // Compute mean and unbiased variance using column sums
    var col = 0
    while (col < nfeatures) {
      xColMean.put(col, xColSumsMap(col)._1 / nexamples)
      val variance =
        (xColSumsMap(col)._2 - (math.pow(xColSumsMap(col)._1, 2) / nexamples)) / nexamples
      xColSd.put(col, math.sqrt(variance))
      col += 1
    }

    (yMean, xColMean, xColSd)
  }

  /**
   * Return the squared Euclidean distance between two vectors.
   */
  def squaredDistance(v1: Array[Double], v2: Array[Double]): Double = {
    if (v1.length != v2.length) {
      throw new IllegalArgumentException("Vector sizes don't match")
    }
    var i = 0
    var sum = 0.0
    while (i < v1.length) {
      sum += (v1(i) - v2(i)) * (v1(i) - v2(i))
      i += 1
    }
    sum
  }
}
