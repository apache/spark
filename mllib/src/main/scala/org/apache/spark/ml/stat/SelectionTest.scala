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

import org.apache.commons.math3.distribution.FDistribution

import org.apache.spark.annotation.Since
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.mllib.linalg.{Vectors => OldVectors}
import org.apache.spark.mllib.regression.{LabeledPoint => OldLabeledPoint}
import org.apache.spark.mllib.stat.{Statistics => OldStatistics}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType


@Since("3.1.0")
object SelectionTest {

  /**
   * @param dataset  DataFrame of categorical labels and categorical features.
   *                 Real-valued features will be treated as categorical for each distinct value.
   * @param featuresCol  Name of features column in dataset, of type `Vector` (`VectorUDT`)
   * @param labelCol  Name of label column in dataset, of any numerical type
   * @return Array containing the SelectionTestResult for every feature against the label.
   */
  @Since("3.1.0")
  def chiSquareTest(
      dataset: Dataset[_],
      featuresCol: String,
      labelCol: String): Array[SelectionTestResult] = {

    val spark = dataset.sparkSession

    SchemaUtils.checkColumnType(dataset.schema, featuresCol, new VectorUDT)
    SchemaUtils.checkNumericType(dataset.schema, labelCol)
    val input: RDD[OldLabeledPoint] =
      dataset.select(col(labelCol).cast(DoubleType), col(featuresCol)).rdd
        .map {
        case Row(label: Double, features: Vector) =>
          OldLabeledPoint(label, OldVectors.fromML(features))
      }
    val chiTestResult = OldStatistics.chiSqTest(input)
    chiTestResult.map(r => new ChiSqTestResult(r.pValue, r.degreesOfFreedom, r.statistic))
  }

  /**
   * @param dataset  DataFrame of continuous labels and continuous features.
   * @param featuresCol  Name of features column in dataset, of type `Vector` (`VectorUDT`)
   * @param labelCol  Name of label column in dataset, of any numerical type
   * @return Array containing the SelectionTestResult for every feature against the label.
   */
  @Since("3.1.0")
  def fValueTest(
      dataset: Dataset[_],
      featuresCol: String,
      labelCol: String): Array[SelectionTestResult] = {

    val spark = dataset.sparkSession
    import spark.implicits._

    SchemaUtils.checkColumnType(dataset.schema, featuresCol, new VectorUDT)
    SchemaUtils.checkNumericType(dataset.schema, labelCol)

    val Row(xMeans: Vector, xStd: Vector, yMean: Double, yStd: Double, count: Long) = dataset
      .select(Summarizer.metrics("mean", "std", "count").summary(col(featuresCol)).as("summary"),
        avg(col(labelCol)).as("yMean"),
        stddev(col(labelCol)).as("yStd"))
      .select("summary.mean", "summary.std", "yMean", "yStd", "summary.count")
      .first()

    val labeledPointRdd = dataset.select(col("label").cast("double"), col("features"))
      .as[(Double, Vector)].rdd

    val numFeatures = xMeans.size
    val numSamples = count
    val degreesOfFreedom = numSamples - 2

    // Use two pass equation Cov[X,Y] = E[(X - E[X]) * (Y - E[Y])] to compute covariance because
    // one pass equation Cov[X,Y] = E[XY] - E[X]E[Y] is susceptible to catastrophic cancellation
    // sumForCov = Sum(((Xi - Avg(X)) * ((Yi-Avg(Y)))
    val sumForCov = labeledPointRdd.mapPartitions { iter =>
      if (iter.hasNext) {
        val array = Array.ofDim[Double](numFeatures)
        while (iter.hasNext) {
          val (label, features) = iter.next
          val yDiff = label - yMean
          if (yDiff != 0) {
            features.iterator.zip(xMeans.iterator)
              .foreach { case ((col, x), (_, xMean)) => array(col) += yDiff * (x - xMean) }
          }
        }
        Iterator.single(array)
      } else Iterator.empty
    }.treeReduce { case (array1, array2) =>
      var i = 0
      while (i < numFeatures) {
        array1(i) += array2(i)
        i += 1
      }
      array1
    }
    var fTestResultArray = new Array[SelectionTestResult](numFeatures)

    val fd = new FDistribution(1, degreesOfFreedom)
    for (i <- 0 until numFeatures) {
      // Cov(X,Y) = Sum(((Xi - Avg(X)) * ((Yi-Avg(Y))) / (N-1)
      val covariance = sumForCov (i) / (numSamples - 1)
      val corr = covariance / (yStd * xStd(i))
      val fValue = corr * corr / (1 - corr * corr) * degreesOfFreedom
      val pValue = 1.0 - fd.cumulativeProbability(fValue)
      fTestResultArray(i) = new FValueTestResult(pValue, degreesOfFreedom, fValue)
    }
    fTestResultArray
  }
}
