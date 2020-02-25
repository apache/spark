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
import org.apache.spark.ml.linalg.{Vector, Vectors, VectorUDT}
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._


/**
 * FValue test for continuous data.
 */
@Since("3.1.0")
object FValueTest {

  /** Used to construct output schema of tests */
  private case class FValueResult(
      pValues: Vector,
      degreesOfFreedom: Array[Long],
      fValues: Vector)

  /**
   * @param dataset  DataFrame of continuous labels and continuous features.
   * @param featuresCol  Name of features column in dataset, of type `Vector` (`VectorUDT`)
   * @param labelCol  Name of label column in dataset, of any numerical type
   * @return DataFrame containing the test result for every feature against the label.
   *         This DataFrame will contain a single Row with the following fields:
   *          - `pValues: Vector`
   *          - `degreesOfFreedom: Array[Int]`
   *          - `fValues: Vector`
   *         Each of these fields has one value per feature.
   */
  @Since("3.1.0")
  def test(dataset: DataFrame, featuresCol: String, labelCol: String): DataFrame = {

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

    val pValues = Array.ofDim[Double](numFeatures)
    val degreesOfFreedoms = Array.fill(numFeatures)(degreesOfFreedom)
    val fValues = Array.ofDim[Double](numFeatures)

    val fd = new FDistribution(1, degreesOfFreedom)
    for (i <- 0 until numFeatures) {
      // Cov(X,Y) = Sum(((Xi - Avg(X)) * ((Yi-Avg(Y))) / (N-1)
      val covariance = sumForCov (i) / (numSamples - 1)
      val corr = covariance / (yStd * xStd(i))
      val fValue = corr * corr / (1 - corr * corr) * degreesOfFreedom
      pValues(i) = 1.0 - fd.cumulativeProbability(fValue)
      fValues(i) = fValue
    }

    spark.createDataFrame(
      Seq(FValueResult(Vectors.dense(pValues), degreesOfFreedoms, Vectors.dense(fValues))))
  }
}
