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
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.{DenseVector, Vector, VectorUDT}
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.mllib.linalg.{Vectors => OldVectors}
import org.apache.spark.mllib.regression.{LabeledPoint => OldLabeledPoint}
import org.apache.spark.mllib.stat.{Statistics => OldStatistics}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions.col
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
  def chiSquareTest(dataset: Dataset[_], featuresCol: String, labelCol: String):
  Array[SelectionTestResult] = {

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
    var chiTestResultArray = new Array[SelectionTestResult](chiTestResult.length)
    for (i <- 0 until chiTestResult.length) {
      chiTestResultArray(i) = new ChiSqTestResult(chiTestResult(i).pValue,
        chiTestResult(i).degreesOfFreedom, chiTestResult(i).statistic)
    }
    chiTestResultArray
  }

  /**
   * @param dataset  DataFrame of continuous labels and continuous features.
   * @param featuresCol  Name of features column in dataset, of type `Vector` (`VectorUDT`)
   * @param labelCol  Name of label column in dataset, of any numerical type
   * @return Array containing the SelectionTestResult for every feature against the label.
   */
  @Since("3.1.0")
  def fValueRegressionTest(dataset: Dataset[_], featuresCol: String, labelCol: String):
    Array[SelectionTestResult] = {

    val spark = dataset.sparkSession
    import spark.implicits._

    SchemaUtils.checkColumnType(dataset.schema, featuresCol, new VectorUDT)
    SchemaUtils.checkNumericType(dataset.schema, labelCol)

    val yMean = dataset.select(col(labelCol)).as[Double].rdd.stats().mean

    val stats = dataset
      .select(Summarizer.metrics("mean", "std").summary(col("features")).as("summary"))
    val xMeans = stats.select("summary.mean").rdd.collect()(0).get(0).asInstanceOf[DenseVector]
      .toArray
    val xStdev = stats.select("summary.std").rdd.collect()(0).get(0).asInstanceOf[DenseVector]
      .toArray

    val labeledPointRdd = dataset.select(col("label").cast("double"), col("features"))
      .as[(Double, Vector)]
      .rdd.map { case (label, features) => LabeledPoint(label, features) }

    val numOfFeatures = labeledPointRdd.first().features.size
    val numOfSamples = labeledPointRdd.count()
    val degreeOfFreedom = numOfSamples.toInt - 2
    var fTestResultArray = new Array[SelectionTestResult](numOfFeatures)

    labeledPointRdd.flatMap { case LabeledPoint(label, features) =>
      features.iterator.map { case (col, value) =>
        (col, (value - xMeans(col.toInt), (label - yMean)))
      }
    }.aggregateByKey[(Double, Double)]((0.0, 0.0))(
      seqOp = {
        // sumForCov: sum((Xi - avg(X)) * ((Yi - avg (Y)))   X: feature, Y: Label
        //            It is used for calculating covariance
        // sumForYStdev: sum ((Yi - avg(Y))^2). It is used for calculating stdev of Y (label)
        case ((sumForCov: Double, sumForYStdev: Double), (features)) =>
          (sumForCov + features._1 * features._2, sumForYStdev + features._2 * features._2)
      },
      combOp = {
        case ((sumForCov1, sumForYStd1), (sumForCov2, sumForYStd2)) =>
          (sumForCov1 + sumForCov2, sumForYStd1 + sumForYStd2)
      }
    ).map {
      case (col, (sumForCov, sumForYStd)) =>
        val covariance = sumForCov / (numOfSamples - 1)
        val yStdev = math.sqrt(sumForYStd / (numOfSamples - 1))
        val corr = covariance / (yStdev * xStdev(col)) // correlation between feature and label
        val fValue = corr * corr / (1 - corr * corr) * degreeOfFreedom
        val pValue = 1.0 - new FDistribution(1, degreeOfFreedom).cumulativeProbability(fValue)
        (col, pValue, degreeOfFreedom, fValue)
    }.collect().sortBy(_._1).map {
      case (col, pValue, degreesOfFreedom, fValue) =>
        fTestResultArray(col) = new FValueRegressionTestResult(pValue, degreeOfFreedom, fValue)
    }
    fTestResultArray
  }
}
