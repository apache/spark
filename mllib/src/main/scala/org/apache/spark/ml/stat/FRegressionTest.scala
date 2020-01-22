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
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.mllib.stat.{Statistics => OldStatistics}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col


/**
 * F-Regression Test
 */
@Since("3.1.0")
object FRegressionTest {

  case class FRegressionTestResult(
      pValue: Double,
      degreesOfFreedom: Int,
      fValue: Double)

  /**
   * @param dataset  DataFrame of continuous labels and continuous features.
   * @param featuresCol  Name of features column in dataset, of type `Vector` (`VectorUDT`)
   * @param labelCol  Name of label column in dataset, of any numerical type
   * @return Array containing the FRegressionTestResult for every feature against the label.
   */
  @Since("3.1.0")
  def test_regression(dataset: Dataset[_], featuresCol: String, labelCol: String):
    Array[FRegressionTestResult] = {

    val spark = dataset.sparkSession
    import spark.implicits._

    SchemaUtils.checkColumnType(dataset.schema, featuresCol, new VectorUDT)
    SchemaUtils.checkNumericType(dataset.schema, labelCol)
    val rdd = dataset.select(col(labelCol).cast("double"), col(featuresCol)).as[(Double, Vector)]
      .rdd.map { case (label, features) => LabeledPoint(label, features) }

    val numOfFeatures = rdd.first().features.size
    val numOfSamples = rdd.count()
    val degreeOfFreedom = numOfSamples.toInt - 2

    var fTestResultArray = new Array[FRegressionTestResult](numOfFeatures)
    val labels = rdd.map(d => d.label)
    for (i <- 0 until numOfFeatures) {
      val feature = rdd.map(d => d.features.toArray(i))
      val corr = OldStatistics.corr(labels, feature)
      val fValue = corr * corr / (1 - corr * corr) * degreeOfFreedom
      val pValue = 1.0 - new FDistribution(1, degreeOfFreedom).cumulativeProbability(fValue)
      fTestResultArray(i) = new FRegressionTestResult(pValue, degreeOfFreedom, fValue)
    }

    fTestResultArray
  }
}
