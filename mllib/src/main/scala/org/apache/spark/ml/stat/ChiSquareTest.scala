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

import org.apache.spark.annotation.Since
import org.apache.spark.ml.linalg.{Vector, Vectors, VectorUDT}
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.mllib.linalg.{Vectors => OldVectors}
import org.apache.spark.mllib.stat.test.{ChiSqTest => OldChiSqTest}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._


/**
 * Chi-square hypothesis testing for categorical data.
 *
 * See <a href="http://en.wikipedia.org/wiki/Chi-squared_test">Wikipedia</a> for more information
 * on the Chi-squared test.
 */
@Since("2.2.0")
object ChiSquareTest {

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
  def test(dataset: DataFrame, featuresCol: String, labelCol: String): DataFrame = {
    test(dataset, featuresCol, labelCol, false)
  }

  /**
   * @param dataset  DataFrame of categorical labels and categorical features.
   *                 Real-valued features will be treated as categorical for each distinct value.
   * @param featuresCol  Name of features column in dataset, of type `Vector` (`VectorUDT`)
   * @param labelCol  Name of label column in dataset, of any numerical type
   * @param flatten  If false, the returned DataFrame contains only a single Row, otherwise, one
   *                 row per feature.
   */
  @Since("3.1.0")
  def test(
      dataset: DataFrame,
      featuresCol: String,
      labelCol: String,
      flatten: Boolean): DataFrame = {
    SchemaUtils.checkColumnType(dataset.schema, featuresCol, new VectorUDT)
    SchemaUtils.checkNumericType(dataset.schema, labelCol)

    val spark = dataset.sparkSession
    import spark.implicits._

    val data = dataset.select(col(labelCol).cast("double"), col(featuresCol)).rdd
      .map { case Row(label: Double, vec: Vector) => (label, OldVectors.fromML(vec)) }

    val resultDF = OldChiSqTest.computeChiSquared(data)
      .map { case (col, pValue, degreesOfFreedom, statistic, _) =>
        (col, pValue, degreesOfFreedom, statistic)
      }.toDF("featureIndex", "pValue", "degreesOfFreedom", "statistic")

    if (flatten) {
      resultDF
    } else {
      resultDF.agg(collect_list(struct("*")))
        .as[Seq[(Int, Double, Int, Double)]]
        .map { seq =>
          val results = seq.toArray.sortBy(_._1)
          val pValues = Vectors.dense(results.map(_._2))
          val degreesOfFreedom = results.map(_._3)
          val statistics = Vectors.dense(results.map(_._4))
          (pValues, degreesOfFreedom, statistics)
        }.toDF("pValues", "degreesOfFreedom", "statistics")
    }
  }
}
