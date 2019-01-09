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

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.linalg.{Vector, Vectors, VectorUDT}
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.mllib.linalg.{Vectors => OldVectors}
import org.apache.spark.mllib.regression.{LabeledPoint => OldLabeledPoint}
import org.apache.spark.mllib.stat.{Statistics => OldStatistics}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col


/**
 * :: Experimental ::
 *
 * Chi-square hypothesis testing for categorical data.
 *
 * See <a href="http://en.wikipedia.org/wiki/Chi-squared_test">Wikipedia</a> for more information
 * on the Chi-squared test.
 */
@Experimental
@Since("2.2.0")
object ChiSquareTest {

  /** Used to construct output schema of tests */
  private case class ChiSquareResult(
      pValues: Vector,
      degreesOfFreedom: Array[Int],
      statistics: Vector)

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
    val spark = dataset.sparkSession
    import spark.implicits._

    SchemaUtils.checkColumnType(dataset.schema, featuresCol, new VectorUDT)
    SchemaUtils.checkNumericType(dataset.schema, labelCol)
    val rdd = dataset.select(col(labelCol).cast("double"), col(featuresCol)).as[(Double, Vector)]
      .rdd.map { case (label, features) => OldLabeledPoint(label, OldVectors.fromML(features)) }
    val testResults = OldStatistics.chiSqTest(rdd)
    val pValues: Vector = Vectors.dense(testResults.map(_.pValue))
    val degreesOfFreedom: Array[Int] = testResults.map(_.degreesOfFreedom)
    val statistics: Vector = Vectors.dense(testResults.map(_.statistic))
    spark.createDataFrame(Seq(ChiSquareResult(pValues, degreesOfFreedom, statistics)))
  }
}
