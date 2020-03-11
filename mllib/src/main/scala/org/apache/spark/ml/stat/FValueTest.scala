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
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.DataFrame

/**
 * FValue test for continuous data.
 */
@Since("3.1.0")
object FValueTest {

  /** Used to construct output schema of tests */
  private  case class FValueResult(
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
    val testResults = SelectionTest.fValueTest(dataset, featuresCol, labelCol)
    val pValues: Vector = Vectors.dense(testResults.map(_.pValue))
    val degreesOfFreedom: Array[Long] = testResults.map(_.degreesOfFreedom)
    val fValues: Vector = Vectors.dense(testResults.map(_.statistic))
    spark.createDataFrame(
      Seq(new FValueResult(pValues, degreesOfFreedom, fValues)))
  }
}
