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

package org.apache.spark.ml.feature

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{Dataset, Row}

class ChiSqSelectorSuite extends SparkFunSuite with MLlibTestSparkContext
  with DefaultReadWriteTest {

  @transient var dataset: Dataset[_] = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Toy dataset, including the top feature for a chi-squared test.
    // These data are chosen such that each feature's test has a distinct p-value.
    /*
     *  Contingency tables
     *  feature1 = {6.0, 0.0, 8.0}
     *  class  0 1 2
     *    6.0||1|0|0|
     *    0.0||0|3|0|
     *    8.0||0|0|2|
     *  degree of freedom = 4, statistic = 12, pValue = 0.017
     *
     *  feature2 = {7.0, 9.0}
     *  class  0 1 2
     *    7.0||1|0|0|
     *    9.0||0|3|2|
     *  degree of freedom = 2, statistic = 6, pValue = 0.049
     *
     *  feature3 = {0.0, 6.0, 3.0, 8.0}
     *  class  0 1 2
     *    0.0||1|0|0|
     *    6.0||0|1|2|
     *    3.0||0|1|0|
     *    8.0||0|1|0|
     *  degree of freedom = 6, statistic = 8.66, pValue = 0.193
     *
     *  feature4 = {7.0, 0.0, 5.0, 4.0}
     *  class  0 1 2
     *    7.0||1|0|0|
     *    0.0||0|2|0|
     *    5.0||0|1|1|
     *    4.0||0|0|1|
     *  degree of freedom = 6, statistic = 9.5, pValue = 0.147
     *
     *  feature5 = {6.0, 5.0, 4.0, 0.0}
     *  class  0 1 2
     *    6.0||1|1|0|
     *    5.0||0|2|0|
     *    4.0||0|0|1|
     *    0.0||0|0|1|
     *  degree of freedom = 6, statistic = 8.0, pValue = 0.238
     *
     *  feature6 = {0.0, 9.0, 5.0, 4.0}
     *  class  0 1 2
     *    0.0||1|0|1|
     *    9.0||0|1|0|
     *    5.0||0|1|0|
     *    4.0||0|1|1|
     *  degree of freedom = 6, statistic = 5, pValue = 0.54
     *
     *  To verify the results with R, run:
     *  library(stats)
     *  x1 <- c(6.0, 0.0, 0.0, 0.0, 8.0, 8.0)
     *  x2 <- c(7.0, 9.0, 9.0, 9.0, 9.0, 9.0)
     *  x3 <- c(0.0, 6.0, 3.0, 8.0, 6.0, 6.0)
     *  x4 <- c(7.0, 0.0, 0.0, 5.0, 5.0, 4.0)
     *  x5 <- c(6.0, 5.0, 5.0, 6.0, 4.0, 0.0)
     *  x6 <- c(0.0, 9.0, 5.0, 4.0, 4.0, 0.0)
     *  y <- c(0.0, 1.0, 1.0, 1.0, 2.0, 2.0)
     *  chisq.test(x1,y)
     *  chisq.test(x2,y)
     *  chisq.test(x3,y)
     *  chisq.test(x4,y)
     *  chisq.test(x5,y)
     *  chisq.test(x6,y)
     */

    dataset = spark.createDataFrame(Seq(
      (0.0, Vectors.sparse(6, Array((0, 6.0), (1, 7.0), (3, 7.0), (4, 6.0))), Vectors.dense(6.0)),
      (1.0, Vectors.sparse(6, Array((1, 9.0), (2, 6.0), (4, 5.0), (5, 9.0))), Vectors.dense(0.0)),
      (1.0, Vectors.sparse(6, Array((1, 9.0), (2, 3.0), (4, 5.0), (5, 5.0))), Vectors.dense(0.0)),
      (1.0, Vectors.dense(Array(0.0, 9.0, 8.0, 5.0, 6.0, 4.0)), Vectors.dense(0.0)),
      (2.0, Vectors.dense(Array(8.0, 9.0, 6.0, 5.0, 4.0, 4.0)), Vectors.dense(8.0)),
      (2.0, Vectors.dense(Array(8.0, 9.0, 6.0, 4.0, 0.0, 0.0)), Vectors.dense(8.0))
    )).toDF("label", "features", "topFeature")
  }

  test("params") {
    ParamsSuite.checkParams(new ChiSqSelector)
    val model = new ChiSqSelectorModel("myModel",
      new org.apache.spark.mllib.feature.ChiSqSelectorModel(Array(1, 3, 4)))
    ParamsSuite.checkParams(model)
  }

  test("Test Chi-Square selector: numTopFeatures") {
    val selector = new ChiSqSelector()
      .setOutputCol("filtered").setSelectorType("numTopFeatures").setNumTopFeatures(1)
    val model = ChiSqSelectorSuite.testSelector(selector, dataset)
    MLTestingUtils.checkCopyAndUids(selector, model)
  }

  test("Test Chi-Square selector: percentile") {
    val selector = new ChiSqSelector()
      .setOutputCol("filtered").setSelectorType("percentile").setPercentile(0.17)
    ChiSqSelectorSuite.testSelector(selector, dataset)
  }

  test("Test Chi-Square selector: fpr") {
    val selector = new ChiSqSelector()
      .setOutputCol("filtered").setSelectorType("fpr").setFpr(0.02)
    ChiSqSelectorSuite.testSelector(selector, dataset)
  }

  test("Test Chi-Square selector: fdr") {
    val selector = new ChiSqSelector()
      .setOutputCol("filtered").setSelectorType("fdr").setFdr(0.12)
    ChiSqSelectorSuite.testSelector(selector, dataset)
  }

  test("Test Chi-Square selector: fwe") {
    val selector = new ChiSqSelector()
      .setOutputCol("filtered").setSelectorType("fwe").setFwe(0.12)
    ChiSqSelectorSuite.testSelector(selector, dataset)
  }

  test("read/write") {
    def checkModelData(model: ChiSqSelectorModel, model2: ChiSqSelectorModel): Unit = {
      assert(model.selectedFeatures === model2.selectedFeatures)
    }
    val nb = new ChiSqSelector
    testEstimatorAndModelReadWrite(nb, dataset, ChiSqSelectorSuite.allParamSettings,
      ChiSqSelectorSuite.allParamSettings, checkModelData)
  }

  test("should support all NumericType labels and not support other types") {
    val css = new ChiSqSelector()
    MLTestingUtils.checkNumericTypes[ChiSqSelectorModel, ChiSqSelector](
      css, spark) { (expected, actual) =>
        assert(expected.selectedFeatures === actual.selectedFeatures)
      }
  }
}

object ChiSqSelectorSuite {

  private def testSelector(selector: ChiSqSelector, dataset: Dataset[_]): ChiSqSelectorModel = {
    val selectorModel = selector.fit(dataset)
    selectorModel.transform(dataset).select("filtered", "topFeature").collect()
      .foreach { case Row(vec1: Vector, vec2: Vector) =>
        assert(vec1 ~== vec2 absTol 1e-1)
      }
    selectorModel
  }

  /**
   * Mapping from all Params to valid settings which differ from the defaults.
   * This is useful for tests which need to exercise all Params, such as save/load.
   * This excludes input columns to simplify some tests.
   */
  val allParamSettings: Map[String, Any] = Map(
    "selectorType" -> "percentile",
    "numTopFeatures" -> 1,
    "percentile" -> 0.12,
    "outputCol" -> "myOutput"
  )
}
