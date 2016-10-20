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

package org.apache.spark.mllib.feature

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.util.Utils

class ChiSqSelectorSuite extends SparkFunSuite with MLlibTestSparkContext {

  /*
   *  Contingency tables
   *  feature0 = {6.0, 0.0, 8.0}
   *  class  0 1 2
   *    6.0||1|0|0|
   *    0.0||0|3|0|
   *    8.0||0|0|2|
   *  degree of freedom = 4, statistic = 12, pValue = 0.017
   *
   *  feature1 = {7.0, 9.0}
   *  class  0 1 2
   *    7.0||1|0|0|
   *    9.0||0|3|2|
   *  degree of freedom = 2, statistic = 6, pValue = 0.049
   *
   *  feature2 = {0.0, 6.0, 3.0, 8.0}
   *  class  0 1 2
   *    0.0||1|0|0|
   *    6.0||0|1|2|
   *    3.0||0|1|0|
   *    8.0||0|1|0|
   *  degree of freedom = 6, statistic = 8.66, pValue = 0.193
   *
   *  feature3 = {7.0, 0.0, 5.0, 4.0}
   *  class  0 1 2
   *    7.0||1|0|0|
   *    0.0||0|2|0|
   *    5.0||0|1|1|
   *    4.0||0|0|1|
   *  degree of freedom = 6, statistic = 9.5, pValue = 0.147
   *
   *  feature4 = {6.0, 5.0, 4.0, 0.0}
   *  class  0 1 2
   *    6.0||1|1|0|
   *    5.0||0|2|0|
   *    4.0||0|0|1|
   *    0.0||0|0|1|
   *  degree of freedom = 6, statistic = 8.0, pValue = 0.238
   *
   *  feature5 = {0.0, 9.0, 5.0, 4.0}
   *  class  0 1 2
   *    0.0||1|0|1|
   *    9.0||0|1|0|
   *    5.0||0|1|0|
   *    4.0||0|1|1|
   *  degree of freedom = 6, statistic = 5, pValue = 0.54
   *
   *  Use chi-squared calculator from Internet
   */

  test("ChiSqSelector transform KBest test (sparse & dense vector)") {
    val labeledDiscreteData = sc.parallelize(
      Seq(LabeledPoint(0.0, Vectors.sparse(6, Array((0, 6.0), (1, 7.0), (3, 7.0), (4, 6.0)))),
        LabeledPoint(1.0, Vectors.sparse(6, Array((1, 9.0), (2, 6.0), (4, 5.0), (5, 9.0)))),
        LabeledPoint(1.0, Vectors.sparse(6, Array((1, 9.0), (2, 3.0), (4, 5.0), (5, 5.0)))),
        LabeledPoint(1.0, Vectors.dense(Array(0.0, 9.0, 8.0, 5.0, 6.0, 4.0))),
        LabeledPoint(2.0, Vectors.dense(Array(8.0, 9.0, 6.0, 5.0, 4.0, 4.0))),
        LabeledPoint(2.0, Vectors.dense(Array(8.0, 9.0, 6.0, 4.0, 0.0, 0.0)))), 2)
    val preFilteredData =
      Set(LabeledPoint(0.0, Vectors.dense(Array(6.0))),
        LabeledPoint(1.0, Vectors.dense(Array(0.0))),
        LabeledPoint(1.0, Vectors.dense(Array(0.0))),
        LabeledPoint(1.0, Vectors.dense(Array(0.0))),
        LabeledPoint(2.0, Vectors.dense(Array(8.0))),
        LabeledPoint(2.0, Vectors.dense(Array(8.0))))
    val model = new ChiSqSelector(1).fit(labeledDiscreteData)
    val filteredData = labeledDiscreteData.map { lp =>
      LabeledPoint(lp.label, model.transform(lp.features))
    }.collect().toSet
    assert(filteredData == preFilteredData)

    val preFilteredData2 =
      Set(LabeledPoint(0.0, Vectors.dense(Array(6.0, 7.0, 7.0))),
        LabeledPoint(1.0, Vectors.dense(Array(0.0, 9.0, 0.0))),
        LabeledPoint(1.0, Vectors.dense(Array(0.0, 9.0, 0.0))),
        LabeledPoint(1.0, Vectors.dense(Array(0.0, 9.0, 5.0))),
        LabeledPoint(2.0, Vectors.dense(Array(8.0, 9.0, 5.0))),
        LabeledPoint(2.0, Vectors.dense(Array(8.0, 9.0, 4.0))))

    val model2 = new ChiSqSelector(3).fit(labeledDiscreteData)
    val filteredData2 = labeledDiscreteData.map { lp =>
      LabeledPoint(lp.label, model2.transform(lp.features))
    }.collect().toSet
    assert(filteredData2 == preFilteredData2)
  }

  test("ChiSqSelector transform Percentile test (sparse & dense vector)") {
    val labeledDiscreteData = sc.parallelize(
      Seq(LabeledPoint(0.0, Vectors.sparse(6, Array((0, 6.0), (1, 7.0), (3, 7.0), (4, 6.0)))),
        LabeledPoint(1.0, Vectors.sparse(6, Array((1, 9.0), (2, 6.0), (4, 5.0), (5, 9.0)))),
        LabeledPoint(1.0, Vectors.sparse(6, Array((1, 9.0), (2, 3.0), (4, 5.0), (5, 5.0)))),
        LabeledPoint(1.0, Vectors.dense(Array(0.0, 9.0, 8.0, 5.0, 6.0, 4.0))),
        LabeledPoint(2.0, Vectors.dense(Array(8.0, 9.0, 6.0, 5.0, 4.0, 4.0))),
        LabeledPoint(2.0, Vectors.dense(Array(8.0, 9.0, 6.0, 4.0, 0.0, 0.0)))), 2)
    val preFilteredData =
      Set(LabeledPoint(0.0, Vectors.dense(Array(6.0))),
        LabeledPoint(1.0, Vectors.dense(Array(0.0))),
        LabeledPoint(1.0, Vectors.dense(Array(0.0))),
        LabeledPoint(1.0, Vectors.dense(Array(0.0))),
        LabeledPoint(2.0, Vectors.dense(Array(8.0))),
        LabeledPoint(2.0, Vectors.dense(Array(8.0))))
    val model = new ChiSqSelector().setSelectorType("percentile").setPercentile(0.2)
      .fit(labeledDiscreteData)
    val filteredData = labeledDiscreteData.map { lp =>
      LabeledPoint(lp.label, model.transform(lp.features))
    }.collect().toSet
    assert(filteredData == preFilteredData)

    val preFilteredData2 =
      Set(LabeledPoint(0.0, Vectors.dense(Array(6.0, 7.0, 7.0))),
        LabeledPoint(1.0, Vectors.dense(Array(0.0, 9.0, 0.0))),
        LabeledPoint(1.0, Vectors.dense(Array(0.0, 9.0, 0.0))),
        LabeledPoint(1.0, Vectors.dense(Array(0.0, 9.0, 5.0))),
        LabeledPoint(2.0, Vectors.dense(Array(8.0, 9.0, 5.0))),
        LabeledPoint(2.0, Vectors.dense(Array(8.0, 9.0, 4.0))))

    val model2 = new ChiSqSelector().setSelectorType("percentile").setPercentile(0.5)
      .fit(labeledDiscreteData)
    val filteredData2 = labeledDiscreteData.map { lp =>
      LabeledPoint(lp.label, model2.transform(lp.features))
    }.collect().toSet
    assert(filteredData2 == preFilteredData2)
  }

  test("ChiSqSelector transform FPR test (sparse & dense vector)") {
    val labeledDiscreteData = sc.parallelize(
      Seq(LabeledPoint(0.0, Vectors.sparse(6, Array((0, 6.0), (1, 7.0), (3, 7.0), (4, 6.0)))),
        LabeledPoint(1.0, Vectors.sparse(6, Array((1, 9.0), (2, 6.0), (4, 5.0), (5, 9.0)))),
        LabeledPoint(1.0, Vectors.sparse(6, Array((1, 9.0), (2, 3.0), (4, 5.0), (5, 5.0)))),
        LabeledPoint(1.0, Vectors.dense(Array(0.0, 9.0, 8.0, 5.0, 6.0, 4.0))),
        LabeledPoint(2.0, Vectors.dense(Array(8.0, 9.0, 6.0, 5.0, 4.0, 4.0))),
        LabeledPoint(2.0, Vectors.dense(Array(8.0, 9.0, 6.0, 4.0, 0.0, 0.0)))), 2)
    val preFilteredData =
      Set(LabeledPoint(0.0, Vectors.dense(Array(6.0))),
        LabeledPoint(1.0, Vectors.dense(Array(0.0))),
        LabeledPoint(1.0, Vectors.dense(Array(0.0))),
        LabeledPoint(1.0, Vectors.dense(Array(0.0))),
        LabeledPoint(2.0, Vectors.dense(Array(8.0))),
        LabeledPoint(2.0, Vectors.dense(Array(8.0))))
    val model = new ChiSqSelector().setSelectorType("fpr").setAlpha(0.02)
      .fit(labeledDiscreteData)
    val filteredData = labeledDiscreteData.map { lp =>
      LabeledPoint(lp.label, model.transform(lp.features))
    }.collect().toSet
    assert(filteredData == preFilteredData)

    val preFilteredData2 =
      Set(LabeledPoint(0.0, Vectors.dense(Array(6.0, 7.0, 7.0))),
        LabeledPoint(1.0, Vectors.dense(Array(0.0, 9.0, 0.0))),
        LabeledPoint(1.0, Vectors.dense(Array(0.0, 9.0, 0.0))),
        LabeledPoint(1.0, Vectors.dense(Array(0.0, 9.0, 5.0))),
        LabeledPoint(2.0, Vectors.dense(Array(8.0, 9.0, 5.0))),
        LabeledPoint(2.0, Vectors.dense(Array(8.0, 9.0, 4.0))))

    val model2 = new ChiSqSelector().setSelectorType("fpr").setAlpha(0.15)
      .fit(labeledDiscreteData)
    val filteredData2 = labeledDiscreteData.map { lp =>
      LabeledPoint(lp.label, model2.transform(lp.features))
    }.collect().toSet
    assert(filteredData2 == preFilteredData2)
  }

  test("ChiSqSelector transform FDR test (sparse & dense vector)") {
    val labeledDiscreteData = sc.parallelize(
      Seq(LabeledPoint(0.0, Vectors.sparse(6, Array((0, 6.0), (1, 7.0), (3, 7.0), (4, 6.0)))),
        LabeledPoint(1.0, Vectors.sparse(6, Array((1, 9.0), (2, 6.0), (4, 5.0), (5, 9.0)))),
        LabeledPoint(1.0, Vectors.sparse(6, Array((1, 9.0), (2, 3.0), (4, 5.0), (5, 5.0)))),
        LabeledPoint(1.0, Vectors.dense(Array(0.0, 9.0, 8.0, 5.0, 6.0, 4.0))),
        LabeledPoint(2.0, Vectors.dense(Array(8.0, 9.0, 6.0, 5.0, 4.0, 4.0))),
        LabeledPoint(2.0, Vectors.dense(Array(8.0, 9.0, 6.0, 4.0, 0.0, 0.0)))), 2)
    val preFilteredData =
      Set(LabeledPoint(0.0, Vectors.dense(Array(6.0))),
        LabeledPoint(1.0, Vectors.dense(Array(0.0))),
        LabeledPoint(1.0, Vectors.dense(Array(0.0))),
        LabeledPoint(1.0, Vectors.dense(Array(0.0))),
        LabeledPoint(2.0, Vectors.dense(Array(8.0))),
        LabeledPoint(2.0, Vectors.dense(Array(8.0))))
    val model = new ChiSqSelector().setSelectorType("fdr").setAlpha(0.12)
      .fit(labeledDiscreteData)
    val filteredData = labeledDiscreteData.map { lp =>
      LabeledPoint(lp.label, model.transform(lp.features))
    }.collect().toSet
    assert(filteredData == preFilteredData)

    val preFilteredData2 =
      Set(LabeledPoint(0.0, Vectors.dense(Array(6.0, 7.0))),
        LabeledPoint(1.0, Vectors.dense(Array(0.0, 9.0))),
        LabeledPoint(1.0, Vectors.dense(Array(0.0, 9.0))),
        LabeledPoint(1.0, Vectors.dense(Array(0.0, 9.0))),
        LabeledPoint(2.0, Vectors.dense(Array(8.0, 9.0))),
        LabeledPoint(2.0, Vectors.dense(Array(8.0, 9.0))))

    val model2 = new ChiSqSelector().setSelectorType("fdr").setAlpha(0.15)
      .fit(labeledDiscreteData)
    val filteredData2 = labeledDiscreteData.map { lp =>
      LabeledPoint(lp.label, model2.transform(lp.features))
    }.collect().toSet
    assert(filteredData2 == preFilteredData2)
  }

  test("ChiSqSelector transform FWE test (sparse & dense vector)") {
    val labeledDiscreteData = sc.parallelize(
      Seq(LabeledPoint(0.0, Vectors.sparse(6, Array((0, 6.0), (1, 7.0), (3, 7.0), (4, 6.0)))),
        LabeledPoint(1.0, Vectors.sparse(6, Array((1, 9.0), (2, 6.0), (4, 5.0), (5, 9.0)))),
        LabeledPoint(1.0, Vectors.sparse(6, Array((1, 9.0), (2, 3.0), (4, 5.0), (5, 5.0)))),
        LabeledPoint(1.0, Vectors.dense(Array(0.0, 9.0, 8.0, 5.0, 6.0, 4.0))),
        LabeledPoint(2.0, Vectors.dense(Array(8.0, 9.0, 6.0, 5.0, 4.0, 4.0))),
        LabeledPoint(2.0, Vectors.dense(Array(8.0, 9.0, 6.0, 4.0, 0.0, 0.0)))), 2)
    val preFilteredData =
      Set(LabeledPoint(0.0, Vectors.dense(Array(6.0))),
        LabeledPoint(1.0, Vectors.dense(Array(0.0))),
        LabeledPoint(1.0, Vectors.dense(Array(0.0))),
        LabeledPoint(1.0, Vectors.dense(Array(0.0))),
        LabeledPoint(2.0, Vectors.dense(Array(8.0))),
        LabeledPoint(2.0, Vectors.dense(Array(8.0))))
    val model = new ChiSqSelector().setSelectorType("fwe").setAlpha(0.15)
      .fit(labeledDiscreteData)
    val filteredData = labeledDiscreteData.map { lp =>
      LabeledPoint(lp.label, model.transform(lp.features))
    }.collect().toSet
    assert(filteredData == preFilteredData)

    val preFilteredData2 =
      Set(LabeledPoint(0.0, Vectors.dense(Array(6.0, 7.0))),
        LabeledPoint(1.0, Vectors.dense(Array(0.0, 9.0))),
        LabeledPoint(1.0, Vectors.dense(Array(0.0, 9.0))),
        LabeledPoint(1.0, Vectors.dense(Array(0.0, 9.0))),
        LabeledPoint(2.0, Vectors.dense(Array(8.0, 9.0))),
        LabeledPoint(2.0, Vectors.dense(Array(8.0, 9.0))))

    val model2 = new ChiSqSelector().setSelectorType("fwe").setAlpha(0.3)
      .fit(labeledDiscreteData)
    val filteredData2 = labeledDiscreteData.map { lp =>
      LabeledPoint(lp.label, model2.transform(lp.features))
    }.collect().toSet
    assert(filteredData2 == preFilteredData2)
  }

  test("model load / save") {
    val model = ChiSqSelectorSuite.createModel()
    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString
    try {
      model.save(sc, path)
      val sameModel = ChiSqSelectorModel.load(sc, path)
      ChiSqSelectorSuite.checkEqual(model, sameModel)
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }
}

object ChiSqSelectorSuite extends SparkFunSuite {

  def createModel(): ChiSqSelectorModel = {
    val arr = Array(1, 2, 3, 4)
    new ChiSqSelectorModel(arr)
  }

  def checkEqual(a: ChiSqSelectorModel, b: ChiSqSelectorModel): Unit = {
    assert(a.selectedFeatures.deep == b.selectedFeatures.deep)
  }
}
