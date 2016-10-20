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
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.Row

class ChiSqSelectorSuite extends SparkFunSuite with MLlibTestSparkContext
  with DefaultReadWriteTest {

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

  test("Test Chi-Square selector KBest") {
    import testImplicits._
    val data = Seq(
      LabeledPoint(0.0, Vectors.sparse(6, Array((0, 6.0), (1, 7.0), (3, 7.0), (4, 6.0)))),
      LabeledPoint(1.0, Vectors.sparse(6, Array((1, 9.0), (2, 6.0), (4, 5.0), (5, 9.0)))),
      LabeledPoint(1.0, Vectors.sparse(6, Array((1, 9.0), (2, 3.0), (4, 5.0), (5, 5.0)))),
      LabeledPoint(1.0, Vectors.dense(Array(0.0, 9.0, 8.0, 5.0, 6.0, 4.0))),
      LabeledPoint(2.0, Vectors.dense(Array(8.0, 9.0, 6.0, 5.0, 4.0, 4.0))),
      LabeledPoint(2.0, Vectors.dense(Array(8.0, 9.0, 6.0, 4.0, 0.0, 0.0)))
    )

    val preFilteredData = Seq(
      Vectors.dense(6.0),
      Vectors.dense(0.0),
      Vectors.dense(0.0),
      Vectors.dense(0.0),
      Vectors.dense(8.0),
      Vectors.dense(8.0)
    )

    val preFilteredData2 = Seq(
      Vectors.dense(6.0, 7.0, 7.0),
      Vectors.dense(0.0, 9.0, 0.0),
      Vectors.dense(0.0, 9.0, 0.0),
      Vectors.dense(0.0, 9.0, 5.0),
      Vectors.dense(8.0, 9.0, 5.0),
      Vectors.dense(8.0, 9.0, 4.0)
    )

    val df = sc.parallelize(data.zip(preFilteredData))
      .map(x => (x._1.label, x._1.features, x._2))
      .toDF("label", "data", "preFilteredData")

    val selector = new ChiSqSelector()
      .setSelectorType("kbest")
      .setNumTopFeatures(1)
      .setFeaturesCol("data")
      .setLabelCol("label")
      .setOutputCol("filtered")

    selector.fit(df).transform(df).select("filtered", "preFilteredData").collect().foreach {
      case Row(vec1: Vector, vec2: Vector) =>
        assert(vec1 ~== vec2 absTol 1e-1)
    }

    val df2 = sc.parallelize(data.zip(preFilteredData2))
      .map(x => (x._1.label, x._1.features, x._2))
      .toDF("label", "data", "preFilteredData")

    selector.setNumTopFeatures(3).fit(df2).transform(df2).select("filtered", "preFilteredData")
      .collect().foreach {
      case Row(vec1: Vector, vec2: Vector) =>
        assert(vec1 ~== vec2 absTol 1e-1)
    }
  }

  test("Test Chi-Square selector Percentile") {
    import testImplicits._
    val data = Seq(
      LabeledPoint(0.0, Vectors.sparse(6, Array((0, 6.0), (1, 7.0), (3, 7.0), (4, 6.0)))),
      LabeledPoint(1.0, Vectors.sparse(6, Array((1, 9.0), (2, 6.0), (4, 5.0), (5, 9.0)))),
      LabeledPoint(1.0, Vectors.sparse(6, Array((1, 9.0), (2, 3.0), (4, 5.0), (5, 5.0)))),
      LabeledPoint(1.0, Vectors.dense(Array(0.0, 9.0, 8.0, 5.0, 6.0, 4.0))),
      LabeledPoint(2.0, Vectors.dense(Array(8.0, 9.0, 6.0, 5.0, 4.0, 4.0))),
      LabeledPoint(2.0, Vectors.dense(Array(8.0, 9.0, 6.0, 4.0, 0.0, 0.0)))
    )

    val preFilteredData = Seq(
      Vectors.dense(6.0),
      Vectors.dense(0.0),
      Vectors.dense(0.0),
      Vectors.dense(0.0),
      Vectors.dense(8.0),
      Vectors.dense(8.0)
    )

    val preFilteredData2 = Seq(
      Vectors.dense(6.0, 7.0, 7.0),
      Vectors.dense(0.0, 9.0, 0.0),
      Vectors.dense(0.0, 9.0, 0.0),
      Vectors.dense(0.0, 9.0, 5.0),
      Vectors.dense(8.0, 9.0, 5.0),
      Vectors.dense(8.0, 9.0, 4.0)
    )

    val df = sc.parallelize(data.zip(preFilteredData))
      .map(x => (x._1.label, x._1.features, x._2))
      .toDF("label", "data", "preFilteredData")

    val selector = new ChiSqSelector()
      .setSelectorType("percentile")
      .setPercentile(0.2)
      .setFeaturesCol("data")
      .setLabelCol("label")
      .setOutputCol("filtered")

    selector.fit(df).transform(df).select("filtered", "preFilteredData").collect().foreach {
      case Row(vec1: Vector, vec2: Vector) =>
        assert(vec1 ~== vec2 absTol 1e-1)
    }

    val df2 = sc.parallelize(data.zip(preFilteredData2))
      .map(x => (x._1.label, x._1.features, x._2))
      .toDF("label", "data", "preFilteredData")

    selector.setPercentile(0.5).fit(df2).transform(df2).select("filtered", "preFilteredData")
      .collect().foreach {
      case Row(vec1: Vector, vec2: Vector) =>
        assert(vec1 ~== vec2 absTol 1e-1)
    }
  }

  test("Test Chi-Square selector FPR") {
    import testImplicits._
    val data = Seq(
      LabeledPoint(0.0, Vectors.sparse(6, Array((0, 6.0), (1, 7.0), (3, 7.0), (4, 6.0)))),
      LabeledPoint(1.0, Vectors.sparse(6, Array((1, 9.0), (2, 6.0), (4, 5.0), (5, 9.0)))),
      LabeledPoint(1.0, Vectors.sparse(6, Array((1, 9.0), (2, 3.0), (4, 5.0), (5, 5.0)))),
      LabeledPoint(1.0, Vectors.dense(Array(0.0, 9.0, 8.0, 5.0, 6.0, 4.0))),
      LabeledPoint(2.0, Vectors.dense(Array(8.0, 9.0, 6.0, 5.0, 4.0, 4.0))),
      LabeledPoint(2.0, Vectors.dense(Array(8.0, 9.0, 6.0, 4.0, 0.0, 0.0)))
    )

    val preFilteredData = Seq(
      Vectors.dense(6.0),
      Vectors.dense(0.0),
      Vectors.dense(0.0),
      Vectors.dense(0.0),
      Vectors.dense(8.0),
      Vectors.dense(8.0)
    )

    val preFilteredData2 = Seq(
      Vectors.dense(6.0, 7.0, 7.0),
      Vectors.dense(0.0, 9.0, 0.0),
      Vectors.dense(0.0, 9.0, 0.0),
      Vectors.dense(0.0, 9.0, 5.0),
      Vectors.dense(8.0, 9.0, 5.0),
      Vectors.dense(8.0, 9.0, 4.0)
    )

    val df = sc.parallelize(data.zip(preFilteredData))
      .map(x => (x._1.label, x._1.features, x._2))
      .toDF("label", "data", "preFilteredData")

    val selector = new ChiSqSelector()
      .setSelectorType("fpr")
      .setAlpha(0.02)
      .setFeaturesCol("data")
      .setLabelCol("label")
      .setOutputCol("filtered")

    selector.fit(df).transform(df).select("filtered", "preFilteredData").collect().foreach {
      case Row(vec1: Vector, vec2: Vector) =>
        assert(vec1 ~== vec2 absTol 1e-1)
    }

    val df2 = sc.parallelize(data.zip(preFilteredData2))
      .map(x => (x._1.label, x._1.features, x._2))
      .toDF("label", "data", "preFilteredData")

    selector.setAlpha(0.15).fit(df2).transform(df2).select("filtered", "preFilteredData")
      .collect().foreach {
      case Row(vec1: Vector, vec2: Vector) =>
        assert(vec1 ~== vec2 absTol 1e-1)
    }
  }

  test("Test Chi-Square selector FDR") {
    import testImplicits._
    val data = Seq(
      LabeledPoint(0.0, Vectors.sparse(6, Array((0, 6.0), (1, 7.0), (3, 7.0), (4, 6.0)))),
      LabeledPoint(1.0, Vectors.sparse(6, Array((1, 9.0), (2, 6.0), (4, 5.0), (5, 9.0)))),
      LabeledPoint(1.0, Vectors.sparse(6, Array((1, 9.0), (2, 3.0), (4, 5.0), (5, 5.0)))),
      LabeledPoint(1.0, Vectors.dense(Array(0.0, 9.0, 8.0, 5.0, 6.0, 4.0))),
      LabeledPoint(2.0, Vectors.dense(Array(8.0, 9.0, 6.0, 5.0, 4.0, 4.0))),
      LabeledPoint(2.0, Vectors.dense(Array(8.0, 9.0, 6.0, 4.0, 0.0, 0.0)))
    )

    val preFilteredData = Seq(
      Vectors.dense(6.0),
      Vectors.dense(0.0),
      Vectors.dense(0.0),
      Vectors.dense(0.0),
      Vectors.dense(8.0),
      Vectors.dense(8.0)
    )

    val preFilteredData2 = Seq(
      Vectors.dense(6.0, 7.0),
      Vectors.dense(0.0, 9.0),
      Vectors.dense(0.0, 9.0),
      Vectors.dense(0.0, 9.0),
      Vectors.dense(8.0, 9.0),
      Vectors.dense(8.0, 9.0)
    )

    val df = sc.parallelize(data.zip(preFilteredData))
      .map(x => (x._1.label, x._1.features, x._2))
      .toDF("label", "data", "preFilteredData")

    val selector = new ChiSqSelector()
      .setSelectorType("fdr")
      .setAlpha(0.12)
      .setFeaturesCol("data")
      .setLabelCol("label")
      .setOutputCol("filtered")

    selector.fit(df).transform(df).select("filtered", "preFilteredData").collect().foreach {
      case Row(vec1: Vector, vec2: Vector) =>
        assert(vec1 ~== vec2 absTol 1e-1)
    }

    val df2 = sc.parallelize(data.zip(preFilteredData2))
      .map(x => (x._1.label, x._1.features, x._2))
      .toDF("label", "data", "preFilteredData")

    selector.setAlpha(0.15).fit(df2).transform(df2).select("filtered", "preFilteredData")
      .collect().foreach {
      case Row(vec1: Vector, vec2: Vector) =>
        assert(vec1 ~== vec2 absTol 1e-1)
    }
  }

  test("Test Chi-Square selector FWE") {
    import testImplicits._
    val data = Seq(
      LabeledPoint(0.0, Vectors.sparse(6, Array((0, 6.0), (1, 7.0), (3, 7.0), (4, 6.0)))),
      LabeledPoint(1.0, Vectors.sparse(6, Array((1, 9.0), (2, 6.0), (4, 5.0), (5, 9.0)))),
      LabeledPoint(1.0, Vectors.sparse(6, Array((1, 9.0), (2, 3.0), (4, 5.0), (5, 5.0)))),
      LabeledPoint(1.0, Vectors.dense(Array(0.0, 9.0, 8.0, 5.0, 6.0, 4.0))),
      LabeledPoint(2.0, Vectors.dense(Array(8.0, 9.0, 6.0, 5.0, 4.0, 4.0))),
      LabeledPoint(2.0, Vectors.dense(Array(8.0, 9.0, 6.0, 4.0, 0.0, 0.0)))
    )

    val preFilteredData = Seq(
      Vectors.dense(6.0),
      Vectors.dense(0.0),
      Vectors.dense(0.0),
      Vectors.dense(0.0),
      Vectors.dense(8.0),
      Vectors.dense(8.0)
    )

    val preFilteredData2 = Seq(
      Vectors.dense(6.0, 7.0),
      Vectors.dense(0.0, 9.0),
      Vectors.dense(0.0, 9.0),
      Vectors.dense(0.0, 9.0),
      Vectors.dense(8.0, 9.0),
      Vectors.dense(8.0, 9.0)
    )

    val df = sc.parallelize(data.zip(preFilteredData))
      .map(x => (x._1.label, x._1.features, x._2))
      .toDF("label", "data", "preFilteredData")

    val selector = new ChiSqSelector()
      .setSelectorType("fwe")
      .setAlpha(0.12)
      .setFeaturesCol("data")
      .setLabelCol("label")
      .setOutputCol("filtered")

    selector.fit(df).transform(df).select("filtered", "preFilteredData").collect().foreach {
      case Row(vec1: Vector, vec2: Vector) =>
        assert(vec1 ~== vec2 absTol 1e-1)
    }

    val df2 = sc.parallelize(data.zip(preFilteredData2))
      .map(x => (x._1.label, x._1.features, x._2))
      .toDF("label", "data", "preFilteredData")

    selector.setAlpha(0.3).fit(df2).transform(df2).select("filtered", "preFilteredData")
      .collect().foreach {
      case Row(vec1: Vector, vec2: Vector) =>
        assert(vec1 ~== vec2 absTol 1e-1)
    }
  }

  test("ChiSqSelector read/write") {
    val t = new ChiSqSelector()
      .setFeaturesCol("myFeaturesCol")
      .setLabelCol("myLabelCol")
      .setOutputCol("myOutputCol")
      .setNumTopFeatures(2)
    testDefaultReadWrite(t)
  }

  test("ChiSqSelectorModel read/write") {
    val oldModel = new feature.ChiSqSelectorModel(Array(1, 3))
    val instance = new ChiSqSelectorModel("myChiSqSelectorModel", oldModel)
    val newInstance = testDefaultReadWrite(instance)
    assert(newInstance.selectedFeatures === instance.selectedFeatures)
  }

  test("should support all NumericType labels and not support other types") {
    val css = new ChiSqSelector()
    MLTestingUtils.checkNumericTypes[ChiSqSelectorModel, ChiSqSelector](
      css, spark) { (expected, actual) =>
        assert(expected.selectedFeatures === actual.selectedFeatures)
      }
  }
}
