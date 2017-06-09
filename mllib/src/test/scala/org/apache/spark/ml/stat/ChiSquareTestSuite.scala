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

import java.util.Random

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.stat.test.ChiSqTest
import org.apache.spark.mllib.util.MLlibTestSparkContext

class ChiSquareTestSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import testImplicits._

  test("test DataFrame of labeled points") {
    // labels: 1.0 (2 / 6), 0.0 (4 / 6)
    // feature1: 0.5 (1 / 6), 1.5 (2 / 6), 3.5 (3 / 6)
    // feature2: 10.0 (1 / 6), 20.0 (1 / 6), 30.0 (2 / 6), 40.0 (2 / 6)
    val data = Seq(
      LabeledPoint(0.0, Vectors.dense(0.5, 10.0)),
      LabeledPoint(0.0, Vectors.dense(1.5, 20.0)),
      LabeledPoint(1.0, Vectors.dense(1.5, 30.0)),
      LabeledPoint(0.0, Vectors.dense(3.5, 30.0)),
      LabeledPoint(0.0, Vectors.dense(3.5, 40.0)),
      LabeledPoint(1.0, Vectors.dense(3.5, 40.0)))
    for (numParts <- List(2, 4, 6, 8)) {
      val df = spark.createDataFrame(sc.parallelize(data, numParts))
      val chi = ChiSquareTest.test(df, "features", "label")
      val (pValues: Vector, degreesOfFreedom: Array[Int], statistics: Vector) =
        chi.select("pValues", "degreesOfFreedom", "statistics")
          .as[(Vector, Array[Int], Vector)].head()
      assert(pValues ~== Vectors.dense(0.6873, 0.6823) relTol 1e-4)
      assert(degreesOfFreedom === Array(2, 3))
      assert(statistics ~== Vectors.dense(0.75, 1.5) relTol 1e-4)
    }
  }

  test("large number of features (SPARK-3087)") {
    // Test that the right number of results is returned
    val numCols = 1001
    val sparseData = Array(
      LabeledPoint(0.0, Vectors.sparse(numCols, Seq((100, 2.0)))),
      LabeledPoint(0.1, Vectors.sparse(numCols, Seq((200, 1.0)))))
    val df = spark.createDataFrame(sparseData)
    val chi = ChiSquareTest.test(df, "features", "label")
    val (pValues: Vector, degreesOfFreedom: Array[Int], statistics: Vector) =
      chi.select("pValues", "degreesOfFreedom", "statistics")
        .as[(Vector, Array[Int], Vector)].head()
    assert(pValues.size === numCols)
    assert(degreesOfFreedom.length === numCols)
    assert(statistics.size === numCols)
    assert(pValues(1000) !== null)  // SPARK-3087
  }

  test("fail on continuous features or labels") {
    val tooManyCategories: Int = 100000
    assert(tooManyCategories > ChiSqTest.maxCategories, "This unit test requires that " +
      "tooManyCategories be large enough to cause ChiSqTest to throw an exception.")

    val random = new Random(11L)
    val continuousLabel = Seq.fill(tooManyCategories)(
      LabeledPoint(random.nextDouble(), Vectors.dense(random.nextInt(2))))
    withClue("ChiSquare should throw an exception when given a continuous-valued label") {
      intercept[SparkException] {
        val df = spark.createDataFrame(continuousLabel)
        ChiSquareTest.test(df, "features", "label")
      }
    }
    val continuousFeature = Seq.fill(tooManyCategories)(
      LabeledPoint(random.nextInt(2), Vectors.dense(random.nextDouble())))
    withClue("ChiSquare should throw an exception when given continuous-valued features") {
      intercept[SparkException] {
        val df = spark.createDataFrame(continuousFeature)
        ChiSquareTest.test(df, "features", "label")
      }
    }
  }
}
