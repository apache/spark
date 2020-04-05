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

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.util.MLlibTestSparkContext

class ANOVATestSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import testImplicits._

  test("test DataFrame of labeled points") {
  // scalastyle:off
  /*
  Use the following sklearn data in this test

  >>> from sklearn.feature_selection import f_classif
  >>> import numpy as np
  >>> np.random.seed(888)
  >>> X = np.random.rand(20, 6)
  >>> X
  array([[0.85956061, 0.1645695 , 0.48347596, 0.92102727, 0.42855644,
          0.05746009],
         [0.92500743, 0.65760154, 0.13295284, 0.53344893, 0.8994776 ,
          0.24836496],
         [0.03017182, 0.07244715, 0.87416449, 0.55843035, 0.91604736,
          0.63346045],
         [0.28325261, 0.36536881, 0.09223386, 0.37251258, 0.34742278,
          0.70517077],
         [0.64850904, 0.04090877, 0.21173176, 0.00148992, 0.13897166,
          0.21182539],
         [0.02609493, 0.44608735, 0.23910531, 0.95449222, 0.90763182,
          0.8624905 ],
         [0.09158744, 0.97745235, 0.41150139, 0.45830467, 0.52590925,
          0.29441554],
         [0.97211594, 0.1814442 , 0.30340642, 0.17445413, 0.52756958,
          0.02069296],
         [0.06354593, 0.63527231, 0.49620335, 0.0141264 , 0.62722219,
          0.63497507],
         [0.10814149, 0.8296426 , 0.51775217, 0.57068344, 0.54633305,
          0.12714921],
         [0.72731796, 0.94010124, 0.45007811, 0.87650674, 0.53735565,
          0.49568415],
         [0.41827208, 0.85100628, 0.38685271, 0.60689503, 0.21784097,
          0.91294433],
         [0.65843656, 0.5880859 , 0.18862706, 0.856398  , 0.18029327,
          0.94851926],
         [0.3841634 , 0.25138793, 0.96746644, 0.77048045, 0.44685196,
          0.19813854],
         [0.65982267, 0.23024125, 0.13598434, 0.60144265, 0.57848927,
          0.85623564],
         [0.35764189, 0.47623815, 0.5459232 , 0.79508298, 0.14462443,
          0.01802919],
         [0.38532153, 0.90614554, 0.86629571, 0.13988735, 0.32062385,
          0.00179492],
         [0.2142368 , 0.28306022, 0.59481646, 0.42567028, 0.52207663,
          0.78082401],
         [0.20788283, 0.76861782, 0.59595468, 0.62103642, 0.17781246,
          0.77655345],
         [0.1751708 , 0.4547537 , 0.46187865, 0.79781199, 0.05104487,
          0.42406092]])
  >>> y = np.array([3, 2, 1, 5, 4, 4, 5, 4, 2, 1, 1, 2, 3, 4, 5, 1, 5, 3, 1, 1])
  >>> y
  array([3, 2, 1, 5, 4, 4, 5, 4, 2, 1, 1, 2, 3, 4, 5, 1, 5, 3, 1, 1])
  >>> f_classif(X, y)
  (array([0.64110932, 1.98689258, 0.55499714, 1.40340562, 0.30881722,
         0.3848595 ]), array([0.64137831, 0.14830724, 0.69858474, 0.28038169, 0.86759161,
         0.81608606]))
  */
  // scalastyle:on

  val data = Seq(
    LabeledPoint(3, Vectors.dense(0.85956061, 0.1645695, 0.48347596, 0.92102727, 0.42855644,
      0.05746009)),
    LabeledPoint(2, Vectors.dense(0.92500743, 0.65760154, 0.13295284, 0.53344893, 0.8994776,
      0.24836496)),
    LabeledPoint(1, Vectors.dense(0.03017182, 0.07244715, 0.87416449, 0.55843035, 0.91604736,
      0.63346045)),
    LabeledPoint(5, Vectors.dense(0.28325261, 0.36536881, 0.09223386, 0.37251258, 0.34742278,
      0.70517077)),
    LabeledPoint(4, Vectors.dense(0.64850904, 0.04090877, 0.21173176, 0.00148992, 0.13897166,
      0.21182539)),
    LabeledPoint(4, Vectors.dense(0.02609493, 0.44608735, 0.23910531, 0.95449222, 0.90763182,
      0.8624905)),
    LabeledPoint(5, Vectors.dense(0.09158744, 0.97745235, 0.41150139, 0.45830467, 0.52590925,
      0.29441554)),
    LabeledPoint(4, Vectors.dense(0.97211594, 0.1814442, 0.30340642, 0.17445413, 0.52756958,
      0.02069296)),
    LabeledPoint(2, Vectors.dense(0.06354593, 0.63527231, 0.49620335, 0.0141264, 0.62722219,
      0.63497507)),
    LabeledPoint(1, Vectors.dense(0.10814149, 0.8296426, 0.51775217, 0.57068344, 0.54633305,
      0.12714921)),
    LabeledPoint(1, Vectors.dense(0.72731796, 0.94010124, 0.45007811, 0.87650674, 0.53735565,
      0.49568415)),
    LabeledPoint(2, Vectors.dense(0.41827208, 0.85100628, 0.38685271, 0.60689503, 0.21784097,
      0.91294433)),
    LabeledPoint(3, Vectors.dense(0.65843656, 0.5880859, 0.18862706, 0.856398, 0.18029327,
      0.94851926)),
    LabeledPoint(4, Vectors.dense(0.3841634, 0.25138793, 0.96746644, 0.77048045, 0.44685196,
      0.19813854)),
    LabeledPoint(5, Vectors.dense(0.65982267, 0.23024125, 0.13598434, 0.60144265, 0.57848927,
      0.85623564)),
    LabeledPoint(1, Vectors.dense(0.35764189, 0.47623815, 0.5459232, 0.79508298, 0.14462443,
      0.01802919)),
    LabeledPoint(5, Vectors.dense(0.38532153, 0.90614554, 0.86629571, 0.13988735, 0.32062385,
      0.00179492)),
    LabeledPoint(3, Vectors.dense(0.2142368, 0.28306022, 0.59481646, 0.42567028, 0.52207663,
      0.78082401)),
    LabeledPoint(1, Vectors.dense(0.20788283, 0.76861782, 0.59595468, 0.62103642, 0.17781246,
      0.77655345)),
    LabeledPoint(1, Vectors.dense(0.1751708, 0.4547537, 0.46187865, 0.79781199, 0.05104487,
      0.42406092)))

    for (numParts <- List(2, 4, 6, 8)) {
      val df = spark.createDataFrame(sc.parallelize(data, numParts))
      val anovaResult = ANOVATest.test(df, "features", "label")
      val (pValues: Vector, fValues: Vector) =
        anovaResult.select("pValues", "fValues")
          .as[(Vector, Vector)].head()
      assert(pValues ~== Vectors.dense(0.64137831, 0.14830724, 0.69858474, 0.28038169, 0.86759161,
        0.81608606) relTol 1e-6)
      assert(fValues ~== Vectors.dense(0.64110932, 1.98689258, 0.55499714, 1.40340562, 0.30881722,
        0.3848595) relTol 1e-6)
    }
  }

  test("test DataFrame with sparse vector") {
    val data = Seq(
      (3, Vectors.dense(Array(6.0, 7.0, 0.0, 7.0, 6.0, 0.0, 0.0))),
      (1, Vectors.dense(Array(0.0, 9.0, 6.0, 0.0, 5.0, 9.0, 0.0))),
      (3, Vectors.dense(Array(0.0, 9.0, 3.0, 0.0, 5.0, 5.0, 0.0))),
      (2, Vectors.dense(Array(0.0, 9.0, 8.0, 5.0, 6.0, 4.0, 0.0))),
      (2, Vectors.dense(Array(8.0, 9.0, 6.0, 5.0, 4.0, 4.0, 0.0))),
      (3, Vectors.dense(Array(8.0, 9.0, 6.0, 4.0, 0.0, 0.0, 0.0))))

    val df1 = spark.createDataFrame(data.map(t => (t._1, t._2.toDense)))
      .toDF("label", "features")
    val df2 = spark.createDataFrame(data.map(t => (t._1, t._2.toSparse)))
      .toDF("label", "features")
    val df3 = spark.createDataFrame(data.map(t => (t._1, t._2.compressed)))
      .toDF("label", "features")

    Seq(df1, df2, df3).foreach { df =>
      val anovaResult = ANOVATest.test(df, "features", "label")
      val (pValues: Vector, fValues: Vector) =
        anovaResult.select("pValues", "fValues")
          .as[(Vector, Vector)].head()
      assert(pValues ~== Vectors.dense(0.71554175, 0.71554175, 0.34278574, 0.45824059, 0.84633632,
        0.15673368, Double.NaN) relTol 1e-6)
      assert(fValues ~== Vectors.dense(0.375, 0.375, 1.5625, 1.02364865, 0.17647059,
        3.66, Double.NaN) relTol 1e-6)
    }
  }
}
