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

class FValueTestSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import testImplicits._

  test("test DataFrame of labeled points") {
  // scalastyle:off
  /*
  Use the following sklearn data in this test

  >>> from sklearn.feature_selection import f_regression
  >>> import numpy as np
  >>> X = np.random.rand(20, 6)
  >>> w = np.array([0.3, 0.4, 0.5, 0, 0, 0])
  >>> y = X @ w
  >>> X
  array([[1.67318514e-01, 1.78398028e-01, 4.36846538e-01, 5.24003164e-01,
          1.80915415e-01, 1.98030859e-01],
         [3.71836586e-01, 6.13453963e-01, 7.15269190e-01, 9.33623792e-03,
          5.36095674e-01, 2.74223333e-01],
         [3.68988949e-01, 5.34104018e-01, 5.24858744e-01, 6.86815853e-01,
          3.26534757e-01, 6.92699400e-01],
         [4.87748505e-02, 3.07080315e-01, 7.82955385e-01, 6.90167375e-01,
          6.44077919e-01, 4.23739024e-01],
         [6.50153455e-01, 8.32746110e-01, 6.88029140e-03, 1.27859556e-01,
          6.80223767e-01, 6.25825675e-01],
         [9.47343271e-01, 2.13193978e-01, 3.71342472e-01, 8.21291956e-01,
          4.38195693e-01, 5.76569439e-01],
         [9.96499254e-01, 8.45833297e-01, 6.56086922e-02, 5.90029174e-01,
          1.68954572e-01, 7.19792823e-02],
         [1.85926914e-01, 9.60329804e-01, 3.13487406e-01, 9.59549928e-01,
          6.89093311e-01, 6.94999427e-01],
         [9.40164576e-01, 2.69042714e-02, 5.39491321e-01, 5.74068666e-01,
          1.10935343e-01, 2.17519760e-01],
         [2.97951848e-02, 1.06592106e-01, 5.74931856e-01, 8.80801522e-01,
          8.60445070e-01, 9.22757966e-01],
         [9.80970473e-01, 3.05909353e-01, 4.96401766e-01, 2.44342697e-01,
          6.90559227e-01, 5.64858704e-01],
         [1.55939260e-01, 2.18626853e-01, 5.01834270e-01, 1.86694987e-01,
          9.15411148e-01, 6.40527848e-01],
         [3.16107608e-01, 9.25906358e-01, 5.47327167e-01, 4.83712979e-01,
          8.42305220e-01, 7.58488462e-01],
         [4.14393503e-01, 1.30817883e-01, 5.62034942e-01, 1.05150633e-01,
          5.35632795e-01, 9.47594074e-04],
         [5.26233981e-01, 7.63781419e-02, 3.19188240e-01, 5.16528633e-02,
          5.28416724e-01, 6.47050470e-03],
         [2.73404764e-01, 7.17070744e-01, 3.12889595e-01, 8.39271965e-01,
          9.67650889e-01, 8.50098873e-01],
         [4.63289495e-01, 3.57055416e-02, 5.43528596e-01, 4.44840919e-01,
          9.36845855e-02, 7.81595037e-01],
         [3.21784993e-01, 3.15622454e-01, 7.58870408e-01, 5.18198558e-01,
          2.28151905e-01, 4.42460325e-01],
         [3.72428352e-01, 1.44447969e-01, 8.40274188e-01, 5.86308041e-01,
          6.09893953e-01, 3.97006473e-01],
         [3.12776786e-01, 9.33630195e-01, 2.29328749e-01, 4.32807208e-01,
          1.51703470e-02, 1.51589320e-01]])
  >>> y
  array([0.33997803, 0.71456716, 0.58676766, 0.52894227, 0.53158463,
         0.55515181, 0.67008744, 0.5966537 , 0.56255674, 0.33904133,
         0.66485577, 0.38514965, 0.73885841, 0.45766267, 0.34801557,
         0.52529452, 0.42503336, 0.60221968, 0.58964479, 0.58194949])
  >>> f_regression(X, y)
  (array([2.76445780e+00, 1.05267800e+01, 4.43399092e-02, 2.04580501e-02,
         3.13208557e-02, 1.35248025e-03]), array([0.11369388, 0.0044996 , 0.83558782, 0.88785417, 0.86150261,
         0.97106833]))
  */
  // scalastyle:on

    val data = Seq(
      LabeledPoint(0.33997803, Vectors.dense(1.67318514e-01, 1.78398028e-01, 4.36846538e-01,
        5.24003164e-01, 1.80915415e-01, 1.98030859e-01)),
      LabeledPoint(0.71456716, Vectors.dense(3.71836586e-01, 6.13453963e-01, 7.15269190e-01,
        9.33623792e-03, 5.36095674e-01, 2.74223333e-01)),
      LabeledPoint(0.58676766, Vectors.dense(3.68988949e-01, 5.34104018e-01, 5.24858744e-01,
        6.86815853e-01, 3.26534757e-01, 6.92699400e-01)),
      LabeledPoint(0.52894227, Vectors.dense(4.87748505e-02, 3.07080315e-01, 7.82955385e-01,
        6.90167375e-01, 6.44077919e-01, 4.23739024e-01)),
      LabeledPoint(0.53158463, Vectors.dense(6.50153455e-01, 8.32746110e-01, 6.88029140e-03,
        1.27859556e-01, 6.80223767e-01, 6.25825675e-01)),
      LabeledPoint(0.55515181, Vectors.dense(9.47343271e-01, 2.13193978e-01, 3.71342472e-01,
        8.21291956e-01, 4.38195693e-01, 5.76569439e-01)),
      LabeledPoint(0.67008744, Vectors.dense(9.96499254e-01, 8.45833297e-01, 6.56086922e-02,
        5.90029174e-01, 1.68954572e-01, 7.19792823e-02)),
      LabeledPoint(0.5966537, Vectors.dense(1.85926914e-01, 9.60329804e-01, 3.13487406e-01,
        9.59549928e-01, 6.89093311e-01, 6.94999427e-01)),
      LabeledPoint(0.56255674, Vectors.dense(9.40164576e-01, 2.69042714e-02, 5.39491321e-01,
        5.74068666e-01, 1.10935343e-01, 2.17519760e-01)),
      LabeledPoint(0.33904133, Vectors.dense(2.97951848e-02, 1.06592106e-01, 5.74931856e-01,
        8.80801522e-01, 8.60445070e-01, 9.22757966e-01)),
      LabeledPoint(0.66485577, Vectors.dense(9.80970473e-01, 3.05909353e-01, 4.96401766e-01,
        2.44342697e-01, 6.90559227e-01, 5.64858704e-01)),
      LabeledPoint(0.38514965, Vectors.dense(1.55939260e-01, 2.18626853e-01, 5.01834270e-01,
        1.86694987e-01, 9.15411148e-01, 6.40527848e-01)),
      LabeledPoint(0.73885841, Vectors.dense(3.16107608e-01, 9.25906358e-01, 5.47327167e-01,
        4.83712979e-01, 8.42305220e-01, 7.58488462e-01)),
      LabeledPoint(0.45766267, Vectors.dense(4.14393503e-01, 1.30817883e-01, 5.62034942e-01,
        1.05150633e-01, 5.35632795e-01, 9.47594074e-04)),
      LabeledPoint(0.34801557, Vectors.dense(5.26233981e-01, 7.63781419e-02, 3.19188240e-01,
        5.16528633e-02, 5.28416724e-01, 6.47050470e-03)),
      LabeledPoint(0.52529452, Vectors.dense(2.73404764e-01, 7.17070744e-01, 3.12889595e-01,
        8.39271965e-01, 9.67650889e-01, 8.50098873e-01)),
      LabeledPoint(0.42503336, Vectors.dense(4.63289495e-01, 3.57055416e-02, 5.43528596e-01,
        4.44840919e-01, 9.36845855e-02, 7.81595037e-01)),
      LabeledPoint(0.60221968, Vectors.dense(3.21784993e-01, 3.15622454e-01, 7.58870408e-01,
        5.18198558e-01, 2.28151905e-01, 4.42460325e-01)),
      LabeledPoint(0.58964479, Vectors.dense(3.72428352e-01, 1.44447969e-01, 8.40274188e-01,
        5.86308041e-01, 6.09893953e-01, 3.97006473e-01)),
      LabeledPoint(0.58194949, Vectors.dense(3.12776786e-01, 9.33630195e-01, 2.29328749e-01,
        4.32807208e-01, 1.51703470e-02, 1.51589320e-01)))

    for (numParts <- List(2, 4, 6, 8)) {
      val df = spark.createDataFrame(sc.parallelize(data, numParts))
      val fRegression = FValueTest.test(df, "features", "label")
      val (pValues: Vector, fValues: Vector) =
        fRegression.select("pValues", "fValues")
          .as[(Vector, Vector)].head()
      assert(pValues ~== Vectors.dense(0.11369388, 0.0044996, 0.83558782, 0.88785417, 0.86150261,
        0.97106833) relTol 1e-6)
      assert(fValues ~== Vectors.dense(2.76445780e+00, 1.05267800e+01, 4.43399092e-02,
        2.04580501e-02, 3.13208557e-02, 1.35248025e-03) relTol 1e-6)
    }
  }

  test("test DataFrame with sparse vector") {
    val df = spark.createDataFrame(Seq(
      (4.6, Vectors.sparse(6, Array((0, 6.0), (1, 7.0), (3, 7.0), (4, 6.0)))),
      (6.6, Vectors.sparse(6, Array((1, 9.0), (2, 6.0), (4, 5.0), (5, 9.0)))),
      (5.1, Vectors.sparse(6, Array((1, 9.0), (2, 3.0), (4, 5.0), (5, 5.0)))),
      (7.6, Vectors.dense(Array(0.0, 9.0, 8.0, 5.0, 6.0, 4.0))),
      (9.0, Vectors.dense(Array(8.0, 9.0, 6.0, 5.0, 4.0, 4.0))),
      (9.0, Vectors.dense(Array(8.0, 9.0, 6.0, 4.0, 0.0, 0.0)))
    )).toDF("label", "features")

    val fRegression = FValueTest.test(df, "features", "label")
    val (pValues: Vector, fValues: Vector) =
      fRegression.select("pValues", "fValues")
        .as[(Vector, Vector)].head()
    assert(pValues ~== Vectors.dense(0.35236913, 0.19167161, 0.06506426, 0.75183662, 0.16111045,
      0.89090362) relTol 1e-6)
    assert(fValues ~== Vectors.dense(1.10558422, 2.46254817, 6.37164347, 0.1147488, 2.94816821,
      0.02134755) relTol 1e-6)
  }
}
