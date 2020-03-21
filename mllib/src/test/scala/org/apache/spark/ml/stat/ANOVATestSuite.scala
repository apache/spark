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
  >>> X = np.random.rand(20, 6)
  >>> X
  array([[1.77744923e-01, 4.46602900e-01, 7.62487415e-01, 5.81095198e-01,
          9.64228053e-01, 2.31090547e-01],
         [8.87725538e-01, 7.39137344e-01, 5.76073881e-01, 7.38266678e-01,
          2.28188254e-01, 4.10480664e-01],
         [1.26764947e-01, 9.53144673e-01, 2.50788458e-01, 3.17260250e-01,
          8.76189581e-01, 2.56902552e-01],
         [3.74475030e-01, 9.23768847e-02, 6.19433427e-01, 4.10280293e-02,
          7.59799273e-04, 3.87053403e-01],
         [8.91109208e-01, 5.21208840e-01, 7.83435405e-02, 8.37642752e-01,
          5.26969965e-02, 3.07387671e-02],
         [7.94006669e-01, 8.55618651e-02, 9.25974068e-01, 4.07848340e-01,
          9.47526767e-01, 2.10230156e-01],
         [6.38404274e-01, 1.71793581e-01, 9.82515893e-01, 3.34181329e-01,
          9.96800651e-02, 4.48531617e-01],
         [1.42029531e-01, 2.53590255e-02, 4.00032820e-01, 3.75843553e-01,
          9.73971121e-01, 3.32346317e-01],
         [9.94329513e-01, 5.61156964e-01, 5.96579626e-01, 1.92286208e-01,
          9.71888097e-01, 2.48574337e-01],
         [9.71838692e-01, 8.50908993e-02, 6.15917459e-01, 1.61320964e-01,
          2.43025079e-01, 7.78200314e-01],
         [7.76735907e-01, 6.10769335e-01, 1.58097504e-01, 2.95018676e-01,
          3.94466695e-01, 7.71700212e-01],
         [8.44787012e-01, 4.76682368e-01, 9.43624130e-01, 2.20926735e-01,
          8.43054317e-02, 8.51276967e-01],
         [3.38797773e-01, 6.78991156e-01, 7.90036698e-01, 4.40145825e-01,
          8.33432294e-01, 9.91810731e-01],
         [1.73295200e-01, 6.83267374e-01, 8.88625086e-01, 6.89072609e-01,
          8.35407299e-01, 9.70359856e-01],
         [7.74552650e-01, 5.70846800e-01, 5.39894150e-01, 5.92696042e-01,
          5.72618852e-01, 7.00850299e-01],
         [7.84482121e-01, 7.80094912e-01, 4.03710589e-02, 4.97916309e-01,
          2.55871739e-01, 5.27961039e-01],
         [1.45590738e-01, 2.43124833e-01, 1.69582546e-01, 6.16891208e-01,
          2.96795519e-01, 9.19985890e-01],
         [6.89274903e-01, 7.13295249e-01, 1.65640967e-01, 5.74821962e-01,
          9.11149662e-01, 1.09691820e-01],
         [3.33334361e-01, 3.40817958e-01, 6.73779642e-01, 6.01719487e-01,
          3.97932741e-01, 6.39527734e-01],
         [2.33981601e-01, 1.41349421e-01, 8.13246213e-01, 9.09664223e-01,
          2.36111304e-01, 9.00214578e-01]])
  >>> y = np.array([3, 2, 1, 5, 4, 4, 5, 4, 2, 1, 1, 2, 3, 4, 5, 1, 5, 3, 1, 1])
  >>> y
  array([3, 2, 1, 5, 4, 4, 5, 4, 2, 1, 1, 2, 3, 4, 5, 1, 5, 3, 1, 1])
  >>> f_classif(X, y)
  (array([1.22822174, 1.10202387, 0.3884059 , 0.41225633, 2.43153435,
         0.50553798]), array([0.34047275, 0.39149917, 0.81363895, 0.79712747, 0.09303083,
         0.73241872]))
  */
  // scalastyle:on

  val data = Seq(
    LabeledPoint(3, Vectors.dense(1.77744923e-01, 4.46602900e-01, 7.62487415e-01, 5.81095198e-01,
      9.64228053e-01, 2.31090547e-01)),
    LabeledPoint(2, Vectors.dense(8.87725538e-01, 7.39137344e-01, 5.76073881e-01, 7.38266678e-01,
      2.28188254e-01, 4.10480664e-01)),
    LabeledPoint(1, Vectors.dense(1.26764947e-01, 9.53144673e-01, 2.50788458e-01, 3.17260250e-01,
      8.76189581e-01, 2.56902552e-01)),
    LabeledPoint(5, Vectors.dense(3.74475030e-01, 9.23768847e-02, 6.19433427e-01, 4.10280293e-02,
      7.59799273e-04, 3.87053403e-01)),
    LabeledPoint(4, Vectors.dense(8.91109208e-01, 5.21208840e-01, 7.83435405e-02, 8.37642752e-01,
      5.26969965e-02, 3.07387671e-02)),
    LabeledPoint(4, Vectors.dense(7.94006669e-01, 8.55618651e-02, 9.25974068e-01, 4.07848340e-01,
      9.47526767e-01, 2.10230156e-01)),
    LabeledPoint(5, Vectors.dense(6.38404274e-01, 1.71793581e-01, 9.82515893e-01, 3.34181329e-01,
      9.96800651e-02, 4.48531617e-01)),
    LabeledPoint(4, Vectors.dense(1.42029531e-01, 2.53590255e-02, 4.00032820e-01, 3.75843553e-01,
      9.73971121e-01, 3.32346317e-01)),
    LabeledPoint(2, Vectors.dense(9.94329513e-01, 5.61156964e-01, 5.96579626e-01, 1.92286208e-01,
      9.71888097e-01, 2.48574337e-01)),
    LabeledPoint(1, Vectors.dense(9.71838692e-01, 8.50908993e-02, 6.15917459e-01, 1.61320964e-01,
      2.43025079e-01, 7.78200314e-01)),
    LabeledPoint(1, Vectors.dense(7.76735907e-01, 6.10769335e-01, 1.58097504e-01, 2.95018676e-01,
      3.94466695e-01, 7.71700212e-01)),
    LabeledPoint(2, Vectors.dense(8.44787012e-01, 4.76682368e-01, 9.43624130e-01, 2.20926735e-01,
      8.43054317e-02, 8.51276967e-01)),
    LabeledPoint(3, Vectors.dense(3.38797773e-01, 6.78991156e-01, 7.90036698e-01, 4.40145825e-01,
      8.33432294e-01, 9.91810731e-01)),
    LabeledPoint(4, Vectors.dense(1.73295200e-01, 6.83267374e-01, 8.88625086e-01, 6.89072609e-01,
      8.35407299e-01, 9.70359856e-01)),
    LabeledPoint(5, Vectors.dense(7.74552650e-01, 5.70846800e-01, 5.39894150e-01, 5.92696042e-01,
      5.72618852e-01, 7.00850299e-01)),
    LabeledPoint(1, Vectors.dense(7.84482121e-01, 7.80094912e-01, 4.03710589e-02, 4.97916309e-01,
      2.55871739e-01, 5.27961039e-01)),
    LabeledPoint(5, Vectors.dense(1.45590738e-01, 2.43124833e-01, 1.69582546e-01, 6.16891208e-01,
      2.96795519e-01, 9.19985890e-01)),
    LabeledPoint(3, Vectors.dense(6.89274903e-01, 7.13295249e-01, 1.65640967e-01, 5.74821962e-01,
      9.11149662e-01, 1.09691820e-01)),
    LabeledPoint(1, Vectors.dense(3.33334361e-01, 3.40817958e-01, 6.73779642e-01, 6.01719487e-01,
      3.97932741e-01, 6.39527734e-01)),
    LabeledPoint(1, Vectors.dense(2.33981601e-01, 1.41349421e-01, 8.13246213e-01, 9.09664223e-01,
      2.36111304e-01, 9.00214578e-01)))

    for (numParts <- List(2, 4, 6, 8)) {
      val df = spark.createDataFrame(sc.parallelize(data, numParts))
      val anovaResult = ANOVATest.test(df, "features", "label")
      val (pValues: Vector, fValues: Vector) =
        anovaResult.select("pValues", "fValues")
          .as[(Vector, Vector)].head()
      assert(pValues ~== Vectors.dense(0.34047275, 0.39149917, 0.81363895, 0.79712747, 0.09303083,
        0.73241872) relTol 1e-6)
      assert(fValues ~== Vectors.dense(1.22822174, 1.10202387, 0.3884059, 0.41225633, 2.43153435,
        0.50553798) relTol 1e-6)
    }
  }

  test("test DataFrame with sparse vector") {
    val df = spark.createDataFrame(Seq(
      (3, Vectors.sparse(6, Array((0, 6.0), (1, 7.0), (3, 7.0), (4, 6.0)))),
      (1, Vectors.sparse(6, Array((1, 9.0), (2, 6.0), (4, 5.0), (5, 9.0)))),
      (3, Vectors.sparse(6, Array((1, 9.0), (2, 3.0), (4, 5.0), (5, 5.0)))),
      (2, Vectors.dense(Array(0.0, 9.0, 8.0, 5.0, 6.0, 4.0))),
      (2, Vectors.dense(Array(8.0, 9.0, 6.0, 5.0, 4.0, 4.0))),
      (3, Vectors.dense(Array(8.0, 9.0, 6.0, 4.0, 0.0, 0.0)))
    )).toDF("label", "features")

    val anovaResult = ANOVATest.test(df, "features", "label")
    val (pValues: Vector, fValues: Vector) =
      anovaResult.select("pValues", "fValues")
        .as[(Vector, Vector)].head()
    assert(pValues ~== Vectors.dense(0.71554175, 0.71554175, 0.34278574, 0.45824059, 0.84633632,
      0.15673368) relTol 1e-6)
    assert(fValues ~== Vectors.dense(0.375, 0.375, 1.5625, 1.02364865, 0.17647059,
      3.66) relTol 1e-6)
  }
}
