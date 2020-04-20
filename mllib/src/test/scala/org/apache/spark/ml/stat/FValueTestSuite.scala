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
  >>> np.random.seed(777)
  >>> X = np.random.rand(20, 6)
  >>> w = np.array([0.3, 0.4, 0.5, 0, 0, 0])
  >>> y = X @ w
  >>> X
  array([[0.15266373, 0.30235661, 0.06203641, 0.45986034, 0.83525338,
          0.92699705],
         [0.72698898, 0.76849622, 0.26920507, 0.64402929, 0.09337326,
          0.07968589],
         [0.58961375, 0.34334054, 0.98887615, 0.62647321, 0.68177928,
          0.55225681],
         [0.26886006, 0.37325939, 0.2229281 , 0.1864426 , 0.39064809,
          0.19316241],
         [0.61091093, 0.88280845, 0.62233882, 0.25311894, 0.17993031,
          0.81640447],
         [0.22537162, 0.51685714, 0.51849582, 0.60037494, 0.53262048,
          0.01331005],
         [0.52409726, 0.89588471, 0.76990129, 0.1228517 , 0.29587269,
          0.61202358],
         [0.72613812, 0.46349747, 0.76911037, 0.19163103, 0.55786672,
          0.55077816],
         [0.47222549, 0.79188496, 0.11524968, 0.6813039 , 0.36233361,
          0.34420889],
         [0.44951875, 0.02694226, 0.41524769, 0.9222317 , 0.09120557,
          0.31512178],
         [0.52802224, 0.32806203, 0.44891554, 0.01633442, 0.0970269 ,
          0.69258857],
         [0.83594341, 0.42432199, 0.8487743 , 0.54679121, 0.35410346,
          0.72724968],
         [0.09385168, 0.8928588 , 0.33625828, 0.89183268, 0.296849  ,
          0.30164829],
         [0.80624061, 0.83760997, 0.63428133, 0.3113273 , 0.02944858,
          0.39977732],
         [0.51817346, 0.00738845, 0.77494778, 0.8544712 , 0.13153282,
          0.28767364],
         [0.32658881, 0.90655956, 0.99955954, 0.77088429, 0.04284752,
          0.96525111],
         [0.97521246, 0.2025168 , 0.67985305, 0.46534506, 0.92001748,
          0.72820735],
         [0.24585653, 0.01953996, 0.70598881, 0.77448287, 0.4729746 ,
          0.80146736],
         [0.17539792, 0.72016934, 0.3678759 , 0.53209295, 0.29719397,
          0.37429151],
         [0.72810013, 0.39850784, 0.1058295 , 0.39858265, 0.52196395,
          0.1060125 ]])
  >>> y
  array([0.19775997, 0.66009772, 0.80865842, 0.34142582, 0.84756607,
         0.53360225, 0.90053371, 0.78779561, 0.51604647, 0.35325637,
         0.51408926, 0.84489897, 0.55342816, 0.89405683, 0.54588131,
         0.96038024, 0.71349698, 0.43456735, 0.52462506, 0.43074793])
  >>> f_regression(X, y)
  (array([ 6.86260598,  7.23175589, 24.11424725,  0.6605354 ,  1.26266286,
          1.82421406]), array([1.73658700e-02, 1.49916659e-02, 1.12697153e-04, 4.26990301e-01,
          2.75911201e-01, 1.93549275e-01]))
  */
  // scalastyle:on

    val data = Seq(
      LabeledPoint(0.19775997, Vectors.dense(0.15266373, 0.30235661, 0.06203641, 0.45986034,
        0.83525338, 0.92699705)),
      LabeledPoint(0.66009772, Vectors.dense(0.72698898, 0.76849622, 0.26920507, 0.64402929,
        0.09337326, 0.07968589)),
      LabeledPoint(0.80865842, Vectors.dense(0.58961375, 0.34334054, 0.98887615, 0.62647321,
        0.68177928, 0.55225681)),
      LabeledPoint(0.34142582, Vectors.dense(0.26886006, 0.37325939, 0.2229281, 0.1864426,
        0.39064809, 0.19316241)),
      LabeledPoint(0.84756607, Vectors.dense(0.61091093, 0.88280845, 0.62233882, 0.25311894,
        0.17993031, 0.81640447)),
      LabeledPoint(0.53360225, Vectors.dense(0.22537162, 0.51685714, 0.51849582, 0.60037494,
        0.53262048, 0.01331005)),
      LabeledPoint(0.90053371, Vectors.dense(0.52409726, 0.89588471, 0.76990129, 0.1228517,
        0.29587269, 0.61202358)),
      LabeledPoint(0.78779561, Vectors.dense(0.72613812, 0.46349747, 0.76911037, 0.19163103,
        0.55786672, 0.55077816)),
      LabeledPoint(0.51604647, Vectors.dense(0.47222549, 0.79188496, 0.11524968, 0.6813039,
        0.36233361, 0.34420889)),
      LabeledPoint(0.35325637, Vectors.dense(0.44951875, 0.02694226, 0.41524769, 0.9222317,
        0.09120557, 0.31512178)),
      LabeledPoint(0.51408926, Vectors.dense(0.52802224, 0.32806203, 0.44891554, 0.01633442,
        0.0970269, 0.69258857)),
      LabeledPoint(0.84489897, Vectors.dense(0.83594341, 0.42432199, 0.8487743, 0.54679121,
        0.35410346, 0.72724968)),
      LabeledPoint(0.55342816, Vectors.dense(0.09385168, 0.8928588, 0.33625828, 0.89183268,
        0.296849, 0.30164829)),
      LabeledPoint(0.89405683, Vectors.dense(0.80624061, 0.83760997, 0.63428133, 0.3113273,
        0.02944858, 0.39977732)),
      LabeledPoint(0.54588131, Vectors.dense(0.51817346, 0.00738845, 0.77494778, 0.8544712,
        0.13153282, 0.28767364)),
      LabeledPoint(0.96038024, Vectors.dense(0.32658881, 0.90655956, 0.99955954, 0.77088429,
        0.04284752, 0.96525111)),
      LabeledPoint(0.71349698, Vectors.dense(0.97521246, 0.2025168, 0.67985305, 0.46534506,
        0.92001748, 0.72820735)),
      LabeledPoint(0.43456735, Vectors.dense(0.24585653, 0.01953996, 0.70598881, 0.77448287,
        0.4729746, 0.80146736)),
      LabeledPoint(0.52462506, Vectors.dense(0.17539792, 0.72016934, 0.3678759, 0.53209295,
        0.29719397, 0.37429151)),
      LabeledPoint(0.43074793, Vectors.dense(0.72810013, 0.39850784, 0.1058295, 0.39858265,
        0.52196395, 0.1060125)))

    for (numParts <- List(2, 4, 6, 8)) {
      val df = spark.createDataFrame(sc.parallelize(data, numParts))
      val fRegression = FValueTest.test(df, "features", "label")
      val (pValues: Vector, fValues: Vector) =
        fRegression.select("pValues", "fValues")
          .as[(Vector, Vector)].head()
      assert(pValues ~== Vectors.dense(1.73658700e-02, 1.49916659e-02, 1.12697153e-04,
        4.26990301e-01, 2.75911201e-01, 1.93549275e-01) relTol 1e-6)
      assert(fValues ~== Vectors.dense(6.86260598, 7.23175589, 24.11424725, 0.6605354, 1.26266286,
        1.82421406) relTol 1e-6)
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
