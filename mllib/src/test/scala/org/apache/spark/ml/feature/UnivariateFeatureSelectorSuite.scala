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

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.stat.{ANOVATest, FValueTest}
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest, MLTestingUtils}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.ArrayImplicits._

class UnivariateFeatureSelectorSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  @transient var datasetChi2: Dataset[_] = _
  @transient var datasetAnova: Dataset[_] = _
  @transient var datasetFRegression: Dataset[_] = _

  private var selector1: UnivariateFeatureSelector = _
  private var selector2: UnivariateFeatureSelector = _
  private var selector3: UnivariateFeatureSelector = _

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

    datasetChi2 = spark.createDataFrame(Seq(
      (0.0, Vectors.sparse(6, Array((0, 6.0), (1, 7.0), (3, 7.0), (4, 6.0)).toImmutableArraySeq),
        Vectors.dense(6.0)),
      (1.0, Vectors.sparse(6, Array((1, 9.0), (2, 6.0), (4, 5.0), (5, 9.0)).toImmutableArraySeq),
        Vectors.dense(0.0)),
      (1.0, Vectors.sparse(6, Array((1, 9.0), (2, 3.0), (4, 5.0), (5, 5.0)).toImmutableArraySeq),
        Vectors.dense(0.0)),
      (1.0, Vectors.dense(Array(0.0, 9.0, 8.0, 5.0, 6.0, 4.0)), Vectors.dense(0.0)),
      (2.0, Vectors.dense(Array(8.0, 9.0, 6.0, 5.0, 4.0, 4.0)), Vectors.dense(8.0)),
      (2.0, Vectors.dense(Array(8.0, 9.0, 6.0, 4.0, 0.0, 0.0)), Vectors.dense(8.0))
    )).toDF("label", "features", "topFeature")

    // scalastyle:off
    /*
      X:
      array([[4.65415496e-03, 1.03550567e-01, -1.17358140e+00,
      1.61408773e-01,  3.92492111e-01,  7.31240882e-01],
      [-9.01651741e-01, -5.28905302e-01,  1.27636785e+00,
      7.02154563e-01,  6.21348351e-01,  1.88397353e-01],
      [ 3.85692159e-01, -9.04639637e-01,  5.09782604e-02,
      8.40043971e-01,  7.45977857e-01,  8.78402288e-01],
      [ 1.36264353e+00,  2.62454094e-01,  7.96306202e-01,
      6.14948000e-01,  7.44948187e-01,  9.74034830e-01],
      [ 9.65874070e-01,  2.52773665e+00, -2.19380094e+00,
      2.33408080e-01,  1.86340919e-01,  8.23390433e-01],
      [ 1.12324305e+01, -2.77121515e-01,  1.12740513e-01,
      2.35184013e-01,  3.46668895e-01,  9.38500782e-02],
      [ 1.06195839e+01, -1.82891238e+00,  2.25085601e-01,
      9.09979851e-01,  6.80257535e-02,  8.24017480e-01],
      [ 1.12806837e+01,  1.30686889e+00,  9.32839108e-02,
      3.49784755e-01,  1.71322408e-02,  7.48465194e-02],
      [ 9.98689462e+00,  9.50808938e-01, -2.90786359e-01,
      2.31253009e-01,  7.46270968e-01,  1.60308169e-01],
      [ 1.08428551e+01, -1.02749936e+00,  1.73951508e-01,
      8.92482744e-02,  1.42651730e-01,  7.66751625e-01],
      [-1.98641448e+00,  1.12811990e+01, -2.35246756e-01,
      8.22809049e-01,  3.26739456e-01,  7.88268404e-01],
      [-6.09864090e-01,  1.07346276e+01, -2.18805509e-01,
      7.33931213e-01,  1.42554396e-01,  7.11225605e-01],
      [-1.58481268e+00,  9.19364039e+00, -5.87490459e-02,
      2.51532056e-01,  2.82729807e-01,  7.16245686e-01],
      [-2.50949277e-01,  1.12815254e+01, -6.94806734e-01,
      5.93898886e-01,  5.68425656e-01,  8.49762330e-01],
      [ 7.63485129e-01,  1.02605138e+01,  1.32617719e+00,
      5.49682879e-01,  8.59931442e-01,  4.88677978e-02],
      [ 9.34900015e-01,  4.11379043e-01,  8.65010205e+00,
      9.23509168e-01,  1.16995043e-01,  5.91894106e-03],
      [ 4.73734933e-01, -1.48321181e+00,  9.73349621e+00,
      4.09421563e-01,  5.09375719e-01,  5.93157850e-01],
      [ 3.41470679e-01, -6.88972582e-01,  9.60347938e+00,
      3.62654055e-01,  2.43437468e-01,  7.13052838e-01],
      [-5.29614251e-01, -1.39262856e+00,  1.01354144e+01,
      8.24123861e-01,  5.84074506e-01,  6.54461558e-01],
      [-2.99454508e-01,  2.20457263e+00,  1.14586015e+01,
      5.16336729e-01,  9.99776159e-01,  3.15769738e-01]])
      y:
      array([1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4])
      scikit-learn result:
      >>> f_classif(X, y)
      (array([228.27701422,  84.33070501, 134.25330675,   0.82211775, 0.82991363,   1.08478943]),
       array([2.43864448e-13, 5.09088367e-10, 1.49033067e-11, 5.00596446e-01, 4.96684374e-01, 3.83798191e-01]))
    */
    // scalastyle:on

    val dataAnova = Seq(
      (1, Vectors.dense(4.65415496e-03, 1.03550567e-01, -1.17358140e+00,
      1.61408773e-01, 3.92492111e-01, 7.31240882e-01), Vectors.dense(4.65415496e-03)),
      (1, Vectors.dense(-9.01651741e-01, -5.28905302e-01, 1.27636785e+00,
      7.02154563e-01, 6.21348351e-01, 1.88397353e-01), Vectors.dense(-9.01651741e-01)),
      (1, Vectors.dense(3.85692159e-01, -9.04639637e-01, 5.09782604e-02,
      8.40043971e-01, 7.45977857e-01, 8.78402288e-01), Vectors.dense(3.85692159e-01)),
      (1, Vectors.dense(1.36264353e+00, 2.62454094e-01, 7.96306202e-01,
      6.14948000e-01, 7.44948187e-01, 9.74034830e-01), Vectors.dense(1.36264353e+00)),
      (1, Vectors.dense(9.65874070e-01, 2.52773665e+00, -2.19380094e+00,
        2.33408080e-01, 1.86340919e-01, 8.23390433e-01), Vectors.dense(9.65874070e-01)),
      (2, Vectors.dense(1.12324305e+01, -2.77121515e-01, 1.12740513e-01,
        2.35184013e-01, 3.46668895e-01, 9.38500782e-02), Vectors.dense(1.12324305e+01)),
      (2, Vectors.dense(1.06195839e+01, -1.82891238e+00, 2.25085601e-01,
        9.09979851e-01, 6.80257535e-02, 8.24017480e-01), Vectors.dense(1.06195839e+01)),
      (2, Vectors.dense(1.12806837e+01, 1.30686889e+00, 9.32839108e-02,
        3.49784755e-01, 1.71322408e-02, 7.48465194e-02), Vectors.dense(1.12806837e+01)),
      (2, Vectors.dense(9.98689462e+00, 9.50808938e-01, -2.90786359e-01,
        2.31253009e-01, 7.46270968e-01, 1.60308169e-01), Vectors.dense(9.98689462e+00)),
      (2, Vectors.dense(1.08428551e+01, -1.02749936e+00, 1.73951508e-01,
        8.92482744e-02, 1.42651730e-01, 7.66751625e-01), Vectors.dense(1.08428551e+01)),
      (3, Vectors.dense(-1.98641448e+00, 1.12811990e+01, -2.35246756e-01,
        8.22809049e-01, 3.26739456e-01, 7.88268404e-01), Vectors.dense(-1.98641448e+00)),
      (3, Vectors.dense(-6.09864090e-01, 1.07346276e+01, -2.18805509e-01,
        7.33931213e-01, 1.42554396e-01, 7.11225605e-01), Vectors.dense(-6.09864090e-01)),
      (3, Vectors.dense(-1.58481268e+00, 9.19364039e+00, -5.87490459e-02,
        2.51532056e-01, 2.82729807e-01, 7.16245686e-01), Vectors.dense(-1.58481268e+00)),
      (3, Vectors.dense(-2.50949277e-01, 1.12815254e+01, -6.94806734e-01,
        5.93898886e-01, 5.68425656e-01, 8.49762330e-01), Vectors.dense(-2.50949277e-01)),
      (3, Vectors.dense(7.63485129e-01, 1.02605138e+01, 1.32617719e+00,
        5.49682879e-01, 8.59931442e-01, 4.88677978e-02), Vectors.dense(7.63485129e-01)),
      (4, Vectors.dense(9.34900015e-01, 4.11379043e-01, 8.65010205e+00,
        9.23509168e-01, 1.16995043e-01, 5.91894106e-03), Vectors.dense(9.34900015e-01)),
      (4, Vectors.dense(4.73734933e-01, -1.48321181e+00, 9.73349621e+00,
        4.09421563e-01, 5.09375719e-01, 5.93157850e-01), Vectors.dense(4.73734933e-01)),
      (4, Vectors.dense(3.41470679e-01, -6.88972582e-01, 9.60347938e+00,
        3.62654055e-01, 2.43437468e-01, 7.13052838e-01), Vectors.dense(3.41470679e-01)),
      (4, Vectors.dense(-5.29614251e-01, -1.39262856e+00, 1.01354144e+01,
        8.24123861e-01, 5.84074506e-01, 6.54461558e-01), Vectors.dense(-5.29614251e-01)),
      (4, Vectors.dense(-2.99454508e-01, 2.20457263e+00, 1.14586015e+01,
        5.16336729e-01, 9.99776159e-01, 3.15769738e-01), Vectors.dense(-2.99454508e-01)))

    datasetAnova = spark.createDataFrame(dataAnova).toDF("label", "features", "topFeature")

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
    array([[0.19151945, 0.62210877, 0.43772774, 0.78535858, 0.77997581,
            0.27259261],
           [0.27646426, 0.80187218, 0.95813935, 0.87593263, 0.35781727,
            0.50099513],
           [0.68346294, 0.71270203, 0.37025075, 0.56119619, 0.50308317,
            0.01376845],
           [0.77282662, 0.88264119, 0.36488598, 0.61539618, 0.07538124,
            0.36882401],
           [0.9331401 , 0.65137814, 0.39720258, 0.78873014, 0.31683612,
            0.56809865],
           [0.86912739, 0.43617342, 0.80214764, 0.14376682, 0.70426097,
            0.70458131],
           [0.21879211, 0.92486763, 0.44214076, 0.90931596, 0.05980922,
            0.18428708],
           [0.04735528, 0.67488094, 0.59462478, 0.53331016, 0.04332406,
            0.56143308],
           [0.32966845, 0.50296683, 0.11189432, 0.60719371, 0.56594464,
            0.00676406],
           [0.61744171, 0.91212289, 0.79052413, 0.99208147, 0.95880176,
            0.79196414],
           [0.28525096, 0.62491671, 0.4780938 , 0.19567518, 0.38231745,
            0.05387369],
           [0.45164841, 0.98200474, 0.1239427 , 0.1193809 , 0.73852306,
            0.58730363],
           [0.47163253, 0.10712682, 0.22921857, 0.89996519, 0.41675354,
            0.53585166],
           [0.00620852, 0.30064171, 0.43689317, 0.612149  , 0.91819808,
            0.62573667],
           [0.70599757, 0.14983372, 0.74606341, 0.83100699, 0.63372577,
            0.43830988],
           [0.15257277, 0.56840962, 0.52822428, 0.95142876, 0.48035918,
            0.50255956],
           [0.53687819, 0.81920207, 0.05711564, 0.66942174, 0.76711663,
             0.70811536],
           [0.79686718, 0.55776083, 0.96583653, 0.1471569 , 0.029647  ,
            0.59389349],
           [0.1140657 , 0.95080985, 0.32570741, 0.19361869, 0.45781165,
            0.92040257],
           [0.87906916, 0.25261576, 0.34800879, 0.18258873, 0.90179605,
            0.70652816]])
    >>> y
    array([0.52516321, 0.88275782, 0.67524507, 0.76734745, 0.73909458,
           0.83628141, 0.65665506, 0.58147135, 0.35603443, 0.94534373,
           0.57458887, 0.59026777, 0.29894977, 0.34056582, 0.64476446,
           0.53724782, 0.5173021 , 0.94508275, 0.57739736, 0.53877145])
    >>> f_regression(X, y)
    (array([5.58025504,  3.98311705, 20.59605518,  0.07993376,  1.25127646,
            0.7676937 ]),
    array([2.96302196e-02, 6.13173918e-02, 2.54580618e-04, 7.80612726e-01,
    2.78015517e-01, 3.92474567e-01]))
    */
    // scalastyle:on

    val dataFRegression = Seq(
      (0.52516321, Vectors.dense(0.19151945, 0.62210877, 0.43772774, 0.78535858, 0.77997581,
        0.27259261), Vectors.dense(0.43772774)),
      (0.88275782, Vectors.dense(0.27646426, 0.80187218, 0.95813935, 0.87593263, 0.35781727,
        0.50099513), Vectors.dense(0.95813935)),
      (0.67524507, Vectors.dense(0.68346294, 0.71270203, 0.37025075, 0.56119619, 0.50308317,
        0.01376845), Vectors.dense(0.37025075)),
      (0.76734745, Vectors.dense(0.77282662, 0.88264119, 0.36488598, 0.61539618, 0.07538124,
        0.36882401), Vectors.dense(0.36488598)),
      (0.73909458, Vectors.dense(0.9331401, 0.65137814, 0.39720258, 0.78873014, 0.31683612,
        0.56809865), Vectors.dense(0.39720258)),

      (0.83628141, Vectors.dense(0.86912739, 0.43617342, 0.80214764, 0.14376682, 0.70426097,
        0.70458131), Vectors.dense(0.80214764)),
      (0.65665506, Vectors.dense(0.21879211, 0.92486763, 0.44214076, 0.90931596, 0.05980922,
        0.18428708), Vectors.dense(0.44214076)),
      (0.58147135, Vectors.dense(0.04735528, 0.67488094, 0.59462478, 0.53331016, 0.04332406,
        0.56143308), Vectors.dense(0.59462478)),
      (0.35603443, Vectors.dense(0.32966845, 0.50296683, 0.11189432, 0.60719371, 0.56594464,
        0.00676406), Vectors.dense(0.11189432)),
      (0.94534373, Vectors.dense(0.61744171, 0.91212289, 0.79052413, 0.99208147, 0.95880176,
        0.79196414), Vectors.dense(0.79052413)),

      (0.57458887, Vectors.dense(0.28525096, 0.62491671, 0.4780938, 0.19567518, 0.38231745,
        0.05387369), Vectors.dense(0.4780938)),
      (0.59026777, Vectors.dense(0.45164841, 0.98200474, 0.1239427, 0.1193809, 0.73852306,
        0.58730363), Vectors.dense(0.1239427)),
      (0.29894977, Vectors.dense(0.47163253, 0.10712682, 0.22921857, 0.89996519, 0.41675354,
        0.53585166), Vectors.dense(0.22921857)),
      (0.34056582, Vectors.dense(0.00620852, 0.30064171, 0.43689317, 0.612149, 0.91819808,
        0.62573667), Vectors.dense(0.43689317)),
      (0.64476446, Vectors.dense(0.70599757, 0.14983372, 0.74606341, 0.83100699, 0.63372577,
        0.43830988), Vectors.dense(0.74606341)),

      (0.53724782, Vectors.dense(0.15257277, 0.56840962, 0.52822428, 0.95142876, 0.48035918,
        0.50255956), Vectors.dense(0.52822428)),
      (0.5173021, Vectors.dense(0.53687819, 0.81920207, 0.05711564, 0.66942174, 0.76711663,
        0.70811536), Vectors.dense(0.05711564)),
      (0.94508275, Vectors.dense(0.79686718, 0.55776083, 0.96583653, 0.1471569, 0.029647,
        0.59389349), Vectors.dense(0.96583653)),
      (0.57739736, Vectors.dense(0.1140657, 0.95080985, 0.96583653, 0.19361869, 0.45781165,
        0.92040257), Vectors.dense(0.96583653)),
      (0.53877145, Vectors.dense(0.87906916, 0.25261576, 0.34800879, 0.18258873, 0.90179605,
        0.70652816), Vectors.dense(0.34800879)))

    datasetFRegression = spark.createDataFrame(dataFRegression)
      .toDF("label", "features", "topFeature")

    selector1 = new UnivariateFeatureSelector()
      .setOutputCol("filtered")
      .setFeatureType("continuous")
      .setLabelType("categorical")
    selector2 = new UnivariateFeatureSelector()
      .setOutputCol("filtered")
      .setFeatureType("continuous")
      .setLabelType("continuous")
    selector3 = new UnivariateFeatureSelector()
      .setOutputCol("filtered")
      .setFeatureType("categorical")
      .setLabelType("categorical")
  }

  test("params") {
    ParamsSuite.checkParams(new UnivariateFeatureSelector())
  }

  test("Test numTopFeatures") {
    val testParams: Seq[(UnivariateFeatureSelector, Dataset[_])] = Seq(
      (selector1.setSelectionMode("numTopFeatures").setSelectionThreshold(1), datasetAnova),
      (selector2.setSelectionMode("numTopFeatures").setSelectionThreshold(1), datasetFRegression),
      (selector3.setSelectionMode("numTopFeatures").setSelectionThreshold(1), datasetChi2)
    )
    for ((sel, dataset) <- testParams) {
      val model = testSelector(sel, dataset)
      MLTestingUtils.checkCopyAndUids(sel, model)
    }
  }

  test("Test percentile") {
    val testParams: Seq[(UnivariateFeatureSelector, Dataset[_])] = Seq(
      (selector1.setSelectionMode("percentile").setSelectionThreshold(0.17), datasetAnova),
      (selector2.setSelectionMode("percentile").setSelectionThreshold(0.17), datasetFRegression),
      (selector3.setSelectionMode("percentile").setSelectionThreshold(0.17), datasetChi2)
    )
    for ((sel, dataset) <- testParams) {
      val model = testSelector(sel, dataset)
      MLTestingUtils.checkCopyAndUids(sel, model)
    }
  }

  test("Test fpr") {
    val testParams: Seq[(UnivariateFeatureSelector, Dataset[_])] = Seq(
      (selector1.setSelectionMode("fpr").setSelectionThreshold(1.0E-12), datasetAnova),
      (selector2.setSelectionMode("fpr").setSelectionThreshold(0.01), datasetFRegression),
      (selector3.setSelectionMode("fpr").setSelectionThreshold(0.02), datasetChi2)
    )
    for ((sel, dataset) <- testParams) {
      val model = testSelector(sel, dataset)
      MLTestingUtils.checkCopyAndUids(sel, model)
    }
  }

  test("Test fdr") {
    val testParams: Seq[(UnivariateFeatureSelector, Dataset[_])] = Seq(
      (selector1.setSelectionMode("fdr").setSelectionThreshold(6.0E-12), datasetAnova),
      (selector2.setSelectionMode("fdr").setSelectionThreshold(0.03), datasetFRegression),
      (selector3.setSelectionMode("fdr").setSelectionThreshold(0.12), datasetChi2)
    )
    for ((sel, dataset) <- testParams) {
      val model = testSelector(sel, dataset)
      MLTestingUtils.checkCopyAndUids(sel, model)
    }
  }

  test("Test fwe") {
    val testParams: Seq[(UnivariateFeatureSelector, Dataset[_])] = Seq(
      (selector1.setSelectionMode("fwe").setSelectionThreshold(6.0E-12), datasetAnova),
      (selector2.setSelectionMode("fwe").setSelectionThreshold(0.03), datasetFRegression),
      (selector3.setSelectionMode("fwe").setSelectionThreshold(0.12), datasetChi2)
    )
    for ((sel, dataset) <- testParams) {
      val model = testSelector(sel, dataset)
      MLTestingUtils.checkCopyAndUids(sel, model)
    }
  }

  // use the following sklean program to verify the test
  // scalastyle:off
  /*
  import numpy as np
    from sklearn.feature_selection import SelectFdr, f_classif

  X = np.random.rand(10, 6)
  w = np.array([5, 5, 0.0, 0, 0, 0]).reshape((-1, 1))
  y = np.rint(0.1 * (X @ w)).flatten()
  print(X)
  print(y)

  F, p = f_classif(X, y)
  print('F', F)
  print('p', p)
  selected = SelectFdr(f_classif, alpha=0.25).fit(X, y).get_support(True)

  print(selected)
  */

  /*
  sklearn result
  [[0.92166066 0.82295823 0.31276624 0.63069973 0.64679537 0.94138368]
  [0.47027783 0.74907889 0.43660557 0.93212582 0.5654378  0.531748  ]
  [0.67771108 0.23926502 0.66906295 0.73117095 0.67340005 0.52864934]
  [0.84565144 0.28050298 0.94137135 0.42479664 0.21600724 0.98956871]
  [0.58818255 0.32223507 0.13727654 0.80948059 0.94617741 0.48460179]
  [0.59528639 0.75838511 0.98648654 0.65561948 0.83818237 0.30178127]
  [0.00264811 0.46492597 0.71428557 0.94708987 0.54587827 0.9484639 ]
  [0.94604186 0.43187098 0.42135172 0.77256283 0.44334613 0.1514674 ]
  [0.45694004 0.00273459 0.14580367 0.74278963 0.57819284 0.99413419]
  [0.02256925 0.56136702 0.0629738  0.64130602 0.01536191 0.56638321]]
  [1. 1. 0. 1. 0. 1. 0. 1. 0. 0.]
  F [5.66456136e+00 4.08120006e+00 1.85418412e+00 8.67095392e-01
  2.87769237e-03 3.66010633e-01]
  p [0.04454332 0.07803464 0.21040406 0.37900428 0.95853411 0.56195058]
  [0 1]

  [[0.27976711 0.48397753 0.18451698 0.59844137 0.01459805 0.98895542]
  [0.97192726 0.46737333 0.08048093 0.38253056 0.04776121 0.55949538]
  [0.62559834 0.44102192 0.19199043 0.959706   0.5332824  0.78621594]
  [0.91649448 0.76501992 0.58678528 0.75239909 0.33179368 0.00893317]
  [0.14086806 0.21876364 0.31767297 0.53061653 0.02786653 0.20021944]
  [0.15214833 0.03028593 0.12326784 0.55663152 0.8333684  0.76923807]
  [0.88178287 0.8492688  0.29417221 0.98122401 0.44103191 0.32709781]
  [0.06686689 0.05834763 0.41316273 0.92850555 0.77308549 0.2931857 ]
  [0.94747449 0.78336777 0.76096282 0.52368192 0.64814324 0.60455684]
  [0.83382261 0.31412713 0.62490246 0.43896432 0.35390503 0.02316754]]
  [0. 1. 1. 1. 0. 0. 1. 0. 1. 1.]
  F [9.22227201e+01 8.36710241e+00 1.22217112e+00 1.63526175e-02
  8.91954821e-03 6.44534477e-01]
  p [1.14739663e-05 2.01189199e-02 3.01070031e-01 9.01402125e-01
  9.27079623e-01 4.45267639e-01]
  [0 1]
  */
  // scalastyle:on
  test("Test selectIndicesFromPValues f_classif") {
    val data_f_classif1 = Seq(
      (1, Vectors.dense(0.92166066, 0.82295823, 0.31276624, 0.63069973, 0.64679537, 0.94138368),
        Vectors.dense(0.92166066, 0.82295823)),
      (1, Vectors.dense(0.47027783, 0.74907889, 0.43660557, 0.93212582, 0.5654378, 0.531748),
        Vectors.dense(0.47027783, 0.74907889)),
      (0, Vectors.dense(0.67771108, 0.23926502, 0.66906295, 0.73117095, 0.67340005, 0.52864934),
        Vectors.dense(0.67771108, 0.23926502)),
      (1, Vectors.dense(0.84565144, 0.28050298, 0.94137135, 0.42479664, 0.21600724, 0.98956871),
        Vectors.dense(0.84565144, 0.28050298)),
      (0, Vectors.dense(0.58818255, 0.32223507, 0.13727654, 0.80948059, 0.94617741, 0.48460179),
        Vectors.dense(0.58818255, 0.32223507)),
      (1, Vectors.dense(0.59528639, 0.75838511, 0.98648654, 0.65561948, 0.83818237, 0.30178127),
        Vectors.dense(0.59528639, 0.75838511)),
      (0, Vectors.dense(0.00264811, 0.46492597, 0.71428557, 0.94708987, 0.54587827, 0.9484639),
        Vectors.dense(0.00264811, 0.46492597)),
      (1, Vectors.dense(0.94604186, 0.43187098, 0.42135172, 0.77256283, 0.44334613, 0.1514674),
        Vectors.dense(0.94604186, 0.43187098)),
      (0, Vectors.dense(0.45694004, 0.00273459, 0.14580367, 0.74278963, 0.57819284, 0.99413419),
        Vectors.dense(0.45694004, 0.00273459)),
      (0, Vectors.dense(0.02256925, 0.56136702, 0.0629738, 0.64130602, 0.01536191, 0.56638321),
        Vectors.dense(0.02256925, 0.56136702)))

    val data_f_classif2 = Seq(
      (0, Vectors.dense(0.27976711, 0.48397753, 0.18451698, 0.59844137, 0.01459805, 0.98895542),
        Vectors.dense(0.27976711, 0.48397753)),
      (1, Vectors.dense(0.97192726, 0.46737333, 0.08048093, 0.38253056, 0.04776121, 0.55949538),
        Vectors.dense(0.97192726, 0.46737333)),
      (1, Vectors.dense(0.62559834, 0.44102192, 0.19199043, 0.959706, 0.5332824, 0.78621594),
        Vectors.dense(0.62559834, 0.44102192)),
      (1, Vectors.dense(0.91649448, 0.76501992, 0.58678528, 0.75239909, 0.33179368, 0.00893317),
        Vectors.dense(0.91649448, 0.76501992)),
      (0, Vectors.dense(0.14086806, 0.21876364, 0.31767297, 0.53061653, 0.02786653, 0.20021944),
        Vectors.dense(0.14086806, 0.21876364)),
      (0, Vectors.dense(0.15214833, 0.03028593, 0.12326784, 0.55663152, 0.8333684, 0.76923807),
        Vectors.dense(0.15214833, 0.03028593)),
      (1, Vectors.dense(0.88178287, 0.8492688, 0.29417221, 0.98122401, 0.44103191, 0.32709781),
        Vectors.dense(0.88178287, 0.8492688)),
      (0, Vectors.dense(0.06686689, 0.05834763, 0.41316273, 0.92850555, 0.77308549, 0.2931857),
        Vectors.dense(0.06686689, 0.05834763)),
      (1, Vectors.dense(0.94747449, 0.78336777, 0.76096282, 0.52368192, 0.64814324, 0.60455684),
        Vectors.dense(0.94747449, 0.78336777)),
      (1, Vectors.dense(0.83382261, 0.31412713, 0.62490246, 0.43896432, 0.35390503, 0.02316754),
        Vectors.dense(0.83382261, 0.31412713)))

    val dataset_f_classification1 =
      spark.createDataFrame(data_f_classif1).toDF("label", "features", "topFeature")

    val dataset_f_classification2 =
      spark.createDataFrame(data_f_classif2).toDF("label", "features", "topFeature")

    val resultDF1 = ANOVATest.test(dataset_f_classification1.toDF(), "features", "label", true)
    val resultDF2 = ANOVATest.test(dataset_f_classification2.toDF(), "features", "label", true)
    val selector = new UnivariateFeatureSelector()
      .setOutputCol("filtered")
      .setFeatureType("continuous")
      .setLabelType("categorical")
    val indices1 = selector.selectIndicesFromPValues(6, resultDF1, "fdr", 0.25)
    val indices2 = selector.selectIndicesFromPValues(6, resultDF2, "fdr", 0.25)
    assert(indices1(0) === 0 && indices1(1) === 1)
    assert(indices2(0) === 0 && indices1(1) === 1)
  }

  // use the following sklean program to verify the test
  // scalastyle:off
  /* import numpy as np
    from sklearn.feature_selection import SelectFdr, f_regression

  X = np.random.rand(10, 6)
  w = np.array([5, 5, 0.0, 0, 0, 0]).reshape((-1, 1))
  y = (X @ w).flatten()
  print(X)
  print(y)

  F, p = f_regression(X, y)
  print('F', F)
  print('p', p)
  selected = SelectFdr(f_regression, alpha=0.1).fit(X, y).get_support(True)

  print(selected) */

  /* sklean result
  [[5.19537247e-01 4.53144603e-01 2.10190418e-01 9.76237361e-01
  9.05792824e-01 9.34081024e-01]
  [8.68906163e-01 5.49099467e-01 6.73567960e-01 3.94736897e-01
  9.98764158e-01 1.14285918e-01]
  [2.56211244e-01 5.21857152e-01 6.55000402e-01 4.81092256e-01
  4.05802734e-02 1.59811005e-01]
  [9.03076723e-01 1.80316576e-01 8.13131160e-01 6.92327901e-01
  4.77693321e-01 2.17284784e-01]
  [4.75926597e-01 6.80511651e-01 9.55843875e-01 1.52627108e-01
  1.72766587e-01 6.45234673e-01]
  [6.05829005e-01 8.43879811e-01 4.48596383e-01 7.25003439e-01
  2.83962640e-02 5.14414827e-01]
  [8.57631869e-01 1.18279868e-01 2.84428492e-01 8.51544596e-01
  1.33220409e-02 1.87044251e-01]
  [2.43360773e-01 4.83288948e-02 1.10430569e-01 4.33097852e-01
  5.63452248e-02 8.24333214e-01]
  [2.18226531e-01 5.28477779e-01 3.01852956e-01 6.31664822e-04
  8.97463990e-01 8.25297034e-01]
  [6.95170305e-01 7.35775299e-01 4.32188618e-01 2.26744166e-01
  5.13186095e-01 2.91635657e-01]]
  [4.86340925 7.09002815 3.89034198 5.4169665  5.78219124 7.24854408
  4.87955868 1.45844834 3.73352155 7.15472802]
  F [6.79932587 7.09311449 2.25262252 0.02652918 0.40812054 2.14464201]
  p [0.03124895 0.02865887 0.17178184 0.87465381 0.54077957 0.18122753]
  [0 1]
  */

  /* SKLearn result
  [[0.21557113 0.66070242 0.89964323 0.1569332  0.84097522 0.61614986]
  [0.14790391 0.40356507 0.2973803  0.53051143 0.35408457 0.88180598]
  [0.39333276 0.42790148 0.41415147 0.82478069 0.57201431 0.49972278]
  [0.46189165 0.460305   0.21054573 0.16588781 0.72898672 0.41290627]
  [0.42527082 0.83902909 0.97275171 0.76947383 0.24470714 0.57847281]
  [0.56185556 0.94463811 0.97741409 0.27233834 0.76460529 0.53085766]
  [0.5828694  0.45827703 0.49305311 0.13803643 0.18242319 0.14182515]
  [0.98848811 0.43453809 0.11712213 0.4849829  0.06431555 0.76125387]
  [0.1181108  0.43820753 0.49576967 0.75729578 0.35355208 0.48165022]
  [0.44250624 0.24310088 0.03976366 0.24023351 0.91659502 0.75260252]]
  [4.38136774 2.7573449  4.10617119 4.61098326 6.32149954 7.53246836
  5.20573215 7.11513098 2.78159163 3.42803558]
  F [11.90962327  6.49595546  1.51054886  0.17751367  0.40829523  0.1797005 ]
  p [0.0086816  0.03424301 0.25397764 0.68461076 0.54069506 0.68279904]
  [0] */
  // scalastyle:on
  test("Test selectIndicesFromPValues f_regression") {
    val data_f_regression1 = Seq(
      (4.86340925, Vectors.dense(5.19537247e-01, 4.53144603e-01, 2.10190418e-01, 9.76237361e-01,
        9.05792824e-01, 9.34081024e-01), Vectors.dense(5.19537247e-01, 4.53144603e-01)),
      (7.09002815, Vectors.dense(8.68906163e-01, 5.49099467e-01, 6.73567960e-01, 3.94736897e-01,
        9.98764158e-01, 1.14285918e-01), Vectors.dense(8.68906163e-01, 5.49099467e-01)),
      (3.89034198, Vectors.dense(2.56211244e-01, 5.21857152e-01, 6.55000402e-01, 4.81092256e-01,
        4.05802734e-02, 1.59811005e-01), Vectors.dense(2.56211244e-01, 5.21857152e-01)),
      (5.4169665, Vectors.dense(9.03076723e-01, 1.80316576e-01, 8.13131160e-01, 6.92327901e-01,
        4.77693321e-01, 2.17284784e-01), Vectors.dense(9.03076723e-01, 1.80316576e-01)),
      (5.78219124, Vectors.dense(4.75926597e-01, 6.80511651e-01, 9.55843875e-01, 1.52627108e-01,
        1.72766587e-01, 6.45234673e-01), Vectors.dense(4.75926597e-01, 6.80511651e-01)),
      (7.24854408, Vectors.dense(6.05829005e-01, 8.43879811e-01, 4.48596383e-01, 7.25003439e-01,
        2.83962640e-02, 5.14414827e-01), Vectors.dense(6.05829005e-01, 8.43879811e-01)),
      (4.87955868, Vectors.dense(8.57631869e-01, 1.18279868e-01, 2.84428492e-01, 8.51544596e-01,
        1.33220409e-02, 1.87044251e-01), Vectors.dense(8.57631869e-01, 1.18279868e-01)),
      (1.45844834, Vectors.dense(2.43360773e-01, 4.83288948e-02, 1.10430569e-01, 4.33097852e-01,
        5.63452248e-02, 8.24333214e-01), Vectors.dense(2.43360773e-01, 4.83288948e-02)),
      (3.73352155, Vectors.dense(2.18226531e-01, 5.28477779e-01, 3.01852956e-01, 6.31664822e-04,
        8.97463990e-01, 8.25297034e-01), Vectors.dense(2.18226531e-01, 5.28477779e-01)),
      (7.15472802, Vectors.dense(6.95170305e-01, 7.35775299e-01, 4.32188618e-01, 2.26744166e-01,
        5.13186095e-01, 2.91635657e-01), Vectors.dense(6.95170305e-01, 7.35775299e-01)))

    val data_f_regression2 = Seq(
      (4.38136774, Vectors.dense(0.21557113, 0.66070242, 0.89964323, 0.1569332, 0.84097522,
        0.61614986), Vectors.dense(0.21557113)),
      (2.7573449, Vectors.dense(0.14790391, 0.40356507, 0.2973803, 0.53051143, 0.35408457,
        0.88180598), Vectors.dense(0.14790391)),
      (4.10617119, Vectors.dense(0.39333276, 0.42790148, 0.41415147, 0.82478069, 0.57201431,
        0.49972278), Vectors.dense(0.39333276)),
      (4.61098326, Vectors.dense(0.46189165, 0.460305, 0.21054573, 0.16588781, 0.72898672,
        0.41290627), Vectors.dense(0.46189165)),
      (6.32149954, Vectors.dense(0.42527082, 0.83902909, 0.97275171, 0.76947383, 0.24470714,
        0.57847281), Vectors.dense(0.42527082)),
      (7.53246836, Vectors.dense(0.56185556, 0.94463811, 0.97741409, 0.27233834, 0.76460529,
        0.53085766), Vectors.dense(0.56185556)),
      (5.20573215, Vectors.dense(0.5828694, 0.45827703, 0.49305311, 0.13803643, 0.18242319,
        0.14182515), Vectors.dense(0.5828694)),
      (7.11513098, Vectors.dense(0.98848811, 0.43453809, 0.11712213, 0.4849829, 0.06431555,
        0.76125387), Vectors.dense(0.98848811)),
      (2.78159163, Vectors.dense(0.1181108, 0.43820753, 0.49576967, 0.75729578, 0.35355208,
        0.48165022), Vectors.dense(0.1181108)),
      (3.42803558, Vectors.dense(0.44250624, 0.24310088, 0.03976366, 0.24023351, 0.91659502,
        0.75260252), Vectors.dense(0.44250624)))

    val dataset_f_regression1 =
      spark.createDataFrame(data_f_regression1).toDF("label", "features", "topFeature")

    val dataset_f_regression2 =
      spark.createDataFrame(data_f_regression2).toDF("label", "features", "topFeature")

    val resultDF1 = FValueTest.test(dataset_f_regression1.toDF(), "features", "label", true)
    val resultDF2 = FValueTest.test(dataset_f_regression2.toDF(), "features", "label", true)
    val selector = new UnivariateFeatureSelector()
      .setOutputCol("filtered")
      .setFeatureType("continuous")
      .setLabelType("continuous")
    val indices1 = selector.selectIndicesFromPValues(6, resultDF1, "fdr", 0.1)
    val indices2 = selector.selectIndicesFromPValues(6, resultDF2, "fdr", 0.1)
    assert(indices1(0) === 1 && indices1(1) === 0)
    assert(indices2(0) === 0)
  }

  test("read/write") {
    def checkModelData(
        model: UnivariateFeatureSelectorModel,
        model2: UnivariateFeatureSelectorModel): Unit = {
      assert(model.selectedFeatures === model2.selectedFeatures)
    }
    val selector = new UnivariateFeatureSelector()
      .setFeatureType("continuous")
      .setLabelType("categorical")
    testEstimatorAndModelReadWrite(selector, datasetAnova,
      UnivariateFeatureSelectorSuite.allParamSettings,
      UnivariateFeatureSelectorSuite.allParamSettings, checkModelData)
  }

  private def testSelector(selector: UnivariateFeatureSelector, data: Dataset[_]):
  UnivariateFeatureSelectorModel = {
    val selectorModel = selector.fit(data)
    testTransformer[(Double, Vector, Vector)](data.toDF(), selectorModel,
      "filtered", "topFeature") {
      case Row(vec1: Vector, vec2: Vector) =>
        assert(vec1 ~== vec2 absTol 1e-1)
    }
    selectorModel
  }
}

object UnivariateFeatureSelectorSuite {

  /**
   * Mapping from all Params to valid settings which differ from the defaults.
   * This is useful for tests which need to exercise all Params, such as save/load.
   * This excludes input columns to simplify some tests.
   */
  val allParamSettings: Map[String, Any] = Map(
    "selectionMode" -> "percentile",
    "selectionThreshold" -> 0.12,
    "outputCol" -> "myOutput"
  )
}
