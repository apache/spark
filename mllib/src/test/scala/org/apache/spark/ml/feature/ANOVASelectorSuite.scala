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
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.sql.{Dataset, Row}

class ANOVASelectorSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  @transient var dataset: Dataset[_] = _

  override def beforeAll(): Unit = {
    super.beforeAll()

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

    val data = Seq(
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

    dataset = spark.createDataFrame(data).toDF("label", "features", "topFeature")
  }

  test("params") {
    ParamsSuite.checkParams(new ANOVASelector())
  }

  test("Test ANOVAFValue calssification selector: numTopFeatures") {
    val selector = new ANOVASelector()
      .setOutputCol("filtered").setSelectorType("numTopFeatures").setNumTopFeatures(1)
    testSelector(selector, dataset)
  }

  test("Test ANOVAFValue calssification selector: percentile") {
    val selector = new ANOVASelector()
      .setOutputCol("filtered").setSelectorType("percentile").setPercentile(0.17)
    testSelector(selector, dataset)
  }

  test("Test ANOVAFValue calssification selector: fpr") {
    val selector = new ANOVASelector()
      .setOutputCol("filtered").setSelectorType("fpr").setFpr(1.0E-12)
    testSelector(selector, dataset)
  }

  test("Test ANOVAFValue calssification selector: fdr") {
    val selector = new ANOVASelector()
      .setOutputCol("filtered").setSelectorType("fdr").setFdr(6.0E-12)
    testSelector(selector, dataset)
  }

  test("Test ANOVAFValue calssification selector: fwe") {
    val selector = new ANOVASelector()
      .setOutputCol("filtered").setSelectorType("fwe").setFwe(6.0E-12)
    testSelector(selector, dataset)
  }

  test("read/write") {
    def checkModelData(model: ANOVASelectorModel, model2: ANOVASelectorModel): Unit = {
      assert(model.selectedFeatures === model2.selectedFeatures)
    }
    val anovaSelector = new ANOVASelector()
    testEstimatorAndModelReadWrite(anovaSelector, dataset,
      ANOVASelectorSuite.allParamSettings,
      ANOVASelectorSuite.allParamSettings, checkModelData)
  }

  private def testSelector(selector: ANOVASelector, data: Dataset[_]):
  ANOVASelectorModel = {
    val selectorModel = selector.fit(data)
    testTransformer[(Double, Vector, Vector)](data.toDF(), selectorModel,
      "filtered", "topFeature") {
      case Row(vec1: Vector, vec2: Vector) =>
        assert(vec1 ~== vec2 absTol 1e-1)
    }
    selectorModel
  }
}

object ANOVASelectorSuite {

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
