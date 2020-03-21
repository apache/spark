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

class FValueSelectorSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  @transient var dataset: Dataset[_] = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // scalastyle:off
    /*
    FValue REGRESSION
     X (features) =
     [[1.67318514e-01, 1.78398028e-01, 4.36846538e-01, 5.24003164e-01, 1.80915415e-01, 1.98030859e-01],
     [3.71836586e-01, 6.13453963e-01, 7.15269190e-01, 9.33623792e-03, 5.36095674e-01, 2.74223333e-01],
     [3.68988949e-01, 5.34104018e-01, 5.24858744e-01, 6.86815853e-01, 3.26534757e-01, 6.92699400e-01],
     [4.87748505e-02, 3.07080315e-01, 7.82955385e-01, 6.90167375e-01, 6.44077919e-01, 4.23739024e-01],
     [6.50153455e-01, 8.32746110e-01, 6.88029140e-03, 1.27859556e-01, 6.80223767e-01, 6.25825675e-01],

     [9.47343271e-01, 2.13193978e-01, 3.71342472e-01, 8.21291956e-01, 4.38195693e-01, 5.76569439e-01],
     [9.96499254e-01, 8.45833297e-01, 6.56086922e-02, 5.90029174e-01, 1.68954572e-01, 7.19792823e-02],
     [1.85926914e-01, 9.60329804e-01, 3.13487406e-01, 9.59549928e-01, 6.89093311e-01, 6.94999427e-01],
     [9.40164576e-01, 2.69042714e-02, 5.39491321e-01, 5.74068666e-01, 1.10935343e-01, 2.17519760e-01],
     [2.97951848e-02, 1.06592106e-01, 5.74931856e-01, 8.80801522e-01, 8.60445070e-01, 9.22757966e-01],

     [9.80970473e-01, 3.05909353e-01, 4.96401766e-01, 2.44342697e-01, 6.90559227e-01, 5.64858704e-01],
     [1.55939260e-01, 2.18626853e-01, 5.01834270e-01, 1.86694987e-01, 9.15411148e-01, 6.40527848e-01],
     [3.16107608e-01, 9.25906358e-01, 5.47327167e-01, 4.83712979e-01, 8.42305220e-01, 7.58488462e-01],
     [4.14393503e-01, 1.30817883e-01, 5.62034942e-01, 1.05150633e-01, 5.35632795e-01, 9.47594074e-04],
     [5.26233981e-01, 7.63781419e-02, 3.19188240e-01, 5.16528633e-02, 5.28416724e-01, 6.47050470e-03],

     [2.73404764e-01, 7.17070744e-01, 3.12889595e-01, 8.39271965e-01, 9.67650889e-01, 8.50098873e-01],
     [4.63289495e-01, 3.57055416e-02, 5.43528596e-01, 4.44840919e-01, 9.36845855e-02, 7.81595037e-01],
     [3.21784993e-01, 3.15622454e-01, 7.58870408e-01, 5.18198558e-01, 2.28151905e-01, 4.42460325e-01],
     [3.72428352e-01, 1.44447969e-01, 8.40274188e-01, 5.86308041e-01, 6.09893953e-01, 3.97006473e-01],
     [3.12776786e-01, 9.33630195e-01, 2.29328749e-01, 4.32807208e-01, 1.51703470e-02, 1.51589320e-01]]

     y (labels) =
     [0.33997803, 0.71456716, 0.58676766, 0.52894227, 0.53158463,
     0.55515181, 0.67008744, 0.5966537 , 0.56255674, 0.33904133,
     0.66485577, 0.38514965, 0.73885841, 0.45766267, 0.34801557,
     0.52529452, 0.42503336, 0.60221968, 0.58964479, 0.58194949]

     Note that y = X @ w, where w = [0.3, 0.4, 0.5, 0. , 0. , 0. ]

    Sklearn results:
    F values per feature: [2.76445780e+00, 1.05267800e+01, 4.43399092e-02, 2.04580501e-02,
     3.13208557e-02, 1.35248025e-03]
    p values per feature: [0.11369388, 0.0044996 , 0.83558782, 0.88785417, 0.86150261, 0.97106833]
    */
    // scalastyle:on

    val data = Seq(
      (0.33997803, Vectors.dense(1.67318514e-01, 1.78398028e-01, 4.36846538e-01,
        5.24003164e-01, 1.80915415e-01, 1.98030859e-01), Vectors.dense(1.78398028e-01)),
      (0.71456716, Vectors.dense(3.71836586e-01, 6.13453963e-01, 7.15269190e-01,
        9.33623792e-03, 5.36095674e-01, 2.74223333e-01), Vectors.dense(6.13453963e-01)),
      (0.58676766, Vectors.dense(3.68988949e-01, 5.34104018e-01, 5.24858744e-01,
        6.86815853e-01, 3.26534757e-01, 6.92699400e-01), Vectors.dense(5.34104018e-01)),
      (0.52894227, Vectors.dense(4.87748505e-02, 3.07080315e-01, 7.82955385e-01,
        6.90167375e-01, 6.44077919e-01, 4.23739024e-01), Vectors.dense(3.07080315e-01)),
      (0.53158463, Vectors.dense(6.50153455e-01, 8.32746110e-01, 6.88029140e-03,
        1.27859556e-01, 6.80223767e-01, 6.25825675e-01), Vectors.dense(8.32746110e-01)),
      (0.55515181, Vectors.dense(9.47343271e-01, 2.13193978e-01, 3.71342472e-01,
        8.21291956e-01, 4.38195693e-01, 5.76569439e-01), Vectors.dense(2.13193978e-01)),
      (0.67008744, Vectors.dense(9.96499254e-01, 8.45833297e-01, 6.56086922e-02,
        5.90029174e-01, 1.68954572e-01, 7.19792823e-02), Vectors.dense(8.45833297e-01)),
      (0.5966537, Vectors.dense(1.85926914e-01, 9.60329804e-01, 3.13487406e-01,
        9.59549928e-01, 6.89093311e-01, 6.94999427e-01), Vectors.dense(9.60329804e-01)),
      (0.56255674, Vectors.dense(9.40164576e-01, 2.69042714e-02, 5.39491321e-01,
        5.74068666e-01, 1.10935343e-01, 2.17519760e-01), Vectors.dense(2.69042714e-02)),
      (0.33904133, Vectors.dense(2.97951848e-02, 1.06592106e-01, 5.74931856e-01,
        8.80801522e-01, 8.60445070e-01, 9.22757966e-01), Vectors.dense(1.06592106e-01)),
      (0.66485577, Vectors.dense(9.80970473e-01, 3.05909353e-01, 4.96401766e-01,
        2.44342697e-01, 6.90559227e-01, 5.64858704e-01), Vectors.dense(3.05909353e-01)),
      (0.38514965, Vectors.dense(1.55939260e-01, 2.18626853e-01, 5.01834270e-01,
        1.86694987e-01, 9.15411148e-01, 6.40527848e-01), Vectors.dense(2.18626853e-01)),
      (0.73885841, Vectors.dense(3.16107608e-01, 9.25906358e-01, 5.47327167e-01,
        4.83712979e-01, 8.42305220e-01, 7.58488462e-01), Vectors.dense(9.25906358e-01)),
      (0.45766267, Vectors.dense(4.14393503e-01, 1.30817883e-01, 5.62034942e-01,
        1.05150633e-01, 5.35632795e-01, 9.47594074e-04), Vectors.dense(1.30817883e-01)),
      (0.34801557, Vectors.dense(5.26233981e-01, 7.63781419e-02, 3.19188240e-01,
        5.16528633e-02, 5.28416724e-01, 6.47050470e-03), Vectors.dense(7.63781419e-02)),
      (0.52529452, Vectors.dense(2.73404764e-01, 7.17070744e-01, 3.12889595e-01,
        8.39271965e-01, 9.67650889e-01, 8.50098873e-01), Vectors.dense(7.17070744e-01)),
      (0.42503336, Vectors.dense(4.63289495e-01, 3.57055416e-02, 5.43528596e-01,
        4.44840919e-01, 9.36845855e-02, 7.81595037e-01), Vectors.dense(3.57055416e-02)),
      (0.60221968, Vectors.dense(3.21784993e-01, 3.15622454e-01, 7.58870408e-01,
        5.18198558e-01, 2.28151905e-01, 4.42460325e-01), Vectors.dense(3.15622454e-01)),
      (0.58964479, Vectors.dense(3.72428352e-01, 1.44447969e-01, 8.40274188e-01,
        5.86308041e-01, 6.09893953e-01, 3.97006473e-01), Vectors.dense(1.44447969e-01)),
      (0.58194949, Vectors.dense(3.12776786e-01, 9.33630195e-01, 2.29328749e-01,
        4.32807208e-01, 1.51703470e-02, 1.51589320e-01), Vectors.dense(9.33630195e-01)))

    dataset = spark.createDataFrame(data).toDF("label", "features", "topFeature")
  }

  test("params") {
    ParamsSuite.checkParams(new FValueSelector)
  }

  test("Test FValue selector: numTopFeatures") {
    val selector = new FValueSelector()
      .setOutputCol("filtered").setSelectorType("numTopFeatures").setNumTopFeatures(1)
    testSelector(selector, dataset)
  }

  test("Test F Value selector: percentile") {
    val selector = new FValueSelector()
      .setOutputCol("filtered").setSelectorType("percentile").setPercentile(0.17)
    testSelector(selector, dataset)
  }

  test("Test F Value selector: fpr") {
    val selector = new FValueSelector()
      .setOutputCol("filtered").setSelectorType("fpr").setFpr(0.01)
    testSelector(selector, dataset)
  }

  test("Test F Value selector: fdr") {
    val selector = new FValueSelector()
      .setOutputCol("filtered").setSelectorType("fdr").setFdr(0.03)
    testSelector(selector, dataset)
  }

  test("Test F Value selector: fwe") {
    val selector = new FValueSelector()
      .setOutputCol("filtered").setSelectorType("fwe").setFwe(0.03)
    testSelector(selector, dataset)
  }

  test("Test FValue selector with sparse vector") {
    val df = spark.createDataFrame(Seq(
      (4.6, Vectors.sparse(6, Array((0, 6.0), (1, 7.0), (3, 7.0), (4, 6.0))), Vectors.dense(0.0)),
      (6.6, Vectors.sparse(6, Array((1, 9.0), (2, 6.0), (4, 5.0), (5, 9.0))), Vectors.dense(6.0)),
      (5.1, Vectors.sparse(6, Array((1, 9.0), (2, 3.0), (4, 5.0), (5, 5.0))), Vectors.dense(3.0)),
      (7.6, Vectors.dense(Array(0.0, 9.0, 8.0, 5.0, 6.0, 4.0)), Vectors.dense(8.0)),
      (9.0, Vectors.dense(Array(8.0, 9.0, 6.0, 5.0, 4.0, 4.0)), Vectors.dense(6.0)),
      (9.0, Vectors.dense(Array(8.0, 9.0, 6.0, 4.0, 0.0, 0.0)), Vectors.dense(6.0))
    )).toDF("label", "features", "topFeature")

    val selector = new FValueSelector()
      .setOutputCol("filtered").setSelectorType("numTopFeatures").setNumTopFeatures(1)
    testSelector(selector, df)
  }

  test("read/write") {
    def checkModelData(model: FValueSelectorModel, model2:
      FValueSelectorModel): Unit = {
      assert(model.selectedFeatures === model2.selectedFeatures)
    }
    val fSelector = new FValueSelector
    testEstimatorAndModelReadWrite(fSelector, dataset,
      FValueSelectorSuite.allParamSettings,
      FValueSelectorSuite.allParamSettings, checkModelData)
  }

  private def testSelector(selector: FValueSelector, data: Dataset[_]):
      FValueSelectorModel = {
    val selectorModel = selector.fit(data)
    testTransformer[(Double, Vector, Vector)](data.toDF(), selectorModel,
      "filtered", "topFeature") {
      case Row(vec1: Vector, vec2: Vector) =>
        assert(vec1 ~== vec2 absTol 1e-6)
    }
    selectorModel
  }
}

object FValueSelectorSuite {

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
