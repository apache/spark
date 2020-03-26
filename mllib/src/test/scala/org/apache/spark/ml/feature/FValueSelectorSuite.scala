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

    val data = Seq(
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
