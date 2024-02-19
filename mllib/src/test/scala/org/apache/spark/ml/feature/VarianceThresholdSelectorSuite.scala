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
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest, MLTestingUtils}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.ArrayImplicits._

class VarianceThresholdSelectorSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  @transient var dataset: Dataset[_] = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val data = Seq(
      (1, Vectors.dense(Array(6.0, 7.0, 0.0, 5.0, 6.0, 0.0)),
        Vectors.dense(Array(6.0, 7.0, 0.0, 6.0, 0.0))),
      (2, Vectors.dense(Array(0.0, 9.0, 6.0, 5.0, 5.0, 9.0)),
        Vectors.dense(Array(0.0, 9.0, 6.0, 5.0, 9.0))),
      (3, Vectors.dense(Array(0.0, 9.0, 3.0, 5.0, 5.0, 5.0)),
        Vectors.dense(Array(0.0, 9.0, 3.0, 5.0, 5.0))),
      (4, Vectors.dense(Array(0.0, 9.0, 8.0, 5.0, 6.0, 4.0)),
        Vectors.dense(Array(0.0, 9.0, 8.0, 6.0, 4.0))),
      (5, Vectors.dense(Array(8.0, 9.0, 6.0, 5.0, 4.0, 4.0)),
        Vectors.dense(Array(8.0, 9.0, 6.0, 4.0, 4.0))),
      (6, Vectors.dense(Array(8.0, 9.0, 6.0, 5.0, 0.0, 0.0)),
        Vectors.dense(Array(8.0, 9.0, 6.0, 0.0, 0.0))))

    dataset = spark.createDataFrame(data).toDF("id", "features", "expected")
  }

  test("params") {
    ParamsSuite.checkParams(new VarianceThresholdSelector)
  }

  test("Test VarianceThresholdSelector: varianceThreshold not set") {
    val selector = new VarianceThresholdSelector().setOutputCol("filtered")
    val model = testSelector(selector, dataset)
    MLTestingUtils.checkCopyAndUids(selector, model)
  }

  test("Test VarianceThresholdSelector: set varianceThreshold") {
    val df = spark.createDataFrame(Seq(
      (1, Vectors.dense(Array(6.0, 7.0, 0.0, 7.0, 6.0, 0.0)),
        Vectors.dense(Array(6.0, 7.0, 0.0))),
      (2, Vectors.dense(Array(0.0, 9.0, 6.0, 0.0, 5.0, 9.0)),
        Vectors.dense(Array(0.0, 0.0, 9.0))),
      (3, Vectors.dense(Array(0.0, 9.0, 3.0, 0.0, 5.0, 5.0)),
        Vectors.dense(Array(0.0, 0.0, 5.0))),
      (4, Vectors.dense(Array(0.0, 9.0, 8.0, 5.0, 6.0, 4.0)),
        Vectors.dense(Array(0.0, 5.0, 4.0))),
      (5, Vectors.dense(Array(8.0, 9.0, 6.0, 5.0, 4.0, 4.0)),
        Vectors.dense(Array(8.0, 5.0, 4.0))),
      (6, Vectors.dense(Array(8.0, 9.0, 6.0, 0.0, 0.0, 0.0)),
        Vectors.dense(Array(8.0, 0.0, 0.0)))
    )).toDF("id", "features", "expected")
    val selector = new VarianceThresholdSelector()
      .setVarianceThreshold(8.2)
      .setOutputCol("filtered")
    val model = testSelector(selector, df)
    MLTestingUtils.checkCopyAndUids(selector, model)
  }

  test("Test VarianceThresholdSelector: sparse vector") {
    val df = spark.createDataFrame(Seq(
      (1, Vectors.sparse(6, Array((0, 6.0), (1, 7.0), (3, 7.0), (4, 6.0)).toImmutableArraySeq),
        Vectors.dense(Array(6.0, 0.0, 7.0, 0.0))),
      (2, Vectors.sparse(6, Array((1, 9.0), (2, 6.0), (4, 5.0), (5, 9.0)).toImmutableArraySeq),
        Vectors.dense(Array(0.0, 6.0, 0.0, 9.0))),
      (3, Vectors.sparse(6, Array((1, 9.0), (2, 3.0), (4, 5.0), (5, 5.0)).toImmutableArraySeq),
        Vectors.dense(Array(0.0, 3.0, 0.0, 5.0))),
      (4, Vectors.dense(Array(0.0, 9.0, 8.0, 5.0, 6.0, 4.0)),
        Vectors.dense(Array(0.0, 8.0, 5.0, 4.0))),
      (5, Vectors.dense(Array(8.0, 9.0, 6.0, 5.0, 4.0, 4.0)),
        Vectors.dense(Array(8.0, 6.0, 5.0, 4.0))),
      (6, Vectors.dense(Array(8.0, 9.0, 6.0, 4.0, 0.0, 0.0)),
        Vectors.dense(Array(8.0, 6.0, 4.0, 0.0)))
    )).toDF("id", "features", "expected")
    val selector = new VarianceThresholdSelector()
      .setVarianceThreshold(8.1)
      .setOutputCol("filtered")
    val model = testSelector(selector, df)
    MLTestingUtils.checkCopyAndUids(selector, model)
  }

  test("read/write") {
    def checkModelData(model: VarianceThresholdSelectorModel, model2:
    VarianceThresholdSelectorModel): Unit = {
      assert(model.selectedFeatures === model2.selectedFeatures)
    }
    val varSelector = new VarianceThresholdSelector
    testEstimatorAndModelReadWrite(varSelector, dataset,
      VarianceThresholdSelectorSuite.allParamSettings,
      VarianceThresholdSelectorSuite.allParamSettings, checkModelData)
  }

  private def testSelector(selector: VarianceThresholdSelector, data: Dataset[_]):
    VarianceThresholdSelectorModel = {
    val selectorModel = selector.fit(data)
    testTransformer[(Int, Vector, Vector)](data.toDF(), selectorModel,
      "filtered", "expected") {
      case Row(vec1: Vector, vec2: Vector) =>
        assert(vec1 ~== vec2 absTol 1e-6)
    }
    selectorModel
  }
}

object VarianceThresholdSelectorSuite {

  /**
   * Mapping from all Params to valid settings which differ from the defaults.
   * This is useful for tests which need to exercise all Params, such as save/load.
   * This excludes input columns to simplify some tests.
   */
  val allParamSettings: Map[String, Any] = Map(
    "varianceThreshold" -> 0.12,
    "outputCol" -> "myOutput"
  )
}
