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

package org.apache.spark.ml.classification

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.linalg.{Vector, Vectors}

final class TestProbabilisticClassificationModel(
    override val uid: String,
    override val numFeatures: Int,
    override val numClasses: Int)
  extends ProbabilisticClassificationModel[Vector, TestProbabilisticClassificationModel] {

  override def copy(extra: org.apache.spark.ml.param.ParamMap): this.type = defaultCopy(extra)

  override protected def predictRaw(input: Vector): Vector = {
    input
  }

  override protected def raw2probabilityInPlace(rawPrediction: Vector): Vector = {
    rawPrediction
  }

  def friendlyPredict(values: Double*): Double = {
    predict(Vectors.dense(values.toArray))
  }
}


class ProbabilisticClassifierSuite extends SparkFunSuite {

  test("test thresholding") {
    val thresholds = Array(0.5, 0.2)
    val testModel = new TestProbabilisticClassificationModel("myuid", 2, 2)
      .setThresholds(thresholds)
    assert(testModel.friendlyPredict(1.0, 1.0) === 1.0)
    assert(testModel.friendlyPredict(1.0, 0.2) === 0.0)
  }

  test("test thresholding not required") {
    val testModel = new TestProbabilisticClassificationModel("myuid", 2, 2)
    assert(testModel.friendlyPredict(1.0, 2.0) === 1.0)
  }

  test("test tiebreak") {
    val testModel = new TestProbabilisticClassificationModel("myuid", 2, 2)
      .setThresholds(Array(0.4, 0.4))
    assert(testModel.friendlyPredict(0.6, 0.6) === 0.0)
  }

  test("bad thresholds") {
    intercept[IllegalArgumentException] {
      new TestProbabilisticClassificationModel("myuid", 2, 2).setThresholds(Array(0.0, 0.1))
    }
    intercept[IllegalArgumentException] {
      new TestProbabilisticClassificationModel("myuid", 2, 2).setThresholds(Array(0.0, 0.0))
    }
    intercept[IllegalArgumentException] {
      new TestProbabilisticClassificationModel("myuid", 2, 2).setThresholds(Array(0.8, 0.8))
    }
  }
}

object ProbabilisticClassifierSuite {

  /**
   * Mapping from all Params to valid settings which differ from the defaults.
   * This is useful for tests which need to exercise all Params, such as save/load.
   * This excludes input columns to simplify some tests.
   */
  val allParamSettings: Map[String, Any] = ClassifierSuite.allParamSettings ++ Map(
    "probabilityCol" -> "myProbability",
    "thresholds" -> Array(0.4, 0.6)
  )

}
