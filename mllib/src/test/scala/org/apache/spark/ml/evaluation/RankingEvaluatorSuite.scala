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

package org.apache.spark.ml.evaluation

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._

class RankingEvaluatorSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import testImplicits._

  test("params") {
    ParamsSuite.checkParams(new RankingEvaluator)
  }

  test("Ranking Evaluator: default params") {

    val predictionAndLabels =
      Seq(
        (1L, 1L, 0.5f),
        (1L, 2L, Float.NaN),
        (2L, 1L, 0.1f),
        (2L, 2L, 0.7f)
      ).toDF(Seq("query", "label", "prediction"): _*)

    // default = mpr, k = 1
    val evaluator = new RankingEvaluator()
    assert(evaluator.evaluate(predictionAndLabels) ~== 0.5 absTol 0.01)

    // mpr, k = 5
    evaluator.setMetricName("mpr").setK(5)
    assert(evaluator.evaluate(predictionAndLabels) ~== 0.375 absTol 0.01)
  }

  test("Ranking Evaluator: no predictions") {
    val predictionAndLabels =
      Seq(
        (1L, 2L, Float.NaN)
      ).toDF(Seq("query", "label", "prediction"): _*)

    // default = mpr, k = 1
    val evaluator = new RankingEvaluator()
    assert(evaluator.evaluate(predictionAndLabels) == 1.0)
  }
}
