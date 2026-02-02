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

  test("read/write") {
    val evaluator = new RankingEvaluator()
      .setPredictionCol("myPrediction")
      .setLabelCol("myLabel")
      .setMetricName("precisionAtK")
      .setK(10)
    testDefaultReadWrite(evaluator)
  }

  test("evaluation metrics") {
    val scoreAndLabels = Seq(
        (Array(1.0, 6.0, 2.0, 7.0, 8.0, 3.0, 9.0, 10.0, 4.0, 5.0),
          Array(1.0, 2.0, 3.0, 4.0, 5.0)),
        (Array(4.0, 1.0, 5.0, 6.0, 2.0, 7.0, 3.0, 8.0, 9.0, 10.0),
          Array(1.0, 2.0, 3.0)),
        (Array(1.0, 2.0, 3.0, 4.0, 5.0), Array.empty[Double])
      ).toDF("prediction", "label")

    val evaluator = new RankingEvaluator()
      .setMetricName("meanAveragePrecision")
    assert(evaluator.evaluate(scoreAndLabels) ~== 0.355026 absTol 1e-5)

    evaluator.setMetricName("precisionAtK")
      .setK(2)
    assert(evaluator.evaluate(scoreAndLabels) ~== 1.0 / 3 absTol 1e-5)
  }

  test("getMetrics") {
    val scoreAndLabels = Seq(
      (Array(1.0, 6.0, 2.0, 7.0, 8.0, 3.0, 9.0, 10.0, 4.0, 5.0),
        Array(1.0, 2.0, 3.0, 4.0, 5.0)),
      (Array(4.0, 1.0, 5.0, 6.0, 2.0, 7.0, 3.0, 8.0, 9.0, 10.0),
        Array(1.0, 2.0, 3.0)),
      (Array(1.0, 2.0, 3.0, 4.0, 5.0), Array.empty[Double])
    ).toDF("prediction", "label")

    val evaluator = new RankingEvaluator().setK(5)

    val metrics = evaluator.getMetrics(scoreAndLabels)
    val meanAveragePrecision = metrics.meanAveragePrecision
    val meanAveragePrecisionAtK = metrics.meanAveragePrecisionAt(evaluator.getK)
    val precisionAtK = metrics.precisionAt(evaluator.getK)
    val ndcgAtK = metrics.ndcgAt(evaluator.getK)
    val recallAtK = metrics.recallAt(evaluator.getK)

    // default = meanAveragePrecision
    assert(evaluator.evaluate(scoreAndLabels) == meanAveragePrecision)

    // meanAveragePrecisionAtK
    evaluator.setMetricName("meanAveragePrecisionAtK")
    assert(evaluator.evaluate(scoreAndLabels) == meanAveragePrecisionAtK)

    // precisionAtK
    evaluator.setMetricName("precisionAtK")
    assert(evaluator.evaluate(scoreAndLabels) == precisionAtK)

    // ndcgAtK
    evaluator.setMetricName("ndcgAtK")
    assert(evaluator.evaluate(scoreAndLabels) == ndcgAtK)

    // recallAtK
    evaluator.setMetricName("recallAtK")
    assert(evaluator.evaluate(scoreAndLabels) == recallAtK)
  }
}
