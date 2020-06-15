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

class MultilabelClassificationEvaluatorSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import testImplicits._

  test("params") {
    ParamsSuite.checkParams(new MultilabelClassificationEvaluator)
  }

  test("evaluation metrics") {
    val scoreAndLabels = Seq((Array(0.0, 1.0), Array(0.0, 2.0)),
      (Array(0.0, 2.0), Array(0.0, 1.0)),
      (Array.empty[Double], Array(0.0)),
      (Array(2.0), Array(2.0)),
      (Array(2.0, 0.0), Array(2.0, 0.0)),
      (Array(0.0, 1.0, 2.0), Array(0.0, 1.0)),
      (Array(1.0), Array(1.0, 2.0))).toDF("prediction", "label")

    val evaluator = new MultilabelClassificationEvaluator()
      .setMetricName("subsetAccuracy")
    assert(evaluator.evaluate(scoreAndLabels) ~== 2.0 / 7 absTol 1e-5)

    evaluator.setMetricName("recallByLabel")
      .setMetricLabel(0.0)
    assert(evaluator.evaluate(scoreAndLabels) ~== 0.8 absTol 1e-5)
  }

  test("read/write") {
    val evaluator = new MultilabelClassificationEvaluator()
      .setPredictionCol("myPrediction")
      .setLabelCol("myLabel")
      .setMetricLabel(1.0)
      .setMetricName("precisionByLabel")
    testDefaultReadWrite(evaluator)
  }

  test("getMetrics") {
    val scoreAndLabels = Seq((Array(0.0, 1.0), Array(0.0, 2.0)),
      (Array(0.0, 2.0), Array(0.0, 1.0)),
      (Array.empty[Double], Array(0.0)),
      (Array(2.0), Array(2.0)),
      (Array(2.0, 0.0), Array(2.0, 0.0)),
      (Array(0.0, 1.0, 2.0), Array(0.0, 1.0)),
      (Array(1.0), Array(1.0, 2.0))).toDF("prediction", "label")

    val evaluator = new MultilabelClassificationEvaluator()

    val metrics = evaluator.getMetrics(scoreAndLabels)
    val f1 = metrics.f1Measure
    val accuracy = metrics.accuracy
    val precision = metrics.precision
    val recall = metrics.recall
    val hammingLoss = metrics.hammingLoss
    val precisionByLabel = metrics.precision(evaluator.getMetricLabel)

    // default = f1
    assert(evaluator.evaluate(scoreAndLabels) == f1)

    // accuracy
    evaluator.setMetricName("accuracy")
    assert(evaluator.evaluate(scoreAndLabels) == accuracy)

    // precision
    evaluator.setMetricName("precision")
    assert(evaluator.evaluate(scoreAndLabels) == precision)

    // recall
    evaluator.setMetricName("recall")
    assert(evaluator.evaluate(scoreAndLabels) == recall)

    // hammingLoss
    evaluator.setMetricName("hammingLoss")
    assert(evaluator.evaluate(scoreAndLabels) == hammingLoss)

    // precisionByLabel
    evaluator.setMetricName("precisionByLabel")
    assert(evaluator.evaluate(scoreAndLabels) == precisionByLabel)

    // truePositiveRateByLabel
    evaluator.setMetricName("recallByLabel").setMetricLabel(1.0)
    assert(evaluator.evaluate(scoreAndLabels) ==
      metrics.recall(evaluator.getMetricLabel))
  }
}
