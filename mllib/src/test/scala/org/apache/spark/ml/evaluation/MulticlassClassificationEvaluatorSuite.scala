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
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._

class MulticlassClassificationEvaluatorSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import testImplicits._

  test("params") {
    ParamsSuite.checkParams(new MulticlassClassificationEvaluator)
  }

  test("read/write") {
    val evaluator = new MulticlassClassificationEvaluator()
      .setPredictionCol("myPrediction")
      .setLabelCol("myLabel")
      .setMetricName("accuracy")
      .setMetricLabel(1.0)
      .setBeta(2.0)
    testDefaultReadWrite(evaluator)
  }

  test("should support all NumericType labels and not support other types") {
    MLTestingUtils.checkNumericTypes(new MulticlassClassificationEvaluator, spark)
  }

  test("evaluation metrics") {
    val predictionAndLabels = Seq((0.0, 0.0), (0.0, 1.0),
      (0.0, 0.0), (1.0, 0.0), (1.0, 1.0),
      (1.0, 1.0), (1.0, 1.0), (2.0, 2.0), (2.0, 0.0)).toDF("prediction", "label")

    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("precisionByLabel")
      .setMetricLabel(0.0)
    assert(evaluator.evaluate(predictionAndLabels) ~== 2.0 / 3 absTol 1e-5)

    evaluator.setMetricName("truePositiveRateByLabel")
      .setMetricLabel(1.0)
    assert(evaluator.evaluate(predictionAndLabels) ~== 3.0 / 4 absTol 1e-5)
  }

  test("MulticlassClassificationEvaluator support logloss") {
    val labels = Seq(1.0, 2.0, 0.0, 1.0)
    val probabilities = Seq(
      Vectors.dense(0.1, 0.8, 0.1),
      Vectors.dense(0.9, 0.05, 0.05),
      Vectors.dense(0.8, 0.2, 0.0),
      Vectors.dense(0.3, 0.65, 0.05))

    val df = sc.parallelize(labels.zip(probabilities)).map {
      case (label, probability) =>
        val prediction = probability.argmax.toDouble
        (prediction, label, probability)
    }.toDF("prediction", "label", "probability")

    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("logLoss")
    assert(evaluator.evaluate(df) ~== 0.9682005730687164 absTol 1e-5)
  }

  test("getMetrics") {
    val predictionAndLabels = Seq((0.0, 0.0), (0.0, 1.0),
      (0.0, 0.0), (1.0, 0.0), (1.0, 1.0),
      (1.0, 1.0), (1.0, 1.0), (2.0, 2.0), (2.0, 0.0)).toDF("prediction", "label")

    val evaluator = new MulticlassClassificationEvaluator()

    val metrics = evaluator.getMetrics(predictionAndLabels)
    val f1 = metrics.weightedFMeasure
    val accuracy = metrics.accuracy
    val precisionByLabel = metrics.precision(evaluator.getMetricLabel)

    // default = f1
    assert(evaluator.evaluate(predictionAndLabels) == f1)

    // accuracy
    evaluator.setMetricName("accuracy")
    assert(evaluator.evaluate(predictionAndLabels) == accuracy)

    // precisionByLabel
    evaluator.setMetricName("precisionByLabel")
    assert(evaluator.evaluate(predictionAndLabels) == precisionByLabel)

    // truePositiveRateByLabel
    evaluator.setMetricName("truePositiveRateByLabel").setMetricLabel(1.0)
    assert(evaluator.evaluate(predictionAndLabels) ==
      metrics.truePositiveRate(evaluator.getMetricLabel))
  }
}
