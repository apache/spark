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
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._

class MulticlassClassificationEvaluatorSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  test("params") {
    ParamsSuite.checkParams(new MulticlassClassificationEvaluator)
  }

  test("read/write") {
    val evaluator = new MulticlassClassificationEvaluator()
      .setPredictionCol("myPrediction")
      .setLabelCol("myLabel")
      .setMetricName("recall")
    testDefaultReadWrite(evaluator)
  }

  test("should support all NumericType labels and not support other types") {
    MLTestingUtils.checkNumericTypes(new MulticlassClassificationEvaluator, spark)
  }

  test("Multiclass Classification Evaluator") {
    val labelAndScores = spark.createDataFrame(Seq(
      (0.0, 0.0),
      (1.0, 1.0),
      (2.0, 2.0),
      (1.0, 2.0),
      (0.0, 2.0))).toDF("label", "prediction")

    /**
     * Using the following Python code to evaluate metrics.
     *
     * > from sklearn.metrics import *
     * > y_true = [0, 1, 2, 1, 0]
     * > y_pred = [0, 1, 2, 2, 2]
     * > f1 = f1_score(y_true, y_pred, average='weighted')
     * > precision = precision_score(y_true, y_pred, average='micro')
     * > recall = recall_score(y_true, y_pred, average='micro')
     * > weighted_precision = precision_score(y_true, y_pred, average='weighted')
     * > weighted_recall = recall_score(y_true, y_pred, average='weighted')
     * > accuracy = accuracy_score(y_true, y_pred)
     */

    // default = weighted f1
    val evaluator = new MulticlassClassificationEvaluator()
    assert(evaluator.evaluate(labelAndScores) ~== 0.633333 absTol 0.01)

    // micro precision
    evaluator.setMetricName("precision")
    assert(evaluator.evaluate(labelAndScores) ~== 0.6 absTol 0.01)

    // micro recall
    evaluator.setMetricName("recall")
    assert(evaluator.evaluate(labelAndScores) ~== 0.6 absTol 0.01)

    // weighted precision
    evaluator.setMetricName("weightedPrecision")
    assert(evaluator.evaluate(labelAndScores) ~== 0.866667 absTol 0.01)

    // weighted recall
    evaluator.setMetricName("weightedRecall")
    assert(evaluator.evaluate(labelAndScores) ~== 0.6 absTol 0.01)

    // accuracy
    evaluator.setMetricName("accuracy")
    assert(evaluator.evaluate(labelAndScores) ~== 0.6 absTol 0.01)
  }
}
