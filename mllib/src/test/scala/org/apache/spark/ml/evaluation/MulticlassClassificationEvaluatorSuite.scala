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
}
