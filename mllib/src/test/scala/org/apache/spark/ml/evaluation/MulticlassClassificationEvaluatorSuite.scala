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

import scala.util.Try

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.mllib.util.MLlibTestSparkContext


class MulticlassClassificationEvaluatorSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  test("params") {
    ParamsSuite.checkParams(new MulticlassClassificationEvaluator)
  }

  test("read/write") {
    val evaluator = new MulticlassClassificationEvaluator()
      .setPredictionCol("myPrediction")
      .setLabelCol("myLabel")
      .setMetricName("accuracy")
    testDefaultReadWrite(evaluator)
  }

  test("metricName without labelValue") {
    val evaluator = new MulticlassClassificationEvaluator()
      .setPredictionCol("myPrediction")
      .setLabelCol("myLabel")

    val weightedOptions: Array[String] = Array("f1", "weightedPrecision",
      "weightedRecall", "accuracy")

    weightedOptions.foreach{
      metricName => assert(Try(evaluator.setMetricName(metricName)).isSuccess
        , s"metric ${metricName} can be defined when label value is not set")
    }

    val onlyLabelOptions: Array[String] = Array("precision", "recall", "tpr", "fpr")

    onlyLabelOptions.foreach{
      metricName => assert(Try(evaluator.setMetricName(metricName)).isFailure
        , s"metric ${metricName} cannot be defined when label value is not set")
    }
  }

  test("metricName with labelValue") {
    val evaluator = new MulticlassClassificationEvaluator()
      .setPredictionCol("myPrediction")
      .setLabelCol("myLabel")

    // f1 is supported for both
    val onlyWeightedOptions: Array[String] = Array("weightedPrecision",
      "weightedRecall", "accuracy")

    onlyWeightedOptions.foreach{
      metricName => assert(Try(evaluator.setLabelValue(1.0).setMetricName(metricName)).isFailure
        , s"metric ${metricName} can be defined when label value is not set")
    }

    val labelOptions: Array[String] = Array("f1", "precision", "recall", "tpr", "fpr")

    labelOptions.foreach{
      metricName => assert(Try(evaluator.setLabelValue(1.0).setMetricName(metricName)).isSuccess
        , s"metric ${metricName} cannot be defined when label value is not set")
    }
  }

  test("should support all NumericType labels and not support other types") {
    MLTestingUtils.checkNumericTypes(new MulticlassClassificationEvaluator, spark)
  }

}
