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

class BinaryClassificationEvaluatorSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  test("params") {
    ParamsSuite.checkParams(new BinaryClassificationEvaluator)
  }

  test("read/write") {
    val evaluator = new BinaryClassificationEvaluator()
      .setRawPredictionCol("myRawPrediction")
      .setLabelCol("myLabel")
      .setMetricName("areaUnderPR")
    testDefaultReadWrite(evaluator)
  }

  test("should accept both vector and double raw prediction col") {
    val evaluator = new BinaryClassificationEvaluator()
      .setMetricName("areaUnderPR")

    val vectorDF = spark.createDataFrame(Seq(
      (0d, Vectors.dense(12, 2.5)),
      (1d, Vectors.dense(1, 3)),
      (0d, Vectors.dense(10, 2))
    )).toDF("label", "rawPrediction")
    assert(evaluator.evaluate(vectorDF) === 1.0)

    val doubleDF = spark.createDataFrame(Seq(
      (0d, 0d),
      (1d, 1d),
      (0d, 0d)
    )).toDF("label", "rawPrediction")
    assert(evaluator.evaluate(doubleDF) === 1.0)

    val stringDF = spark.createDataFrame(Seq(
      (0d, "0d"),
      (1d, "1d"),
      (0d, "0d")
    )).toDF("label", "rawPrediction")
    val thrown = intercept[IllegalArgumentException] {
      evaluator.evaluate(stringDF)
    }
    assert(thrown.getMessage.replace("\n", "") contains "Column rawPrediction must be of type " +
      "equal to one of the following types: [DoubleType, ")
    assert(thrown.getMessage.replace("\n", "") contains "but was actually of type StringType.")
  }

  test("should support all NumericType labels and not support other types") {
    val evaluator = new BinaryClassificationEvaluator().setRawPredictionCol("prediction")
    MLTestingUtils.checkNumericTypes(evaluator, spark)
  }
}
