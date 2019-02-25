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

  import testImplicits._

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

    val vectorDF = Seq(
      (0.0, Vectors.dense(12, 2.5)),
      (1.0, Vectors.dense(1, 3)),
      (0.0, Vectors.dense(10, 2))
    ).toDF("label", "rawPrediction")
    assert(evaluator.evaluate(vectorDF) === 1.0)

    val doubleDF = Seq(
      (0.0, 0.0),
      (1.0, 1.0),
      (0.0, 0.0)
    ).toDF("label", "rawPrediction")
    assert(evaluator.evaluate(doubleDF) === 1.0)

    val stringDF = Seq(
      (0.0, "0.0"),
      (1.0, "1.0"),
      (0.0, "0.0")
    ).toDF("label", "rawPrediction")
    val thrown = intercept[IllegalArgumentException] {
      evaluator.evaluate(stringDF)
    }
    assert(thrown.getMessage.replace("\n", "") contains "Column rawPrediction must be of type " +
      "equal to one of the following types: [double, ")
    assert(thrown.getMessage.replace("\n", "") contains "but was actually of type string.")
  }

  test("should accept weight column") {
    val weightCol = "weight"
    // get metric with weight column
    val evaluator = new BinaryClassificationEvaluator()
      .setMetricName("areaUnderROC").setWeightCol(weightCol)
    val vectorDF = Seq(
      (0.0, Vectors.dense(2.5, 12), 1.0),
      (1.0, Vectors.dense(1, 3), 1.0),
      (0.0, Vectors.dense(10, 2), 1.0)
    ).toDF("label", "rawPrediction", weightCol)
    val result = evaluator.evaluate(vectorDF)
    // without weight column
    val evaluator2 = new BinaryClassificationEvaluator()
      .setMetricName("areaUnderROC")
    val result2 = evaluator2.evaluate(vectorDF)
    assert(result === result2)
    // use different weights, validate metrics change
    val vectorDF2 = Seq(
      (0.0, Vectors.dense(2.5, 12), 2.5),
      (1.0, Vectors.dense(1, 3), 0.1),
      (0.0, Vectors.dense(10, 2), 2.0)
    ).toDF("label", "rawPrediction", weightCol)
    val result3 = evaluator.evaluate(vectorDF2)
    // Since wrong result weighted more heavily, expect the score to be lower
    assert(result3 < result)
  }

  test("should support all NumericType labels and not support other types") {
    val evaluator = new BinaryClassificationEvaluator().setRawPredictionCol("prediction")
    MLTestingUtils.checkNumericTypes(evaluator, spark)
  }
}
