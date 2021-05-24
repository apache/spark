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
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.mllib.util.{LinearDataGenerator, MLlibTestSparkContext}
import org.apache.spark.mllib.util.TestingUtils._

class RegressionEvaluatorSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import testImplicits._

  test("params") {
    ParamsSuite.checkParams(new RegressionEvaluator)
  }

  test("Regression Evaluator: default params") {
    /**
     * Here is the instruction describing how to export the test data into CSV format
     * so we can validate the metrics compared with R's mmetric function.
     *
     * import org.apache.spark.mllib.util.LinearDataGenerator
     * val data = sc.parallelize(LinearDataGenerator.generateLinearInput(6.3,
     *   Array(4.7, 7.2), Array(0.9, -1.3), Array(0.7, 1.2), 100, 42, 0.1))
     * data.map(x=> x.label + ", " + x.features(0) + ", " + x.features(1))
     *   .saveAsTextFile("path")
     */
    val dataset = LinearDataGenerator.generateLinearInput(
      6.3, Array(4.7, 7.2), Array(0.9, -1.3), Array(0.7, 1.2), 100, 42, 0.1)
      .map(_.asML).toDF()

    /**
     * Using the following R code to load the data, train the model and evaluate metrics.
     *
     * > library("glmnet")
     * > library("rminer")
     * > data <- read.csv("path", header=FALSE, stringsAsFactors=FALSE)
     * > features <- as.matrix(data.frame(as.numeric(data$V2), as.numeric(data$V3)))
     * > label <- as.numeric(data$V1)
     * > model <- glmnet(features, label, family="gaussian", alpha = 0, lambda = 0)
     * > rmse <- mmetric(label, predict(model, features), metric='RMSE')
     * > mae <- mmetric(label, predict(model, features), metric='MAE')
     * > r2 <- mmetric(label, predict(model, features), metric='R2')
     */
    val trainer = new LinearRegression
    val model = trainer.fit(dataset)
    val predictions = model.transform(dataset)

    // default = rmse
    val evaluator = new RegressionEvaluator()
    assert(evaluator.evaluate(predictions) ~== 0.1013829 absTol 0.01)

    // r2 score
    evaluator.setMetricName("r2")
    assert(evaluator.evaluate(predictions) ~== 0.9998387 absTol 0.01)

    // mae
    evaluator.setMetricName("mae")
    assert(evaluator.evaluate(predictions) ~== 0.08399089 absTol 0.01)

    // var
    evaluator.setMetricName("var")
    assert(evaluator.evaluate(predictions) ~== 63.6944519 absTol 0.01)
  }

  test("read/write") {
    val evaluator = new RegressionEvaluator()
      .setPredictionCol("myPrediction")
      .setLabelCol("myLabel")
      .setMetricName("r2")
    testDefaultReadWrite(evaluator)
  }

  test("should support all NumericType labels and not support other types") {
    MLTestingUtils.checkNumericTypes(new RegressionEvaluator, spark)
  }

  test("getMetrics") {
    val dataset = LinearDataGenerator.generateLinearInput(
      6.3, Array(4.7, 7.2), Array(0.9, -1.3), Array(0.7, 1.2), 100, 42, 0.1)
      .map(_.asML).toDF()

    val trainer = new LinearRegression
    val model = trainer.fit(dataset)
    val predictions = model.transform(dataset)

    val evaluator = new RegressionEvaluator()

    val metrics = evaluator.getMetrics(predictions)
    val rmse = metrics.rootMeanSquaredError
    val r2 = metrics.r2
    val mae = metrics.meanAbsoluteError
    val variance = metrics.explainedVariance

    // default = rmse
    assert(evaluator.evaluate(predictions) == rmse)

    // r2 score
    evaluator.setMetricName("r2")
    assert(evaluator.evaluate(predictions) == r2)

    // mae
    evaluator.setMetricName("mae")
    assert(evaluator.evaluate(predictions) == mae)

    // var
    evaluator.setMetricName("var")
    assert(evaluator.evaluate(predictions) == variance)
  }
}
