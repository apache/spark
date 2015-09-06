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

package org.apache.spark.ml.ensemble

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.classification.LogisticRegressionSuite.generateLogisticInput
import org.apache.spark.mllib.util.{LinearDataGenerator, MLlibTestSparkContext}
import org.apache.spark.mllib.util.TestingUtils._

class BaggingSuite extends SparkFunSuite with MLlibTestSparkContext {
  test("params") {
    val dataset = sqlContext.createDataFrame(
      sc.parallelize(LinearDataGenerator.generateLinearInput(
        6.3, Array(4.7, 7.2), Array(0.9, -1.3), Array(0.7, 1.2), 100, 42, 0.1), 2))

    val lr = new LinearRegression
    val bagging = new Bagging()
      .setPredictor(lr)
      .setPredictionCol("test")
      .setIsClassifier(false)
      .setNumModels(3)
      .setSeed(42L)
    assert(bagging.getPredictor.uid === lr.uid)
    assert(bagging.getIsClassifier === false)
    assert(bagging.getNumModels === 3)
    assert(bagging.getSeed === 42)

    val baggedModel = bagging.fit(dataset)
    assert(baggedModel.transform(dataset).columns.contains("test"))
  }

  test("bagging with linear regression") {
    val dataset = sqlContext.createDataFrame(
      sc.parallelize(LinearDataGenerator.generateLinearInput(
        6.3, Array(4.7, 7.2), Array(0.9, -1.3), Array(0.7, 1.2), 100, 42, 0.1), 2))

    val lr = new LinearRegression
    val bagging = new Bagging()
      .setPredictor(lr)
      .setIsClassifier(false)
      .setNumModels(3)
      .setSeed(42L)
    val baggedModel = bagging.fit(dataset)

    val eval = (new RegressionEvaluator).setMetricName("mse")
    val baggedMetric = eval.evaluate(baggedModel.transform(dataset))
    val baselineMetric = eval.evaluate(lr.fit(dataset).transform(dataset))
    assert(baggedMetric ~== baselineMetric relTol 0.05)
  }

  test("bagging with logistic regression") {
    val dataset = sqlContext.createDataFrame(
        sc.parallelize(generateLogisticInput(1.0, 1.0, 100, 42), 2))

    val lr = new LogisticRegression
    val bagging = new Bagging()
      .setPredictor(lr)
      .setIsClassifier(true)
      .setNumModels(3)
      .setSeed(42L)
    val baggedModel = bagging.fit(dataset)

    val numCorrectBaseline = lr.fit(dataset)
      .transform(dataset)
      .where(lr.getLabelCol + " = " + lr.getPredictionCol)
      .count()
    val numCorrectBagged = baggedModel
      .transform(dataset)
      .where(lr.getLabelCol + " = " + lr.getPredictionCol)
      .count()
    assert(numCorrectBaseline.toDouble ~== numCorrectBagged.toDouble relTol 0.05)
  }
}
