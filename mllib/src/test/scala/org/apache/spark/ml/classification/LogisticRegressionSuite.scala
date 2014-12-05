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

package org.apache.spark.ml.classification

import org.scalatest.FunSuite

import org.apache.spark.mllib.classification.LogisticRegressionSuite.generateLogisticInput
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}


class LogisticRegressionSuite extends FunSuite with MLlibTestSparkContext {

  @transient var sqlContext: SQLContext = _
  @transient var dataset: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sqlContext = new SQLContext(sc)
    dataset = sqlContext.createDataFrame(
      sc.parallelize(generateLogisticInput(1.0, 1.0, nPoints = 100, seed = 42), 2))
  }

  test("logistic regression: default params") {
    val lr = new LogisticRegression
    val model = lr.fit(dataset)
    model.transform(dataset)
      .select("label", "prediction")
      .collect()
    // Check defaults
    assert(model.getThreshold === 0.5)
    assert(model.getFeaturesCol == "features")
    assert(model.getPredictionCol == "prediction")
    assert(model.getScoreCol == "score")
  }

  test("logistic regression with setters") {
    // Set params, train, and check as many as we can.
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(1.0)
      .setThreshold(0.6)
      .setScoreCol("probability")
    val model = lr.fit(dataset)
    model.transform(dataset, model.threshold -> 0.8) // overwrite threshold
      .select("label", "score", "prediction")
      .collect()
  }

  test("logistic regression fit and transform with varargs") {
    val lr = new LogisticRegression
    val model = lr.fit(dataset, lr.maxIter -> 10, lr.regParam -> 1.0)
    model.transform(dataset, model.threshold -> 0.8, model.scoreCol -> "probability")
      .select("label", "probability", "prediction")
    assert(model.fittingParamMap.get(lr.maxIter) === Some(10))
    assert(model.fittingParamMap.get(lr.regParam) === Some(1.0))
    assert(model.fittingParamMap.get(lr.threshold) === Some(0.6))
    assert(model.getThreshold === 0.6)

    // Modify model params, and check that they work.
    model.setThreshold(1.0)
    val predAllZero = model.transform(dataset)
      .select('prediction, 'probability)
      .collect()
      .map { case Row(pred: Double, prob: Double) => pred }
    assert(predAllZero.forall(_ === 0.0))
    // Call transform with params, and check that they work.
    val predNotAllZero =
      model.transform(dataset, model.threshold -> 0.0, model.scoreCol -> "myProb")
        .select('prediction, 'myProb)
        .collect()
        .map { case Row(pred: Double, prob: Double) => pred }
    assert(predNotAllZero.exists(_ !== 0.0))

    // Call fit() with new params, and check as many as we can.
    val model2 = lr.fit(dataset, lr.maxIter -> 5, lr.regParam -> 0.1, lr.threshold -> 0.4,
      lr.scoreCol -> "theProb")
    assert(model2.fittingParamMap.get(lr.maxIter) === Some(5))
    assert(model2.fittingParamMap.get(lr.regParam) === Some(0.1))
    assert(model2.fittingParamMap.get(lr.threshold) === Some(0.4))
    assert(model2.getThreshold === 0.4)
    assert(model2.getScoreCol == "theProb")
  }
}
