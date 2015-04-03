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
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}


class LogisticRegressionSuite extends FunSuite with MLlibTestSparkContext {

  @transient var sqlContext: SQLContext = _
  @transient var dataset: DataFrame = _
  private val eps: Double = 1e-5

  override def beforeAll(): Unit = {
    super.beforeAll()
    sqlContext = new SQLContext(sc)
    dataset = sqlContext.createDataFrame(
      sc.parallelize(generateLogisticInput(1.0, 1.0, nPoints = 100, seed = 42), 2))
  }

  test("logistic regression: default params") {
    val lr = new LogisticRegression
    assert(lr.getLabelCol == "label")
    assert(lr.getFeaturesCol == "features")
    assert(lr.getPredictionCol == "prediction")
    assert(lr.getRawPredictionCol == "rawPrediction")
    assert(lr.getProbabilityCol == "probability")
    assert(lr.getFitIntercept == true)
    val model = lr.fit(dataset)
    model.transform(dataset)
      .select("label", "probability", "prediction", "rawPrediction")
      .collect()
    assert(model.getThreshold === 0.5)
    assert(model.getFeaturesCol == "features")
    assert(model.getPredictionCol == "prediction")
    assert(model.getRawPredictionCol == "rawPrediction")
    assert(model.getProbabilityCol == "probability")
    assert(model.intercept !== 0.0)
  }

  test("logistic regression doesn't fit intercept when fitIntercept is off") {
    val lr = new LogisticRegression
    lr.setFitIntercept(false)
    val model = lr.fit(dataset)
    assert(model.intercept === 0.0)
  }

  test("logistic regression with setters") {
    // Set params, train, and check as many params as we can.
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(1.0)
      .setThreshold(0.6)
      .setProbabilityCol("myProbability")
    val model = lr.fit(dataset)
    assert(model.fittingParamMap.get(lr.maxIter) === Some(10))
    assert(model.fittingParamMap.get(lr.regParam) === Some(1.0))
    assert(model.fittingParamMap.get(lr.threshold) === Some(0.6))
    assert(model.getThreshold === 0.6)

    // Modify model params, and check that the params worked.
    model.setThreshold(1.0)
    val predAllZero = model.transform(dataset)
      .select("prediction", "myProbability")
      .collect()
      .map { case Row(pred: Double, prob: Vector) => pred }
    assert(predAllZero.forall(_ === 0),
      s"With threshold=1.0, expected predictions to be all 0, but only" +
      s" ${predAllZero.count(_ === 0)} of ${dataset.count()} were 0.")
    // Call transform with params, and check that the params worked.
    val predNotAllZero =
      model.transform(dataset, model.threshold -> 0.0, model.probabilityCol -> "myProb")
        .select("prediction", "myProb")
        .collect()
        .map { case Row(pred: Double, prob: Vector) => pred }
    assert(predNotAllZero.exists(_ !== 0.0))

    // Call fit() with new params, and check as many params as we can.
    val model2 = lr.fit(dataset, lr.maxIter -> 5, lr.regParam -> 0.1, lr.threshold -> 0.4,
      lr.probabilityCol -> "theProb")
    assert(model2.fittingParamMap.get(lr.maxIter).get === 5)
    assert(model2.fittingParamMap.get(lr.regParam).get === 0.1)
    assert(model2.fittingParamMap.get(lr.threshold).get === 0.4)
    assert(model2.getThreshold === 0.4)
    assert(model2.getProbabilityCol == "theProb")
  }

  test("logistic regression: Predictor, Classifier methods") {
    val sqlContext = this.sqlContext
    val lr = new LogisticRegression

    val model = lr.fit(dataset)
    assert(model.numClasses === 2)

    val threshold = model.getThreshold
    val results = model.transform(dataset)

    // Compare rawPrediction with probability
    results.select("rawPrediction", "probability").collect().map {
      case Row(raw: Vector, prob: Vector) =>
        assert(raw.size === 2)
        assert(prob.size === 2)
        val probFromRaw1 = 1.0 / (1.0 + math.exp(-raw(1)))
        assert(prob(1) ~== probFromRaw1 relTol eps)
        assert(prob(0) ~== 1.0 - probFromRaw1 relTol eps)
    }

    // Compare prediction with probability
    results.select("prediction", "probability").collect().map {
      case Row(pred: Double, prob: Vector) =>
        val predFromProb = prob.toArray.zipWithIndex.maxBy(_._1)._2
        assert(pred == predFromProb)
    }
  }
}
