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

import org.apache.spark.mllib.classification.LogisticRegressionSuite
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}


class LogisticRegressionSuite extends FunSuite with MLlibTestSparkContext {

  @transient var sqlContext: SQLContext = _
  @transient var dataset: DataFrame = _
  @transient var binaryDataset: DataFrame = _
  private val eps: Double = 1e-5

  override def beforeAll(): Unit = {
    super.beforeAll()
    sqlContext = new SQLContext(sc)

    dataset = sqlContext.createDataFrame(sc.parallelize(LogisticRegressionSuite
      .generateLogisticInput(1.0, 1.0, nPoints = 100, seed = 42), 4))

    binaryDataset = {
      val nPoints = 10000
      val weights = Array(-0.57997, 0.912083, -0.371077, -0.819866, 2.688191)
      val xMean = Array(5.843, 3.057, 3.758, 1.199)
      val xVariance = Array(0.6856, 0.1899, 3.116, 0.581)

      val testData = LogisticRegressionSuite.generateMultinomialLogisticInput(
        weights, xMean, xVariance, true, nPoints, 42)

      sqlContext.createDataFrame(sc.parallelize(LogisticRegressionSuite
        .generateMultinomialLogisticInput(weights, xMean, xVariance, true, nPoints, 42), 4))
    }
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
    val parent = model.parent
    assert(parent.getMaxIter === 10)
    assert(parent.getRegParam === 1.0)
    assert(parent.getThreshold === 0.6)
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
    val parent2 = model2.parent
    assert(parent2.getMaxIter === 5)
    assert(parent2.getRegParam === 0.1)
    assert(parent2.getThreshold === 0.4)
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
    results.select("rawPrediction", "probability").collect().foreach {
      case Row(raw: Vector, prob: Vector) =>
        assert(raw.size === 2)
        assert(prob.size === 2)
        val probFromRaw1 = 1.0 / (1.0 + math.exp(-raw(1)))
        assert(prob(1) ~== probFromRaw1 relTol eps)
        assert(prob(0) ~== 1.0 - probFromRaw1 relTol eps)
    }

    // Compare prediction with probability
    results.select("prediction", "probability").collect().foreach {
      case Row(pred: Double, prob: Vector) =>
        val predFromProb = prob.toArray.zipWithIndex.maxBy(_._1)._2
        assert(pred == predFromProb)
    }
  }

  test("MultilabelSummarizer") {
    val summarizer1 = (new MultiClassSummarizer)
      .add(0.0).add(3.0).add(4.0).add(3.0).add(6.0)
    assert(summarizer1.histogram.zip(Array[Long](1, 0, 0, 2, 1, 0, 1)).forall(x => x._1 === x._2))
    assert(summarizer1.countInvalid === 0)
    assert(summarizer1.numClasses === 7)

    val summarizer2 = (new MultiClassSummarizer)
      .add(1.0).add(5.0).add(3.0).add(0.0).add(4.0).add(1.0)
    assert(summarizer2.histogram.zip(Array[Long](1, 2, 0, 1, 1, 1)).forall(x => x._1 === x._2))
    assert(summarizer2.countInvalid === 0)
    assert(summarizer2.numClasses === 6)

    val summarizer3 = (new MultiClassSummarizer)
      .add(0.0).add(1.3).add(5.2).add(2.5).add(2.0).add(4.0).add(4.0).add(4.0).add(1.0)
    assert(summarizer3.histogram.zip(Array[Long](1, 1, 1, 0, 3)).forall(x => x._1 === x._2))
    assert(summarizer3.countInvalid === 3)
    assert(summarizer3.numClasses === 5)

    val summarizer4 = (new MultiClassSummarizer)
      .add(3.1).add(4.3).add(2.0).add(1.0).add(3.0)
    assert(summarizer4.histogram.zip(Array[Long](0, 1, 1, 1)).forall(x => x._1 === x._2))
    assert(summarizer4.countInvalid === 2)
    assert(summarizer4.numClasses === 4)

    // small map merges large one
    val summarizerA = summarizer1.merge(summarizer2)
    assert(summarizerA.hashCode() === summarizer2.hashCode())
    assert(summarizerA.histogram.zip(Array[Long](2, 2, 0, 3, 2, 1, 1)).forall(x => x._1 === x._2))
    assert(summarizerA.countInvalid === 0)
    assert(summarizerA.numClasses === 7)

    // large map merges small one
    val summarizerB = summarizer3.merge(summarizer4)
    assert(summarizerB.hashCode() === summarizer3.hashCode())
    assert(summarizerB.histogram.zip(Array[Long](1, 2, 2, 1, 3)).forall(x => x._1 === x._2))
    assert(summarizerB.countInvalid === 5)
    assert(summarizerB.numClasses === 5)
  }

  test("binary logistic regression with intercept without regularization") {
    val trainer = (new LogisticRegression).setFitIntercept(true).setMaxIter(100).setThreshold(1E-16)

    val model = trainer.fit(binaryDataset)

    val interceptR = 2.83610
    val weightsR = Array(-0.5895, 0.8932, -0.3924, -0.79966)

    assert(model.intercept ~== interceptR relTol 1E-3)
    assert(model.weights(0) ~== weightsR(0) relTol 1E-3)
    assert(model.weights(1) ~== weightsR(1) relTol 1E-3)
    assert(model.weights(2) ~== weightsR(2) relTol 1E-3)
    assert(model.weights(3) ~== weightsR(3) relTol 1E-3)
  }

  test("binary logistic regression without intercept without regularization") {
  }

  test("binary logistic regression with intercept with L1 regularization") {
  }

  test("binary logistic regression without intercept with L1 regularization") {
  }

  test("binary logistic regression with intercept with L2 regularization") {
  }

  test("binary logistic regression without intercept with L2 regularization") {
  }

  test("binary logistic regression with intercept with ElasticNet regularization") {
  }

  test("binary logistic regression without intercept with ElasticNet regularization") {
  }
}
