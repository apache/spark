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

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.classification.NaiveBayesSuite._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

class NaiveBayesSuite extends SparkFunSuite with MLlibTestSparkContext {

  def validatePrediction(predictionAndLabels: DataFrame): Unit = {
    val numOfErrorPredictions = predictionAndLabels.collect().count {
      case Row(prediction: Double, label: Double) =>
        prediction != label
    }
    // At least 80% of the predictions should be on.
    assert(numOfErrorPredictions < predictionAndLabels.count() / 5)
  }

  def validateModelFit(
      piData: Array[Double],
      thetaData: Array[Array[Double]],
      model: NaiveBayesModel): Unit = {
    def closeFit(d1: Double, d2: Double, precision: Double): Boolean = {
      (d1 - d2).abs <= precision
    }
    val modelIndex = (0 until piData.length).zip(model.labels.map(_.toInt))
    for (i <- modelIndex) {
      assert(closeFit(math.exp(piData(i._2)), math.exp(model.pi(i._1)), 0.05))
    }
    for (i <- modelIndex) {
      for (j <- 0 until thetaData(i._2).length) {
        assert(closeFit(math.exp(thetaData(i._2)(j)), math.exp(model.theta(i._1)(j)), 0.05))
      }
    }
  }

  test("params") {
    ParamsSuite.checkParams(new NaiveBayes)
    val model = new NaiveBayesModel("nb", labels = Array(0.0, 1.0),
      pi = Array(0.2, 0.8), theta = Array(Array(0.1, 0.3, 0.6), Array(0.2, 0.4, 0.4)),
      "multinomial")
    ParamsSuite.checkParams(model)
  }

  test("naive bayes: default params") {
    val nb = new NaiveBayes
    assert(nb.getLabelCol === "label")
    assert(nb.getFeaturesCol === "features")
    assert(nb.getPredictionCol === "prediction")
    assert(nb.getLambda === 1.0)
    assert(nb.getModelType === "multinomial")
  }

  test("Naive Bayes Multinomial") {
    val nPoints = 1000
    val pi = Array(0.5, 0.1, 0.4).map(math.log)
    val theta = Array(
      Array(0.70, 0.10, 0.10, 0.10), // label 0
      Array(0.10, 0.70, 0.10, 0.10), // label 1
      Array(0.10, 0.10, 0.70, 0.10)  // label 2
    ).map(_.map(math.log))

    val testDataset = sqlContext.createDataFrame(generateNaiveBayesInput(
      pi, theta, nPoints, 42, "multinomial"))
    val nb = new NaiveBayes().setLambda(1.0).setModelType("multinomial")
    val model = nb.fit(testDataset)

    validateModelFit(pi, theta, model)
    assert(model.hasParent)

    val validationDataset = sqlContext.createDataFrame(generateNaiveBayesInput(
      pi, theta, nPoints, 17, "multinomial"))

    val predictionAndLabels = model.transform(validationDataset).select("prediction", "label")

    validatePrediction(predictionAndLabels)
  }

  test("Naive Bayes Bernoulli") {
    val nPoints = 10000
    val pi = Array(0.5, 0.3, 0.2).map(math.log)
    val theta = Array(
      Array(0.50, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.40), // label 0
      Array(0.02, 0.70, 0.10, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02), // label 1
      Array(0.02, 0.02, 0.60, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.30)  // label 2
    ).map(_.map(math.log))

    val testDataset = sqlContext.createDataFrame(generateNaiveBayesInput(
      pi, theta, nPoints, 45, "bernoulli"))
    val nb = new NaiveBayes().setLambda(1.0).setModelType("bernoulli")
    val model = nb.fit(testDataset)

    validateModelFit(pi, theta, model)
    assert(model.hasParent)

    val validationDataset = sqlContext.createDataFrame(generateNaiveBayesInput(
      pi, theta, nPoints, 20, "bernoulli"))

    val predictionAndLabels = model.transform(validationDataset).select("prediction", "label")

    validatePrediction(predictionAndLabels)
  }
}
