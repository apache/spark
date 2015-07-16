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
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
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
      piData: Vector,
      thetaData: Matrix,
      model: NaiveBayesModel): Unit = {
    assert(Vectors.dense(model.pi.toArray.map(math.exp)) ~==
      Vectors.dense(piData.toArray.map(math.exp)) absTol 0.05, "pi mismatch")
    assert(model.theta.map(math.exp) ~== thetaData.map(math.exp) absTol 0.05, "theta mismatch")
  }

  test("params") {
    ParamsSuite.checkParams(new NaiveBayes)
    val model = new NaiveBayesModel("nb", pi = Vectors.dense(Array(0.2, 0.8)),
      theta = new DenseMatrix(2, 3, Array(0.1, 0.2, 0.3, 0.4, 0.6, 0.4)))
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
    val piArray = Array(0.5, 0.1, 0.4).map(math.log)
    val thetaArray = Array(
      Array(0.70, 0.10, 0.10, 0.10), // label 0
      Array(0.10, 0.70, 0.10, 0.10), // label 1
      Array(0.10, 0.10, 0.70, 0.10)  // label 2
    ).map(_.map(math.log))
    val pi = Vectors.dense(piArray)
    val theta = new DenseMatrix(3, 4, thetaArray.flatten, true)

    val testDataset = sqlContext.createDataFrame(generateNaiveBayesInput(
      piArray, thetaArray, nPoints, 42, "multinomial"))
    val nb = new NaiveBayes().setLambda(1.0).setModelType("multinomial")
    val model = nb.fit(testDataset)

    validateModelFit(pi, theta, model)
    assert(model.hasParent)

    val validationDataset = sqlContext.createDataFrame(generateNaiveBayesInput(
      piArray, thetaArray, nPoints, 17, "multinomial"))
    val predictionAndLabels = model.transform(validationDataset).select("prediction", "label")

    validatePrediction(predictionAndLabels)
  }

  test("Naive Bayes Bernoulli") {
    val nPoints = 10000
    val piArray = Array(0.5, 0.3, 0.2).map(math.log)
    val thetaArray = Array(
      Array(0.50, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.40), // label 0
      Array(0.02, 0.70, 0.10, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02), // label 1
      Array(0.02, 0.02, 0.60, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.30)  // label 2
    ).map(_.map(math.log))
    val pi = Vectors.dense(piArray)
    val theta = new DenseMatrix(3, 12, thetaArray.flatten, true)

    val testDataset = sqlContext.createDataFrame(generateNaiveBayesInput(
      piArray, thetaArray, nPoints, 45, "bernoulli"))
    val nb = new NaiveBayes().setLambda(1.0).setModelType("bernoulli")
    val model = nb.fit(testDataset)

    validateModelFit(pi, theta, model)
    assert(model.hasParent)

    val validationDataset = sqlContext.createDataFrame(generateNaiveBayesInput(
      piArray, thetaArray, nPoints, 20, "bernoulli"))
    val predictionAndLabels = model.transform(validationDataset).select("prediction", "label")

    validatePrediction(predictionAndLabels)
  }
}
