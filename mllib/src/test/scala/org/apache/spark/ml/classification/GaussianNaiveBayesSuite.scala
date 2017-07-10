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

import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.classification.GaussianNaiveBayesSuite._
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row}


class GaussianNaiveBayesSuite extends SparkFunSuite with MLlibTestSparkContext
  with DefaultReadWriteTest {

  import testImplicits._

  @transient var dataset: Dataset[_] = _

  private val piArray = Array(0.5, 0.1, 0.4).map(math.log)

  private val thetaArray = Array(
    Array(0.70, 0.10, 0.10, 0.10), // label 0: mean
    Array(0.10, 0.70, 0.10, 0.10), // label 1: mean
    Array(0.10, 0.10, 0.70, 0.10)  // label 2: mean
  )

  private val sigmaArray = Array(
    Array(0.10, 0.10, 0.50, 0.10), // label 0: variance
    Array(0.50, 0.10, 0.10, 0.10), // label 1: variance
    Array(0.10, 0.10, 0.10, 0.50)  // label 2: variance
  )

  private val seed = 42

  override def beforeAll(): Unit = {
    super.beforeAll()
    dataset = generateGaussianNaiveBayesInput(piArray, thetaArray, sigmaArray, 100, seed).toDF()
  }

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
      sigmaData: Matrix,
      model: GaussianNaiveBayesModel): Unit = {
    assert(Vectors.dense(model.pi.toArray.map(math.exp)) ~==
      Vectors.dense(piData.toArray.map(math.exp)) absTol 0.05, "pi mismatch")
    assert(model.theta.map(math.exp) ~== thetaData.map(math.exp) absTol 0.05, "theta mismatch")
    assert(model.sigma.map(math.exp) ~== sigmaData.map(math.exp) absTol 0.05, "sigma mismatch")
  }

  def expectedGaussianProbabilities(model: GaussianNaiveBayesModel, feature: Vector): Vector = {
    val pi = model.pi.toArray.map(math.exp)
    val classProbs = pi.indices.map { i =>
      feature.toArray.zipWithIndex.map { case (v, j) =>
        val mean = model.theta(i, j)
        val variance = model.sigma(i, j)
        math.exp(- (v - mean) * (v - mean) / variance / 2) / math.sqrt(variance * math.Pi * 2)
      }.product * pi(i)
    }.toArray
    val classProbsSum = classProbs.sum
    Vectors.dense(classProbs.map(_ / classProbsSum))
  }

  def validateProbabilities(
      featureAndProbabilities: DataFrame,
      model: GaussianNaiveBayesModel): Unit = {
    featureAndProbabilities.collect().foreach {
      case Row(features: Vector, probability: Vector) =>
        assert(probability.toArray.sum ~== 1.0 relTol 1.0e-10)
        val expected = expectedGaussianProbabilities(model, features)
        assert(probability ~== expected relTol 1.0e-10)
    }
  }

  test("params") {
    ParamsSuite.checkParams(new GaussianNaiveBayes)
    val model = new GaussianNaiveBayesModel("gnb", pi = Vectors.dense(Array(0.2, 0.8)),
      theta = new DenseMatrix(2, 3, Array(0.1, 0.2, 0.3, 0.4, 0.6, 0.4)),
      sigma = new DenseMatrix(2, 3, Array(0.1, 0.2, 0.3, 0.4, 0.6, 0.4)))
    ParamsSuite.checkParams(model)
  }

  test("gaussian naive bayes: default params") {
    val nb = new GaussianNaiveBayes
    assert(nb.getLabelCol === "label")
    assert(nb.getFeaturesCol === "features")
    assert(nb.getPredictionCol === "prediction")
  }

  test("Gaussian Naive Bayes") {
    val pi = Vectors.dense(piArray)
    val theta = new DenseMatrix(3, 4, thetaArray.flatten, true)
    val sigma = new DenseMatrix(3, 4, sigmaArray.flatten, true)

    val nPoints = 10000
    val testDataset =
      generateGaussianNaiveBayesInput(piArray, thetaArray, sigmaArray, nPoints, 42).toDF()
    val gnb = new GaussianNaiveBayes()
    val model = gnb.fit(testDataset)

    validateModelFit(pi, theta, sigma, model)
    assert(model.hasParent)

    val validationDataset =
      generateGaussianNaiveBayesInput(piArray, thetaArray, sigmaArray, nPoints, 17).toDF()

    val predictionAndLabels = model.transform(validationDataset).select("prediction", "label")
    validatePrediction(predictionAndLabels)

    val featureAndProbabilities = model.transform(validationDataset)
      .select("features", "probability")
    validateProbabilities(featureAndProbabilities, model)
  }

  test("Gaussian Naive Bayes with weighted samples") {
    val numClasses = 3
    def modelEquals(m1: GaussianNaiveBayesModel, m2: GaussianNaiveBayesModel): Unit = {
      assert(m1.pi ~== m2.pi relTol 0.01)
      assert(m1.theta ~== m2.theta relTol 0.01)
      assert(m1.sigma ~== m2.sigma relTol 0.01)
    }
    val gnb = new GaussianNaiveBayes()
    MLTestingUtils.testArbitrarilyScaledWeights[GaussianNaiveBayesModel, GaussianNaiveBayes](
      dataset.as[LabeledPoint], gnb, modelEquals)
    MLTestingUtils.testOutliersWithSmallWeights[GaussianNaiveBayesModel, GaussianNaiveBayes](
      dataset.as[LabeledPoint], gnb, numClasses, modelEquals)
    MLTestingUtils.testOversamplingVsWeighting[GaussianNaiveBayesModel, GaussianNaiveBayes](
      dataset.as[LabeledPoint], gnb, modelEquals, seed)
  }

  test("read/write") {
    def checkModelData(model: GaussianNaiveBayesModel, model2: GaussianNaiveBayesModel): Unit = {
      assert(model.pi === model2.pi)
      assert(model.theta === model2.theta)
      assert(model.sigma === model2.sigma)
    }
    val gnb = new GaussianNaiveBayes()
    testEstimatorAndModelReadWrite(gnb, dataset, allParamSettings, checkModelData)
  }

  test("should support all NumericType labels and not support other types") {
    val gnb = new GaussianNaiveBayes()
    MLTestingUtils.checkNumericTypes[GaussianNaiveBayesModel, GaussianNaiveBayes](
      gnb, spark) { (expected, actual) =>
      assert(expected.pi === actual.pi)
      assert(expected.theta === actual.theta)
      assert(expected.sigma === actual.sigma)
    }
  }
}

object GaussianNaiveBayesSuite {

  /**
   * Mapping from all Params to valid settings which differ from the defaults.
   * This is useful for tests which need to exercise all Params, such as save/load.
   * This excludes input columns to simplify some tests.
   */
  val allParamSettings: Map[String, Any] = Map(
    "predictionCol" -> "myPrediction"
  )

  private def calcLabel(p: Double, pi: Array[Double]): Int = {
    var sum = 0.0
    for (j <- 0 until pi.length) {
      sum += pi(j)
      if (p < sum) return j
    }
    -1
  }

  // Generate input
  def generateGaussianNaiveBayesInput(
    pi: Array[Double],            // 1XC
    theta: Array[Array[Double]],  // CXD
    sigma: Array[Array[Double]],  // CXD
    nPoints: Int,
    seed: Int): Seq[LabeledPoint] = {
    val D = theta(0).length
    val rnd = new Random(seed)
    val _pi = pi.map(math.exp)

    for (i <- 0 until nPoints) yield {
      val y = calcLabel(rnd.nextDouble(), _pi)
      val xi = Array.tabulate[Double] (D) { j =>
        val mean = theta(y)(j)
        val variance = sigma(y)(j)
        mean + rnd.nextGaussian() * math.sqrt(variance)
      }
      LabeledPoint(y, Vectors.dense(xi))
    }
  }
}
