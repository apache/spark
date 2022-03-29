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

import breeze.linalg.{DenseVector => BDV, Vector => BV}
import breeze.stats.distributions.{Multinomial => BrzMultinomial, RandBasis => BrzRandBasis}

import org.apache.spark.ml.classification.NaiveBayes._
import org.apache.spark.ml.classification.NaiveBayesSuite._
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest, MLTestingUtils}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.sql.{Dataset, Row}

class NaiveBayesSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  @transient var dataset: Dataset[_] = _
  @transient var bernoulliDataset: Dataset[_] = _
  @transient var gaussianDataset: Dataset[_] = _
  @transient var gaussianDataset2: Dataset[_] = _
  @transient var complementDataset: Dataset[_] = _

  private val seed = 42

  override def beforeAll(): Unit = {
    super.beforeAll()

    val pi = Array(0.3, 0.3, 0.4).map(math.log)
    val theta = Array(
      Array(0.30, 0.30, 0.30, 0.30), // label 0
      Array(0.30, 0.30, 0.30, 0.30), // label 1
      Array(0.40, 0.40, 0.40, 0.40)  // label 2
    ).map(_.map(math.log))

    dataset = generateNaiveBayesInput(pi, theta, 100, seed).toDF()
    bernoulliDataset = generateNaiveBayesInput(pi, theta, 100, seed, "bernoulli").toDF()

    // theta for gaussian nb
    val theta2 = Array(
      Array(0.70, 0.10, 0.10, 0.10), // label 0: mean
      Array(0.10, 0.70, 0.10, 0.10), // label 1: mean
      Array(0.10, 0.10, 0.70, 0.10)  // label 2: mean
    )

    // sigma for gaussian nb
    val sigma = Array(
      Array(0.10, 0.10, 0.50, 0.10), // label 0: variance
      Array(0.50, 0.10, 0.10, 0.10), // label 1: variance
      Array(0.10, 0.10, 0.10, 0.50)  // label 2: variance
    )
    gaussianDataset = generateGaussianNaiveBayesInput(pi, theta2, sigma, 1000, seed).toDF()

    gaussianDataset2 = spark.read.format("libsvm")
      .load("../data/mllib/sample_multiclass_classification_data.txt")

    complementDataset = spark.read.format("libsvm")
        .load("../data/mllib/sample_libsvm_data.txt")
  }

  def validatePrediction(predictionAndLabels: Seq[Row]): Unit = {
    val numOfErrorPredictions = predictionAndLabels.count {
      case Row(prediction: Double, label: Double) =>
        prediction != label
    }
    // At least 80% of the predictions should be on.
    assert(numOfErrorPredictions < predictionAndLabels.length / 5)
  }

  def validateModelFit(
      piData: Vector,
      thetaData: Matrix,
      sigmaData: Matrix,
      model: NaiveBayesModel): Unit = {
    assert(Vectors.dense(model.pi.toArray.map(math.exp)) ~==
      Vectors.dense(piData.toArray.map(math.exp)) absTol 0.05, "pi mismatch")
    assert(model.theta.map(math.exp) ~== thetaData.map(math.exp) absTol 0.05, "theta mismatch")
    if (sigmaData === Matrices.zeros(0, 0)) {
      assert(model.sigma === Matrices.zeros(0, 0), "sigma mismatch")
    } else {
      assert(model.sigma.map(math.exp) ~== sigmaData.map(math.exp) absTol 0.05,
        "sigma mismatch")
    }
  }

  def expectedMultinomialProbabilities(model: NaiveBayesModel, feature: Vector): Vector = {
    val logClassProbs: BV[Double] = model.pi.asBreeze + model.theta.multiply(feature).asBreeze
    val classProbs = logClassProbs.toArray.map(math.exp)
    val classProbsSum = classProbs.sum
    Vectors.dense(classProbs.map(_ / classProbsSum))
  }

  def expectedBernoulliProbabilities(model: NaiveBayesModel, feature: Vector): Vector = {
    val negThetaMatrix = model.theta.map(v => math.log1p(-math.exp(v)))
    val negFeature = Vectors.dense(feature.toArray.map(v => 1.0 - v))
    val piTheta: BV[Double] = model.pi.asBreeze + model.theta.multiply(feature).asBreeze
    val logClassProbs: BV[Double] = piTheta + negThetaMatrix.multiply(negFeature).asBreeze
    val classProbs = logClassProbs.toArray.map(math.exp)
    val classProbsSum = classProbs.sum
    Vectors.dense(classProbs.map(_ / classProbsSum))
  }

  def expectedGaussianProbabilities(model: NaiveBayesModel, feature: Vector): Vector = {
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
      featureAndProbabilities: Seq[Row],
      model: NaiveBayesModel,
      modelType: String): Unit = {
    featureAndProbabilities.foreach {
      case Row(features: Vector, probability: Vector) =>
        assert(probability.toArray.sum ~== 1.0 relTol 1.0e-10)
        val expected = modelType match {
          case Multinomial =>
            expectedMultinomialProbabilities(model, features)
          case Bernoulli =>
            expectedBernoulliProbabilities(model, features)
          case Gaussian =>
            expectedGaussianProbabilities(model, features)
          case _ =>
            throw new IllegalArgumentException(s"Invalid modelType: $modelType.")
        }
        assert(probability ~== expected relTol 1.0e-10)
    }
  }

  test("model types") {
    assert(Multinomial === "multinomial")
    assert(Bernoulli === "bernoulli")
    assert(Gaussian === "gaussian")
    assert(Complement === "complement")
  }

  test("params") {
    ParamsSuite.checkParams(new NaiveBayes)
    val model = new NaiveBayesModel("nb", pi = Vectors.dense(Array(0.2, 0.8)),
      theta = new DenseMatrix(2, 3, Array(0.1, 0.2, 0.3, 0.4, 0.6, 0.4)),
      sigma = Matrices.zeros(0, 0))
    ParamsSuite.checkParams(model)
  }

  test("naive bayes: default params") {
    val nb = new NaiveBayes
    assert(nb.getLabelCol === "label")
    assert(nb.getFeaturesCol === "features")
    assert(nb.getPredictionCol === "prediction")
    assert(nb.getSmoothing === 1.0)
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

    val testDataset =
      generateNaiveBayesInput(piArray, thetaArray, nPoints, seed, "multinomial").toDF()
    val nb = new NaiveBayes().setSmoothing(1.0).setModelType("multinomial")
    val model = nb.fit(testDataset)

    validateModelFit(pi, theta, Matrices.zeros(0, 0), model)
    assert(model.hasParent)
    MLTestingUtils.checkCopyAndUids(nb, model)

    val validationDataset =
      generateNaiveBayesInput(piArray, thetaArray, nPoints, 17, "multinomial").toDF()

    testTransformerByGlobalCheckFunc[(Double, Vector)](validationDataset, model,
      "prediction", "label") { predictionAndLabels: Seq[Row] =>
      validatePrediction(predictionAndLabels)
    }

    testTransformerByGlobalCheckFunc[(Double, Vector)](validationDataset, model,
      "features", "probability") { featureAndProbabilities: Seq[Row] =>
      validateProbabilities(featureAndProbabilities, model, "multinomial")
    }

    ProbabilisticClassifierSuite.testPredictMethods[
      Vector, NaiveBayesModel](this, model, testDataset)
  }

  test("prediction on single instance") {
    val nPoints = 1000
    val piArray = Array(0.5, 0.1, 0.4).map(math.log)
    val thetaArray = Array(
      Array(0.70, 0.10, 0.10, 0.10), // label 0
      Array(0.10, 0.70, 0.10, 0.10), // label 1
      Array(0.10, 0.10, 0.70, 0.10)  // label 2
    ).map(_.map(math.log))

    val trainDataset =
      generateNaiveBayesInput(piArray, thetaArray, nPoints, seed, "multinomial").toDF()
    val nb = new NaiveBayes().setSmoothing(1.0).setModelType("multinomial")
    val model = nb.fit(trainDataset)

    val validationDataset =
      generateNaiveBayesInput(piArray, thetaArray, nPoints, 17, "multinomial").toDF()

    testPredictionModelSinglePrediction(model, validationDataset)
    testClassificationModelSingleRawPrediction(model, validationDataset)
    testProbClassificationModelSingleProbPrediction(model, validationDataset)
  }

  test("Naive Bayes with weighted samples") {
    val numClasses = 3
    def modelEquals(m1: NaiveBayesModel, m2: NaiveBayesModel): Unit = {
      assert(m1.getModelType === m2.getModelType)
      assert(m1.pi ~== m2.pi relTol 0.01)
      assert(m1.theta ~== m2.theta relTol 0.01)
      if (m1.getModelType == Gaussian) {
        assert(m1.sigma ~== m2.sigma relTol 0.01)
      }
    }
    val testParams = Seq[(String, Dataset[_])](
      ("bernoulli", bernoulliDataset),
      ("multinomial", dataset),
      ("complement", dataset),
      ("gaussian", gaussianDataset)
    )
    testParams.foreach { case (family, dataset) =>
      // NaiveBayes is sensitive to constant scaling of the weights unless smoothing is set to 0
      val estimatorNoSmoothing = new NaiveBayes().setSmoothing(0.0).setModelType(family)
      val estimatorWithSmoothing = new NaiveBayes().setModelType(family)
      MLTestingUtils.testArbitrarilyScaledWeights[NaiveBayesModel, NaiveBayes](
        dataset.as[LabeledPoint], estimatorNoSmoothing, modelEquals)
      MLTestingUtils.testOutliersWithSmallWeights[NaiveBayesModel, NaiveBayes](
        dataset.as[LabeledPoint], estimatorWithSmoothing, numClasses, modelEquals, outlierRatio = 3)
      MLTestingUtils.testOversamplingVsWeighting[NaiveBayesModel, NaiveBayes](
        dataset.as[LabeledPoint], estimatorWithSmoothing, modelEquals, seed)
    }
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

    val testDataset =
      generateNaiveBayesInput(piArray, thetaArray, nPoints, 45, "bernoulli").toDF()
    val nb = new NaiveBayes().setSmoothing(1.0).setModelType("bernoulli")
    val model = nb.fit(testDataset)

    validateModelFit(pi, theta, Matrices.zeros(0, 0), model)
    assert(model.hasParent)

    val validationDataset =
      generateNaiveBayesInput(piArray, thetaArray, nPoints, 20, "bernoulli").toDF()

    testTransformerByGlobalCheckFunc[(Double, Vector)](validationDataset, model,
      "prediction", "label") { predictionAndLabels: Seq[Row] =>
      validatePrediction(predictionAndLabels)
    }

    testTransformerByGlobalCheckFunc[(Double, Vector)](validationDataset, model,
      "features", "probability") { featureAndProbabilities: Seq[Row] =>
      validateProbabilities(featureAndProbabilities, model, "bernoulli")
    }

    ProbabilisticClassifierSuite.testPredictMethods[
      Vector, NaiveBayesModel](this, model, testDataset)
  }

  test("NaiveBayes validate input dataset") {
    testInvalidClassificationLabels(new NaiveBayes().fit(_), None)
    testInvalidWeights(new NaiveBayes().setWeightCol("weight").fit(_))
    testInvalidVectors(new NaiveBayes().setModelType(Gaussian).fit(_))
  }

  test("Multinomial and Complement: check vectors") {
    Seq(Multinomial, Complement).foreach { mode =>
      val df1 = sc.parallelize(Seq(
        (1.0, 1.0, Vectors.dense(1.0, 2.0)),
        (0.0, 1.0, null)
      )).toDF("label", "weight", "features")
      val e1 = intercept[Exception] { new NaiveBayes().setModelType(mode).fit(df1) }
      assert(e1.getMessage.contains("Vectors MUST NOT be Null"))

      val df2 = spark.createDataFrame(Seq(
        LabeledPoint(1.0, Vectors.dense(1.0)),
        LabeledPoint(0.0, Vectors.dense(-1.0)),
        LabeledPoint(1.0, Vectors.dense(1.0)),
        LabeledPoint(1.0, Vectors.dense(0.0))))
      val e2 = intercept[Exception] { new NaiveBayes().setModelType(mode).fit(df2) }
      assert(e2.getMessage.contains("Vector values MUST NOT be Negative, NaN or Infinity"))

      val df3 = spark.createDataFrame(Seq(
        LabeledPoint(1.0, Vectors.sparse(1, Array(0), Array(1.0))),
        LabeledPoint(0.0, Vectors.sparse(1, Array(0), Array(-1.0))),
        LabeledPoint(1.0, Vectors.sparse(1, Array(0), Array(1.0))),
        LabeledPoint(1.0, Vectors.sparse(1, Array.empty, Array.empty))))
      val e3 = intercept[Exception] { new NaiveBayes().setModelType(mode).fit(df3) }
      assert(e3.getMessage.contains("Vector values MUST NOT be Negative, NaN or Infinity"))

      val df4 = spark.createDataFrame(Seq(
        LabeledPoint(1.0, Vectors.sparse(1, Array(0), Array(1.0))),
        LabeledPoint(0.0, Vectors.sparse(1, Array(0), Array(Double.NaN))),
        LabeledPoint(1.0, Vectors.sparse(1, Array(0), Array(1.0))),
        LabeledPoint(1.0, Vectors.sparse(1, Array.empty, Array.empty))))
      val e4 = intercept[Exception] { new NaiveBayes().setModelType(mode).fit(df4) }
      assert(e4.getMessage.contains("Vector values MUST NOT be Negative, NaN or Infinity"))
    }
  }

  test("Bernoulli: check vectors") {
    val df1 = sc.parallelize(Seq(
      (1.0, 1.0, Vectors.dense(1.0, 2.0)),
      (0.0, 1.0, null)
    )).toDF("label", "weight", "features")
    val e1 = intercept[Exception] {
      new NaiveBayes().setModelType(Bernoulli).setSmoothing(1.0).fit(df1)
    }
    assert(e1.getMessage.contains("Vectors MUST NOT be Null"))

    val df2 = spark.createDataFrame(Seq(
      LabeledPoint(1.0, Vectors.dense(1.0)),
      LabeledPoint(0.0, Vectors.dense(2.0)),
      LabeledPoint(1.0, Vectors.dense(1.0)),
      LabeledPoint(1.0, Vectors.dense(0.0))))

    val e2 = intercept[Exception] {
      new NaiveBayes().setModelType(Bernoulli).setSmoothing(1.0).fit(df2)
    }
    assert(e2.getMessage.contains("Vector values MUST be in {0, 1}"))

    val okTrain = spark.createDataFrame(Seq(
      LabeledPoint(1.0, Vectors.dense(1.0)),
      LabeledPoint(0.0, Vectors.dense(0.0)),
      LabeledPoint(1.0, Vectors.dense(1.0)),
      LabeledPoint(1.0, Vectors.dense(1.0)),
      LabeledPoint(0.0, Vectors.dense(0.0)),
      LabeledPoint(1.0, Vectors.dense(1.0)),
      LabeledPoint(1.0, Vectors.dense(1.0))))

    val model = new NaiveBayes().setModelType(Bernoulli).setSmoothing(1.0).fit(okTrain)

    val badPredict = spark.createDataFrame(Seq(
      LabeledPoint(1.0, Vectors.dense(1.0)),
      LabeledPoint(1.0, Vectors.dense(2.0)),
      LabeledPoint(1.0, Vectors.dense(1.0)),
      LabeledPoint(1.0, Vectors.dense(0.0))))

    intercept[Exception] {
      model.transform(badPredict).collect()
    }
  }

  test("Naive Bayes Gaussian") {
    val piArray = Array(0.5, 0.1, 0.4).map(math.log)

    val thetaArray = Array(
      Array(0.70, 0.10, 0.10, 0.10), // label 0: mean
      Array(0.10, 0.70, 0.10, 0.10), // label 1: mean
      Array(0.10, 0.10, 0.70, 0.10)  // label 2: mean
    )

    val sigmaArray = Array(
      Array(0.10, 0.10, 0.50, 0.10), // label 0: variance
      Array(0.50, 0.10, 0.10, 0.10), // label 1: variance
      Array(0.10, 0.10, 0.10, 0.50)  // label 2: variance
    )

    val pi = Vectors.dense(piArray)
    val theta = new DenseMatrix(3, 4, thetaArray.flatten, true)
    val sigma = new DenseMatrix(3, 4, sigmaArray.flatten, true)

    val nPoints = 10000
    val testDataset =
      generateGaussianNaiveBayesInput(piArray, thetaArray, sigmaArray, nPoints, 42).toDF()
    val gnb = new NaiveBayes().setModelType("gaussian")
    val model = gnb.fit(testDataset)

    validateModelFit(pi, theta, sigma, model)
    assert(model.hasParent)

    val validationDataset =
      generateGaussianNaiveBayesInput(piArray, thetaArray, sigmaArray, nPoints, 17).toDF()

    val predictionAndLabels = model.transform(validationDataset).select("prediction", "label")
    validatePrediction(predictionAndLabels.collect())

    val featureAndProbabilities = model.transform(validationDataset)
      .select("features", "probability")
    validateProbabilities(featureAndProbabilities.collect(), model, "gaussian")
  }

  test("Naive Bayes Gaussian - Model Coefficients") {
    /*
     Using the following Python code to verify the correctness.

     import numpy as np
     from sklearn.naive_bayes import GaussianNB
     from sklearn.datasets import load_svmlight_file

     path = "./data/mllib/sample_multiclass_classification_data.txt"
     X, y = load_svmlight_file(path)
     X = X.toarray()
     clf = GaussianNB()
     clf.fit(X, y)

     >>> clf.class_prior_
     array([0.33333333, 0.33333333, 0.33333333])
     >>> clf.theta_
     array([[ 0.27111101, -0.18833335,  0.54305072,  0.60500005],
            [-0.60777778,  0.18166667, -0.84271174, -0.88000014],
            [-0.09111114, -0.35833336,  0.10508474,  0.0216667 ]])
     >>> clf.sigma_
     array([[0.12230125, 0.07078052, 0.03430001, 0.05133607],
            [0.03758145, 0.0988028 , 0.0033903 , 0.00782224],
            [0.08058764, 0.06701387, 0.02486641, 0.02661392]])
    */

    val gnb = new NaiveBayes().setModelType(Gaussian)
    val model = gnb.fit(gaussianDataset2)
    assert(Vectors.dense(model.pi.toArray.map(math.exp)) ~=
      Vectors.dense(0.33333333, 0.33333333, 0.33333333) relTol 1E-5)

    val thetaRows = model.theta.rowIter.toArray
    assert(thetaRows(0) ~=
      Vectors.dense(0.27111101, -0.18833335, 0.54305072, 0.60500005) relTol 1E-5)
    assert(thetaRows(1) ~=
      Vectors.dense(-0.60777778, 0.18166667, -0.84271174, -0.88000014) relTol 1E-5)
    assert(thetaRows(2) ~=
      Vectors.dense(-0.09111114, -0.35833336, 0.10508474, 0.0216667) relTol 1E-5)

    val sigmaRows = model.sigma.rowIter.toArray
    assert(sigmaRows(0) ~=
      Vectors.dense(0.12230125, 0.07078052, 0.03430001, 0.05133607) relTol 1E-5)
    assert(sigmaRows(1) ~=
      Vectors.dense(0.03758145, 0.0988028, 0.0033903, 0.00782224) relTol 1E-5)
    assert(sigmaRows(2) ~=
      Vectors.dense(0.08058764, 0.06701387, 0.02486641, 0.02661392) relTol 1E-5)
  }

  test("Naive Bayes Complement") {
    /*
     Using the following Python code to verify the correctness.

     import numpy as np
     from sklearn.naive_bayes import ComplementNB
     from sklearn.datasets import load_svmlight_file

     path = "./data/mllib/sample_libsvm_data.txt"
     X, y = load_svmlight_file(path)
     X = X.toarray()
     clf = ComplementNB()
     clf.fit(X, y)

     >>> clf.feature_log_prob_[:, -5:]
     array([[ 7.2937608 , 10.26577655, 13.73151245, 13.73151245, 13.73151245],
            [ 6.99678043,  7.51387415,  7.74399483,  8.32904552,  9.53119848]])
     >>> clf.predict_log_proba(X[:5])
     array([[     0.        , -74732.70765355],
            [-36018.30169185,      0.        ],
            [-37126.4015229 ,      0.        ],
            [-27649.81038619,      0.        ],
            [-28767.84075587,      0.        ]])
     >>> clf.predict_proba(X[:5])
     array([[1., 0.],
            [0., 1.],
            [0., 1.],
            [0., 1.],
            [0., 1.]])
    */

    val cnb = new NaiveBayes().setModelType(Complement)
    val model = cnb.fit(complementDataset)

    val thetaRows = model.theta.rowIter.map(vec => Vectors.dense(vec.toArray.takeRight(5))).toArray
    assert(thetaRows(0) ~=
      Vectors.dense(7.2937608, 10.26577655, 13.73151245, 13.73151245, 13.73151245) relTol 1E-5)
    assert(thetaRows(1) ~=
      Vectors.dense(6.99678043, 7.51387415, 7.74399483, 8.32904552, 9.53119848) relTol 1E-5)

    val preds = model.transform(complementDataset)
      .select("rawPrediction", "probability")
      .as[(Vector, Vector)]
      .take(5)
    assert(preds(0)._1 ~= Vectors.dense(0.0, -74732.70765355) relTol 1E-5)
    assert(preds(0)._2 ~= Vectors.dense(1.0, 0.0) relTol 1E-5)
    assert(preds(1)._1 ~= Vectors.dense(-36018.30169185, 0.0) relTol 1E-5)
    assert(preds(1)._2 ~= Vectors.dense(0.0, 1.0) relTol 1E-5)
    assert(preds(2)._1 ~= Vectors.dense(-37126.4015229, 0.0) relTol 1E-5)
    assert(preds(2)._2 ~= Vectors.dense(0.0, 1.0) relTol 1E-5)
    assert(preds(3)._1 ~= Vectors.dense(-27649.81038619, 0.0) relTol 1E-5)
    assert(preds(3)._2 ~= Vectors.dense(0.0, 1.0) relTol 1E-5)
    assert(preds(4)._1 ~= Vectors.dense(-28767.84075587, 0.0) relTol 1E-5)
    assert(preds(4)._2 ~= Vectors.dense(0.0, 1.0) relTol 1E-5)
  }

  test("read/write") {
    def checkModelData(model: NaiveBayesModel, model2: NaiveBayesModel): Unit = {
      assert(model.getModelType === model2.getModelType)
      assert(model.pi === model2.pi)
      assert(model.theta === model2.theta)
      if (model.getModelType == "gaussian") {
        assert(model.sigma === model2.sigma)
      } else {
        assert(model.sigma === Matrices.zeros(0, 0) && model2.sigma === Matrices.zeros(0, 0))
      }
    }
    val nb = new NaiveBayes()
    testEstimatorAndModelReadWrite(nb, dataset, NaiveBayesSuite.allParamSettings,
      NaiveBayesSuite.allParamSettings, checkModelData)

    val gnb = new NaiveBayes().setModelType("gaussian")
    testEstimatorAndModelReadWrite(gnb, gaussianDataset,
      NaiveBayesSuite.allParamSettingsForGaussian,
      NaiveBayesSuite.allParamSettingsForGaussian, checkModelData)
  }

  test("should support all NumericType labels and weights, and not support other types") {
    val nb = new NaiveBayes()
    MLTestingUtils.checkNumericTypes[NaiveBayesModel, NaiveBayes](
      nb, spark) { (expected, actual) =>
        assert(expected.pi === actual.pi)
        assert(expected.theta === actual.theta)
        assert(expected.sigma === Matrices.zeros(0, 0) && actual.sigma === Matrices.zeros(0, 0))
      }
  }
}

object NaiveBayesSuite {

  /**
   * Mapping from all Params to valid settings which differ from the defaults.
   * This is useful for tests which need to exercise all Params, such as save/load.
   * This excludes input columns to simplify some tests.
   */
  val allParamSettings: Map[String, Any] = Map(
    "predictionCol" -> "myPrediction",
    "smoothing" -> 0.1
  )

  /**
   * Mapping from all Params to valid settings which differ from the defaults.
   * This is useful for tests which need to exercise all Params, such as save/load.
   * This excludes input columns to simplify some tests.
   */
  val allParamSettingsForGaussian: Map[String, Any] = Map(
    "predictionCol" -> "myPrediction",
    "modelType" -> "gaussian"
  )

  private def calcLabel(p: Double, pi: Array[Double]): Int = {
    var sum = 0.0
    for (j <- 0 until pi.length) {
      sum += pi(j)
      if (p < sum) return j
    }
    -1
  }

  // Generate input of the form Y = (theta * x).argmax()
  def generateNaiveBayesInput(
    pi: Array[Double],            // 1XC
    theta: Array[Array[Double]],  // CXD
    nPoints: Int,
    seed: Int,
    modelType: String = Multinomial,
    sample: Int = 10): Seq[LabeledPoint] = {
    val D = theta(0).length
    val rnd = new Random(seed)
    val _pi = pi.map(math.exp)
    val _theta = theta.map(row => row.map(math.exp))

    implicit val rngForBrzMultinomial = BrzRandBasis.withSeed(seed)
    for (i <- 0 until nPoints) yield {
      val y = calcLabel(rnd.nextDouble(), _pi)
      val xi = modelType match {
        case Bernoulli => Array.tabulate[Double] (D) { j =>
            if (rnd.nextDouble () < _theta(y)(j) ) 1 else 0
        }
        case Multinomial =>
          val mult = BrzMultinomial(BDV(_theta(y)))
          val emptyMap = (0 until D).map(x => (x, 0.0)).toMap
          val counts = emptyMap ++ mult.sample(sample).groupBy(x => x).map {
            case (index, reps) => (index, reps.size.toDouble)
          }
          counts.toArray.sortBy(_._1).map(_._2)
        case _ =>
          // This should never happen.
          throw new IllegalArgumentException(s"Invalid modelType: $modelType.")
      }

      LabeledPoint(y, Vectors.dense(xi))
    }
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
