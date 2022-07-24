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

package org.apache.spark.ml.regression

import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.LogisticRegressionSuite._
import org.apache.spark.ml.feature.{Instance, OffsetInstance}
import org.apache.spark.ml.feature.{LabeledPoint, RFormula}
import org.apache.spark.ml.linalg.{BLAS, DenseVector, Vector, Vectors}
import org.apache.spark.ml.param.{ParamMap, ParamsSuite}
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest, MLTestingUtils}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.random._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType

class GeneralizedLinearRegressionSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  private val seed: Int = 42
  @transient var datasetGaussianIdentity: DataFrame = _
  @transient var datasetGaussianLog: DataFrame = _
  @transient var datasetGaussianInverse: DataFrame = _
  @transient var datasetBinomial: DataFrame = _
  @transient var datasetPoissonLog: DataFrame = _
  @transient var datasetPoissonLogWithZero: DataFrame = _
  @transient var datasetPoissonIdentity: DataFrame = _
  @transient var datasetPoissonSqrt: DataFrame = _
  @transient var datasetGammaInverse: DataFrame = _
  @transient var datasetGammaIdentity: DataFrame = _
  @transient var datasetGammaLog: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    import GeneralizedLinearRegressionSuite._

    datasetGaussianIdentity = generateGeneralizedLinearRegressionInput(
      intercept = 2.5, coefficients = Array(2.2, 0.6), xMean = Array(2.9, 10.5),
      xVariance = Array(0.7, 1.2), nPoints = 10000, seed, noiseLevel = 0.01,
      family = "gaussian", link = "identity").toDF()

    datasetGaussianLog = generateGeneralizedLinearRegressionInput(
      intercept = 0.25, coefficients = Array(0.22, 0.06), xMean = Array(2.9, 10.5),
      xVariance = Array(0.7, 1.2), nPoints = 10000, seed, noiseLevel = 0.01,
      family = "gaussian", link = "log").toDF()

    datasetGaussianInverse = generateGeneralizedLinearRegressionInput(
      intercept = 2.5, coefficients = Array(2.2, 0.6), xMean = Array(2.9, 10.5),
      xVariance = Array(0.7, 1.2), nPoints = 10000, seed, noiseLevel = 0.01,
      family = "gaussian", link = "inverse").toDF()

    datasetBinomial = {
      val nPoints = 10000
      val coefficients = Array(-0.57997, 0.912083, -0.371077, -0.819866, 2.688191)
      val xMean = Array(5.843, 3.057, 3.758, 1.199)
      val xVariance = Array(0.6856, 0.1899, 3.116, 0.581)

      val testData =
        generateMultinomialLogisticInput(coefficients, xMean, xVariance,
          addIntercept = true, nPoints, seed)

      testData.toDF()
    }

    datasetPoissonLog = generateGeneralizedLinearRegressionInput(
      intercept = 0.25, coefficients = Array(0.22, 0.06), xMean = Array(2.9, 10.5),
      xVariance = Array(0.7, 1.2), nPoints = 10000, seed, noiseLevel = 0.01,
      family = "poisson", link = "log").toDF()

    datasetPoissonLogWithZero = Seq(
      LabeledPoint(0.0, Vectors.dense(18, 1.0)),
      LabeledPoint(1.0, Vectors.dense(12, 0.0)),
      LabeledPoint(0.0, Vectors.dense(15, 0.0)),
      LabeledPoint(0.0, Vectors.dense(13, 2.0)),
      LabeledPoint(0.0, Vectors.dense(15, 1.0)),
      LabeledPoint(1.0, Vectors.dense(16, 1.0))
    ).toDF()

    datasetPoissonIdentity = generateGeneralizedLinearRegressionInput(
      intercept = 2.5, coefficients = Array(2.2, 0.6), xMean = Array(2.9, 10.5),
      xVariance = Array(0.7, 1.2), nPoints = 10000, seed, noiseLevel = 0.01,
      family = "poisson", link = "identity").toDF()

    datasetPoissonSqrt = generateGeneralizedLinearRegressionInput(
      intercept = 2.5, coefficients = Array(2.2, 0.6), xMean = Array(2.9, 10.5),
      xVariance = Array(0.7, 1.2), nPoints = 10000, seed, noiseLevel = 0.01,
      family = "poisson", link = "sqrt").toDF()

    datasetGammaInverse = generateGeneralizedLinearRegressionInput(
      intercept = 2.5, coefficients = Array(2.2, 0.6), xMean = Array(2.9, 10.5),
      xVariance = Array(0.7, 1.2), nPoints = 10000, seed, noiseLevel = 0.01,
      family = "gamma", link = "inverse").toDF()

    datasetGammaIdentity = generateGeneralizedLinearRegressionInput(
      intercept = 2.5, coefficients = Array(2.2, 0.6), xMean = Array(2.9, 10.5),
      xVariance = Array(0.7, 1.2), nPoints = 10000, seed, noiseLevel = 0.01,
      family = "gamma", link = "identity").toDF()

    datasetGammaLog = generateGeneralizedLinearRegressionInput(
      intercept = 0.25, coefficients = Array(0.22, 0.06), xMean = Array(2.9, 10.5),
      xVariance = Array(0.7, 1.2), nPoints = 10000, seed, noiseLevel = 0.01,
      family = "gamma", link = "log").toDF()
  }

  /**
   * Enable the ignored test to export the dataset into CSV format,
   * so we can validate the training accuracy compared with R's glm and glmnet package.
   */
  ignore("export test data into CSV format") {
    datasetGaussianIdentity.rdd.map { case Row(label: Double, features: Vector) =>
      label + "," + features.toArray.mkString(",")
    }.repartition(1).saveAsTextFile(
      "target/tmp/GeneralizedLinearRegressionSuite/datasetGaussianIdentity")
    datasetGaussianLog.rdd.map { case Row(label: Double, features: Vector) =>
      label + "," + features.toArray.mkString(",")
    }.repartition(1).saveAsTextFile(
      "target/tmp/GeneralizedLinearRegressionSuite/datasetGaussianLog")
    datasetGaussianInverse.rdd.map { case Row(label: Double, features: Vector) =>
      label + "," + features.toArray.mkString(",")
    }.repartition(1).saveAsTextFile(
      "target/tmp/GeneralizedLinearRegressionSuite/datasetGaussianInverse")
    datasetBinomial.rdd.map { case Row(label: Double, features: Vector) =>
      label + "," + features.toArray.mkString(",")
    }.repartition(1).saveAsTextFile(
      "target/tmp/GeneralizedLinearRegressionSuite/datasetBinomial")
    datasetPoissonLog.rdd.map { case Row(label: Double, features: Vector) =>
      label + "," + features.toArray.mkString(",")
    }.repartition(1).saveAsTextFile(
      "target/tmp/GeneralizedLinearRegressionSuite/datasetPoissonLog")
    datasetPoissonLogWithZero.rdd.map { case Row(label: Double, features: Vector) =>
      label + "," + features.toArray.mkString(",")
    }.repartition(1).saveAsTextFile(
      "target/tmp/GeneralizedLinearRegressionSuite/datasetPoissonLogWithZero")
    datasetPoissonIdentity.rdd.map { case Row(label: Double, features: Vector) =>
      label + "," + features.toArray.mkString(",")
    }.repartition(1).saveAsTextFile(
      "target/tmp/GeneralizedLinearRegressionSuite/datasetPoissonIdentity")
    datasetPoissonSqrt.rdd.map { case Row(label: Double, features: Vector) =>
      label + "," + features.toArray.mkString(",")
    }.repartition(1).saveAsTextFile(
      "target/tmp/GeneralizedLinearRegressionSuite/datasetPoissonSqrt")
    datasetGammaInverse.rdd.map { case Row(label: Double, features: Vector) =>
      label + "," + features.toArray.mkString(",")
    }.repartition(1).saveAsTextFile(
      "target/tmp/GeneralizedLinearRegressionSuite/datasetGammaInverse")
    datasetGammaIdentity.rdd.map { case Row(label: Double, features: Vector) =>
      label + "," + features.toArray.mkString(",")
    }.repartition(1).saveAsTextFile(
      "target/tmp/GeneralizedLinearRegressionSuite/datasetGammaIdentity")
    datasetGammaLog.rdd.map { case Row(label: Double, features: Vector) =>
      label + "," + features.toArray.mkString(",")
    }.repartition(1).saveAsTextFile(
      "target/tmp/GeneralizedLinearRegressionSuite/datasetGammaLog")
  }

  test("params") {
    ParamsSuite.checkParams(new GeneralizedLinearRegression)
    val model = new GeneralizedLinearRegressionModel("genLinReg", Vectors.dense(0.0), 0.0)
    ParamsSuite.checkParams(model)
  }

  test("generalized linear regression: default params") {
    val glr = new GeneralizedLinearRegression
    assert(glr.getLabelCol === "label")
    assert(glr.getFeaturesCol === "features")
    assert(glr.getPredictionCol === "prediction")
    assert(glr.getFitIntercept)
    assert(glr.getTol === 1E-6)
    assert(!glr.isDefined(glr.weightCol))
    assert(glr.getRegParam === 0.0)
    assert(glr.getSolver == "irls")
    assert(glr.getVariancePower === 0.0)

    // TODO: Construct model directly instead of via fitting.
    val model = glr.setFamily("gaussian").setLink("identity")
      .fit(datasetGaussianIdentity)

    MLTestingUtils.checkCopyAndUids(glr, model)
    assert(model.hasSummary)
    val copiedModel = model.copy(ParamMap.empty)
    assert(copiedModel.hasSummary)
    model.setSummary(None)
    assert(!model.hasSummary)

    assert(model.getFeaturesCol === "features")
    assert(model.getPredictionCol === "prediction")
    assert(model.intercept !== 0.0)
    assert(model.hasParent)
    assert(model.getFamily === "gaussian")
    assert(model.getLink === "identity")
  }

  test("GeneralizedLinearRegression validate input dataset") {
    testInvalidRegressionLabels(new GeneralizedLinearRegression().fit(_))
    testInvalidWeights(new GeneralizedLinearRegression().setWeightCol("weight").fit(_))
    testInvalidVectors(new GeneralizedLinearRegression().fit(_))

    // offsets contains NULL
    val df1 = sc.parallelize(Seq(
      (1.0, null, Vectors.dense(1.0, 2.0)),
      (1.0, "1.0", Vectors.dense(1.0, 2.0))
    )).toDF("label", "str_offset", "features")
      .select(col("label"), col("str_offset").cast("double").as("offset"), col("features"))
    val e1 = intercept[Exception](new GeneralizedLinearRegression().setOffsetCol("offset").fit(df1))
    assert(e1.getMessage.contains("Offsets MUST NOT be Null or NaN"))

    // offsets contains NaN
    val df2 = sc.parallelize(Seq(
      (1.0, Double.NaN, Vectors.dense(1.0, 2.0)),
      (1.0, 1.0, Vectors.dense(1.0, 2.0))
    )).toDF("label", "offset", "features")
    val e2 = intercept[Exception](new GeneralizedLinearRegression().setOffsetCol("offset").fit(df2))
    assert(e2.getMessage.contains("Offsets MUST NOT be Null or NaN"))

    // offsets contains Infinity
    val df3 = sc.parallelize(Seq(
      (1.0, Double.PositiveInfinity, Vectors.dense(1.0, 2.0)),
      (1.0, 1.0, Vectors.dense(1.0, 2.0))
    )).toDF("label", "offset", "features")
    val e3 = intercept[Exception](new GeneralizedLinearRegression().setOffsetCol("offset").fit(df3))
    assert(e3.getMessage.contains("Offsets MUST NOT be Infinity"))
  }

  test("prediction on single instance") {
    val glr = new GeneralizedLinearRegression
    val model = glr.setFamily("gaussian").setLink("identity")
      .fit(datasetGaussianIdentity)

    testPredictionModelSinglePrediction(model, datasetGaussianIdentity)
  }

  test("generalized linear regression: gaussian family against glm") {
    /*
       R code:
       f1 <- data$V1 ~ data$V2 + data$V3 - 1
       f2 <- data$V1 ~ data$V2 + data$V3

       data <- read.csv("path", header=FALSE)
       for (formula in c(f1, f2)) {
         model <- glm(formula, family="gaussian", data=data)
         print(as.vector(coef(model)))
       }

       [1] 2.2958751 0.8088523
       [1] 2.5009266 2.1997901 0.5999522

       data <- read.csv("path", header=FALSE)
       model1 <- glm(f1, family=gaussian(link=log), data=data, start=c(0,0))
       model2 <- glm(f2, family=gaussian(link=log), data=data, start=c(0,0,0))
       print(as.vector(coef(model1)))
       print(as.vector(coef(model2)))

       [1] 0.23063118 0.07995495
       [1] 0.25016124 0.21995737 0.05999335

       data <- read.csv("path", header=FALSE)
       for (formula in c(f1, f2)) {
         model <- glm(formula, family=gaussian(link=inverse), data=data)
         print(as.vector(coef(model)))
       }

       [1] 2.3320341 0.8121904
       [1] 2.2837064 2.2487147 0.6120262
     */

    val expected = Seq(
      Vectors.dense(0.0, 2.2958751, 0.8088523),
      Vectors.dense(2.5009266, 2.1997901, 0.5999522),
      Vectors.dense(0.0, 0.23063118, 0.07995495),
      Vectors.dense(0.25016124, 0.21995737, 0.05999335),
      Vectors.dense(0.0, 2.3320341, 0.8121904),
      Vectors.dense(2.2837064, 2.2487147, 0.6120262))

    import GeneralizedLinearRegression._

    var idx = 0
    for ((link, dataset) <- Seq(("identity", datasetGaussianIdentity), ("log", datasetGaussianLog),
      ("inverse", datasetGaussianInverse))) {
      for (fitIntercept <- Seq(false, true)) {
        val trainer = new GeneralizedLinearRegression().setFamily("gaussian").setLink(link)
          .setFitIntercept(fitIntercept).setLinkPredictionCol("linkPrediction").setTol(1e-3)
        val model = trainer.fit(dataset)
        val actual = Vectors.dense(model.intercept, model.coefficients(0), model.coefficients(1))
        assert(actual ~= expected(idx) absTol 1e-4, "Model mismatch: GLM with gaussian family, " +
          s"$link link and fitIntercept = $fitIntercept.")

        val familyLink = FamilyAndLink(trainer)
        testTransformer[(Double, Vector)](dataset, model,
          "features", "prediction", "linkPrediction") {
            case Row(features: DenseVector, prediction1: Double, linkPrediction1: Double) =>
              val eta = BLAS.dot(features, model.coefficients) + model.intercept
              val prediction2 = familyLink.fitted(eta)
              val linkPrediction2 = eta
              assert(prediction1 ~= prediction2 relTol 1E-5, "Prediction mismatch: GLM with " +
                s"gaussian family, $link link and fitIntercept = $fitIntercept.")
              assert(linkPrediction1 ~= linkPrediction2 relTol 1E-5, "Link Prediction mismatch: " +
                s"GLM with gaussian family, $link link and fitIntercept = $fitIntercept.")
        }

        idx += 1
      }
    }
  }

  test("generalized linear regression: gaussian family against glmnet") {
    /*
       R code:
       library(glmnet)
       data <- read.csv("path", header=FALSE)
       label = data$V1
       features = as.matrix(data.frame(data$V2, data$V3))
       for (intercept in c(FALSE, TRUE)) {
         for (lambda in c(0.0, 0.1, 1.0)) {
           model <- glmnet(features, label, family="gaussian", intercept=intercept,
                           lambda=lambda, alpha=0, thresh=1E-14)
           print(as.vector(coef(model)))
         }
       }

       [1] 0.0000000 2.2958757 0.8088521
       [1] 0.0000000 2.2128149 0.8310136
       [1] 0.0000000 1.7174260 0.9611137
       [1] 2.5009266 2.1997901 0.5999522
       [1] 3.1113269 2.0932659 0.5712717
       [1] 6.7604302 1.4578902 0.3994153
     */

    val expected = Seq(
      Vectors.dense(0.0, 2.2958757, 0.8088521),
      Vectors.dense(0.0, 2.2128149, 0.8310136),
      Vectors.dense(0.0, 1.7174260, 0.9611137),
      Vectors.dense(2.5009266, 2.1997901, 0.5999522),
      Vectors.dense(3.1113269, 2.0932659, 0.5712717),
      Vectors.dense(6.7604302, 1.4578902, 0.3994153))

    var idx = 0
    for (fitIntercept <- Seq(false, true);
         regParam <- Seq(0.0, 0.1, 1.0)) {
      val trainer = new GeneralizedLinearRegression().setFamily("gaussian")
        .setFitIntercept(fitIntercept).setRegParam(regParam).setTol(1e-3)
      val model = trainer.fit(datasetGaussianIdentity)
      val actual = Vectors.dense(model.intercept, model.coefficients(0), model.coefficients(1))
      assert(actual ~= expected(idx) absTol 1e-4, "Model mismatch: GLM with gaussian family, " +
        s"fitIntercept = $fitIntercept and regParam = $regParam.")

      idx += 1
    }
  }

  test("generalized linear regression: binomial family against glm") {
    /*
       R code:
       f1 <- data$V1 ~ data$V2 + data$V3 + data$V4 + data$V5 - 1
       f2 <- data$V1 ~ data$V2 + data$V3 + data$V4 + data$V5
       data <- read.csv("path", header=FALSE)

       for (formula in c(f1, f2)) {
         model <- glm(formula, family="binomial", data=data)
         print(as.vector(coef(model)))
       }

       [1] -0.3560284  1.3010002 -0.3570805 -0.7406762
       [1]  2.8367406 -0.5896187  0.8931655 -0.3925169 -0.7996989

       for (formula in c(f1, f2)) {
         model <- glm(formula, family=binomial(link=probit), data=data)
         print(as.vector(coef(model)))
       }

       [1] -0.2134390  0.7800646 -0.2144267 -0.4438358
       [1]  1.6995366 -0.3524694  0.5332651 -0.2352985 -0.4780850

       for (formula in c(f1, f2)) {
         model <- glm(formula, family=binomial(link=cloglog), data=data)
         print(as.vector(coef(model)))
       }

       [1] -0.2832198  0.8434144 -0.2524727 -0.5293452
       [1]  1.5063590 -0.4038015  0.6133664 -0.2687882 -0.5541758
     */
    val expected = Seq(
      Vectors.dense(0.0, -0.3560284, 1.3010002, -0.3570805, -0.7406762),
      Vectors.dense(2.8367406, -0.5896187, 0.8931655, -0.3925169, -0.7996989),
      Vectors.dense(0.0, -0.2134390, 0.7800646, -0.2144267, -0.4438358),
      Vectors.dense(1.6995366, -0.3524694, 0.5332651, -0.2352985, -0.4780850),
      Vectors.dense(0.0, -0.2832198, 0.8434144, -0.2524727, -0.5293452),
      Vectors.dense(1.5063590, -0.4038015, 0.6133664, -0.2687882, -0.5541758))

    import GeneralizedLinearRegression._

    var idx = 0
    for ((link, dataset) <- Seq(("logit", datasetBinomial), ("probit", datasetBinomial),
      ("cloglog", datasetBinomial))) {
      for (fitIntercept <- Seq(false, true)) {
        val trainer = new GeneralizedLinearRegression().setFamily("binomial").setLink(link)
          .setFitIntercept(fitIntercept).setLinkPredictionCol("linkPrediction").setTol(1e-3)
        val model = trainer.fit(dataset)
        val actual = Vectors.dense(model.intercept, model.coefficients(0), model.coefficients(1),
          model.coefficients(2), model.coefficients(3))
        assert(actual ~= expected(idx) absTol 1e-4, "Model mismatch: GLM with binomial family, " +
          s"$link link and fitIntercept = $fitIntercept.")

        val familyLink = FamilyAndLink(trainer)
        testTransformer[(Double, Vector)](dataset, model,
          "features", "prediction", "linkPrediction") {
            case Row(features: DenseVector, prediction1: Double, linkPrediction1: Double) =>
              val eta = BLAS.dot(features, model.coefficients) + model.intercept
              val prediction2 = familyLink.fitted(eta)
              val linkPrediction2 = eta
              assert(prediction1 ~= prediction2 relTol 1E-5, "Prediction mismatch: GLM with " +
                s"binomial family, $link link and fitIntercept = $fitIntercept.")
              assert(linkPrediction1 ~= linkPrediction2 relTol 1E-5, "Link Prediction mismatch: " +
                s"GLM with binomial family, $link link and fitIntercept = $fitIntercept.")
        }

        idx += 1
      }
    }
  }

  test("generalized linear regression: poisson family against glm") {
    /*
       R code:
       f1 <- data$V1 ~ data$V2 + data$V3 - 1
       f2 <- data$V1 ~ data$V2 + data$V3

       data <- read.csv("path", header=FALSE)
       for (formula in c(f1, f2)) {
         model <- glm(formula, family="poisson", data=data)
         print(as.vector(coef(model)))
       }

       [1] 0.22999393 0.08047088
       [1] 0.25022353 0.21998599 0.05998621

       data <- read.csv("path", header=FALSE)
       for (formula in c(f1, f2)) {
         model <- glm(formula, family=poisson(link=identity), data=data)
         print(as.vector(coef(model)))
       }

       [1] 2.2929501 0.8119415
       [1] 2.5012730 2.1999407 0.5999107

       data <- read.csv("path", header=FALSE)
       for (formula in c(f1, f2)) {
         model <- glm(formula, family=poisson(link=sqrt), data=data)
         print(as.vector(coef(model)))
       }

       [1] 2.2958947 0.8090515
       [1] 2.5000480 2.1999972 0.5999968
     */
    val expected = Seq(
      Vectors.dense(0.0, 0.22999393, 0.08047088),
      Vectors.dense(0.25022353, 0.21998599, 0.05998621),
      Vectors.dense(0.0, 2.2929501, 0.8119415),
      Vectors.dense(2.5012730, 2.1999407, 0.5999107),
      Vectors.dense(0.0, 2.2958947, 0.8090515),
      Vectors.dense(2.5000480, 2.1999972, 0.5999968))

    import GeneralizedLinearRegression._

    var idx = 0
    for ((link, dataset) <- Seq(("log", datasetPoissonLog), ("identity", datasetPoissonIdentity),
      ("sqrt", datasetPoissonSqrt))) {
      for (fitIntercept <- Seq(false, true)) {
        val trainer = new GeneralizedLinearRegression().setFamily("poisson").setLink(link)
          .setFitIntercept(fitIntercept).setLinkPredictionCol("linkPrediction").setTol(1e-3)
        val model = trainer.fit(dataset)
        val actual = Vectors.dense(model.intercept, model.coefficients(0), model.coefficients(1))
        assert(actual ~= expected(idx) absTol 1e-4, "Model mismatch: GLM with poisson family, " +
          s"$link link and fitIntercept = $fitIntercept.")

        val familyLink = FamilyAndLink(trainer)
        testTransformer[(Double, Vector)](dataset, model,
          "features", "prediction", "linkPrediction") {
            case Row(features: DenseVector, prediction1: Double, linkPrediction1: Double) =>
              val eta = BLAS.dot(features, model.coefficients) + model.intercept
              val prediction2 = familyLink.fitted(eta)
              val linkPrediction2 = eta
              assert(prediction1 ~= prediction2 relTol 1E-5, "Prediction mismatch: GLM with " +
                s"poisson family, $link link and fitIntercept = $fitIntercept.")
              assert(linkPrediction1 ~= linkPrediction2 relTol 1E-5, "Link Prediction mismatch: " +
                s"GLM with poisson family, $link link and fitIntercept = $fitIntercept.")
        }

        idx += 1
      }
    }
  }

  test("generalized linear regression: poisson family against glm (with zero values)") {
    /*
       R code:
       f1 <- data$V1 ~ data$V2 + data$V3 - 1
       f2 <- data$V1 ~ data$V2 + data$V3

       data <- read.csv("path", header=FALSE)
       for (formula in c(f1, f2)) {
         model <- glm(formula, family="poisson", data=data)
         print(as.vector(coef(model)))
       }
       [1] -0.0457441 -0.6833928
       [1] 1.8121235  -0.1747493  -0.5815417

       R code for deviance calculation:
       data = cbind(y=c(0,1,0,0,0,1), x1=c(18, 12, 15, 13, 15, 16), x2=c(1,0,0,2,1,1))
       summary(glm(y~x1+x2, family=poisson, data=data.frame(data)))$deviance
       [1] 3.70055
       summary(glm(y~x1+x2-1, family=poisson, data=data.frame(data)))$deviance
       [1] 3.809296
     */
    val expected = Seq(
      Vectors.dense(0.0, -0.0457441, -0.6833928),
      Vectors.dense(1.8121235, -0.1747493, -0.5815417))

    val residualDeviancesR = Array(3.809296, 3.70055)

    var idx = 0
    val link = "log"
    val dataset = datasetPoissonLogWithZero
    for (fitIntercept <- Seq(false, true)) {
      val trainer = new GeneralizedLinearRegression().setFamily("poisson").setLink(link)
        .setFitIntercept(fitIntercept).setLinkPredictionCol("linkPrediction").setTol(1e-3)
      val model = trainer.fit(dataset)
      val actual = Vectors.dense(model.intercept, model.coefficients(0), model.coefficients(1))
      assert(actual ~= expected(idx) absTol 1e-4, "Model mismatch: GLM with poisson family, " +
        s"$link link and fitIntercept = $fitIntercept (with zero values).")
      assert(model.summary.deviance ~== residualDeviancesR(idx) absTol 1E-3)
      idx += 1
    }
  }

  test("generalized linear regression: gamma family against glm") {
    /*
       R code:
       f1 <- data$V1 ~ data$V2 + data$V3 - 1
       f2 <- data$V1 ~ data$V2 + data$V3

       data <- read.csv("path", header=FALSE)
       for (formula in c(f1, f2)) {
         model <- glm(formula, family="Gamma", data=data)
         print(as.vector(coef(model)))
       }

       [1] 2.3392419 0.8058058
       [1] 2.3507700 2.2533574 0.6042991

       data <- read.csv("path", header=FALSE)
       for (formula in c(f1, f2)) {
         model <- glm(formula, family=Gamma(link=identity), data=data)
         print(as.vector(coef(model)))
       }

       [1] 2.2908883 0.8147796
       [1] 2.5002406 2.1998346 0.6000059

       data <- read.csv("path", header=FALSE)
       for (formula in c(f1, f2)) {
         model <- glm(formula, family=Gamma(link=log), data=data)
         print(as.vector(coef(model)))
       }

       [1] 0.22958970 0.08091066
       [1] 0.25003210 0.21996957 0.06000215
     */
    val expected = Seq(
      Vectors.dense(0.0, 2.3392419, 0.8058058),
      Vectors.dense(2.3507700, 2.2533574, 0.6042991),
      Vectors.dense(0.0, 2.2908883, 0.8147796),
      Vectors.dense(2.5002406, 2.1998346, 0.6000059),
      Vectors.dense(0.0, 0.22958970, 0.08091066),
      Vectors.dense(0.25003210, 0.21996957, 0.06000215))

    import GeneralizedLinearRegression._

    var idx = 0
    for ((link, dataset) <- Seq(("inverse", datasetGammaInverse),
      ("identity", datasetGammaIdentity), ("log", datasetGammaLog))) {
      for (fitIntercept <- Seq(false, true)) {
        val trainer = new GeneralizedLinearRegression().setFamily("Gamma").setLink(link)
          .setFitIntercept(fitIntercept).setLinkPredictionCol("linkPrediction").setTol(1e-3)
        val model = trainer.fit(dataset)
        val actual = Vectors.dense(model.intercept, model.coefficients(0), model.coefficients(1))
        assert(actual ~= expected(idx) absTol 1e-4, "Model mismatch: GLM with gamma family, " +
          s"$link link and fitIntercept = $fitIntercept.")

        val familyLink = FamilyAndLink(trainer)
        testTransformer[(Double, Vector)](dataset, model,
          "features", "prediction", "linkPrediction") {
            case Row(features: DenseVector, prediction1: Double, linkPrediction1: Double) =>
              val eta = BLAS.dot(features, model.coefficients) + model.intercept
              val prediction2 = familyLink.fitted(eta)
              val linkPrediction2 = eta
              assert(prediction1 ~= prediction2 relTol 1E-5, "Prediction mismatch: GLM with " +
                s"gamma family, $link link and fitIntercept = $fitIntercept.")
              assert(linkPrediction1 ~= linkPrediction2 relTol 1E-5, "Link Prediction mismatch: " +
                s"GLM with gamma family, $link link and fitIntercept = $fitIntercept.")
        }

        idx += 1
      }
    }
  }

  test("generalized linear regression: tweedie family against glm") {
    /*
      R code:
      library(statmod)
      df <- as.data.frame(matrix(c(
        1.0, 1.0, 0.0, 5.0,
        0.5, 1.0, 1.0, 2.0,
        1.0, 1.0, 2.0, 1.0,
        2.0, 1.0, 3.0, 3.0), 4, 4, byrow = TRUE))

      f1 <- V1 ~ -1 + V3 + V4
      f2 <- V1 ~ V3 + V4

      for (f in c(f1, f2)) {
        for (lp in c(0, 1, -1))
          for (vp in c(1.6, 2.5)) {
            model <- glm(f, df, family = tweedie(var.power = vp, link.power = lp))
            print(as.vector(coef(model)))
          }
      }
      [1] 0.1496480 -0.0122283
      [1] 0.1373567 -0.0120673
      [1] 0.3919109 0.1846094
      [1] 0.3684426 0.1810662
      [1] 0.1759887 0.2195818
      [1] 0.1108561 0.2059430
      [1] -1.3163732  0.4378139  0.2464114
      [1] -1.4396020  0.4817364  0.2680088
      [1] -0.7090230  0.6256309  0.3294324
      [1] -0.9524928  0.7304267  0.3792687
      [1] 2.1188978 -0.3360519 -0.2067023
      [1] 2.1659028 -0.3499170 -0.2128286
    */
    val datasetTweedie = Seq(
      Instance(1.0, 1.0, Vectors.dense(0.0, 5.0)),
      Instance(0.5, 1.0, Vectors.dense(1.0, 2.0)),
      Instance(1.0, 1.0, Vectors.dense(2.0, 1.0)),
      Instance(2.0, 1.0, Vectors.dense(3.0, 3.0))
    ).toDF()

    val expected = Seq(
      Vectors.dense(0, 0.149648, -0.0122283),
      Vectors.dense(0, 0.1373567, -0.0120673),
      Vectors.dense(0, 0.3919109, 0.1846094),
      Vectors.dense(0, 0.3684426, 0.1810662),
      Vectors.dense(0, 0.1759887, 0.2195818),
      Vectors.dense(0, 0.1108561, 0.205943),
      Vectors.dense(-1.3163732, 0.4378139, 0.2464114),
      Vectors.dense(-1.439602, 0.4817364, 0.2680088),
      Vectors.dense(-0.709023, 0.6256309, 0.3294324),
      Vectors.dense(-0.9524928, 0.7304267, 0.3792687),
      Vectors.dense(2.1188978, -0.3360519, -0.2067023),
      Vectors.dense(2.1659028, -0.349917, -0.2128286))

    import GeneralizedLinearRegression._

    var idx = 0
    for (fitIntercept <- Seq(false, true);
         linkPower <- Seq(0.0, 1.0, -1.0);
         variancePower <- Seq(1.6, 2.5)) {
      val trainer = new GeneralizedLinearRegression().setFamily("tweedie")
        .setFitIntercept(fitIntercept).setLinkPredictionCol("linkPrediction")
        .setVariancePower(variancePower).setLinkPower(linkPower).setTol(1e-4)
      val model = trainer.fit(datasetTweedie)
      val actual = Vectors.dense(model.intercept, model.coefficients(0), model.coefficients(1))
      assert(actual ~= expected(idx) absTol 1e-4, "Model mismatch: GLM with tweedie family, " +
        s"linkPower = $linkPower, fitIntercept = $fitIntercept " +
        s"and variancePower = $variancePower.")

      val familyLink = FamilyAndLink(trainer)
      testTransformer[(Double, Double, Vector)](datasetTweedie, model,
        "features", "prediction", "linkPrediction") {
          case Row(features: DenseVector, prediction1: Double, linkPrediction1: Double) =>
            val eta = BLAS.dot(features, model.coefficients) + model.intercept
            val prediction2 = familyLink.fitted(eta)
            val linkPrediction2 = eta
            assert(prediction1 ~= prediction2 relTol 1E-5, "Prediction mismatch: GLM with " +
              s"tweedie family, linkPower = $linkPower, fitIntercept = $fitIntercept " +
              s"and variancePower = $variancePower.")
            assert(linkPrediction1 ~= linkPrediction2 relTol 1E-5, "Link Prediction mismatch: " +
              s"GLM with tweedie family, linkPower = $linkPower, fitIntercept = $fitIntercept " +
              s"and variancePower = $variancePower.")
      }

      idx += 1
    }
  }

  test("generalized linear regression: tweedie family against glm (default power link)") {
    /*
      R code:
      library(statmod)
      df <- as.data.frame(matrix(c(
        1.0, 1.0, 0.0, 5.0,
        0.5, 1.0, 1.0, 2.0,
        1.0, 1.0, 2.0, 1.0,
        2.0, 1.0, 3.0, 3.0), 4, 4, byrow = TRUE))
      var.power <- c(0, 1, 2, 1.5)
      f1 <- V1 ~ -1 + V3 + V4
      f2 <- V1 ~ V3 + V4
      for (f in c(f1, f2)) {
        for (vp in var.power) {
          model <- glm(f, df, family = tweedie(var.power = vp))
          print(as.vector(coef(model)))
        }
      }
      [1] 0.4310345 0.1896552
      [1] 0.15776482 -0.01189032
      [1] 0.1468853 0.2116519
      [1] 0.2282601 0.2132775
      [1] -0.5158730  0.5555556  0.2936508
      [1] -1.2689559  0.4230934  0.2388465
      [1] 2.137852 -0.341431 -0.209090
      [1] 1.5953393 -0.1884985 -0.1106335
    */
    val datasetTweedie = Seq(
      Instance(1.0, 1.0, Vectors.dense(0.0, 5.0)),
      Instance(0.5, 1.0, Vectors.dense(1.0, 2.0)),
      Instance(1.0, 1.0, Vectors.dense(2.0, 1.0)),
      Instance(2.0, 1.0, Vectors.dense(3.0, 3.0))
    ).toDF()

    val expected = Seq(
      Vectors.dense(0, 0.4310345, 0.1896552),
      Vectors.dense(0, 0.15776482, -0.01189032),
      Vectors.dense(0, 0.1468853, 0.2116519),
      Vectors.dense(0, 0.2282601, 0.2132775),
      Vectors.dense(-0.515873, 0.5555556, 0.2936508),
      Vectors.dense(-1.2689559, 0.4230934, 0.2388465),
      Vectors.dense(2.137852, -0.341431, -0.20909),
      Vectors.dense(1.5953393, -0.1884985, -0.1106335))

    import GeneralizedLinearRegression._

    var idx = 0
    for (fitIntercept <- Seq(false, true)) {
      for (variancePower <- Seq(0.0, 1.0, 2.0, 1.5)) {
        val trainer = new GeneralizedLinearRegression().setFamily("tweedie")
          .setFitIntercept(fitIntercept).setLinkPredictionCol("linkPrediction")
          .setVariancePower(variancePower).setTol(1e-3)
        val model = trainer.fit(datasetTweedie)
        val actual = Vectors.dense(model.intercept, model.coefficients(0), model.coefficients(1))
        assert(actual ~= expected(idx) absTol 1e-4, "Model mismatch: GLM with tweedie family, " +
          s"fitIntercept = $fitIntercept and variancePower = $variancePower.")

        val familyLink = FamilyAndLink(trainer)
        testTransformer[(Double, Double, Vector)](datasetTweedie, model,
          "features", "prediction", "linkPrediction") {
            case Row(features: DenseVector, prediction1: Double, linkPrediction1: Double) =>
              val eta = BLAS.dot(features, model.coefficients) + model.intercept
              val prediction2 = familyLink.fitted(eta)
              val linkPrediction2 = eta
              assert(prediction1 ~= prediction2 relTol 1E-5, "Prediction mismatch: GLM with " +
                s"tweedie family, fitIntercept = $fitIntercept " +
                s"and variancePower = $variancePower.")
              assert(linkPrediction1 ~= linkPrediction2 relTol 1E-5, "Link Prediction mismatch: " +
                s"GLM with tweedie family, fitIntercept = $fitIntercept " +
                s"and variancePower = $variancePower.")
        }

        idx += 1
      }
    }
  }

  test("generalized linear regression: intercept only") {
    /*
      R code:

      library(statmod)
      y <- c(1.0, 0.5, 0.7, 0.3)
      w <- c(1, 2, 3, 4)
      for (fam in list(binomial(), Gamma(), gaussian(), poisson(), tweedie(1.6))) {
        model1 <- glm(y ~ 1, family = fam)
        model2 <- glm(y ~ 1, family = fam, weights = w)
        print(as.vector(c(coef(model1), coef(model2))))
      }
      [1] 0.5108256 0.1201443
      [1] 1.600000 1.886792
      [1] 0.625 0.530
      [1] -0.4700036 -0.6348783
      [1] 1.325782 1.463641
     */

    val dataset = Seq(
      Instance(1.0, 1.0, Vectors.zeros(0)),
      Instance(0.5, 2.0, Vectors.zeros(0)),
      Instance(0.7, 3.0, Vectors.zeros(0)),
      Instance(0.3, 4.0, Vectors.zeros(0))
    ).toDF()

    val expected = Seq(0.5108256, 0.1201443, 1.600000, 1.886792, 0.625, 0.530,
      -0.4700036, -0.6348783, 1.325782, 1.463641)

    var idx = 0
    for (family <- GeneralizedLinearRegression.supportedFamilyNames.sortWith(_ < _)) {
      for (useWeight <- Seq(false, true)) {
        val trainer = new GeneralizedLinearRegression().setFamily(family)
        if (useWeight) trainer.setWeightCol("weight")
        if (family == "tweedie") trainer.setVariancePower(1.6)
        val model = trainer.fit(dataset)
        val actual = model.intercept
        assert(actual ~== expected(idx) absTol 1E-3, "Model mismatch: intercept only GLM with " +
          s"useWeight = $useWeight and family = $family.")
        assert(model.coefficients === new DenseVector(Array.empty[Double]))
        idx += 1
      }
    }

    // throw exception for empty model
    val trainer = new GeneralizedLinearRegression().setFitIntercept(false)
    withClue("Specified model is empty with neither intercept nor feature") {
      intercept[IllegalArgumentException] {
        trainer.fit(dataset)
      }
    }
  }

  test("generalized linear regression with weight and offset") {
    /*
      R code:
      library(statmod)

      df <- as.data.frame(matrix(c(
        0.2, 1.0, 2.0, 0.0, 5.0,
        0.5, 2.1, 0.5, 1.0, 2.0,
        0.9, 0.4, 1.0, 2.0, 1.0,
        0.7, 0.7, 0.0, 3.0, 3.0), 4, 5, byrow = TRUE))
      families <- list(binomial, Gamma, gaussian, poisson, tweedie(1.5))
      f1 <- V1 ~ -1 + V4 + V5
      f2 <- V1 ~ V4 + V5
      for (f in c(f1, f2)) {
        for (fam in families) {
          model <- glm(f, df, family = fam, weights = V2, offset = V3)
          print(as.vector(coef(model)))
        }
      }
      [1]  0.9419107 -0.6864404
      [1] -0.2869094  0.7857710
      [1]  0.5169222 -0.3344444
      [1]  0.1812436 -0.6568422
      [1] 0.1055254 0.2979113
      [1] -0.2147117  0.9911750 -0.6356096
      [1]  0.3390397 -0.3406099  0.6870259
      [1] -0.05990345  0.53188982 -0.32118415
      [1] -1.5616130  0.6646470 -0.3192581
      [1] 0.3665034 0.1039416 0.1484616
    */
    val dataset = Seq(
      OffsetInstance(0.2, 1.0, 2.0, Vectors.dense(0.0, 5.0)),
      OffsetInstance(0.5, 2.1, 0.5, Vectors.dense(1.0, 2.0)),
      OffsetInstance(0.9, 0.4, 1.0, Vectors.dense(2.0, 1.0)),
      OffsetInstance(0.7, 0.7, 0.0, Vectors.dense(3.0, 3.0))
    ).toDF()

    val expected = Seq(
      Vectors.dense(0, 0.9419107, -0.6864404),
      Vectors.dense(0, -0.2869094, 0.785771),
      Vectors.dense(0, 0.5169222, -0.3344444),
      Vectors.dense(0, 0.1812436, -0.6568422),
      Vectors.dense(0, 0.1055254, 0.2979113),
      Vectors.dense(-0.2147117, 0.991175, -0.6356096),
      Vectors.dense(0.3390397, -0.3406099, 0.6870259),
      Vectors.dense(-0.05990345, 0.53188982, -0.32118415),
      Vectors.dense(-1.561613, 0.664647, -0.3192581),
      Vectors.dense(0.3665034, 0.1039416, 0.1484616))

    import GeneralizedLinearRegression._

    var idx = 0
    for (fitIntercept <- Seq(false, true)) {
      for (family <- GeneralizedLinearRegression.supportedFamilyNames.sortWith(_ < _)) {
        val trainer = new GeneralizedLinearRegression().setFamily(family)
          .setFitIntercept(fitIntercept).setOffsetCol("offset")
          .setWeightCol("weight").setLinkPredictionCol("linkPrediction")
        if (family == "tweedie") trainer.setVariancePower(1.5)
        val model = trainer.fit(dataset)
        val actual = Vectors.dense(model.intercept, model.coefficients(0), model.coefficients(1))
        assert(actual ~= expected(idx) absTol 1e-4, s"Model mismatch: GLM with family = $family," +
          s" and fitIntercept = $fitIntercept.")

        val familyLink = FamilyAndLink(trainer)
        testTransformer[(Double, Double, Double, Vector)](dataset, model,
          "features", "offset", "prediction", "linkPrediction") {
          case Row(features: DenseVector, offset: Double, prediction1: Double,
          linkPrediction1: Double) =>
            val eta = BLAS.dot(features, model.coefficients) + model.intercept + offset
            val prediction2 = familyLink.fitted(eta)
            val linkPrediction2 = eta
            assert(prediction1 ~= prediction2 relTol 1E-5, "Prediction mismatch: GLM with " +
              s"family = $family, and fitIntercept = $fitIntercept.")
            assert(linkPrediction1 ~= linkPrediction2 relTol 1E-5, "Link Prediction mismatch: " +
              s"GLM with family = $family, and fitIntercept = $fitIntercept.")
        }

        idx += 1
      }
    }
  }

  test("glm summary: gaussian family with weight and offset") {
    /*
      R code:

      A <- matrix(c(0, 1, 2, 3, 5, 7, 11, 13), 4, 2)
      b <- c(17, 19, 23, 29)
      w <- c(1, 2, 3, 4)
      off <- c(2, 3, 1, 4)
      df <- as.data.frame(cbind(A, b))
     */
    val dataset = Seq(
      OffsetInstance(17.0, 1.0, 2.0, Vectors.dense(0.0, 5.0).toSparse),
      OffsetInstance(19.0, 2.0, 3.0, Vectors.dense(1.0, 7.0)),
      OffsetInstance(23.0, 3.0, 1.0, Vectors.dense(2.0, 11.0)),
      OffsetInstance(29.0, 4.0, 4.0, Vectors.dense(3.0, 13.0))
    ).toDF()
    /*
      R code:

      model <- glm(formula = "b ~ .", family = "gaussian", data = df,
                   weights = w, offset = off)
      summary(model)

      Deviance Residuals:
            1        2        3        4
       0.9600  -0.6788  -0.5543   0.4800

      Coefficients:
                  Estimate Std. Error t value Pr(>|t|)
      (Intercept)   5.5400     4.8040   1.153    0.455
      V1           -0.9600     2.7782  -0.346    0.788
      V2            1.7000     0.9798   1.735    0.333

      (Dispersion parameter for gaussian family taken to be 1.92)

          Null deviance: 152.10  on 3  degrees of freedom
      Residual deviance:   1.92  on 1  degrees of freedom
      AIC: 13.238

      Number of Fisher Scoring iterations: 2

      residuals(model, type = "pearson")
               1          2          3          4
      0.9600000 -0.6788225 -0.5542563  0.4800000
      residuals(model, type = "working")
          1     2     3     4
      0.96 -0.48 -0.32  0.24
      residuals(model, type = "response")
          1     2     3     4
      0.96 -0.48 -0.32  0.24
     */
    val trainer = new GeneralizedLinearRegression()
      .setWeightCol("weight").setOffsetCol("offset")

    val model = trainer.fit(dataset)

    val coefficientsR = Vectors.dense(Array(-0.96, 1.7))
    val interceptR = 5.54
    val devianceResidualsR = Array(0.96, -0.67882, -0.55426, 0.48)
    val pearsonResidualsR = Array(0.96, -0.67882, -0.55426, 0.48)
    val workingResidualsR = Array(0.96, -0.48, -0.32, 0.24)
    val responseResidualsR = Array(0.96, -0.48, -0.32, 0.24)
    val seCoefR = Array(2.7782, 0.9798, 4.804)
    val tValsR = Array(-0.34555, 1.73506, 1.15321)
    val pValsR = Array(0.78819, 0.33286, 0.45478)
    val dispersionR = 1.92
    val nullDevianceR = 152.1
    val residualDevianceR = 1.92
    val residualDegreeOfFreedomNullR = 3
    val residualDegreeOfFreedomR = 1
    val aicR = 13.23758

    assert(model.hasSummary)
    val summary = model.summary
    assert(summary.isInstanceOf[GeneralizedLinearRegressionTrainingSummary])

    val devianceResiduals = summary.residuals()
      .select(col("devianceResiduals"))
      .collect()
      .map(_.getDouble(0))
    val pearsonResiduals = summary.residuals("pearson")
      .select(col("pearsonResiduals"))
      .collect()
      .map(_.getDouble(0))
    val workingResiduals = summary.residuals("working")
      .select(col("workingResiduals"))
      .collect()
      .map(_.getDouble(0))
    val responseResiduals = summary.residuals("response")
      .select(col("responseResiduals"))
      .collect()
      .map(_.getDouble(0))

    assert(model.coefficients ~== coefficientsR absTol 1E-3)
    assert(model.intercept ~== interceptR absTol 1E-3)
    devianceResiduals.zip(devianceResidualsR).foreach { x =>
      assert(x._1 ~== x._2 absTol 1E-3) }
    pearsonResiduals.zip(pearsonResidualsR).foreach { x =>
      assert(x._1 ~== x._2 absTol 1E-3) }
    workingResiduals.zip(workingResidualsR).foreach { x =>
      assert(x._1 ~== x._2 absTol 1E-3) }
    responseResiduals.zip(responseResidualsR).foreach { x =>
      assert(x._1 ~== x._2 absTol 1E-3) }
    summary.coefficientStandardErrors.zip(seCoefR).foreach{ x =>
      assert(x._1 ~== x._2 absTol 1E-3) }
    summary.tValues.zip(tValsR).foreach{ x => assert(x._1 ~== x._2 absTol 1E-3) }
    summary.pValues.zip(pValsR).foreach{ x => assert(x._1 ~== x._2 absTol 1E-3) }
    assert(summary.dispersion ~== dispersionR absTol 1E-3)
    assert(summary.nullDeviance ~== nullDevianceR absTol 1E-3)
    assert(summary.deviance ~== residualDevianceR absTol 1E-3)
    assert(summary.residualDegreeOfFreedom === residualDegreeOfFreedomR)
    assert(summary.residualDegreeOfFreedomNull === residualDegreeOfFreedomNullR)
    assert(summary.aic ~== aicR absTol 1E-3)
    assert(summary.solver === "irls")

    val summary2: GeneralizedLinearRegressionSummary = model.evaluate(dataset)
    assert(summary.predictions.columns.toSet === summary2.predictions.columns.toSet)
    assert(summary.predictionCol === summary2.predictionCol)
    assert(summary.rank === summary2.rank)
    assert(summary.degreesOfFreedom === summary2.degreesOfFreedom)
    assert(summary.residualDegreeOfFreedom === summary2.residualDegreeOfFreedom)
    assert(summary.residualDegreeOfFreedomNull === summary2.residualDegreeOfFreedomNull)
    assert(summary.nullDeviance === summary2.nullDeviance)
    assert(summary.deviance === summary2.deviance)
    assert(summary.dispersion === summary2.dispersion)
    assert(summary.aic === summary2.aic)
  }

  test("glm summary: binomial family with weight and offset") {
    /*
      R code:

      df <- as.data.frame(matrix(c(
          0.2, 1.0, 2.0, 0.0, 5.0,
          0.5, 2.1, 0.5, 1.0, 2.0,
          0.9, 0.4, 1.0, 2.0, 1.0,
          0.7, 0.7, 0.0, 3.0, 3.0), 4, 5, byrow = TRUE))
     */
    val dataset = Seq(
      OffsetInstance(0.2, 1.0, 2.0, Vectors.dense(0.0, 5.0)),
      OffsetInstance(0.5, 2.1, 0.5, Vectors.dense(1.0, 2.0)),
      OffsetInstance(0.9, 0.4, 1.0, Vectors.dense(2.0, 1.0)),
      OffsetInstance(0.7, 0.7, 0.0, Vectors.dense(3.0, 3.0))
    ).toDF()
    /*
      R code:

      model <- glm(formula = "V1 ~ V4 + V5", family = "binomial", data = df,
                   weights = V2, offset = V3)
      summary(model)

      Deviance Residuals:
              1          2          3          4
       0.002584  -0.003800   0.012478  -0.001796

      Coefficients:
                  Estimate Std. Error z value Pr(>|z|)
      (Intercept)  -0.2147     3.5687  -0.060    0.952
      V4            0.9912     1.2344   0.803    0.422
      V5           -0.6356     0.9669  -0.657    0.511

      (Dispersion parameter for binomial family taken to be 1)

          Null deviance: 2.17560881  on 3  degrees of freedom
      Residual deviance: 0.00018005  on 1  degrees of freedom
      AIC: 10.245

      Number of Fisher Scoring iterations: 4

      residuals(model, type = "pearson")
                 1            2            3            4
      0.002586113 -0.003799744  0.012372235 -0.001796892
      residuals(model, type = "working")
                 1            2            3            4
      0.006477857 -0.005244163  0.063541250 -0.004691064
      residuals(model, type = "response")
                  1             2             3             4
      0.0010324375 -0.0013110318  0.0060225522 -0.0009832738
    */
    val trainer = new GeneralizedLinearRegression()
      .setFamily("Binomial")
      .setWeightCol("weight")
      .setOffsetCol("offset")

    val model = trainer.fit(dataset)

    val coefficientsR = Vectors.dense(Array(0.99117, -0.63561))
    val interceptR = -0.21471
    val devianceResidualsR = Array(0.00258, -0.0038, 0.01248, -0.0018)
    val pearsonResidualsR = Array(0.00259, -0.0038, 0.01237, -0.0018)
    val workingResidualsR = Array(0.00648, -0.00524, 0.06354, -0.00469)
    val responseResidualsR = Array(0.00103, -0.00131, 0.00602, -0.00098)
    val seCoefR = Array(1.23439, 0.9669, 3.56866)
    val tValsR = Array(0.80297, -0.65737, -0.06017)
    val pValsR = Array(0.42199, 0.51094, 0.95202)
    val dispersionR = 1.0
    val nullDevianceR = 2.17561
    val residualDevianceR = 0.00018
    val residualDegreeOfFreedomNullR = 3
    val residualDegreeOfFreedomR = 1
    val aicR = 10.24453

    val summary = model.summary
    val devianceResiduals = summary.residuals()
      .select(col("devianceResiduals"))
      .collect()
      .map(_.getDouble(0))
    val pearsonResiduals = summary.residuals("pearson")
      .select(col("pearsonResiduals"))
      .collect()
      .map(_.getDouble(0))
    val workingResiduals = summary.residuals("working")
      .select(col("workingResiduals"))
      .collect()
      .map(_.getDouble(0))
    val responseResiduals = summary.residuals("response")
      .select(col("responseResiduals"))
      .collect()
      .map(_.getDouble(0))

    assert(model.coefficients ~== coefficientsR absTol 1E-3)
    assert(model.intercept ~== interceptR absTol 1E-3)
    devianceResiduals.zip(devianceResidualsR).foreach { x =>
      assert(x._1 ~== x._2 absTol 1E-3) }
    pearsonResiduals.zip(pearsonResidualsR).foreach { x =>
      assert(x._1 ~== x._2 absTol 1E-3) }
    workingResiduals.zip(workingResidualsR).foreach { x =>
      assert(x._1 ~== x._2 absTol 1E-3) }
    responseResiduals.zip(responseResidualsR).foreach { x =>
      assert(x._1 ~== x._2 absTol 1E-3) }
    summary.coefficientStandardErrors.zip(seCoefR).foreach{ x =>
      assert(x._1 ~== x._2 absTol 1E-3) }
    summary.tValues.zip(tValsR).foreach{ x => assert(x._1 ~== x._2 absTol 1E-3) }
    summary.pValues.zip(pValsR).foreach{ x => assert(x._1 ~== x._2 absTol 1E-3) }
    assert(summary.dispersion === dispersionR)
    assert(summary.nullDeviance ~== nullDevianceR absTol 1E-3)
    assert(summary.deviance ~== residualDevianceR absTol 1E-3)
    assert(summary.residualDegreeOfFreedom === residualDegreeOfFreedomR)
    assert(summary.residualDegreeOfFreedomNull === residualDegreeOfFreedomNullR)
    assert(summary.aic ~== aicR absTol 1E-3)
    assert(summary.solver === "irls")
  }

  test("glm summary: poisson family with weight and offset") {
    /*
      R code:

      A <- matrix(c(0, 1, 2, 3, 5, 7, 11, 13), 4, 2)
      b <- c(2, 8, 3, 9)
      w <- c(1, 2, 3, 4)
      off <- c(2, 3, 1, 4)
      df <- as.data.frame(cbind(A, b))
     */
    val dataset = Seq(
      OffsetInstance(2.0, 1.0, 2.0, Vectors.dense(0.0, 5.0).toSparse),
      OffsetInstance(8.0, 2.0, 3.0, Vectors.dense(1.0, 7.0)),
      OffsetInstance(3.0, 3.0, 1.0, Vectors.dense(2.0, 11.0)),
      OffsetInstance(9.0, 4.0, 4.0, Vectors.dense(3.0, 13.0))
    ).toDF()
    /*
      R code:

      model <- glm(formula = "b ~ .", family = "poisson", data = df,
                   weights = w, offset = off)
      summary(model)

      Deviance Residuals:
            1        2        3        4
      -2.0480   1.2315   1.8293  -0.7107

      Coefficients:
                  Estimate Std. Error z value Pr(>|z|)
      (Intercept)  -4.5678     1.9625  -2.328   0.0199
      V1           -2.8784     1.1683  -2.464   0.0137
      V2            0.8859     0.4170   2.124   0.0336

      (Dispersion parameter for poisson family taken to be 1)

          Null deviance: 22.5585  on 3  degrees of freedom
      Residual deviance:  9.5622  on 1  degrees of freedom
      AIC: 51.242

      Number of Fisher Scoring iterations: 5

      residuals(model, type = "pearson")
               1          2          3          4
      -1.7480418  1.3037611  2.0750099 -0.6972966
      residuals(model, type = "working")
               1          2          3          4
      -0.6891489  0.3833588  0.9710682 -0.1096590
      residuals(model, type = "response")
              1         2         3         4
      -4.433948  2.216974  1.477983 -1.108487
     */
    val trainer = new GeneralizedLinearRegression()
      .setFamily("Poisson")
      .setWeightCol("weight")
      .setOffsetCol("offset")

    val model = trainer.fit(dataset)

    val coefficientsR = Vectors.dense(Array(-2.87843, 0.88589))
    val interceptR = -4.56784
    val devianceResidualsR = Array(-2.04796, 1.23149, 1.82933, -0.71066)
    val pearsonResidualsR = Array(-1.74804, 1.30376, 2.07501, -0.6973)
    val workingResidualsR = Array(-0.68915, 0.38336, 0.97107, -0.10966)
    val responseResidualsR = Array(-4.43395, 2.21697, 1.47798, -1.10849)
    val seCoefR = Array(1.16826, 0.41703, 1.96249)
    val tValsR = Array(-2.46387, 2.12428, -2.32757)
    val pValsR = Array(0.01374, 0.03365, 0.01993)
    val dispersionR = 1.0
    val nullDevianceR = 22.55853
    val residualDevianceR = 9.5622
    val residualDegreeOfFreedomNullR = 3
    val residualDegreeOfFreedomR = 1
    val aicR = 51.24218

    val summary = model.summary
    val devianceResiduals = summary.residuals()
      .select(col("devianceResiduals"))
      .collect()
      .map(_.getDouble(0))
    val pearsonResiduals = summary.residuals("pearson")
      .select(col("pearsonResiduals"))
      .collect()
      .map(_.getDouble(0))
    val workingResiduals = summary.residuals("working")
      .select(col("workingResiduals"))
      .collect()
      .map(_.getDouble(0))
    val responseResiduals = summary.residuals("response")
      .select(col("responseResiduals"))
      .collect()
      .map(_.getDouble(0))

    assert(model.coefficients ~== coefficientsR absTol 1E-3)
    assert(model.intercept ~== interceptR absTol 1E-3)
    devianceResiduals.zip(devianceResidualsR).foreach { x =>
      assert(x._1 ~== x._2 absTol 1E-3) }
    pearsonResiduals.zip(pearsonResidualsR).foreach { x =>
      assert(x._1 ~== x._2 absTol 1E-3) }
    workingResiduals.zip(workingResidualsR).foreach { x =>
      assert(x._1 ~== x._2 absTol 1E-3) }
    responseResiduals.zip(responseResidualsR).foreach { x =>
      assert(x._1 ~== x._2 absTol 1E-3) }
    summary.coefficientStandardErrors.zip(seCoefR).foreach{ x =>
      assert(x._1 ~== x._2 absTol 1E-3) }
    summary.tValues.zip(tValsR).foreach{ x => assert(x._1 ~== x._2 absTol 1E-3) }
    summary.pValues.zip(pValsR).foreach{ x => assert(x._1 ~== x._2 absTol 1E-3) }
    assert(summary.dispersion === dispersionR)
    assert(summary.nullDeviance ~== nullDevianceR absTol 1E-3)
    assert(summary.deviance ~== residualDevianceR absTol 1E-3)
    assert(summary.residualDegreeOfFreedom === residualDegreeOfFreedomR)
    assert(summary.residualDegreeOfFreedomNull === residualDegreeOfFreedomNullR)
    assert(summary.aic ~== aicR absTol 1E-3)
    assert(summary.solver === "irls")
  }

  test("glm summary: gamma family with weight and offset") {
    /*
      R code:

      A <- matrix(c(0, 5, 1, 2, 2, 1, 3, 3), 4, 2, byrow = TRUE)
      b <- c(1, 2, 1, 2)
      w <- c(1, 2, 3, 4)
      off <- c(0, 0.5, 1, 0)
      df <- as.data.frame(cbind(A, b))
     */
    val dataset = Seq(
      OffsetInstance(1.0, 1.0, 0.0, Vectors.dense(0.0, 5.0)),
      OffsetInstance(2.0, 2.0, 0.5, Vectors.dense(1.0, 2.0)),
      OffsetInstance(1.0, 3.0, 1.0, Vectors.dense(2.0, 1.0)),
      OffsetInstance(2.0, 4.0, 0.0, Vectors.dense(3.0, 3.0))
    ).toDF()
    /*
      R code:

      model <- glm(formula = "b ~ .", family = "Gamma", data = df,
                   weights = w, offset = off)
      summary(model)

      Deviance Residuals:
             1         2         3         4
      -0.17095   0.19867  -0.23604   0.03241

      Coefficients:
                  Estimate Std. Error t value Pr(>|t|)
      (Intercept) -0.56474    0.23866  -2.366    0.255
      V1           0.07695    0.06931   1.110    0.467
      V2           0.28068    0.07320   3.835    0.162

      (Dispersion parameter for Gamma family taken to be 0.1212174)

          Null deviance: 2.02568  on 3  degrees of freedom
      Residual deviance: 0.12546  on 1  degrees of freedom
      AIC: 0.93388

      Number of Fisher Scoring iterations: 4

      residuals(model, type = "pearson")
                1           2           3           4
      -0.16134949  0.20807694 -0.22544551  0.03258777
      residuals(model, type = "working")
                 1            2            3            4
      0.135315831 -0.084390309  0.113219135 -0.008279688
      residuals(model, type = "response")
               1          2          3          4
      -0.1923918  0.2565224 -0.1496381  0.0320653
     */
    val trainer = new GeneralizedLinearRegression()
      .setFamily("Gamma")
      .setWeightCol("weight")
      .setOffsetCol("offset")

    val model = trainer.fit(dataset)

    val coefficientsR = Vectors.dense(Array(0.07695, 0.28068))
    val interceptR = -0.56474
    val devianceResidualsR = Array(-0.17095, 0.19867, -0.23604, 0.03241)
    val pearsonResidualsR = Array(-0.16135, 0.20808, -0.22545, 0.03259)
    val workingResidualsR = Array(0.13532, -0.08439, 0.11322, -0.00828)
    val responseResidualsR = Array(-0.19239, 0.25652, -0.14964, 0.03207)
    val seCoefR = Array(0.06931, 0.0732, 0.23866)
    val tValsR = Array(1.11031, 3.83453, -2.3663)
    val pValsR = Array(0.46675, 0.16241, 0.25454)
    val dispersionR = 0.12122
    val nullDevianceR = 2.02568
    val residualDevianceR = 0.12546
    val residualDegreeOfFreedomNullR = 3
    val residualDegreeOfFreedomR = 1
    val aicR = 0.93388

    val summary = model.summary
    val devianceResiduals = summary.residuals()
      .select(col("devianceResiduals"))
      .collect()
      .map(_.getDouble(0))
    val pearsonResiduals = summary.residuals("pearson")
      .select(col("pearsonResiduals"))
      .collect()
      .map(_.getDouble(0))
    val workingResiduals = summary.residuals("working")
      .select(col("workingResiduals"))
      .collect()
      .map(_.getDouble(0))
    val responseResiduals = summary.residuals("response")
      .select(col("responseResiduals"))
      .collect()
      .map(_.getDouble(0))

    assert(model.coefficients ~== coefficientsR absTol 1E-3)
    assert(model.intercept ~== interceptR absTol 1E-3)
    devianceResiduals.zip(devianceResidualsR).foreach { x =>
      assert(x._1 ~== x._2 absTol 1E-3) }
    pearsonResiduals.zip(pearsonResidualsR).foreach { x =>
      assert(x._1 ~== x._2 absTol 1E-3) }
    workingResiduals.zip(workingResidualsR).foreach { x =>
      assert(x._1 ~== x._2 absTol 1E-3) }
    responseResiduals.zip(responseResidualsR).foreach { x =>
      assert(x._1 ~== x._2 absTol 1E-3) }
    summary.coefficientStandardErrors.zip(seCoefR).foreach{ x =>
      assert(x._1 ~== x._2 absTol 1E-3) }
    summary.tValues.zip(tValsR).foreach{ x => assert(x._1 ~== x._2 absTol 1E-3) }
    summary.pValues.zip(pValsR).foreach{ x => assert(x._1 ~== x._2 absTol 1E-3) }
    assert(summary.dispersion ~== dispersionR absTol 1E-3)
    assert(summary.nullDeviance ~== nullDevianceR absTol 1E-3)
    assert(summary.deviance ~== residualDevianceR absTol 1E-3)
    assert(summary.residualDegreeOfFreedom === residualDegreeOfFreedomR)
    assert(summary.residualDegreeOfFreedomNull === residualDegreeOfFreedomNullR)
    assert(summary.aic ~== aicR absTol 1E-3)
    assert(summary.solver === "irls")
  }

  test("glm summary: tweedie family with weight and offset") {
    /*
      R code:

      df <- as.data.frame(matrix(c(
        1.0, 1.0, 1.0, 0.0, 5.0,
        0.5, 2.0, 3.0, 1.0, 2.0,
        1.0, 3.0, 2.0, 2.0, 1.0,
        0.0, 4.0, 0.0, 3.0, 3.0), 4, 5, byrow = TRUE))
    */
    val dataset = Seq(
      OffsetInstance(1.0, 1.0, 1.0, Vectors.dense(0.0, 5.0)),
      OffsetInstance(0.5, 2.0, 3.0, Vectors.dense(1.0, 2.0)),
      OffsetInstance(1.0, 3.0, 2.0, Vectors.dense(2.0, 1.0)),
      OffsetInstance(0.0, 4.0, 0.0, Vectors.dense(3.0, 3.0))
    ).toDF()
    /*
      R code:

      library(statmod)
      model <- glm(V1 ~ V4 + V5, data = df, weights = V2, offset = V3,
                   family = tweedie(var.power = 1.6, link.power = 0.0))
      summary(model)

      Deviance Residuals:
            1        2        3        4
       0.8917  -2.1396   1.2252  -1.7946

      Coefficients:
                  Estimate Std. Error t value Pr(>|t|)
      (Intercept) -0.03047    3.65000  -0.008    0.995
      V4          -1.14577    1.41674  -0.809    0.567
      V5          -0.36585    0.97065  -0.377    0.771

      (Dispersion parameter for Tweedie family taken to be 6.334961)

          Null deviance: 12.784  on 3  degrees of freedom
      Residual deviance: 10.095  on 1  degrees of freedom
      AIC: NA

      Number of Fisher Scoring iterations: 18

      residuals(model, type = "pearson")
               1          2          3          4
      1.1472554 -1.4642569  1.4935199 -0.8025842
      residuals(model, type = "working")
               1          2          3          4
      1.3624928 -0.8322375  0.9894580 -1.0000000
      residuals(model, type = "response")
                1           2           3           4
      0.57671828 -2.48040354  0.49735052 -0.01040646
     */
    val trainer = new GeneralizedLinearRegression()
      .setFamily("tweedie")
      .setVariancePower(1.6)
      .setLinkPower(0.0)
      .setWeightCol("weight")
      .setOffsetCol("offset")

    val model = trainer.fit(dataset)

    val coefficientsR = Vectors.dense(Array(-1.14577, -0.36585))
    val interceptR = -0.03047
    val devianceResidualsR = Array(0.89171, -2.13961, 1.2252, -1.79463)
    val pearsonResidualsR = Array(1.14726, -1.46426, 1.49352, -0.80258)
    val workingResidualsR = Array(1.36249, -0.83224, 0.98946, -1)
    val responseResidualsR = Array(0.57672, -2.4804, 0.49735, -0.01041)
    val seCoefR = Array(1.41674, 0.97065, 3.65)
    val tValsR = Array(-0.80873, -0.37691, -0.00835)
    val pValsR = Array(0.56707, 0.77053, 0.99468)
    val dispersionR = 6.33496
    val nullDevianceR = 12.78358
    val residualDevianceR = 10.09488
    val residualDegreeOfFreedomNullR = 3
    val residualDegreeOfFreedomR = 1

    val summary = model.summary

    val devianceResiduals = summary.residuals()
      .select(col("devianceResiduals"))
      .collect()
      .map(_.getDouble(0))
    val pearsonResiduals = summary.residuals("pearson")
      .select(col("pearsonResiduals"))
      .collect()
      .map(_.getDouble(0))
    val workingResiduals = summary.residuals("working")
      .select(col("workingResiduals"))
      .collect()
      .map(_.getDouble(0))
    val responseResiduals = summary.residuals("response")
      .select(col("responseResiduals"))
      .collect()
      .map(_.getDouble(0))

    assert(model.coefficients ~== coefficientsR absTol 1E-3)
    assert(model.intercept ~== interceptR absTol 1E-3)
    devianceResiduals.zip(devianceResidualsR).foreach { x =>
      assert(x._1 ~== x._2 absTol 1E-3) }
    pearsonResiduals.zip(pearsonResidualsR).foreach { x =>
      assert(x._1 ~== x._2 absTol 1E-3) }
    workingResiduals.zip(workingResidualsR).foreach { x =>
      assert(x._1 ~== x._2 absTol 1E-3) }
    responseResiduals.zip(responseResidualsR).foreach { x =>
      assert(x._1 ~== x._2 absTol 1E-3) }

    summary.coefficientStandardErrors.zip(seCoefR).foreach{ x =>
      assert(x._1 ~== x._2 absTol 1E-3) }
    summary.tValues.zip(tValsR).foreach{ x => assert(x._1 ~== x._2 absTol 1E-3) }
    summary.pValues.zip(pValsR).foreach{ x => assert(x._1 ~== x._2 absTol 1E-3) }

    assert(summary.dispersion ~== dispersionR absTol 1E-3)
    assert(summary.nullDeviance ~== nullDevianceR absTol 1E-3)
    assert(summary.deviance ~== residualDevianceR absTol 1E-3)
    assert(summary.residualDegreeOfFreedom === residualDegreeOfFreedomR)
    assert(summary.residualDegreeOfFreedomNull === residualDegreeOfFreedomNullR)
    assert(summary.solver === "irls")
  }

  test("glm handle collinear features") {
    val collinearInstances = Seq(
      Instance(1.0, 1.0, Vectors.dense(1.0, 2.0)),
      Instance(2.0, 1.0, Vectors.dense(2.0, 4.0)),
      Instance(3.0, 1.0, Vectors.dense(3.0, 6.0)),
      Instance(4.0, 1.0, Vectors.dense(4.0, 8.0))
    ).toDF()
    val trainer = new GeneralizedLinearRegression()
    val model = trainer.fit(collinearInstances)
    // to make it clear that underlying WLS did not solve analytically
    intercept[UnsupportedOperationException] {
      model.summary.coefficientStandardErrors
    }
    intercept[UnsupportedOperationException] {
      model.summary.pValues
    }
    intercept[UnsupportedOperationException] {
      model.summary.tValues
    }
  }

  test("read/write") {
    def checkModelData(
        model: GeneralizedLinearRegressionModel,
        model2: GeneralizedLinearRegressionModel): Unit = {
      assert(model.intercept === model2.intercept)
      assert(model.coefficients.toArray === model2.coefficients.toArray)
    }

    val glr = new GeneralizedLinearRegression()
    testEstimatorAndModelReadWrite(glr, datasetPoissonLog,
      GeneralizedLinearRegressionSuite.allParamSettings,
      GeneralizedLinearRegressionSuite.allParamSettings, checkModelData)
  }

  test("should support all NumericType labels and weights, and not support other types") {
    val glr = new GeneralizedLinearRegression().setMaxIter(1)
    MLTestingUtils.checkNumericTypes[
        GeneralizedLinearRegressionModel, GeneralizedLinearRegression](
      glr, spark, isClassification = false) { (expected, actual) =>
        assert(expected.intercept === actual.intercept)
        assert(expected.coefficients === actual.coefficients)
      }
  }

  test("glm accepts Dataset[LabeledPoint]") {
    val context = spark
    import context.implicits._
    new GeneralizedLinearRegression()
      .setFamily("gaussian")
      .fit(datasetGaussianIdentity.as[LabeledPoint])
  }

  test("glm summary: feature name") {
    // dataset1 with no attribute
    val dataset1 = Seq(
      Instance(2.0, 1.0, Vectors.dense(0.0, 5.0)),
      Instance(8.0, 2.0, Vectors.dense(1.0, 7.0)),
      Instance(3.0, 3.0, Vectors.dense(2.0, 11.0)),
      Instance(9.0, 4.0, Vectors.dense(3.0, 13.0)),
      Instance(2.0, 5.0, Vectors.dense(2.0, 3.0))
    ).toDF()

    // dataset2 with attribute
    val datasetTmp = Seq(
      (2.0, 1.0, 0.0, 5.0),
      (8.0, 2.0, 1.0, 7.0),
      (3.0, 3.0, 2.0, 11.0),
      (9.0, 4.0, 3.0, 13.0),
      (2.0, 5.0, 2.0, 3.0)
    ).toDF("y", "w", "x1", "x2")
    val formula = new RFormula().setFormula("y ~ x1 + x2")
    val dataset2 = formula.fit(datasetTmp).transform(datasetTmp)

    val expectedFeature = Seq(Array("features_0", "features_1"), Array("x1", "x2"))

    var idx = 0
    for (dataset <- Seq(dataset1, dataset2)) {
      val model = new GeneralizedLinearRegression().fit(dataset)
      model.summary.featureNames.zip(expectedFeature(idx))
        .foreach{ x => assert(x._1 === x._2) }
      idx += 1
    }
  }

  test("glm summary: coefficient with statistics") {
    /*
      R code:

      A <- matrix(c(0, 1, 2, 3, 2, 5, 7, 11, 13, 3), 5, 2)
      b <- c(2, 8, 3, 9, 2)
      df <- as.data.frame(cbind(A, b))
      model <- glm(formula = "b ~ .",  data = df)
      summary(model)

      Coefficients:
                  Estimate Std. Error t value Pr(>|t|)
      (Intercept)   0.7903     4.0129   0.197    0.862
      V1            0.2258     2.1153   0.107    0.925
      V2            0.4677     0.5815   0.804    0.506
    */
    val dataset = Seq(
      Instance(2.0, 1.0, Vectors.dense(0.0, 5.0)),
      Instance(8.0, 2.0, Vectors.dense(1.0, 7.0)),
      Instance(3.0, 3.0, Vectors.dense(2.0, 11.0)),
      Instance(9.0, 4.0, Vectors.dense(3.0, 13.0)),
      Instance(2.0, 5.0, Vectors.dense(2.0, 3.0))
    ).toDF()

    val expectedFeature = Seq(Array("features_0", "features_1"),
      Array("(Intercept)", "features_0", "features_1"))
    val expectedEstimate = Seq(Vectors.dense(0.2884, 0.538),
      Vectors.dense(0.7903, 0.2258, 0.4677))
    val expectedStdError = Seq(Vectors.dense(1.724, 0.3787),
      Vectors.dense(4.0129, 2.1153, 0.5815))

    var idx = 0
    for (fitIntercept <- Seq(false, true)) {
      val trainer = new GeneralizedLinearRegression()
        .setFamily("gaussian")
        .setFitIntercept(fitIntercept)
      val model = trainer.fit(dataset)
      val coefficientsWithStatistics = model.summary.coefficientsWithStatistics

      coefficientsWithStatistics.map(_._1).zip(expectedFeature(idx)).foreach { x =>
        assert(x._1 === x._2, "Feature name mismatch in coefficientsWithStatistics") }
      assert(Vectors.dense(coefficientsWithStatistics.map(_._2)) ~= expectedEstimate(idx)
        absTol 1E-3, "Coefficients mismatch in coefficientsWithStatistics")
      assert(Vectors.dense(coefficientsWithStatistics.map(_._3)) ~= expectedStdError(idx)
        absTol 1E-3, "Standard error mismatch in coefficientsWithStatistics")
      idx += 1
    }
  }

  test("generalized linear regression: regularization parameter") {
    /*
      R code:

      a1 <- c(0, 1, 2, 3)
      a2 <- c(5, 2, 1, 3)
      b <- c(1, 0, 1, 0)
      data <- as.data.frame(cbind(a1, a2, b))
      df <- suppressWarnings(createDataFrame(data))

      for (regParam in c(0.0, 0.1, 1.0)) {
        model <- spark.glm(df, b ~ a1 + a2, regParam = regParam)
        print(as.vector(summary(model)$aic))
      }

      [1] 12.88188
      [1] 12.92681
      [1] 13.32836
     */
    val dataset = Seq(
      LabeledPoint(1, Vectors.dense(5, 0)),
      LabeledPoint(0, Vectors.dense(2, 1)),
      LabeledPoint(1, Vectors.dense(1, 2)),
      LabeledPoint(0, Vectors.dense(3, 3))
    ).toDF()
    val expected = Seq(12.88188, 12.92681, 13.32836)

    var idx = 0
    for (regParam <- Seq(0.0, 0.1, 1.0)) {
      val trainer = new GeneralizedLinearRegression()
        .setRegParam(regParam)
        .setLabelCol("label")
        .setFeaturesCol("features")
      val model = trainer.fit(dataset)
      val actual = model.summary.aic
      assert(actual ~= expected(idx) absTol 1e-4, "Model mismatch: GLM with regParam = $regParam.")
      idx += 1
    }
  }

  test("evaluate with labels that are not doubles") {
    // Evaluate with a dataset that contains Labels not as doubles to verify correct casting
    val dataset = Seq(
      Instance(17.0, 1.0, Vectors.dense(0.0, 5.0).toSparse),
      Instance(19.0, 1.0, Vectors.dense(1.0, 7.0)),
      Instance(23.0, 1.0, Vectors.dense(2.0, 11.0)),
      Instance(29.0, 1.0, Vectors.dense(3.0, 13.0))
    ).toDF()

    val trainer = new GeneralizedLinearRegression()
      .setMaxIter(1)
    val model = trainer.fit(dataset)
    assert(model.hasSummary)
    val summary = model.summary

    val longLabelDataset = dataset.select(col(model.getLabelCol).cast(FloatType),
      col(model.getFeaturesCol))
    val evalSummary = model.evaluate(longLabelDataset)
    // The calculations below involve pattern matching with Label as a double
    assert(evalSummary.nullDeviance === summary.nullDeviance)
    assert(evalSummary.deviance === summary.deviance)
    assert(evalSummary.aic === summary.aic)
  }

  test("SPARK-23131 Kryo raises StackOverflow during serializing GLR model") {
    val conf = new SparkConf(false)
    val ser = new KryoSerializer(conf).newInstance()
    val trainer = new GeneralizedLinearRegression()
    val model = trainer.fit(Seq(Instance(1.0, 1.0, Vectors.dense(1.0, 7.0))).toDF)
    ser.serialize[GeneralizedLinearRegressionModel](model)
  }
}

object GeneralizedLinearRegressionSuite {

  /**
   * Mapping from all Params to valid settings which differ from the defaults.
   * This is useful for tests which need to exercise all Params, such as save/load.
   * This excludes input columns to simplify some tests.
   */
  val allParamSettings: Map[String, Any] = Map(
    "family" -> "poisson",
    "link" -> "log",
    "fitIntercept" -> true,
    "maxIter" -> 2,  // intentionally small
    "tol" -> 0.8,
    "regParam" -> 0.01,
    "predictionCol" -> "myPrediction",
    "variancePower" -> 1.0)

  def generateGeneralizedLinearRegressionInput(
      intercept: Double,
      coefficients: Array[Double],
      xMean: Array[Double],
      xVariance: Array[Double],
      nPoints: Int,
      seed: Int,
      noiseLevel: Double,
      family: String,
      link: String): Seq[LabeledPoint] = {

    val rnd = new Random(seed)
    def rndElement(i: Int) = {
      (rnd.nextDouble() - 0.5) * math.sqrt(12.0 * xVariance(i)) + xMean(i)
    }
    val (generator, mean) = family match {
      case "gaussian" => (new StandardNormalGenerator, 0.0)
      case "poisson" => (new PoissonGenerator(1.0), 1.0)
      case "gamma" => (new GammaGenerator(1.0, 1.0), 1.0)
    }
    generator.setSeed(seed)

    (0 until nPoints).map { _ =>
      val features = Vectors.dense(coefficients.indices.map(rndElement).toArray)
      val eta = BLAS.dot(Vectors.dense(coefficients), features) + intercept
      val mu = link match {
        case "identity" => eta
        case "log" => math.exp(eta)
        case "sqrt" => math.pow(eta, 2.0)
        case "inverse" => 1.0 / eta
      }
      val label = mu + noiseLevel * (generator.nextValue() - mean)
      // Return LabeledPoints with DenseVector
      LabeledPoint(label, features)
    }
  }
}
