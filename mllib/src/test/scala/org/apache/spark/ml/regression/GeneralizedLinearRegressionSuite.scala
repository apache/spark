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

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.mllib.classification.LogisticRegressionSuite._
import org.apache.spark.mllib.linalg.{BLAS, DenseVector, Vectors}
import org.apache.spark.mllib.random._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.sql.{DataFrame, Row}

class GeneralizedLinearRegressionSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  private val seed: Int = 42
  @transient var datasetGaussianIdentity: DataFrame = _
  @transient var datasetGaussianLog: DataFrame = _
  @transient var datasetGaussianInverse: DataFrame = _
  @transient var datasetBinomial: DataFrame = _
  @transient var datasetPoissonLog: DataFrame = _
  @transient var datasetPoissonIdentity: DataFrame = _
  @transient var datasetPoissonSqrt: DataFrame = _
  @transient var datasetGammaInverse: DataFrame = _
  @transient var datasetGammaIdentity: DataFrame = _
  @transient var datasetGammaLog: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    import GeneralizedLinearRegressionSuite._

    datasetGaussianIdentity = sqlContext.createDataFrame(
      sc.parallelize(generateGeneralizedLinearRegressionInput(
        intercept = 2.5, coefficients = Array(2.2, 0.6), xMean = Array(2.9, 10.5),
        xVariance = Array(0.7, 1.2), nPoints = 10000, seed, noiseLevel = 0.01,
        family = "gaussian", link = "identity"), 2))

    datasetGaussianLog = sqlContext.createDataFrame(
      sc.parallelize(generateGeneralizedLinearRegressionInput(
        intercept = 0.25, coefficients = Array(0.22, 0.06), xMean = Array(2.9, 10.5),
        xVariance = Array(0.7, 1.2), nPoints = 10000, seed, noiseLevel = 0.01,
        family = "gaussian", link = "log"), 2))

    datasetGaussianInverse = sqlContext.createDataFrame(
      sc.parallelize(generateGeneralizedLinearRegressionInput(
        intercept = 2.5, coefficients = Array(2.2, 0.6), xMean = Array(2.9, 10.5),
        xVariance = Array(0.7, 1.2), nPoints = 10000, seed, noiseLevel = 0.01,
        family = "gaussian", link = "inverse"), 2))

    datasetBinomial = {
      val nPoints = 10000
      val coefficients = Array(-0.57997, 0.912083, -0.371077, -0.819866, 2.688191)
      val xMean = Array(5.843, 3.057, 3.758, 1.199)
      val xVariance = Array(0.6856, 0.1899, 3.116, 0.581)

      val testData =
        generateMultinomialLogisticInput(coefficients, xMean, xVariance,
          addIntercept = true, nPoints, seed)

      sqlContext.createDataFrame(sc.parallelize(testData, 2))
    }

    datasetPoissonLog = sqlContext.createDataFrame(
      sc.parallelize(generateGeneralizedLinearRegressionInput(
        intercept = 0.25, coefficients = Array(0.22, 0.06), xMean = Array(2.9, 10.5),
        xVariance = Array(0.7, 1.2), nPoints = 10000, seed, noiseLevel = 0.01,
        family = "poisson", link = "log"), 2))

    datasetPoissonIdentity = sqlContext.createDataFrame(
      sc.parallelize(generateGeneralizedLinearRegressionInput(
        intercept = 2.5, coefficients = Array(2.2, 0.6), xMean = Array(2.9, 10.5),
        xVariance = Array(0.7, 1.2), nPoints = 10000, seed, noiseLevel = 0.01,
        family = "poisson", link = "identity"), 2))

    datasetPoissonSqrt = sqlContext.createDataFrame(
      sc.parallelize(generateGeneralizedLinearRegressionInput(
        intercept = 2.5, coefficients = Array(2.2, 0.6), xMean = Array(2.9, 10.5),
        xVariance = Array(0.7, 1.2), nPoints = 10000, seed, noiseLevel = 0.01,
        family = "poisson", link = "sqrt"), 2))

    datasetGammaInverse = sqlContext.createDataFrame(
      sc.parallelize(generateGeneralizedLinearRegressionInput(
        intercept = 2.5, coefficients = Array(2.2, 0.6), xMean = Array(2.9, 10.5),
        xVariance = Array(0.7, 1.2), nPoints = 10000, seed, noiseLevel = 0.01,
        family = "gamma", link = "inverse"), 2))

    datasetGammaIdentity = sqlContext.createDataFrame(
      sc.parallelize(generateGeneralizedLinearRegressionInput(
        intercept = 2.5, coefficients = Array(2.2, 0.6), xMean = Array(2.9, 10.5),
        xVariance = Array(0.7, 1.2), nPoints = 10000, seed, noiseLevel = 0.01,
        family = "gamma", link = "identity"), 2))

    datasetGammaLog = sqlContext.createDataFrame(
      sc.parallelize(generateGeneralizedLinearRegressionInput(
        intercept = 0.25, coefficients = Array(0.22, 0.06), xMean = Array(2.9, 10.5),
        xVariance = Array(0.7, 1.2), nPoints = 10000, seed, noiseLevel = 0.01,
        family = "gamma", link = "log"), 2))
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
    assert(glr.getWeightCol === "")
    assert(glr.getRegParam === 0.0)
    assert(glr.getSolver == "irls")
    // TODO: Construct model directly instead of via fitting.
    val model = glr.setFamily("gaussian").setLink("identity")
      .fit(datasetGaussianIdentity)

    // copied model must have the same parent.
    MLTestingUtils.checkCopy(model)

    assert(model.getFeaturesCol === "features")
    assert(model.getPredictionCol === "prediction")
    assert(model.intercept !== 0.0)
    assert(model.hasParent)
    assert(model.getFamily === "gaussian")
    assert(model.getLink === "identity")
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

       [1] 2.2960999 0.8087933
       [1] 2.5002642 2.2000403 0.5999485

       data <- read.csv("path", header=FALSE)
       model1 <- glm(f1, family=gaussian(link=log), data=data, start=c(0,0))
       model2 <- glm(f2, family=gaussian(link=log), data=data, start=c(0,0,0))
       print(as.vector(coef(model1)))
       print(as.vector(coef(model2)))

       [1] 0.23069326 0.07993778
       [1] 0.25001858 0.22002452 0.05998789

       data <- read.csv("path", header=FALSE)
       for (formula in c(f1, f2)) {
         model <- glm(formula, family=gaussian(link=inverse), data=data)
         print(as.vector(coef(model)))
       }

       [1] 2.3010179 0.8198976
       [1] 2.4108902 2.2130248 0.6086152
     */

    val expected = Seq(
      Vectors.dense(0.0, 2.2960999, 0.8087933),
      Vectors.dense(2.5002642, 2.2000403, 0.5999485),
      Vectors.dense(0.0, 0.23069326, 0.07993778),
      Vectors.dense(0.25001858, 0.22002452, 0.05998789),
      Vectors.dense(0.0, 2.3010179, 0.8198976),
      Vectors.dense(2.4108902, 2.2130248, 0.6086152))

    import GeneralizedLinearRegression._

    var idx = 0
    for ((link, dataset) <- Seq(("identity", datasetGaussianIdentity), ("log", datasetGaussianLog),
      ("inverse", datasetGaussianInverse))) {
      for (fitIntercept <- Seq(false, true)) {
        val trainer = new GeneralizedLinearRegression().setFamily("gaussian").setLink(link)
          .setFitIntercept(fitIntercept)
        val model = trainer.fit(dataset)
        val actual = Vectors.dense(model.intercept, model.coefficients(0), model.coefficients(1))
        assert(actual ~= expected(idx) absTol 1e-4, "Model mismatch: GLM with gaussian family, " +
          s"$link link and fitIntercept = $fitIntercept.")

        val familyLink = new FamilyAndLink(Gaussian, Link.fromName(link))
        model.transform(dataset).select("features", "prediction").collect().foreach {
          case Row(features: DenseVector, prediction1: Double) =>
            val eta = BLAS.dot(features, model.coefficients) + model.intercept
            val prediction2 = familyLink.fitted(eta)
            assert(prediction1 ~= prediction2 relTol 1E-5, "Prediction mismatch: GLM with " +
              s"gaussian family, $link link and fitIntercept = $fitIntercept.")
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

       [1] 0.0000000 2.2961005 0.8087932
       [1] 0.0000000 2.2130368 0.8309556
       [1] 0.0000000 1.7176137 0.9610657
       [1] 2.5002642 2.2000403 0.5999485
       [1] 3.1106389 2.0935142 0.5712711
       [1] 6.7597127 1.4581054 0.3994266
     */

    val expected = Seq(
      Vectors.dense(0.0, 2.2961005, 0.8087932),
      Vectors.dense(0.0, 2.2130368, 0.8309556),
      Vectors.dense(0.0, 1.7176137, 0.9610657),
      Vectors.dense(2.5002642, 2.2000403, 0.5999485),
      Vectors.dense(3.1106389, 2.0935142, 0.5712711),
      Vectors.dense(6.7597127, 1.4581054, 0.3994266))

    var idx = 0
    for (fitIntercept <- Seq(false, true);
         regParam <- Seq(0.0, 0.1, 1.0)) {
      val trainer = new GeneralizedLinearRegression().setFamily("gaussian")
        .setFitIntercept(fitIntercept).setRegParam(regParam)
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
          .setFitIntercept(fitIntercept)
        val model = trainer.fit(dataset)
        val actual = Vectors.dense(model.intercept, model.coefficients(0), model.coefficients(1),
          model.coefficients(2), model.coefficients(3))
        assert(actual ~= expected(idx) absTol 1e-4, "Model mismatch: GLM with binomial family, " +
          s"$link link and fitIntercept = $fitIntercept.")

        val familyLink = new FamilyAndLink(Binomial, Link.fromName(link))
        model.transform(dataset).select("features", "prediction").collect().foreach {
          case Row(features: DenseVector, prediction1: Double) =>
            val eta = BLAS.dot(features, model.coefficients) + model.intercept
            val prediction2 = familyLink.fitted(eta)
            assert(prediction1 ~= prediction2 relTol 1E-5, "Prediction mismatch: GLM with " +
              s"binomial family, $link link and fitIntercept = $fitIntercept.")
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
          .setFitIntercept(fitIntercept)
        val model = trainer.fit(dataset)
        val actual = Vectors.dense(model.intercept, model.coefficients(0), model.coefficients(1))
        assert(actual ~= expected(idx) absTol 1e-4, "Model mismatch: GLM with poisson family, " +
          s"$link link and fitIntercept = $fitIntercept.")

        val familyLink = new FamilyAndLink(Poisson, Link.fromName(link))
        model.transform(dataset).select("features", "prediction").collect().foreach {
          case Row(features: DenseVector, prediction1: Double) =>
            val eta = BLAS.dot(features, model.coefficients) + model.intercept
            val prediction2 = familyLink.fitted(eta)
            assert(prediction1 ~= prediction2 relTol 1E-5, "Prediction mismatch: GLM with " +
              s"poisson family, $link link and fitIntercept = $fitIntercept.")
        }

        idx += 1
      }
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
        val trainer = new GeneralizedLinearRegression().setFamily("gamma").setLink(link)
          .setFitIntercept(fitIntercept)
        val model = trainer.fit(dataset)
        val actual = Vectors.dense(model.intercept, model.coefficients(0), model.coefficients(1))
        assert(actual ~= expected(idx) absTol 1e-4, "Model mismatch: GLM with gamma family, " +
          s"$link link and fitIntercept = $fitIntercept.")

        val familyLink = new FamilyAndLink(Gamma, Link.fromName(link))
        model.transform(dataset).select("features", "prediction").collect().foreach {
          case Row(features: DenseVector, prediction1: Double) =>
            val eta = BLAS.dot(features, model.coefficients) + model.intercept
            val prediction2 = familyLink.fitted(eta)
            assert(prediction1 ~= prediction2 relTol 1E-5, "Prediction mismatch: GLM with " +
              s"gamma family, $link link and fitIntercept = $fitIntercept.")
        }

        idx += 1
      }
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
      GeneralizedLinearRegressionSuite.allParamSettings, checkModelData)
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
    "predictionCol" -> "myPrediction")

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
      val features = Vectors.dense(coefficients.indices.map { rndElement(_) }.toArray)
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
