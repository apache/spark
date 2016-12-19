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

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.classification.LogisticRegressionSuite._
import org.apache.spark.ml.feature.{Instance, LabeledPoint}
import org.apache.spark.ml.linalg.{BLAS, DenseVector, Vector, Vectors}
import org.apache.spark.ml.param.{ParamMap, ParamsSuite}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.mllib.random._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.{DataFrame, Row}

import scala.util.Random

class TestSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import testImplicits._


  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  test("generalized linear regression: tweedie family against glm") {
  /*
      R code:
      df <- as.data.frame(matrix(c(
        1.0, 1.0, 0.0, 5.0,
        0.5, 1.0, 1.0, 2.0,
        1.0, 1.0, 2.0, 1.0,
        2.0, 1.0, 3.0, 3.0), 4, 4, byrow = TRUE))

      f1 <- V1 ~ -1 + V3 + V4
      f2 <- V1 ~ V3 + V4

      for (f in c(f1, f2))
        for (lp in c(0, 1))
          for (vp in c(1.6, 2.5, 3.0, 4.0)){
            model <- glm(f, df, family = tweedie(var.power = vp, link.power = lp))
            print(as.vector(coef(model)))
          }

      [1]  0.1496480 -0.0122283
      [1]  0.1373567 -0.0120673
      [1]  0.13077402 -0.01181116
      [1]  0.11853618 -0.01118475
      [1] 0.3919109 0.1846094
      [1] 0.3684426 0.1810662
      [1] 0.3566982 0.1788412
      [1] 0.3370804 0.1740093
      [1] -1.3163732  0.4378139  0.2464114
      [1] -1.4396020  0.4817364  0.2680088
      [1] -1.5975930  0.5440060  0.2982824
      [1] -3.4044522  1.3557615  0.6797386
      [1] -0.7090230  0.6256309  0.3294324
      [1] -0.9524928  0.7304267  0.3792687
      [1] -1.1216622  0.8089538  0.4156152
      [1] -1.3594653  0.9262326  0.4682795
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
      Vectors.dense(0, 0.13077402, -0.01181116),
      Vectors.dense(0, 0.11853618, -0.01118475),
      Vectors.dense(0, 0.3919109, 0.1846094),
      Vectors.dense(0, 0.3684426, 0.1810662),
      Vectors.dense(0, 0.3566982, 0.1788412),
      Vectors.dense(0, 0.3370804, 0.1740093),
      Vectors.dense(-1.3163732, 0.4378139, 0.2464114),
      Vectors.dense(-1.439602, 0.4817364, 0.2680088),
      Vectors.dense(-1.597593, 0.544006, 0.2982824),
      Vectors.dense(-3.4044522, 1.3557615, 0.6797386),
      Vectors.dense(-0.709023, 0.6256309, 0.3294324),
      Vectors.dense(-0.9524928, 0.7304267, 0.3792687),
      Vectors.dense(-1.1216622, 0.8089538, 0.4156152),
      Vectors.dense(-1.3594653, 0.9262326, 0.4682795))

    import GeneralizedLinearRegression._

    var idx = 0
    for (fitIntercept <- Seq(false, true); link <- Seq("log", "identity")) {
      for (variancePower <- Seq(1.6, 2.5, 3.0, 4.0)) {
        val trainer = new GeneralizedLinearRegression().setFamily("tweedie")
          .setFitIntercept(fitIntercept).setLink(link).setLinkPredictionCol("linkPrediction")
          .setVariancePower(variancePower)
        val model = trainer.fit(datasetTweedie)
        val actual = Vectors.dense(model.intercept, model.coefficients(0), model.coefficients(1))
        assert(actual ~= expected(idx) absTol 1e-4, "Model mismatch: GLM with tweedie family, " +
          s"$link link, fitIntercept = $fitIntercept and variancePower = $variancePower.")

        val familyLink = new FamilyAndLink(Tweedie, Link.fromName(link))
        model.transform(datasetTweedie).select("features", "prediction", "linkPrediction").collect()
          .foreach {
            case Row(features: DenseVector, prediction1: Double, linkPrediction1: Double) =>
              val eta = BLAS.dot(features, model.coefficients) + model.intercept
              val prediction2 = familyLink.fitted(eta)
              val linkPrediction2 = eta
              assert(prediction1 ~= prediction2 relTol 1E-5, "Prediction mismatch: GLM with " +
                s"tweedie family, $link link, fitIntercept = $fitIntercept " +
                s"and variancePower = $variancePower.")
              assert(linkPrediction1 ~= linkPrediction2 relTol 1E-5, "Link Prediction mismatch: " +
                s"GLM with tweedie family, $link link and fitIntercept = $fitIntercept " +
                s"and variancePower = $variancePower.")
          }
        idx += 1
      }
    }
  }

  test("glm summary: tweedie family with weight") {
    /*
      R code:

      df <- as.data.frame(matrix(c(
        1.0, 1.0, 0.0, 5.0,
        0.5, 2.0, 1.0, 2.0,
        1.0, 3.0, 2.0, 1.0,
        0.0, 4.0, 3.0, 3.0), 4, 4, byrow = TRUE))
     */
    val datasetWithWeight = Seq(
      Instance(1.0, 1.0, Vectors.dense(0.0, 5.0)),
      Instance(0.5, 2.0, Vectors.dense(1.0, 2.0)),
      Instance(1.0, 3.0, Vectors.dense(2.0, 1.0)),
      Instance(0.0, 4.0, Vectors.dense(3.0, 3.0))
    ).toDF()

    val trainer = new GeneralizedLinearRegression()
      .setFamily("tweedie")
      .setVariancePower(1.6)
      .setWeightCol("weight")
      .setFitIntercept(false)

    val model = trainer.fit(datasetWithWeight)
    val coefficientsR = Vectors.dense(Array(-0.408746, -0.12125))
    val interceptR = 0.0
    val devianceResidualsR = Array(0.621047, -0.051515, 1.693473, -3.253946)
    val pearsonResidualsR = Array(0.738362, -0.050946, 2.234834, -1.455209)
    val workingResidualsR = Array(0.833541, -0.041036, 1.556764, -1.0)
    val responseResidualsR = Array(0.454607, -0.021396, 0.608881, -0.203928)
    val seCoefR = Array(0.520519, 0.408215)
    val tValsR = Array(-0.785267, -0.297024)
    val pValsR = Array(0.514549, 0.794457)
    val dispersionR = 3.830036
    val nullDevianceR = 20.702
    val residualDevianceR = 13.844
    val residualDegreeOfFreedomNullR = 4
    val residualDegreeOfFreedomR = 2
    // val aicR = 0.0

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
    // assert(summary.aic ~== aicR absTol 1E-3)
    assert(summary.solver === "irls")

  }
}

