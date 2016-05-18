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

package org.apache.spark.ml.optim

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.feature.Instance
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.rdd.RDD

class IterativelyReweightedLeastSquaresSuite extends SparkFunSuite with MLlibTestSparkContext {

  private var instances1: RDD[Instance] = _
  private var instances2: RDD[Instance] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    /*
       R code:

       A <- matrix(c(0, 1, 2, 3, 5, 2, 1, 3), 4, 2)
       b <- c(1, 0, 1, 0)
       w <- c(1, 2, 3, 4)
     */
    instances1 = sc.parallelize(Seq(
      Instance(1.0, 1.0, Vectors.dense(0.0, 5.0).toSparse),
      Instance(0.0, 2.0, Vectors.dense(1.0, 2.0)),
      Instance(1.0, 3.0, Vectors.dense(2.0, 1.0)),
      Instance(0.0, 4.0, Vectors.dense(3.0, 3.0))
    ), 2)
    /*
       R code:

       A <- matrix(c(0, 1, 2, 3, 5, 7, 11, 13), 4, 2)
       b <- c(2, 8, 3, 9)
       w <- c(1, 2, 3, 4)
     */
    instances2 = sc.parallelize(Seq(
      Instance(2.0, 1.0, Vectors.dense(0.0, 5.0).toSparse),
      Instance(8.0, 2.0, Vectors.dense(1.0, 7.0)),
      Instance(3.0, 3.0, Vectors.dense(2.0, 11.0)),
      Instance(9.0, 4.0, Vectors.dense(3.0, 13.0))
    ), 2)
  }

  test("IRLS against GLM with Binomial errors") {
    /*
       R code:

       df <- as.data.frame(cbind(A, b))
       for (formula in c(b ~ . -1, b ~ .)) {
         model <- glm(formula, family="binomial", data=df, weights=w)
         print(as.vector(coef(model)))
       }

       [1] -0.30216651 -0.04452045
       [1]  3.5651651 -1.2334085 -0.7348971
     */
    val expected = Seq(
      Vectors.dense(0.0, -0.30216651, -0.04452045),
      Vectors.dense(3.5651651, -1.2334085, -0.7348971))

    import IterativelyReweightedLeastSquaresSuite._

    var idx = 0
    for (fitIntercept <- Seq(false, true)) {
      val newInstances = instances1.map { instance =>
        val mu = (instance.label + 0.5) / 2.0
        val eta = math.log(mu / (1.0 - mu))
        Instance(eta, instance.weight, instance.features)
      }
      val initial = new WeightedLeastSquares(fitIntercept, regParam = 0.0,
        standardizeFeatures = false, standardizeLabel = false).fit(newInstances)
      val irls = new IterativelyReweightedLeastSquares(initial, BinomialReweightFunc,
        fitIntercept, regParam = 0.0, maxIter = 25, tol = 1e-8).fit(instances1)
      val actual = Vectors.dense(irls.intercept, irls.coefficients(0), irls.coefficients(1))
      assert(actual ~== expected(idx) absTol 1e-4)
      idx += 1
    }
  }

  test("IRLS against GLM with Poisson errors") {
    /*
       R code:

       df <- as.data.frame(cbind(A, b))
       for (formula in c(b ~ . -1, b ~ .)) {
         model <- glm(formula, family="poisson", data=df, weights=w)
         print(as.vector(coef(model)))
       }

       [1] -0.09607792  0.18375613
       [1]  6.299947  3.324107 -1.081766
     */
    val expected = Seq(
      Vectors.dense(0.0, -0.09607792, 0.18375613),
      Vectors.dense(6.299947, 3.324107, -1.081766))

    import IterativelyReweightedLeastSquaresSuite._

    var idx = 0
    for (fitIntercept <- Seq(false, true)) {
      val yMean = instances2.map(_.label).mean
      val newInstances = instances2.map { instance =>
        val mu = (instance.label + yMean) / 2.0
        val eta = math.log(mu)
        Instance(eta, instance.weight, instance.features)
      }
      val initial = new WeightedLeastSquares(fitIntercept, regParam = 0.0,
        standardizeFeatures = false, standardizeLabel = false).fit(newInstances)
      val irls = new IterativelyReweightedLeastSquares(initial, PoissonReweightFunc,
        fitIntercept, regParam = 0.0, maxIter = 25, tol = 1e-8).fit(instances2)
      val actual = Vectors.dense(irls.intercept, irls.coefficients(0), irls.coefficients(1))
      assert(actual ~== expected(idx) absTol 1e-4)
      idx += 1
    }
  }

  test("IRLS against L1Regression") {
    /*
       R code:

       library(quantreg)

       df <- as.data.frame(cbind(A, b))
       for (formula in c(b ~ . -1, b ~ .)) {
         model <- rq(formula, data=df, weights=w)
         print(as.vector(coef(model)))
       }

       [1] 1.266667 0.400000
       [1] 29.5 17.0 -5.5
     */
    val expected = Seq(
      Vectors.dense(0.0, 1.266667, 0.400000),
      Vectors.dense(29.5, 17.0, -5.5))

    import IterativelyReweightedLeastSquaresSuite._

    var idx = 0
    for (fitIntercept <- Seq(false, true)) {
      val initial = new WeightedLeastSquares(fitIntercept, regParam = 0.0,
        standardizeFeatures = false, standardizeLabel = false).fit(instances2)
      val irls = new IterativelyReweightedLeastSquares(initial, L1RegressionReweightFunc,
        fitIntercept, regParam = 0.0, maxIter = 200, tol = 1e-7).fit(instances2)
      val actual = Vectors.dense(irls.intercept, irls.coefficients(0), irls.coefficients(1))
      assert(actual ~== expected(idx) absTol 1e-4)
      idx += 1
    }
  }
}

object IterativelyReweightedLeastSquaresSuite {

  def BinomialReweightFunc(
      instance: Instance,
      model: WeightedLeastSquaresModel): (Double, Double) = {
    val eta = model.predict(instance.features)
    val mu = 1.0 / (1.0 + math.exp(-1.0 * eta))
    val z = eta + (instance.label - mu) / (mu * (1.0 - mu))
    val w = mu * (1 - mu) * instance.weight
    (z, w)
  }

  def PoissonReweightFunc(
      instance: Instance,
      model: WeightedLeastSquaresModel): (Double, Double) = {
    val eta = model.predict(instance.features)
    val mu = math.exp(eta)
    val z = eta + (instance.label - mu) / mu
    val w = mu * instance.weight
    (z, w)
  }

  def L1RegressionReweightFunc(
      instance: Instance,
      model: WeightedLeastSquaresModel): (Double, Double) = {
    val eta = model.predict(instance.features)
    val e = math.max(math.abs(eta - instance.label), 1e-7)
    val w = 1 / e
    val y = instance.label
    (y, w)
  }
}
