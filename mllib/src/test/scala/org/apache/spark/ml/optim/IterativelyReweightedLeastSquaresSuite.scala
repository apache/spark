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
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
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
       b <- c(17, 19, 23, 29)
       w <- c(1, 2, 3, 4)
     */
    instances2 = sc.parallelize(Seq(
      Instance(17.0, 1.0, Vectors.dense(0.0, 5.0).toSparse),
      Instance(19.0, 2.0, Vectors.dense(1.0, 7.0)),
      Instance(23.0, 3.0, Vectors.dense(2.0, 11.0)),
      Instance(29.0, 4.0, Vectors.dense(3.0, 13.0))
    ), 2)
  }

  test("IRLS against GLM with Binomial") {
    /*
       R code:

       df <- as.data.frame(cbind(A, b))
       for (formula in c(b ~ . -1, b ~ .)) {
         model <- glm(formula, family = "binomial", data=df, weights=w)
         print(as.vector(coef(model)))
       }

       [1] -0.30216651 -0.04452045
       [1]  3.5651651 -1.2334085 -0.7348971
     */
    val expected = Seq(
      Vectors.dense(0.0, -0.30216651, -0.04452045),
      Vectors.dense(3.5651651, -1.2334085, -0.7348971))

    var idx = 0
    for (fitIntercept <- Seq(false, true)) {
      val irls = new IterativelyReweightedLeastSquares(family = new Binomial,
        fitIntercept, regParam = 0.0, standardizeFeatures = false, standardizeLabel = false,
        maxIter = 100, tol = 1e-8)
        .fit(instances1)
      val actual = Vectors.dense(irls.intercept, irls.coefficients(0), irls.coefficients(1))
      assert(actual ~== expected(idx) absTol 1e-4)
      idx += 1
    }
  }

  test("IRLS against GLM with Poisson") {
    /*
       R code:

       df <- as.data.frame(cbind(A, b))
       for (formula in c(b ~ . -1, b ~ .)) {
         model <- glm(formula, family = "poisson", data=df, weights=w)
         print(as.vector(coef(model)))
       }

       [1] -1.1932276  0.5273013
       [1]  2.83678860  0.23464121 -0.01407145
     */
    val expected = Seq(
      Vectors.dense(0.0, -1.1932276, 0.5273013),
      Vectors.dense(2.83678860, 0.23464121, -0.01407145))

    var idx = 0
    for (fitIntercept <- Seq(false, true)) {
      val irls = new IterativelyReweightedLeastSquares(family = new Poisson(new Log),
        fitIntercept, regParam = 0.0, standardizeFeatures = false, standardizeLabel = false,
        maxIter = 100, tol = 1e-8)
        .fit(instances2)
      val actual = Vectors.dense(irls.intercept, irls.coefficients(0), irls.coefficients(1))
      assert(actual ~== expected(idx) absTol 1e-4)
      idx += 1
    }
  }
}
