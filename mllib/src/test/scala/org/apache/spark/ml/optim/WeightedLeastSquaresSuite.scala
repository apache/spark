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

class WeightedLeastSquaresSuite extends SparkFunSuite with MLlibTestSparkContext {

  private var instances: RDD[Instance] = _
  private var instancesConstLabel: RDD[Instance] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    /*
       R code:

       A <- matrix(c(0, 1, 2, 3, 5, 7, 11, 13), 4, 2)
       b <- c(17, 19, 23, 29)
       w <- c(1, 2, 3, 4)
     */
    instances = sc.parallelize(Seq(
      Instance(17.0, 1.0, Vectors.dense(0.0, 5.0).toSparse),
      Instance(19.0, 2.0, Vectors.dense(1.0, 7.0)),
      Instance(23.0, 3.0, Vectors.dense(2.0, 11.0)),
      Instance(29.0, 4.0, Vectors.dense(3.0, 13.0))
    ), 2)

    /*
       R code:

       A <- matrix(c(0, 1, 2, 3, 5, 7, 11, 13), 4, 2)
       b.const <- c(17, 17, 17, 17)
       w <- c(1, 2, 3, 4)
     */
    instancesConstLabel = sc.parallelize(Seq(
      Instance(17.0, 1.0, Vectors.dense(0.0, 5.0).toSparse),
      Instance(17.0, 2.0, Vectors.dense(1.0, 7.0)),
      Instance(17.0, 3.0, Vectors.dense(2.0, 11.0)),
      Instance(17.0, 4.0, Vectors.dense(3.0, 13.0))
    ), 2)
  }

  test("WLS against lm") {
    /*
       R code:

       df <- as.data.frame(cbind(A, b))
       for (formula in c(b ~ . -1, b ~ .)) {
         model <- lm(formula, data=df, weights=w)
         print(as.vector(coef(model)))
       }

       [1] -3.727121  3.009983
       [1] 18.08  6.08 -0.60
     */

    val expected = Seq(
      Vectors.dense(0.0, -3.727121, 3.009983),
      Vectors.dense(18.08, 6.08, -0.60))

    var idx = 0
    for (fitIntercept <- Seq(false, true)) {
       for (standardization <- Seq(false, true)) {
         val wls = new WeightedLeastSquares(
           fitIntercept, regParam = 0.0, standardizeFeatures = standardization,
           standardizeLabel = standardization).fit(instances)
         val actual = Vectors.dense(wls.intercept, wls.coefficients(0), wls.coefficients(1))
         assert(actual ~== expected(idx) absTol 1e-4)
       }
      idx += 1
    }
  }

  test("WLS against lm when label is constant and no regularization") {
    /*
       R code:

       df.const.label <- as.data.frame(cbind(A, b.const))
       for (formula in c(b.const ~ . -1, b.const ~ .)) {
         model <- lm(formula, data=df.const.label, weights=w)
         print(as.vector(coef(model)))
       }

      [1] -9.221298  3.394343
      [1] 17  0  0
    */

    val expected = Seq(
      Vectors.dense(0.0, -9.221298, 3.394343),
      Vectors.dense(17.0, 0.0, 0.0))

    var idx = 0
    for (fitIntercept <- Seq(false, true)) {
      for (standardization <- Seq(false, true)) {
        val wls = new WeightedLeastSquares(
          fitIntercept, regParam = 0.0, standardizeFeatures = standardization,
          standardizeLabel = standardization).fit(instancesConstLabel)
        val actual = Vectors.dense(wls.intercept, wls.coefficients(0), wls.coefficients(1))
        assert(actual ~== expected(idx) absTol 1e-4)
      }
      idx += 1
    }
  }

  test("WLS with regularization when label is constant") {
    // if regParam is non-zero and standardization is true, the problem is ill-defined and
    // an exception is thrown.
    val wls = new WeightedLeastSquares(
      fitIntercept = false, regParam = 0.1, standardizeFeatures = true,
      standardizeLabel = true)
    intercept[IllegalArgumentException]{
      wls.fit(instancesConstLabel)
    }
  }

  test("WLS against glmnet") {
    /*
       R code:

       library(glmnet)

       for (intercept in c(FALSE, TRUE)) {
         for (lambda in c(0.0, 0.1, 1.0)) {
           for (standardize in c(FALSE, TRUE)) {
             model <- glmnet(A, b, weights=w, intercept=intercept, lambda=lambda,
                             standardize=standardize, alpha=0, thresh=1E-14)
             print(as.vector(coef(model)))
           }
         }
       }

       [1]  0.000000 -3.727117  3.009982
       [1]  0.000000 -3.727117  3.009982
       [1]  0.000000 -3.307532  2.924206
       [1]  0.000000 -2.914790  2.840627
       [1]  0.000000 -1.526575  2.558158
       [1] 0.00000000 0.06984238 2.20488344
       [1] 18.0799727  6.0799832 -0.5999941
       [1] 18.0799727  6.0799832 -0.5999941
       [1] 13.5356178  3.2714044  0.3770744
       [1] 14.064629  3.565802  0.269593
       [1] 10.1238013  0.9708569  1.1475466
       [1] 13.1860638  2.1761382  0.6213134
     */

    val expected = Seq(
      Vectors.dense(0.0, -3.727117, 3.009982),
      Vectors.dense(0.0, -3.727117, 3.009982),
      Vectors.dense(0.0, -3.307532, 2.924206),
      Vectors.dense(0.0, -2.914790, 2.840627),
      Vectors.dense(0.0, -1.526575, 2.558158),
      Vectors.dense(0.0, 0.06984238, 2.20488344),
      Vectors.dense(18.0799727, 6.0799832, -0.5999941),
      Vectors.dense(18.0799727, 6.0799832, -0.5999941),
      Vectors.dense(13.5356178, 3.2714044, 0.3770744),
      Vectors.dense(14.064629, 3.565802, 0.269593),
      Vectors.dense(10.1238013, 0.9708569, 1.1475466),
      Vectors.dense(13.1860638, 2.1761382, 0.6213134))

    var idx = 0
    for (fitIntercept <- Seq(false, true);
         regParam <- Seq(0.0, 0.1, 1.0);
         standardizeFeatures <- Seq(false, true)) {
      val wls = new WeightedLeastSquares(
        fitIntercept, regParam, standardizeFeatures, standardizeLabel = true)
        .fit(instances)
      val actual = Vectors.dense(wls.intercept, wls.coefficients(0), wls.coefficients(1))
      assert(actual ~== expected(idx) absTol 1e-4)
      idx += 1
    }
  }
}
