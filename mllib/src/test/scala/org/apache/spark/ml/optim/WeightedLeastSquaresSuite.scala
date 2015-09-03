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
import org.apache.spark.ml.optim.WeightedLeastSquares.Instance
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.rdd.RDD

class WeightedLeastSquaresSuite extends SparkFunSuite with MLlibTestSparkContext {

  private var instances: RDD[Instance] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    instances = sc.parallelize(Seq(
      Instance(1.0, Vectors.dense(0.0, 5.0).toSparse, 17.0),
      Instance(2.0, Vectors.dense(1.0, 7.0), 19.0),
      Instance(3.0, Vectors.dense(2.0, 11.0), 23.0),
      Instance(4.0, Vectors.dense(3.0, 13.0), 29.0)
    ), 2)
  }

  test("WLS against glmnet") {
    /*
       R code:

library(glmnet)

A <- matrix(c(0, 1, 2, 3, 5, 7, 11, 13), 4, 2)
b <- c(17, 19, 23, 29)
w <- c(1, 2, 3, 4)

for (intercept in c(FALSE, TRUE)) {
  model <- glmnet(A, b, weights=w, intercept=intercept, lambda=0.0, standardize=FALSE, alpha=0)
  print(as.vector(coef(model)))
}

[1]  0.000000 -3.713230  3.007162
[1] 17.9935922  6.0267241 -0.5814462
     */

    val expected = Seq(
      Vectors.dense(0.0, -3.713230, 3.007162),
      Vectors.dense(17.9935922, 6.0267241, -0.5814462))

    var idx = 0
    for (fitIntercept <- Seq(false, true);
         regParam <- Seq(0.0);
         standardization <- Seq(false)) {
      val wls = new WeightedLeastSquares(fitIntercept, regParam, standardization)
        .fit(instances)
      val actual = Vectors.dense(wls.intercept, wls.coefficients(0), wls.coefficients(1))
      assert(actual ~== expected(idx) absTol 1e-1)
      idx += 1
    }
  }
}
