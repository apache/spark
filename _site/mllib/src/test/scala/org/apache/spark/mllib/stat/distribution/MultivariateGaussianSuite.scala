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

package org.apache.spark.mllib.stat.distribution

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.linalg.{ Vectors, Matrices }
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._

class MultivariateGaussianSuite extends SparkFunSuite with MLlibTestSparkContext {
  test("univariate") {
    val x1 = Vectors.dense(0.0)
    val x2 = Vectors.dense(1.5)

    val mu = Vectors.dense(0.0)
    val sigma1 = Matrices.dense(1, 1, Array(1.0))
    val dist1 = new MultivariateGaussian(mu, sigma1)
    assert(dist1.pdf(x1) ~== 0.39894 absTol 1E-5)
    assert(dist1.pdf(x2) ~== 0.12952 absTol 1E-5)

    val sigma2 = Matrices.dense(1, 1, Array(4.0))
    val dist2 = new MultivariateGaussian(mu, sigma2)
    assert(dist2.pdf(x1) ~== 0.19947 absTol 1E-5)
    assert(dist2.pdf(x2) ~== 0.15057 absTol 1E-5)
  }

  test("multivariate") {
    val x1 = Vectors.dense(0.0, 0.0)
    val x2 = Vectors.dense(1.0, 1.0)

    val mu = Vectors.dense(0.0, 0.0)
    val sigma1 = Matrices.dense(2, 2, Array(1.0, 0.0, 0.0, 1.0))
    val dist1 = new MultivariateGaussian(mu, sigma1)
    assert(dist1.pdf(x1) ~== 0.15915 absTol 1E-5)
    assert(dist1.pdf(x2) ~== 0.05855 absTol 1E-5)

    val sigma2 = Matrices.dense(2, 2, Array(4.0, -1.0, -1.0, 2.0))
    val dist2 = new MultivariateGaussian(mu, sigma2)
    assert(dist2.pdf(x1) ~== 0.060155 absTol 1E-5)
    assert(dist2.pdf(x2) ~== 0.033971 absTol 1E-5)
  }

  test("multivariate degenerate") {
    val x1 = Vectors.dense(0.0, 0.0)
    val x2 = Vectors.dense(1.0, 1.0)

    val mu = Vectors.dense(0.0, 0.0)
    val sigma = Matrices.dense(2, 2, Array(1.0, 1.0, 1.0, 1.0))
    val dist = new MultivariateGaussian(mu, sigma)
    assert(dist.pdf(x1) ~== 0.11254 absTol 1E-5)
    assert(dist.pdf(x2) ~== 0.068259 absTol 1E-5)
  }
}
