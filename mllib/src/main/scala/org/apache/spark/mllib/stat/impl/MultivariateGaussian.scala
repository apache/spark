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

package org.apache.spark.mllib.stat.impl

import breeze.linalg.{DenseVector => DBV, DenseMatrix => DBM, Transpose, det, pinv}

/** 
   * Utility class to implement the density function for multivariate Gaussian distribution.
   * Breeze provides this functionality, but it requires the Apache Commons Math library,
   * so this class is here so-as to not introduce a new dependency in Spark.
   */
private[mllib] class MultivariateGaussian(
    val mu: DBV[Double], 
    val sigma: DBM[Double]) extends Serializable {
  private val sigmaInv2 = pinv(sigma) * -0.5
  private val U = math.pow(2.0 * math.Pi, -mu.length / 2.0) * math.pow(det(sigma), -0.5)
    
  /** Returns density of this multivariate Gaussian at given point, x */
  def pdf(x: DBV[Double]): Double = {
    val delta = x - mu
    val deltaTranspose = new Transpose(delta)
    U * math.exp(deltaTranspose * sigmaInv2 * delta)
  }
}
