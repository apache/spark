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

package org.apache.spark.ml.recommendation.logistic.local

object Opts {
  def implicitOpts(dim: Int,
                   useBias: Boolean,
                   negative: Int,
                   pow: Float,
                   lr: Float,
                   lambdaL: Float,
                   lambdaR: Float,
                   gamma: Float,
                   verbose: Boolean): Opts = {
    new Opts(dim, useBias, negative, pow, lr,
      lambdaL, lambdaR, gamma, true, verbose)
  }

  def explicitOpts(dim: Int,
                   useBias: Boolean,
                   lr: Float,
                   lambdaL: Float,
                   lambdaR: Float,
                   verbose: Boolean): Opts = {
    new Opts(dim, useBias, 0, Float.NaN, lr,
      lambdaL, lambdaR, Float.NaN, false, verbose)
  }
}

private[ml] class Opts private(val dim: Int,
                               val useBias: Boolean,
                               val negative: Int,
                               val pow: Float,
                               val lr: Float,
                               val lambdaL: Float,
                               val lambdaR: Float,
                               val gamma: Float,
                               val implicitPref: Boolean,
                               val verbose: Boolean) extends Serializable {

  if (!implicitPref && (negative != 0 || !gamma.isNaN || !pow.isNaN)) {
    throw new IllegalArgumentException()
  }

  def vectorSize: Int = if (useBias) dim + 1 else dim
}