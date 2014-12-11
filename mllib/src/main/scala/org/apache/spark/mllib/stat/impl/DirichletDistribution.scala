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

import cern.jet.stat.Gamma

private[mllib] class DirichletDistribution(private[mllib] val alpha: Float) {
  private def logBeta(x: Array[Float]) = {
    val n = x.size
    n * Gamma.logGamma(alpha) - Gamma.logGamma(n * alpha)
  }

  /**
   * We need this just because in case of alpha < 1 dirichlet log likelihood is infinite for x st
   * x_i = 0
   */
  private val SMALL_VALUE: Double = 0.00001

  /**
   *
   * @param x 
   * @return probability density function at x: Dir(x | alpha)
   */
  def logPDF(x: Array[Float]) =
    (-logBeta(x) + (alpha - 1) * x.map(xx => math.log(xx + SMALL_VALUE)).sum).toFloat
}
