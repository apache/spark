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

package org.apache.spark.util.random

import org.apache.commons.math3.distribution.{PoissonDistribution, NormalDistribution}

private[random] object PoissonBounds {

  val delta = 1e-4 / 3.0
  val phi = new NormalDistribution().cumulativeProbability(1.0 - delta)

  def getLambda1(s: Double): Double = {
    var lb = math.max(0.0, s - math.sqrt(s / delta)) // Chebyshev's inequality
    var ub = s
    while (lb < ub - 1.0) {
      val m = (lb + ub) / 2.0
      val poisson = new PoissonDistribution(m, 1e-15)
      val y = poisson.inverseCumulativeProbability(1 - delta)
      if (y > s) ub = m else lb = m
    }
    lb
  }

  def getMinCount(lmbd: Double): Double = {
    if(lmbd == 0) return 0
    val poisson = new PoissonDistribution(lmbd, 1e-15)
    poisson.inverseCumulativeProbability(delta)
  }

  def getLambda2(s: Double): Double = {
    var lb = s
    var ub = s + math.sqrt(s / delta) // Chebyshev's inequality
    while (lb < ub - 1.0) {
      val m = (lb + ub) / 2.0
      val poisson = new PoissonDistribution(m, 1e-15)
      val y = poisson.inverseCumulativeProbability(delta)
      if (y >= s) ub = m else lb = m
    }
    ub
  }
}
