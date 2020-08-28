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

package org.apache.spark.mllib.stat

import org.apache.commons.math3.distribution.NormalDistribution

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._

class KernelDensitySuite extends SparkFunSuite with MLlibTestSparkContext {
  test("kernel density single sample") {
    val rdd = sc.parallelize(Array(5.0))
    val evaluationPoints = Array(5.0, 6.0)
    val densities = new KernelDensity().setSample(rdd).setBandwidth(3.0).estimate(evaluationPoints)
    val normal = new NormalDistribution(5.0, 3.0)
    val acceptableErr = 1e-6
    assert(densities(0) ~== normal.density(5.0) absTol acceptableErr)
    assert(densities(1) ~== normal.density(6.0) absTol acceptableErr)
  }

  test("kernel density multiple samples") {
    val rdd = sc.parallelize(Array(5.0, 10.0))
    val evaluationPoints = Array(5.0, 6.0)
    val densities = new KernelDensity().setSample(rdd).setBandwidth(3.0).estimate(evaluationPoints)
    val normal1 = new NormalDistribution(5.0, 3.0)
    val normal2 = new NormalDistribution(10.0, 3.0)
    val acceptableErr = 1e-6
    assert(
      densities(0) ~== ((normal1.density(5.0) + normal2.density(5.0)) / 2) absTol acceptableErr)
    assert(
      densities(1) ~== ((normal1.density(6.0) + normal2.density(6.0)) / 2) absTol acceptableErr)
  }
}
