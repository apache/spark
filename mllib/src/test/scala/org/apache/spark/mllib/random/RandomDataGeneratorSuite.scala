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

package org.apache.spark.mllib.random

import org.apache.commons.math3.special.Gamma

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.util.StatCounter

class RandomDataGeneratorSuite extends SparkFunSuite {

  def apiChecks(gen: RandomDataGenerator[Double]): Unit = {
    // resetting seed should generate the same sequence of random numbers
    gen.setSeed(42L)
    val array1 = (0 until 1000).map(_ => gen.nextValue())
    gen.setSeed(42L)
    val array2 = (0 until 1000).map(_ => gen.nextValue())
    assert(array1.equals(array2))

    // newInstance should contain a difference instance of the rng
    // i.e. setting difference seeds for difference instances produces different sequences of
    // random numbers.
    val gen2 = gen.copy()
    gen.setSeed(0L)
    val array3 = (0 until 1000).map(_ => gen.nextValue())
    gen2.setSeed(1L)
    val array4 = (0 until 1000).map(_ => gen2.nextValue())
    // Compare arrays instead of elements since individual elements can coincide by chance but the
    // sequences should differ given two different seeds.
    assert(!array3.equals(array4))

    // test that setting the same seed in the copied instance produces the same sequence of numbers
    gen.setSeed(0L)
    val array5 = (0 until 1000).map(_ => gen.nextValue())
    gen2.setSeed(0L)
    val array6 = (0 until 1000).map(_ => gen2.nextValue())
    assert(array5.equals(array6))
  }

  def distributionChecks(gen: RandomDataGenerator[Double],
      mean: Double = 0.0,
      stddev: Double = 1.0,
      epsilon: Double = 0.01): Unit = {
    for (seed <- 0 until 5) {
      gen.setSeed(seed.toLong)
      val sample = (0 until 100000).map { _ => gen.nextValue()}
      val stats = new StatCounter(sample)
      assert(stats.mean ~== mean absTol epsilon)
      assert(stats.stdev ~== stddev absTol epsilon)
    }
  }

  test("UniformGenerator") {
    val uniform = new UniformGenerator()
    apiChecks(uniform)
    // Stddev of uniform distribution = (ub - lb) / math.sqrt(12)
    distributionChecks(uniform, 0.5, 1 / math.sqrt(12))
  }

  test("StandardNormalGenerator") {
    val normal = new StandardNormalGenerator()
    apiChecks(normal)
    distributionChecks(normal, 0.0, 1.0)
  }

  test("LogNormalGenerator") {
    List((0.0, 1.0), (0.0, 2.0), (2.0, 1.0), (2.0, 2.0)).foreach {
      case (mean: Double, vari: Double) =>
        val normal = new LogNormalGenerator(mean, math.sqrt(vari))
        apiChecks(normal)

        // mean of log normal = e^(mean + var / 2)
        val expectedMean = math.exp(mean + 0.5 * vari)

        // variance of log normal = (e^var - 1) * e^(2 * mean + var)
        val expectedStd = math.sqrt(math.expm1(vari) * math.exp(2.0 * mean + vari))

        // since sampling error increases with variance, let's set
        // the absolute tolerance as a percentage
        val epsilon = 0.05 * expectedStd * expectedStd

        distributionChecks(normal, expectedMean, expectedStd, epsilon)
    }
  }

  test("PoissonGenerator") {
    // mean = 0.0 will not pass the API checks since 0.0 is always deterministically produced.
    for (mean <- List(1.0, 5.0, 100.0)) {
      val poisson = new PoissonGenerator(mean)
      apiChecks(poisson)
      distributionChecks(poisson, mean, math.sqrt(mean), 0.1)
    }
  }

  test("ExponentialGenerator") {
    // mean = 0.0 will not pass the API checks since 0.0 is always deterministically produced.
    for (mean <- List(2.0, 5.0, 10.0, 50.0, 100.0)) {
      val exponential = new ExponentialGenerator(mean)
      apiChecks(exponential)
      // var of exp = lambda^-2 = (1.0 / mean)^-2 = mean^2

      // since sampling error increases with variance, let's set
      // the absolute tolerance as a percentage
      val epsilon = 0.05 * mean * mean

      distributionChecks(exponential, mean, mean, epsilon)
    }
  }

  test("GammaGenerator") {
    // mean = 0.0 will not pass the API checks since 0.0 is always deterministically produced.
    List((1.0, 2.0), (2.0, 2.0), (3.0, 2.0), (5.0, 1.0), (9.0, 0.5)).foreach {
      case (shape: Double, scale: Double) =>
        val gamma = new GammaGenerator(shape, scale)
        apiChecks(gamma)
        // mean of gamma = shape * scale
        val expectedMean = shape * scale
        // var of gamma = shape * scale^2
        val expectedStd = math.sqrt(shape * scale * scale)
        distributionChecks(gamma, expectedMean, expectedStd, 0.1)
    }
  }

  test("WeibullGenerator") {
    List((1.0, 2.0), (2.0, 3.0), (2.5, 3.5), (10.4, 2.222)).foreach {
      case (alpha: Double, beta: Double) =>
        val weibull = new WeibullGenerator(alpha, beta)
        apiChecks(weibull)

        val expectedMean = math.exp(Gamma.logGamma(1 + (1 / alpha))) * beta
        val expectedVariance = math.exp(
          Gamma.logGamma(1 + (2 / alpha))) * beta * beta - expectedMean * expectedMean
        val expectedStd = math.sqrt(expectedVariance)
        distributionChecks(weibull, expectedMean, expectedStd, 0.1)
    }
  }
}
