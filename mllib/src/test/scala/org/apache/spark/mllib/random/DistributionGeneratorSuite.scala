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

import org.scalatest.FunSuite

import org.apache.spark.util.StatCounter

// TODO update tests to use TestingUtils for floating point comparison after PR 1367 is merged
class DistributionGeneratorSuite extends FunSuite {

  def apiChecks(gen: DistributionGenerator) {

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

  def distributionChecks(gen: DistributionGenerator,
      mean: Double = 0.0,
      stddev: Double = 1.0,
      epsilon: Double = 0.01) {
    for (seed <- 0 until 5) {
      gen.setSeed(seed.toLong)
      val sample = (0 until 100000).map { _ => gen.nextValue()}
      val stats = new StatCounter(sample)
      assert(math.abs(stats.mean - mean) < epsilon)
      assert(math.abs(stats.stdev - stddev) < epsilon)
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

  test("PoissonGenerator") {
    // mean = 0.0 will not pass the API checks since 0.0 is always deterministically produced.
    for (mean <- List(1.0, 5.0, 100.0)) {
      val poisson = new PoissonGenerator(mean)
      apiChecks(poisson)
      distributionChecks(poisson, mean, math.sqrt(mean), 0.1)
    }
  }
}
