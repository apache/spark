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

package org.apache.spark.ml.stat

import org.apache.commons.math3.distribution.{ExponentialDistribution, NormalDistribution,
  RealDistribution, UniformRealDistribution}
import org.apache.commons.math3.stat.inference.{KolmogorovSmirnovTest => Math3KSTest}

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.Row
import org.apache.spark.util.ArrayImplicits._

class KolmogorovSmirnovTestSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import testImplicits._

  def apacheCommonMath3EquivalenceTest(
    sampleDist: RealDistribution,
    theoreticalDist: RealDistribution,
    theoreticalDistByName: (String, Array[Double]),
    rejectNullHypothesis: Boolean): Unit = {

    // set seeds
    val seed = 10L
    sampleDist.reseedRandomGenerator(seed)
    if (theoreticalDist != null) {
      theoreticalDist.reseedRandomGenerator(seed)
    }

    // Sample data from the distributions and parallelize it
    val n = 100000
    val sampledArray = sampleDist.sample(n)
    val sampledDF = sc.parallelize(sampledArray.toImmutableArraySeq, 10).toDF("sample")

    // Use a apache math commons local KS test to verify calculations
    val ksTest = new Math3KSTest()
    val pThreshold = 0.05

    // Comparing a standard normal sample to a standard normal distribution
    val Row(pValue1: Double, statistic1: Double) =
      if (theoreticalDist != null) {
        val cdf = (x: Double) => theoreticalDist.cumulativeProbability(x)
        KolmogorovSmirnovTest.test(sampledDF, "sample", cdf).head()
      } else {
        import org.apache.spark.util.ArrayImplicits._
        KolmogorovSmirnovTest.test(sampledDF, "sample",
          theoreticalDistByName._1,
          theoreticalDistByName._2.toImmutableArraySeq: _*
        ).head()
      }
    val theoreticalDistMath3 = if (theoreticalDist == null) {
      assert(theoreticalDistByName._1 == "norm")
      val params = theoreticalDistByName._2
      new NormalDistribution(params(0), params(1))
    } else {
      theoreticalDist
    }
    val referenceStat1 = ksTest.kolmogorovSmirnovStatistic(theoreticalDistMath3, sampledArray)
    val referencePVal1 = 1 - ksTest.cdf(referenceStat1, n)
    // Verify vs apache math commons ks test
    assert(statistic1 ~== referenceStat1 relTol 1e-4)
    assert(pValue1 ~== referencePVal1 relTol 1e-4)

    if (rejectNullHypothesis) {
      assert(pValue1 < pThreshold)
    } else {
      assert(pValue1 > pThreshold)
    }
  }

  test("1 sample Kolmogorov-Smirnov test: apache commons math3 implementation equivalence") {
    // Create theoretical distributions
    val stdNormalDist = new NormalDistribution(0.0, 1.0)
    val expDist = new ExponentialDistribution(0.6)
    val uniformDist = new UniformRealDistribution(0.0, 1.0)
    val expDist2 = new ExponentialDistribution(0.2)
    val stdNormByName = Tuple2("norm", Array(0.0, 1.0))

    apacheCommonMath3EquivalenceTest(stdNormalDist, null, stdNormByName, false)
    apacheCommonMath3EquivalenceTest(expDist, null, stdNormByName, true)
    apacheCommonMath3EquivalenceTest(uniformDist, null, stdNormByName, true)
    apacheCommonMath3EquivalenceTest(expDist, expDist2, null, true)
  }

  test("1 sample Kolmogorov-Smirnov test: R implementation equivalence") {
    /*
      Comparing results with R's implementation of Kolmogorov-Smirnov for 1 sample
      > sessionInfo()
      R version 3.2.0 (2015-04-16)
      Platform: x86_64-apple-darwin13.4.0 (64-bit)
      > set.seed(20)
      > v <- rnorm(20)
      > v
       [1]  1.16268529 -0.58592447  1.78546500 -1.33259371 -0.44656677  0.56960612
       [7] -2.88971761 -0.86901834 -0.46170268 -0.55554091 -0.02013537 -0.15038222
      [13] -0.62812676  1.32322085 -1.52135057 -0.43742787  0.97057758  0.02822264
      [19] -0.08578219  0.38921440
      > ks.test(v, pnorm, alternative = "two.sided")

               One-sample Kolmogorov-Smirnov test

      data:  v
      D = 0.18874, p-value = 0.4223
      alternative hypothesis: two-sided
    */

    val rKSStat = 0.18874
    val rKSPVal = 0.4223
    val rData = sc.parallelize(
      Array(
        1.1626852897838, -0.585924465893051, 1.78546500331661, -1.33259371048501,
        -0.446566766553219, 0.569606122374976, -2.88971761441412, -0.869018343326555,
        -0.461702683149641, -0.555540910137444, -0.0201353678515895, -0.150382224136063,
        -0.628126755843964, 1.32322085193283, -1.52135057001199, -0.437427868856691,
        0.970577579543399, 0.0282226444247749, -0.0857821886527593, 0.389214404984942
      ).toImmutableArraySeq
    ).toDF("sample")
    val Row(pValue: Double, statistic: Double) = KolmogorovSmirnovTest
      .test(rData, "sample", "norm", 0, 1).head()
    assert(statistic ~== rKSStat relTol 1e-4)
    assert(pValue ~== rKSPVal relTol 1e-4)
  }
}
