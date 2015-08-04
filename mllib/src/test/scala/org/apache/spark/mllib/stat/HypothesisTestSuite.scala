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

import java.util.Random

import org.apache.commons.math3.distribution.{ExponentialDistribution,
  NormalDistribution, UniformRealDistribution}
import org.apache.commons.math3.stat.inference.{KolmogorovSmirnovTest => CommonMathKolmogorovSmirnovTest}

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.mllib.linalg.{DenseVector, Matrices, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.test.{ChiSqTest, KolmogorovSmirnovTest}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._

class HypothesisTestSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("chi squared pearson goodness of fit") {

    val observed = new DenseVector(Array[Double](4, 6, 5))
    val pearson = Statistics.chiSqTest(observed)

    // Results validated against the R command `chisq.test(c(4, 6, 5), p=c(1/3, 1/3, 1/3))`
    assert(pearson.statistic === 0.4)
    assert(pearson.degreesOfFreedom === 2)
    assert(pearson.pValue ~== 0.8187 relTol 1e-4)
    assert(pearson.method === ChiSqTest.PEARSON.name)
    assert(pearson.nullHypothesis === ChiSqTest.NullHypothesis.goodnessOfFit.toString)

    // different expected and observed sum
    val observed1 = new DenseVector(Array[Double](21, 38, 43, 80))
    val expected1 = new DenseVector(Array[Double](3, 5, 7, 20))
    val pearson1 = Statistics.chiSqTest(observed1, expected1)

    // Results validated against the R command
    // `chisq.test(c(21, 38, 43, 80), p=c(3/35, 1/7, 1/5, 4/7))`
    assert(pearson1.statistic ~== 14.1429 relTol 1e-4)
    assert(pearson1.degreesOfFreedom === 3)
    assert(pearson1.pValue ~== 0.002717 relTol 1e-4)
    assert(pearson1.method === ChiSqTest.PEARSON.name)
    assert(pearson1.nullHypothesis === ChiSqTest.NullHypothesis.goodnessOfFit.toString)

    // Vectors with different sizes
    val observed3 = new DenseVector(Array(1.0, 2.0, 3.0))
    val expected3 = new DenseVector(Array(1.0, 2.0, 3.0, 4.0))
    intercept[IllegalArgumentException](Statistics.chiSqTest(observed3, expected3))

    // negative counts in observed
    val negObs = new DenseVector(Array(1.0, 2.0, 3.0, -4.0))
    intercept[IllegalArgumentException](Statistics.chiSqTest(negObs, expected1))

    // count = 0.0 in expected but not observed
    val zeroExpected = new DenseVector(Array(1.0, 0.0, 3.0))
    val inf = Statistics.chiSqTest(observed, zeroExpected)
    assert(inf.statistic === Double.PositiveInfinity)
    assert(inf.degreesOfFreedom === 2)
    assert(inf.pValue === 0.0)
    assert(inf.method === ChiSqTest.PEARSON.name)
    assert(inf.nullHypothesis === ChiSqTest.NullHypothesis.goodnessOfFit.toString)

    // 0.0 in expected and observed simultaneously
    val zeroObserved = new DenseVector(Array(2.0, 0.0, 1.0))
    intercept[IllegalArgumentException](Statistics.chiSqTest(zeroObserved, zeroExpected))
  }

  test("chi squared pearson matrix independence") {
    val data = Array(40.0, 24.0, 29.0, 56.0, 32.0, 42.0, 31.0, 10.0, 0.0, 30.0, 15.0, 12.0)
    // [[40.0, 56.0, 31.0, 30.0],
    //  [24.0, 32.0, 10.0, 15.0],
    //  [29.0, 42.0, 0.0,  12.0]]
    val chi = Statistics.chiSqTest(Matrices.dense(3, 4, data))
    // Results validated against R command
    // `chisq.test(rbind(c(40, 56, 31, 30),c(24, 32, 10, 15), c(29, 42, 0, 12)))`
    assert(chi.statistic ~== 21.9958 relTol 1e-4)
    assert(chi.degreesOfFreedom === 6)
    assert(chi.pValue ~== 0.001213 relTol 1e-4)
    assert(chi.method === ChiSqTest.PEARSON.name)
    assert(chi.nullHypothesis === ChiSqTest.NullHypothesis.independence.toString)

    // Negative counts
    val negCounts = Array(4.0, 5.0, 3.0, -3.0)
    intercept[IllegalArgumentException](Statistics.chiSqTest(Matrices.dense(2, 2, negCounts)))

    // Row sum = 0.0
    val rowZero = Array(0.0, 1.0, 0.0, 2.0)
    intercept[IllegalArgumentException](Statistics.chiSqTest(Matrices.dense(2, 2, rowZero)))

    // Column sum  = 0.0
    val colZero = Array(0.0, 0.0, 2.0, 2.0)
    // IllegalArgumentException thrown here since it's thrown on driver, not inside a task
    intercept[IllegalArgumentException](Statistics.chiSqTest(Matrices.dense(2, 2, colZero)))
  }

  test("chi squared pearson RDD[LabeledPoint]") {
    // labels: 1.0 (2 / 6), 0.0 (4 / 6)
    // feature1: 0.5 (1 / 6), 1.5 (2 / 6), 3.5 (3 / 6)
    // feature2: 10.0 (1 / 6), 20.0 (1 / 6), 30.0 (2 / 6), 40.0 (2 / 6)
    val data = Seq(
      LabeledPoint(0.0, Vectors.dense(0.5, 10.0)),
      LabeledPoint(0.0, Vectors.dense(1.5, 20.0)),
      LabeledPoint(1.0, Vectors.dense(1.5, 30.0)),
      LabeledPoint(0.0, Vectors.dense(3.5, 30.0)),
      LabeledPoint(0.0, Vectors.dense(3.5, 40.0)),
      LabeledPoint(1.0, Vectors.dense(3.5, 40.0)))
    for (numParts <- List(2, 4, 6, 8)) {
      val chi = Statistics.chiSqTest(sc.parallelize(data, numParts))
      val feature1 = chi(0)
      assert(feature1.statistic === 0.75)
      assert(feature1.degreesOfFreedom === 2)
      assert(feature1.pValue ~== 0.6873 relTol 1e-4)
      assert(feature1.method === ChiSqTest.PEARSON.name)
      assert(feature1.nullHypothesis === ChiSqTest.NullHypothesis.independence.toString)
      val feature2 = chi(1)
      assert(feature2.statistic === 1.5)
      assert(feature2.degreesOfFreedom === 3)
      assert(feature2.pValue ~== 0.6823 relTol 1e-4)
      assert(feature2.method === ChiSqTest.PEARSON.name)
      assert(feature2.nullHypothesis === ChiSqTest.NullHypothesis.independence.toString)
    }

    // Test that the right number of results is returned
    val numCols = 1001
    val sparseData = Array(
      new LabeledPoint(0.0, Vectors.sparse(numCols, Seq((100, 2.0)))),
      new LabeledPoint(0.1, Vectors.sparse(numCols, Seq((200, 1.0)))))
    val chi = Statistics.chiSqTest(sc.parallelize(sparseData))
    assert(chi.size === numCols)
    assert(chi(1000) != null) // SPARK-3087

    // Detect continous features or labels
    val random = new Random(11L)
    val continuousLabel =
      Seq.fill(100000)(LabeledPoint(random.nextDouble(), Vectors.dense(random.nextInt(2))))
    intercept[SparkException] {
      Statistics.chiSqTest(sc.parallelize(continuousLabel, 2))
    }
    val continuousFeature =
      Seq.fill(100000)(LabeledPoint(random.nextInt(2), Vectors.dense(random.nextDouble())))
    intercept[SparkException] {
      Statistics.chiSqTest(sc.parallelize(continuousFeature, 2))
    }
  }

  test("1 sample Kolmogorov-Smirnov test: apache commons math3 implementation equivalence") {
    // Create theoretical distributions
    val stdNormalDist = new NormalDistribution(0, 1)
    val expDist = new ExponentialDistribution(0.6)
    val unifDist = new UniformRealDistribution()

    // set seeds
    val seed = 10L
    stdNormalDist.reseedRandomGenerator(seed)
    expDist.reseedRandomGenerator(seed)
    unifDist.reseedRandomGenerator(seed)

    // Sample data from the distributions and parallelize it
    val n = 100000
    val sampledNorm = sc.parallelize(stdNormalDist.sample(n), 10)
    val sampledExp = sc.parallelize(expDist.sample(n), 10)
    val sampledUnif = sc.parallelize(unifDist.sample(n), 10)

    // Use a apache math commons local KS test to verify calculations
    val ksTest = new CommonMathKolmogorovSmirnovTest()
    val pThreshold = 0.05

    // Comparing a standard normal sample to a standard normal distribution
    val result1 = Statistics.kolmogorovSmirnovTest(sampledNorm, "norm", 0, 1)
    val referenceStat1 = ksTest.kolmogorovSmirnovStatistic(stdNormalDist, sampledNorm.collect())
    val referencePVal1 = 1 - ksTest.cdf(referenceStat1, n)
    // Verify vs apache math commons ks test
    assert(result1.statistic ~== referenceStat1 relTol 1e-4)
    assert(result1.pValue ~== referencePVal1 relTol 1e-4)
    // Cannot reject null hypothesis
    assert(result1.pValue > pThreshold)

    // Comparing an exponential sample to a standard normal distribution
    val result2 = Statistics.kolmogorovSmirnovTest(sampledExp, "norm", 0, 1)
    val referenceStat2 = ksTest.kolmogorovSmirnovStatistic(stdNormalDist, sampledExp.collect())
    val referencePVal2 = 1 - ksTest.cdf(referenceStat2, n)
    // verify vs apache math commons ks test
    assert(result2.statistic ~== referenceStat2 relTol 1e-4)
    assert(result2.pValue ~== referencePVal2 relTol 1e-4)
    // reject null hypothesis
    assert(result2.pValue < pThreshold)

    // Testing the use of a user provided CDF function
    // Distribution is not serializable, so will have to create in the lambda
    val expCDF = (x: Double) => new ExponentialDistribution(0.2).cumulativeProbability(x)

    // Comparing an exponential sample with mean X to an exponential distribution with mean Y
    // Where X != Y
    val result3 = Statistics.kolmogorovSmirnovTest(sampledExp, expCDF)
    val referenceStat3 = ksTest.kolmogorovSmirnovStatistic(new ExponentialDistribution(0.2),
      sampledExp.collect())
    val referencePVal3 = 1 - ksTest.cdf(referenceStat3, sampledNorm.count().toInt)
    // verify vs apache math commons ks test
    assert(result3.statistic ~== referenceStat3 relTol 1e-4)
    assert(result3.pValue ~== referencePVal3 relTol 1e-4)
    // reject null hypothesis
    assert(result3.pValue < pThreshold)
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
      )
    )
    val rCompResult = Statistics.kolmogorovSmirnovTest(rData, "norm", 0, 1)
    assert(rCompResult.statistic ~== rKSStat relTol 1e-4)
    assert(rCompResult.pValue ~== rKSPVal relTol 1e-4)
  }

  test("2 sample Kolmogorov-Smirnov test: apache commons math3 implementation equivalence") {
    // Create theoretical distributions
    val stdNormalDist = new NormalDistribution(0, 1)
    val normalDist = new NormalDistribution(2, 3)
    val expDist = new ExponentialDistribution(0.6)

    // create data samples and parallelize
    val n = 10000
    // local copies
    val sampledStdNorm1L = stdNormalDist.sample(n)
    val sampledStdNorm2L = stdNormalDist.sample(n)
    val sampledNormL = normalDist.sample(n)
    val sampledExpL = expDist.sample(n)
    // distributed
    val sampledStdNorm1P = sc.parallelize(sampledStdNorm1L, 10)
    val sampledStdNorm2P = sc.parallelize(sampledStdNorm2L, 10)
    val sampledNormP = sc.parallelize(sampledNormL, 10)
    val sampledExpP = sc.parallelize(sampledExpL, 10)

    // Use apache math commons local KS test to verify calculations
    val ksTest = new CommonMathKolmogorovSmirnovTest()
    val pThreshold = 0.05

    // Comparing 2 samples from same standard normal distribution
    val result1 = Statistics.kolmogorovSmirnovTest2Sample(sampledStdNorm1P, sampledStdNorm2P)
    val refStat1 = ksTest.kolmogorovSmirnovStatistic(sampledStdNorm1L, sampledStdNorm2L)
    val refP1 = ksTest.kolmogorovSmirnovTest(sampledStdNorm1L, sampledStdNorm2L)
    assert(result1.statistic ~== refStat1 relTol 1e-4)
    assert(result1.pValue ~== refP1 relTol 1e-4)
    assert(result1.pValue > pThreshold) // accept H0

    // Comparing 2 samples from different normal distributions
    val result2 = Statistics.kolmogorovSmirnovTest2Sample(sampledStdNorm1P, sampledNormP)
    val refStat2 = ksTest.kolmogorovSmirnovStatistic(sampledStdNorm1L, sampledNormL)
    val refP2 = ksTest.kolmogorovSmirnovTest(sampledStdNorm1L, sampledNormL)
    assert(result2.statistic ~== refStat2 relTol 1e-4)
    assert(result2.pValue ~== refP2 relTol 1e-4)
    assert(result2.pValue < pThreshold) // reject H0

    // Comparing 1 sample from normal distribution to 1 sample from exponential distribution
    val result3 = Statistics.kolmogorovSmirnovTest2Sample(sampledNormP, sampledExpP)
    val refStat3 = ksTest.kolmogorovSmirnovStatistic(sampledNormL, sampledExpL)
    val refP3 = ksTest.kolmogorovSmirnovTest(sampledNormL, sampledExpL)
    assert(result3.statistic ~== refStat3 relTol 1e-4)
    assert(result3.pValue ~== refP3 relTol 1e-4)
    assert(result3.pValue < pThreshold) // reject H0
  }

  test("2 sample Kolmogorov-Smirnov test: R implementation equivalence") {
    /*
     Comparing results with the R implementation of KS
     > sessionInfo()
     R version 3.2.0 (2015-04-16)
     Platform: x86_64-apple-darwin13.4.0 (64-bit)
     > set.seed(20)
     > v <- rnorm(20)
     > v2 <- rnorm(20)
     > v
      [1]  1.16268529 -0.58592447  1.78546500 -1.33259371 -0.44656677  0.56960612
      [7] -2.88971761 -0.86901834 -0.46170268 -0.55554091 -0.02013537 -0.15038222
     [13] -0.62812676  1.32322085 -1.52135057 -0.43742787  0.97057758  0.02822264
     [19] -0.08578219  0.38921440
     > v2
      [1]  0.23668737 -0.14444023  0.72222970  0.36990686 -0.24206631 -1.47206332
      [7] -0.59615955 -1.14670013 -2.47463643 -0.61350858 -0.21631151  1.59014577
     [13]  1.55614328  1.10845089 -1.09734184 -1.86060572 -0.91357885  1.24556891
     [19]  0.08785472  0.42348190
    */
    val rData1 = sc.parallelize(
      Array(
        1.1626852897838, -0.585924465893051, 1.78546500331661, -1.33259371048501,
        -0.446566766553219, 0.569606122374976, -2.88971761441412, -0.869018343326555,
        -0.461702683149641, -0.555540910137444, -0.0201353678515895, -0.150382224136063,
        -0.628126755843964, 1.32322085193283, -1.52135057001199, -0.437427868856691,
        0.970577579543399, 0.0282226444247749, -0.0857821886527593, 0.389214404984942
      )
    )

    val rData2 = sc.parallelize(
      Array(
        0.236687367712904, -0.144440226694072, 0.722229700806146, 0.369906857410192,
        -0.242066314481781, -1.47206331842053, -0.596159545765696, -1.1467001312186,
        -2.47463643305885, -0.613508578410268, -0.216311514038102, 1.5901457684867,
        1.55614327565194, 1.10845089348356, -1.09734184488477, -1.86060571637755,
        -0.913578847977252, 1.24556891198713, 0.0878547183607045, 0.423481895050245
      )
    )

    val rKSStat = 0.15
    val rKSPval = 0.9831
    val kSCompResult = Statistics.kolmogorovSmirnovTest2Sample(rData1, rData2)
    assert(kSCompResult.statistic ~== rKSStat relTol 1e-4)
    // we're more lenient with the p-value here since the approximate p-value calculated
    // by apache math commons is likely to be slightly off given the small sample size
    assert(kSCompResult.pValue ~== rKSPval relTol 1e-2)
  }

  test("2 sample Kolmogorov-Smirnov test: helper functions in case partitions have no data") {
    // we use the R data provided in the prior test
    // Once we have combined and sorted we partitino with a larger number than
    // the number of elements to guarantee we have empty partitions.
    // We test various critical package private functions in this circumstance.
    val rData1 = Array(
        1.1626852897838, -0.585924465893051, 1.78546500331661, -1.33259371048501,
        -0.446566766553219, 0.569606122374976, -2.88971761441412, -0.869018343326555,
        -0.461702683149641, -0.555540910137444, -0.0201353678515895, -0.150382224136063,
        -0.628126755843964, 1.32322085193283, -1.52135057001199, -0.437427868856691,
        0.970577579543399, 0.0282226444247749, -0.0857821886527593, 0.389214404984942
      )

    val rData2 = Array(
        0.236687367712904, -0.144440226694072, 0.722229700806146, 0.369906857410192,
        -0.242066314481781, -1.47206331842053, -0.596159545765696, -1.1467001312186,
        -2.47463643305885, -0.613508578410268, -0.216311514038102, 1.5901457684867,
        1.55614327565194, 1.10845089348356, -1.09734184488477, -1.86060571637755,
        -0.913578847977252, 1.24556891198713, 0.0878547183607045, 0.423481895050245
      )


    val n1 = rData1.length
    val n2 = rData2.length
    val unioned = (rData1.map((_, true)) ++ rData2.map((_, false))).sortBy(_._1)
    val parallel = sc.parallelize(unioned, 100)
    // verify that there are empty partitions
    assert(parallel.mapPartitions(x => Array(x.size).iterator).collect().contains(0))
    val localExtrema = parallel.mapPartitions(
      KolmogorovSmirnovTest.searchTwoSampleCandidates(_, n1, n2)
    ).collect()
    val ksCompStat = KolmogorovSmirnovTest.searchTwoSampleStatistic(localExtrema, n1 * n2)

    val rKSStat = 0.15
    assert(ksCompStat ~== rKSStat relTol 1e-4)
  }

  test("2 sample Kolmogorov-Smirnov test: helper functions in case partitions have only 1 sample") {
    // Creating 2 samples that don't overlap and request a large number of partitions to guarantee
    // that there will be partitions with only data from 1 sample. We test critical helper
    // functions in these circumstances.
    val n = 100
    val lower = (1 to n).toArray.map(_.toDouble)
    val upper = (1 to n).toArray.map(n + _.toDouble * 100)

    val unioned = (lower.map((_, true)) ++ upper.map((_, false))).sortBy(_._1)
    val parallel = sc.parallelize(unioned, 200)
    // verify that there is at least 1 partition with only 1 sample
    assert(parallel.mapPartitions(x =>
      Array(x.toArray.map(_._1).distinct.length).iterator
      ).collect().contains(1)
    )
    val localExtrema = parallel.mapPartitions(
      KolmogorovSmirnovTest.searchTwoSampleCandidates(_, n, n)
    ).collect()
    val ksCompStat = KolmogorovSmirnovTest.searchTwoSampleStatistic(localExtrema, n * n)

    // Use apache math commons local KS test to verify calculations
    val ksTest = new CommonMathKolmogorovSmirnovTest()

    val refStat4 = ksTest.kolmogorovSmirnovStatistic(lower, upper)
    assert(ksCompStat ~== refStat4 relTol 1e-3)
  }
}
