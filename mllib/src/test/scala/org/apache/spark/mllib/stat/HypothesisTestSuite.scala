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
import org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.mllib.linalg.{DenseVector, Matrices, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.test.ChiSqTest
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

    // Detect continuous features or labels
    val tooManyCategories: Int = 100000
    assert(tooManyCategories > ChiSqTest.maxCategories, "This unit test requires that " +
      "tooManyCategories be large enough to cause ChiSqTest to throw an exception.")
    val random = new Random(11L)
    val continuousLabel = Seq.fill(tooManyCategories)(
      LabeledPoint(random.nextDouble(), Vectors.dense(random.nextInt(2))))
    intercept[SparkException] {
      Statistics.chiSqTest(sc.parallelize(continuousLabel, 2))
    }
    val continuousFeature = Seq.fill(tooManyCategories)(
      LabeledPoint(random.nextInt(2), Vectors.dense(random.nextDouble())))
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
    val ksTest = new KolmogorovSmirnovTest()
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

  test("one sample Anderson-Darling test: R implementation equivalence") {
    /*
      Data generated with R
      > sessionInfo() #truncated
        R version 3.2.0 (2015-04-16)
        Platform: x86_64-apple-darwin13.4.0 (64-bit)
        Running under: OS X 10.10.2 (Yosemite)
      > set.seed(10)
      > dataNorm <- rnorm(20)
      > dataExp <- rexp(20)
      > dataUnif <- runif(20)
      > mean(dataNorm)
      [1] -0.06053267
      > sd(dataNorm)
      [1] 0.7999093
      > mean(dataExp)
      [1] 1.044636
      > sd(dataExp)
      [1] 0.96727
      > mean(dataUnif)
      [1] 0.4420219
      > sd(dataUnif)
      [1] 0.2593285
    */

    val dataNorm = sc.parallelize(
      Array(0.0187461709418264, -0.184252542069064, -1.37133054992251,
        -0.599167715783718, 0.294545126567508, 0.389794300700167, -1.20807617542949,
        -0.363676017470862, -1.62667268170309, -0.256478394123992, 1.10177950308713,
        0.755781508027337, -0.238233556018718, 0.98744470341339, 0.741390128383824,
        0.0893472664958216, -0.954943856152377, -0.195150384667239, 0.92552126209408,
        0.482978524836611)
    )

    val dataExp = sc.parallelize(
      Array(0.795082630547595, 1.39629918233218, 1.39810742601556, 1.11045944034578,
        0.170421596598791, 1.91878133072498, 0.166443939786404, 0.97028998914142, 0.010571192484349,
        2.79300971312409, 2.35461177957702, 0.667238388210535, 0.522243486717343, 0.146712897811085,
        0.751234306178963, 2.28856621111248, 0.0688535687513649, 0.282713153399527,
        0.0514786350540817, 3.02959313971882)
    )

    val dataUnif = sc.parallelize(
      Array(0.545859839767218, 0.372763097286224, 0.961302414536476, 0.257341569056734,
        0.207951683318242, 0.861382439732552, 0.464391982648522, 0.222867433447391,
        0.623549601528794, 0.203647700604051, 0.0196734135970473, 0.797993005951867,
        0.274318896699697, 0.166609104024246, 0.170151718193665, 0.4885059366934,
        0.711409077281132, 0.591934921452776, 0.517156876856461, 0.381627685856074)
    )

    /* normality test in R
      > library(nortest)
      > ad.test(dataNorm)
            Anderson-Darling normality test
      data:  dataNorm
      A = 0.27523, p-value = 0.6216
      > ad.test(dataExp)
             Anderson-Darling normality test
      data:  dataExp
      A = 0.79034, p-value = 0.03336
      > ad.test(dataUnif)
            Anderson-Darling normality test
      data:  dataUnif
      A = 0.31831, p-value = 0.5114
    */

    val rNormADStats = Map("norm" -> 0.27523, "exp" -> 0.79034, "unif" -> 0.31831)
    val params = Map(
      "norm" -> (-0.06053267, 0.7999093),
      "exp" -> (1.044636, 0.96727),
      "unif" -> (0.4420219, 0.2593285)
    )

    assert(
      Statistics.andersonDarlingTest(
        dataNorm,
        "norm",
        params("norm")._1,
        params("norm")._2
      ).statistic
        ~== rNormADStats("norm") relTol 1e-4
    )

    assert(
      Statistics.andersonDarlingTest(
        dataExp,
        "norm",
        params("exp")._1,
        params("exp")._2
      ).statistic
        ~== rNormADStats("exp") relTol 1e-4
    )

    assert(
      Statistics.andersonDarlingTest(
        dataUnif,
        "norm",
        params("unif")._1,
        params("unif")._2
      ).statistic
        ~== rNormADStats("unif") relTol 1e-4
    )
  }

  test("one sample Anderson-Darling test: SciPy implementation equivalence") {
    val dataNorm = sc.parallelize(
      Array(0.0187461709418264, -0.184252542069064, -1.37133054992251,
        -0.599167715783718, 0.294545126567508, 0.389794300700167, -1.20807617542949,
        -0.363676017470862, -1.62667268170309, -0.256478394123992, 1.10177950308713,
        0.755781508027337, -0.238233556018718, 0.98744470341339, 0.741390128383824,
        0.0893472664958216, -0.954943856152377, -0.195150384667239, 0.92552126209408,
        0.482978524836611)
    )

    val dataExp = sc.parallelize(
      Array(0.795082630547595, 1.39629918233218, 1.39810742601556, 1.11045944034578,
        0.170421596598791, 1.91878133072498, 0.166443939786404, 0.97028998914142, 0.010571192484349,
        2.79300971312409, 2.35461177957702, 0.667238388210535, 0.522243486717343, 0.146712897811085,
        0.751234306178963, 2.28856621111248, 0.0688535687513649, 0.282713153399527,
        0.0514786350540817, 3.02959313971882)
    )

    val params = Map("norm" -> (-0.06053267, 0.7999093))

    /*
      normality test in scipy: comparing critical values
      >>> from scipy.stats import anderson
      >>> # drop in values as arrays
      ...
      >>> anderson(dataNorm, "norm")
      (0.27523090925717852, array([ 0.506,  0.577,  0.692,  0.807,  0.96 ]),
      array([ 15. ,  10. ,   5. ,   2.5,   1. ]))
      >>> anderson(dataExp, "expon")
      (0.45714575153590431, array([ 0.895,  1.047,  1.302,  1.559,  1.9  ]),
      array([ 15. ,  10. ,   5. ,   2.5,   1. ]))
    */
    val sciPyNormCVs = Array(0.506, 0.577, 0.692, 0.807, 0.96)
    val adNormCVs = Statistics.andersonDarlingTest(
      dataNorm,
      "norm",
      params("norm")._1,
      params("norm")._2
    ).criticalValues.values.toArray

    assert(Vectors.dense(adNormCVs) ~== Vectors.dense(sciPyNormCVs) relTol 1e-3)

    val sciPyExpCVs = Array(0.895, 1.047, 1.302, 1.559, 1.9)
    val scaleParam = dataExp.mean()
    val adExpCVs = Statistics.andersonDarlingTest(dataExp, "exp", scaleParam).criticalValues
    assert(Vectors.dense(adExpCVs.values.toArray) ~== Vectors.dense(sciPyExpCVs) relTol 1e-3)

    /*
      >>> from scipy.stats.distributions import logistic
      >>> logistic.fit(dataExp)
      (0.93858397620886713, 0.55032469036705811)
      >>> anderson(dataExp, "logistic")
      (0.72718900969834621, array([ 0.421,  0.556,  0.652,  0.76 ,  0.895,  0.998]),
      array([ 25. ,  10. ,   5. ,   2.5,   1. ,   0.5]))
    */
    val sciPyLogADStat = 0.72718900969834621
    val sciPyLogCVs = Array(0.421, 0.556, 0.652, 0.76, 0.895, 0.998)
    val adTestExpLog = Statistics.andersonDarlingTest(
      dataExp,
      "logistic",
      0.93858397620886713,
      0.55032469036705811)

    assert(adTestExpLog.statistic ~== sciPyLogADStat relTol 1e-4)
    assert(
      Vectors.dense(adTestExpLog.criticalValues.values.toArray)
        ~== Vectors.dense(sciPyLogCVs) relTol 1e-3
    )
  }

  test("one sample Anderson-Darling test: gumbel and weibull") {
    val dataGumbel = sc.parallelize(
      Array(2.566070163268321, 2.797136867663637, 1.301680801603979, 4.968891406617431,
        2.736825109869105, 1.7156182506225437, 4.849728670708153, 2.2718140406461034,
        1.7349531624810886, 1.7220364354275257, 2.755987927298529, 1.5140172593783077,
        1.3178298892942695, 4.031431825825987, 2.062204842676713, 1.3800199221612661,
        3.4851421470537165, 1.8851564957011937, 3.6773776009634975, 2.5361429810121083)
    )
    val gumbelResult = Statistics.andersonDarlingTest(dataGumbel, "gumbel", 2, 1)
    assert(gumbelResult.statistic < gumbelResult.criticalValues(0.25))

    val dataWeibull = sc.parallelize(
      Array(0.14611490926735415, 0.7338331105666904, 1.3064941546142883, 0.8848645072661437,
        0.4041816812459189, 1.1742756450140364, 0.9327038477408471, 0.34414711566818174,
        1.7224814642768693, 0.9173020785734072, 0.6353718988539053, 1.657112759681775,
        0.5634766278998253, 1.2568957312640854, 1.7248031467826292, 0.8116808125903335,
        1.1108852360343489, 0.2983634954239019, 0.7063880887496812, 0.6382180292078736)
    )
    val weibullResult = Statistics.andersonDarlingTest(dataWeibull, "weibull", 2, 1)
    assert(weibullResult.statistic < weibullResult.criticalValues(0.25))
  }
}
