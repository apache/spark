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

import java.util.Random

import scala.collection.mutable.ArrayBuffer

import org.apache.commons.math3.distribution.PoissonDistribution
import org.scalatest.Matchers

import org.apache.spark.SparkFunSuite

class RandomSamplerSuite extends SparkFunSuite with Matchers {
  /**
   * My statistical testing methodology is to run a Kolmogorov-Smirnov (KS) test
   * between the random samplers and simple reference samplers (known to work correctly).
   * The sampling gap sizes between chosen samples should show up as having the same
   * distributions between test and reference, if things are working properly.  That is,
   * the KS test will fail to strongly reject the null hypothesis that the distributions of
   * sampling gaps are the same.
   * There are no actual KS tests implemented for scala (that I can find) - and so what I
   * have done here is pre-compute "D" - the KS statistic - that corresponds to a "weak"
   * p-value for a particular sample size.  I can then test that my measured KS stats
   * are less than D.  Computing D-values is easy, and implemented below.
   *
   * I used the scipy 'kstwobign' distribution to pre-compute my D value:
   *
   * def ksdval(q=0.1, n=1000):
   *     en = np.sqrt(float(n) / 2.0)
   *     return stats.kstwobign.isf(float(q)) / (en + 0.12 + 0.11 / en)
   *
   * When comparing KS stats I take the median of a small number of independent test runs
   * to compensate for the issue that any sampled statistic will show "false positive" with
   * some probability.  Even when two distributions are the same, they will register as
   * different 10% of the time at a p-value of 0.1
   */

  // This D value is the precomputed KS statistic for p-value 0.1, sample size 1000:
  val sampleSize = 1000
  val D = 0.0544280747619

  // I'm not a big fan of fixing seeds, but unit testing based on running statistical tests
  // will always fail with some nonzero probability, so I'll fix the seed to prevent these
  // tests from generating random failure noise in CI testing, etc.
  val rngSeed: Random = RandomSampler.newDefaultRNG
  rngSeed.setSeed(235711)

  // Reference implementation of sampling without replacement (bernoulli)
  def sample[T](data: Iterator[T], f: Double): Iterator[T] = {
    val rng: Random = RandomSampler.newDefaultRNG
    rng.setSeed(rngSeed.nextLong)
    data.filter(_ => (rng.nextDouble <= f))
  }

  // Reference implementation of sampling with replacement
  def sampleWR[T](data: Iterator[T], f: Double): Iterator[T] = {
    val rng = new PoissonDistribution(f)
    rng.reseedRandomGenerator(rngSeed.nextLong)
    data.flatMap { v => {
      val rep = rng.sample()
      if (rep == 0) Iterator.empty else Iterator.fill(rep)(v)
    }}
  }

  // Returns iterator over gap lengths between samples.
  // This function assumes input data is integers sampled from the sequence of
  // increasing integers: {0, 1, 2, ...}.  This works because that is how I generate them,
  // and the samplers preserve their input order
  def gaps(data: Iterator[Int]): Iterator[Int] = {
    data.sliding(2).withPartial(false).map { x => x(1) - x(0) }
  }

  // Returns the cumulative distribution from a histogram
  def cumulativeDist(hist: Array[Int]): Array[Double] = {
    val n = hist.sum.toDouble
    assert(n > 0.0)
    hist.scanLeft(0)(_ + _).drop(1).map { _.toDouble / n }
  }

  // Returns aligned cumulative distributions from two arrays of data
  def cumulants(d1: Array[Int], d2: Array[Int],
      ss: Int = sampleSize): (Array[Double], Array[Double]) = {
    assert(math.min(d1.length, d2.length) > 0)
    assert(math.min(d1.min, d2.min)  >=  0)
    val m = 1 + math.max(d1.max, d2.max)
    val h1 = Array.fill[Int](m)(0)
    val h2 = Array.fill[Int](m)(0)
    for (v <- d1) { h1(v) += 1 }
    for (v <- d2) { h2(v) += 1 }
    assert(h1.sum == h2.sum)
    assert(h1.sum == ss)
    (cumulativeDist(h1), cumulativeDist(h2))
  }

  // Computes the Kolmogorov-Smirnov 'D' statistic from two cumulative distributions
  def KSD(cdf1: Array[Double], cdf2: Array[Double]): Double = {
    assert(cdf1.length == cdf2.length)
    val n = cdf1.length
    assert(n > 0)
    assert(cdf1(n-1) == 1.0)
    assert(cdf2(n-1) == 1.0)
    cdf1.zip(cdf2).map { x => Math.abs(x._1 - x._2) }.max
  }

  // Returns the median KS 'D' statistic between two samples, over (m) sampling trials
  def medianKSD(data1: => Iterator[Int], data2: => Iterator[Int], m: Int = 5): Double = {
    val t = Array.fill[Double](m) {
      val (c1, c2) = cumulants(data1.take(sampleSize).toArray,
                               data2.take(sampleSize).toArray)
      KSD(c1, c2)
    }.sorted
    // return the median KS statistic
    t(m / 2)
  }

  def replacementSampling(data: Iterator[Int], sampler: PoissonSampler[Int]): Iterator[Int] = {
    data.flatMap { item =>
      val count = sampler.sample()
      if (count == 0) Iterator.empty else Iterator.fill(count)(item)
    }
  }

  test("utilities") {
    val s1 = Array(0, 1, 1, 0, 2)
    val s2 = Array(1, 0, 3, 2, 1)
    val (c1, c2) = cumulants(s1, s2, ss = 5)
    c1 should be (Array(0.4, 0.8, 1.0, 1.0))
    c2 should be (Array(0.2, 0.6, 0.8, 1.0))
    KSD(c1, c2) should be (0.2 +- 0.000001)
    KSD(c2, c1) should be (KSD(c1, c2))
    gaps(List(0, 1, 1, 2, 4, 11).iterator).toArray should be (Array(1, 0, 1, 2, 7))
  }

  test("sanity check medianKSD against references") {
    var d: Double = 0.0

    // should be statistically same, i.e. fail to reject null hypothesis strongly
    d = medianKSD(gaps(sample(Iterator.from(0), 0.5)), gaps(sample(Iterator.from(0), 0.5)))
    d should be < D

    // should be statistically different - null hypothesis will have high D value,
    // corresponding to low p-value that rejects the null hypothesis
    d = medianKSD(gaps(sample(Iterator.from(0), 0.4)), gaps(sample(Iterator.from(0), 0.5)))
    d should be > D

    // same!
    d = medianKSD(gaps(sampleWR(Iterator.from(0), 0.5)), gaps(sampleWR(Iterator.from(0), 0.5)))
    d should be < D

    // different!
    d = medianKSD(gaps(sampleWR(Iterator.from(0), 0.5)), gaps(sampleWR(Iterator.from(0), 0.6)))
    d should be > D
  }

  test("bernoulli sampling") {
    // Tests expect maximum gap sampling fraction to be this value
    RandomSampler.defaultMaxGapSamplingFraction should be (0.4)

    var d: Double = 0.0

    var sampler: RandomSampler[Int, Int] = new BernoulliSampler[Int](0.5)
    sampler.setSeed(rngSeed.nextLong)
    d = medianKSD(gaps(sampler.sample(Iterator.from(0))), gaps(sample(Iterator.from(0), 0.5)))
    d should be < D

    sampler = new BernoulliSampler[Int](0.7)
    sampler.setSeed(rngSeed.nextLong)
    d = medianKSD(gaps(sampler.sample(Iterator.from(0))), gaps(sample(Iterator.from(0), 0.7)))
    d should be < D

    sampler = new BernoulliSampler[Int](0.9)
    sampler.setSeed(rngSeed.nextLong)
    d = medianKSD(gaps(sampler.sample(Iterator.from(0))), gaps(sample(Iterator.from(0), 0.9)))
    d should be < D

    // sampling at different frequencies should show up as statistically different:
    sampler = new BernoulliSampler[Int](0.5)
    sampler.setSeed(rngSeed.nextLong)
    d = medianKSD(gaps(sampler.sample(Iterator.from(0))), gaps(sample(Iterator.from(0), 0.6)))
    d should be > D
  }

  test("bernoulli sampling without iterator") {
    // Tests expect maximum gap sampling fraction to be this value
    RandomSampler.defaultMaxGapSamplingFraction should be (0.4)

    var d: Double = 0.0

    val data = Iterator.from(0)

    var sampler: RandomSampler[Int, Int] = new BernoulliSampler[Int](0.5)
    sampler.setSeed(rngSeed.nextLong)
    d = medianKSD(gaps(data.filter(_ => sampler.sample() > 0)), gaps(sample(Iterator.from(0), 0.5)))
    d should be < D

    sampler = new BernoulliSampler[Int](0.7)
    sampler.setSeed(rngSeed.nextLong)
    d = medianKSD(gaps(data.filter(_ => sampler.sample() > 0)), gaps(sample(Iterator.from(0), 0.7)))
    d should be < D

    sampler = new BernoulliSampler[Int](0.9)
    sampler.setSeed(rngSeed.nextLong)
    d = medianKSD(gaps(data.filter(_ => sampler.sample() > 0)), gaps(sample(Iterator.from(0), 0.9)))
    d should be < D

    // sampling at different frequencies should show up as statistically different:
    sampler = new BernoulliSampler[Int](0.5)
    sampler.setSeed(rngSeed.nextLong)
    d = medianKSD(gaps(data.filter(_ => sampler.sample() > 0)), gaps(sample(Iterator.from(0), 0.6)))
    d should be > D
  }

  test("bernoulli sampling with gap sampling optimization") {
    // Tests expect maximum gap sampling fraction to be this value
    RandomSampler.defaultMaxGapSamplingFraction should be (0.4)

    var d: Double = 0.0

    var sampler: RandomSampler[Int, Int] = new BernoulliSampler[Int](0.01)
    sampler.setSeed(rngSeed.nextLong)
    d = medianKSD(gaps(sampler.sample(Iterator.from(0))), gaps(sample(Iterator.from(0), 0.01)))
    d should be < D

    sampler = new BernoulliSampler[Int](0.1)
    sampler.setSeed(rngSeed.nextLong)
    d = medianKSD(gaps(sampler.sample(Iterator.from(0))), gaps(sample(Iterator.from(0), 0.1)))
    d should be < D

    sampler = new BernoulliSampler[Int](0.3)
    sampler.setSeed(rngSeed.nextLong)
    d = medianKSD(gaps(sampler.sample(Iterator.from(0))), gaps(sample(Iterator.from(0), 0.3)))
    d should be < D

    // sampling at different frequencies should show up as statistically different:
    sampler = new BernoulliSampler[Int](0.3)
    sampler.setSeed(rngSeed.nextLong)
    d = medianKSD(gaps(sampler.sample(Iterator.from(0))), gaps(sample(Iterator.from(0), 0.4)))
    d should be > D
  }

  test("bernoulli sampling (without iterator) with gap sampling optimization") {
    // Tests expect maximum gap sampling fraction to be this value
    RandomSampler.defaultMaxGapSamplingFraction should be (0.4)

    var d: Double = 0.0

    val data = Iterator.from(0)

    var sampler: RandomSampler[Int, Int] = new BernoulliSampler[Int](0.01)
    sampler.setSeed(rngSeed.nextLong)
    d = medianKSD(gaps(data.filter(_ => sampler.sample() > 0)),
      gaps(sample(Iterator.from(0), 0.01)))
    d should be < D

    sampler = new BernoulliSampler[Int](0.1)
    sampler.setSeed(rngSeed.nextLong)
    d = medianKSD(gaps(data.filter(_ => sampler.sample() > 0)), gaps(sample(Iterator.from(0), 0.1)))
    d should be < D

    sampler = new BernoulliSampler[Int](0.3)
    sampler.setSeed(rngSeed.nextLong)
    d = medianKSD(gaps(data.filter(_ => sampler.sample() > 0)), gaps(sample(Iterator.from(0), 0.3)))
    d should be < D

    // sampling at different frequencies should show up as statistically different:
    sampler = new BernoulliSampler[Int](0.3)
    sampler.setSeed(rngSeed.nextLong)
    d = medianKSD(gaps(data.filter(_ => sampler.sample() > 0)), gaps(sample(Iterator.from(0), 0.4)))
    d should be > D
  }

  test("bernoulli boundary cases") {
    val data = (1 to 100).toArray

    var sampler = new BernoulliSampler[Int](0.0)
    sampler.sample(data.iterator).toArray should be (Array.empty[Int])

    sampler = new BernoulliSampler[Int](1.0)
    sampler.sample(data.iterator).toArray should be (data)

    sampler = new BernoulliSampler[Int](0.0 - (RandomSampler.roundingEpsilon / 2.0))
    sampler.sample(data.iterator).toArray should be (Array.empty[Int])

    sampler = new BernoulliSampler[Int](1.0 + (RandomSampler.roundingEpsilon / 2.0))
    sampler.sample(data.iterator).toArray should be (data)
  }

  test("bernoulli (without iterator) boundary cases") {
    val data = (1 to 100).toArray

    var sampler = new BernoulliSampler[Int](0.0)
    data.filter(_ => sampler.sample() > 0) should be (Array.empty[Int])

    sampler = new BernoulliSampler[Int](1.0)
    data.filter(_ => sampler.sample() > 0) should be (data)

    sampler = new BernoulliSampler[Int](0.0 - (RandomSampler.roundingEpsilon / 2.0))
    data.filter(_ => sampler.sample() > 0) should be (Array.empty[Int])

    sampler = new BernoulliSampler[Int](1.0 + (RandomSampler.roundingEpsilon / 2.0))
    data.filter(_ => sampler.sample() > 0) should be (data)
  }

  test("bernoulli data types") {
    // Tests expect maximum gap sampling fraction to be this value
    RandomSampler.defaultMaxGapSamplingFraction should be (0.4)

    var d: Double = 0.0
    var sampler = new BernoulliSampler[Int](0.1)
    sampler.setSeed(rngSeed.nextLong)

    // Array iterator (indexable type)
    d = medianKSD(
      gaps(sampler.sample(Iterator.from(0).take(20*sampleSize).toArray.iterator)),
      gaps(sample(Iterator.from(0), 0.1)))
    d should be < D

    // ArrayBuffer iterator (indexable type)
    d = medianKSD(
      gaps(sampler.sample(Iterator.from(0).take(20*sampleSize).to[ArrayBuffer].iterator)),
      gaps(sample(Iterator.from(0), 0.1)))
    d should be < D

    // List iterator (non-indexable type)
    d = medianKSD(
      gaps(sampler.sample(Iterator.from(0).take(20*sampleSize).toList.iterator)),
      gaps(sample(Iterator.from(0), 0.1)))
    d should be < D
  }

  test("bernoulli clone") {
    // Tests expect maximum gap sampling fraction to be this value
    RandomSampler.defaultMaxGapSamplingFraction should be (0.4)

    var d = 0.0
    var sampler = new BernoulliSampler[Int](0.1).clone
    sampler.setSeed(rngSeed.nextLong)
    d = medianKSD(gaps(sampler.sample(Iterator.from(0))), gaps(sample(Iterator.from(0), 0.1)))
    d should be < D

    sampler = new BernoulliSampler[Int](0.9).clone
    sampler.setSeed(rngSeed.nextLong)
    d = medianKSD(gaps(sampler.sample(Iterator.from(0))), gaps(sample(Iterator.from(0), 0.9)))
    d should be < D
  }

  test("bernoulli set seed") {
    RandomSampler.defaultMaxGapSamplingFraction should be (0.4)

    var d: Double = 0.0
    var sampler1 = new BernoulliSampler[Int](0.2)
    var sampler2 = new BernoulliSampler[Int](0.2)

    // distributions should be identical if seeds are set same
    sampler1.setSeed(73)
    sampler2.setSeed(73)
    d = medianKSD(gaps(sampler1.sample(Iterator.from(0))), gaps(sampler2.sample(Iterator.from(0))))
    d should be (0.0)

    // should be different for different seeds
    sampler1.setSeed(73)
    sampler2.setSeed(37)
    d = medianKSD(gaps(sampler1.sample(Iterator.from(0))), gaps(sampler2.sample(Iterator.from(0))))
    d should be > 0.0
    d should be < D

    sampler1 = new BernoulliSampler[Int](0.8)
    sampler2 = new BernoulliSampler[Int](0.8)

    // distributions should be identical if seeds are set same
    sampler1.setSeed(73)
    sampler2.setSeed(73)
    d = medianKSD(gaps(sampler1.sample(Iterator.from(0))), gaps(sampler2.sample(Iterator.from(0))))
    d should be (0.0)

    // should be different for different seeds
    sampler1.setSeed(73)
    sampler2.setSeed(37)
    d = medianKSD(gaps(sampler1.sample(Iterator.from(0))), gaps(sampler2.sample(Iterator.from(0))))
    d should be > 0.0
    d should be < D
  }

  test("replacement sampling") {
    // Tests expect maximum gap sampling fraction to be this value
    RandomSampler.defaultMaxGapSamplingFraction should be (0.4)

    var d: Double = 0.0

    var sampler = new PoissonSampler[Int](0.5)
    sampler.setSeed(rngSeed.nextLong)
    d = medianKSD(gaps(sampler.sample(Iterator.from(0))), gaps(sampleWR(Iterator.from(0), 0.5)))
    d should be < D

    sampler = new PoissonSampler[Int](0.7)
    sampler.setSeed(rngSeed.nextLong)
    d = medianKSD(gaps(sampler.sample(Iterator.from(0))), gaps(sampleWR(Iterator.from(0), 0.7)))
    d should be < D

    sampler = new PoissonSampler[Int](0.9)
    sampler.setSeed(rngSeed.nextLong)
    d = medianKSD(gaps(sampler.sample(Iterator.from(0))), gaps(sampleWR(Iterator.from(0), 0.9)))
    d should be < D

    // sampling at different frequencies should show up as statistically different:
    sampler = new PoissonSampler[Int](0.5)
    sampler.setSeed(rngSeed.nextLong)
    d = medianKSD(gaps(sampler.sample(Iterator.from(0))), gaps(sampleWR(Iterator.from(0), 0.6)))
    d should be > D
  }

  test("replacement sampling without iterator") {
    // Tests expect maximum gap sampling fraction to be this value
    RandomSampler.defaultMaxGapSamplingFraction should be (0.4)

    var d: Double = 0.0

    val data = Iterator.from(0)

    var sampler = new PoissonSampler[Int](0.5)
    sampler.setSeed(rngSeed.nextLong)
    d = medianKSD(gaps(replacementSampling(data, sampler)), gaps(sampleWR(Iterator.from(0), 0.5)))
    d should be < D

    sampler = new PoissonSampler[Int](0.7)
    sampler.setSeed(rngSeed.nextLong)
    d = medianKSD(gaps(replacementSampling(data, sampler)), gaps(sampleWR(Iterator.from(0), 0.7)))
    d should be < D

    sampler = new PoissonSampler[Int](0.9)
    sampler.setSeed(rngSeed.nextLong)
    d = medianKSD(gaps(replacementSampling(data, sampler)), gaps(sampleWR(Iterator.from(0), 0.9)))
    d should be < D

    // sampling at different frequencies should show up as statistically different:
    sampler = new PoissonSampler[Int](0.5)
    sampler.setSeed(rngSeed.nextLong)
    d = medianKSD(gaps(replacementSampling(data, sampler)), gaps(sampleWR(Iterator.from(0), 0.6)))
    d should be > D
  }

  test("replacement sampling with gap sampling") {
    // Tests expect maximum gap sampling fraction to be this value
    RandomSampler.defaultMaxGapSamplingFraction should be (0.4)

    var d: Double = 0.0

    var sampler = new PoissonSampler[Int](0.01)
    sampler.setSeed(rngSeed.nextLong)
    d = medianKSD(gaps(sampler.sample(Iterator.from(0))), gaps(sampleWR(Iterator.from(0), 0.01)))
    d should be < D

    sampler = new PoissonSampler[Int](0.1)
    sampler.setSeed(rngSeed.nextLong)
    d = medianKSD(gaps(sampler.sample(Iterator.from(0))), gaps(sampleWR(Iterator.from(0), 0.1)))
    d should be < D

    sampler = new PoissonSampler[Int](0.3)
    sampler.setSeed(rngSeed.nextLong)
    d = medianKSD(gaps(sampler.sample(Iterator.from(0))), gaps(sampleWR(Iterator.from(0), 0.3)))
    d should be < D

    // sampling at different frequencies should show up as statistically different:
    sampler = new PoissonSampler[Int](0.3)
    sampler.setSeed(rngSeed.nextLong)
    d = medianKSD(gaps(sampler.sample(Iterator.from(0))), gaps(sampleWR(Iterator.from(0), 0.4)))
    d should be > D
  }

  test("replacement sampling (without iterator) with gap sampling") {
    // Tests expect maximum gap sampling fraction to be this value
    RandomSampler.defaultMaxGapSamplingFraction should be (0.4)

    var d: Double = 0.0

    val data = Iterator.from(0)

    var sampler = new PoissonSampler[Int](0.01)
    sampler.setSeed(rngSeed.nextLong)
    d = medianKSD(gaps(replacementSampling(data, sampler)), gaps(sampleWR(Iterator.from(0), 0.01)))
    d should be < D

    sampler = new PoissonSampler[Int](0.1)
    sampler.setSeed(rngSeed.nextLong)
    d = medianKSD(gaps(replacementSampling(data, sampler)), gaps(sampleWR(Iterator.from(0), 0.1)))
    d should be < D

    sampler = new PoissonSampler[Int](0.3)
    sampler.setSeed(rngSeed.nextLong)
    d = medianKSD(gaps(replacementSampling(data, sampler)), gaps(sampleWR(Iterator.from(0), 0.3)))
    d should be < D

    // sampling at different frequencies should show up as statistically different:
    sampler = new PoissonSampler[Int](0.3)
    sampler.setSeed(rngSeed.nextLong)
    d = medianKSD(gaps(replacementSampling(data, sampler)), gaps(sampleWR(Iterator.from(0), 0.4)))
    d should be > D
  }

  test("replacement boundary cases") {
    val data = (1 to 100).toArray

    var sampler = new PoissonSampler[Int](0.0)
    sampler.sample(data.iterator).toArray should be (Array.empty[Int])

    sampler = new PoissonSampler[Int](0.0 - (RandomSampler.roundingEpsilon / 2.0))
    sampler.sample(data.iterator).toArray should be (Array.empty[Int])

    // sampling with replacement has no upper bound on sampling fraction
    sampler = new PoissonSampler[Int](2.0)
    sampler.sample(data.iterator).length should be > (data.length)
  }

  test("replacement (without) boundary cases") {
    val data = (1 to 100).toArray

    var sampler = new PoissonSampler[Int](0.0)
    replacementSampling(data.iterator, sampler).toArray should be (Array.empty[Int])

    sampler = new PoissonSampler[Int](0.0 - (RandomSampler.roundingEpsilon / 2.0))
    replacementSampling(data.iterator, sampler).toArray should be (Array.empty[Int])

    // sampling with replacement has no upper bound on sampling fraction
    sampler = new PoissonSampler[Int](2.0)
    replacementSampling(data.iterator, sampler).length should be > (data.length)
  }

  test("replacement data types") {
    // Tests expect maximum gap sampling fraction to be this value
    RandomSampler.defaultMaxGapSamplingFraction should be (0.4)

    var d: Double = 0.0
    var sampler = new PoissonSampler[Int](0.1)
    sampler.setSeed(rngSeed.nextLong)

    // Array iterator (indexable type)
    d = medianKSD(
      gaps(sampler.sample(Iterator.from(0).take(20*sampleSize).toArray.iterator)),
      gaps(sampleWR(Iterator.from(0), 0.1)))
    d should be < D

    // ArrayBuffer iterator (indexable type)
    d = medianKSD(
      gaps(sampler.sample(Iterator.from(0).take(20*sampleSize).to[ArrayBuffer].iterator)),
      gaps(sampleWR(Iterator.from(0), 0.1)))
    d should be < D

    // List iterator (non-indexable type)
    d = medianKSD(
      gaps(sampler.sample(Iterator.from(0).take(20*sampleSize).toList.iterator)),
      gaps(sampleWR(Iterator.from(0), 0.1)))
    d should be < D
  }

  test("replacement clone") {
    // Tests expect maximum gap sampling fraction to be this value
    RandomSampler.defaultMaxGapSamplingFraction should be (0.4)

    var d = 0.0
    var sampler = new PoissonSampler[Int](0.1).clone
    sampler.setSeed(rngSeed.nextLong)
    d = medianKSD(gaps(sampler.sample(Iterator.from(0))), gaps(sampleWR(Iterator.from(0), 0.1)))
    d should be < D

    sampler = new PoissonSampler[Int](0.9).clone
    sampler.setSeed(rngSeed.nextLong)
    d = medianKSD(gaps(sampler.sample(Iterator.from(0))), gaps(sampleWR(Iterator.from(0), 0.9)))
    d should be < D
  }

  test("replacement set seed") {
    RandomSampler.defaultMaxGapSamplingFraction should be (0.4)

    var d: Double = 0.0
    var sampler1 = new PoissonSampler[Int](0.2)
    var sampler2 = new PoissonSampler[Int](0.2)

    // distributions should be identical if seeds are set same
    sampler1.setSeed(73)
    sampler2.setSeed(73)
    d = medianKSD(gaps(sampler1.sample(Iterator.from(0))), gaps(sampler2.sample(Iterator.from(0))))
    d should be (0.0)

    // should be different for different seeds
    sampler1.setSeed(73)
    sampler2.setSeed(37)
    d = medianKSD(gaps(sampler1.sample(Iterator.from(0))), gaps(sampler2.sample(Iterator.from(0))))
    d should be > 0.0
    d should be < D

    sampler1 = new PoissonSampler[Int](0.8)
    sampler2 = new PoissonSampler[Int](0.8)

    // distributions should be identical if seeds are set same
    sampler1.setSeed(73)
    sampler2.setSeed(73)
    d = medianKSD(gaps(sampler1.sample(Iterator.from(0))), gaps(sampler2.sample(Iterator.from(0))))
    d should be (0.0)

    // should be different for different seeds
    sampler1.setSeed(73)
    sampler2.setSeed(37)
    d = medianKSD(gaps(sampler1.sample(Iterator.from(0))), gaps(sampler2.sample(Iterator.from(0))))
    d should be > 0.0
    d should be < D
  }

  test("bernoulli partitioning sampling") {
    var d: Double = 0.0

    var sampler = new BernoulliCellSampler[Int](0.1, 0.2)
    sampler.setSeed(rngSeed.nextLong)
    d = medianKSD(gaps(sampler.sample(Iterator.from(0))), gaps(sample(Iterator.from(0), 0.1)))
    d should be < D

    sampler = new BernoulliCellSampler[Int](0.1, 0.2, true)
    sampler.setSeed(rngSeed.nextLong)
    d = medianKSD(gaps(sampler.sample(Iterator.from(0))), gaps(sample(Iterator.from(0), 0.9)))
    d should be < D
  }

  test("bernoulli partitioning sampling without iterator") {
    var d: Double = 0.0

    val data = Iterator.from(0)

    var sampler = new BernoulliCellSampler[Int](0.1, 0.2)
    sampler.setSeed(rngSeed.nextLong)
    d = medianKSD(gaps(data.filter(_ => sampler.sample() > 0)), gaps(sample(Iterator.from(0), 0.1)))
    d should be < D

    sampler = new BernoulliCellSampler[Int](0.1, 0.2, true)
    sampler.setSeed(rngSeed.nextLong)
    d = medianKSD(gaps(data.filter(_ => sampler.sample() > 0)), gaps(sample(Iterator.from(0), 0.9)))
    d should be < D
  }

  test("bernoulli partitioning boundary cases") {
    val data = (1 to 100).toArray
    val d = RandomSampler.roundingEpsilon / 2.0

    var sampler = new BernoulliCellSampler[Int](0.0, 0.0)
    sampler.sample(data.iterator).toArray should be (Array.empty[Int])

    sampler = new BernoulliCellSampler[Int](0.5, 0.5)
    sampler.sample(data.iterator).toArray should be (Array.empty[Int])

    sampler = new BernoulliCellSampler[Int](1.0, 1.0)
    sampler.sample(data.iterator).toArray should be (Array.empty[Int])

    sampler = new BernoulliCellSampler[Int](0.0, 1.0)
    sampler.sample(data.iterator).toArray should be (data)

    sampler = new BernoulliCellSampler[Int](0.0 - d, 1.0 + d)
    sampler.sample(data.iterator).toArray should be (data)

    sampler = new BernoulliCellSampler[Int](0.5, 0.5 - d)
    sampler.sample(data.iterator).toArray should be (Array.empty[Int])
  }

  test("bernoulli partitioning (without iterator) boundary cases") {
    val data = (1 to 100).toArray
    val d = RandomSampler.roundingEpsilon / 2.0

    var sampler = new BernoulliCellSampler[Int](0.0, 0.0)
    data.filter(_ => sampler.sample() > 0).toArray should be (Array.empty[Int])

    sampler = new BernoulliCellSampler[Int](0.5, 0.5)
    data.filter(_ => sampler.sample() > 0).toArray should be (Array.empty[Int])

    sampler = new BernoulliCellSampler[Int](1.0, 1.0)
    data.filter(_ => sampler.sample() > 0).toArray should be (Array.empty[Int])

    sampler = new BernoulliCellSampler[Int](0.0, 1.0)
    data.filter(_ => sampler.sample() > 0).toArray should be (data)

    sampler = new BernoulliCellSampler[Int](0.0 - d, 1.0 + d)
    data.filter(_ => sampler.sample() > 0).toArray should be (data)

    sampler = new BernoulliCellSampler[Int](0.5, 0.5 - d)
    data.filter(_ => sampler.sample() > 0).toArray should be (Array.empty[Int])
  }

  test("bernoulli partitioning data") {
    val seed = rngSeed.nextLong
    val data = (1 to 100).toArray

    var sampler = new BernoulliCellSampler[Int](0.4, 0.6)
    sampler.setSeed(seed)
    val s1 = sampler.sample(data.iterator).toArray
    s1.length should be > 0

    sampler = new BernoulliCellSampler[Int](0.4, 0.6, true)
    sampler.setSeed(seed)
    val s2 = sampler.sample(data.iterator).toArray
    s2.length should be > 0

    (s1 ++ s2).sorted should be (data)

    sampler = new BernoulliCellSampler[Int](0.5, 0.5)
    sampler.sample(data.iterator).toArray should be (Array.empty[Int])

    sampler = new BernoulliCellSampler[Int](0.5, 0.5, true)
    sampler.sample(data.iterator).toArray should be (data)
  }

  test("bernoulli partitioning clone") {
    val seed = rngSeed.nextLong
    val data = (1 to 100).toArray
    val base = new BernoulliCellSampler[Int](0.35, 0.65)

    var sampler = base.clone
    sampler.setSeed(seed)
    val s1 = sampler.sample(data.iterator).toArray
    s1.length should be > 0

    sampler = base.cloneComplement
    sampler.setSeed(seed)
    val s2 = sampler.sample(data.iterator).toArray
    s2.length should be > 0

    (s1 ++ s2).sorted should be (data)
  }
}
