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

package org.apache.spark.mllib.stat.test

import org.apache.commons.math3.distribution.{NormalDistribution, RealDistribution}
import org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest

import org.apache.spark.rdd.RDD

/**
 * Conduct the two-sided Kolmogorov Smirnov test for data sampled from a
 * continuous distribution. By comparing the largest difference between the empirical cumulative
 * distribution of the sample data and the theoretical distribution we can provide a test for the
 * the null hypothesis that the sample data comes from that theoretical distribution.
 * For more information on KS Test: https://en.wikipedia.org/wiki/Kolmogorov%E2%80%93Smirnov_test
 *
 * Implementation note: We seek to implement the KS test with a minimal number of distributed
 * passes. We sort the RDD, and then perform the following operations on a per-partition basis:
 * calculate an empirical cumulative distribution value for each observation, and a theoretical
 * cumulative distribution value. We know the latter to be correct, while the former will be off by
 * a constant (how large the constant is depends on how many values precede it in other partitions).
 * However, given that this constant simply shifts the ECDF upwards, but doesn't change its shape,
 * and furthermore, that constant is the same within a given partition, we can pick 2 values
 * in each partition that can potentially resolve to the largest global distance. Namely, we
 * pick the minimum distance and the maximum distance. Additionally, we keep track of how many
 * elements are in each partition. Once these three values have been returned for every partition,
 * we can collect and operate locally. Locally, we can now adjust each distance by the appropriate
 * constant (the cumulative sum of # of elements in the prior partitions divided by the data set
 * size). Finally, we take the maximum absolute value, and this is the statistic.
 */
private[stat] object KSTest {

  // Null hypothesis for the type of KS test to be included in the result.
  object NullHypothesis extends Enumeration {
    type NullHypothesis = Value
    val oneSampleTwoSided = Value("Sample follows theoretical distribution.")
  }

  /**
   * Runs a KS test for 1 set of sample data, comparing it to a theoretical distribution
   * @param data `RDD[Double]` data on which to run test
   * @param cdf `Double => Double` function to calculate the theoretical CDF
   * @return KSTestResult summarizing the test results (pval, statistic, and null hypothesis)
   */
  def testOneSample(data: RDD[Double], cdf: Double => Double): KSTestResult = {
    val n = data.count().toDouble
    val localData = data.sortBy(x => x).mapPartitions { part =>
      val partDiffs = oneSampleDifferences(part, n, cdf) // local distances
      searchOneSampleCandidates(partDiffs) // candidates: local extrema
    }.collect()
    val ksStat = searchOneSampleStatistic(localData, n) // result: global extreme
    evalOneSampleP(ksStat, n.toLong)
  }

  /**
   * Runs a KS test for 1 set of sample data, comparing it to a theoretical distribution
   * @param data `RDD[Double]` data on which to run test
   * @param createDist `Unit => RealDistribution` function to create a theoretical distribution
   * @return KSTestResult summarizing the test results (pval, statistic, and null hypothesis)
   */
  def testOneSample(data: RDD[Double], createDist: () => RealDistribution): KSTestResult = {
    val n = data.count().toDouble
    val localData = data.sortBy(x => x).mapPartitions { part =>
      val partDiffs = oneSampleDifferences(part, n, createDist) // local distances
      searchOneSampleCandidates(partDiffs) // candidates: local extrema
    }.collect()
    val ksStat = searchOneSampleStatistic(localData, n) // result: global extreme
    evalOneSampleP(ksStat, n.toLong)
  }

  /**
   * Calculate unadjusted distances between the empirical CDF and the theoretical CDF in a
   * partition
   * @param partData `Iterator[Double]` 1 partition of a sorted RDD
   * @param n `Double` the total size of the RDD
   * @param cdf `Double => Double` a function the calculates the theoretical CDF of a value
   * @return `Iterator[Double] `Unadjusted (ie. off by a constant) differences between
   *        ECDF (empirical cumulative distribution function) and CDF. We subtract in such a way
   *        that when adjusted by the appropriate constant, the difference will be equivalent
   *        to the KS statistic calculation described in
   *        http://www.itl.nist.gov/div898/handbook/eda/section3/eda35g.htm
   *        where the difference is not exactly symmetric
   */
  private def oneSampleDifferences(partData: Iterator[Double], n: Double, cdf: Double => Double)
    : Iterator[Double] = {
    // zip data with index (within that partition)
    // calculate local (unadjusted) ECDF and subtract CDF
    partData.zipWithIndex.map { case (v, ix) =>
      // dp and dl are later adjusted by constant, when global info is available
      val dp = (ix + 1) / n
      val dl = ix / n
      val cdfVal = cdf(v)
      // if dp > cdfVal the adjusted dp is still above cdfVal, if dp < cdfVal
      // we want negative distance so that constant adjusted gives correct distance
      if (dp > cdfVal) dp - cdfVal else dl - cdfVal
    }
  }

  private def oneSampleDifferences(
      partData: Iterator[Double],
      n: Double,
      createDist: () => RealDistribution)
    : Iterator[Double] = {
    val dist = createDist()
    oneSampleDifferences(partData, n, x => dist.cumulativeProbability(x))
  }

  /**
   * Search the unadjusted differences between ECDF and CDF in a partition and return the
   * two extrema (furthest below and furthest above CDF), along with a count of elements in that
   * partition
   * @param partDiffs `Iterator[Double]` the unadjusted differences between ECDF and CDF in a
   *                 partition
   * @return `Iterator[(Double, Double, Double)]` the local extrema and a count of elements
   */
  private def searchOneSampleCandidates(partDiffs: Iterator[Double])
    : Iterator[(Double, Double, Double)] = {
    val initAcc = (Double.MaxValue, Double.MinValue, 0.0)
    val partResults = partDiffs.foldLeft(initAcc) { case ((pMin, pMax, pCt), currDiff) =>
      (Math.min(pMin, currDiff), Math.max(pMax, currDiff), pCt + 1)
    }
    Array(partResults).iterator
  }

  /**
   * Find the global maximum distance between ECDF and CDF (i.e. the KS Statistic) after adjusting
   * local extrema estimates from individual partitions with the amount of elements in preceding
   * partitions
   * @param localData `Array[(Double, Double, Double)]` A local array containing the collected
   *                 results of `searchOneSampleCandidates` across all partitions
   * @param n `Double`The size of the RDD
   * @return The one-sample Kolmogorov Smirnov Statistic
   */
  private def searchOneSampleStatistic(localData: Array[(Double, Double, Double)], n: Double)
    : Double = {
    val initAcc = (Double.MinValue, 0.0)
    // adjust differences based on the # of elements preceding it, which should provide
    // the correct distance between ECDF and CDF
    val results = localData.foldLeft(initAcc) { case ((prevMax, prevCt), (minCand, maxCand, ct)) =>
      val adjConst = prevCt / n
      val pdist1 = minCand + adjConst
      val pdist2 = maxCand + adjConst
      // adjust by 1 / N if pre-constant the value is less than cdf and post-constant
      // it is greater than or equal to the cdf
      val dist1 = if (pdist1 >= 0 && minCand < 0) pdist1 + 1 / n else Math.abs(pdist1)
      val dist2 = if (pdist2 >= 0 && maxCand < 0) pdist2 + 1 / n else Math.abs(pdist2)
      val maxVal = Array(prevMax, dist1, dist2).max
      (maxVal, prevCt + ct)
    }
    results._1
  }

  /**
   * A convenience function that allows running the KS test for 1 set of sample data against
   * a named distribution
   * @param data the sample data that we wish to evaluate
   * @param distName the name of the theoretical distribution
   * @return KSTestResult summarizing the test results (pval, statistic, and null hypothesis)
   */
  def testOneSample(data: RDD[Double], distName: String): KSTestResult = {
    val distanceCalc =
      distName match {
        case "stdnorm" => () => new NormalDistribution(0, 1)
        case  _ => throw new UnsupportedOperationException(s"$distName not yet supported through" +
          s" convenience method. Current options are:[stdnorm].")
      }

    testOneSample(data, distanceCalc)
  }

  private def evalOneSampleP(ksStat: Double, n: Long): KSTestResult = {
    val pval = 1 - new KolmogorovSmirnovTest().cdf(ksStat, n.toInt)
    new KSTestResult(pval, ksStat, NullHypothesis.oneSampleTwoSided.toString)
  }
}

