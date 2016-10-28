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

import scala.annotation.varargs

import org.apache.commons.math3.distribution.{NormalDistribution, RealDistribution}
import org.apache.commons.math3.stat.inference.{KolmogorovSmirnovTest => CommonMathKolmogorovSmirnovTest}

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

/**
 * Conduct the two-sided Kolmogorov Smirnov (KS) test for data sampled from a
 * continuous distribution. By comparing the largest difference between the empirical cumulative
 * distribution of the sample data and the theoretical distribution we can provide a test for the
 * the null hypothesis that the sample data comes from that theoretical distribution.
 * For more information on KS Test:
 * @see [[https://en.wikipedia.org/wiki/Kolmogorov%E2%80%93Smirnov_test]]
 *
 * Implementation note: We seek to implement the KS test with a minimal number of distributed
 * passes. We sort the RDD, and then perform the following operations on a per-partition basis:
 * calculate an empirical cumulative distribution value for each observation, and a theoretical
 * cumulative distribution value. We know the latter to be correct, while the former will be off by
 * a constant (how large the constant is depends on how many values precede it in other partitions).
 * However, given that this constant simply shifts the empirical CDF upwards, but doesn't
 * change its shape, and furthermore, that constant is the same within a given partition, we can
 * pick 2 values in each partition that can potentially resolve to the largest global distance.
 * Namely, we pick the minimum distance and the maximum distance. Additionally, we keep track of how
 * many elements are in each partition. Once these three values have been returned for every
 * partition, we can collect and operate locally. Locally, we can now adjust each distance by the
 * appropriate constant (the cumulative sum of number of elements in the prior partitions divided by
 * the data set size). Finally, we take the maximum absolute value, and this is the statistic.
 */
private[stat] object KolmogorovSmirnovTest extends Logging {

  // Null hypothesis for the type of KS test to be included in the result.
  object NullHypothesis extends Enumeration {
    type NullHypothesis = Value
    val OneSampleTwoSided = Value("Sample follows theoretical distribution")
  }

  /**
   * Runs a KS test for 1 set of sample data, comparing it to a theoretical distribution
   * @param data `RDD[Double]` data on which to run test
   * @param cdf `Double => Double` function to calculate the theoretical CDF
   * @return [[org.apache.spark.mllib.stat.test.KolmogorovSmirnovTestResult]] summarizing the test
   *        results (p-value, statistic, and null hypothesis)
   */
  def testOneSample(data: RDD[Double], cdf: Double => Double): KolmogorovSmirnovTestResult = {
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
   * @param distObj `RealDistribution` a theoretical distribution
   * @return [[org.apache.spark.mllib.stat.test.KolmogorovSmirnovTestResult]] summarizing the test
   *        results (p-value, statistic, and null hypothesis)
   */
  def testOneSample(data: RDD[Double], distObj: RealDistribution): KolmogorovSmirnovTestResult = {
    val cdf = (x: Double) => distObj.cumulativeProbability(x)
    testOneSample(data, cdf)
  }

  /**
   * Calculate unadjusted distances between the empirical CDF and the theoretical CDF in a
   * partition
   * @param partData `Iterator[Double]` 1 partition of a sorted RDD
   * @param n `Double` the total size of the RDD
   * @param cdf `Double => Double` a function the calculates the theoretical CDF of a value
   * @return `Iterator[(Double, Double)] `Unadjusted (ie. off by a constant) potential extrema
   *        in a partition. The first element corresponds to the (empirical CDF - 1/N) - CDF,
   *        the second element corresponds to empirical CDF - CDF.  We can then search the resulting
   *        iterator for the minimum of the first and the maximum of the second element, and provide
   *        this as a partition's candidate extrema
   */
  private def oneSampleDifferences(partData: Iterator[Double], n: Double, cdf: Double => Double)
    : Iterator[(Double, Double)] = {
    // zip data with index (within that partition)
    // calculate local (unadjusted) empirical CDF and subtract CDF
    partData.zipWithIndex.map { case (v, ix) =>
      // dp and dl are later adjusted by constant, when global info is available
      val dp = (ix + 1) / n
      val dl = ix / n
      val cdfVal = cdf(v)
      (dl - cdfVal, dp - cdfVal)
    }
  }

  /**
   * Search the unadjusted differences in a partition and return the
   * two extrema (furthest below and furthest above CDF), along with a count of elements in that
   * partition
   * @param partDiffs `Iterator[(Double, Double)]` the unadjusted differences between empirical CDF
   *                 and CDFin a partition, which come as a tuple of
   *                 (empirical CDF - 1/N - CDF, empirical CDF - CDF)
   * @return `Iterator[(Double, Double, Double)]` the local extrema and a count of elements
   */
  private def searchOneSampleCandidates(partDiffs: Iterator[(Double, Double)])
    : Iterator[(Double, Double, Double)] = {
    val initAcc = (Double.MaxValue, Double.MinValue, 0.0)
    val pResults = partDiffs.foldLeft(initAcc) { case ((pMin, pMax, pCt), (dl, dp)) =>
      (math.min(pMin, dl), math.max(pMax, dp), pCt + 1)
    }
    val results =
      if (pResults == initAcc) Array.empty[(Double, Double, Double)] else Array(pResults)
    results.iterator
  }

  /**
   * Find the global maximum distance between empirical CDF and CDF (i.e. the KS statistic) after
   * adjusting local extrema estimates from individual partitions with the amount of elements in
   * preceding partitions
   * @param localData `Array[(Double, Double, Double)]` A local array containing the collected
   *                 results of `searchOneSampleCandidates` across all partitions
   * @param n `Double`The size of the RDD
   * @return The one-sample Kolmogorov Smirnov Statistic
   */
  private def searchOneSampleStatistic(localData: Array[(Double, Double, Double)], n: Double)
    : Double = {
    val initAcc = (Double.MinValue, 0.0)
    // adjust differences based on the number of elements preceding it, which should provide
    // the correct distance between empirical CDF and CDF
    val results = localData.foldLeft(initAcc) { case ((prevMax, prevCt), (minCand, maxCand, ct)) =>
      val adjConst = prevCt / n
      val dist1 = math.abs(minCand + adjConst)
      val dist2 = math.abs(maxCand + adjConst)
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
   * @param params Variable length parameter for distribution's parameters
   * @return [[org.apache.spark.mllib.stat.test.KolmogorovSmirnovTestResult]] summarizing the
   *        test results (p-value, statistic, and null hypothesis)
   */
  @varargs
  def testOneSample(data: RDD[Double], distName: String, params: Double*)
    : KolmogorovSmirnovTestResult = {
    val distObj =
      distName match {
        case "norm" =>
          if (params.nonEmpty) {
            // parameters are passed, then can only be 2
            require(params.length == 2, "Normal distribution requires mean and standard " +
              "deviation as parameters")
            new NormalDistribution(params(0), params(1))
          } else {
            // if no parameters passed in initializes to standard normal
            logInfo("No parameters specified for normal distribution," +
              "initialized to standard normal (i.e. N(0, 1))")
            new NormalDistribution(0, 1)
          }
        case  _ => throw new UnsupportedOperationException(s"$distName not yet supported through" +
          s" convenience method. Current options are:['norm'].")
      }

    testOneSample(data, distObj)
  }

  private def evalOneSampleP(ksStat: Double, n: Long): KolmogorovSmirnovTestResult = {
    val pval = 1 - new CommonMathKolmogorovSmirnovTest().cdf(ksStat, n.toInt)
    new KolmogorovSmirnovTestResult(pval, ksStat, NullHypothesis.OneSampleTwoSided.toString)
  }
}

