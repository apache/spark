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

import collection.immutable.ListMap

import org.apache.commons.math3.distribution.{ExponentialDistribution, GumbelDistribution,
  LogisticDistribution, NormalDistribution, WeibullDistribution}

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD

/**
 * The Anderson-Darling (AD) test, similarly to the Kolmogorov-Smirnov (KS) test, tests whether the
 * data follow a given theoretical distribution. It should be used with continuous data and
 * assumes that no repeated values occur (the presence of ties can affect the validity of the test).
 * The AD test provides an alternative to the KS test. Namely, it is better
 * suited to identify departures from the theoretical distribution at the tails.
 * It is worth noting that the the AD test's critical values depend on the
 * distribution being tested against. The AD statistic is defined as
 * {{{
 * A^2 = -N - \frac{1}{N}\sum_{i = 0}^{N} (2i + 1)(\ln{\Phi{(x_i)}} + \ln{(1 - \Phi{(x_{N+1-i})})
 * }}}
 * where {{{\Phi}}} is the CDF of the given distribution and `N` is the sample size.
 * For more information @see[[https://en.wikipedia.org/wiki/Anderson%E2%80%93Darling_test]]
 */
private[stat] object AndersonDarlingTest extends Logging {

  object NullHypothesis extends Enumeration {
    type NullHypothesis = Value
    val OneSample = Value("Sample follows theoretical distribution.")
  }

  /**
   * AndersonDarlingTheoreticalDist is a trait that every distribution used in an AD test must
   * extend. The rationale for this is that the AD test has distribution-dependent critical values,
   * and by requiring extension of this trait we guarantee that future additional distributions
   * make sure to add the appropriate critical values (CVs) (or at least acknowledge
   * that they should be added)
   */
  sealed trait AndersonDarlingTheoreticalDist extends Serializable {
    // parameters used to initialized the distribution
    val params: Seq[Double]
    // calculate the cdf under the given distribution for value x
    def cdf(x: Double): Double
    // return appropriate CVs, adjusted for sample size
    def getCriticalValues(n: Double): Map[Double, Double]
  }

  /**
   * Critical values and adjustments for distributions sourced from
   * [[http://civil.colorado.edu/~balajir/CVEN5454/lectures/Ang-n-Tang-Chap7-Goodness-of-fit-PDFs-
   * test.pdf]]
   * [[https://github.com/scipy/scipy/blob/v0.15.1/scipy/stats/morestats.py#L1017]], which in turn
   * references:
   *
   * Stephens, M. A. (1974). EDF Statistics for Goodness of Fit and
   * Some Comparisons, Journal of the American Statistical Association,
   * Vol. 69, pp. 730-737.
   *
   * Stephens, M. A. (1976). Asymptotic Results for Goodness-of-Fit
   * Statistics with Unknown Parameters, Annals of Statistics, Vol. 4,
   * pp. 357-369.
   *
   * Stephens, M. A. (1977). Goodness of Fit for the Extreme Value
   * Distribution, Biometrika, Vol. 64, pp. 583-588.
   *
   * Stephens, M. A. (1977). Goodness of Fit with Special Reference
   * to Tests for Exponentiality , Technical Report No. 262,
   * Department of Statistics, Stanford University, Stanford, CA.
   *
   * Stephens, M. A. (1979). Tests of Fit for the Logistic Distribution
   * Based on the Empirical Distribution Function, Biometrika, Vol. 66,
   * pp. 591-595.
   */

  // Exponential distribution
  class AndersonDarlingExponential(val params: Seq[Double]) extends AndersonDarlingTheoreticalDist {
    private val theoretical = new ExponentialDistribution(params(0))

    private val rawCriticalValues = ListMap(
      0.15 -> 0.922, 0.10 -> 1.078, 0.05 -> 1.341, 0.025 -> 1.606, 0.01 -> 1.957
    )

    def cdf(x: Double): Double = theoretical.cumulativeProbability(x)

    def getCriticalValues(n: Double): Map[Double, Double] = {
      rawCriticalValues.map { case (sig, cv) => sig -> cv / (1 + 0.6 / n) }
    }
  }

  // Normal Distribution
  class AndersonDarlingNormal(val params: Seq[Double]) extends AndersonDarlingTheoreticalDist {
    private val theoretical = new NormalDistribution(params(0), params(1))

    private val rawCriticalValues = ListMap(
      0.15 -> 0.576, 0.10 -> 0.656, 0.05 -> 0.787, 0.025 -> 0.918, 0.01 -> 1.092
    )

    def cdf(x: Double): Double = theoretical.cumulativeProbability(x)

    def getCriticalValues(n: Double): Map[Double, Double] = {
      rawCriticalValues.map { case (sig, cv) => sig -> cv / (1 + 4.0 / n - 25.0 / (n * n)) }
    }
  }

  // Gumbel distribution
  class AndersonDarlingGumbel(val params: Seq[Double]) extends AndersonDarlingTheoreticalDist {
    private val theoretical = new GumbelDistribution(params(0), params(1))

    private val rawCriticalValues = ListMap(
      0.25 -> 0.474, 0.10 -> 0.637, 0.05 -> 0.757, 0.025 -> 0.877, 0.01 -> 1.038
    )

    def cdf(x: Double): Double = theoretical.cumulativeProbability(x)

    def getCriticalValues(n: Double): Map[Double, Double] = {
      rawCriticalValues.map { case (sig, cv) => sig -> cv / (1 + 0.2 / math.sqrt(n)) }
    }
  }

  // Logistic distribution
  class AndersonDarlingLogistic(val params: Seq[Double]) extends AndersonDarlingTheoreticalDist {
    private val theoretical = new LogisticDistribution(params(0), params(1))

    private val rawCriticalValues = ListMap(
      0.25 -> 0.426, 0.10 -> 0.563, 0.05 -> 0.660, 0.025 -> 0.769, 0.01 -> 0.906, 0.005 -> 1.010
    )

    def cdf(x: Double): Double = theoretical.cumulativeProbability(x)

    def getCriticalValues(n: Double): Map[Double, Double] = {
      rawCriticalValues.map { case (sig, cv) => sig -> cv / (1 + 0.25 / n) }
    }
  }

  // Weibull distribution
  class AndersonDarlingWeibull(val params: Seq[Double]) extends AndersonDarlingTheoreticalDist {
    private val theoretical = new WeibullDistribution(params(0), params(1))

    private val rawCriticalValuess = ListMap(
      0.25 -> 0.474, 0.10 -> 0.637, 0.05 -> 0.757, 0.025 -> 0.877, 0.01 -> 1.038
    )

    def cdf(x: Double): Double = theoretical.cumulativeProbability(x)

    def getCriticalValues(n: Double): Map[Double, Double] = {
      rawCriticalValuess.map { case (sig, cv) => sig -> cv / (1 + 0.2 / math.sqrt(n)) }
    }
  }

  /**
   * Perform a one sample Anderson-Darling test
   * @param data data to test for a given distribution
   * @param distName name of theoretical distribution: currently supports normal,
   *            exponential, gumbel, logistic, weibull as
   *            ['norm', 'exp', 'gumbel', 'logistic', 'weibull']
   * @param params variable-length argument providing parameters for given distribution. When none
   *               are provided, default parameters appropriate to each distribution are chosen. In
   *               either case, critical values reflect adjustments that assume the parameters were
   *               estimated from the data
   * @return
   */
  @varargs
  def testOneSample(data: RDD[Double], distName: String, params: Double*)
    : AndersonDarlingTestResult = {
    val n = data.count()
    val dist = initDist(distName, params)
    val localData = data.sortBy(x => x).mapPartitions(calcPartAD(_, dist, n)).collect()
    val s = localData.foldLeft((0.0, 0.0)) { case ((prevStat, prevCt), (rawStat, adj, ct)) =>
      val adjVal = 2 * prevCt * adj
      val adjustedStat = rawStat + adjVal
      val cumCt = prevCt + ct
      (prevStat + adjustedStat, cumCt)
    }._1
    val ADStat = -1 * n - s / n
    val criticalVals = dist.getCriticalValues(n)
    new AndersonDarlingTestResult(ADStat, criticalVals, NullHypothesis.OneSample.toString)
  }


  /**
   * Calculate a partition's contribution to the Anderson-Darling statistic.
   * In each partition we calculate 2 values, an unadjusted value that is contributed to the AD
   * statistic directly, a value that must be adjusted by the number of values in the prior
   * partitions, and a count of the elements in that partition
   * @param part a partition of the data sample to be analyzed
   * @param dist a theoretical distribution that extends the AndersonDarlingTheoreticalDist trait,
   *             used to calculate CDF values and critical values
   * @param n the total size of the data sample
   * @return The first element corresponds to the position-independent contribution to the
   *         statistic, the second is the value that must be scaled by the number of elements in
   *         prior partitions, and the third is the number of elements in this partition
   */
  private def calcPartAD(part: Iterator[Double], dist: AndersonDarlingTheoreticalDist, n: Double)
    : Iterator[(Double, Double, Double)] = {
    val initAcc = (0.0, 0.0, 0.0)
    val pResult = part.zipWithIndex.foldLeft(initAcc) { case ((prevS, prevC, prevCt), (v, i)) =>
      val y = dist.cdf(v)
      val a = math.log(y)
      val b = math.log(1 - y)
      val unAdjusted = a * (2 * i + 1) + b * (2 * n - 2 * i - 1)
      val adjConstant = a - b
      (prevS + unAdjusted, prevC + adjConstant, prevCt + 1)
    }
    Array(pResult).iterator
  }

  /**
   * Create a theoretical distribution to be used in the one sample Anderson-Darling test
   * @param distName name of distribution
   * @param params Initialization parameters for distribution, if none provided, default values
   *               are chosen.
   * @return distribution object used to calculate CDF values
   */
  private def initDist(distName: String, params: Seq[Double]): AndersonDarlingTheoreticalDist = {
    distName match {
      case "norm" => {
        val checkedParams = validateParams(distName, params, 2, Seq(0.0, 1.0))
        new AndersonDarlingNormal(checkedParams)
      }
      case "exp" => {
        val checkedParams = validateParams(distName, params, 1, Seq(1.0))
        new AndersonDarlingExponential(checkedParams)
      }
      case "gumbel" => {
        val checkedParams = validateParams(distName, params, 2, Seq(0.0, 1.0))
        new AndersonDarlingGumbel(checkedParams)
      }
      case "logistic" => {
        val checkedParams = validateParams(distName, params, 2, Seq(0.0, 1.0))
        new AndersonDarlingLogistic(checkedParams)
      }
      case "weibull" => {
        val checkedParams = validateParams(distName, params, 2, Seq(0.0, 1.0))
        new AndersonDarlingWeibull(checkedParams)
      }
      case _ => throw new IllegalArgumentException(
        s"Anderson-Darling does not currently support $distName distribution" +
          " must be one of 'norm', 'exp', 'gumbel', 'logistic', or 'weibull'")
    }
  }

  /**
   * Validate the length of parameters passed in by the user, if none are passed, return default
   * values
   * @param distName name of distribution
   * @param params parameters passed by user
   * @param reqLen the required length of the parameter sequence
   * @param defParams default alternative for the parameter in case `params` is empty
   * @return parameters that will be used to initialize the distribution
   */
  private def validateParams(
      distName: String,
      params: Seq[Double],
      reqLen: Int,
      defParams: Seq[Double]): Seq[Double] = {
    if (params.nonEmpty) {
      require(params.length == reqLen, s"$distName distribution requires $reqLen parameters.")
      params
    } else {
      logInfo(s"No parameters passed for $distName distribution, " +
        s"initialized with " + defParams.mkString(", "))
      defParams
    }
  }
}
