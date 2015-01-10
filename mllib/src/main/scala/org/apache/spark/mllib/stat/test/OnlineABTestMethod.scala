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

import java.io.Serializable

import scala.language.implicitConversions
import scala.math.pow

import com.twitter.chill.MeatLocker
import org.apache.commons.math3.stat.descriptive.StatisticalSummaryValues
import org.apache.commons.math3.stat.inference.TTest

import org.apache.spark.Logging
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.util.StatCounter

/**
 * Significance testing methods for [[OnlineABTest]]. New statistical tests for assessing
 * significance of AB testing results should be implemented in OnlineABTestMethod.scala, extend
 * [[OnlineABTestMethod]], and introduce a new entry in [[OnlineABTestMethodNames.NameToObjectMap]].
 */
sealed trait OnlineABTestMethod extends Serializable {

  val MethodName: String
  val NullHypothesis: String

  protected type SummaryPairStream =
    DStream[(StatCounter, StatCounter)]

  /**
   * Perform online 2-sample statistical significance testing.
   *
   * @param sampleSummaries stream pairs of summary statistics for the 2 samples
   * @return stream of rest results
   */
  def doTest(sampleSummaries: SummaryPairStream): DStream[OnlineABTestResult]


  /**
   * Implicit adapter to convert between online summary statistics type and the type required by
   * the t-testing libraries.
   */
  protected implicit def toApacheCommonsStats(
      summaryStats: StatCounter): StatisticalSummaryValues = {
    new StatisticalSummaryValues(
      summaryStats.mean,
      summaryStats.variance,
      summaryStats.count,
      summaryStats.max,
      summaryStats.min,
      summaryStats.mean * summaryStats.count
    )
  }
}

/**
 * Performs Welch's 2-sample t-test. The null hypothesis is that the two data sets have equal mean.
 * This test does not assume equal variance between the two samples and does not assume equal
 * sample size.
 *
 * More information: http://en.wikipedia.org/wiki/Welch%27s_t_test
 */
private[stat] object WelchTTest extends OnlineABTestMethod with Logging {

  final val MethodName = "Welch's 2-sample T-test"
  final val NullHypothesis = "A and B groups have same mean"

  private final val TTester = MeatLocker(new TTest())

  def doTest(data: SummaryPairStream): DStream[OnlineABTestResult] =
    data.map[OnlineABTestResult]((test _).tupled)

  private def test(
      statsA: StatCounter,
      statsB: StatCounter): OnlineABTestResult = {
    def welchDF(sample1: StatisticalSummaryValues, sample2: StatisticalSummaryValues): Double = {
      val s1 = sample1.getVariance
      val n1 = sample1.getN
      val s2 = sample2.getVariance
      val n2 = sample2.getN

      val a = pow(s1, 2) / n1
      val b = pow(s2, 2) / n2

      pow(a + b, 2) / ((pow(a, 2) / (n1 - 1)) + (pow(b, 2) / (n2 - 1)))
    }

    new OnlineABTestResult(
      TTester.get.tTest(statsA, statsB),
      welchDF(statsA, statsB),
      TTester.get.t(statsA, statsB),
      MethodName,
      NullHypothesis
    )
  }
}

/**
 * Performs Students's 2-sample t-test. The null hypothesis is that the two data sets have equal
 * mean. This test assumes equal variance between the two samples and does not assume equal sample
 * size. For unequal variances, Welch's t-test should be used instead.
 *
 * More information: http://en.wikipedia.org/wiki/Student%27s_t-test
 */
private[stat] object StudentTTest extends OnlineABTestMethod with Logging {

  final val MethodName = "Student's 2-sample T-test"
  final val NullHypothesis = "A and B groups have same mean"

  private final val TTester = MeatLocker(new TTest())

  def doTest(data: SummaryPairStream): DStream[OnlineABTestResult] =
    data.map[OnlineABTestResult]((test _).tupled)

  private def test(
      statsA: StatCounter,
      statsB: StatCounter): OnlineABTestResult = {
    def studentDF(sample1: StatisticalSummaryValues, sample2: StatisticalSummaryValues): Double =
      sample1.getN + sample2.getN - 2

    new OnlineABTestResult(
      TTester.get.homoscedasticTTest(statsA, statsB),
      studentDF(statsA, statsB),
      TTester.get.homoscedasticT(statsA, statsB),
      MethodName,
      NullHypothesis
    )
  }
}

/**
 * Maintains supported [[OnlineABTestMethod]] names and handles conversion between strings used in
 * [[OnlineABTest]] configuration and actual method implementation.
 *
 * Currently supported correlations: `welch`, `student`.
 */
private[stat] object OnlineABTestMethodNames {
  // Note: after new OnlineABTestMethods are implemented, please update this map.
  final val NameToObjectMap = Map(("welch", WelchTTest), ("student", StudentTTest))

  // Match input correlation name with a known name via simple string matching.
  def getTestMethodFromName(method: String): OnlineABTestMethod = {
    try {
      NameToObjectMap(method)
    } catch {
      case nse: NoSuchElementException =>
        throw new IllegalArgumentException("Unrecognized method name. Supported A/B test methods: "
          + NameToObjectMap.keys.mkString(", "))
    }
  }
}

