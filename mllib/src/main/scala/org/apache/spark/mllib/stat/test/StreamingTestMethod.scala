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

import org.apache.spark.internal.Logging
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.util.StatCounter

/**
 * Significance testing methods for [[StreamingTest]]. New 2-sample statistical significance tests
 * should extend [[StreamingTestMethod]] and introduce a new entry in
 * `StreamingTestMethod.TEST_NAME_TO_OBJECT`
 */
private[stat] sealed trait StreamingTestMethod extends Serializable {

  val methodName: String
  val nullHypothesis: String

  protected type SummaryPairStream =
    DStream[(StatCounter, StatCounter)]

  /**
   * Perform streaming 2-sample statistical significance testing.
   *
   * @param sampleSummaries stream pairs of summary statistics for the 2 samples
   * @return stream of rest results
   */
  def doTest(sampleSummaries: SummaryPairStream): DStream[StreamingTestResult]

  /**
   * Implicit adapter to convert between streaming summary statistics type and the type required by
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
 * @see <a href="http://en.wikipedia.org/wiki/Welch%27s_t_test">Welch's t-test (Wikipedia)</a>
 */
private[stat] object WelchTTest extends StreamingTestMethod with Logging {

  override final val methodName = "Welch's 2-sample t-test"
  override final val nullHypothesis = "Both groups have same mean"

  private final val tTester = MeatLocker(new TTest())

  override def doTest(data: SummaryPairStream): DStream[StreamingTestResult] =
    data.map[StreamingTestResult]((test _).tupled)

  private def test(
      statsA: StatCounter,
      statsB: StatCounter): StreamingTestResult = {
    def welchDF(sample1: StatisticalSummaryValues, sample2: StatisticalSummaryValues): Double = {
      val s1 = sample1.getVariance
      val n1 = sample1.getN
      val s2 = sample2.getVariance
      val n2 = sample2.getN

      val a = pow(s1, 2) / n1
      val b = pow(s2, 2) / n2

      pow(a + b, 2) / ((pow(a, 2) / (n1 - 1)) + (pow(b, 2) / (n2 - 1)))
    }

    new StreamingTestResult(
      tTester.get.tTest(statsA, statsB),
      welchDF(statsA, statsB),
      tTester.get.t(statsA, statsB),
      methodName,
      nullHypothesis
    )
  }
}

/**
 * Performs Students's 2-sample t-test. The null hypothesis is that the two data sets have equal
 * mean. This test assumes equal variance between the two samples and does not assume equal sample
 * size. For unequal variances, Welch's t-test should be used instead.
 *
 * @see <a href="http://en.wikipedia.org/wiki/Student%27s_t-test">Student's t-test (Wikipedia)</a>
 */
private[stat] object StudentTTest extends StreamingTestMethod with Logging {

  override final val methodName = "Student's 2-sample t-test"
  override final val nullHypothesis = "Both groups have same mean"

  private final val tTester = MeatLocker(new TTest())

  override def doTest(data: SummaryPairStream): DStream[StreamingTestResult] =
    data.map[StreamingTestResult]((test _).tupled)

  private def test(
      statsA: StatCounter,
      statsB: StatCounter): StreamingTestResult = {
    def studentDF(sample1: StatisticalSummaryValues, sample2: StatisticalSummaryValues): Double =
      sample1.getN + sample2.getN - 2

    new StreamingTestResult(
      tTester.get.homoscedasticTTest(statsA, statsB),
      studentDF(statsA, statsB),
      tTester.get.homoscedasticT(statsA, statsB),
      methodName,
      nullHypothesis
    )
  }
}

/**
 * Companion object holding supported [[StreamingTestMethod]] names and handles conversion between
 * strings used in [[StreamingTest]] configuration and actual method implementation.
 *
 * Currently supported tests: `welch`, `student`.
 */
private[stat] object StreamingTestMethod {
  // Note: after new `StreamingTestMethod`s are implemented, please update this map.
  private final val TEST_NAME_TO_OBJECT: Map[String, StreamingTestMethod] = Map(
    "welch" -> WelchTTest,
    "student" -> StudentTTest)

  def getTestMethodFromName(method: String): StreamingTestMethod =
    TEST_NAME_TO_OBJECT.get(method) match {
      case Some(test) => test
      case None =>
        throw new IllegalArgumentException(
          "Unrecognized method name. Supported streaming test methods: "
            + TEST_NAME_TO_OBJECT.keys.mkString(", "))
    }
}

