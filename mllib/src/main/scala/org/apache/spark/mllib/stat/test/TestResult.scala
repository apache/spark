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

import org.apache.spark.annotation.{Experimental, Since}

/**
 * :: Experimental ::
 * Trait for hypothesis test results.
 * @tparam DF Return type of `degreesOfFreedom`.
 */
@Experimental
@Since("1.1.0")
trait TestResult[DF] {

  /**
   * The probability of obtaining a test statistic result at least as extreme as the one that was
   * actually observed, assuming that the null hypothesis is true.
   */
  @Since("1.1.0")
  def pValue: Double

  /**
   * Returns the degree(s) of freedom of the hypothesis test.
   * Return type should be Number(e.g. Int, Double) or tuples of Numbers for toString compatibility.
   */
  @Since("1.1.0")
  def degreesOfFreedom: DF

  /**
   * Test statistic.
   */
  @Since("1.1.0")
  def statistic: Double

  /**
   * Null hypothesis of the test.
   */
  @Since("1.1.0")
  def nullHypothesis: String

  /**
   * String explaining the hypothesis test result.
   * Specific classes implementing this trait should override this method to output test-specific
   * information.
   */
  override def toString: String = {

    // String explaining what the p-value indicates.
    val pValueExplain = if (pValue <= 0.01) {
      s"Very strong presumption against null hypothesis: $nullHypothesis."
    } else if (0.01 < pValue && pValue <= 0.05) {
      s"Strong presumption against null hypothesis: $nullHypothesis."
    } else if (0.05 < pValue && pValue <= 0.1) {
      s"Low presumption against null hypothesis: $nullHypothesis."
    } else {
      s"No presumption against null hypothesis: $nullHypothesis."
    }

    s"degrees of freedom = ${degreesOfFreedom.toString} \n" +
    s"statistic = $statistic \n" +
    s"pValue = $pValue \n" + pValueExplain
  }
}

/**
 * :: Experimental ::
 * Object containing the test results for the chi-squared hypothesis test.
 */
@Experimental
@Since("1.1.0")
class ChiSqTestResult private[stat] (override val pValue: Double,
    @Since("1.1.0") override val degreesOfFreedom: Int,
    @Since("1.1.0") override val statistic: Double,
    @Since("1.1.0") val method: String,
    @Since("1.1.0") override val nullHypothesis: String) extends TestResult[Int] {

  override def toString: String = {
    "Chi squared test summary:\n" +
      s"method: $method\n" +
      super.toString
  }
}

/**
 * :: Experimental ::
 * Object containing the test results for the Kolmogorov-Smirnov test.
 */
@Experimental
@Since("1.5.0")
class KolmogorovSmirnovTestResult private[stat] (
    @Since("1.5.0") override val pValue: Double,
    @Since("1.5.0") override val statistic: Double,
    @Since("1.5.0") override val nullHypothesis: String) extends TestResult[Int] {

  @Since("1.5.0")
  override val degreesOfFreedom = 0

  override def toString: String = {
    "Kolmogorov-Smirnov test summary:\n" + super.toString
  }
}
