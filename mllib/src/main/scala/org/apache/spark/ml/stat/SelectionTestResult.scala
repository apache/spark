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

import org.apache.spark.annotation.Since

/**
 * Trait for selection test results.
 */
@Since("3.1.0")
trait SelectionTestResult {

  /**
   * The probability of obtaining a test statistic result at least as extreme as the one that was
   * actually observed, assuming that the null hypothesis is true.
   */
  @Since("3.1.0")
  def pValue: Double

  /**
   * Test statistic.
   * In ChiSqSelector, this is chi square statistic
   * In ANOVASelector and FValueSelector, this is F Value
   */
  @Since("3.1.0")
  def statistic: Double

  /**
   * Returns the degrees of freedom of the hypothesis test.
   */
  @Since("3.1.0")
  def degreesOfFreedom: Long

  /**
   * String explaining the hypothesis test result.
   * Specific classes implementing this trait should override this method to output test-specific
   * information.
   */
  override def toString: String = {

    // String explaining what the p-value indicates.
    val pValueExplain = if (pValue <= 0.01) {
      s"Very strong presumption against null hypothesis."
    } else if (0.01 < pValue && pValue <= 0.05) {
      s"Strong presumption against null hypothesis."
    } else if (0.05 < pValue && pValue <= 0.1) {
      s"Low presumption against null hypothesis."
    } else {
      s"No presumption against null hypothesis."
    }

    s"degrees of freedom = ${degreesOfFreedom.toString} \n" + s"pValue = $pValue \n" + pValueExplain
  }
}

/**
 * Object containing the test results for the chi-squared hypothesis test.
 */
@Since("3.1.0")
class ChiSqTestResult private[stat] (
    override val pValue: Double,
    override val degreesOfFreedom: Long,
    override val statistic: Double) extends SelectionTestResult {

  override def toString: String = {
    "Chi square test summary:\n" +
      super.toString +
      s"Chi square statistic = $statistic \n"
  }
}

/**
 * Object containing the test results for the FValue regression test.
 */
@Since("3.1.0")
class FValueTestResult private[stat] (
    override val pValue: Double,
    override val degreesOfFreedom: Long,
    override val statistic: Double) extends SelectionTestResult {

  override def toString: String = {
    "FValue Regression test summary:\n" +
      super.toString +
      s"F Value = $statistic \n"
  }
}

/**
 * Object containing the test results for the ANOVA classification test.
 */
@Since("3.1.0")
class ANOVATestResult private[stat] (
    override val pValue: Double,
    override val degreesOfFreedom: Long,
    override val statistic: Double) extends SelectionTestResult {

  override def toString: String = {
    "ANOVA Regression test summary:\n" +
      super.toString +
      s"F Value = $statistic \n"
  }
}
