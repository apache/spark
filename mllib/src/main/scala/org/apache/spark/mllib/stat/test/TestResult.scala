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

import org.apache.spark.annotation.Experimental

/**
 * :: Experimental ::
 * Trait for hypothesis test results.
 * @tparam DF Return type of `degreesOfFreedom`
 */
@Experimental
trait TestResult[DF] {

  /**
   *
   */
  def pValue: Double

  /**
   *
   * @return
   */
  def degreesOfFreedom: DF

  /**
   *
   * @return
   */
  def statistic: Double

  /**
   * String explaining the hypothesis test result.
   * Specific classes implementing this trait should override this method to output test-specific
   * information.
   */
  override def toString: String = {

    // String explaining what the p-value indicates.
    val pValueExplain = if (pValue <= 0.01) {
      "Very strong presumption against null hypothesis."
    } else if (0.01 < pValue && pValue <= 0.05) {
      "Strong presumption against null hypothesis."
    } else if (0.05 < pValue && pValue <= 0.01) {
      "Low presumption against null hypothesis."
    } else {
      "No presumption against null hypothesis."
    }

    s"degrees of freedom = ${degreesOfFreedom.toString} \n" +
    s"statistic = $statistic \n" +
    s"pValue = $pValue \n" + pValueExplain
  }
}

/**
 * :: Experimental ::
 * Object containing the test results for the chi squared hypothesis test.
 */
@Experimental
case class ChiSquaredTestResult(override val pValue: Double,
    override val degreesOfFreedom: Int,
    override val statistic: Double,
    val method: String,
    val nullHypothesis: String) extends TestResult[Int] {

  override def toString: String = {
    "Chi squared test summary: \n" +
    s"method: $method \n" +
    s"null hypothesis: $nullHypothesis \n" +
    super.toString
  }
}
