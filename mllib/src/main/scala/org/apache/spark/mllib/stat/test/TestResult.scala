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
 */
@Experimental
trait TestResult {

  def pValue: Double

  def degreesOfFreedom: Array[Int]

  def statistic: Double

  /**
   * Returns a String explaining the hypothesis test result.
   * Specific classes implementing this trait should override this method to output test-specific
   * information.
   */
  override def toString: String = {
    s"pValue = $pValue \n" + // TODO explain what pValue is
    s"degrees of freedom = ${degreesOfFreedom.mkString} \n" +
    s"statistic = $statistic"
  }
}

/**
 * :: Experimental ::
 */
@Experimental
case class ChiSquaredTestResult(override val pValue: Double,
    override val degreesOfFreedom: Array[Int],
    override val statistic: Double,
    val method: String) extends TestResult {

  override def toString: String = {
    "Chi squared test summary: \n" +
    s"method: $method \n" +
    super.toString
  }
}
