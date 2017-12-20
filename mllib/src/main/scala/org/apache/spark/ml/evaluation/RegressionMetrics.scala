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

package org.apache.spark.ml.evaluation

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.sql.{Dataset, Row}

/**
 * :: Experimental ::
 * Metrics for regression, which expects two input columns: prediction and label.
 */
@Since("2.3.0")
@Experimental
class RegressionMetrics private[spark] (dataset: Dataset[_], throughOrigin: Boolean = false) {

  private val mllibMetrics = {
    val rdd = dataset.toDF().rdd.map {
      case Row(prediction: Double, label: Double) => (prediction, label)
    }
    new org.apache.spark.mllib.evaluation.RegressionMetrics(rdd, throughOrigin)
  }

  /**
   * Returns the mean absolute error, which is a risk function corresponding to the
   * expected value of the absolute error loss or l1-norm loss.
   */
  @Since("2.3.0")
  lazy val meanAbsoluteError: Double = mllibMetrics.meanAbsoluteError

  /**
   * Returns the mean squared error, which is a risk function corresponding to the
   * expected value of the squared error loss or quadratic loss.
   */
  @Since("2.3.0")
  lazy val meanSquaredError: Double = mllibMetrics.meanSquaredError

  /**
   * Returns R^2^, the unadjusted coefficient of determination.
   * @see <a href="http://en.wikipedia.org/wiki/Coefficient_of_determination">
   * Coefficient of determination (Wikipedia)</a>
   * In case of regression through the origin, the definition of R^2^ is to be modified.
   * @see <a href="https://online.stat.psu.edu/~ajw13/stat501/SpecialTopics/Reg_thru_origin.pdf">
   * J. G. Eisenhauer, Regression through the Origin. Teaching Statistics 25, 76-80 (2003)</a>
   */
  @Since("2.3.0")
  lazy val r2: Double = mllibMetrics.r2

  /**
   * Returns the root mean squared error, which is defined as the square root of
   * the mean squared error.
   */
  @Since("2.3.0")
  lazy val rootMeanSquaredError: Double = mllibMetrics.rootMeanSquaredError

}

