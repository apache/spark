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

package org.apache.spark.sql.catalyst.util

import java.math.{BigDecimal => JBigDecimal}

/**
 * Helper functions for BigDecimal.
 */
object MathUtils {

  /**
   * Returns double type input truncated to scale decimal places.
   */
  def trunc(input: Double, scale: Int): Double = {
    trunc(JBigDecimal.valueOf(input), scale).doubleValue()
  }

  /**
   * Returns BigDecimal type input truncated to scale decimal places.
   */
  def trunc(input: JBigDecimal, scale: Int): JBigDecimal = {
    // Copy from (https://github.com/apache/hive/blob/release-2.3.0-rc0
    // /ql/src/java/org/apache/hadoop/hive/ql/udf/generic/GenericUDFTrunc.java#L471-L487)
    val pow = if (scale >= 0) {
      JBigDecimal.valueOf(Math.pow(10, scale))
    } else {
      JBigDecimal.valueOf(Math.pow(10, Math.abs(scale)))
    }

    val out = if (scale > 0) {
      val longValue = input.multiply(pow).longValue()
      JBigDecimal.valueOf(longValue).divide(pow)
    } else if (scale == 0) {
      JBigDecimal.valueOf(input.longValue())
    } else {
      val longValue = input.divide(pow).longValue()
      JBigDecimal.valueOf(longValue).multiply(pow)
    }
    out
  }
}
