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

import org.apache.spark.sql.AnalysisException

object MathUtils {

  /**
   *  Returns the bucket number into which
   *  the value of this expression would fall after being evaluated.
   *
   * @param expr is the expression for which the histogram is being created
   * @param minValue is an expression that resolves
   *                 to the minimum end point of the acceptable range for expr
   * @param maxValue is an expression that resolves
   *                 to the maximum end point of the acceptable range for expr
   * @param numBucket is an An expression that resolves to
   *                  a constant indicating the number of buckets
   * @return Returns an long between 0 and numBucket+1 by mapping the expr into buckets defined by
   *         the range [minValue, maxValue].
   */
  def widthBucket(expr: Double, minValue: Double, maxValue: Double, numBucket: Long): Long = {

    if (numBucket <= 0) {
      throw new AnalysisException(s"The num of bucket must be greater than 0, but got ${numBucket}")
    }

    val lower: Double = Math.min(minValue, maxValue)
    val upper: Double = Math.max(minValue, maxValue)

    val result: Long = if (expr < lower) {
      0
    } else if (expr >= upper) {
      numBucket + 1L
    } else {
      (numBucket.toDouble * (expr - lower) / (upper - lower) + 1).toLong
    }

    if (minValue > maxValue) (numBucket - result) + 1 else result
  }
}
