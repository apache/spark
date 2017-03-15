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

package org.apache.spark.ml.util

import breeze.linalg.{Matrix => BM}

import org.apache.spark.internal.Logging

/**
 * Utility test methods for linear algebra.
 */
object LinalgUtils extends Logging {


  /**
   * Returns true if two numbers are equal up to some tolerance.
   */
  def approxEqual(v1: Double, v2: Double, threshold: Double = 1e-6): Boolean = {
    if (v1.isNaN) {
      v2.isNaN
    } else {
      math.abs(v1 - v2) <= threshold
    }
  }

  /**
   * Returns true if two numbers are equal coefficient-wise up to some tolerance.
   */
  def matrixApproxEqual(A: BM[Double], B: BM[Double], threshold: Double = 1e-6): Boolean = {
    for (i <- 0 until A.rows; j <- 0 until A.cols) {
      if (!approxEqual(A(i, j), B(i, j), threshold)) {
        logInfo("i, j = " + i + ", " + j + " actual: " + A(i, j) + " expected:" + B(i, j))
        return false
      }
    }
    true
  }

}
