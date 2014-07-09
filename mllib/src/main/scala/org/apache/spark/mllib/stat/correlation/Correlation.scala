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

package org.apache.spark.mllib.stat.correlation

import org.apache.spark.mllib.linalg.{Matrix, Vector}
import org.apache.spark.rdd.RDD

trait Correlation {

  def computeCorrelation(x: RDD[Double], y: RDD[Double]): Double

  def computeCorrelationMatrix(X: RDD[Vector]): Matrix

}

/**
 * Delegates computation to the specific correlation object based on the input method name
 *
 * Maintains the default correlation type
 */
object Correlations {

  // Note: after new types of correlations are implemented, please update this map
  val nameToObjectMap = Map(("pearson", PearsonCorrelation), ("spearman", SpearmansCorrelation))
  val defaultCorrName: String = "pearson"
  val defaultCorr: Correlation = nameToObjectMap(defaultCorrName)

  def corr(x: RDD[Double], y: RDD[Double], method: String = defaultCorrName): Double = {
    val correlation = getCorrelationFromName(method)
    correlation.computeCorrelation(x, y)
  }

  def corrMatrix(X: RDD[Vector], method: String = defaultCorrName): Matrix = {
    val correlation = getCorrelationFromName(method)
    correlation.computeCorrelationMatrix(X)
  }

  /**
   * Perform simple string processing to match the input correlation name with a known name
   */
  private def getCorrelationFromName(method: String): Correlation = {
    if (method.equals(defaultCorrName)) {
      defaultCorr
    } else {
      var correlation: Correlation = defaultCorr
      var matched = false
      val initialsAllowed = areInitialsUnique()
      val inputLower = method.toLowerCase()
      nameToObjectMap.foreach { case (name, corr) =>
        if (!matched) {
          if ((initialsAllowed && method.size == 1 && name.startsWith(inputLower)) ||
            inputLower.contains(name)) { // match names like "spearmans"
            correlation = corr
            matched = true
          }
        }
      }

      if (matched) {
        correlation
      } else {
        throw new IllegalArgumentException("Correlation name not recognized.")
      }
    }
  }

  /**
   * Check if the first letters of known correlation names are all unique
   */
  private def areInitialsUnique(): Boolean = {
    nameToObjectMap.keys.map(key => key.charAt(0)).toSet.size == nameToObjectMap.keySet.size
  }
}
