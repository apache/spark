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

package org.apache.spark.ml.impl


private[ml] object Utils {

  lazy val EPSILON = {
    var eps = 1.0
    while ((1.0 + (eps / 2.0)) != 1.0) {
      eps /= 2.0
    }
    eps
  }

  /**
   * Predict a single label by one-dimensional linear interpolation.
   *
   * @param xp x-coordinates of the data points, must be increasing.
   * @param yp y-coordinates of the data points.
   * @param x input x-coordinate of the interpolated values.
   * @return Predicted label.
   *         1) If testData exactly matches a boundary then associated prediction is returned.
   *           In case there are multiple predictions with the same boundary then one of them
   *           is returned. Which one is undefined (same as java.util.Arrays.binarySearch).
   *         2) If testData is lower or higher than all boundaries then first or last prediction
   *           is returned respectively. In case there are multiple predictions with the same
   *           boundary then the lowest or highest is returned respectively.
   *         3) If testData falls between two values in boundary array then prediction is treated
   *           as piecewise linear function and interpolated value is returned. In case there are
   *           multiple values with the same boundary then the same rules as in 2) are used.
   *
   */
  private[spark] def interpolate(
      xp: Array[Double],
      yp: Array[Double],
      x: Double): Double = {
    val j = java.util.Arrays.binarySearch(xp, x)
    if (j < 0) {
      val i = -j - 1
      if (i == 0) {
        yp.head
      } else if (i == xp.length) {
        yp.last
      } else {
        val x1 = xp(i - 1)
        val x2 = xp(i)
        val y1 = yp(i - 1)
        val y2 = yp(i)
        y1 + (y2 - y1) * (x - x1) / (x2 - x1)
      }
    } else {
      yp(j)
    }
  }

}
