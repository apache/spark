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

package org.apache.spark.mllib.evaluation

import org.apache.spark.rdd.RDD

/**
 * Computes the area under the curve (AUC) using the trapezoidal rule.
 */
private[evaluation] object AreaUnderCurve {

  /**
   * Uses the trapezoidal rule to compute the area under the line connecting the two input points.
   * @param points two 2D points stored in Seq
   */
  private def trapezoid(points: Seq[(Double, Double)]): Double = {
    require(points.length == 2)
    val x = points.head
    val y = points.last
    (y._1 - x._1) * (y._2 + x._2) / 2.0
  }

  /**
   * Returns the area under the given curve.
   *
   * @param curve an RDD of ordered 2D points stored in pairs representing a curve
   */
  def of(curve: RDD[(Double, Double)]): Double = {
    val localAreas = curve.mapPartitions { iter =>
      if (iter.nonEmpty) {
        var localArea = 0.0
        var head = true
        var firstPoint = (Double.NaN, Double.NaN)
        var lastPoint = (Double.NaN, Double.NaN)

        iter.sliding(2).foreach { points =>
          if (head) {
            firstPoint = points.head
            head = false
          }
          lastPoint = points.last

          if (points.length == 2) {
            localArea += trapezoid(points)
          }
        }
        Iterator.single((localArea, (firstPoint, lastPoint)))
      } else {
        Iterator.empty
      }
    }.collect()

    localAreas.map(_._1).sum + localAreas.iterator.map(_._2)
      .sliding(2).withPartial(false)
      .map { case Seq((_, last1), (first2, _)) =>
        trapezoid(Seq(last1, first2))
      }.sum
  }

  /**
   * Returns the area under the given curve.
   *
   * @param curve an iterator over ordered 2D points stored in pairs representing a curve
   */
  def of(curve: Iterable[(Double, Double)]): Double = {
    curve.iterator.sliding(2).withPartial(false).foldLeft(0.0)(
      op = (auc: Double, points: Seq[(Double, Double)]) => auc + trapezoid(points)
    )
  }
}
