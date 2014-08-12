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

package org.apache.spark.mllib.grouped

import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashMap

/**
 * Computes the area under the curve (AUC) using the trapezoidal rule.
 */
private[mllib] object GroupedAreaUnderCurve {

  /**
   * Uses the trapezoidal rule to compute the area under the line connecting the two input points.
   * @param points two 2D points stored in Seq
   */
  private def trapezoid[K](points: Seq[(Double, Double)]): Double = {
    require(points.length == 2)
    val x = points.head
    val y = points.last
    (y._1 - x._1) * (y._2 + x._2) / 2.0
  }

  /**
   * Returns the area under the given curve.
   *
   * @param curve a RDD of ordered 2D points stored in pairs representing a curve
   */
  def of[K](curve: RDD[(K,(Double, Double))]): Map[K,Double] = {
    val agg = curve.mapPartitions( iter => {
      val first = new HashMap[K,(Double,Double)]()
      val last = new HashMap[K,(Double,Double)]()
      val aucs = new HashMap[K,Double]()
      iter.foreach( x => {
        if (last.contains(x._1)) {
          aucs(x._1) = aucs.getOrElse(x._1, 0.0) + trapezoid(Seq(last(x._1), x._2))
        }
        if (!first.contains(x._1)) {
          first(x._1) = x._2
        }
        last(x._1) = x._2
      })
      Iterator((first.toMap,last.toMap,aucs.toMap))
    }).collect()

    val s = agg.foldLeft( (Map[K,(Double,Double)]()), Map[K,Double]() )(
      (agg:(Map[K,(Double,Double)], Map[K,Double]),
       n:(Map[K,(Double,Double)],Map[K,(Double,Double)],Map[K,Double]) ) => {
        val prev = new HashMap[K,(Double,Double)]()
        val aucs = new HashMap[K,Double]()
        (agg._1.keySet ++ n._1.keySet).foreach( k => {
          // sum the aucs from the two partitions
          aucs(k) = agg._2.getOrElse(k, 0.0)
          if (n._3.contains(k)) {
            aucs(k) += n._3.getOrElse(k,0.0)
          }
          // sum areas between the partitions
          if (agg._1.contains(k) && n._1.contains(k)) {
            aucs(k) += trapezoid(Seq(agg._1(k),n._1(k)))
          }
          // get the last occurance for each key
          if (n._2.contains(k)) {
            prev(k) = n._2(k)
          } else if (agg._1.contains(k)) {
            prev(k) = agg._1(k)
          }
        })
        (prev.toMap, aucs.toMap)
    })
    s._2
  }

  /**
   * Returns the area under the given curve.
   *
   * @param curve an iterator over ordered 2D points stored in pairs representing a curve
   */
  def of[K](curve: Iterable[(K,(Double, Double))]): Map[K,Double] = {
    val prev = new HashMap[K,(Double,Double)]()
    val aucs = new HashMap[K,Double]()
    curve.foreach( x => {
      if (prev.contains(x._1)) {
        aucs(x._1) = aucs.getOrElse(x._1, 0.0) + trapezoid(Seq(prev(x._1), x._2))
      }
      prev(x._1) = x._2
    })
    aucs.toMap
  }
}
