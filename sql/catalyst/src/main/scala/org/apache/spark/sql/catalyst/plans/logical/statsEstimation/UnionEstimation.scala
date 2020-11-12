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

package org.apache.spark.sql.catalyst.plans.logical.statsEstimation

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, Statistics, Union}

object UnionEstimation {
  import EstimationUtils._

  def compare(a: Any, b: Any): Boolean = {
    a match {
      case _: Int => a.asInstanceOf[Int] < b.asInstanceOf[Int]
      case _: Long => a.asInstanceOf[Long] < b.asInstanceOf[Long]
      case _: Float => a.asInstanceOf[Float] < b.asInstanceOf[Float]
      case _: Double => a.asInstanceOf[Double] < b.asInstanceOf[Double]
      case _: Short => a.asInstanceOf[Short] < b.asInstanceOf[Short]
      case _: Byte => a.asInstanceOf[Byte] < b.asInstanceOf[Byte]
      case _ => false
    }
  }

  def estimate(union: Union): Option[Statistics] = {
    val sizeInBytes = union.children.map(_.stats.sizeInBytes).sum
    val outputRows: Option[BigInt] = if (rowCountsExist(union.children: _*)) {
      Some(union.children.map(_.stats.rowCount.get).sum)
    } else {
      None
    }

    val output = union.output
    val outputAttrStats = new ArrayBuffer[(Attribute, ColumnStat)]()

    union.children.map(_.output).transpose.zipWithIndex.foreach {
      case (attrs, outputIndex) =>
        val validStat = attrs.zipWithIndex.forall {
          case (attr, childIndex) =>
            val attrStats = union.children(childIndex).stats.attributeStats
            attrStats.get(attr).isDefined && attrStats(attr).hasMinMaxStats
        }
        if (validStat) {
          var min: Option[Any] = None
          var max: Option[Any] = None
          attrs.zipWithIndex.foreach {
            case (attr, childIndex) =>
              val colStat = union.children(childIndex).stats.attributeStats(attr)
              min = if (min.isEmpty || compare(colStat.min.get, min.get)) {
                colStat.min
              } else {
                min
              }

              max = if (max.isEmpty || compare(max.get, colStat.max.get)) {
                colStat.max
              } else {
                max
              }
          }
          val newStat = ColumnStat(min = min, max = max)
          outputAttrStats += output(outputIndex) -> newStat
        }
    }

    Some(
      Statistics(
        sizeInBytes = sizeInBytes,
        rowCount = outputRows,
        attributeStats = AttributeMap(outputAttrStats.toSeq)))
  }
}
