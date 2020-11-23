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
import org.apache.spark.sql.types.{ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, TimestampType}

object UnionEstimation {
  import EstimationUtils._

  def compare(a: Any, b: Any, dataType: DataType): Boolean = {
    dataType match {
      case dt: IntegerType => dt.ordering.asInstanceOf[Ordering[Any]].lt(a, b)
      case dt: LongType => dt.ordering.asInstanceOf[Ordering[Any]].lt(a, b)
      case dt: FloatType => dt.ordering.asInstanceOf[Ordering[Any]].lt(a, b)
      case dt: DoubleType => dt.ordering.asInstanceOf[Ordering[Any]].lt(a, b)
      case dt: ShortType => dt.ordering.asInstanceOf[Ordering[Any]].lt(a, b)
      case dt: ByteType => dt.ordering.asInstanceOf[Ordering[Any]].lt(a, b)
      case dt: DateType => dt.ordering.asInstanceOf[Ordering[Any]].lt(a, b)
      case dt: TimestampType => dt.ordering.asInstanceOf[Ordering[Any]].lt(a, b)
      case dt: DecimalType => dt.ordering.asInstanceOf[Ordering[Any]].lt(a, b)
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
          val dataType = output(outputIndex).dataType
          val minStart : Option[Any] = None
          val maxStart : Option[Any] = None
          val result = attrs.zipWithIndex.foldLeft((minStart, maxStart)) {
            case((minVal, maxVal), (attr, childIndex)) =>
              val colStat = union.children(childIndex).stats.attributeStats(attr)
              val min = if (minVal.isEmpty || compare(colStat.min.get, minVal.get, dataType)) {
                colStat.min
              } else {
                minVal
              }
              val max = if (maxVal.isEmpty || compare(maxVal.get, colStat.max.get, dataType)) {
                colStat.max
              } else {
                maxVal
              }
              (min, max)
          }
          val newStat = ColumnStat(min = result._1, max = result._2)
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
