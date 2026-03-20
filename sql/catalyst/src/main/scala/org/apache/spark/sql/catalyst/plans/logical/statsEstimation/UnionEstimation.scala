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

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeSet}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, Statistics, Union}
import org.apache.spark.sql.catalyst.types.PhysicalDataType
import org.apache.spark.sql.types._

/**
 * Estimate the number of output rows by doing the sum of output rows for each child of union,
 * and estimate column stats (min, max, nullCount, distinctCount) for each column from its
 * children. Min and max are computed as the overall min/max across children, nullCount is summed,
 * and distinctCount is estimated as the max across children (capped by total row count).
 */
object UnionEstimation {
  import EstimationUtils._

  private def createStatComparator(dt: DataType): (Any, Any) => Boolean = {
    PhysicalDataType.ordering(dt).lt _
  }

  private def isTypeSupported(dt: DataType): Boolean = dt match {
    case ByteType | IntegerType | ShortType | FloatType | LongType |
         DoubleType | DateType | _: DecimalType | TimestampType | TimestampNTZType |
         _: AnsiIntervalType => true
    case _ => false
  }

  def estimate(union: Union): Option[Statistics] = {
    val sizeInBytes = union.children.map(_.stats.sizeInBytes).sum
    val outputRows = if (rowCountsExist(union.children: _*)) {
      Some(union.children.map(_.stats.rowCount.get).sum)
    } else {
      None
    }

    val newMinMaxStats = computeMinMaxStats(union)
    val newNullCountStats = computeNullCountStats(union)
    val newDistinctCountStats = computeDistinctCountStats(union, outputRows)

    val minMaxMap = AttributeMap(newMinMaxStats)
    val nullCountMap = AttributeMap(newNullCountStats)
    val distinctCountMap = AttributeMap(newDistinctCountStats)

    val allAttrs = AttributeSet(newMinMaxStats.map(_._1) ++ newNullCountStats.map(_._1) ++
      newDistinctCountStats.map(_._1))
    val newAttrStats = AttributeMap(allAttrs.map { attr =>
      val base = minMaxMap.getOrElse(attr, ColumnStat())
      val withNull = nullCountMap.get(attr).fold(base)(s => base.copy(nullCount = s.nullCount))
      val withDistinctCount = distinctCountMap.get(attr)
        .fold(withNull)(s => withNull.copy(distinctCount = s.distinctCount))
      attr -> withDistinctCount
    })

    Some(
      Statistics(
        sizeInBytes = sizeInBytes,
        rowCount = outputRows,
        attributeStats = newAttrStats))
  }

  private def computeMinMaxStats(union: Union): Seq[(Attribute, ColumnStat)] = {
    union.children.map(_.output).transpose.zip(union.output).flatMap {
      case (attrs, outputAttr) if isTypeSupported(outputAttr.dataType) =>
        val statComparator = createStatComparator(outputAttr.dataType)
        val zipped = attrs.zip(union.children)
        val (firstAttr, firstChild) = zipped.head
        val initialMinMax = firstChild.stats.attributeStats.get(firstAttr)
          .collect { case s if s.hasMinMaxStats => (s.min.get, s.max.get) }
        val maybeMinMax = zipped.tail.foldLeft(initialMinMax) {
          case (Some((minVal, maxVal)), (attr, child)) =>
            child.stats.attributeStats.get(attr).collect {
              case s if s.hasMinMaxStats =>
                val min = if (statComparator(s.min.get, minVal)) s.min.get else minVal
                val max = if (statComparator(maxVal, s.max.get)) s.max.get else maxVal
                (min, max)
            }
          case _ => None
        }
        maybeMinMax.map { case (min, max) =>
          outputAttr -> ColumnStat(min = Some(min), max = Some(max))
        }
      case _ => None
    }
  }

  private def computeNullCountStats(union: Union): Seq[(Attribute, ColumnStat)] = {
    union.children.map(_.output).transpose.zip(union.output).flatMap {
      case (attrs, outputAttr) =>
        val maybeTotalNullCount = attrs.zip(union.children).foldLeft(Option(BigInt(0))) {
          case (Some(total), (attr, child)) =>
            child.stats.attributeStats.get(attr).flatMap(_.nullCount).map(total + _)
          case _ => None
        }
        maybeTotalNullCount.map { totalNullCount =>
          outputAttr -> ColumnStat(nullCount = Some(totalNullCount))
        }
    }
  }

  /**
   * For each column, if all children have distinctCount, propagate the max distinctCount
   * across children. The result is capped by outputRows when available.
   *
   * For UNION ALL, true distinctCount satisfies:
   *   max(dc_i) <= true_dc <= min(sum(dc_i), rowCount)
   * We use max as a lower-bound estimate. This may underestimate when branches have
   * disjoint values, but UNION ALL branches typically share overlapping values
   * (e.g. web_sales and catalog_sales reference the same date keys),
   * so max is a reasonable approximation in the common case.
   */
  private def computeDistinctCountStats(
      union: Union,
      outputRows: Option[BigInt]): Seq[(Attribute, ColumnStat)] = {
    union.children.map(_.output).transpose.zip(union.output).flatMap {
      case (attrs, outputAttr) =>
        val maybeMaxDistinctCount = attrs.zip(union.children).foldLeft(Option(BigInt(0))) {
          case (Some(currentMax), (attr, child)) =>
            child.stats.attributeStats.get(attr).flatMap(_.distinctCount).map(currentMax.max)
          case _ => None
        }
        maybeMaxDistinctCount.map { maxDistinctCount =>
          // Cap distinctCount by outputRows if available
          val cappedDistinctCount = outputRows.fold(maxDistinctCount)(maxDistinctCount.min)
          outputAttr -> ColumnStat(distinctCount = Some(cappedDistinctCount))
        }
    }
  }
}
