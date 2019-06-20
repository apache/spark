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

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types._


/** Value range of a column. */
trait ValueInterval {
  def contains(l: Literal): Boolean
}

/** For simplicity we use double to unify operations of numeric intervals. */
case class NumericValueInterval(min: Double, max: Double) extends ValueInterval {
  override def contains(l: Literal): Boolean = {
    val lit = EstimationUtils.toDouble(l.value, l.dataType)
    min <= lit && max >= lit
  }
}

/**
 * This version of Spark does not have min/max for binary/string types, we define their default
 * behaviors by this class.
 */
class DefaultValueInterval extends ValueInterval {
  override def contains(l: Literal): Boolean = true
}

/** This is for columns with only null values. */
class NullValueInterval extends ValueInterval {
  override def contains(l: Literal): Boolean = false
}

object ValueInterval {
  def apply(
      min: Option[Any],
      max: Option[Any],
      dataType: DataType): ValueInterval = dataType match {
    case StringType | BinaryType => new DefaultValueInterval()
    case _ if min.isEmpty || max.isEmpty => new NullValueInterval()
    case _ =>
      NumericValueInterval(
        min = EstimationUtils.toDouble(min.get, dataType),
        max = EstimationUtils.toDouble(max.get, dataType))
  }

  def isIntersected(r1: ValueInterval, r2: ValueInterval): Boolean = (r1, r2) match {
    case (_, _: DefaultValueInterval) | (_: DefaultValueInterval, _) =>
      // The DefaultValueInterval represents string/binary types which do not have max/min stats,
      // we assume they are intersected to be conservative on estimation
      true
    case (_, _: NullValueInterval) | (_: NullValueInterval, _) =>
      false
    case (n1: NumericValueInterval, n2: NumericValueInterval) =>
      n1.min.compareTo(n2.max) <= 0 && n1.max.compareTo(n2.min) >= 0
    case _ =>
      throw new UnsupportedOperationException(s"Not supported pair: $r1, $r2 at isIntersected()")
  }

  /**
   * Intersected results of two intervals. This is only for two overlapped intervals.
   * The outputs are the intersected min/max values.
   */
  def intersect(r1: ValueInterval, r2: ValueInterval, dt: DataType): (Option[Any], Option[Any]) = {
    (r1, r2) match {
      case (_, _: DefaultValueInterval) | (_: DefaultValueInterval, _) =>
        // binary/string types don't support intersecting.
        (None, None)
      case (n1: NumericValueInterval, n2: NumericValueInterval) =>
        // Choose the maximum of two min values, and the minimum of two max values.
        val newMin = if (n1.min <= n2.min) n2.min else n1.min
        val newMax = if (n1.max <= n2.max) n1.max else n2.max
        (Some(EstimationUtils.fromDouble(newMin, dt)),
          Some(EstimationUtils.fromDouble(newMax, dt)))
      case _ =>
        throw new UnsupportedOperationException(s"Not supported pair: $r1, $r2 at intersect()")
    }
  }
}
