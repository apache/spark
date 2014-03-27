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

package org.apache.spark.sql.columnar

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.types._

private[sql] sealed abstract class ColumnStats[T <: NativeType] extends Serializable{
  type JvmType = T#JvmType

  protected var (_lower, _upper) = initialBounds

  protected val ordering: Ordering[JvmType]

  protected def columnType: NativeColumnType[T]

  /**
   * Closed lower bound of this column.
   */
  def lowerBound = _lower

  /**
   * Closed upper bound of this column.
   */
  def upperBound = _upper

  /**
   * Initial values for the closed lower/upper bounds, in the format of `(lower, upper)`.
   */
  protected def initialBounds: (JvmType, JvmType)

  /**
   * Gathers statistics information from `row(ordinal)`.
   */
  @inline def gatherStats(row: Row, ordinal: Int) {
    val field = columnType.getField(row, ordinal)
    if (ordering.gt(field, upperBound)) _upper = field
    if (ordering.lt(field, lowerBound)) _lower = field
  }

  /**
   * Returns `true` if `lower <= row(ordinal) <= upper`.
   */
  @inline def contains(row: Row, ordinal: Int) = {
    val field = columnType.getField(row, ordinal)
    ordering.lteq(lowerBound, field) && ordering.lteq(field, upperBound)
  }

  /**
   * Returns `true` if `row(ordinal) < upper` holds.
   */
  @inline def isAbove(row: Row, ordinal: Int) = {
    val field = columnType.getField(row, ordinal)
    ordering.lt(field, upperBound)
  }

  /**
   * Returns `true` if `lower < row(ordinal)` holds.
   */
  @inline def isBelow(row: Row, ordinal: Int) = {
    val field = columnType.getField(row, ordinal)
    ordering.lt(lowerBound, field)
  }

  /**
   * Returns `true` if `row(ordinal) <= upper` holds.
   */
  @inline def isAtOrAbove(row: Row, ordinal: Int) = {
    contains(row, ordinal) || isAbove(row, ordinal)
  }

  /**
   * Returns `true` if `lower <= row(ordinal)` holds.
   */
  @inline def isAtOrBelow(row: Row, ordinal: Int) = {
    contains(row, ordinal) || isBelow(row, ordinal)
  }
}

private[sql] abstract class BasicColumnStats[T <: NativeType](
    protected val columnType: NativeColumnType[T])
  extends ColumnStats[T]

private[sql] class BooleanColumnStats extends BasicColumnStats(BOOLEAN) {
  override protected val ordering = implicitly[Ordering[JvmType]]
  override protected def initialBounds = (true, false)
}

private[sql] class ByteColumnStats extends BasicColumnStats(BYTE) {
  override protected val ordering = implicitly[Ordering[JvmType]]
  override protected def initialBounds = (Byte.MaxValue, Byte.MinValue)
}

private[sql] class ShortColumnStats extends BasicColumnStats(SHORT) {
  override protected val ordering = implicitly[Ordering[JvmType]]
  override protected def initialBounds = (Short.MaxValue, Short.MinValue)
}

private[sql] class LongColumnStats extends BasicColumnStats(LONG) {
  override protected val ordering = implicitly[Ordering[JvmType]]
  override protected def initialBounds = (Long.MaxValue, Long.MinValue)
}

private[sql] class DoubleColumnStats extends BasicColumnStats(DOUBLE) {
  override protected val ordering = implicitly[Ordering[JvmType]]
  override protected def initialBounds = (Double.MaxValue, Double.MinValue)
}

private[sql] class FloatColumnStats extends BasicColumnStats(FLOAT) {
  override protected val ordering = implicitly[Ordering[JvmType]]
  override protected def initialBounds = (Float.MaxValue, Float.MinValue)
}

private[sql] class IntColumnStats extends BasicColumnStats(INT) {
  private object OrderedState extends Enumeration {
    val Uninitialized, Initialized, Ascending, Descending, Unordered = Value
  }

  import OrderedState._

  private var orderedState = Uninitialized
  private var lastValue: Int = _
  private var _maxDelta: Int = _

  def isAscending = orderedState != Descending && orderedState != Unordered
  def isDescending = orderedState != Ascending && orderedState != Unordered
  def isOrdered = isAscending || isDescending
  def maxDelta = _maxDelta

  override protected val ordering = implicitly[Ordering[JvmType]]
  override protected def initialBounds = (Int.MaxValue, Int.MinValue)

  override def gatherStats(row: Row, ordinal: Int) = {
    val field = columnType.getField(row, ordinal)

    if (field > upperBound) _upper = field
    if (field < lowerBound) _lower = field

    orderedState = orderedState match {
      case Uninitialized =>
        lastValue = field
        Initialized

      case Initialized =>
        // If all the integers in the column are the same, ordered state is set to Ascending.
        // TODO (lian) Confirm whether this is the standard behaviour.
        val nextState = if (field >= lastValue) Ascending else Descending
        _maxDelta = math.abs(field - lastValue)
        lastValue = field
        nextState

      case Ascending if field < lastValue =>
        Unordered

      case Descending if field > lastValue =>
        Unordered

      case state @ (Ascending | Descending) =>
        _maxDelta = _maxDelta.max(field - lastValue)
        lastValue = field
        state
    }
  }
}

private[sql] class StringColumnStates extends BasicColumnStats(STRING) {
  override protected val ordering = implicitly[Ordering[JvmType]]
  override protected def initialBounds = (null, null)

  override def contains(row: Row, ordinal: Int) = {
    !(upperBound eq null) && super.contains(row, ordinal)
  }

  override def isAbove(row: Row, ordinal: Int) = {
    !(upperBound eq null) && super.isAbove(row, ordinal)
  }

  override def isBelow(row: Row, ordinal: Int) = {
    !(lowerBound eq null) && super.isBelow(row, ordinal)
  }
}
