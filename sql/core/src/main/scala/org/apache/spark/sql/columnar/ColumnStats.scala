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

private[sql] sealed abstract class ColumnStats[T <: DataType, JvmType] extends Serializable{
  /**
   * Closed lower bound of this column.
   */
  def lowerBound: JvmType

  /**
   * Closed upper bound of this column.
   */
  def upperBound: JvmType

  /**
   * Gathers statistics information from `row(ordinal)`.
   */
  def gatherStats(row: Row, ordinal: Int)

  /**
   * Returns `true` if `lower <= row(ordinal) <= upper`.
   */
  def contains(row: Row, ordinal: Int): Boolean

  /**
   * Returns `true` if `row(ordinal) < upper` holds.
   */
  def isAbove(row: Row, ordinal: Int): Boolean

  /**
   * Returns `true` if `lower < row(ordinal)` holds.
   */
  def isBelow(row: Row, ordinal: Int): Boolean

  /**
   * Returns `true` if `row(ordinal) <= upper` holds.
   */
  def isAtOrAbove(row: Row, ordinal: Int): Boolean

  /**
   * Returns `true` if `lower <= row(ordinal)` holds.
   */
  def isAtOrBelow(row: Row, ordinal: Int): Boolean
}

private[sql] sealed abstract class NativeColumnStats[T <: NativeType]
  extends ColumnStats[T, T#JvmType] {

  type JvmType = T#JvmType

  protected var (_lower, _upper) = initialBounds

  val ordering: Ordering[JvmType]

  def initialBounds: (JvmType, JvmType)

  protected def columnType: NativeColumnType[T]

  override def lowerBound = _lower

  override def upperBound = _upper

  override def gatherStats(row: Row, ordinal: Int) {
    val field = columnType.getField(row, ordinal)
    if (upperBound == null || ordering.gt(field, upperBound)) _upper = field
    if (lowerBound == null || ordering.lt(field, lowerBound)) _lower = field
  }

  override def contains(row: Row, ordinal: Int) = {
    val field = columnType.getField(row, ordinal)
    ordering.lteq(lowerBound, field) && ordering.lteq(field, upperBound)
  }

  override def isAbove(row: Row, ordinal: Int) = {
    val field = columnType.getField(row, ordinal)
    ordering.lt(field, upperBound)
  }

  override def isBelow(row: Row, ordinal: Int) = {
    val field = columnType.getField(row, ordinal)
    ordering.lt(lowerBound, field)
  }

  override def isAtOrAbove(row: Row, ordinal: Int) = {
    contains(row, ordinal) || isAbove(row, ordinal)
  }

  override def isAtOrBelow(row: Row, ordinal: Int) = {
    contains(row, ordinal) || isBelow(row, ordinal)
  }
}

private[sql] class NoopColumnStats[T <: DataType, JvmType] extends ColumnStats[T, JvmType] {
  override def isAtOrBelow(row: Row, ordinal: Int) = true

  override def isAtOrAbove(row: Row, ordinal: Int) = true

  override def isBelow(row: Row, ordinal: Int) = true

  override def isAbove(row: Row, ordinal: Int) = true

  override def contains(row: Row, ordinal: Int) = true

  override def gatherStats(row: Row, ordinal: Int) {}

  override def upperBound = null.asInstanceOf[JvmType]

  override def lowerBound = null.asInstanceOf[JvmType]
}

private[sql] abstract class BasicColumnStats[T <: NativeType](
    protected val columnType: NativeColumnType[T])
  extends NativeColumnStats[T]

private[sql] class BooleanColumnStats extends BasicColumnStats(BOOLEAN) {
  override val ordering = implicitly[Ordering[JvmType]]
  override def initialBounds = (true, false)
}

private[sql] class ByteColumnStats extends BasicColumnStats(BYTE) {
  override val ordering = implicitly[Ordering[JvmType]]
  override def initialBounds = (Byte.MaxValue, Byte.MinValue)
}

private[sql] class ShortColumnStats extends BasicColumnStats(SHORT) {
  override val ordering = implicitly[Ordering[JvmType]]
  override def initialBounds = (Short.MaxValue, Short.MinValue)
}

private[sql] class LongColumnStats extends BasicColumnStats(LONG) {
  override val ordering = implicitly[Ordering[JvmType]]
  override def initialBounds = (Long.MaxValue, Long.MinValue)
}

private[sql] class DoubleColumnStats extends BasicColumnStats(DOUBLE) {
  override val ordering = implicitly[Ordering[JvmType]]
  override def initialBounds = (Double.MaxValue, Double.MinValue)
}

private[sql] class FloatColumnStats extends BasicColumnStats(FLOAT) {
  override val ordering = implicitly[Ordering[JvmType]]
  override def initialBounds = (Float.MaxValue, Float.MinValue)
}

object IntColumnStats {
  val UNINITIALIZED = 0
  val INITIALIZED = 1
  val ASCENDING = 2
  val DESCENDING = 3
  val UNORDERED = 4
}

private[sql] class IntColumnStats extends BasicColumnStats(INT) {
  import IntColumnStats._

  private var orderedState = UNINITIALIZED
  private var lastValue: Int = _
  private var _maxDelta: Int = _

  def isAscending = orderedState != DESCENDING && orderedState != UNORDERED
  def isDescending = orderedState != ASCENDING && orderedState != UNORDERED
  def isOrdered = isAscending || isDescending
  def maxDelta = _maxDelta

  override val ordering = implicitly[Ordering[JvmType]]
  override def initialBounds = (Int.MaxValue, Int.MinValue)

  override def gatherStats(row: Row, ordinal: Int) = {
    val field = columnType.getField(row, ordinal)

    if (field > upperBound) _upper = field
    if (field < lowerBound) _lower = field

    orderedState = orderedState match {
      case UNINITIALIZED =>
        lastValue = field
        INITIALIZED

      case INITIALIZED =>
        // If all the integers in the column are the same, ordered state is set to Ascending.
        // TODO (lian) Confirm whether this is the standard behaviour.
        val nextState = if (field >= lastValue) ASCENDING else DESCENDING
        _maxDelta = math.abs(field - lastValue)
        lastValue = field
        nextState

      case ASCENDING if field < lastValue =>
        UNORDERED

      case DESCENDING if field > lastValue =>
        UNORDERED

      case state @ (ASCENDING | DESCENDING) =>
        _maxDelta = _maxDelta.max(field - lastValue)
        lastValue = field
        state

      case _ =>
        orderedState
    }
  }
}

private[sql] class StringColumnStats extends BasicColumnStats(STRING) {
  override val ordering = implicitly[Ordering[JvmType]]
  override def initialBounds = (null, null)

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
