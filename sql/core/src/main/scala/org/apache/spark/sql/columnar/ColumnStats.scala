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

/**
 * Used to collect statistical information when building in-memory columns.
 *
 * NOTE: we intentionally avoid using `Ordering[T]` to compare values here because `Ordering[T]`
 * brings significant performance penalty.
 */
private[sql] sealed abstract class ColumnStats[T <: DataType, JvmType] extends Serializable {
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

  def initialBounds: (JvmType, JvmType)

  protected def columnType: NativeColumnType[T]

  override def lowerBound: T#JvmType = _lower

  override def upperBound: T#JvmType = _upper

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
  override def initialBounds = (true, false)

  override def isBelow(row: Row, ordinal: Int) = {
    lowerBound < columnType.getField(row, ordinal)
  }

  override def isAbove(row: Row, ordinal: Int) = {
    columnType.getField(row, ordinal) < upperBound
  }

  override def contains(row: Row, ordinal: Int) = {
    val field = columnType.getField(row, ordinal)
    lowerBound <= field && field <= upperBound
  }

  override def gatherStats(row: Row, ordinal: Int) {
    val field = columnType.getField(row, ordinal)
    if (field > upperBound) _upper = field
    if (field < lowerBound) _lower = field
  }
}

private[sql] class ByteColumnStats extends BasicColumnStats(BYTE) {
  override def initialBounds = (Byte.MaxValue, Byte.MinValue)

  override def isBelow(row: Row, ordinal: Int) = {
    lowerBound < columnType.getField(row, ordinal)
  }

  override def isAbove(row: Row, ordinal: Int) = {
    columnType.getField(row, ordinal) < upperBound
  }

  override def contains(row: Row, ordinal: Int) = {
    val field = columnType.getField(row, ordinal)
    lowerBound <= field && field <= upperBound
  }

  override def gatherStats(row: Row, ordinal: Int) {
    val field = columnType.getField(row, ordinal)
    if (field > upperBound) _upper = field
    if (field < lowerBound) _lower = field
  }
}

private[sql] class ShortColumnStats extends BasicColumnStats(SHORT) {
  override def initialBounds = (Short.MaxValue, Short.MinValue)

  override def isBelow(row: Row, ordinal: Int) = {
    lowerBound < columnType.getField(row, ordinal)
  }

  override def isAbove(row: Row, ordinal: Int) = {
    columnType.getField(row, ordinal) < upperBound
  }

  override def contains(row: Row, ordinal: Int) = {
    val field = columnType.getField(row, ordinal)
    lowerBound <= field && field <= upperBound
  }

  override def gatherStats(row: Row, ordinal: Int) {
    val field = columnType.getField(row, ordinal)
    if (field > upperBound) _upper = field
    if (field < lowerBound) _lower = field
  }
}

private[sql] class LongColumnStats extends BasicColumnStats(LONG) {
  override def initialBounds = (Long.MaxValue, Long.MinValue)

  override def isBelow(row: Row, ordinal: Int) = {
    lowerBound < columnType.getField(row, ordinal)
  }

  override def isAbove(row: Row, ordinal: Int) = {
    columnType.getField(row, ordinal) < upperBound
  }

  override def contains(row: Row, ordinal: Int) = {
    val field = columnType.getField(row, ordinal)
    lowerBound <= field && field <= upperBound
  }

  override def gatherStats(row: Row, ordinal: Int) {
    val field = columnType.getField(row, ordinal)
    if (field > upperBound) _upper = field
    if (field < lowerBound) _lower = field
  }
}

private[sql] class DoubleColumnStats extends BasicColumnStats(DOUBLE) {
  override def initialBounds = (Double.MaxValue, Double.MinValue)

  override def isBelow(row: Row, ordinal: Int) = {
    lowerBound < columnType.getField(row, ordinal)
  }

  override def isAbove(row: Row, ordinal: Int) = {
    columnType.getField(row, ordinal) < upperBound
  }

  override def contains(row: Row, ordinal: Int) = {
    val field = columnType.getField(row, ordinal)
    lowerBound <= field && field <= upperBound
  }

  override def gatherStats(row: Row, ordinal: Int) {
    val field = columnType.getField(row, ordinal)
    if (field > upperBound) _upper = field
    if (field < lowerBound) _lower = field
  }
}

private[sql] class FloatColumnStats extends BasicColumnStats(FLOAT) {
  override def initialBounds = (Float.MaxValue, Float.MinValue)

  override def isBelow(row: Row, ordinal: Int) = {
    lowerBound < columnType.getField(row, ordinal)
  }

  override def isAbove(row: Row, ordinal: Int) = {
    columnType.getField(row, ordinal) < upperBound
  }

  override def contains(row: Row, ordinal: Int) = {
    val field = columnType.getField(row, ordinal)
    lowerBound <= field && field <= upperBound
  }

  override def gatherStats(row: Row, ordinal: Int) {
    val field = columnType.getField(row, ordinal)
    if (field > upperBound) _upper = field
    if (field < lowerBound) _lower = field
  }
}

private[sql] object IntColumnStats {
  val UNINITIALIZED = 0
  val INITIALIZED = 1
  val ASCENDING = 2
  val DESCENDING = 3
  val UNORDERED = 4
}

/**
 * Statistical information for `Int` columns. More information is collected since `Int` is
 * frequently used. Extra information include:
 *
 * - Ordering state (ascending/descending/unordered), may be used to decide whether binary search
 *   is applicable when searching elements.
 * - Maximum delta between adjacent elements, may be used to guide the `IntDelta` compression
 *   scheme.
 *
 * (This two kinds of information are not used anywhere yet and might be removed later.)
 */
private[sql] class IntColumnStats extends BasicColumnStats(INT) {
  import IntColumnStats._

  private var orderedState = UNINITIALIZED
  private var lastValue: Int = _
  private var _maxDelta: Int = _

  def isAscending = orderedState != DESCENDING && orderedState != UNORDERED
  def isDescending = orderedState != ASCENDING && orderedState != UNORDERED
  def isOrdered = isAscending || isDescending
  def maxDelta = _maxDelta

  override def initialBounds = (Int.MaxValue, Int.MinValue)

  override def isBelow(row: Row, ordinal: Int) = {
    lowerBound < columnType.getField(row, ordinal)
  }

  override def isAbove(row: Row, ordinal: Int) = {
    columnType.getField(row, ordinal) < upperBound
  }

  override def contains(row: Row, ordinal: Int) = {
    val field = columnType.getField(row, ordinal)
    lowerBound <= field && field <= upperBound
  }

  override def gatherStats(row: Row, ordinal: Int) {
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
  override def initialBounds = (null, null)

  override def gatherStats(row: Row, ordinal: Int) {
    val field = columnType.getField(row, ordinal)
    if ((upperBound eq null) || field.compareTo(upperBound) > 0) _upper = field
    if ((lowerBound eq null) || field.compareTo(lowerBound) < 0) _lower = field
  }

  override def contains(row: Row, ordinal: Int) = {
    (upperBound ne null) && {
      val field = columnType.getField(row, ordinal)
      lowerBound.compareTo(field) <= 0 && field.compareTo(upperBound) <= 0
    }
  }

  override def isAbove(row: Row, ordinal: Int) = {
    (upperBound ne null) && {
      val field = columnType.getField(row, ordinal)
      field.compareTo(upperBound) < 0
    }
  }

  override def isBelow(row: Row, ordinal: Int) = {
    (lowerBound ne null) && {
      val field = columnType.getField(row, ordinal)
      lowerBound.compareTo(field) < 0
    }
  }
}

private[sql] class TimestampColumnStats extends BasicColumnStats(TIMESTAMP) {
  override def initialBounds = (null, null)

  override def gatherStats(row: Row, ordinal: Int) {
    val field = columnType.getField(row, ordinal)
    if ((upperBound eq null) || field.compareTo(upperBound) > 0) _upper = field
    if ((lowerBound eq null) || field.compareTo(lowerBound) < 0) _lower = field
  }

  override def contains(row: Row, ordinal: Int) = {
    (upperBound ne null) && {
      val field = columnType.getField(row, ordinal)
      lowerBound.compareTo(field) <= 0 && field.compareTo(upperBound) <= 0
    }
  }

  override def isAbove(row: Row, ordinal: Int) = {
    (lowerBound ne null) && {
      val field = columnType.getField(row, ordinal)
      field.compareTo(upperBound) < 0
    }
  }

  override def isBelow(row: Row, ordinal: Int) = {
    (lowerBound ne null) && {
      val field = columnType.getField(row, ordinal)
      lowerBound.compareTo(field) < 0
    }
  }
}
