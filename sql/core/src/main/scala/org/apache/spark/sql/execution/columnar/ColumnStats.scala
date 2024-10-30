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

package org.apache.spark.sql.execution.columnar

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class ColumnStatisticsSchema(a: Attribute) extends Serializable {
  val upperBound = AttributeReference(a.name + ".upperBound", a.dataType, nullable = true)()
  val lowerBound = AttributeReference(a.name + ".lowerBound", a.dataType, nullable = true)()
  val nullCount = AttributeReference(a.name + ".nullCount", IntegerType, nullable = false)()
  val count = AttributeReference(a.name + ".count", IntegerType, nullable = false)()
  val sizeInBytes = AttributeReference(a.name + ".sizeInBytes", LongType, nullable = false)()

  val schema = Seq(lowerBound, upperBound, nullCount, count, sizeInBytes)
}

class PartitionStatistics(tableSchema: Seq[Attribute]) extends Serializable {
  val (forAttribute: AttributeMap[ColumnStatisticsSchema], schema: Seq[AttributeReference]) = {
    val allStats = tableSchema.map(a => a -> new ColumnStatisticsSchema(a))
    (AttributeMap(allStats), allStats.flatMap(_._2.schema))
  }
}

/**
 * Used to collect statistical information when building in-memory columns.
 *
 * NOTE: we intentionally avoid using `Ordering[T]` to compare values here because `Ordering[T]`
 * brings significant performance penalty.
 */
private[columnar] sealed trait ColumnStats extends Serializable {
  protected var count = 0
  protected var nullCount = 0
  private[columnar] var sizeInBytes = 0L

  /**
   * Gathers statistics information from `row(ordinal)`.
   */
  def gatherStats(row: InternalRow, ordinal: Int): Unit

  /**
   * Gathers statistics information on `null`.
   */
  def gatherNullStats(): Unit = {
    nullCount += 1
    // 4 bytes for null position
    sizeInBytes += 4
    count += 1
  }

  /**
   * Column statistics represented as an array, currently including closed lower bound, closed
   * upper bound and null count.
   */
  def collectedStatistics: Array[Any]
}

/**
 * A no-op ColumnStats only used for testing purposes.
 */
private[columnar] final class NoopColumnStats extends ColumnStats {
  override def gatherStats(row: InternalRow, ordinal: Int): Unit = {
    if (!row.isNullAt(ordinal)) {
      count += 1
    } else {
      gatherNullStats()
    }
  }

  override def collectedStatistics: Array[Any] = Array[Any](null, null, nullCount, count, 0L)
}

private[columnar] final class BooleanColumnStats extends ColumnStats {
  protected var upper = false
  protected var lower = true

  override def gatherStats(row: InternalRow, ordinal: Int): Unit = {
    if (!row.isNullAt(ordinal)) {
      val value = row.getBoolean(ordinal)
      gatherValueStats(value)
    } else {
      gatherNullStats()
    }
  }

  def gatherValueStats(value: Boolean): Unit = {
    if (value > upper) upper = value
    if (value < lower) lower = value
    sizeInBytes += BOOLEAN.defaultSize
    count += 1
  }

  override def collectedStatistics: Array[Any] =
    Array[Any](lower, upper, nullCount, count, sizeInBytes)
}

private[columnar] final class ByteColumnStats extends ColumnStats {
  protected var upper = Byte.MinValue
  protected var lower = Byte.MaxValue

  override def gatherStats(row: InternalRow, ordinal: Int): Unit = {
    if (!row.isNullAt(ordinal)) {
      val value = row.getByte(ordinal)
      gatherValueStats(value)
    } else {
      gatherNullStats()
    }
  }

  def gatherValueStats(value: Byte): Unit = {
    if (value > upper) upper = value
    if (value < lower) lower = value
    sizeInBytes += BYTE.defaultSize
    count += 1
  }

  override def collectedStatistics: Array[Any] =
    Array[Any](lower, upper, nullCount, count, sizeInBytes)
}

private[columnar] final class ShortColumnStats extends ColumnStats {
  protected var upper = Short.MinValue
  protected var lower = Short.MaxValue

  override def gatherStats(row: InternalRow, ordinal: Int): Unit = {
    if (!row.isNullAt(ordinal)) {
      val value = row.getShort(ordinal)
      gatherValueStats(value)
    } else {
      gatherNullStats()
    }
  }

  def gatherValueStats(value: Short): Unit = {
    if (value > upper) upper = value
    if (value < lower) lower = value
    sizeInBytes += SHORT.defaultSize
    count += 1
  }

  override def collectedStatistics: Array[Any] =
    Array[Any](lower, upper, nullCount, count, sizeInBytes)
}

private[columnar] final class IntColumnStats extends ColumnStats {
  protected var upper = Int.MinValue
  protected var lower = Int.MaxValue

  override def gatherStats(row: InternalRow, ordinal: Int): Unit = {
    if (!row.isNullAt(ordinal)) {
      val value = row.getInt(ordinal)
      gatherValueStats(value)
    } else {
      gatherNullStats()
    }
  }

  def gatherValueStats(value: Int): Unit = {
    if (value > upper) upper = value
    if (value < lower) lower = value
    sizeInBytes += INT.defaultSize
    count += 1
  }

  override def collectedStatistics: Array[Any] =
    Array[Any](lower, upper, nullCount, count, sizeInBytes)
}

private[columnar] final class LongColumnStats extends ColumnStats {
  protected var upper = Long.MinValue
  protected var lower = Long.MaxValue

  override def gatherStats(row: InternalRow, ordinal: Int): Unit = {
    if (!row.isNullAt(ordinal)) {
      val value = row.getLong(ordinal)
      gatherValueStats(value)
    } else {
      gatherNullStats()
    }
  }

  def gatherValueStats(value: Long): Unit = {
    if (value > upper) upper = value
    if (value < lower) lower = value
    sizeInBytes += LONG.defaultSize
    count += 1
  }

  override def collectedStatistics: Array[Any] =
    Array[Any](lower, upper, nullCount, count, sizeInBytes)
}

private[columnar] final class FloatColumnStats extends ColumnStats {
  protected var upper = Float.MinValue
  protected var lower = Float.MaxValue

  override def gatherStats(row: InternalRow, ordinal: Int): Unit = {
    if (!row.isNullAt(ordinal)) {
      val value = row.getFloat(ordinal)
      gatherValueStats(value)
    } else {
      gatherNullStats()
    }
  }

  def gatherValueStats(value: Float): Unit = {
    if (value > upper) upper = value
    if (value < lower) lower = value
    sizeInBytes += FLOAT.defaultSize
    count += 1
  }

  override def collectedStatistics: Array[Any] =
    Array[Any](lower, upper, nullCount, count, sizeInBytes)
}

private[columnar] final class DoubleColumnStats extends ColumnStats {
  protected var upper = Double.MinValue
  protected var lower = Double.MaxValue

  override def gatherStats(row: InternalRow, ordinal: Int): Unit = {
    if (!row.isNullAt(ordinal)) {
      val value = row.getDouble(ordinal)
      gatherValueStats(value)
    } else {
      gatherNullStats()
    }
  }

  def gatherValueStats(value: Double): Unit = {
    if (value > upper) upper = value
    if (value < lower) lower = value
    sizeInBytes += DOUBLE.defaultSize
    count += 1
  }

  override def collectedStatistics: Array[Any] =
    Array[Any](lower, upper, nullCount, count, sizeInBytes)
}

private[columnar] final class StringColumnStats(collationId: Int) extends ColumnStats {
  def this(dt: StringType) = this(dt.collationId)

  protected var upper: UTF8String = null
  protected var lower: UTF8String = null

  override def gatherStats(row: InternalRow, ordinal: Int): Unit = {
    if (!row.isNullAt(ordinal)) {
      val value = row.getUTF8String(ordinal)
      val size = STRING(collationId).actualSize(row, ordinal)
      gatherValueStats(value, size)
    } else {
      gatherNullStats()
    }
  }

  def gatherValueStats(value: UTF8String, size: Int): Unit = {
    if (upper == null || value.semanticCompare(upper, collationId) > 0) upper = value.clone()
    if (lower == null || value.semanticCompare(lower, collationId) < 0) lower = value.clone()
    sizeInBytes += size
    count += 1
  }

  override def collectedStatistics: Array[Any] =
    Array[Any](lower, upper, nullCount, count, sizeInBytes)
}

private[columnar] final class BinaryColumnStats extends ColumnStats {
  override def gatherStats(row: InternalRow, ordinal: Int): Unit = {
    if (!row.isNullAt(ordinal)) {
      val size = BINARY.actualSize(row, ordinal)
      sizeInBytes += size
      count += 1
    } else {
      gatherNullStats()
    }
  }

  override def collectedStatistics: Array[Any] =
    Array[Any](null, null, nullCount, count, sizeInBytes)
}

private[columnar] final class VariantColumnStats extends ColumnStats {
  override def gatherStats(row: InternalRow, ordinal: Int): Unit = {
    if (!row.isNullAt(ordinal)) {
      val size = VARIANT.actualSize(row, ordinal)
      sizeInBytes += size
      count += 1
    } else {
      gatherNullStats()
    }
  }

  override def collectedStatistics: Array[Any] =
    Array[Any](null, null, nullCount, count, sizeInBytes)
}

private[columnar] final class IntervalColumnStats extends ColumnStats {
  override def gatherStats(row: InternalRow, ordinal: Int): Unit = {
    if (!row.isNullAt(ordinal)) {
      sizeInBytes += CALENDAR_INTERVAL.actualSize(row, ordinal)
      count += 1
    } else {
      gatherNullStats()
    }
  }

  override def collectedStatistics: Array[Any] =
    Array[Any](null, null, nullCount, count, sizeInBytes)
}

private[columnar] final class DecimalColumnStats(precision: Int, scale: Int) extends ColumnStats {
  def this(dt: DecimalType) = this(dt.precision, dt.scale)

  protected var upper: Decimal = null
  protected var lower: Decimal = null

  override def gatherStats(row: InternalRow, ordinal: Int): Unit = {
    if (!row.isNullAt(ordinal)) {
      val value = row.getDecimal(ordinal, precision, scale)
      // TODO: this is not right for DecimalType with precision > 18
      gatherValueStats(value)
    } else {
      gatherNullStats()
    }
  }

  def gatherValueStats(value: Decimal): Unit = {
    if (upper == null || value.compareTo(upper) > 0) upper = value
    if (lower == null || value.compareTo(lower) < 0) lower = value
    sizeInBytes += 8
    count += 1
  }

  override def collectedStatistics: Array[Any] =
    Array[Any](lower, upper, nullCount, count, sizeInBytes)
}

private[columnar] final class ObjectColumnStats(dataType: DataType) extends ColumnStats {
  val columnType = ColumnType(dataType)

  override def gatherStats(row: InternalRow, ordinal: Int): Unit = {
    if (!row.isNullAt(ordinal)) {
      val size = columnType.actualSize(row, ordinal)
      sizeInBytes += size
      count += 1
    } else {
      gatherNullStats()
    }
  }

  override def collectedStatistics: Array[Any] =
    Array[Any](null, null, nullCount, count, sizeInBytes)
}
