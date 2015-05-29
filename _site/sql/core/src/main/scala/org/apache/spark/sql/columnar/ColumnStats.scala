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

import java.sql.Timestamp

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{AttributeMap, Attribute, AttributeReference}
import org.apache.spark.sql.types._

private[sql] class ColumnStatisticsSchema(a: Attribute) extends Serializable {
  val upperBound = AttributeReference(a.name + ".upperBound", a.dataType, nullable = true)()
  val lowerBound = AttributeReference(a.name + ".lowerBound", a.dataType, nullable = true)()
  val nullCount = AttributeReference(a.name + ".nullCount", IntegerType, nullable = false)()
  val count = AttributeReference(a.name + ".count", IntegerType, nullable = false)()
  val sizeInBytes = AttributeReference(a.name + ".sizeInBytes", LongType, nullable = false)()

  val schema = Seq(lowerBound, upperBound, nullCount, count, sizeInBytes)
}

private[sql] class PartitionStatistics(tableSchema: Seq[Attribute]) extends Serializable {
  val (forAttribute, schema) = {
    val allStats = tableSchema.map(a => a -> new ColumnStatisticsSchema(a))
    (AttributeMap(allStats), allStats.map(_._2.schema).foldLeft(Seq.empty[Attribute])(_ ++ _))
  }
}

/**
 * Used to collect statistical information when building in-memory columns.
 *
 * NOTE: we intentionally avoid using `Ordering[T]` to compare values here because `Ordering[T]`
 * brings significant performance penalty.
 */
private[sql] sealed trait ColumnStats extends Serializable {
  protected var count = 0
  protected var nullCount = 0
  protected var sizeInBytes = 0L

  /**
   * Gathers statistics information from `row(ordinal)`.
   */
  def gatherStats(row: Row, ordinal: Int): Unit = {
    if (row.isNullAt(ordinal)) {
      nullCount += 1
      // 4 bytes for null position
      sizeInBytes += 4
    }
    count += 1
  }

  /**
   * Column statistics represented as a single row, currently including closed lower bound, closed
   * upper bound and null count.
   */
  def collectedStatistics: Row
}

/**
 * A no-op ColumnStats only used for testing purposes.
 */
private[sql] class NoopColumnStats extends ColumnStats {
  override def gatherStats(row: Row, ordinal: Int): Unit = super.gatherStats(row, ordinal)

  override def collectedStatistics: Row = Row(null, null, nullCount, count, 0L)
}

private[sql] class BooleanColumnStats extends ColumnStats {
  protected var upper = false
  protected var lower = true

  override def gatherStats(row: Row, ordinal: Int): Unit = {
    super.gatherStats(row, ordinal)
    if (!row.isNullAt(ordinal)) {
      val value = row.getBoolean(ordinal)
      if (value > upper) upper = value
      if (value < lower) lower = value
      sizeInBytes += BOOLEAN.defaultSize
    }
  }

  override def collectedStatistics: Row = Row(lower, upper, nullCount, count, sizeInBytes)
}

private[sql] class ByteColumnStats extends ColumnStats {
  protected var upper = Byte.MinValue
  protected var lower = Byte.MaxValue

  override def gatherStats(row: Row, ordinal: Int): Unit = {
    super.gatherStats(row, ordinal)
    if (!row.isNullAt(ordinal)) {
      val value = row.getByte(ordinal)
      if (value > upper) upper = value
      if (value < lower) lower = value
      sizeInBytes += BYTE.defaultSize
    }
  }

  override def collectedStatistics: Row = Row(lower, upper, nullCount, count, sizeInBytes)
}

private[sql] class ShortColumnStats extends ColumnStats {
  protected var upper = Short.MinValue
  protected var lower = Short.MaxValue

  override def gatherStats(row: Row, ordinal: Int): Unit = {
    super.gatherStats(row, ordinal)
    if (!row.isNullAt(ordinal)) {
      val value = row.getShort(ordinal)
      if (value > upper) upper = value
      if (value < lower) lower = value
      sizeInBytes += SHORT.defaultSize
    }
  }

  override def collectedStatistics: Row = Row(lower, upper, nullCount, count, sizeInBytes)
}

private[sql] class LongColumnStats extends ColumnStats {
  protected var upper = Long.MinValue
  protected var lower = Long.MaxValue

  override def gatherStats(row: Row, ordinal: Int): Unit = {
    super.gatherStats(row, ordinal)
    if (!row.isNullAt(ordinal)) {
      val value = row.getLong(ordinal)
      if (value > upper) upper = value
      if (value < lower) lower = value
      sizeInBytes += LONG.defaultSize
    }
  }

  override def collectedStatistics: Row = Row(lower, upper, nullCount, count, sizeInBytes)
}

private[sql] class DoubleColumnStats extends ColumnStats {
  protected var upper = Double.MinValue
  protected var lower = Double.MaxValue

  override def gatherStats(row: Row, ordinal: Int): Unit = {
    super.gatherStats(row, ordinal)
    if (!row.isNullAt(ordinal)) {
      val value = row.getDouble(ordinal)
      if (value > upper) upper = value
      if (value < lower) lower = value
      sizeInBytes += DOUBLE.defaultSize
    }
  }

  override def collectedStatistics: Row = Row(lower, upper, nullCount, count, sizeInBytes)
}

private[sql] class FloatColumnStats extends ColumnStats {
  protected var upper = Float.MinValue
  protected var lower = Float.MaxValue

  override def gatherStats(row: Row, ordinal: Int): Unit = {
    super.gatherStats(row, ordinal)
    if (!row.isNullAt(ordinal)) {
      val value = row.getFloat(ordinal)
      if (value > upper) upper = value
      if (value < lower) lower = value
      sizeInBytes += FLOAT.defaultSize
    }
  }

  override def collectedStatistics: Row = Row(lower, upper, nullCount, count, sizeInBytes)
}

private[sql] class FixedDecimalColumnStats extends ColumnStats {
  protected var upper: Decimal = null
  protected var lower: Decimal = null

  override def gatherStats(row: Row, ordinal: Int): Unit = {
    super.gatherStats(row, ordinal)
    if (!row.isNullAt(ordinal)) {
      val value = row(ordinal).asInstanceOf[Decimal]
      if (upper == null || value.compareTo(upper) > 0) upper = value
      if (lower == null || value.compareTo(lower) < 0) lower = value
      sizeInBytes += FIXED_DECIMAL.defaultSize
    }
  }

  override def collectedStatistics: Row = Row(lower, upper, nullCount, count, sizeInBytes)
}

private[sql] class IntColumnStats extends ColumnStats {
  protected var upper = Int.MinValue
  protected var lower = Int.MaxValue

  override def gatherStats(row: Row, ordinal: Int): Unit = {
    super.gatherStats(row, ordinal)
    if (!row.isNullAt(ordinal)) {
      val value = row.getInt(ordinal)
      if (value > upper) upper = value
      if (value < lower) lower = value
      sizeInBytes += INT.defaultSize
    }
  }

  override def collectedStatistics: Row = Row(lower, upper, nullCount, count, sizeInBytes)
}

private[sql] class StringColumnStats extends ColumnStats {
  protected var upper: UTF8String = null
  protected var lower: UTF8String = null

  override def gatherStats(row: Row, ordinal: Int): Unit = {
    super.gatherStats(row, ordinal)
    if (!row.isNullAt(ordinal)) {
      val value = row(ordinal).asInstanceOf[UTF8String]
      if (upper == null || value.compareTo(upper) > 0) upper = value
      if (lower == null || value.compareTo(lower) < 0) lower = value
      sizeInBytes += STRING.actualSize(row, ordinal)
    }
  }

  override def collectedStatistics: Row = Row(lower, upper, nullCount, count, sizeInBytes)
}

private[sql] class DateColumnStats extends IntColumnStats

private[sql] class TimestampColumnStats extends ColumnStats {
  protected var upper: Timestamp = null
  protected var lower: Timestamp = null

  override def gatherStats(row: Row, ordinal: Int): Unit = {
    super.gatherStats(row, ordinal)
    if (!row.isNullAt(ordinal)) {
      val value = row(ordinal).asInstanceOf[Timestamp]
      if (upper == null || value.compareTo(upper) > 0) upper = value
      if (lower == null || value.compareTo(lower) < 0) lower = value
      sizeInBytes += TIMESTAMP.defaultSize
    }
  }

  override def collectedStatistics: Row = Row(lower, upper, nullCount, count, sizeInBytes)
}

private[sql] class BinaryColumnStats extends ColumnStats {
  override def gatherStats(row: Row, ordinal: Int): Unit = {
    super.gatherStats(row, ordinal)
    if (!row.isNullAt(ordinal)) {
      sizeInBytes += BINARY.actualSize(row, ordinal)
    }
  }

  override def collectedStatistics: Row = Row(null, null, nullCount, count, sizeInBytes)
}

private[sql] class GenericColumnStats extends ColumnStats {
  override def gatherStats(row: Row, ordinal: Int): Unit = {
    super.gatherStats(row, ordinal)
    if (!row.isNullAt(ordinal)) {
      sizeInBytes += GENERIC.actualSize(row, ordinal)
    }
  }

  override def collectedStatistics: Row = Row(null, null, nullCount, count, sizeInBytes)
}
