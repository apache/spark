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

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{AttributeMap, Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.types._

private[sql] class ColumnStatisticsSchema(a: Attribute) extends Serializable {
  val upperBound = AttributeReference(a.name + ".upperBound", a.dataType, nullable = false)()
  val lowerBound = AttributeReference(a.name + ".lowerBound", a.dataType, nullable = false)()
  val nullCount =  AttributeReference(a.name + ".nullCount", IntegerType, nullable = false)()

  val schema = Seq(lowerBound, upperBound, nullCount)
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
  /**
   * Gathers statistics information from `row(ordinal)`.
   */
  def gatherStats(row: Row, ordinal: Int): Unit

  /**
   * Column statistics represented as a single row, currently including closed lower bound, closed
   * upper bound and null count.
   */
  def collectedStatistics: Row
}

private[sql] class NoopColumnStats extends ColumnStats {

  override def gatherStats(row: Row, ordinal: Int): Unit = {}

  override def collectedStatistics = Row()
}

private[sql] class ByteColumnStats extends ColumnStats {
  var upper = Byte.MinValue
  var lower = Byte.MaxValue
  var nullCount = 0

  override def gatherStats(row: Row, ordinal: Int): Unit = {
    if (!row.isNullAt(ordinal)) {
      val value = row.getByte(ordinal)
      if (value > upper) upper = value
      if (value < lower) lower = value
    } else {
      nullCount += 1
    }
  }

  def collectedStatistics = Row(lower, upper, nullCount)
}

private[sql] class ShortColumnStats extends ColumnStats {
  var upper = Short.MinValue
  var lower = Short.MaxValue
  var nullCount = 0

  override def gatherStats(row: Row, ordinal: Int): Unit = {
    if (!row.isNullAt(ordinal)) {
      val value = row.getShort(ordinal)
      if (value > upper) upper = value
      if (value < lower) lower = value
    } else {
      nullCount += 1
    }
  }

  def collectedStatistics = Row(lower, upper, nullCount)
}

private[sql] class LongColumnStats extends ColumnStats {
  var upper = Long.MinValue
  var lower = Long.MaxValue
  var nullCount = 0

  override def gatherStats(row: Row, ordinal: Int): Unit = {
    if (!row.isNullAt(ordinal)) {
      val value = row.getLong(ordinal)
      if (value > upper) upper = value
      if (value < lower) lower = value
    } else {
      nullCount += 1
    }
  }

  def collectedStatistics = Row(lower, upper, nullCount)
}

private[sql] class DoubleColumnStats extends ColumnStats {
  var upper = Double.MinValue
  var lower = Double.MaxValue
  var nullCount = 0

  override def gatherStats(row: Row, ordinal: Int): Unit = {
    if (!row.isNullAt(ordinal)) {
      val value = row.getDouble(ordinal)
      if (value > upper) upper = value
      if (value < lower) lower = value
    } else {
      nullCount += 1
    }
  }

  def collectedStatistics = Row(lower, upper, nullCount)
}

private[sql] class FloatColumnStats extends ColumnStats {
  var upper = Float.MinValue
  var lower = Float.MaxValue
  var nullCount = 0

  override def gatherStats(row: Row, ordinal: Int): Unit = {
    if (!row.isNullAt(ordinal)) {
      val value = row.getFloat(ordinal)
      if (value > upper) upper = value
      if (value < lower) lower = value
    } else {
      nullCount += 1
    }
  }

  def collectedStatistics = Row(lower, upper, nullCount)
}

private[sql] class IntColumnStats extends ColumnStats {
  var upper = Int.MinValue
  var lower = Int.MaxValue
  var nullCount = 0

  override def gatherStats(row: Row, ordinal: Int): Unit = {
    if (!row.isNullAt(ordinal)) {
      val value = row.getInt(ordinal)
      if (value > upper) upper = value
      if (value < lower) lower = value
    } else {
      nullCount += 1
    }
  }

  def collectedStatistics = Row(lower, upper, nullCount)
}

private[sql] class StringColumnStats extends ColumnStats {
  var upper: String = null
  var lower: String = null
  var nullCount = 0

  override def gatherStats(row: Row, ordinal: Int): Unit = {
    if (!row.isNullAt(ordinal)) {
      val value = row.getString(ordinal)
      if (upper == null || value.compareTo(upper) > 0) upper = value
      if (lower == null || value.compareTo(lower) < 0) lower = value
    } else {
      nullCount += 1
    }
  }

  def collectedStatistics = Row(lower, upper, nullCount)
}

private[sql] class DateColumnStats extends ColumnStats {
  var upper: Date = null
  var lower: Date = null
  var nullCount = 0

  override def gatherStats(row: Row, ordinal: Int) {
    if (!row.isNullAt(ordinal)) {
      val value = row(ordinal).asInstanceOf[Date]
      if (upper == null || value.compareTo(upper) > 0) upper = value
      if (lower == null || value.compareTo(lower) < 0) lower = value
    } else {
      nullCount += 1
    }
  }

  def collectedStatistics = Row(lower, upper, nullCount)
}

private[sql] class TimestampColumnStats extends ColumnStats {
  var upper: Timestamp = null
  var lower: Timestamp = null
  var nullCount = 0

  override def gatherStats(row: Row, ordinal: Int): Unit = {
    if (!row.isNullAt(ordinal)) {
      val value = row(ordinal).asInstanceOf[Timestamp]
      if (upper == null || value.compareTo(upper) > 0) upper = value
      if (lower == null || value.compareTo(lower) < 0) lower = value
    } else {
      nullCount += 1
    }
  }

  def collectedStatistics = Row(lower, upper, nullCount)
}
