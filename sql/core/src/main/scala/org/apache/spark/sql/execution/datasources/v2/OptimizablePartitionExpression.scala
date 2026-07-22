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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Cast, DateFormatClass, DayOfMonth, Expression, Hour, IsNull, Literal, Month, Or, Substring, TruncDate, TruncTimestamp, UnixTimestamp, Year}
import org.apache.spark.sql.types.{DateType, StringType, TimestampType}

/**
 * Defines rules to convert a data filter to a partition filter for a special generation expression
 * of a partition column.
 *
 * Note:
 * - This may be shared across multiple `SparkSession`s, so implementations should not store any
 *   state (such as expressions) referring to a specific `SparkSession`.
 * - Partition columns may have different behaviors than data columns. For example, writing an empty
 *   string to a partition column would become `null` (SPARK-24438). We need to pay attention to
 *   these slight behavior differences and make sure applying the auto generated partition filters
 *   would still return the same result as if they were not applied.
 */
sealed trait OptimizablePartitionExpression {
  /**
   * Assume we have a partition column `part`, and a data column `col`. Return a partition filter
   * based on `part` for a data filter `col < lit`.
   */
  def lessThan(lit: Literal): Option[Expression] = None

  /**
   * Assume we have a partition column `part`, and a data column `col`. Return a partition filter
   * based on `part` for a data filter `col <= lit`.
   */
  def lessThanOrEqual(lit: Literal): Option[Expression] = None

  /**
   * Assume we have a partition column `part`, and a data column `col`. Return a partition filter
   * based on `part` for a data filter `col = lit`.
   */
  def equalTo(lit: Literal): Option[Expression] = None

  /**
   * Assume we have a partition column `part`, and a data column `col`. Return a partition filter
   * based on `part` for a data filter `col > lit`.
   */
  def greaterThan(lit: Literal): Option[Expression] = None

  /**
   * Assume we have a partition column `part`, and a data column `col`. Return a partition filter
   * based on `part` for a data filter `col >= lit`.
   */
  def greaterThanOrEqual(lit: Literal): Option[Expression] = None

  /**
   * Assume we have a partition column `part`, and a data column `col`. Return a partition filter
   * based on `part` for a data filter `col IS NULL`.
   */
  def isNull(): Option[Expression] = None
}

object OptimizablePartitionExpression {
  /** Provide a convenient method to convert a string to a column expression. */
  implicit class ColumnExpression(val colName: String) extends AnyVal {
    // This will always be a top level column so it is safe to reference it by name directly.
    def toPartCol: Expression = UnresolvedAttribute(Seq(colName))
  }
}

import org.apache.spark.sql.execution.datasources.v2.OptimizablePartitionExpression._

/** The rules for the generation expression `CAST(col AS DATE)`. */
case class DatePartitionExpr(partitionColumn: String) extends OptimizablePartitionExpression {
  override def lessThan(lit: Literal): Option[Expression] = {
    // As the partition column has truncated information, we need to turn "<" to "<=".
    lessThanOrEqual(lit)
  }

  override def lessThanOrEqual(lit: Literal): Option[Expression] = {
    val expr = lit.dataType match {
      case TimestampType => Some(partitionColumn.toPartCol <= Cast(lit, DateType))
      case DateType => Some(partitionColumn.toPartCol <= lit)
      case _ => None
    }
    // to avoid any expression which yields null
    expr.map(e => Or(e, IsNull(e)))
  }

  override def equalTo(lit: Literal): Option[Expression] = {
    val expr = lit.dataType match {
      case TimestampType => Some(partitionColumn.toPartCol === Cast(lit, DateType))
      case DateType => Some(partitionColumn.toPartCol === lit)
      case _ => None
    }
    // to avoid any expression which yields null
    expr.map(e => Or(e, IsNull(e)))
  }

  override def greaterThan(lit: Literal): Option[Expression] = {
    // As the partition column has truncated information, we need to turn ">" to ">=".
    greaterThanOrEqual(lit)
  }

  override def greaterThanOrEqual(lit: Literal): Option[Expression] = {
    val expr = lit.dataType match {
      case TimestampType => Some(partitionColumn.toPartCol >= Cast(lit, DateType))
      case DateType => Some(partitionColumn.toPartCol >= lit)
      case _ => None
    }
    // to avoid any expression which yields null
    expr.map(e => Or(e, IsNull(e)))
  }

  override def isNull(): Option[Expression] = Some(partitionColumn.toPartCol.isNull)
}

/**
 * The rules for the generation expression `YEAR(col)`.
 *
 * @param yearPart the year partition column name.
 */
case class YearPartitionExpr(yearPart: String) extends OptimizablePartitionExpression {

  override def lessThan(lit: Literal): Option[Expression] = {
    // As the partition column has truncated information, we need to turn "<" to "<=".
    lessThanOrEqual(lit)
  }

  override def lessThanOrEqual(lit: Literal): Option[Expression] = {
    val expr = lit.dataType match {
      case TimestampType | DateType => Some(yearPart.toPartCol <= Year(lit))
      case _ => None
    }
    // to avoid any expression which yields null
    expr.map(e => Or(e, IsNull(e)))
  }

  override def equalTo(lit: Literal): Option[Expression] = {
    val expr = lit.dataType match {
      case TimestampType | DateType => Some(yearPart.toPartCol === Year(lit))
      case _ => None
    }
    // to avoid any expression which yields null
    expr.map(e => Or(e, IsNull(e)))
  }

  override def greaterThan(lit: Literal): Option[Expression] = {
    // As the partition column has truncated information, we need to turn ">" to ">=".
    greaterThanOrEqual(lit)
  }

  override def greaterThanOrEqual(lit: Literal): Option[Expression] = {
    val expr = lit.dataType match {
      case TimestampType | DateType => Some(yearPart.toPartCol >= Year(lit))
      case _ => None
    }
    // to avoid any expression which yields null
    expr.map(e => Or(e, IsNull(e)))
  }

  override def isNull(): Option[Expression] = Some(yearPart.toPartCol.isNull)
}

/**
 * This is a placeholder to catch `month(col)` so that we can merge [[YearPartitionExpr]] and
 * [[MonthPartitionExpr]] to [[YearMonthDayPartitionExpr]].
 *
 * @param monthPart the month partition column name.
 */
case class MonthPartitionExpr(monthPart: String) extends OptimizablePartitionExpression

/**
 * This is a placeholder to catch `day(col)` so that we can merge [[YearPartitionExpr]],
 * [[MonthPartitionExpr]] and [[DayPartitionExpr]] to [[YearMonthDayPartitionExpr]].
 *
 * @param dayPart the day partition column name.
 */
case class DayPartitionExpr(dayPart: String) extends OptimizablePartitionExpression

/**
 * This is a placeholder to catch `hour(col)` so that we can merge [[YearPartitionExpr]],
 * [[MonthPartitionExpr]], [[DayPartitionExpr]] and [[HourPartitionExpr]] to
 * [[YearMonthDayHourPartitionExpr]].
 *
 * @param hourPart the hour partition column name.
 */
case class HourPartitionExpr(hourPart: String) extends OptimizablePartitionExpression

/**
 * Optimize the case that two partition columns use YEAR and MONTH on the same column, such
 * as `YEAR(eventTime)` and `MONTH(eventTime)`.
 *
 * @param yearPart the year partition column name
 * @param monthPart the month partition column name
 */
case class YearMonthPartitionExpr(
    yearPart: String,
    monthPart: String) extends OptimizablePartitionExpression {

  override def lessThan(lit: Literal): Option[Expression] = {
    // As the partition column has truncated information, we need to turn "<" to "<=".
    lessThanOrEqual(lit)
  }

  override def lessThanOrEqual(lit: Literal): Option[Expression] = {
    lit.dataType match {
      case TimestampType =>
        Some(
          (yearPart.toPartCol < Year(lit)) ||
            (yearPart.toPartCol === Year(lit) && monthPart.toPartCol <= Month(lit)))
      case _ => None
    }
  }

  override def equalTo(lit: Literal): Option[Expression] = {
    lit.dataType match {
      case TimestampType =>
        Some(yearPart.toPartCol === Year(lit) && monthPart.toPartCol === Month(lit))
      case _ => None
    }
  }

  override def greaterThan(lit: Literal): Option[Expression] = {
    // As the partition column has truncated information, we need to turn ">" to ">=".
    greaterThanOrEqual(lit)
  }

  override def greaterThanOrEqual(lit: Literal): Option[Expression] = {
    lit.dataType match {
      case TimestampType =>
        Some(
          (yearPart.toPartCol > Year(lit)) ||
            (yearPart.toPartCol === Year(lit) && monthPart.toPartCol >= Month(lit)))
      case _ => None
    }
  }

  override def isNull(): Option[Expression] = {
    // `yearPart` and `monthPart` are derived columns, so they must be `null` when the input column
    // is `null`.
    Some(yearPart.toPartCol.isNull && monthPart.toPartCol.isNull)
  }
}

/**
 * Optimize the case that three partition columns use YEAR, MONTH and DAY on the same column,
 * such as `YEAR(eventTime)`, `MONTH(eventTime)` and `DAY(eventTime)`.
 *
 * @param yearPart the year partition column name
 * @param monthPart the month partition column name
 * @param dayPart the day partition column name
 */
case class YearMonthDayPartitionExpr(
    yearPart: String,
    monthPart: String,
    dayPart: String) extends OptimizablePartitionExpression {
  override def lessThan(lit: Literal): Option[Expression] = {
    // As the partition column has truncated information, we need to turn "<" to "<=".
    lessThanOrEqual(lit)
  }

  override def lessThanOrEqual(lit: Literal): Option[Expression] = {
    lit.dataType match {
      case TimestampType =>
        Some(
          (yearPart.toPartCol < Year(lit)) ||
            (yearPart.toPartCol === Year(lit) && monthPart.toPartCol < Month(lit)) ||
            (yearPart.toPartCol === Year(lit) && monthPart.toPartCol === Month(lit) &&
              dayPart.toPartCol <= DayOfMonth(lit)))
      case _ => None
    }
  }

  override def equalTo(lit: Literal): Option[Expression] = {
    lit.dataType match {
      case TimestampType =>
        Some(
          yearPart.toPartCol === Year(lit) && monthPart.toPartCol === Month(lit) &&
            dayPart.toPartCol === DayOfMonth(lit))
      case _ => None
    }
  }

  override def greaterThan(lit: Literal): Option[Expression] = {
    // As the partition column has truncated information, we need to turn ">" to ">=".
    greaterThanOrEqual(lit)
  }

  override def greaterThanOrEqual(lit: Literal): Option[Expression] = {
    lit.dataType match {
      case TimestampType =>
        Some(
          (yearPart.toPartCol > Year(lit)) ||
            (yearPart.toPartCol === Year(lit) && monthPart.toPartCol > Month(lit)) ||
            (yearPart.toPartCol === Year(lit) && monthPart.toPartCol === Month(lit) &&
              dayPart.toPartCol >= DayOfMonth(lit)))
      case _ => None
    }
  }

  override def isNull(): Option[Expression] = {
    // `yearPart`, `monthPart` and `dayPart` are derived columns, so they must be `null` when the
    // input column is `null`.
    Some(yearPart.toPartCol.isNull && monthPart.toPartCol.isNull && dayPart.toPartCol.isNull)
  }
}

/**
 * Optimize the case that four partition columns use YEAR, MONTH, DAY and HOUR on the same
 * column, such as `YEAR(eventTime)`, `MONTH(eventTime)`, `DAY(eventTime)`, `HOUR(eventTime)`.
 *
 * @param yearPart the year partition column name
 * @param monthPart the month partition column name
 * @param dayPart the day partition column name
 * @param hourPart the hour partition column name
 */
case class YearMonthDayHourPartitionExpr(
    yearPart: String,
    monthPart: String,
    dayPart: String,
    hourPart: String) extends OptimizablePartitionExpression {
  override def lessThan(lit: Literal): Option[Expression] = {
    // As the partition column has truncated information, we need to turn "<" to "<=".
    lessThanOrEqual(lit)
  }

  override def lessThanOrEqual(lit: Literal): Option[Expression] = {
    lit.dataType match {
      case TimestampType =>
        Some(
          (yearPart.toPartCol < Year(lit)) ||
            (yearPart.toPartCol === Year(lit) && monthPart.toPartCol < Month(lit)) ||
            (yearPart.toPartCol === Year(lit) && monthPart.toPartCol === Month(lit) &&
              dayPart.toPartCol < DayOfMonth(lit)) ||
            (yearPart.toPartCol === Year(lit) && monthPart.toPartCol === Month(lit) &&
              dayPart.toPartCol === DayOfMonth(lit) && hourPart.toPartCol <= Hour(lit)))
      case _ => None
    }
  }

  override def equalTo(lit: Literal): Option[Expression] = {
    lit.dataType match {
      case TimestampType =>
        Some(
          yearPart.toPartCol === Year(lit) && monthPart.toPartCol === Month(lit) &&
            dayPart.toPartCol === DayOfMonth(lit) && hourPart.toPartCol === Hour(lit))
      case _ => None
    }
  }

  override def greaterThan(lit: Literal): Option[Expression] = {
    // As the partition column has truncated information, we need to turn ">" to ">=".
    greaterThanOrEqual(lit)
  }

  override def greaterThanOrEqual(lit: Literal): Option[Expression] = {
    lit.dataType match {
      case TimestampType =>
        Some(
          (yearPart.toPartCol > Year(lit)) ||
            (yearPart.toPartCol === Year(lit) && monthPart.toPartCol > Month(lit)) ||
            (yearPart.toPartCol === Year(lit) && monthPart.toPartCol === Month(lit) &&
              dayPart.toPartCol > DayOfMonth(lit)) ||
            (yearPart.toPartCol === Year(lit) && monthPart.toPartCol === Month(lit) &&
              dayPart.toPartCol === DayOfMonth(lit) && hourPart.toPartCol >= Hour(lit)))
      case _ => None
    }
  }

  override def isNull(): Option[Expression] = {
    // `yearPart`, `monthPart`, `dayPart` and `hourPart` are derived columns, so they must be `null`
    // when the input column is `null`.
    Some(yearPart.toPartCol.isNull && monthPart.toPartCol.isNull &&
      dayPart.toPartCol.isNull && hourPart.toPartCol.isNull)
  }
}

/**
 * The rules for the generation expression `SUBSTRING(col, pos, len)`. Note:
 * - Writing an empty string to a partition column would become `null` (SPARK-24438) so generated
 *   partition filters always pick up the `null` partition for safety.
 * - When `pos` is 0 or 1, we also support optimizations for comparison operators. Otherwise, we
 *   only support optimizations for EqualTo.
 *
 * @param partitionColumn the partition column name using SUBSTRING in its generation expression.
 * @param substringPos the `pos` parameter of SUBSTRING in the generation expression.
 * @param substringLen the `len` parameter of SUBSTRING in the generation expression.
 */
case class SubstringPartitionExpr(
    partitionColumn: String,
    substringPos: Int,
    substringLen: Int) extends OptimizablePartitionExpression {

  override def lessThan(lit: Literal): Option[Expression] = {
    // As the partition column has truncated information, we need to turn "<" to "<=".
    lessThanOrEqual(lit)
  }

  override def lessThanOrEqual(lit: Literal): Option[Expression] = {
    // Both `pos == 0` and `pos == 1` start from the first char. See UTF8String.substringSQL.
    if (substringPos == 0 || substringPos == 1) {
      lit.dataType match {
        case StringType =>
          Some(
            partitionColumn.toPartCol.isNull ||
              partitionColumn.toPartCol <= Substring(lit, substringPos, substringLen))
        case _ => None
      }
    } else {
      None
    }
  }

  override def equalTo(lit: Literal): Option[Expression] = {
    lit.dataType match {
      case StringType =>
        Some(
          partitionColumn.toPartCol.isNull ||
            partitionColumn.toPartCol === Substring(lit, substringPos, substringLen))
      case _ => None
    }
  }

  override def greaterThan(lit: Literal): Option[Expression] = {
    // As the partition column has truncated information, we need to turn ">" to ">=".
    greaterThanOrEqual(lit)
  }

  override def greaterThanOrEqual(lit: Literal): Option[Expression] = {
    // Both `pos == 0` and `pos == 1` start from the first char. See UTF8String.substringSQL.
    if (substringPos == 0 || substringPos == 1) {
      lit.dataType match {
        case StringType =>
          Some(
            partitionColumn.toPartCol.isNull ||
              partitionColumn.toPartCol >= Substring(lit, substringPos, substringLen))
        case _ => None
      }
    } else {
      None
    }
  }

  override def isNull(): Option[Expression] = Some(partitionColumn.toPartCol.isNull)
}

/**
 * The rules for the generation expression `DATE_FORMAT(col, format)`, such as
 * `DATE_FORMAT(timestamp, 'yyyy-MM')` and `DATE_FORMAT(timestamp, 'yyyy-MM-dd-HH')`.
 *
 * @param partitionColumn the partition column name using DATE_FORMAT in its generation expression.
 * @param format the `format` parameter of DATE_FORMAT in the generation expression.
 *
 *            unix_timestamp('12345-12', 'yyyy-MM') | unix_timestamp('+12345-12', 'yyyy-MM')
 * EXCEPTION               fail                     |           327432240000
 * CORRECTED               null                     |           327432240000
 * LEGACY               327432240000                |               null
 */
case class DateFormatPartitionExpr(
    partitionColumn: String, format: String) extends OptimizablePartitionExpression {

  private val partitionColumnUnixTimestamp =
    UnixTimestamp(partitionColumn.toPartCol, format, failOnError = false)

  private def litUnixTimestamp(lit: Literal): UnixTimestamp =
    UnixTimestamp(DateFormatClass(lit, format), format, failOnError = false)

  override def lessThan(lit: Literal): Option[Expression] = {
    // As the partition column has truncated information, we need to turn "<" to "<=".
    // timestamp + date are truncated to yyyy-MM
    // timestamp are truncated to yyyy-MM-dd-HH
    lessThanOrEqual(lit)
  }

  override def lessThanOrEqual(lit: Literal): Option[Expression] = {
    val expr = lit.dataType match {
      case TimestampType | DateType =>
        Some(partitionColumnUnixTimestamp <= litUnixTimestamp(lit))
      case _ => None
    }
    // when write and read timeParserPolicy-s are different, UnixTimestamp will yield null
    // thus e would be null if either of two operands is null, we should not drop the data
    expr.map(e => Or(e, IsNull(e)))
  }

  override def equalTo(lit: Literal): Option[Expression] = {
    val expr = lit.dataType match {
      case TimestampType | DateType =>
        Some(partitionColumnUnixTimestamp === litUnixTimestamp(lit))
      case _ => None
    }
    // when write and read timeParserPolicy-s are different, UnixTimestamp will yield null
    // thus e would be null if either of two operands is null, we should not drop the data
    expr.map(e => Or(e, IsNull(e)))
  }

  override def greaterThan(lit: Literal): Option[Expression] = {
    // As the partition column has truncated information, we need to turn ">" to ">=".
    // timestamp + date are truncated to yyyy-MM
    // timestamp are truncated to yyyy-MM-dd-HH
    greaterThanOrEqual(lit)
  }

  override def greaterThanOrEqual(lit: Literal): Option[Expression] = {
    val expr = lit.dataType match {
      case TimestampType | DateType =>
        Some(partitionColumnUnixTimestamp >= litUnixTimestamp(lit))
      case _ => None
    }
    // when write and read timeParserPolicy-s are different, UnixTimestamp will yield null
    // thus e would be null if either of two operands is null, we should not drop the data
    expr.map(e => Or(e, IsNull(e)))
  }

  override def isNull(): Option[Expression] = Some(partitionColumn.toPartCol.isNull)
}

/** The rules for the generation expression `date_trunc(field, col)`. */
case class TimestampTruncPartitionExpr(format: String, partitionColumn: String)
  extends OptimizablePartitionExpression {
  override def lessThan(lit: Literal): Option[Expression] = {
    // As the partition column has truncated information, we need to turn "<" to "<=".
    lessThanOrEqual(lit)
  }

  override def lessThanOrEqual(lit: Literal): Option[Expression] = {
    val expr = lit.dataType match {
      case TimestampType => Some(partitionColumn.toPartCol <= TruncTimestamp(format, lit))
      case DateType =>
        Some(partitionColumn.toPartCol <= TruncTimestamp(format, Cast(lit, TimestampType)))
      case _ => None
    }
    // to avoid any expression which yields null
    expr.map(e => Or(e, IsNull(e)))
  }

  override def equalTo(lit: Literal): Option[Expression] = {
    val expr = lit.dataType match {
      case TimestampType => Some(partitionColumn.toPartCol === TruncTimestamp(format, lit))
      case DateType =>
        Some(partitionColumn.toPartCol === TruncTimestamp(format, Cast(lit, TimestampType)))
      case _ => None
    }
    // to avoid any expression which yields null
    expr.map(e => Or(e, IsNull(e)))
  }

  override def greaterThan(lit: Literal): Option[Expression] = {
    // As the partition column has truncated information, we need to turn ">" to ">=".
    greaterThanOrEqual(lit)
  }

  override def greaterThanOrEqual(lit: Literal): Option[Expression] = {
    val expr = lit.dataType match {
      case TimestampType => Some(partitionColumn.toPartCol >= TruncTimestamp(format, lit))
      case DateType =>
        Some(partitionColumn.toPartCol >= TruncTimestamp(format, Cast(lit, TimestampType)))
      case _ => None
    }
    // to avoid any expression which yields null
    expr.map(e => Or(e, IsNull(e)))
  }

  override def isNull(): Option[Expression] = Some(partitionColumn.toPartCol.isNull)
}

/**
 * The rules for the identity generation expression, used for partitioning on a nested column.
 * Note:
 * - Writing an empty string to a partition column would become `null` (SPARK-24438) so generated
 *   partition filters always pick up the `null` partition for safety.
 *
 * @param partitionColumn the partition column name used in the generation expression.
 */
case class IdentityPartitionExpr(partitionColumn: String)
    extends OptimizablePartitionExpression {

  override def lessThan(lit: Literal): Option[Expression] = {
    Some(partitionColumn.toPartCol.isNull || partitionColumn.toPartCol < lit)
  }

  override def lessThanOrEqual(lit: Literal): Option[Expression] = {
    Some(partitionColumn.toPartCol.isNull || partitionColumn.toPartCol <= lit)
  }

  override def equalTo(lit: Literal): Option[Expression] = {
    Some(partitionColumn.toPartCol.isNull || partitionColumn.toPartCol === lit)
  }

  override def greaterThan(lit: Literal): Option[Expression] = {
    Some(partitionColumn.toPartCol.isNull || partitionColumn.toPartCol > lit)
  }

  override def greaterThanOrEqual(lit: Literal): Option[Expression] = {
    Some(partitionColumn.toPartCol.isNull || partitionColumn.toPartCol >= lit)
  }

  override def isNull(): Option[Expression] = Some(partitionColumn.toPartCol.isNull)
}

/**
 * The rules for generation expressions that use the function `trunc(col, format)` such as
 * `trunc(timestamp, 'year')`, `trunc(date, 'week')` and `trunc(timestampStr, 'hour')`.
 *
 * @param partitionColumn partition column using trunc function in the generation expression
 * @param format the format that specifies the unit of truncation applied to the partitionColumn
 */
case class TruncDatePartitionExpr(partitionColumn: String, format: String)
  extends OptimizablePartitionExpression {

  override def lessThan(lit: Literal): Option[Expression] = {
    lessThanOrEqual(lit)
  }

  override def lessThanOrEqual(lit: Literal): Option[Expression] = {
    val expr = lit.dataType match {
      case TimestampType | DateType =>
        Some(partitionColumn.toPartCol <= TruncDate(lit, Literal(format)))
      case _ => None
    }
    expr.map(e => Or(e, IsNull(e)))
  }

  override def equalTo(lit: Literal): Option[Expression] = {
    val expr = lit.dataType match {
      case TimestampType | DateType | StringType =>
        Some(partitionColumn.toPartCol === TruncDate(lit, Literal(format)))
      case _ => None
    }
    expr.map(e => Or(e, IsNull(e)))
  }

  override def greaterThan(lit: Literal): Option[Expression] = {
    greaterThanOrEqual(lit)
  }

  override def greaterThanOrEqual(lit: Literal): Option[Expression] = {
    val expr = lit.dataType match {
      case TimestampType | DateType =>
        Some(partitionColumn.toPartCol >= TruncDate(lit, Literal(format)))
      case _ => None
    }
    expr.map(e => Or(e, IsNull(e)))
  }

  override def isNull(): Option[Expression] = Some(partitionColumn.toPartCol.isNull)
}
