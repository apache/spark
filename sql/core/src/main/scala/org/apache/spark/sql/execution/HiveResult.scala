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

package org.apache.spark.sql.execution

import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.util.{DateFormatter, DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.execution.command.{DescribeCommandBase, ExecutedCommandExec, ShowTablesCommand, ShowViewsCommand}
import org.apache.spark.sql.execution.datasources.v2.{DescribeTableExec, ShowTablesExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

/**
 * Runs a query returning the result in Hive compatible form.
 */
object HiveResult {
  /**
   * Returns the result as a hive compatible sequence of strings. This is used in tests and
   * `SparkSQLDriver` for CLI applications.
   */
  def hiveResultString(executedPlan: SparkPlan): Seq[String] = executedPlan match {
    case ExecutedCommandExec(_: DescribeCommandBase) =>
      formatDescribeTableOutput(executedPlan.executeCollectPublic())
    case _: DescribeTableExec =>
      formatDescribeTableOutput(executedPlan.executeCollectPublic())
    // SHOW TABLES in Hive only output table names while our v1 command outputs
    // database, table name, isTemp.
    case command @ ExecutedCommandExec(s: ShowTablesCommand) if !s.isExtended =>
      command.executeCollect().map(_.getString(1))
    // SHOW TABLES in Hive only output table names while our v2 command outputs
    // namespace and table name.
    case command : ShowTablesExec =>
      command.executeCollect().map(_.getString(1))
    // SHOW VIEWS in Hive only outputs view names while our v1 command outputs
    // namespace, viewName, and isTemporary.
    case command @ ExecutedCommandExec(_: ShowViewsCommand) =>
      command.executeCollect().map(_.getString(1))
    case other =>
      val result: Seq[Seq[Any]] = other.executeCollectPublic().map(_.toSeq).toSeq
      // We need the types so we can output struct field names
      val types = executedPlan.output.map(_.dataType)
      // Reformat to match hive tab delimited output.
      result.map(_.zip(types).map(e => toHiveString(e)))
        .map(_.mkString("\t"))
  }

  private def formatDescribeTableOutput(rows: Array[Row]): Seq[String] = {
    rows.map {
      case Row(name: String, dataType: String, comment) =>
        Seq(name, dataType, Option(comment.asInstanceOf[String]).getOrElse(""))
          .map(s => String.format(s"%-20s", s))
          .mkString("\t")
    }
  }

  private def zoneId = DateTimeUtils.getZoneId(SQLConf.get.sessionLocalTimeZone)
  private def dateFormatter = DateFormatter(zoneId)
  private def timestampFormatter = TimestampFormatter.getFractionFormatter(zoneId)

  /** Formats a datum (based on the given data type) and returns the string representation. */
  def toHiveString(a: (Any, DataType), nested: Boolean = false): String = a match {
    case (null, _) => if (nested) "null" else "NULL"
    case (b, BooleanType) => b.toString
    case (d: Date, DateType) => dateFormatter.format(DateTimeUtils.fromJavaDate(d))
    case (ld: LocalDate, DateType) =>
      dateFormatter.format(DateTimeUtils.localDateToDays(ld))
    case (t: Timestamp, TimestampType) =>
      timestampFormatter.format(DateTimeUtils.fromJavaTimestamp(t))
    case (i: Instant, TimestampType) =>
      timestampFormatter.format(DateTimeUtils.instantToMicros(i))
    case (bin: Array[Byte], BinaryType) => new String(bin, StandardCharsets.UTF_8)
    case (decimal: java.math.BigDecimal, DecimalType()) => decimal.toPlainString
    case (n, _: NumericType) => n.toString
    case (s: String, StringType) => if (nested) "\"" + s + "\"" else s
    case (interval: CalendarInterval, CalendarIntervalType) => interval.toString
    case (seq: Seq[_], ArrayType(typ, _)) =>
      seq.map(v => (v, typ)).map(e => toHiveString(e, true)).mkString("[", ",", "]")
    case (m: Map[_, _], MapType(kType, vType, _)) =>
      m.map { case (key, value) =>
        toHiveString((key, kType), true) + ":" + toHiveString((value, vType), true)
      }.toSeq.sorted.mkString("{", ",", "}")
    case (struct: Row, StructType(fields)) =>
      struct.toSeq.zip(fields).map { case (v, t) =>
        s""""${t.name}":${toHiveString((v, t.dataType), true)}"""
      }.mkString("{", ",", "}")
    case (other, _: UserDefinedType[_]) => other.toString
  }
}
