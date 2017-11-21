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
package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.{AnalysisException}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringType, TimestampType}

/**
 * Apply a correction to data loaded from, or saved to, tables that have a configured time zone, so
 * that timestamps can be read like TIMESTAMP WITHOUT TIMEZONE.  This gives correct behavior if you
 * process data with machines in different timezones, or if you access the data from multiple SQL
 * engines.
 */
case class AdjustTimestamps(conf: SQLConf) extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case insert: InsertIntoHadoopFsRelationCommand =>
      val adjusted = adjustTimestampsForWrite(insert.query, insert.catalogTable, insert.options)
      insert.copy(query = adjusted)

    case insert @ InsertIntoTable(table: HiveTableRelation, _, query, _, _) =>
      val adjusted = adjustTimestampsForWrite(insert.query, Some(table.tableMeta), Map())
      insert.copy(query = adjusted)

    case other =>
      convertInputs(plan)
  }

  private def convertInputs(plan: LogicalPlan): LogicalPlan = plan match {
    case adjusted @ Project(exprs, _) if hasCorrection(exprs) =>
      adjusted

    case lr @ LogicalRelation(fsRelation: HadoopFsRelation, _, _, _) =>
      adjustTimestamps(lr, lr.catalogTable, fsRelation.options, true)

    case hr @ HiveTableRelation(table, _, _) =>
      adjustTimestamps(hr, Some(table), Map(), true)

    case other =>
      other.mapChildren { originalPlan =>
        convertInputs(originalPlan)
      }
  }

  private def adjustTimestamps(
      plan: LogicalPlan,
      table: Option[CatalogTable],
      options: Map[String, String],
      reading: Boolean): LogicalPlan = {
    val tableTz = table.flatMap(_.properties.get(DateTimeUtils.TIMEZONE_PROPERTY))
      .orElse(options.get(DateTimeUtils.TIMEZONE_PROPERTY))

    tableTz.map { tz =>
      val sessionTz = conf.sessionLocalTimeZone
      val toTz = if (reading) sessionTz else tz
      val fromTz = if (reading) tz else sessionTz
      logDebug(
        s"table tz = $tz; converting ${if (reading) "to" else "from"} session tz = $sessionTz\n")

      var hasTimestamp = false
      val adjusted = plan.expressions.map {
        case e: NamedExpression if e.dataType == TimestampType =>
          val adjustment = TimestampTimezoneCorrection(e.toAttribute,
            Literal.create(fromTz, StringType), Literal.create(toTz, StringType))
          hasTimestamp = true
          Alias(adjustment, e.name)()

        case other: NamedExpression =>
          other.toAttribute

        case unnamed =>
          throw new AnalysisException(s"Unexpected expr: $unnamed")
      }.toList

      if (hasTimestamp) Project(adjusted, plan) else plan
    }.getOrElse(plan)
  }

  private def adjustTimestampsForWrite(
      query: LogicalPlan,
      table: Option[CatalogTable],
      options: Map[String, String]): LogicalPlan = query match {
    case unadjusted if !hasOutputCorrection(unadjusted.expressions) =>
      // The query might be reading from a table with a configured time zone; this makes sure we
      // apply the correct conversions for that data.
      val fixedInputs = convertInputs(unadjusted)
      adjustTimestamps(fixedInputs, table, options, false)

    case _ =>
      query
  }

  private def hasCorrection(exprs: Seq[Expression]): Boolean = {
    exprs.exists { expr =>
      expr.isInstanceOf[TimestampTimezoneCorrection] || hasCorrection(expr.children)
    }
  }

  private def hasOutputCorrection(exprs: Seq[Expression]): Boolean = {
    // Output correction is any TimestampTimezoneCorrection that converts from the current
    // session's time zone.
    val sessionTz = conf.sessionLocalTimeZone
    exprs.exists {
      case TimestampTimezoneCorrection(_, from, _) => from.toString() == sessionTz
      case other => hasOutputCorrection(other.children)
    }
  }

}
