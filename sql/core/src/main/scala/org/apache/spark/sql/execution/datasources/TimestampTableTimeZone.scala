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

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedException
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.{StringType, TimestampType}

/**
 * Apply a correction to data loaded from, or saved to, Parquet, so that it timestamps can be read
 * like TIMESTAMP WITHOUT TIMEZONE.  This gives correct behavior if you process data with
 * machines in different timezones, or if you access the data from multiple SQL engines.
 */
private[sql] case class TimestampTableTimeZone(sparkSession: SparkSession)
    extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = {
    // we can't use transformUp because we want to terminate recursion if there was already
    // timestamp correction, to keep this idempotent.
    plan match {
      case insertIntoHadoopFs: InsertIntoHadoopFsRelationCommand =>
        // The query might be reading from a parquet table which requires a different conversion;
        // this makes sure we apply the correct conversions there.
        val (fixedQuery, _) = readConversion(insertIntoHadoopFs.query)
        writeConversion(insertIntoHadoopFs.copy(query = fixedQuery))

      case other =>
        // recurse into children to see if we're reading data that needs conversion
        val (convertedPlan, _) = readConversion(plan)
        convertedPlan
    }
  }

  private def readConversion(
      plan: LogicalPlan
      ): (LogicalPlan, Map[ExprId, NamedExpression]) = plan match {
    case alreadyConverted@Project(exprs, _) if hasCorrection(exprs) =>
      logDebug(s"not transforming $alreadyConverted because its already converted")
      (alreadyConverted, Map())
    case lr@LogicalRelation(fsRelation: HadoopFsRelation, _, _, _) =>
      val tzOpt = extractTableTz(lr.catalogTable, fsRelation.options)
      tzOpt.map { tableTz =>
        // the table has a timezone set, so after reading the data, apply a conversion

        // SessionTZ will make the time display correctly in SQL queries, but incorrectly if you
        // pull Timestamp objects out (eg. with a dataset.collect())
        val toTz = sparkSession.sessionState.conf.sessionLocalTimeZone
        if (toTz != tableTz) {
          logDebug(s"table tz = $tableTz; converting to current session tz = $toTz")
          // find timestamp columns, and convert their tz
          val (foundTs, modifiedFields, replacements) =
            convertTzForAllTimestamps(lr, tableTz, toTz)
          if (foundTs) {
            (new Project(modifiedFields, lr), replacements)
          } else {
            (lr, Map[ExprId, NamedExpression]())
          }
        } else {
          (lr, Map[ExprId, NamedExpression]())
        }
      }.getOrElse((lr, Map[ExprId, NamedExpression]()))
    case other =>
      // first, process all the children -- this ensures we have the right renames in scope.
      var newReplacements = Map[ExprId, NamedExpression]()
      val fixedPlan = other.mapChildren { originalPlan =>
        val (newPlan, extraReplacements) = readConversion(originalPlan)
        newReplacements ++= extraReplacements
        newPlan
      }
      // now we need to adjust all names to use the new version.
      val fixedExpressions = fixedPlan.mapExpressions { outerExp =>
        val adjustedExp = outerExp.transformUp { case exp: NamedExpression =>
          try {
            newReplacements.get(exp.exprId).getOrElse(exp)
          } catch {
            // UnresolvedAttributes etc. will cause problems later anyway, we just dont' want to
            // expose the error here
            case ue: UnresolvedException[_] => exp
          }
        }
        logDebug(s"adjusted $outerExp to $adjustedExp using $newReplacements")
        adjustedExp
      }
      (fixedExpressions, newReplacements)
  }

  private def hasCorrection(exprs: Seq[NamedExpression]): Boolean = {
    var hasCorrection = false
    exprs.foreach { expr =>
      expr.foreach {
        case _: TimestampTimezoneCorrection =>
          hasCorrection = true
        case other => // no-op
      }
    }
    hasCorrection
  }

  private def insertIntoFsCopy(
      insert: InsertIntoHadoopFsRelationCommand,
      newQuery: LogicalPlan): InsertIntoHadoopFsRelationCommand = {
    insert.copy(query = newQuery)
  }

  private def writeConversion(
      insertIntoHadoopFs: InsertIntoHadoopFsRelationCommand): InsertIntoHadoopFsRelationCommand = {
    val query = insertIntoHadoopFs.query
    val tableTz = extractTableTz(insertIntoHadoopFs.catalogTable, insertIntoHadoopFs.options)
    val internalTz = sparkSession.sessionState.conf.sessionLocalTimeZone
    if (tableTz.isDefined && tableTz != internalTz) {
      val (foundTsFields, modifiedFields, _) =
        convertTzForAllTimestamps(query, internalTz, tableTz.get)
      if (foundTsFields) {
        insertIntoHadoopFs.copy(query = new Project(modifiedFields, query))
      } else {
        insertIntoHadoopFs
      }
    } else {
      insertIntoHadoopFs
    }
  }

  private def extractTableTz(options: Map[String, String]): Option[String] = {
    options.get(TimestampTableTimeZone.TIMEZONE_PROPERTY)
  }

  private def extractTableTz(
      table: Option[CatalogTable],
      options: Map[String, String]): Option[String] = {
    table.flatMap { tbl => extractTableTz(tbl.properties) }.orElse(extractTableTz(options))
  }

  /**
   * Find all timestamp fields in the given relation.  For each one, replace it with an expression
   * that converts the timezone of the timestamp, and assigns an alias to that new expression.
   * (Leave non-timestamp fields alone.)  Also return a map from the original id for the timestamp
   * field, to the new alias of the timezone-corrected expression.
   */
  private def convertTzForAllTimestamps(
      relation: LogicalPlan,
      fromTz: String,
      toTz: String): (Boolean, Seq[NamedExpression], Map[ExprId, NamedExpression]) = {
    val schema = relation.schema
    var foundTs = false
    var replacements = Map[ExprId, NamedExpression]()
    val modifiedFields: Seq[NamedExpression] = schema.map { field =>
      val exp = relation.resolve(Seq(field.name), sparkSession.sessionState.conf.resolver)
        .getOrElse {
          val inputColumns = schema.map(_.name).mkString(", ")
          throw new AnalysisException(
            s"cannot resolve '${field.name}' given input columns: [$inputColumns]")
        }
      if (field.dataType == TimestampType) {
        foundTs = true
        val adjustedTs = Alias(
          TimestampTimezoneCorrection(
            exp,
            Literal.create(fromTz, StringType),
            Literal.create(toTz, StringType)
          ),
          field.name
        )()
        // we also need to rename all occurrences of this field further up in the plan
        // to refer to our new adjusted timestamp, so we pass this replacement up the call stack.
        replacements += exp.exprId -> adjustedTs.toAttribute
        adjustedTs
      } else {
        exp
      }
    }
    (foundTs, modifiedFields, replacements)
  }
}

private[sql] object TimestampTableTimeZone {
  val TIMEZONE_PROPERTY = "table.timezone"
  /**
   * Throw an AnalysisException if we're trying to set an invalid timezone for this table.
   */
  private[sql] def checkTableTz(table: TableIdentifier, properties: Map[String, String]): Unit = {
    checkTableTz(s"in table ${table.toString}", properties)
  }

  /**
   * Throw an AnalysisException if we're trying to set an invalid timezone for this table.
   */
  private[sql] def checkTableTz(dest: String, properties: Map[String, String]): Unit = {
    properties.get(TIMEZONE_PROPERTY).foreach { tz =>
      if (!DateTimeUtils.isValidTimezone(tz)) {
        throw new AnalysisException(s"Cannot set $TIMEZONE_PROPERTY to invalid " +
          s"timezone $tz $dest")
      }
    }
  }
}
