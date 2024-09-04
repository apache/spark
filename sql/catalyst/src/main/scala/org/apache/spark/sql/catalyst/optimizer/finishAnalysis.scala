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

package org.apache.spark.sql.catalyst.optimizer

import java.time.{Instant, LocalDateTime, ZoneId}

import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.{CurrentUserContext, InternalRow}
import org.apache.spark.sql.catalyst.analysis.{CastSupport, ResolvedInlineTable}
import org.apache.spark.sql.catalyst.analysis.ResolveInlineTables.prepareForEval
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.catalyst.trees.TreePatternBits
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{convertSpecialDate, convertSpecialTimestamp, convertSpecialTimestampNTZ, instantToMicros, localDateTimeToMicros}
import org.apache.spark.sql.catalyst.util.TypeUtils.toSQLExpr
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._


/**
 * Finds all the [[RuntimeReplaceable]] expressions that are unevaluable and replace them
 * with semantically equivalent expressions that can be evaluated.
 *
 * This is mainly used to provide compatibility with other databases.
 * Few examples are:
 *   we use this to support "left" by replacing it with "substring".
 *   we use this to replace Every and Any with Min and Max respectively.
 */
object ReplaceExpressions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsAnyPattern(RUNTIME_REPLACEABLE)) {
    case p => p.mapExpressions(replace)
  }

  private def replace(e: Expression): Expression = e match {
    case r: RuntimeReplaceable => replace(r.replacement)
    case _ => e.mapChildren(replace)
  }
}

/**
 * Rewrite non correlated exists subquery to use ScalarSubquery
 *   WHERE EXISTS (SELECT A FROM TABLE B WHERE COL1 > 10)
 * will be rewritten to
 *   WHERE (SELECT 1 FROM (SELECT A FROM TABLE B WHERE COL1 > 10) LIMIT 1) IS NOT NULL
 */
object RewriteNonCorrelatedExists extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressionsWithPruning(
    _.containsPattern(EXISTS_SUBQUERY)) {
    case exists: Exists if exists.children.isEmpty =>
      IsNotNull(
        ScalarSubquery(
          plan = Limit(Literal(1), Project(Seq(Alias(Literal(1), "col")()), exists.plan)),
          exprId = exists.exprId,
          hint = exists.hint))
  }
}

/**
 * Computes expressions in inline tables. This rule is supposed to be called at the very end
 * of the analysis phase, given that all the expressions need to be fully resolved/replaced
 * at this point.
 */
object EvalInlineTables extends Rule[LogicalPlan] with CastSupport {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformDownWithSubqueriesAndPruning(_.containsPattern(INLINE_TABLE_EVAL)) {
      case table: ResolvedInlineTable => eval(table)
    }
  }

    def eval(table: ResolvedInlineTable): LocalRelation = {
      val newRows: Seq[InternalRow] =
        table.rows.map { row => InternalRow.fromSeq(row.map { e =>
          try {
            prepareForEval(e).eval()
          } catch {
            case NonFatal(ex) =>
              table.failAnalysis(
                errorClass = "INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION",
                messageParameters = Map("sqlExpr" -> toSQLExpr(e)),
                cause = ex)
          }})
        }

      LocalRelation(table.output, newRows)
    }
}

/**
 * Computes the current date and time to make sure we return the same result in a single query.
 */
object ComputeCurrentTime extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    val instant = Instant.now()
    val currentTimestampMicros = instantToMicros(instant)
    val currentTime = Literal.create(currentTimestampMicros, TimestampType)
    val timezone = Literal.create(conf.sessionLocalTimeZone, StringType)
    val currentDates = collection.mutable.HashMap.empty[ZoneId, Literal]
    val localTimestamps = collection.mutable.HashMap.empty[ZoneId, Literal]

    def transformCondition(treePatternbits: TreePatternBits): Boolean = {
      treePatternbits.containsPattern(CURRENT_LIKE)
    }

    plan.transformDownWithSubqueriesAndPruning(transformCondition) {
      case subQuery =>
        subQuery.transformAllExpressionsWithPruning(transformCondition) {
          case cd: CurrentDate =>
            currentDates.getOrElseUpdate(cd.zoneId, {
              Literal.create(
                DateTimeUtils.microsToDays(currentTimestampMicros, cd.zoneId), DateType)
            })
          case CurrentTimestamp() | Now() => currentTime
          case CurrentTimeZone() => timezone
          case localTimestamp: LocalTimestamp =>
            localTimestamps.getOrElseUpdate(localTimestamp.zoneId, {
              val asDateTime = LocalDateTime.ofInstant(instant, localTimestamp.zoneId)
              Literal.create(localDateTimeToMicros(asDateTime), TimestampNTZType)
            })
        }
    }
  }
}

/**
 * Replaces the expression of CurrentDatabase with the current database name.
 * Replaces the expression of CurrentCatalog with the current catalog name.
 */
case class ReplaceCurrentLike(catalogManager: CatalogManager) extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
    val currentNamespace = catalogManager.currentNamespace.quoted
    val currentCatalog = catalogManager.currentCatalog.name()
    val currentUser = CurrentUserContext.getCurrentUser

    plan.transformAllExpressionsWithPruning(_.containsPattern(CURRENT_LIKE)) {
      case CurrentDatabase() =>
        Literal.create(currentNamespace, SQLConf.get.defaultStringType)
      case CurrentCatalog() =>
        Literal.create(currentCatalog, SQLConf.get.defaultStringType)
      case CurrentUser() =>
        Literal.create(currentUser, SQLConf.get.defaultStringType)
    }
  }
}

/**
 * Replaces casts of special datetime strings by its date/timestamp values
 * if the input strings are foldable.
 */
object SpecialDatetimeValues extends Rule[LogicalPlan] {
  private val conv = Map[DataType, (String, java.time.ZoneId) => Option[Any]](
    DateType -> convertSpecialDate,
    TimestampType -> convertSpecialTimestamp,
    TimestampNTZType -> convertSpecialTimestampNTZ)
  def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformAllExpressionsWithPruning(_.containsPattern(CAST)) {
      case cast @ Cast(e, dt @ (DateType | TimestampType | TimestampNTZType), _, _)
        if e.foldable && e.dataType == StringType =>
        Option(e.eval())
          .flatMap(s => conv(dt)(s.toString, cast.zoneId))
          .map(Literal(_, dt))
          .getOrElse(cast)
    }
  }
}

object ReplaceTranspose extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan.transform {
    case t @ Transpose(output, data) =>
      LocalRelation(output, data)
  }
}
