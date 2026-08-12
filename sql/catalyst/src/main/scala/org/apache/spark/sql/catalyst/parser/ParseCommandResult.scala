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

package org.apache.spark.sql.catalyst.parser

import scala.collection.mutable
import scala.util.control.NonFatal

import org.json4s._
import org.json4s.jackson.JsonMethods.{compact, parse => parseJson, render}

import org.apache.spark.{ErrorMessageFormat, SparkThrowable, SparkThrowableHelper}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._

/**
 * Parses a SQL statement string and returns a compact JSON description of the
 * unresolved plan (parse-only; no catalog resolution).
 *
 * On success the JSON includes statement classification (ISO/IEC 9075-2:2023
 * Table 39), table/function references, select-list items, and parameter
 * markers. On parse failure it returns `parse_success: false` with a nested
 * STANDARD-format error object and does not throw.
 */
object ParseCommandResult {

  private val parser: ThreadLocal[CatalystSqlParser] =
    ThreadLocal.withInitial(() => new CatalystSqlParser())

  /** Parse `sql` and render the JSON result string. Never throws for bad SQL. */
  def fromSql(sql: String): String = {
    try {
      val plan = parser.get().parsePlan(sql)
      fromPlan(plan)
    } catch {
      case e: ParseException =>
        errorJson(e)
      case e: SparkThrowable with Throwable =>
        errorJson(e)
      case NonFatal(e) =>
        // Unexpected failures still must not fail a batch row.
        compact(render(JObject(
          "parse_success" -> JBool(false),
          "error" -> JObject(
            "errorClass" -> JString("LEGACY"),
            "messageParameters" -> JObject(
              "message" -> JString(Option(e.getMessage).getOrElse(e.toString))
            )
          )
        )))
    }
  }

  /** Build success JSON from an already-parsed unresolved plan. */
  def fromPlan(plan: LogicalPlan): String = {
    val classification = SqlStatementCodes.classify(plan)
    val fields = mutable.ListBuffer.empty[JField]
    fields += "parse_success" -> JBool(true)
    fields += "statement_identifier" -> JString(classification.statementIdentifier)
    fields += "statement_code" -> JInt(classification.statementCode)
    fields += "statement_type" -> JString(classification.statementType)
    fields += "statement_class" -> JString(classification.statementClass)
    if (classification.asSubquery) {
      fields += "as_subquery" -> JBool(true)
    }
    fields += "table_references" -> JArray(
      collectTableReferences(plan).map(partsToJArray).toList)
    fields += "function_references" -> JArray(
      collectFunctionReferences(plan).map(partsToJArray).toList)
    fields += "select_list" -> JArray(collectSelectList(plan).toList)
    fields += "parameter_markers" -> parameterMarkersJson(plan)
    compact(render(JObject(fields.toList)))
  }

  private def errorJson(e: SparkThrowable with Throwable): String = {
    val errorObj = parseJson(
      SparkThrowableHelper.getMessage(e, ErrorMessageFormat.STANDARD))
    compact(render(JObject(
      "parse_success" -> JBool(false),
      "error" -> errorObj
    )))
  }

  private def partsToJArray(parts: Seq[String]): JArray =
    JArray(parts.map(JString).toList)

  /**
   * Deep plan walk covering tree slots that standard `collect` /
   * `collectWithSubqueries` miss:
   *   - [[UnresolvedWith]] CTE definitions (`innerChildren`, not `children`)
   *   - [[InsertIntoStatement]].table (non-child plan slot)
   * Nested expression subqueries are still covered by `foreachWithSubqueries`.
   */
  private def foreachPlanDeep(plan: LogicalPlan)(f: LogicalPlan => Unit): Unit = {
    plan.foreachWithSubqueries { p =>
      f(p)
      p match {
        case w: UnresolvedWith =>
          w.cteRelations.foreach { case (_, ctePlan, _) =>
            foreachPlanDeep(ctePlan)(f)
          }
        case InsertIntoStatement(table, _, _, _, _, _, _, _, _) =>
          foreachPlanDeep(table)(f)
        case _ =>
      }
    }
  }

  /**
   * Collect multipart table/view identifiers as written in the SQL.
   * Deduplicates while preserving first-seen order.
   */
  def collectTableReferences(plan: LogicalPlan): Seq[Seq[String]] = {
    val seen = mutable.LinkedHashSet.empty[Seq[String]]
    def add(parts: Seq[String]): Unit = {
      if (parts.nonEmpty) seen += parts
    }
    foreachPlanDeep(plan) {
      case u: UnresolvedRelation => add(u.multipartIdentifier)
      case u: UnresolvedTable => add(u.multipartIdentifier)
      case u: UnresolvedView => add(u.multipartIdentifier)
      case u: UnresolvedTableOrView => add(u.multipartIdentifier)
      case u: UnresolvedIdentifier => add(u.nameParts)
      case _ =>
    }
    seen.toSeq
  }

  /** Collect multipart function names, including table-valued functions. */
  def collectFunctionReferences(plan: LogicalPlan): Seq[Seq[String]] = {
    val seen = mutable.LinkedHashSet.empty[Seq[String]]
    def add(parts: Seq[String]): Unit = {
      if (parts.nonEmpty) seen += parts
    }
    def collectInExpression(e: Expression): Unit = e.foreach {
      case f: UnresolvedFunction => add(f.nameParts)
      case _ =>
    }
    foreachPlanDeep(plan) { p =>
      p.expressions.foreach(collectInExpression)
      p match {
        case u: UnresolvedTableValuedFunction => add(u.name)
        case _ =>
      }
    }
    seen.toSeq
  }

  /**
   * Collect the primary select list as `{name, expression}` objects.
   * Empty for non-query statements without a projected query body.
   */
  def collectSelectList(plan: LogicalPlan): Seq[JObject] = {
    val query = primaryQueryPlan(plan)
    val named: Seq[NamedExpression] = query match {
      case p: Project => p.projectList
      case a: Aggregate => a.aggregateExpressions
      case _ => Nil
    }
    named.map(selectListItem)
  }

  private def primaryQueryPlan(plan: LogicalPlan): LogicalPlan = plan match {
    case UnresolvedWith(child, _, _) => primaryQueryPlan(child)
    case InsertIntoStatement(_, _, _, query, _, _, _, _, _) =>
      primaryQueryPlan(query)
    case c: CreateTableAsSelect => primaryQueryPlan(c.query)
    case r: ReplaceTableAsSelect => primaryQueryPlan(r.query)
    case SubqueryAlias(_, child) => primaryQueryPlan(child)
    case other => other
  }

  private def selectListItem(ne: NamedExpression): JObject = ne match {
    case Alias(child, name) =>
      JObject(
        "name" -> partsToJArray(Seq(name)),
        "expression" -> JString(child.sql))
    case u: UnresolvedAlias =>
      JObject(
        "name" -> partsToJArray(Nil),
        "expression" -> JString(u.child.sql))
    case s: UnresolvedStar =>
      val name = s.target.map(_ :+ "*").getOrElse(Seq("*"))
      JObject(
        "name" -> partsToJArray(name),
        "expression" -> JString(s.sql))
    case a: UnresolvedAttribute =>
      JObject(
        "name" -> partsToJArray(a.nameParts),
        "expression" -> JString(a.sql))
    case other =>
      JObject(
        "name" -> partsToJArray(Seq(other.name)),
        "expression" -> JString(other.sql))
  }

  private def parameterMarkersJson(plan: LogicalPlan): JObject = {
    val named = mutable.LinkedHashSet.empty[String]
    var unnamedCount = 0
    def visitExpr(e: Expression): Unit = e.foreach {
      case n: NamedParameter => named += n.name
      case _: PosParameter => unnamedCount += 1
      case _ =>
    }
    foreachPlanDeep(plan) { p =>
      p.expressions.foreach(visitExpr)
    }
    JObject(
      "named" -> JArray(named.toList.map(JString)),
      "unnamed_count" -> JInt(unnamedCount)
    )
  }
}
