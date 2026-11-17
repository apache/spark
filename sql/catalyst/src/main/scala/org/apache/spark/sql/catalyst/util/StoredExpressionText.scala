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

package org.apache.spark.sql.catalyst.util

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{Analyzer, ExpressionWithUnresolvedIdentifier, SimpleAnalyzer, StoredExpressionFunctions}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, ExprId, NamedExpression}
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParseException}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.internal.SQLConf

/**
 * Reads and analyzes generation expressions stored with a table independently of the writing
 * session, with a session fallback for SQL text that the fixed settings cannot resolve.
 */
object StoredExpressionText {

  /**
   * The settings a stored text is read under. The session time zone, case sensitivity, timestamp
   * type and decimal precision loss stay with the given session, because the settings of the DDL
   * are not known here and pinning them would silently change existing tables of a deployment that
   * sets them. Any other config that the parsing or the analysis reads takes its default value,
   * also one that a function reads while it is built, such as
   * `spark.sql.legacy.negativeIndexInArrayInsert`. With
   * `spark.sql.legacy.useCurrentConfigsForGeneratedColumn`, these are the settings of the session.
   */
  def conf(session: SQLConf): SQLConf = {
    if (session.useCurrentSQLConfigsForGeneratedColumn) {
      session.clone()
    } else {
      fixedConf(session)
    }
  }

  /** The fixed ANSI settings, also for modeled expressions when the legacy flag is enabled. */
  private[sql] def fixedConf(session: SQLConf): SQLConf = {
    val c = new SQLConf
    Analyzer.retainResolutionConfigsForAnalysis(newConf = c, existingConf = session)
    c.setConfString(SQLConf.ANSI_ENABLED.key, "true")
    c.setConfString(SQLConf.SESSION_LOCAL_TIMEZONE.key, session.sessionLocalTimeZone)
    c.setConfString(SQLConf.CASE_SENSITIVE.key, session.caseSensitiveAnalysis.toString)
    c.setConf(SQLConf.TIMESTAMP_TYPE, session.getConf(SQLConf.TIMESTAMP_TYPE))
    c.setConf(
      SQLConf.DECIMAL_OPERATIONS_ALLOW_PREC_LOSS, session.decimalOperationsAllowPrecisionLoss)
    c
  }

  /**
   * Parses `text` under [[conf]]. A text these settings cannot parse is parsed under the active
   * session, as a text they cannot resolve is analyzed under it.
   */
  def parse(text: String): Expression = parse(conf(SQLConf.get), text)

  /**
   * Parses `text` under [[conf]] with the ANSI value of the active session, for a text that
   * [[conf]] cannot resolve and the session analyzes. Only its ANSI behavior follows the session.
   */
  def parseWithSessionAnsi(text: String): Expression = parse(confWithSessionAnsi(SQLConf.get), text)

  private def parse(c: SQLConf, text: String): Expression = {
    val parsed = try {
      SQLConf.withExistingConf(c) {
        CatalystSqlParser.parseExpression(text)
      }
    } catch {
      case _: ParseException => CatalystSqlParser.parseExpression(text)
    }
    // Neither the SQL path nor a temporary function of the session can shadow a built-in function.
    StoredExpressionFunctions.qualifyBuiltinFunctions(parsed)
  }

  private def confWithSessionAnsi(session: SQLConf): SQLConf = {
    val c = conf(session)
    c.setConfString(SQLConf.ANSI_ENABLED.key, session.ansiEnabled.toString)
    c
  }

  /**
   * Analyzes `expr` over `input` with `analyzer` under [[conf]], so that an auto-filled value and
   * the check of a provided value read a stored text the same way. If these settings cannot
   * analyze or resolve it, such as an identifier string that only the session can parse, or it
   * analyzes to another plan, such as an aggregate that a catalog stores, this returns `inSession`
   * instead, and the analysis of the write resolves that under the session.
   */
  def analyze[E <: Expression](
      expr: E,
      inSession: => E,
      input: Seq[Attribute],
      analyzer: LogicalPlan => LogicalPlan): E = {
    val analyzed = try {
      expressionFromPlan(expr, analyzePlan(expr, input, analyzer, conf(SQLConf.get)))
    } catch {
      case _: AnalysisException => None
    }
    analyzed.getOrElse(inSession)
  }

  /**
   * Analyzes a modeled expression under fixed ANSI settings and reports errors without fallback.
   */
  def analyzeStrict[E <: Expression](
      expr: E,
      input: Seq[Attribute],
      analyzer: LogicalPlan => LogicalPlan): E = {
    val plan = analyzePlan(expr, input, analyzer, fixedConf(SQLConf.get))
    // Check under the session's representation settings, including first-class CHAR/VARCHAR.
    SimpleAnalyzer.checkAnalysis(plan)
    expressionFromPlan(expr, plan).getOrElse {
      throw SparkException.internalError(s"Expected a scalar stored expression, but got $plan")
    }
  }

  private def analyzePlan(
      expr: Expression,
      input: Seq[Attribute],
      analyzer: LogicalPlan => LogicalPlan,
      settings: SQLConf): LogicalPlan = {
    val named = expr match {
      case n: NamedExpression => n
      case e => Alias(e, "value")()
    }
    SQLConf.withExistingConf(settings) {
      analyzer(Project(Seq(named), LocalRelation(input)))
    }
  }

  private def expressionFromPlan[E <: Expression](expr: E, plan: LogicalPlan): Option[E] = {
    plan match {
      case Project(Seq(a), _: LocalRelation) if a.resolved =>
        val expression = a match {
          case Alias(child, _) if !expr.isInstanceOf[NamedExpression] => child
          case other => other
        }
        Some(expression.asInstanceOf[E])
      case _ => None
    }
  }

  /**
   * Whether `text` reads the same under [[conf]] as it does in the active session. The comparison
   * uses the ANSI value of the session, so that it is about how the text reads.
   */
  def readsAsInSession(text: String): Boolean = {
    val fixed = confWithSessionAnsi(SQLConf.get)
    val parsedInSession = CatalystSqlParser.parseExpression(text)
    try {
      val parsed = SQLConf.withExistingConf(fixed) {
        CatalystSqlParser.parseExpression(text)
      }
      normalize(parsed) == normalize(parsedInSession)
    } catch {
      case _: ParseException => false
    }
  }

  private def normalize(e: Expression): Expression = e.transform {
    case a: Alias => a.withExprId(ExprId(0))
    // Two parses of the same text never share the builder, which is a function.
    case i: ExpressionWithUnresolvedIdentifier => i.copy(exprBuilder = noBuilder)
  }

  private val noBuilder: (Seq[String], Seq[Expression]) => Expression = (_, exprs) => exprs.head
}
