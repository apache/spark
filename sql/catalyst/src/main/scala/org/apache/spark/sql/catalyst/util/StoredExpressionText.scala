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

import org.apache.spark.sql.catalyst.analysis.{Analyzer, ExpressionWithUnresolvedIdentifier}
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, ExprId}
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParseException}
import org.apache.spark.sql.internal.SQLConf

/**
 * Reads the SQL text of a generation expression stored with a table, independently of the
 * settings of the session that reads it.
 */
object StoredExpressionText {

  /**
   * The settings a stored text is read under. The session time zone, case sensitivity, timestamp
   * type and decimal precision loss stay with the given session, because the settings of the DDL
   * are not known here and pinning them would silently change existing tables of a deployment that
   * sets them.
   */
  def conf(session: SQLConf): SQLConf = {
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
  def parse(text: String): Expression = {
    try {
      SQLConf.withExistingConf(conf(SQLConf.get)) {
        CatalystSqlParser.parseExpression(text)
      }
    } catch {
      case _: ParseException => CatalystSqlParser.parseExpression(text)
    }
  }

  /**
   * Whether `text` reads the same under [[conf]] as it does in the active session. The comparison
   * uses the ANSI value of the session, so that it is about how the text reads.
   */
  def readsAsInSession(text: String): Boolean = {
    val session = SQLConf.get
    val fixed = conf(session)
    fixed.setConfString(SQLConf.ANSI_ENABLED.key, session.ansiEnabled.toString)
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
