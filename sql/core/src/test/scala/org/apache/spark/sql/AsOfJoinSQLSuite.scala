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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.AsOfJoin
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * SQL ASOF JOIN surface tests (parser, analysis, and feature gating).
 * Execution semantics and complex MATCH_CONDITION types are covered by
 * `AsOfJoinSortMergeSQLSuite`, which requires sort-merge ASOF join.
 */
class AsOfJoinSQLSuite extends QueryTest with SharedSparkSession {

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(SQLConf.SQL_ASOF_JOIN_ENABLED.key, "true")
  }

  override def afterAll(): Unit = {
    spark.conf.unset(SQLConf.SQL_ASOF_JOIN_ENABLED.key)
    super.afterAll()
  }

  private def setupTradeQuoteViews(): Unit = {
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW trades(trade_time, symbol) AS
        |VALUES (TIMESTAMP '2026-06-29 10:00:05', 'AAPL')
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW quotes(quote_time, symbol) AS
        |VALUES (TIMESTAMP '2026-06-29 10:00:00', 'AAPL')
        |""".stripMargin)
  }

  test("equality operator is rejected in MATCH_CONDITION") {
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW trades(trade_time, symbol) AS
        |VALUES (TIMESTAMP '2026-06-29 10:00:00', 'AAPL')
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW quotes(quote_time, symbol) AS
        |VALUES (TIMESTAMP '2026-06-29 09:00:00', 'AAPL')
        |""".stripMargin)
    val sqlText =
      """
        |SELECT * FROM trades t
        |ASOF JOIN quotes q
        |  MATCH_CONDITION (t.trade_time = q.quote_time)
        |  ON t.symbol = q.symbol
        |""".stripMargin
    checkError(
      exception = intercept[ParseException](sql(sqlText)),
      condition = "ASOF_JOIN_MATCH_CONDITION_INVALID_OPERATOR",
      sqlState = Some("42K0E"),
      parameters = Map("operator" -> "="),
      queryContext = Array(
        ExpectedContext(
          fragment = """ASOF JOIN quotes q
                       |  MATCH_CONDITION (t.trade_time = q.quote_time)
                       |  ON t.symbol = q.symbol""".stripMargin,
          start = 24,
          stop = 114)))
  }

  test("SQL ASOF JOIN requires sort-merge conf") {
    setupTradeQuoteViews()
    val sqlText =
      """
        |SELECT t.trade_time, q.quote_time
        |FROM trades t ASOF JOIN quotes q
        |  MATCH_CONDITION (t.trade_time >= q.quote_time)
        |  ON t.symbol = q.symbol
        |""".stripMargin
    withSQLConf(SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> "false") {
      checkError(
        exception = intercept[AnalysisException](sql(sqlText)),
        condition = "AS_OF_JOIN.SORT_MERGE_REQUIRED",
        parameters = Map("config" -> SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key),
        queryContext = Array(
          ExpectedContext(
            fragment = """ASOF JOIN quotes q
                         |  MATCH_CONDITION (t.trade_time >= q.quote_time)
                         |  ON t.symbol = q.symbol""".stripMargin,
            start = 49,
            stop = 140)))
    }
  }

  test("valid TIMESTAMP MATCH_CONDITION passes analysis with sort-merge enabled") {
    setupTradeQuoteViews()
    val sqlText =
      """
        |SELECT t.trade_time, q.quote_time
        |FROM trades t ASOF JOIN quotes q
        |  MATCH_CONDITION (t.trade_time >= q.quote_time)
        |  ON t.symbol = q.symbol
        |""".stripMargin
    withSQLConf(SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> "true") {
      val asOfJoin = sql(sqlText).queryExecution.analyzed.collectFirst {
        case j: AsOfJoin => j
      }.get
      assert(asOfJoin.asOfCondition.resolved)
      assert(asOfJoin.leftSortExprs.nonEmpty)
      assert(asOfJoin.rightSortExprs.nonEmpty)
      assert(asOfJoin.matchLeftOperand.isEmpty)
    }
  }

  test("MATCH_CONDITION rejects STRING operands until composite sort-merge lands") {
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW left_s(k) AS VALUES ('c')
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW right_s(k) AS VALUES ('a'), ('b')
        |""".stripMargin)
    val sqlText =
      """
        |SELECT l.k
        |FROM left_s l ASOF JOIN right_s r
        |  MATCH_CONDITION (l.k >= r.k)
        |""".stripMargin
    withSQLConf(SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> "true") {
      checkError(
        exception = intercept[AnalysisException](sql(sqlText)),
        condition = "AS_OF_JOIN.UNSUPPORTED_MATCH_CONDITION_OPERAND",
        sqlState = Some("42604"),
        parameters = Map(
          "type1" -> "\"STRING\"",
          "type2" -> "\"STRING\""),
        queryContext = Array(
          ExpectedContext(
            fragment = """ASOF JOIN right_s r
                         |  MATCH_CONDITION (l.k >= r.k)""".stripMargin,
            start = 26,
            stop = 75)))
    }
  }

  test("MATCH_CONDITION rejects cross-side operand references") {
    setupTradeQuoteViews()
    val sqlText =
      """
        |SELECT count(*)
        |FROM trades t ASOF JOIN quotes q
        |  MATCH_CONDITION (t.trade_time + q.quote_time >= q.quote_time)
        |  ON t.symbol = q.symbol
        |""".stripMargin
    checkError(
      exception = intercept[AnalysisException](sql(sqlText)),
      condition = "ASOF_JOIN_MATCH_CONDITION_TABLE_REFERENCE",
      sqlState = Some("42K0E"),
      parameters = Map(
        "refs1" -> "\"(trade_time + quote_time)\"",
        "refs2" -> "\"quote_time\""),
      queryContext = Array(
        ExpectedContext(
          fragment = """ASOF JOIN quotes q
                       |  MATCH_CONDITION (t.trade_time + q.quote_time >= q.quote_time)
                       |  ON t.symbol = q.symbol""".stripMargin,
          start = 31,
          stop = 137)))
  }

  test("MATCH_CONDITION rejects invalid table references") {
    setupTradeQuoteViews()
    val sqlText =
      """
        |SELECT count(*)
        |FROM trades t ASOF JOIN quotes q
        |  MATCH_CONDITION (t.symbol >= t.symbol)
        |  ON t.symbol = q.symbol
        |""".stripMargin
    checkError(
      exception = intercept[AnalysisException](sql(sqlText)),
      condition = "ASOF_JOIN_MATCH_CONDITION_TABLE_REFERENCE",
      sqlState = Some("42K0E"),
      parameters = Map(
        "refs1" -> "\"symbol\"",
        "refs2" -> "\"symbol\""))
  }

  test("MATCH_CONDITION rejects non-deterministic expressions") {
    setupTradeQuoteViews()
    val sqlText =
      """
        |SELECT t.trade_time
        |FROM trades t ASOF JOIN quotes q
        |  MATCH_CONDITION (rand() >= q.quote_time)
        |  ON t.symbol = q.symbol
        |""".stripMargin
    checkError(
      exception = intercept[AnalysisException](sql(sqlText)),
      condition = "ASOF_JOIN_MATCH_CONDITION_INVALID_EXPRESSION",
      sqlState = Some("42903"),
      parameters = Map("expr" -> "\"rand()\""),
      queryContext = Array(
        ExpectedContext(
          fragment = """ASOF JOIN quotes q
                       |  MATCH_CONDITION (rand() >= q.quote_time)
                       |  ON t.symbol = q.symbol""".stripMargin,
          start = 35,
          stop = 120)))
  }

  test("MATCH_CONDITION rejects incompatible operand types") {
    setupTradeQuoteViews()
    val sqlText =
      """
        |SELECT t.trade_time
        |FROM trades t ASOF JOIN quotes q
        |  MATCH_CONDITION (t.trade_time >= q.symbol)
        |  ON t.symbol = q.symbol
        |""".stripMargin
    checkError(
      exception = intercept[AnalysisException](sql(sqlText)),
      condition = "ASOF_JOIN_MATCH_CONDITION_INVALID_TYPE",
      sqlState = Some("42K09"),
      parameters = Map(
        "type1" -> "\"TIMESTAMP\"",
        "type2" -> "\"STRING\""),
      queryContext = Array(
        ExpectedContext(
          fragment = """ASOF JOIN quotes q
                       |  MATCH_CONDITION (t.trade_time >= q.symbol)
                       |  ON t.symbol = q.symbol""".stripMargin,
          start = 35,
          stop = 122)))
  }
}
