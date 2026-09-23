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

import java.sql.Timestamp

import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.AsOfJoin
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DateType, IntegerType, LongType}

/**
 * SQL ASOF JOIN surface tests (parser, analysis, and feature gating).
 * Execution semantics and complex MATCH_CONDITION types are covered by
 * `AsOfJoinSortMergeSQLSuite`.
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
        |CREATE OR REPLACE TEMP VIEW quotes(quote_time, symbol, bid_price) AS
        |VALUES (TIMESTAMP '2026-06-29 10:00:00', 'AAPL', 180.10)
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

  test("valid TIMESTAMP MATCH_CONDITION passes analysis") {
    setupTradeQuoteViews()
    val sqlText =
      """
        |SELECT t.trade_time, q.quote_time
        |FROM trades t ASOF JOIN quotes q
        |  MATCH_CONDITION (t.trade_time >= q.quote_time)
        |  ON t.symbol = q.symbol
        |""".stripMargin
    val asOfJoin = sql(sqlText).queryExecution.analyzed.collectFirst {
      case j: AsOfJoin => j
    }.get
    assert(asOfJoin.asOfCondition.resolved)
    assert(asOfJoin.leftSortExprs.nonEmpty)
    assert(asOfJoin.rightSortExprs.nonEmpty)
    assert(asOfJoin.matchLeftOperand.isEmpty)
  }

  test("SQL ASOF JOIN uses sort-merge without DataFrame sort-merge conf") {
    setupTradeQuoteViews()
    val sqlText =
      """
        |SELECT t.trade_time, q.quote_time
        |FROM trades t ASOF JOIN quotes q
        |  MATCH_CONDITION (t.trade_time >= q.quote_time)
        |  ON t.symbol = q.symbol
        |""".stripMargin
    withSQLConf(SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> "false") {
      checkAnswer(sql(sqlText), Row(Timestamp.valueOf("2026-06-29 10:00:05"),
        Timestamp.valueOf("2026-06-29 10:00:00")))
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
        "operand1" -> "\"(trade_time + quote_time)\"",
        "operand2" -> "\"quote_time\""),
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
        "operand1" -> "\"symbol\"",
        "operand2" -> "\"symbol\""))
  }

  test("MATCH_CONDITION rejects a literal even when a same-side column is present") {
    setupTradeQuoteViews()
    // `'AAPL'` references no join input, so no operand binds to the left side and it is rejected.
    val sqlText =
      """
        |SELECT count(*)
        |FROM trades t ASOF JOIN quotes q
        |  MATCH_CONDITION (q.symbol >= 'AAPL')
        |  ON t.symbol = q.symbol
        |""".stripMargin
    checkError(
      exception = intercept[AnalysisException](sql(sqlText)),
      condition = "ASOF_JOIN_MATCH_CONDITION_TABLE_REFERENCE",
      sqlState = Some("42K0E"),
      parameters = Map("operand1" -> "\"symbol\"", "operand2" -> "\"AAPL\""))
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

  // A numeric MATCH_CONDITION must sort by value, not lexicographically (the string case is the
  // only one that shows it). ANSI widens INT vs STRING to LONG, the default mode to INT.
  Seq(true -> LongType, false -> IntegerType).foreach { case (ansi, sortType) =>
    test(s"MATCH_CONDITION coerces INT vs STRING to a numeric sort key (ansi=$ansi)") {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi.toString) {
        val sqlText =
          """
            |SELECT t.k, r.s
            |FROM VALUES (20) AS t(k) ASOF JOIN
            |     VALUES ('9'), ('10'), ('20') AS r(s)
            |  MATCH_CONDITION (t.k >= r.s)
            |""".stripMargin
        val asOfJoin = sql(sqlText).queryExecution.analyzed.collectFirst {
          case j: AsOfJoin => j
        }.get
        assert(asOfJoin.leftSortExprs.forall(_.dataType == sortType))
        assert(asOfJoin.rightSortExprs.forall(_.dataType == sortType))
        // A lexicographic STRING sort key would order '9' last and wrongly match it for k = 20.
        checkAnswer(sql(sqlText), Row(20, "20"))
      }
    }
  }

  test("MATCH_CONDITION rejects incompatible operand types") {
    setupTradeQuoteViews()
    val sqlText =
      """
        |SELECT t.trade_time
        |FROM trades t ASOF JOIN quotes q
        |  MATCH_CONDITION (t.trade_time >= q.bid_price)
        |  ON t.symbol = q.symbol
        |""".stripMargin
    checkError(
      exception = intercept[AnalysisException](sql(sqlText)),
      condition = "ASOF_JOIN_MATCH_CONDITION_INVALID_TYPE",
      sqlState = Some("42K09"),
      parameters = Map(
        "type1" -> "\"TIMESTAMP\"",
        "type2" -> "\"DECIMAL(5,2)\""),
      queryContext = Array(
        ExpectedContext(
          fragment = """ASOF JOIN quotes q
                       |  MATCH_CONDITION (t.trade_time >= q.bid_price)
                       |  ON t.symbol = q.symbol""".stripMargin,
          start = 35,
          stop = 125)))
  }

  // SPARK-59527 reproduces with ANSI on and off, so both are checked.
  Seq(true, false).foreach { ansi =>
    test(s"MATCH_CONDITION coerces DATE vs STRING like the comparison operator (ansi=$ansi)") {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi.toString) {
        // Coerced to DATE like `>=`; before SPARK-59527 this failed with INVALID_TYPE.
        val sqlText =
          """
            |SELECT l.d, r.s
            |FROM VALUES (DATE '2024-01-03') AS l(d) ASOF JOIN
            |     VALUES ('2024-01-01') AS r(s)
            |  MATCH_CONDITION (l.d >= r.s)
            |""".stripMargin
        val asOfJoin = sql(sqlText).queryExecution.analyzed.collectFirst {
          case j: AsOfJoin => j
        }.get
        assert(asOfJoin.asOfCondition.resolved)
        // Both sort keys are DATE, so the sort order matches the coerced comparison.
        assert(asOfJoin.leftSortExprs.nonEmpty && asOfJoin.rightSortExprs.nonEmpty)
        assert(asOfJoin.leftSortExprs.forall(_.dataType == DateType))
        assert(asOfJoin.rightSortExprs.forall(_.dataType == DateType))
        checkAnswer(sql(sqlText), Row(java.sql.Date.valueOf("2024-01-03"), "2024-01-01"))
      }
    }
  }

  test("MATCH_CONDITION rejects a query-foldable constant operand (no join input reference)") {
    setupTradeQuoteViews()
    val sqlText =
      """
        |SELECT count(*)
        |FROM trades t ASOF JOIN quotes q
        |  MATCH_CONDITION (current_timestamp() >= q.quote_time)
        |  ON t.symbol = q.symbol
        |""".stripMargin
    checkError(
      exception = intercept[AnalysisException](sql(sqlText)),
      condition = "ASOF_JOIN_MATCH_CONDITION_TABLE_REFERENCE",
      sqlState = Some("42K0E"),
      parameters = Map(
        "operand1" -> "\"current_timestamp()\"",
        "operand2" -> "\"quote_time\""))
  }

  test("MATCH_CONDITION rejects a literal constant operand (no join input reference)") {
    setupTradeQuoteViews()
    val sqlText =
      """
        |SELECT count(*)
        |FROM trades t ASOF JOIN quotes q
        |  MATCH_CONDITION (t.trade_time >= TIMESTAMP '2026-06-29 10:00:00')
        |  ON t.symbol = q.symbol
        |""".stripMargin
    checkError(
      exception = intercept[AnalysisException](sql(sqlText)),
      condition = "ASOF_JOIN_MATCH_CONDITION_TABLE_REFERENCE",
      sqlState = Some("42K0E"),
      parameters = Map(
        "operand1" -> "\"trade_time\"",
        "operand2" -> "\"TIMESTAMP '2026-06-29 10:00:00'\""))
  }

  test("MATCH_CONDITION rejects two literal operands (neither references a join input)") {
    setupTradeQuoteViews()
    // Both operands are literals that reference no join input, so neither binds to a side.
    val sqlText =
      """
        |SELECT count(*)
        |FROM trades t ASOF JOIN quotes q
        |  MATCH_CONDITION (TIMESTAMP '2026-06-29 10:00:00' >= TIMESTAMP '2026-06-29 09:00:00')
        |  ON t.symbol = q.symbol
        |""".stripMargin
    checkError(
      exception = intercept[AnalysisException](sql(sqlText)),
      condition = "ASOF_JOIN_MATCH_CONDITION_TABLE_REFERENCE",
      sqlState = Some("42K0E"),
      parameters = Map(
        "operand1" -> "\"TIMESTAMP '2026-06-29 10:00:00'\"",
        "operand2" -> "\"TIMESTAMP '2026-06-29 09:00:00'\""))
  }

  test("MATCH_CONDITION rejects a constant operand under the single-pass analyzer") {
    setupTradeQuoteViews()
    val sqlText =
      """
        |SELECT count(*)
        |FROM trades t ASOF JOIN quotes q
        |  MATCH_CONDITION (t.trade_time >= TIMESTAMP '2026-06-29 10:00:00')
        |  ON t.symbol = q.symbol
        |""".stripMargin
    withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
      checkError(
        exception = intercept[AnalysisException](sql(sqlText)),
        condition = "ASOF_JOIN_MATCH_CONDITION_TABLE_REFERENCE",
        sqlState = Some("42K0E"),
        parameters = Map(
          "operand1" -> "\"trade_time\"",
          "operand2" -> "\"TIMESTAMP '2026-06-29 10:00:00'\""))
    }
  }

  test("MATCH_CONDITION rejects scalar subquery operand") {
    setupTradeQuoteViews()
    val sqlText =
      """
        |SELECT * FROM trades t ASOF JOIN quotes q
        |  MATCH_CONDITION (t.trade_time >= (SELECT max(trade_time) FROM trades))
        |  ON t.symbol = q.symbol
        |""".stripMargin
    checkError(
      exception = intercept[AnalysisException](sql(sqlText)),
      condition = "ASOF_JOIN_MATCH_CONDITION_INVALID_EXPRESSION",
      sqlState = Some("42903"),
      parameters = Map("expr" -> "\"scalarsubquery()\""),
      queryContext = Array(
        ExpectedContext(
          fragment = """ASOF JOIN quotes q
                       |  MATCH_CONDITION (t.trade_time >= (SELECT max(trade_time) FROM trades))
                       |  ON t.symbol = q.symbol""".stripMargin,
          start = 24,
          stop = 139)))
  }

  test("uncorrelated scalar subquery in ON clause is allowed") {
    setupTradeQuoteViews()
    checkAnswer(
      sql(
        """
          |SELECT t.symbol
          |FROM trades t ASOF JOIN quotes q
          |  MATCH_CONDITION (t.trade_time >= q.quote_time)
          |  ON t.symbol = q.symbol
          | AND t.trade_time >= (SELECT max(trade_time) FROM trades)
          |""".stripMargin),
      Row("AAPL"))
  }

  test("correlated scalar subquery in ON clause is rejected like regular JOIN") {
    setupTradeQuoteViews()
    val sqlText =
      """
        |SELECT t.symbol, q.bid_price
        |FROM trades t ASOF JOIN quotes q
        |  MATCH_CONDITION (t.trade_time >= q.quote_time)
        |  ON t.symbol = q.symbol
        | AND q.bid_price > (
        |   SELECT avg(bid_price) FROM quotes qq WHERE qq.symbol = q.symbol)
        |""".stripMargin
    val e = intercept[AnalysisException](sql(sqlText))
    assert(
      e.getCondition ===
        "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.UNSUPPORTED_CORRELATED_SCALAR_SUBQUERY")
  }

  test("MATCH_CONDITION accepts STRUCT tuple operands") {
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW deploys(deploy_ts, seq, service, version) AS
        |VALUES (TIMESTAMP '2026-06-29 10:00:00', 1, 'api', 'v1.0')
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW requests(req_ts, seq, service) AS
        |VALUES (TIMESTAMP '2026-06-29 10:03:00', 1, 'api')
        |""".stripMargin)
    val sqlText =
      """
        |SELECT r.req_ts, d.version
        |FROM requests r ASOF JOIN deploys d
        |  MATCH_CONDITION ((r.req_ts, r.seq) >= (d.deploy_ts, d.seq))
        |  ON r.service = d.service
        |""".stripMargin
    val asOfJoin = sql(sqlText).queryExecution.analyzed.collectFirst {
      case j: AsOfJoin => j
    }.get
    assert(asOfJoin.asOfCondition.resolved)
    assert(asOfJoin.leftSortExprs.nonEmpty)
    assert(asOfJoin.rightSortExprs.nonEmpty)
  }

  test("MATCH_CONDITION accepts ARRAY operands") {
    val sqlText =
      """
        |SELECT t.a
        |FROM VALUES (ARRAY(1, 3)) AS t(a) ASOF JOIN VALUES (ARRAY(1, 2)) AS r(a)
        |  MATCH_CONDITION (t.a >= r.a)
        |""".stripMargin
    val asOfJoin = sql(sqlText).queryExecution.analyzed.collectFirst {
      case j: AsOfJoin => j
    }.get
    assert(asOfJoin.asOfCondition.resolved)
    assert(asOfJoin.leftSortExprs.nonEmpty)
    assert(asOfJoin.rightSortExprs.nonEmpty)
  }

  test("ARRAY<STRUCT> MATCH_CONDITION with mismatched element names and types is rejected") {
    // ASOF operand validation accepts these (fields compared positionally, names ignored), the
    // failure is the generic comparison check, since array elements are not name-realigned.
    val sqlText =
      """
        |SELECT r.a
        |FROM VALUES (ARRAY(named_struct('x', CAST(5 AS INT)))) AS t(a)
        |ASOF JOIN (
        |  SELECT * FROM VALUES (ARRAY(named_struct('y', CAST(5 AS BIGINT)))) AS r(a)
        |) r
        |  MATCH_CONDITION (t.a >= r.a)
        |""".stripMargin
    checkError(
      exception = intercept[AnalysisException](sql(sqlText)),
      condition = "DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES",
      sqlState = Some("42K09"),
      parameters = Map(
        "left" -> "\"ARRAY<STRUCT<x: INT NOT NULL>>\"",
        "right" -> "\"ARRAY<STRUCT<y: BIGINT NOT NULL>>\"",
        "sqlExpr" -> "\"(a >= a)\""),
      queryContext = Array(
        ExpectedContext(
          fragment = """ASOF JOIN (
                     |  SELECT * FROM VALUES (ARRAY(named_struct('y', CAST(5 AS BIGINT)))) AS r(a)
                     |) r
                     |  MATCH_CONDITION (t.a >= r.a)""".stripMargin,
          start = 75,
          stop = 197)))
  }

  test("MATCH_CONDITION accepts nested STRUCT column operands") {
    val sqlText =
      """
        |SELECT r.k.tag
        |FROM VALUES (named_struct(
        |  'inner_ts', TIMESTAMP '2026-06-29 10:00:00',
        |  'inner_seq', 2,
        |  'tag', 'a')) AS t(k)
        |ASOF JOIN VALUES (named_struct(
        |  'inner_ts', TIMESTAMP '2026-06-29 09:00:00',
        |  'inner_seq', 1,
        |  'tag', 'a')) AS r(k)
        |  MATCH_CONDITION (t.k >= r.k)
        |""".stripMargin
    val asOfJoin = sql(sqlText).queryExecution.analyzed.collectFirst {
      case j: AsOfJoin => j
    }.get
    assert(asOfJoin.asOfCondition.resolved)
  }

  test("MATCH_CONDITION rejects empty STRUCT operands") {
    val sqlText =
      """
        |SELECT *
        |FROM VALUES (named_struct()) AS t(s) ASOF JOIN VALUES (named_struct()) AS r(s)
        |  MATCH_CONDITION (t.s >= r.s)
        |""".stripMargin
    checkError(
      exception = intercept[AnalysisException](sql(sqlText)),
      condition = "ASOF_JOIN_MATCH_CONDITION_INVALID_TYPE",
      sqlState = Some("42K09"),
      parameters = Map(
        "type1" -> "\"STRUCT<>\"",
        "type2" -> "\"STRUCT<>\""),
      queryContext = Array(
        ExpectedContext(
          fragment = """ASOF JOIN VALUES (named_struct()) AS r(s)
                       |  MATCH_CONDITION (t.s >= r.s)""".stripMargin,
          start = 47,
          stop = 118)))
  }
}
