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

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.joins.SortMergeAsOfJoinExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * SQL ASOF JOIN tests that require the sort-merge physical operator
 * (`spark.sql.join.sortMergeAsOfJoin.enabled=true`).
 */
class AsOfJoinSortMergeSQLSuite extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(SQLConf.SQL_ASOF_JOIN_ENABLED.key, "true")
    spark.conf.set(SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key, "true")
  }

  override def afterAll(): Unit = {
    spark.conf.unset(SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key)
    spark.conf.unset(SQLConf.SQL_ASOF_JOIN_ENABLED.key)
    super.afterAll()
  }

  private def assertUsesSortMergeAsOfJoin(query: DataFrame): Unit = {
    val plan = query.queryExecution.executedPlan
    assert(collectWithSubqueries(plan) {
      case _: SortMergeAsOfJoinExec => true
    }.nonEmpty, s"Expected SortMergeAsOfJoinExec in plan:\n$plan")
  }

  private def checkSortMergeAsOf(query: => DataFrame, expected: Seq[Row]): Unit = {
    val df = query
    assertUsesSortMergeAsOfJoin(df)
    checkAnswer(df, expected)
  }

  private def setupTradeQuoteViews(): Unit = {
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW trades(trade_time, symbol, quantity) AS
        |VALUES (TIMESTAMP '2026-06-29 10:00:05', 'AAPL', 100),
        |       (TIMESTAMP '2026-06-29 10:00:11', 'AAPL', 200),
        |       (TIMESTAMP '2026-06-29 10:00:12', 'MSFT',  50),
        |       (TIMESTAMP '2026-06-29 09:59:59', 'GOOG',  30)
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW quotes(quote_time, symbol, bid_price) AS
        |VALUES (TIMESTAMP '2026-06-29 10:00:00', 'AAPL', 180.10),
        |       (TIMESTAMP '2026-06-29 10:00:07', 'AAPL', 180.15),
        |       (TIMESTAMP '2026-06-29 10:00:10', 'AAPL', 180.20),
        |       (TIMESTAMP '2026-06-29 10:00:08', 'MSFT', 420.50)
        |""".stripMargin)
  }

  test("SQL ASOF JOIN requires sort-merge conf") {
    setupTradeQuoteViews()
    val sqlText =
      """
        |SELECT t.trade_time, q.bid_price
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
            start = 48,
            stop = 139)))
    }
  }

  test("INNER ASOF JOIN with TIMESTAMP MATCH_CONDITION") {
    setupTradeQuoteViews()
    checkSortMergeAsOf(
      sql(
        """
          |SELECT t.trade_time, t.symbol, t.quantity, q.bid_price
          |FROM trades t
          |ASOF JOIN quotes q
          |  MATCH_CONDITION (t.trade_time >= q.quote_time)
          |  ON t.symbol = q.symbol
          |""".stripMargin),
      Seq(
        Row(Timestamp.valueOf("2026-06-29 10:00:05"), "AAPL", 100, 180.10),
        Row(Timestamp.valueOf("2026-06-29 10:00:11"), "AAPL", 200, 180.20),
        Row(Timestamp.valueOf("2026-06-29 10:00:12"), "MSFT", 50, 420.50)))
  }

  test("LEFT ASOF JOIN preserves unmatched left rows") {
    setupTradeQuoteViews()
    checkSortMergeAsOf(
      sql(
        """
          |SELECT t.trade_time, t.symbol, q.bid_price
          |FROM trades t
          |LEFT ASOF JOIN quotes q
          |  MATCH_CONDITION (t.trade_time >= q.quote_time)
          |  ON t.symbol = q.symbol
          |ORDER BY t.trade_time
          |""".stripMargin),
      Seq(
        Row(Timestamp.valueOf("2026-06-29 09:59:59"), "GOOG", null),
        Row(Timestamp.valueOf("2026-06-29 10:00:05"), "AAPL", 180.10),
        Row(Timestamp.valueOf("2026-06-29 10:00:11"), "AAPL", 180.20),
        Row(Timestamp.valueOf("2026-06-29 10:00:12"), "MSFT", 420.50)))
  }

  test("USING is equivalent to ON symbol equality") {
    setupTradeQuoteViews()
    checkSortMergeAsOf(
      sql(
        """
          |SELECT trade_time, symbol, bid_price
          |FROM trades
          |ASOF JOIN quotes
          |  MATCH_CONDITION (trades.trade_time >= quotes.quote_time)
          |  USING (symbol)
          |ORDER BY trade_time
          |""".stripMargin),
      Seq(
        Row(Timestamp.valueOf("2026-06-29 10:00:05"), "AAPL", 180.10),
        Row(Timestamp.valueOf("2026-06-29 10:00:11"), "AAPL", 180.20),
        Row(Timestamp.valueOf("2026-06-29 10:00:12"), "MSFT", 420.50)))
  }

  test("forward match with <=") {
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW alerts(alert_time, host) AS
        |VALUES (TIMESTAMP '2026-06-29 10:00:00', 'db-01')
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW maintenance(window_start, host) AS
        |VALUES (TIMESTAMP '2026-06-29 08:00:00', 'db-01'),
        |       (TIMESTAMP '2026-06-29 12:00:00', 'db-01')
        |""".stripMargin)
    checkSortMergeAsOf(
      sql(
        """
          |SELECT a.alert_time, a.host, m.window_start
          |FROM alerts a
          |ASOF JOIN maintenance m
          |  MATCH_CONDITION (a.alert_time <= m.window_start)
          |  ON a.host = m.host
          |""".stripMargin),
      Row(
        Timestamp.valueOf("2026-06-29 10:00:00"),
        "db-01",
        Timestamp.valueOf("2026-06-29 12:00:00")) :: Nil)
  }

  test("DATE scalar MATCH_CONDITION") {
    checkSortMergeAsOf(
      sql(
        """
          |SELECT t.d, r.d AS matched_d
          |FROM VALUES (DATE '2026-06-29') AS t(d) ASOF JOIN
          |     VALUES (DATE '2026-06-28'), (DATE '2026-06-29') AS r(d)
          |  MATCH_CONDITION (t.d >= r.d)
          |""".stripMargin),
      Row(Date.valueOf("2026-06-29"), Date.valueOf("2026-06-29")) :: Nil)
  }

  test("INT scalar MATCH_CONDITION") {
    checkSortMergeAsOf(
      sql(
        """
          |SELECT t.k, r.k AS matched_k
          |FROM VALUES (10) AS t(k) ASOF JOIN VALUES (5), (7) AS r(k)
          |  MATCH_CONDITION (t.k >= r.k)
          |""".stripMargin),
      Row(10, 7) :: Nil)
  }

  test("STRING scalar MATCH_CONDITION") {
    checkSortMergeAsOf(
      sql(
        """
          |SELECT t.k, r.k AS matched_k
          |FROM VALUES ('c') AS t(k) ASOF JOIN VALUES ('a'), ('b') AS r(k)
          |  MATCH_CONDITION (t.k >= r.k)
          |""".stripMargin),
      Row("c", "b") :: Nil)
  }

  test("STRING scalar forward MATCH_CONDITION with <=") {
    checkSortMergeAsOf(
      sql(
        """
          |SELECT t.k, r.k AS matched_k
          |FROM VALUES ('b') AS t(k) ASOF JOIN VALUES ('c'), ('d'), ('z') AS r(k)
          |  MATCH_CONDITION (t.k <= r.k)
          |""".stripMargin),
      Row("b", "c") :: Nil)
  }

  test("DECIMAL scalar MATCH_CONDITION") {
    checkSortMergeAsOf(
      sql(
        """
          |SELECT t.k, r.k AS matched_k
          |FROM VALUES (CAST(10.50 AS DECIMAL(10, 2))) AS t(k) ASOF JOIN
          |     VALUES (CAST(5.00 AS DECIMAL(10, 2))),
          |            (CAST(7.25 AS DECIMAL(10, 2))),
          |            (CAST(9.99 AS DECIMAL(10, 2))) AS r(k)
          |  MATCH_CONDITION (t.k >= r.k)
          |""".stripMargin),
      Row(
        new java.math.BigDecimal("10.50"),
        new java.math.BigDecimal("9.99")) :: Nil)
  }

  test("DECIMAL scalar forward MATCH_CONDITION with <=") {
    checkSortMergeAsOf(
      sql(
        """
          |SELECT t.k, r.k AS matched_k
          |FROM VALUES (CAST(10.50 AS DECIMAL(10, 2))) AS t(k) ASOF JOIN
          |     VALUES (CAST(11.00 AS DECIMAL(10, 2))),
          |            (CAST(12.50 AS DECIMAL(10, 2))),
          |            (CAST(15.00 AS DECIMAL(10, 2))) AS r(k)
          |  MATCH_CONDITION (t.k <= r.k)
          |""".stripMargin),
      Row(
        new java.math.BigDecimal("10.50"),
        new java.math.BigDecimal("11.00")) :: Nil)
  }

  test("DOUBLE scalar MATCH_CONDITION") {
    checkSortMergeAsOf(
      sql(
        """
          |SELECT t.k, r.k AS matched_k
          |FROM VALUES (CAST(10.5 AS DOUBLE)) AS t(k) ASOF JOIN
          |     VALUES (CAST(5.0 AS DOUBLE)),
          |            (CAST(7.25 AS DOUBLE)),
          |            (CAST(9.99 AS DOUBLE)) AS r(k)
          |  MATCH_CONDITION (t.k >= r.k)
          |""".stripMargin),
      Row(10.5, 9.99) :: Nil)
  }

  test("DOUBLE scalar forward MATCH_CONDITION with <=") {
    checkSortMergeAsOf(
      sql(
        """
          |SELECT t.k, r.k AS matched_k
          |FROM VALUES (CAST(10.5 AS DOUBLE)) AS t(k) ASOF JOIN
          |     VALUES (CAST(11.0 AS DOUBLE)),
          |            (CAST(12.5 AS DOUBLE)),
          |            (CAST(15.0 AS DOUBLE)) AS r(k)
          |  MATCH_CONDITION (t.k <= r.k)
          |""".stripMargin),
      Row(10.5, 11.0) :: Nil)
  }

  test("STRUCT tuple MATCH_CONDITION") {
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW requests(req_ts, seq, service) AS
        |VALUES (TIMESTAMP '2026-06-29 10:00:00', 5, 'api'),
        |       (TIMESTAMP '2026-06-29 10:03:00', 1, 'api')
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW deploys(deploy_ts, seq, service, version) AS
        |VALUES (TIMESTAMP '2026-06-29 10:00:00', 1, 'api', 'v1.0'),
        |       (TIMESTAMP '2026-06-29 10:00:00', 3, 'api', 'v1.1')
        |""".stripMargin)
    checkSortMergeAsOf(
      sql(
        """
          |SELECT r.req_ts, r.seq, d.version
          |FROM requests r ASOF JOIN deploys d
          |  MATCH_CONDITION ((r.req_ts, r.seq) >= (d.deploy_ts, d.seq))
          |  ON r.service = d.service
          |ORDER BY r.req_ts, r.seq
          |""".stripMargin),
      Seq(
        Row(Timestamp.valueOf("2026-06-29 10:00:00"), 5, "v1.1"),
        Row(Timestamp.valueOf("2026-06-29 10:03:00"), 1, "v1.1")))
  }

  test("scalar leaf STRUCT tuple MATCH_CONDITION") {
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW left_nested(inner_ts, inner_seq, tag) AS
        |VALUES (TIMESTAMP '2026-06-29 10:00:00', 2, 'a')
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW right_nested(inner_ts, inner_seq, tag) AS
        |VALUES (TIMESTAMP '2026-06-29 09:00:00', 1, 'a'),
        |       (TIMESTAMP '2026-06-29 10:00:00', 1, 'a')
        |""".stripMargin)
    checkSortMergeAsOf(
      sql(
        """
          |SELECT r.tag, r.inner_seq
          |FROM left_nested t
          |ASOF JOIN right_nested r
          |  MATCH_CONDITION ((t.inner_ts, t.inner_seq, t.tag) >= (r.inner_ts, r.inner_seq, r.tag))
          |""".stripMargin),
      Row("a", 1) :: Nil)
  }

  test("whole STRUCT column MATCH_CONDITION") {
    checkSortMergeAsOf(
      sql(
        """
          |SELECT r.k.tag, r.k.inner_seq
          |FROM VALUES (named_struct(
          |  'inner_ts', TIMESTAMP '2026-06-29 10:00:00',
          |  'inner_seq', 2,
          |  'tag', 'a')) AS t(k)
          |ASOF JOIN (
          |  SELECT * FROM VALUES
          |    (named_struct(
          |      'inner_ts', TIMESTAMP '2026-06-29 09:00:00',
          |      'inner_seq', 1,
          |      'tag', 'a')),
          |    (named_struct(
          |      'inner_ts', TIMESTAMP '2026-06-29 10:00:00',
          |      'inner_seq', 1,
          |      'tag', 'a')) AS r(k)
          |) r
          |  MATCH_CONDITION (t.k >= r.k)
          |""".stripMargin),
      Row("a", 1) :: Nil)
  }

  test("nested whole STRUCT column MATCH_CONDITION") {
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW requests(req_ts, seq, service) AS
        |VALUES (TIMESTAMP '2026-06-29 10:00:00', 5, 'api'),
        |       (TIMESTAMP '2026-06-29 10:03:00', 1, 'api')
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW deploys(deploy_ts, seq, service, version) AS
        |VALUES (TIMESTAMP '2026-06-29 10:00:00', 1, 'api', 'v1.0'),
        |       (TIMESTAMP '2026-06-29 10:00:00', 3, 'api', 'v1.1')
        |""".stripMargin)
    checkSortMergeAsOf(
      sql(
        """
          |SELECT r.req_ts, r.seq, d.version
          |FROM (
          |  SELECT named_struct('ts', req_ts, 'seq', seq) AS k, service, req_ts, seq
          |  FROM requests
          |) r
          |ASOF JOIN (
          |  SELECT named_struct('ts', deploy_ts, 'seq', seq) AS k, service, version, deploy_ts, seq
          |  FROM deploys
          |) d
          |  MATCH_CONDITION (r.k >= d.k)
          |  ON r.service = d.service
          |ORDER BY r.req_ts, r.seq
          |""".stripMargin),
      Seq(
        Row(Timestamp.valueOf("2026-06-29 10:00:00"), 5, "v1.1"),
        Row(Timestamp.valueOf("2026-06-29 10:03:00"), 1, "v1.1")))
  }

  test("ARRAY<INT> MATCH_CONDITION") {
    checkSortMergeAsOf(
      sql(
        """
          |SELECT r.a
          |FROM VALUES (ARRAY(1, 3)) AS t(a)
          |ASOF JOIN VALUES (ARRAY(1, 2)), (ARRAY(1, 4)) AS r(a)
          |MATCH_CONDITION (t.a >= r.a)
          |""".stripMargin),
      Row(Seq(1, 2)) :: Nil)
  }

  test("ARRAY<STRUCT> whole column MATCH_CONDITION") {
    checkSortMergeAsOf(
      sql(
        """
          |SELECT r.a
          |FROM VALUES (ARRAY(named_struct('seq', 1, 'val', 3))) AS t(a)
          |ASOF JOIN (
          |  SELECT * FROM VALUES
          |    (ARRAY(named_struct('seq', 1, 'val', 2))),
          |    (ARRAY(named_struct('seq', 1, 'val', 4))) AS r(a)
          |) r
          |  MATCH_CONDITION (t.a >= r.a)
          |""".stripMargin),
      Row(Seq(Row(1, 2))) :: Nil)
  }

  test("STRUCT tuple from scalar columns MATCH_CONDITION") {
    checkSortMergeAsOf(
      sql(
        """
          |SELECT r.c1, r.c2
          |FROM VALUES (1, 2) AS t(c1, c2)
          |ASOF JOIN VALUES (1, 1) AS r(c1, c2)
          |MATCH_CONDITION ((t.c1, t.c2) >= (r.c1, r.c2))
          |""".stripMargin),
      Row(1, 1) :: Nil)
  }

  test("MATCH_CONDITION operand arithmetic with ON") {
    setupTradeQuoteViews()
    checkSortMergeAsOf(
      sql(
        """
          |SELECT count(*) AS cnt
          |FROM trades t ASOF JOIN quotes q
          |  MATCH_CONDITION (t.trade_time >= date_trunc('hour', q.quote_time))
          |  ON t.symbol = q.symbol
          |""".stripMargin),
      Row(3L) :: Nil)
  }
}
