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
import org.apache.spark.sql.test.SharedSparkSession

class AsOfJoinSQLSuite extends QueryTest with SharedSparkSession {

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

  test("INNER ASOF JOIN with ON") {
    setupTradeQuoteViews()
    checkAnswer(
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
    checkAnswer(
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
    checkAnswer(
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

  test("first-following match with <=") {
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
    checkAnswer(
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

  test("equality operator is rejected in MATCH_CONDITION") {
    val sqlText =
      """
        |SELECT * FROM trades t
        |ASOF JOIN quotes q
        |  MATCH_CONDITION (t.trade_time = q.quote_time)
        |  ON t.symbol = q.symbol
        |""".stripMargin
    checkError(
      exception = intercept[ParseException](sql(sqlText)),
      condition = "PARSE_SYNTAX_ERROR",
      sqlState = "42601",
      parameters = Map("error" -> "')'", "hint" -> ""))
  }
}
