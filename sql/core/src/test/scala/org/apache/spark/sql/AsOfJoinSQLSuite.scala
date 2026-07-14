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
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * SQL ASOF JOIN surface tests (parser and feature gating).
 * Execution semantics and complex MATCH_CONDITION types are covered by
 * [[AsOfJoinSortMergeSQLSuite]], which requires sort-merge ASOF join.
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
}
