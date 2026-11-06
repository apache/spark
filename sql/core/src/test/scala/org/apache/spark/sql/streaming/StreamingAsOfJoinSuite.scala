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

package org.apache.spark.sql.streaming

import java.sql.Timestamp

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.internal.SQLConf

class StreamingAsOfJoinSuite extends StreamTest {

  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(SQLConf.SQL_ASOF_JOIN_ENABLED.key, "true")
  }

  override def afterAll(): Unit = {
    spark.conf.unset(SQLConf.SQL_ASOF_JOIN_ENABLED.key)
    super.afterAll()
  }

  private def timestamp(value: String): Timestamp = Timestamp.valueOf(value)

  private def withTradeQuoteViews(
      testCode: MemoryStream[(Timestamp, String, Int)] => Unit): Unit = {
    withTempView("streaming_trades", "static_quotes") {
      val input = MemoryStream[(Timestamp, String, Int)]
      input.toDF().toDF("trade_time", "symbol", "quantity")
        .createOrReplaceTempView("streaming_trades")
      sql(
        """
          |CREATE TEMP VIEW static_quotes(quote_time, symbol, bid_price) AS
          |VALUES (TIMESTAMP '2026-06-29 10:00:00', 'AAPL', 18010),
          |       (TIMESTAMP '2026-06-29 10:00:07', 'AAPL', 18015),
          |       (TIMESTAMP '2026-06-29 10:00:08', 'MSFT', 42050)
          |""".stripMargin)
      testCode(input)
    }
  }

  test("stream-static ASOF join matches against the static snapshot") {
    withTradeQuoteViews { input =>
      val joined = sql(
        """
          |SELECT t.trade_time, t.symbol, t.quantity, q.bid_price
          |FROM streaming_trades t ASOF JOIN static_quotes q
          |  MATCH_CONDITION (t.trade_time >= q.quote_time)
          |  ON t.symbol = q.symbol
          |""".stripMargin)

      testStream(joined)(
        AddData(input,
          (timestamp("2026-06-29 10:00:05"), "AAPL", 100),
          (timestamp("2026-06-29 10:00:09"), "MSFT", 50)),
        CheckNewAnswer(
          (timestamp("2026-06-29 10:00:05"), "AAPL", 100, 18010),
          (timestamp("2026-06-29 10:00:09"), "MSFT", 50, 42050)),
        AddData(input, (timestamp("2026-06-29 10:00:11"), "AAPL", 200)),
        CheckNewAnswer((timestamp("2026-06-29 10:00:11"), "AAPL", 200, 18015)))
    }
  }

  test("stream-static LEFT ASOF join preserves unmatched streaming rows") {
    withTradeQuoteViews { input =>
      val joined = sql(
        """
          |SELECT t.trade_time, t.symbol, t.quantity, q.bid_price
          |FROM streaming_trades t LEFT ASOF JOIN static_quotes q
          |  MATCH_CONDITION (t.trade_time >= q.quote_time)
          |  ON t.symbol = q.symbol
          |""".stripMargin)

      testStream(joined)(
        AddData(input,
          (timestamp("2026-06-29 09:59:59"), "AAPL", 30),
          (timestamp("2026-06-29 10:00:09"), "GOOG", 40),
          (timestamp("2026-06-29 10:00:11"), "AAPL", 200)),
        CheckNewAnswer(
          Row(timestamp("2026-06-29 09:59:59"), "AAPL", 30, null),
          Row(timestamp("2026-06-29 10:00:09"), "GOOG", 40, null),
          Row(timestamp("2026-06-29 10:00:11"), "AAPL", 200, 18015)))
    }
  }

  Seq("INNER", "LEFT").foreach { joinType =>
    Seq(false, true).foreach { isLeftStreaming =>
      val inputType = if (isLeftStreaming) "stream-stream" else "static-stream"

      test(s"$inputType $joinType ASOF join is not supported") {
        withTempView("trades", "quotes") {
          val quotesInput = MemoryStream[(Timestamp, String, Int)]
          quotesInput.toDF().toDF("quote_time", "symbol", "bid_price")
            .createOrReplaceTempView("quotes")

          if (isLeftStreaming) {
            val tradesInput = MemoryStream[(Timestamp, String, Int)]
            tradesInput.toDF().toDF("trade_time", "symbol", "quantity")
              .createOrReplaceTempView("trades")
          } else {
            Seq((timestamp("2026-06-29 10:00:05"), "AAPL", 100))
              .toDF("trade_time", "symbol", "quantity")
              .createOrReplaceTempView("trades")
          }

          val joined = sql(
            s"""
               |SELECT t.trade_time, t.symbol, t.quantity, q.bid_price
               |FROM trades t $joinType ASOF JOIN quotes q
               |  MATCH_CONDITION (t.trade_time >= q.quote_time)
               |  ON t.symbol = q.symbol
               |""".stripMargin)
          val error = intercept[AnalysisException] {
            joined.writeStream.format("noop").start()
          }
          assert(error.getMessage.contains(
            "ASOF join with a streaming DataFrame/Dataset on the right is not supported"))
        }
      }
    }
  }
}
