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

import java.io.File
import java.sql.Timestamp

import org.apache.spark.sql.{classic, AnalysisException, DataFrame, Row}
import org.apache.spark.sql.catalyst.plans.logical.AsOfJoin
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

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

  Seq("inner", "left").foreach { joinType =>
    test(s"SPARK-62248: stream-static DataFrame $joinType as-of join") {
      withTradeQuoteViews { input =>
        val trades = spark.table("streaming_trades")
        val quotes = spark.table("static_quotes")
        val joined = trades.joinAsOf(
          quotes, trades("trade_time"), quotes("quote_time"),
          joinExprs = trades("symbol") === quotes("symbol"), joinType = joinType,
          tolerance = null, allowExactMatches = true, direction = "backward"
        ).select(trades("trade_time"), trades("symbol"), trades("quantity"), quotes("bid_price"))
        val unmatched = Row(timestamp("2026-06-29 09:59:59"), "AAPL", 30, null)

        testStream(joined)(
          AddData(input,
            (timestamp("2026-06-29 10:00:05"), "AAPL", 100),
            (timestamp("2026-06-29 10:00:09"), "MSFT", 50),
            (timestamp("2026-06-29 09:59:59"), "AAPL", 30)),
          CheckNewAnswer(Seq(
            Row(timestamp("2026-06-29 10:00:05"), "AAPL", 100, 18010),
            Row(timestamp("2026-06-29 10:00:09"), "MSFT", 50, 42050)) ++
            (if (joinType == "left") Seq(unmatched) else Nil): _*))
      }
    }
  }

  test("SPARK-62248: legacy conf rewrites a stream-static DataFrame as-of join to a subquery") {
    withTradeQuoteViews { _ =>
      Seq(false, true).foreach { legacy =>
        withSQLConf(
            SQLConf.LEGACY_AS_OF_JOIN_REWRITE_STREAMING_LEFT_TO_SUBQUERY.key -> legacy.toString) {
          val trades = spark.table("streaming_trades")
          val quotes = spark.table("static_quotes")
          val joined = trades.joinAsOf(
            quotes, trades("trade_time"), quotes("quote_time"),
            joinExprs = trades("symbol") === quotes("symbol"), joinType = "inner",
            tolerance = null, allowExactMatches = true, direction = "backward")
          val asOfJoin = joined.queryExecution.analyzed.collectFirst { case j: AsOfJoin => j }
          assert(asOfJoin.get.requiresSortMergeAsOfJoin == !legacy)
        }
      }
    }
  }

  private val keepBothUsingColumns = SQLConf.LEGACY_DATAFRAME_AS_OF_JOIN_KEEP_BOTH_USING_COLUMNS.key

  // A stream-static LEFT join of trades with quotes on sym.
  private def joinTradesWithQuotes(input: MemoryStream[(Int, String, Int)]): DataFrame = {
    val trades = input.toDF().toDF("ts", "sym", "qty").asInstanceOf[classic.DataFrame]
    val quotes = Seq((2, "A", 100), (4, "A", 200)).toDF("ts", "sym", "bid")
      .asInstanceOf[classic.DataFrame]
    trades.joinAsOf(
      quotes, trades.col("ts"), quotes.col("ts"), usingColumns = Seq("sym"),
      joinType = "left", tolerance = null, allowExactMatches = true, direction = "backward")
  }

  // Unlike a batch join, a streaming join keeps the key column of both sides on purpose: the
  // state of a stateful operator above it then keeps its schema across the upgrade.
  test("stream-static DataFrame LEFT ASOF join with using columns keeps both key columns") {
    // The legacy flag is for batch joins only.
    Seq("false", "true").foreach { legacy =>
      withSQLConf(keepBothUsingColumns -> legacy) {
        val input = MemoryStream[(Int, String, Int)]
        val joined = joinTradesWithQuotes(input)
        assert(joined.columns.toSeq == Seq("ts", "sym", "qty", "ts", "sym", "bid"))
        testStream(joined)(
          AddData(input, (5, "A", 1), (5, "B", 2)),
          CheckNewAnswer(Row(5, "A", 1, 4, "A", 200), Row(5, "B", 2, null, null, null)))
      }
    }
    // A streaming right side keeps both key columns too.
    val quotes = MemoryStream[(Int, String, Int)].toDF().toDF("ts", "sym", "bid")
      .asInstanceOf[classic.DataFrame]
    val trades = Seq((5, "A", 1)).toDF("ts", "sym", "qty").asInstanceOf[classic.DataFrame]
    val staticStream = trades.joinAsOf(
      quotes, trades.col("ts"), quotes.col("ts"), usingColumns = Seq("sym"),
      joinType = "inner", tolerance = null, allowExactMatches = true, direction = "backward")
    assert(staticStream.columns.toSeq == Seq("ts", "sym", "qty", "ts", "sym", "bid"))
  }

  // Restarts `query` from a copy of a checkpoint resource and checks that the state is used.
  // Spark 4.3.0 wrote these checkpoints with 5 shuffle partitions and the HDFS state store, in
  // two batches: (5, A, 1), (5, B, 2), then (5, A, 1), (6, A, 3). It ran the stream-static join
  // with spark.sql.join.sortMergeAsOfJoin.enabled=true, since it rejects the query without it.
  private def checkRestartOfStreamStaticJoin(
      checkpointResource: String,
      query: DataFrame => DataFrame,
      expected: Row): Unit = {
    val resource = getClass.getResource(s"/structured-streaming/$checkpointResource/").toURI
    withTempDir { checkpoint =>
      // Copy the resource, so the test does not change it.
      Utils.copyDirectory(new File(resource), checkpoint)
      val input = MemoryStream[(Int, String, Int)]
      input.addData((5, "A", 1), (5, "B", 2))
      input.addData((5, "A", 1), (6, "A", 3))
      testStream(query(joinTradesWithQuotes(input)))(
        StartStream(checkpointLocation = checkpoint.getCanonicalPath),
        // The restored state drops (6, A, 3), seen in batch 1, so only the new row comes out.
        AddData(input, (6, "A", 3), (7, "A", 9)),
        CheckNewAnswer(expected),
        StopStream)
    }
  }

  test("stream-static DataFrame ASOF join with using columns, then dropDuplicates(), " +
      "restarts from a Spark 4.3.0 checkpoint") {
    // Its state keys are ts, sym, qty, ts, sym, bid.
    checkRestartOfStreamStaticJoin(
      "checkpoint-version-4.3.0-asof-join-using-columns-dedup",
      _.dropDuplicates(),
      Row(7, "A", 9, 4, "A", 200))
  }

  test("stream-static DataFrame ASOF join with using columns, then withColumn and " +
      "dropDuplicates(), restarts from a Spark 4.3.0 checkpoint") {
    // Its state keys are ts, sym, qty, ts, sym, bid, x.
    checkRestartOfStreamStaticJoin(
      "checkpoint-version-4.3.0-asof-join-using-columns-with-column-dedup",
      _.withColumn("x", lit(1)).dropDuplicates(),
      Row(7, "A", 9, 4, "A", 200, 1))
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
