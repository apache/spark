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

import org.apache.spark.SparkUnsupportedOperationException
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

  // A batch LEFT join of one trade with the quote before it, on sym. Its output is
  // sym, ts, qty, ts, bid, or ts, sym, qty, ts, sym, bid with both key columns.
  private def batchTradeWithQuote(singlePass: Boolean = false): classic.DataFrame = {
    withSQLConf(
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString,
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
        SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false",
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_ASOF_JOIN_RESOLUTION.key -> "true") {
      val trades = Seq((5, "A", 1)).toDF("ts", "sym", "qty").asInstanceOf[classic.DataFrame]
      val quotes = Seq((2, "A", 100)).toDF("ts", "sym", "bid").asInstanceOf[classic.DataFrame]
      trades.joinAsOf(
        quotes, trades.col("ts"), quotes.col("ts"), usingColumns = Seq("sym"),
        joinType = "left", tolerance = null, allowExactMatches = true, direction = "backward")
    }
  }

  // Each value of the stream with the batch join, then dropDuplicates() on all columns.
  private def dedupWithBatchJoin(input: MemoryStream[Int], join: DataFrame): DataFrame =
    input.toDF().crossJoin(join).dropDuplicates()

  private def assertKeepBothUsingColumnsInOffsetLog(expected: String): StreamAction =
    Execute { q =>
      val metadata = q.offsetLog.getLatest().get._2.metadataOpt.get
      assert(metadata.conf.get(keepBothUsingColumns).contains(expected))
    }

  private val batchJoinInStreamCheckpoint =
    "checkpoint-version-4.3.0-batch-asof-join-using-columns-in-stream-dedup"

  // Copies a checkpoint resource to `checkpoint`, so the test does not change it.
  private def copyCheckpoint(resourceName: String, checkpoint: File): Unit = {
    val resource = getClass.getResource(s"/structured-streaming/$resourceName/").toURI
    Utils.copyDirectory(new File(resource), checkpoint)
  }

  // Spark 4.3.0 wrote this checkpoint with default configs, 5 shuffle partitions and the HDFS
  // state store, in two batches: 1, 2, then 1, 3. Its state keys are value, ts, sym, qty, ts,
  // sym, bid. Its offset log has no keepBothUsingColumns, so the restart reads true, and the
  // state gets both key columns back. The output keeps the columns of the DataFrame. Fails with
  // STATE_STORE_KEY_SCHEMA_NOT_COMPATIBLE if the state keys have one key column, and the offset
  // log check fails if the next restart would not read true again.
  Seq(false, true).foreach { singlePass =>
    test("batch DataFrame ASOF join with using columns in a stream, then dropDuplicates(), " +
        s"restarts from a Spark 4.3.0 checkpoint, single-pass analyzer $singlePass") {
      withTempDir { checkpoint =>
        copyCheckpoint(batchJoinInStreamCheckpoint, checkpoint)
        val input = MemoryStream[Int]
        input.addData(1, 2)
        input.addData(1, 3)
        val join = batchTradeWithQuote(singlePass)
        assert(join.columns.toSeq == Seq("sym", "ts", "qty", "ts", "bid"))
        testStream(dedupWithBatchJoin(input, join))(
          StartStream(checkpointLocation = checkpoint.getCanonicalPath),
          // The restored state drops 3, seen in batch 1, so only 4 comes out.
          AddData(input, 3, 4),
          CheckNewAnswer(Row(4, "A", 5, 1, 2, 100)),
          assertKeepBothUsingColumnsInOffsetLog("true"),
          StopStream)
      }
    }
  }

  // Fails if a new query gets both key columns back, or if the restart reads the session value
  // instead of the value in the offset log: then the state keys change and the restart fails.
  test("batch DataFrame ASOF join with using columns in a stream returns one key column " +
      "for a new checkpoint, also after a restart with the legacy config") {
    withTempDir { checkpoint =>
      val input = MemoryStream[Int]
      val df = dedupWithBatchJoin(input, batchTradeWithQuote())
      testStream(df)(
        StartStream(checkpointLocation = checkpoint.getCanonicalPath),
        AddData(input, 1, 2),
        CheckNewAnswer(Row(1, "A", 5, 1, 2, 100), Row(2, "A", 5, 1, 2, 100)),
        assertKeepBothUsingColumnsInOffsetLog("false"),
        StopStream)
      withSQLConf(keepBothUsingColumns -> "true") {
        testStream(df)(
          StartStream(checkpointLocation = checkpoint.getCanonicalPath),
          AddData(input, 1, 3),
          CheckNewAnswer(Row(3, "A", 5, 1, 2, 100)),
          assertKeepBothUsingColumnsInOffsetLog("false"),
          StopStream)
      }
    }
  }

  // The DataFrame is built without the legacy config, so only the query start gives the state
  // both key columns. The last restart, with a DataFrame that has both key columns itself, needs
  // that state. Fails if the start does not give the state both key columns, or if the session
  // value at the start is not pinned, so that a restart without the config gives it one.
  test("batch DataFrame ASOF join with using columns in a stream keeps both key columns " +
      "when the query starts with the legacy config") {
    withTempDir { checkpoint =>
      val input = MemoryStream[Int]
      val df = dedupWithBatchJoin(input, batchTradeWithQuote())
      withSQLConf(keepBothUsingColumns -> "true") {
        testStream(df)(
          StartStream(checkpointLocation = checkpoint.getCanonicalPath),
          AddData(input, 1, 2),
          CheckNewAnswer(Row(1, "A", 5, 1, 2, 100), Row(2, "A", 5, 1, 2, 100)),
          assertKeepBothUsingColumnsInOffsetLog("true"),
          StopStream)
      }
      testStream(df)(
        StartStream(checkpointLocation = checkpoint.getCanonicalPath),
        AddData(input, 1, 3),
        CheckNewAnswer(Row(3, "A", 5, 1, 2, 100)),
        assertKeepBothUsingColumnsInOffsetLog("true"),
        StopStream)
      val bothKeys = withSQLConf(keepBothUsingColumns -> "true") {
        dedupWithBatchJoin(input, batchTradeWithQuote())
      }
      testStream(bothKeys)(
        StartStream(checkpointLocation = checkpoint.getCanonicalPath),
        AddData(input, 3, 4),
        CheckNewAnswer(Row(4, 5, "A", 1, 2, "A", 100)),
        StopStream)
    }
  }

  // A positional rename written for the old output fails when the DataFrame is built, before
  // the query starts, so the restart cannot help with it. Fails if a join with a streaming side
  // stops taking the old names, if the batch join takes them without the legacy config, or if
  // the query does not restart from the Spark 4.3.0 checkpoint with the config set.
  test("positional toDF rename for the old output after a DataFrame ASOF join with using " +
      "columns in a stream") {
    val names = Seq("ts", "sym", "qty", "ts2", "sym2", "bid")
    // A join with a streaming side keeps both key columns, so the old names fit.
    assert(joinTradesWithQuotes(MemoryStream[(Int, String, Int)]).toDF(names: _*).columns.toSeq ==
      names)
    val e = intercept[AnalysisException] {
      batchTradeWithQuote().toDF(names: _*)
    }
    assert(e.getCondition == "ASSIGNMENT_ARITY_MISMATCH")
    withTempDir { checkpoint =>
      copyCheckpoint(batchJoinInStreamCheckpoint, checkpoint)
      val input = MemoryStream[Int]
      input.addData(1, 2)
      input.addData(1, 3)
      val join = withSQLConf(keepBothUsingColumns -> "true") {
        batchTradeWithQuote().toDF(names: _*)
      }
      testStream(dedupWithBatchJoin(input, join))(
        StartStream(checkpointLocation = checkpoint.getCanonicalPath),
        AddData(input, 3, 4),
        CheckNewAnswer(Row(4, 5, "A", 1, 2, "A", 100)),
        StopStream)
    }
  }

  // Two chained batch joins on column names. The last restart, with a DataFrame where both joins
  // have both key columns themselves, needs a state where both have them. Fails if the start
  // gives both key columns back to only one of the joins.
  test("chained batch DataFrame ASOF joins with using columns in a stream keep both key " +
      "columns when the query starts with the legacy config") {
    def chain(): DataFrame = {
      val trades = Seq((5, "A", "X", 1)).toDF("ts", "sym", "ex", "qty")
        .asInstanceOf[classic.DataFrame]
      val quotes = Seq((2, "A", 100)).toDF("ts", "sym", "bid").asInstanceOf[classic.DataFrame]
      val venues = Seq((1, "X", 7)).toDF("vts", "ex", "fee").asInstanceOf[classic.DataFrame]
      val first = trades.joinAsOf(
        quotes, trades.col("ts"), quotes.col("ts"), usingColumns = Seq("sym"),
        joinType = "left", tolerance = null, allowExactMatches = true, direction = "backward")
      first.joinAsOf(
        venues, first.col("qty"), venues.col("vts"), usingColumns = Seq("ex"),
        joinType = "left", tolerance = null, allowExactMatches = true, direction = "backward")
    }
    withTempDir { checkpoint =>
      val input = MemoryStream[Int]
      val df = dedupWithBatchJoin(input, chain())
      withSQLConf(keepBothUsingColumns -> "true") {
        testStream(df)(
          StartStream(checkpointLocation = checkpoint.getCanonicalPath),
          AddData(input, 1, 2),
          CheckNewAnswer(
            Row(1, "X", "A", 5, 1, 2, 100, 1, 7), Row(2, "X", "A", 5, 1, 2, 100, 1, 7)),
          StopStream)
      }
      val bothKeys = withSQLConf(keepBothUsingColumns -> "true") {
        dedupWithBatchJoin(input, chain())
      }
      testStream(bothKeys)(
        StartStream(checkpointLocation = checkpoint.getCanonicalPath),
        AddData(input, 1, 3),
        CheckNewAnswer(Row(3, 5, "A", "X", 1, 2, "A", 100, 1, "X", 7)),
        StopStream)
    }
  }

  // A union of the join with a plan of one-key width cannot get both key columns back: the two
  // sides of the union would then differ in width. The state of this query has no column of the
  // join, so it needs no restore. Fails if the query does not start, or loses rows.
  test("batch DataFrame ASOF join with using columns in a union with a plan of one-key width " +
      "in a stream starts with the legacy config") {
    withTempDir { checkpoint =>
      val input = MemoryStream[Int]
      val sameWidth = Seq(("B", 1, 1, 1, 1)).toDF("a", "b", "c", "d", "e")
      val df = input.toDF().crossJoin(batchTradeWithQuote().union(sameWidth))
        .groupBy("value").count()
      withSQLConf(keepBothUsingColumns -> "true") {
        testStream(df, OutputMode.Update())(
          StartStream(checkpointLocation = checkpoint.getCanonicalPath),
          AddData(input, 1),
          CheckNewAnswer(Row(1, 2)),
          StopStream)
      }
    }
  }

  // Two joins on different names give their key columns in different orders, so the union
  // pairs other columns with both key columns than with one. Fails if the restart changes which
  // values the union puts under each name: then the two rows become one.
  test("union of batch DataFrame ASOF joins with different using columns in a stream keeps " +
      "its values with the legacy config") {
    val t = Seq((5, "A", "X", 1)).toDF("ts", "sym", "ex", "qty").asInstanceOf[classic.DataFrame]
    val q = Seq((2, "A", "X", 100)).toDF("ts", "sym", "ex", "bid")
      .asInstanceOf[classic.DataFrame]
    def join(key: String): DataFrame = t.joinAsOf(
      q, t.col("ts"), q.col("ts"), usingColumns = Seq(key), joinType = "left",
      tolerance = null, allowExactMatches = true, direction = "backward")
    val union = join("sym").union(join("ex"))
    assert(union.columns.toSeq == Seq("sym", "ts", "ex", "qty", "ts", "ex", "bid"))
    withTempDir { checkpoint =>
      val input = MemoryStream[Int]
      withSQLConf(keepBothUsingColumns -> "true") {
        testStream(dedupWithBatchJoin(input, union))(
          StartStream(checkpointLocation = checkpoint.getCanonicalPath),
          AddData(input, 1),
          CheckNewAnswer(Row(1, "A", 5, "X", 1, 2, "X", 100), Row(1, "X", 5, "A", 1, 2, "A", 100)),
          StopStream)
      }
    }
  }

  // A CTE reference keeps the columns that its body had when it was resolved, and the optimizer
  // matches them to the body by position. Fails if the body gets both key columns: the values
  // then move to other columns or fail to cast.
  test("batch DataFrame ASOF join with using columns in a CTE of a streaming SQL query keeps " +
      "its values with the legacy config") {
    withTempView("asof_batch_join", "asof_stream") {
      batchTradeWithQuote().createOrReplaceTempView("asof_batch_join")
      val input = MemoryStream[Int]
      input.toDF().createOrReplaceTempView("asof_stream")
      val df = sql(
        "WITH c AS (TABLE asof_batch_join) SELECT * FROM asof_stream CROSS JOIN c")
        .dropDuplicates()
      withTempDir { checkpoint =>
        withSQLConf(keepBothUsingColumns -> "true") {
          testStream(df)(
            StartStream(checkpointLocation = checkpoint.getCanonicalPath),
            AddData(input, 1),
            CheckNewAnswer(Row(1, "A", 5, 1, 2, 100)),
            StopStream)
        }
      }
    }
  }

  // dropDuplicates("sym") keys on every column named sym. The first run builds the DataFrame with
  // the legacy config, so its state has the key columns of both sides, as Spark 4.3 writes it.
  // The restart builds it without the config. Fails with STATE_STORE_KEY_SCHEMA_NOT_COMPATIBLE if
  // the restart keys on one sym column only.
  test("batch DataFrame ASOF join with using columns in a stream, then dropDuplicates(sym), " +
      "restarts from a checkpoint with both key columns") {
    withTempDir { checkpoint =>
      val input = MemoryStream[Int]
      def query(): DataFrame = input.toDF().crossJoin(batchTradeWithQuote()).dropDuplicates("sym")
      val bothKeys = withSQLConf(keepBothUsingColumns -> "true") { query() }
      withSQLConf(keepBothUsingColumns -> "true") {
        testStream(bothKeys)(
          StartStream(checkpointLocation = checkpoint.getCanonicalPath),
          AddData(input, 1),
          CheckNewAnswer(Row(1, 5, "A", 1, 2, "A", 100)),
          StopStream)
      }
      testStream(query())(
        StartStream(checkpointLocation = checkpoint.getCanonicalPath),
        // The restored state has sym A, so the new row is a duplicate.
        AddData(input, 2),
        CheckNewAnswer(),
        assertKeepBothUsingColumnsInOffsetLog("true"),
        StopStream)
    }
  }

  // The legacy config only gives the old output to a query whose checkpoint has it. A new
  // checkpoint keeps one key column, so a DataFrame built with the config, which has both key
  // columns itself, does not fit its state. Fails if this is no longer so: then the config
  // doc and the migration guide need a change.
  test("batch DataFrame ASOF join with using columns in a stream: a new checkpoint does not " +
      "take a DataFrame built with the legacy config") {
    withTempDir { checkpoint =>
      val input = MemoryStream[Int]
      testStream(dedupWithBatchJoin(input, batchTradeWithQuote()))(
        StartStream(checkpointLocation = checkpoint.getCanonicalPath),
        AddData(input, 1, 2),
        CheckNewAnswer(Row(1, "A", 5, 1, 2, 100), Row(2, "A", 5, 1, 2, 100)),
        StopStream)
      val bothKeys = withSQLConf(keepBothUsingColumns -> "true") {
        dedupWithBatchJoin(input, batchTradeWithQuote())
      }
      val e = intercept[StreamingQueryException] {
        testStream(bothKeys)(
          StartStream(checkpointLocation = checkpoint.getCanonicalPath),
          AddData(input, 1, 3),
          CheckNewAnswer(Row(3, 5, "A", 1, 2, "A", 100)))
      }
      checkError(
        e.getCause.asInstanceOf[SparkUnsupportedOperationException],
        condition = "STATE_STORE_KEY_SCHEMA_NOT_COMPATIBLE",
        parameters = Map("storedKeySchema" -> ".*", "newKeySchema" -> ".*"),
        matchPVals = true)
    }
  }

  // withColumn lists the one-key output of the join, so the restart cannot give its state both
  // key columns back. Fails if this case does not fail loudly, or if the advice in the migration
  // guide (set the config before building the DataFrame) does not make the query restart.
  test("batch DataFrame ASOF join with using columns in a stream, then withColumn and " +
      "dropDuplicates(), needs the legacy config when the DataFrame is built") {
    withTempDir { checkpoint =>
      val input = MemoryStream[Int]
      def query(): DataFrame =
        input.toDF().crossJoin(batchTradeWithQuote()).withColumn("x", lit(1)).dropDuplicates()
      // Writes state keys with both key columns, as Spark 4.3 does.
      withSQLConf(keepBothUsingColumns -> "true") {
        testStream(query())(
          StartStream(checkpointLocation = checkpoint.getCanonicalPath),
          AddData(input, 1, 2),
          CheckNewAnswer(Row(1, 5, "A", 1, 2, "A", 100, 1), Row(2, 5, "A", 1, 2, "A", 100, 1)),
          StopStream)
      }
      val e = intercept[StreamingQueryException] {
        testStream(query())(
          StartStream(checkpointLocation = checkpoint.getCanonicalPath),
          AddData(input, 1, 3),
          CheckNewAnswer(Row(3, 5, "A", 1, 2, "A", 100, 1)))
      }
      checkError(
        e.getCause.asInstanceOf[SparkUnsupportedOperationException],
        condition = "STATE_STORE_KEY_SCHEMA_NOT_COMPATIBLE",
        parameters = Map("storedKeySchema" -> ".*", "newKeySchema" -> ".*"),
        matchPVals = true)
      val withConfig = withSQLConf(keepBothUsingColumns -> "true") { query() }
      testStream(withConfig)(
        StartStream(checkpointLocation = checkpoint.getCanonicalPath),
        AddData(input, 4),
        CheckNewAnswer(Row(3, 5, "A", 1, 2, "A", 100, 1), Row(4, 5, "A", 1, 2, "A", 100, 1)),
        StopStream)
    }
  }

  // foreachBatch gets the columns of the DataFrame, also when the state has both key columns.
  // Its session has the value of the legacy config in the checkpoint, so a DataFrame join on
  // column names that it runs returns both key columns for a Spark 4.3.0 checkpoint, as in Spark
  // 4.3, and one for a new checkpoint. Fails with UNSUPPORTED_DESERIALIZER if foreachBatch gets
  // more columns than the DataFrame has, and fails if the session value changes.
  test("foreachBatch of a query with a batch DataFrame ASOF join with using columns sees " +
      "the value of the legacy config in the checkpoint") {
    def run(
        checkpoint: File,
        input: MemoryStream[Int],
        newValues: Int*): (Seq[String], String, Seq[String]) = {
      var seen: Option[(Seq[String], String, Seq[String])] = None
      val query = dedupWithBatchJoin(input, batchTradeWithQuote()).writeStream
        .option("checkpointLocation", checkpoint.getCanonicalPath)
        .foreachBatch { (batch: DataFrame, _: Long) =>
          // Reads every partition, so the state store of each one commits.
          batch.collect()
          val session = batch.sparkSession
          val trades = session.range(1).selectExpr("5 AS ts", "'A' AS sym", "1 AS qty")
            .asInstanceOf[classic.DataFrame]
          val quotes = session.range(1).selectExpr("2 AS ts", "'A' AS sym", "100 AS bid")
            .asInstanceOf[classic.DataFrame]
          val join = trades.joinAsOf(
            quotes, trades.col("ts"), quotes.col("ts"), usingColumns = Seq("sym"),
            joinType = "left", tolerance = null, allowExactMatches = true,
            direction = "backward")
          seen = Some(
            (batch.columns.toSeq, session.conf.get(keepBothUsingColumns), join.columns.toSeq))
        }
        .start()
      try {
        input.addData(newValues: _*)
        query.processAllAvailable()
      } finally {
        query.stop()
      }
      seen.get
    }
    withTempDir { checkpoint =>
      copyCheckpoint(batchJoinInStreamCheckpoint, checkpoint)
      val input = MemoryStream[Int]
      input.addData(1, 2)
      input.addData(1, 3)
      assert(run(checkpoint, input, 3, 4) == (
        Seq("value", "sym", "ts", "qty", "ts", "bid"),
        "true",
        Seq("ts", "sym", "qty", "ts", "sym", "bid")))
    }
    withTempDir { checkpoint =>
      assert(run(checkpoint, MemoryStream[Int], 1, 2) == (
        Seq("value", "sym", "ts", "qty", "ts", "bid"),
        "false",
        Seq("sym", "ts", "qty", "ts", "bid")))
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
