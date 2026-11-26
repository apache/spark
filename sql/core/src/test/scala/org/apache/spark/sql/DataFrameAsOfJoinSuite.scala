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

import java.time.Duration

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.plans.AsOfJoinDirection
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.tags.SlowSQLTest

@SlowSQLTest
class DataFrameAsOfJoinSuite extends SharedSparkSession
  with AdaptiveSparkPlanHelper {

  import testImplicits._

  def prepareForAsOfJoin(): (classic.DataFrame, classic.DataFrame) = {
    val schema1 = StructType(
      StructField("a", IntegerType, false) ::
        StructField("b", StringType, false) ::
        StructField("left_val", StringType, false) :: Nil)
    val rowSeq1: List[Row] = List(Row(1, "x", "a"), Row(5, "y", "b"), Row(10, "z", "c"))
    val df1 = spark.createDataFrame(rowSeq1.asJava, schema1)

    val schema2 = StructType(
      StructField("a", IntegerType) ::
        StructField("b", StringType) ::
        StructField("right_val", IntegerType) :: Nil)
    val rowSeq2: List[Row] = List(Row(1, "v", 1), Row(2, "w", 2), Row(3, "x", 3),
      Row(6, "y", 6), Row(7, "z", 7))
    val df2 = spark.createDataFrame(rowSeq2.asJava, schema2)

    (df1, df2)
  }

  test("as-of join - simple") {
    val (df1, df2) = prepareForAsOfJoin()
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("a"), df2.col("a"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null, allowExactMatches = true, direction = "backward"),
      Seq(
        Row(1, "x", "a", 1, "v", 1),
        Row(5, "y", "b", 3, "x", 3),
        Row(10, "z", "c", 7, "z", 7)
      )
    )
  }

  test("as-of join - usingColumns") {
    val (df1, df2) = prepareForAsOfJoin()
    checkAnswer(
      df1.joinAsOf(df2, df1.col("a"), df2.col("a"), usingColumns = Seq("b"),
        joinType = "inner", tolerance = null, allowExactMatches = true, direction = "backward"),
      Seq(
        Row("z", 10, "c", 7, 7)
      )
    )
  }

  test("as-of join - usingColumns, left outer") {
    val (df1, df2) = prepareForAsOfJoin()
    checkAnswer(
      df1.joinAsOf(df2, df1.col("a"), df2.col("a"), usingColumns = Seq("b"),
        joinType = "left", tolerance = null, allowExactMatches = true, direction = "backward"),
      Seq(
        Row("x", 1, "a", null, null),
        Row("y", 5, "b", null, null),
        Row("z", 10, "c", 7, 7)
      )
    )
  }

  test("as-of join - tolerance = 1") {
    val (df1, df2) = prepareForAsOfJoin()
    checkAnswer(
      df1.joinAsOf(df2, df1.col("a"), df2.col("a"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = lit(1), allowExactMatches = true, direction = "backward"),
      Seq(
        Row(1, "x", "a", 1, "v", 1)
      )
    )
  }

  test("as-of join - tolerance should be a constant") {
    val (df1, df2) = prepareForAsOfJoin()
    checkError(
      exception = intercept[AnalysisException] {
        df1.joinAsOf(
          df2, df1.col("a"), df2.col("a"), usingColumns = Seq.empty,
          joinType = "inner", tolerance = df1.col("b"), allowExactMatches = true,
          direction = "backward")
      },
      condition = "AS_OF_JOIN.TOLERANCE_IS_UNFOLDABLE",
      parameters = Map.empty)
  }

  test("as-of join - tolerance should be non-negative") {
    val (df1, df2) = prepareForAsOfJoin()
    checkError(
      exception = intercept[AnalysisException] {
        df1.joinAsOf(df2, df1.col("a"), df2.col("a"), usingColumns = Seq.empty,
          joinType = "inner", tolerance = lit(-1), allowExactMatches = true,
          direction = "backward")
      },
      condition = "AS_OF_JOIN.TOLERANCE_IS_NON_NEGATIVE",
      parameters = Map.empty)
  }

  test("as-of join - unsupported direction") {
    val (df1, df2) = prepareForAsOfJoin()
    val direction = "unknown"
    checkError(
      exception = intercept[AnalysisException] {
        df1.joinAsOf(df2, df1.col("a"), df2.col("a"), usingColumns = Seq.empty,
          joinType = "inner", tolerance = lit(-1), allowExactMatches = true,
          direction = direction)
      },
      condition = "AS_OF_JOIN.UNSUPPORTED_DIRECTION",
      sqlState = "42604",
      parameters = Map(
        "direction" -> direction,
        "supported" -> AsOfJoinDirection.supported.mkString("'", "', '", "'")
      )
    )
  }

  test("as-of join - allowExactMatches = false") {
    val (df1, df2) = prepareForAsOfJoin()
    checkAnswer(
      df1.joinAsOf(df2, df1.col("a"), df2.col("a"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null, allowExactMatches = false, direction = "backward"),
      Seq(
        Row(5, "y", "b", 3, "x", 3),
        Row(10, "z", "c", 7, "z", 7)
      )
    )
  }

  test("as-of join - direction = \"forward\"") {
    val (df1, df2) = prepareForAsOfJoin()
    checkAnswer(
      df1.joinAsOf(df2, df1.col("a"), df2.col("a"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null, allowExactMatches = true, direction = "forward"),
      Seq(
        Row(1, "x", "a", 1, "v", 1),
        Row(5, "y", "b", 6, "y", 6)
      )
    )
  }

  test("as-of join - direction = \"nearest\"") {
    val (df1, df2) = prepareForAsOfJoin()
    checkAnswer(
      df1.joinAsOf(df2, df1.col("a"), df2.col("a"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null, allowExactMatches = true, direction = "nearest"),
      Seq(
        Row(1, "x", "a", 1, "v", 1),
        Row(5, "y", "b", 6, "y", 6),
        Row(10, "z", "c", 7, "z", 7)
      )
    )
  }

  test("as-of join - self") {
    val (df1, _) = prepareForAsOfJoin()
    checkAnswer(
      df1.joinAsOf(
        df1, df1.col("a"), df1.col("a"), usingColumns = Seq.empty,
        joinType = "left", tolerance = null, allowExactMatches = false, direction = "nearest"),
      Seq(
        Row(1, "x", "a", 5, "y", "b"),
        Row(5, "y", "b", 1, "x", "a"),
        Row(10, "z", "c", 5, "y", "b")
      )
    )
  }

  // Left: (ts, sym, ex, qty). Right: (ts, sym, ex, bid). Both sides have `sym` and `ex`.
  private def prepareTradesQuotes(): (classic.DataFrame, classic.DataFrame) = {
    val trades = Seq(
      (1, "A", "X", 10), (5, "A", "X", 20), (5, "A", "Y", 30), (7, "B", "X", 40),
      (9, "C", "X", 50)).toDF("ts", "sym", "ex", "qty").asInstanceOf[classic.DataFrame]
    val quotes = Seq(
      (2, "A", "X", 100), (4, "A", "X", 200), (6, "A", "Y", 300), (3, "B", "X", 400),
      (8, "B", "Z", 500)).toDF("ts", "sym", "ex", "bid").asInstanceOf[classic.DataFrame]
    (trades, quotes)
  }

  private def withAsOfConf(singlePass: Boolean, sortMerge: Boolean, ansi: Boolean)(
      f: => Unit): Unit = {
    withSQLConf(
      SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString,
      SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
      SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false",
      SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_ASOF_JOIN_RESOLUTION.key -> "true",
      SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> sortMerge.toString,
      SQLConf.SQL_ASOF_JOIN_ENABLED.key -> "true",
      SQLConf.ANSI_ENABLED.key -> ansi.toString) {
      f
    }
  }

  // Analyzer (fixed-point or single-pass), sort-merge flag, ANSI.
  private val asOfConfs = for {
    singlePass <- Seq(false, true)
    sortMerge <- Seq(false, true)
    ansi <- Seq(false, true)
  } yield (singlePass, sortMerge, ansi)

  // The single-pass analyzer does not support the deserializer that checkToRDD uses.
  private def checkRows(df: => DataFrame, expected: Seq[Row]): Unit =
    QueryTest.checkAnswer(df, expected, checkToRDD = false)

  private def usingJoin(
      left: classic.DataFrame,
      right: classic.DataFrame,
      using: Seq[String],
      joinType: String,
      tolerance: Column = null,
      allowExactMatches: Boolean = true,
      direction: String = "backward"): classic.DataFrame = {
    left.joinAsOf(
      right, left.col("ts"), right.col("ts"), usingColumns = using, joinType = joinType,
      tolerance = tolerance, allowExactMatches = allowExactMatches, direction = direction)
  }

  private def sortedRows(df: DataFrame): Seq[String] = df.collect().map(_.toString).sorted.toSeq

  private def sortedRows(rows: Seq[Row]): Seq[String] = rows.map(_.toString).sorted

  // Fails if the DataFrame keeps a key column of each side, or puts the key elsewhere.
  gridTest("usingColumns gives the columns and rows of SQL ASOF JOIN USING")(asOfConfs) {
    case (singlePass, sortMerge, ansi) =>
      withTempView("trades", "quotes") {
        val (trades, quotes) = prepareTradesQuotes()
        trades.createOrReplaceTempView("trades")
        quotes.createOrReplaceTempView("quotes")
        withAsOfConf(singlePass, sortMerge, ansi) {
          val oneKeyInner = Seq(
            Row("A", 5, "X", 20, 4, "X", 200), Row("A", 5, "Y", 30, 4, "X", 200),
            Row("B", 7, "X", 40, 3, "X", 400))
          val oneKeyLeft = oneKeyInner ++ Seq(
            Row("A", 1, "X", 10, null, null, null), Row("C", 9, "X", 50, null, null, null))
          val twoKeysInner = Seq(Row("A", "X", 5, 20, 4, 200), Row("B", "X", 7, 40, 3, 400))
          val twoKeysLeft = twoKeysInner ++ Seq(
            Row("A", "X", 1, 10, null, null), Row("A", "Y", 5, 30, null, null),
            Row("C", "X", 9, 50, null, null))
          val cases = Seq(
            (Seq("sym"), "inner", "INNER", oneKeyInner, 7),
            (Seq("sym"), "left", "LEFT", oneKeyLeft, 7),
            (Seq("sym", "ex"), "inner", "INNER", twoKeysInner, 6),
            (Seq("sym", "ex"), "left", "LEFT", twoKeysLeft, 6))
          for ((using, joinType, sqlJoinType, expected, width) <- cases) {
            withClue(s"using $using, $joinType: ") {
              val df = usingJoin(trades, quotes, using, joinType)
              val sqlDf = sql(
                s"""SELECT * FROM trades $sqlJoinType ASOF JOIN quotes
                   |MATCH_CONDITION (trades.ts >= quotes.ts) USING (${using.mkString(", ")})"""
                  .stripMargin)
              assert(df.columns.length == width)
              assert(df.columns.toSeq == sqlDf.columns.toSeq)
              assert(df.schema == sqlDf.schema)
              assert(sortedRows(df) == sortedRows(sqlDf))
              assert(sortedRows(df) == sortedRows(expected))
            }
          }
        }
      }
  }

  // Fails with AMBIGUOUS_REFERENCE if the output keeps the key of both sides.
  gridTest("usingColumns, a parent selects, filters and groups by the key")(asOfConfs) {
    case (singlePass, sortMerge, ansi) =>
      withAsOfConf(singlePass, sortMerge, ansi) {
        val (trades, quotes) = prepareTradesQuotes()
        val df = usingJoin(trades, quotes, Seq("sym"), "left")
        checkRows(
          df.select("sym", "qty", "bid").where("sym = 'A'"),
          Seq(Row("A", 20, 200), Row("A", 30, 200), Row("A", 10, null)))
        checkRows(
          df.groupBy("sym").count().orderBy("sym"),
          Seq(Row("A", 3), Row("B", 1), Row("C", 1)))
        // Code that selects a key by its Dataset must keep working.
        checkRows(
          df.select(trades.col("sym")).orderBy("sym"),
          Seq(Row("A"), Row("A"), Row("A"), Row("B"), Row("C")))
      }
  }

  // Fails if the key comes from the right side: the unmatched row with key B then has a NULL key.
  gridTest("usingColumns, a LEFT row without a match keeps its key, also NULL")(asOfConfs) {
    case (singlePass, sortMerge, ansi) =>
      withAsOfConf(singlePass, sortMerge, ansi) {
        val trades = Seq((5, Some("A"), 1), (5, None, 2), (5, Some("B"), 3))
          .toDF("ts", "sym", "qty")
          .asInstanceOf[classic.DataFrame]
        val quotes = Seq((4, Some("A"), 10), (4, None, 20)).toDF("ts", "sym", "bid")
          .asInstanceOf[classic.DataFrame]
        checkRows(
          usingJoin(trades, quotes, Seq("sym"), "left"),
          Seq(Row("A", 5, 1, 4, 10), Row(null, 5, 2, null, null), Row("B", 5, 3, null, null)))
      }
  }

  gridTest("usingColumns, a self join gives one key column")(asOfConfs) {
    case (singlePass, sortMerge, ansi) =>
      withTempView("trades") {
        val (trades, _) = prepareTradesQuotes()
        trades.createOrReplaceTempView("trades")
        withAsOfConf(singlePass, sortMerge, ansi) {
          val df = usingJoin(
            trades, trades, Seq("sym"), "left", allowExactMatches = false, direction = "backward")
          val sqlDf = sql(
            """SELECT * FROM trades t1 LEFT ASOF JOIN trades t2
              |MATCH_CONDITION (t1.ts > t2.ts) USING (sym)""".stripMargin)
          assert(df.columns.toSeq == Seq("sym", "ts", "ex", "qty", "ts", "ex", "qty"))
          assert(df.columns.toSeq == sqlDf.columns.toSeq)
          assert(sortedRows(df) == sortedRows(sqlDf))
          checkRows(
            df,
            Seq(
              Row("A", 1, "X", 10, null, null, null), Row("A", 5, "X", 20, 1, "X", 10),
              Row("A", 5, "Y", 30, 1, "X", 10), Row("B", 7, "X", 40, null, null, null),
              Row("C", 9, "X", 50, null, null, null)))
        }
      }
  }

  gridTest("usingColumns, a name missing on one side fails as in SQL USING")(asOfConfs) {
    case (singlePass, sortMerge, ansi) =>
      withTempView("only_left_trades", "quotes") {
        val (trades, quotes) = prepareTradesQuotes()
        val onlyLeft = trades.withColumn("only_left", lit(1)).asInstanceOf[classic.DataFrame]
        onlyLeft.createOrReplaceTempView("only_left_trades")
        quotes.createOrReplaceTempView("quotes")
        withAsOfConf(singlePass, sortMerge, ansi) {
          val sqlError = intercept[AnalysisException] {
            sql("""SELECT * FROM only_left_trades ASOF JOIN quotes
                  |MATCH_CONDITION (only_left_trades.ts >= quotes.ts) USING (only_left)"""
              .stripMargin)
          }
          assert(sqlError.getCondition == "UNRESOLVED_USING_COLUMN_FOR_JOIN")
          Seq(onlyLeft -> quotes, quotes -> onlyLeft).foreach { case (l, r) =>
            val e = intercept[AnalysisException] {
              usingJoin(l, r, Seq("only_left"), "inner")
            }
            assert(e.getCondition == sqlError.getCondition)
            assert(e.getMessageParameters.get("colName") == "`only_left`")
          }
        }
      }
  }

  // The new output has the rows of the Column-condition join, minus the key of the right side.
  gridTest("usingColumns, tolerance and direction keep the rows of a Column join")(asOfConfs) {
    case (singlePass, sortMerge, ansi) =>
      withAsOfConf(singlePass, sortMerge, ansi) {
        val (trades, quotes) = prepareTradesQuotes()
        for {
          direction <- Seq("backward", "forward", "nearest")
          joinType <- Seq("inner", "left")
          tolerance <- Seq(false, true)
          exact <- Seq(false, true)
        } withClue(s"$direction, $joinType, tolerance $tolerance, exact matches $exact: ") {
          def join(): classic.DataFrame = usingJoin(
            trades, quotes, Seq("sym", "ex"), joinType,
            if (tolerance) lit(1) else null, exact, direction)
          // The Column condition path keeps the key of both sides.
          val columnJoin = trades.joinAsOf(
            quotes, trades.col("ts"), quotes.col("ts"),
            trades("sym") === quotes("sym") && trades("ex") === quotes("ex"),
            joinType, if (tolerance) lit(1) else null, exact, direction)
          // Column-condition join columns: ts, sym, ex, qty, ts, sym, ex, bid.
          assert(columnJoin.columns.length == 8)
          val expected =
            columnJoin.collect().map(r => Row(r(1), r(2), r(0), r(3), r(4), r(7))).toSeq
          val df = join()
          assert(df.columns.toSeq == Seq("sym", "ex", "ts", "qty", "ts", "bid"))
          assert(sortedRows(df) == sortedRows(expected))
        }
      }
  }

  // The right key is hidden but stays reachable by its Dataset. Fails if a select or filter on it
  // stops working, or if an unmatched LEFT row gets a key it does not have: its right key is NULL.
  // The regular USING join is the baseline for what the single-pass analyzer accepts.
  gridTest("usingColumns, qualified access to the right key works as in a regular USING join")(
    asOfConfs) { case (singlePass, sortMerge, ansi) =>
    withAsOfConf(singlePass, sortMerge, ansi) {
      val (trades, quotes) = prepareTradesQuotes()
      def attempt(f: => DataFrame): Either[String, Seq[Row]] =
        scala.util.Try(f.collect().toSeq).toEither.left.map {
          case e: AnalysisException => e.getCondition
          case e => e.getClass.getName
        }
      for ((joinType, expected) <- Seq(
          "inner" -> Seq("A", "A", "B"), "left" -> Seq("A", "A", "B", null, null))) {
        withClue(s"$joinType: ") {
          val asOf = usingJoin(trades, quotes, Seq("sym"), joinType)
          val regular = trades.join(quotes, Seq("sym"), joinType)
          val asOfSelect = attempt(asOf.select(quotes.col("sym")))
          val regularSelect = attempt(regular.select(quotes.col("sym")))
          assert(asOfSelect.isRight == regularSelect.isRight, s"$asOfSelect vs $regularSelect")
          if (asOfSelect.isLeft) {
            assert(asOfSelect == regularSelect)
          } else {
            assert(asOfSelect.toOption.get.map(_.get(0)).sortBy(String.valueOf) ==
              expected.sortBy(String.valueOf))
            checkRows(
              asOf.where(quotes.col("sym") === "A").select(trades.col("sym")),
              Seq(Row("A"), Row("A")))
          }
        }
      }
    }
  }

  // The key keeps the type of the left side. ANSI changes how a STRING key is compared with an
  // INT key, so the result is compared with SQL for both settings.
  gridTest("usingColumns, keys of different types give the result of SQL USING")(asOfConfs) {
    case (singlePass, sortMerge, ansi) =>
      withTempView("lt", "rt_long", "rt_string") {
        val left = Seq((1, 1, "a"), (5, 2, "b"), (9, 3, "c")).toDF("ts", "k", "lv")
          .asInstanceOf[classic.DataFrame]
        val rightLong = Seq((0, 1L, "x"), (4, 2L, "y"), (6, 3L, "z")).toDF("ts", "k", "rv")
          .asInstanceOf[classic.DataFrame]
        val rightString = Seq((0, "1", "x"), (4, "2", "y"), (6, "3", "z")).toDF("ts", "k", "rv")
          .asInstanceOf[classic.DataFrame]
        left.createOrReplaceTempView("lt")
        rightLong.createOrReplaceTempView("rt_long")
        rightString.createOrReplaceTempView("rt_string")
        withAsOfConf(singlePass, sortMerge, ansi) {
          for ((right, view) <- Seq(rightLong -> "rt_long", rightString -> "rt_string");
              (joinType, sqlJoin) <- Seq("inner" -> "INNER", "left" -> "LEFT")) {
            withClue(s"$view, $joinType: ") {
              def run(f: => DataFrame): Either[String, (StructType, Seq[String])] =
                scala.util.Try { val df = f; (df.schema, sortedRows(df)) }.toEither.left.map {
                  case e: AnalysisException => e.getCondition
                  case e => e.getClass.getName
                }
              val asOf = run(usingJoin(left, right, Seq("k"), joinType))
              val sqlResult = run(sql(
                s"""SELECT * FROM lt $sqlJoin ASOF JOIN $view
                   |MATCH_CONDITION (lt.ts >= $view.ts) USING (k)""".stripMargin))
              assert(asOf == sqlResult)
              if (view == "rt_long") {
                assert(asOf.isRight)
                assert(asOf.toOption.get._1.fields.head.dataType == IntegerType)
              }
            }
          }
        }
      }
  }

  // The analyzer matches the names with spark.sql.caseSensitive.
  gridTest("usingColumns, the key name follows spark.sql.caseSensitive")(asOfConfs) {
    case (singlePass, sortMerge, ansi) =>
      withAsOfConf(singlePass, sortMerge, ansi) {
        val (trades, quotes) = prepareTradesQuotes()
        withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
          val df = usingJoin(trades, quotes, Seq("SYM"), "inner")
          assert(df.columns.toSeq == Seq("sym", "ts", "ex", "qty", "ts", "ex", "bid"))
          assert(df.count() == 3)
        }
        withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
          val e = intercept[AnalysisException] {
            usingJoin(trades, quotes, Seq("SYM"), "inner")
          }
          assert(e.getCondition == "UNRESOLVED_USING_COLUMN_FOR_JOIN")
        }
      }
  }

  // A nested field is not a top-level column name, so it cannot be a using column.
  test("usingColumns, a nested field name fails as in a regular USING join") {
    val left = sql("SELECT 1 AS ts, named_struct('x', 1) AS s")
      .asInstanceOf[classic.DataFrame]
    val right = sql("SELECT 0 AS ts, named_struct('x', 1) AS s")
      .asInstanceOf[classic.DataFrame]
    val regular = intercept[AnalysisException] { left.join(right, Seq("s.x")) }
    val asOf = intercept[AnalysisException] {
      usingJoin(left, right, Seq("s.x"), "inner")
    }
    assert(asOf.getCondition == regular.getCondition)
  }

  // The key stays usable by later steps. Each step must behave as it does after a regular
  // USING join: a second as-of join on the same name, drop of the right key, and aliases.
  gridTest("usingColumns, later steps use the key as after a regular USING join")(asOfConfs) {
    case (singlePass, sortMerge, ansi) =>
      withAsOfConf(singlePass, sortMerge, ansi) {
        val (trades, quotes) = prepareTradesQuotes()
        val first = usingJoin(trades, quotes, Seq("sym"), "left")
        val second = first.joinAsOf(
          quotes.withColumnRenamed("ts", "qts").select("qts", "sym", "bid")
            .withColumnRenamed("bid", "bid2"),
          first.col("qty"), col("qts"), usingColumns = Seq("sym"),
          joinType = "left", tolerance = null, allowExactMatches = true, direction = "backward")
        assert(second.columns.toSeq ==
          Seq("sym", "ts", "ex", "qty", "ts", "ex", "bid", "qts", "bid2"))
        assert(second.count() == 5)
        val t = trades.as("t")
        val q = quotes.as("q")
        val aliased = t.joinAsOf(
          q, t.col("ts"), q.col("ts"), usingColumns = Seq("sym"), joinType = "inner",
          tolerance = null, allowExactMatches = true, direction = "backward")
        val regular = t.join(q, Seq("sym"), "inner")
        def attempt(f: => DataFrame): Either[String, Int] =
          scala.util.Try(f.collect().length).toEither.left.map {
            case e: AnalysisException => e.getCondition
            case e => e.getClass.getName
          }
        val selectQ = (df: classic.DataFrame) => attempt(df.select(col("q.sym"))).isRight
        val dropQ = (df: classic.DataFrame) => attempt(df.drop(q.col("sym"))).isRight
        assert(selectQ(aliased) == selectQ(regular))
        assert(dropQ(aliased) == dropQ(regular))
        assert(attempt(aliased.select(col("t.sym"))) == Right(3))
        assert(aliased.select("*").columns.length == 7)
      }
  }

  // A name that matches two columns of the left side uses the first one, as a regular USING join
  // does. The old code failed with AMBIGUOUS_REFERENCE. The second `sym` holds a value no quote
  // has, so a join on the wrong column would show.
  gridTest("usingColumns, a name that matches two left columns follows a regular USING join")(
    asOfConfs) { case (singlePass, sortMerge, ansi) =>
    withAsOfConf(singlePass, sortMerge, ansi) {
      val (trades, quotes) = prepareTradesQuotes()
      val other = Seq((5, "ZZ", 7)).toDF("ts2", "sym", "z").asInstanceOf[classic.DataFrame]
      val twice = trades.join(other, trades.col("ts") === other.col("ts2"))
        .asInstanceOf[classic.DataFrame]
      def attempt(f: => DataFrame): Either[String, (Seq[String], Seq[Row])] =
        scala.util.Try { val df = f; (df.columns.toSeq, df.collect().toSeq) }.toEither.left.map {
          case e: AnalysisException => e.getCondition
          case e => e.getClass.getName
        }
      val regular = attempt(twice.join(quotes, Seq("sym"), "inner"))
      val asOf = attempt(twice.joinAsOf(
        quotes, twice.col("ts"), quotes.col("ts"), usingColumns = Seq("sym"), joinType = "inner",
        tolerance = null, allowExactMatches = true, direction = "backward"))
      (regular, asOf) match {
        case (Right((regularColumns, _)), Right((asOfColumns, asOfRows))) =>
          assert(asOfColumns == regularColumns)
          // Fails if the key is the second `sym` ("ZZ"), or the join drops its rows.
          assert(asOfRows.nonEmpty && asOfRows.forall(_.getString(0) == "A"))
        case _ => assert(regular.isLeft && regular == asOf, s"$regular vs $asOf")
      }
    }
  }

  // Right key offsets from the left key (offset 0 is the left key) for each case. The tolerance is
  // 2, so offsets -2 and 2 sit exactly on the tolerance bound.
  private val toleranceCases: Seq[(String, Seq[Int])] = Seq(
    "on_bound_before" -> Seq(-2),
    "on_bound_after" -> Seq(2),
    "past_bound_before" -> Seq(-3),
    "past_bound_after" -> Seq(3),
    "exact_only" -> Seq(0),
    "exact_and_bounds" -> Seq(-2, 0, 1),
    "tie_on_bounds" -> Seq(-2, 2),
    "inside" -> Seq(-1, 2))

  // The right offset the join must pick, or None. Only the match depends on allowExactMatches.
  // The tolerance bound is always inclusive.
  private def expectedOffset(
      direction: String,
      allowExactMatches: Boolean,
      offsets: Seq[Int]): Option[Int] = {
    val matching = offsets.filter { r =>
      math.abs(r) <= 2 && (direction match {
        case "backward" => if (allowExactMatches) r <= 0 else r < 0
        case "forward" => if (allowExactMatches) r >= 0 else r > 0
        case _ => allowExactMatches || r != 0
      })
    }
    if (matching.isEmpty) {
      None
    } else {
      direction match {
        case "backward" => Some(matching.max)
        case "forward" => Some(matching.min)
        case _ => Some(matching.minBy(r => (math.abs(r), r)))
      }
    }
  }

  private def toleranceJoinResults(
      keyType: String,
      direction: String,
      allowExactMatches: Boolean,
      cases: Seq[(String, Seq[Int])]): DataFrame = {
    def key(offset: String): String = keyType match {
      case "INT" => s"10 + $offset"
      case "DATE" => s"date_add(DATE'2024-01-10', $offset)"
      case _ => s"TIMESTAMP'2024-01-10 00:00:00' + make_dt_interval($offset)"
    }
    val leftRows = cases.map(c => s"('${c._1}')").mkString(", ")
    val rightRows = cases.flatMap { case (id, offsets) =>
      offsets.map(o => s"('$id', $o)")
    }.mkString(", ")
    val left = spark.sql(
      s"SELECT case_id, ${key("0")} AS k FROM VALUES $leftRows AS t(case_id)")
    val right = spark.sql(
      s"SELECT case_id, ${key("ro")} AS k, ro FROM VALUES $rightRows AS t(case_id, ro)")
    val tolerance = if (keyType == "INT") lit(2) else lit(Duration.ofDays(2))
    left.joinAsOf(
      right, left.col("k"), right.col("k"), usingColumns = Seq("case_id"),
      joinType = "left", tolerance = tolerance, allowExactMatches = allowExactMatches,
      direction = direction
    ).select(left.col("case_id"), right.col("ro"))
  }

  for {
    keyType <- Seq("INT", "DATE", "TIMESTAMP")
    direction <- Seq("backward", "forward", "nearest")
  } {
    test(s"as-of join - tolerance bound is inclusive: $direction, $keyType keys") {
      for {
        sortMerge <- Seq(false, true)
        singlePass <- if (sortMerge) Seq(false, true) else Seq(false)
        ansi <- Seq(false, true)
        allowExactMatches <- Seq(false, true)
      } {
        withSQLConf(
          SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> sortMerge.toString,
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString,
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
          SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false",
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_ASOF_JOIN_RESOLUTION.key -> "true",
          SQLConf.ANSI_ENABLED.key -> ansi.toString) {
          // The old rewrite picks an arbitrary row among nearest candidates at equal distance.
          val cases = toleranceCases.filter { c =>
            sortMerge || direction != "nearest" || c._1 != "tie_on_bounds"
          }
          val expected = cases.map { case (id, offsets) =>
            Row(id, expectedOffset(direction, allowExactMatches, offsets)
              .map(Int.box).orNull)
          }
          withClue(s"sortMerge=$sortMerge, singlePass=$singlePass, ansi=$ansi, " +
              s"allowExactMatches=$allowExactMatches: ") {
            val result = toleranceJoinResults(keyType, direction, allowExactMatches, cases)
            // checkAnswer re-analyzes the plan with an encoder, which the single-pass analyzer
            // does not support yet.
            val mismatch = QueryTest.sameRows(expected, result.collect().toSeq)
            assert(mismatch.isEmpty, mismatch.getOrElse(""))
          }
        }
      }
    }
  }
}
