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

import java.util.Locale

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{GroupFrame, Literal,
  NonFoldableLiteral, RangeFrame, SpecifiedWindowFrame, WindowExpression}
import org.apache.spark.sql.catalyst.optimizer.EliminateWindowPartitions
import org.apache.spark.sql.catalyst.plans.logical.{Window => WindowNode}
import org.apache.spark.sql.classic.ExpressionColumnNode
import org.apache.spark.sql.execution.{ExtendedMode, SortExec}
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{CalendarIntervalType, DayTimeIntervalType, IntegerType}

/**
 * Window frame testing for DataFrame API.
 */
class DataFrameWindowFramesSuite extends SharedSparkSession {
  import testImplicits._

  test("reuse window partitionBy") {
    val df = Seq((1, "1"), (2, "2"), (1, "1"), (2, "2")).toDF("key", "value")
    val w = Window.partitionBy("key").orderBy("value")

    checkAnswer(
      df.select(
        lead("key", 1).over(w),
        lead("value", 1).over(w)),
      Row(1, "1") :: Row(2, "2") :: Row(null, null) :: Row(null, null) :: Nil)
  }

  test("reuse window orderBy") {
    val df = Seq((1, "1"), (2, "2"), (1, "1"), (2, "2")).toDF("key", "value")
    val w = Window.orderBy("value").partitionBy("key")

    checkAnswer(
      df.select(
        lead("key", 1).over(w),
        lead("value", 1).over(w)),
      Row(1, "1") :: Row(2, "2") :: Row(null, null) :: Row(null, null) :: Nil)
  }

  test("lead/lag with empty data frame") {
    val df = Seq.empty[(Int, String)].toDF("key", "value")
    val window = Window.partitionBy($"key").orderBy($"value")

    checkAnswer(
      df.select(
        lead("value", 1).over(window),
        lag("value", 1).over(window)),
      Nil)
  }

  test("lead/lag with positive offset") {
    val df = Seq((1, "1"), (2, "2"), (1, "3"), (2, "4")).toDF("key", "value")
    val window = Window.partitionBy($"key").orderBy($"value")

    checkAnswer(
      df.select(
        $"key",
        lead("value", 1).over(window),
        lag("value", 1).over(window)),
      Row(1, "3", null) :: Row(1, null, "1") :: Row(2, "4", null) :: Row(2, null, "2") :: Nil)
  }

  test("reverse lead/lag with positive offset") {
    val df = Seq((1, "1"), (2, "2"), (1, "3"), (2, "4")).toDF("key", "value")
    val window = Window.partitionBy($"key").orderBy($"value".desc)

    checkAnswer(
      df.select(
        $"key",
        lead("value", 1).over(window),
        lag("value", 1).over(window)),
      Row(1, "1", null) :: Row(1, null, "3") :: Row(2, "2", null) :: Row(2, null, "4") :: Nil)
  }

  test("lead/lag with positive offset that greater than window group size") {
    val df = Seq((1, "1"), (2, "2"), (1, "3"), (2, "4")).toDF("key", "value")
    val window = Window.partitionBy($"key").orderBy($"value")

    checkAnswer(
      df.select(
        $"key",
        lead("value", 3).over(window),
        lag("value", 3).over(window)),
      Row(1, null, null) :: Row(1, null, null) :: Row(2, null, null) :: Row(2, null, null) :: Nil)
  }

  test("lead/lag with negative offset") {
    val df = Seq((1, "1"), (2, "2"), (1, "3"), (2, "4")).toDF("key", "value")
    val window = Window.partitionBy($"key").orderBy($"value")

    checkAnswer(
      df.select(
        $"key",
        lead("value", -1).over(window),
        lag("value", -1).over(window)),
      Row(1, null, "3") :: Row(1, "1", null) :: Row(2, null, "4") :: Row(2, "2", null) :: Nil)
  }

  test("lead/lag with negative offset that absolute value greater than window group size") {
    val df = Seq((1, "1"), (2, "2"), (1, "3"), (2, "4")).toDF("key", "value")
    val window = Window.partitionBy($"key").orderBy($"value")

    checkAnswer(
      df.select(
        $"key",
        lead("value", -3).over(window),
        lag("value", -3).over(window)),
      Row(1, null, null) :: Row(1, null, null) :: Row(2, null, null) :: Row(2, null, null) :: Nil)
  }

  test("reverse lead/lag with negative offset") {
    val df = Seq((1, "1"), (2, "2"), (1, "3"), (2, "4")).toDF("key", "value")
    val window = Window.partitionBy($"key").orderBy($"value".desc)

    checkAnswer(
      df.select(
        $"key",
        lead("value", -1).over(window),
        lag("value", -1).over(window)),
      Row(1, null, "1") :: Row(1, "3", null) :: Row(2, null, "2") :: Row(2, "4", null) :: Nil)
  }

  test("lead/lag with default value") {
    val default = "n/a"
    val df = Seq((1, "1"), (2, "2"), (1, "3"), (2, "4"), (2, "5")).toDF("key", "value")
    val window = Window.partitionBy($"key").orderBy($"value")

    checkAnswer(
      df.select(
        $"key",
        lead("value", 2, default).over(window),
        lag("value", 2, default).over(window),
        lead("value", -2, default).over(window),
        lag("value", -2, default).over(window)),
      Row(1, default, default, default, default) :: Row(1, default, default, default, default) ::
        Row(2, "5", default, default, "5") :: Row(2, default, "2", "2", default) ::
        Row(2, default, default, default, default) :: Nil)
  }

  test("lead/lag with column reference as default when offset exceeds window group size") {
    val df = spark.range(0, 10, 1, 1).toDF("id")
    val window = Window.partitionBy(expr("div(id, 2)")).orderBy($"id")

    val result = df.select(
      $"id",
      lead($"id", 1, $"id").over(window).as("lead_1"),
      lead($"id", 3, $"id").over(window).as("lead_3"),
      lag($"id", 1, $"id").over(window).as("lag_1"),
      lag($"id", 3, $"id").over(window).as("lag_3")
    ).orderBy("id")

    // check the output in one table
    // col0: id, col1: lead_1 result, col2: lead_3 result,
    // col3: lag_1 result, col4: lag_3 result
    val expected = Seq(
      Row(0, 1, 0, 0, 0),
      Row(1, 1, 1, 0, 1),
      Row(2, 3, 2, 2, 2),
      Row(3, 3, 3, 2, 3),
      Row(4, 5, 4, 4, 4),
      Row(5, 5, 5, 4, 5),
      Row(6, 7, 6, 6, 6),
      Row(7, 7, 7, 6, 7),
      Row(8, 9, 8, 8, 8),
      Row(9, 9, 9, 8, 9)
    )

    checkAnswer(result, expected)
  }

  test("rows/range between with empty data frame") {
    val df = Seq.empty[(String, Int)].toDF("key", "value")
    val window = Window.partitionBy($"key").orderBy($"value")

    checkAnswer(
      df.select(
        $"key",
        first("value").over(
          window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
        first("value").over(
          window.rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing))),
      Nil)
  }

  test("rows between should accept int/long values as boundary") {
    val df = Seq((1L, "1"), (1L, "1"), (2147483650L, "1"), (3L, "2"), (2L, "1"), (2147483650L, "2"))
      .toDF("key", "value")

    checkAnswer(
      df.select(
        $"key",
        count("key").over(
          Window.partitionBy($"value").orderBy($"key").rowsBetween(0, 2147483647))),
      Seq(Row(1, 3), Row(1, 4), Row(2, 2), Row(3, 2), Row(2147483650L, 1), Row(2147483650L, 1))
    )

    checkError(
      exception = intercept[AnalysisException](
        df.select(
          $"key",
          count("key").over(
            Window.partitionBy($"value").orderBy($"key").rowsBetween(2147483648L, 0)))),
      condition = "INVALID_BOUNDARY.START",
      parameters = Map(
        "invalidValue" -> "2147483648L",
        "boundary" -> "`start`",
        "intMaxValue" -> "2147483647",
        "intMinValue" -> "-2147483648",
        "longMinValue" -> "-9223372036854775808L"))

    checkError(
      exception = intercept[AnalysisException](
        df.select(
          $"key",
          count("key").over(
            Window.partitionBy($"value").orderBy($"key").rowsBetween(0, 2147483648L)))),
      condition = "INVALID_BOUNDARY.END",
      parameters = Map(
        "invalidValue" -> "2147483648L",
        "boundary" -> "`end`",
        "intMaxValue" -> "2147483647",
        "intMinValue" -> "-2147483648",
        "longMaxValue" -> "9223372036854775807L"))
  }

  test("range between should accept at most one ORDER BY expression when unbounded") {
    val df = Seq((1, 1)).toDF("key", "value")
    val window = Window.orderBy($"key", $"value")

    checkAnswer(
      df.select(
        $"key",
        min("key").over(
          window.rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing))),
      Seq(Row(1, 1))
    )

    checkError(
      exception = intercept[AnalysisException](
        df.select(
          min("key").over(window.rangeBetween(Window.unboundedPreceding, 1)))
      ),
      condition = "DATATYPE_MISMATCH.RANGE_FRAME_MULTI_ORDER",
      parameters = Map(
        "orderSpec" -> """key#\d+ ASC NULLS FIRST,value#\d+ ASC NULLS FIRST""",
        "sqlExpr" -> (""""\(ORDER BY key ASC NULLS FIRST, value ASC NULLS FIRST RANGE """ +
          """BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING\)"""")
      ),
      matchPVals = true,
      queryContext =
        Array(ExpectedContext(fragment = "over", callSitePattern = getCurrentClassCallSitePattern))
    )

    checkError(
      exception = intercept[AnalysisException](
        df.select(
          min("key").over(window.rangeBetween(-1, Window.unboundedFollowing)))
      ),
      condition = "DATATYPE_MISMATCH.RANGE_FRAME_MULTI_ORDER",
      parameters = Map(
        "orderSpec" -> """key#\d+ ASC NULLS FIRST,value#\d+ ASC NULLS FIRST""",
        "sqlExpr" -> (""""\(ORDER BY key ASC NULLS FIRST, value ASC NULLS FIRST RANGE """ +
          """BETWEEN -1 FOLLOWING AND UNBOUNDED FOLLOWING\)"""")
      ),
      matchPVals = true,
      queryContext =
        Array(ExpectedContext(fragment = "over", callSitePattern = getCurrentClassCallSitePattern))
    )

    checkError(
      exception = intercept[AnalysisException](
        df.select(
          min("key").over(window.rangeBetween(-1, 1)))
      ),
      condition = "DATATYPE_MISMATCH.RANGE_FRAME_MULTI_ORDER",
      parameters = Map(
        "orderSpec" -> """key#\d+ ASC NULLS FIRST,value#\d+ ASC NULLS FIRST""",
        "sqlExpr" -> (""""\(ORDER BY key ASC NULLS FIRST, value ASC NULLS FIRST RANGE """ +
          """BETWEEN -1 FOLLOWING AND 1 FOLLOWING\)"""")
      ),
      matchPVals = true,
      queryContext =
        Array(ExpectedContext(fragment = "over", callSitePattern = getCurrentClassCallSitePattern))
    )

  }

  test("range between should accept numeric values only when bounded") {
    val df = Seq("non_numeric").toDF("value")
    val window = Window.orderBy($"value")

    checkAnswer(
      df.select(
        $"value",
        min("value").over(
          window.rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing))),
      Row("non_numeric", "non_numeric") :: Nil)

    checkError(
      exception = intercept[AnalysisException](
        df.select(
          min("value").over(window.rangeBetween(Window.unboundedPreceding, 1)))
      ),
      condition = "DATATYPE_MISMATCH.SPECIFIED_WINDOW_FRAME_UNACCEPTED_TYPE",
      parameters = Map(
        "location" -> "upper",
        "exprType" -> "\"STRING\"",
        "expectedType" -> ("(\"NUMERIC\" or \"INTERVAL DAY TO SECOND\" or \"INTERVAL YEAR " +
          "TO MONTH\" or \"INTERVAL\")"),
        "sqlExpr" -> "\"RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING\""
      ),
      context = ExpectedContext(fragment = "over", callSitePattern = getCurrentClassCallSitePattern)
    )

    checkError(
      exception = intercept[AnalysisException](
        df.select(
          min("value").over(window.rangeBetween(-1, Window.unboundedFollowing)))
      ),
      condition = "DATATYPE_MISMATCH.SPECIFIED_WINDOW_FRAME_UNACCEPTED_TYPE",
      parameters = Map(
        "location" -> "lower",
        "exprType" -> "\"STRING\"",
        "expectedType" -> ("(\"NUMERIC\" or \"INTERVAL DAY TO SECOND\" or \"INTERVAL YEAR " +
          "TO MONTH\" or \"INTERVAL\")"),
        "sqlExpr" -> "\"RANGE BETWEEN -1 FOLLOWING AND UNBOUNDED FOLLOWING\""
      ),
      context = ExpectedContext(fragment = "over", callSitePattern = getCurrentClassCallSitePattern)
    )

    checkError(
      exception = intercept[AnalysisException](
        df.select(
          min("value").over(window.rangeBetween(-1, 1)))
      ),
      condition = "DATATYPE_MISMATCH.SPECIFIED_WINDOW_FRAME_UNACCEPTED_TYPE",
      parameters = Map(
        "location" -> "lower",
        "exprType" -> "\"STRING\"",
        "expectedType" -> ("(\"NUMERIC\" or \"INTERVAL DAY TO SECOND\" or \"INTERVAL YEAR " +
          "TO MONTH\" or \"INTERVAL\")"),
        "sqlExpr" -> "\"RANGE BETWEEN -1 FOLLOWING AND 1 FOLLOWING\""
      ),
      context = ExpectedContext(fragment = "over", callSitePattern = getCurrentClassCallSitePattern)
    )
  }

  test("range between should accept int/long values as boundary") {
    val df = Seq((1L, "1"), (1L, "1"), (2147483650L, "1"), (3L, "2"), (2L, "1"), (2147483650L, "2"))
      .toDF("key", "value")

    checkAnswer(
      df.select(
        $"key",
        count("key").over(
          Window.partitionBy($"value").orderBy($"key").rangeBetween(0, 2147483648L))),
      Seq(Row(1, 3), Row(1, 3), Row(2, 2), Row(3, 2), Row(2147483650L, 1), Row(2147483650L, 1))
    )
    checkAnswer(
      df.select(
        $"key",
        count("key").over(
          Window.partitionBy($"value").orderBy($"key").rangeBetween(-2147483649L, 0))),
      Seq(Row(1, 2), Row(1, 2), Row(2, 3), Row(2147483650L, 2), Row(2147483650L, 4), Row(3, 1))
    )
  }

  test("unbounded rows/range between with aggregation") {
    val df = Seq(("one", 1), ("two", 2), ("one", 3), ("two", 4)).toDF("key", "value")
    val window = Window.partitionBy($"key").orderBy($"value")

    checkAnswer(
      df.select(
        $"key",
        sum("value").over(window.
          rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
        sum("value").over(window.
          rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing))),
      Row("one", 4, 4) :: Row("one", 4, 4) :: Row("two", 6, 6) :: Row("two", 6, 6) :: Nil)
  }

  test("unbounded preceding/following rows between with aggregation") {
    val df = Seq((1, "1"), (2, "2"), (2, "3"), (1, "3"), (3, "2"), (4, "3")).toDF("key", "value")
    val window = Window.partitionBy($"value").orderBy($"key")

    checkAnswer(
      df.select(
        $"key",
        last("key").over(
          window.rowsBetween(Window.currentRow, Window.unboundedFollowing)),
        last("key").over(
          window.rowsBetween(Window.unboundedPreceding, Window.currentRow))),
      Row(1, 1, 1) :: Row(2, 3, 2) :: Row(3, 3, 3) :: Row(1, 4, 1) :: Row(2, 4, 2) ::
        Row(4, 4, 4) :: Nil)
  }

  test("reverse unbounded preceding/following rows between with aggregation") {
    val df = Seq((1, "1"), (2, "2"), (2, "3"), (1, "3"), (3, "2"), (4, "3")).toDF("key", "value")
    val window = Window.partitionBy($"value").orderBy($"key".desc)

    checkAnswer(
      df.select(
        $"key",
        last("key").over(
          window.rowsBetween(Window.currentRow, Window.unboundedFollowing)),
        last("key").over(
          window.rowsBetween(Window.unboundedPreceding, Window.currentRow))),
      Row(1, 1, 1) :: Row(3, 2, 3) :: Row(2, 2, 2) :: Row(4, 1, 4) :: Row(2, 1, 2) ::
        Row(1, 1, 1) :: Nil)
  }

  test("unbounded preceding/following range between with aggregation") {
    val df = Seq((5, "1"), (5, "2"), (4, "2"), (6, "2"), (3, "1"), (2, "2")).toDF("key", "value")
    val window = Window.partitionBy("value").orderBy("key")

    checkAnswer(
      df.select(
        $"key",
        avg("key").over(window.rangeBetween(Window.unboundedPreceding, 1))
          .as("avg_key1"),
        avg("key").over(window.rangeBetween(Window.currentRow, Window.unboundedFollowing))
          .as("avg_key2")),
      Row(3, 3.0d, 4.0d) :: Row(5, 4.0d, 5.0d) :: Row(2, 2.0d, 17.0d / 4.0d) ::
        Row(4, 11.0d / 3.0d, 5.0d) :: Row(5, 17.0d / 4.0d, 11.0d / 2.0d) ::
        Row(6, 17.0d / 4.0d, 6.0d) :: Nil)
  }

  // This is here to illustrate the fact that reverse order also reverses offsets.
  test("reverse preceding/following range between with aggregation") {
    val df = Seq(1, 2, 4, 3, 2, 1).toDF("value")
    val window = Window.orderBy($"value".desc)

    checkAnswer(
      df.select(
        $"value",
        sum($"value").over(window.rangeBetween(Window.unboundedPreceding, 1)),
        sum($"value").over(window.rangeBetween(1, Window.unboundedFollowing))),
      Row(1, 13, null) :: Row(2, 13, 2) :: Row(4, 7, 9) :: Row(3, 11, 6) ::
        Row(2, 13, 2) :: Row(1, 13, null) :: Nil)
  }

  test("sliding rows between with aggregation") {
    val df = Seq((1, "1"), (2, "1"), (2, "2"), (1, "1"), (2, "2")).toDF("key", "value")
    val window = Window.partitionBy($"value").orderBy($"key").rowsBetween(-1, 2)

    checkAnswer(
      df.select(
        $"key",
        avg("key").over(window)),
      Row(1, 4.0d / 3.0d) :: Row(1, 4.0d / 3.0d) :: Row(2, 3.0d / 2.0d) :: Row(2, 2.0d) ::
        Row(2, 2.0d) :: Nil)
  }

  test("reverse sliding rows between with aggregation") {
    val df = Seq((1, "1"), (2, "1"), (2, "2"), (1, "1"), (2, "2")).toDF("key", "value")
    val window = Window.partitionBy($"value").orderBy($"key".desc).rowsBetween(-1, 2)

    checkAnswer(
      df.select(
        $"key",
        avg("key").over(window)),
      Row(1, 1.0d) :: Row(1, 4.0d / 3.0d) :: Row(2, 4.0d / 3.0d) :: Row(2, 2.0d) ::
        Row(2, 2.0d) :: Nil)
  }

  test("sliding range between with aggregation") {
    val df = Seq((1, "1"), (1, "1"), (3, "1"), (2, "2"), (2, "1"), (2, "2")).toDF("key", "value")
    val window = Window.partitionBy($"value").orderBy($"key").rangeBetween(-1, 1)

    checkAnswer(
      df.select(
        $"key",
        avg("key").over(window)),
      Row(1, 4.0d / 3.0d) :: Row(1, 4.0d / 3.0d) :: Row(2, 7.0d / 4.0d) :: Row(3, 5.0d / 2.0d) ::
        Row(2, 2.0d) :: Row(2, 2.0d) :: Nil)
  }

  test("reverse sliding range between with aggregation") {
    val df = Seq(
      (1, "Thin", "Cell Phone", 6000),
      (2, "Normal", "Tablet", 1500),
      (3, "Mini", "Tablet", 5500),
      (4, "Ultra thin", "Cell Phone", 5500),
      (5, "Very thin", "Cell Phone", 6000),
      (6, "Big", "Tablet", 2500),
      (7, "Bendable", "Cell Phone", 3000),
      (8, "Foldable", "Cell Phone", 3000),
      (9, "Pro", "Tablet", 4500),
      (10, "Pro2", "Tablet", 6500)).
      toDF("id", "product", "category", "revenue")
    val window = Window.partitionBy($"category").orderBy($"revenue".desc).
      rangeBetween(-2000L, 1000L)

    checkAnswer(
      df.select(
        $"id",
        avg($"revenue").over(window).cast("int")),
      Row(1, 5833) :: Row(2, 2000) :: Row(3, 5500) ::
        Row(4, 5833) :: Row(5, 5833) :: Row(6, 2833) ::
        Row(7, 3000) :: Row(8, 3000) :: Row(9, 5500) ::
        Row(10, 6000) :: Nil)
  }

  test("SPARK-24033: Analysis Failure of OffsetWindowFunction") {
    val ds = Seq((1, 1), (1, 2), (1, 3), (2, 1), (2, 2)).toDF("n", "i")
    val res =
      Row(1, 1, null) :: Row (1, 2, 1) :: Row(1, 3, 2) :: Row(2, 1, null) :: Row(2, 2, 1) :: Nil
    checkAnswer(
      ds.withColumn("m",
        lead("i", -1).over(Window.partitionBy("n").orderBy("i").rowsBetween(-1, -1))),
      res)
    checkAnswer(
      ds.withColumn("m",
        lag("i", 1).over(Window.partitionBy("n").orderBy("i").rowsBetween(-1, -1))),
      res)
  }

  test("Window frame bounds lower and upper do not have the same type") {
    val df = Seq((1L, "1"), (1L, "1")).toDF("key", "value")

    val windowSpec = Window.partitionBy($"value").orderBy($"key".asc).withFrame(
      internal.WindowFrame.Range,
      internal.WindowFrame.Value(ExpressionColumnNode(Literal.create(null, CalendarIntervalType))),
      internal.WindowFrame.Value(lit(2).node))
    checkError(
      exception = intercept[AnalysisException] {
        df.select($"key", count("key").over(windowSpec)).collect()
      },
      condition = "DATATYPE_MISMATCH.SPECIFIED_WINDOW_FRAME_DIFF_TYPES",
      parameters = Map(
        "sqlExpr" -> "\"RANGE BETWEEN NULL FOLLOWING AND 2 FOLLOWING\"",
        "lower" -> "\"NULL\"",
        "upper" -> "\"2\"",
        "lowerType" -> "\"INTERVAL\"",
        "upperType" -> "\"BIGINT\""
      ),
      context = ExpectedContext(fragment = "over", callSitePattern = getCurrentClassCallSitePattern)
    )
  }

  test("Window frame lower bound is not a literal") {
    val df = Seq((1L, "1"), (1L, "1")).toDF("key", "value")
    val windowSpec = Window.partitionBy($"value").orderBy($"key".asc).withFrame(
      internal.WindowFrame.Range,
      internal.WindowFrame.Value(ExpressionColumnNode(NonFoldableLiteral(1))),
      internal.WindowFrame.Value(lit(2).node))
    checkError(
      exception = intercept[AnalysisException] {
        df.select($"key", count("key").over(windowSpec)).collect()
      },
      condition = "DATATYPE_MISMATCH.SPECIFIED_WINDOW_FRAME_WITHOUT_FOLDABLE",
      parameters = Map(
        "sqlExpr" -> "\"RANGE BETWEEN nonfoldableliteral() FOLLOWING AND 2 FOLLOWING\"",
        "location" -> "lower",
        "expression" -> "\"nonfoldableliteral()\""),
      context = ExpectedContext(fragment = "over", callSitePattern = getCurrentClassCallSitePattern)
    )
  }

  test("SPARK-41805: Reuse expressions in WindowSpecDefinition") {
    val ds = Seq((1, 1), (1, 2), (1, 3), (2, 1), (2, 2)).toDF("n", "i")
    val window = Window.partitionBy($"n").orderBy($"n".cast("string").asc)
    val df = ds.select(sum("i").over(window), avg("i").over(window))
    val ws = df.queryExecution.analyzed.collect { case w: WindowNode => w }
    assert(ws.size === 1)
    checkAnswer(df,
      Row(3, 1.5) :: Row(3, 1.5) :: Row(6, 2.0) :: Row(6, 2.0) :: Row(6, 2.0) :: Nil)
  }

  test("SPARK-41793: Incorrect result for window frames defined by a range clause on large " +
    "decimals") {
    val window = Window.partitionBy($"a").orderBy($"b".asc).withFrame(
      internal.WindowFrame.Range,
      internal.WindowFrame.Value((-lit(BigDecimal(10.2345))).node),
      internal.WindowFrame.Value(lit(BigDecimal(10.2345)).node))

    val df = Seq(
      1 -> "11342371013783243717493546650944543.47",
      1 -> "999999999999999999999999999999999999.99"
    ).toDF("a", "b")
      .select($"a", $"b".cast("decimal(38, 2)"))
      .select(count("*").over(window))

    checkAnswer(
      df,
      Row(1) :: Row(1) :: Nil)
  }

  test("SPARK-45352: Eliminate foldable window partitions") {
    val df = Seq((1, 1), (1, 2), (1, 3), (2, 1), (2, 2)).toDF("a", "b")

    Seq(true, false).foreach { eliminateWindowPartitionsEnabled =>
      val excludedRules =
        if (eliminateWindowPartitionsEnabled) "" else EliminateWindowPartitions.ruleName
      withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> excludedRules) {
        val window1 = Window.partitionBy(lit(1)).orderBy($"b")
        checkAnswer(
          df.select($"a", $"b", row_number().over(window1)),
          Seq(Row(1, 1, 1), Row(1, 2, 3), Row(1, 3, 5), Row(2, 1, 2), Row(2, 2, 4)))

        val window2 = Window.partitionBy($"a", lit(1)).orderBy($"b")
        checkAnswer(
          df.select($"a", $"b", row_number().over(window2)),
          Seq(Row(1, 1, 1), Row(1, 2, 2), Row(1, 3, 3), Row(2, 1, 1), Row(2, 2, 2)))
      }
    }
  }

  test("SPARK-34227: WindowFunctionFrame should clear its states during preparation") {
    // This creates a single partition dataframe with 3 records:
    //   "a", 0, null
    //   "a", 1, "x"
    //   "b", 0, null
    val df = spark.range(0, 3, 1, 1).select(
      when($"id" < 2, lit("a")).otherwise(lit("b")).as("key"),
      ($"id" % 2).cast("int").as("order"),
      when($"id" % 2 === 0, lit(null)).otherwise(lit("x")).as("value"))

    val window1 = Window.partitionBy($"key").orderBy($"order")
      .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    val window2 = Window.partitionBy($"key").orderBy($"order")
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    checkAnswer(
      df.select(
        $"key",
        $"order",
        nth_value($"value", 1, ignoreNulls = true).over(window1),
        nth_value($"value", 1, ignoreNulls = true).over(window2)),
      Seq(
        Row("a", 0, "x", null),
        Row("a", 1, "x", "x"),
        Row("b", 0, null, null)))
  }

  test("GROUPS frame requires an ORDER BY") {
    withTempView("t") {
      Seq((1, 1), (2, 2)).toDF("key", "value").createOrReplaceTempView("t")
      checkError(
        exception = intercept[AnalysisException](
          spark.sql(
            "select sum(value) over (partition by key groups between " +
              "unbounded preceding and current row) from t").collect()),
        condition = "DATATYPE_MISMATCH.GROUPS_FRAME_WITHOUT_ORDER",
        parameters = Map(
          "sqlExpr" ->
            "\"(PARTITION BY key GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)\""),
        queryContext = Array(
          ExpectedContext(
            fragment = "(partition by key groups between " +
              "unbounded preceding and current row)",
            start = 23,
            stop = 91)))
    }
  }

  test("GROUPS frame rejects a non-integral offset (decimal)") {
    withTempView("t") {
      Seq((1, 1), (2, 2)).toDF("key", "value").createOrReplaceTempView("t")
      checkError(
        exception = intercept[AnalysisException](
          spark.sql(
            "select sum(value) over (order by key groups between " +
              "1.5 preceding and current row) from t").collect()),
        condition = "DATATYPE_MISMATCH.SPECIFIED_WINDOW_FRAME_UNACCEPTED_TYPE",
        parameters = Map(
          "sqlExpr" -> "\"GROUPS BETWEEN 1.5 PRECEDING AND CURRENT ROW\"",
          "location" -> "lower",
          "exprType" -> "\"DECIMAL(2,1)\"",
          "expectedType" -> "\"INT\""),
        queryContext = Array(
          ExpectedContext(
            fragment = "(order by key groups between " +
              "1.5 preceding and current row)",
            start = 23,
            stop = 81)))
    }
  }

  test("GROUPS frame rejects a non-integral offset (interval)") {
    withTempView("t") {
      Seq((1, 1), (2, 2)).toDF("key", "value").createOrReplaceTempView("t")
      checkError(
        exception = intercept[AnalysisException](
          spark.sql(
            "select sum(value) over (order by key groups between " +
              "interval 1 day preceding and current row) from t").collect()),
        condition = "DATATYPE_MISMATCH.SPECIFIED_WINDOW_FRAME_UNACCEPTED_TYPE",
        parameters = Map(
          "sqlExpr" -> "\"GROUPS BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW\"",
          "location" -> "lower",
          "exprType" -> "\"INTERVAL DAY\"",
          "expectedType" -> "\"INT\""),
        queryContext = Array(
          ExpectedContext(
            fragment = "(order by key groups between " +
              "interval 1 day preceding and current row)",
            start = 23,
            stop = 92)))
    }
  }

  test("GROUPS frame rejects a null offset") {
    withTempView("t") {
      Seq((1, 1), (2, 2)).toDF("key", "value").createOrReplaceTempView("t")
      checkError(
        exception = intercept[AnalysisException](
          spark.sql(
            "select sum(value) over (order by key groups between " +
              "cast(null as int) preceding and current row) from t").collect()),
        condition = "DATATYPE_MISMATCH.GROUPS_FRAME_NULL_OFFSET",
        parameters = Map(
          "sqlExpr" -> "\"GROUPS BETWEEN CAST(NULL AS INT) PRECEDING AND CURRENT ROW\"",
          "location" -> "lower"),
        queryContext = Array(
          ExpectedContext(
            fragment = "(order by key groups between " +
              "cast(null as int) preceding and current row)",
            start = 23,
            stop = 95)))
    }
  }

  // Use VALUES because the single-pass resolver does not support CreateViewCommand.
  test("GROUPS frame rejects a null offset with single-pass resolver") {
    withSQLConf(
      SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false",
      SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
      SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
      checkError(
        exception = intercept[AnalysisException](
          spark.sql(
            "select sum(value) over (order by key groups between " +
              "cast(null as int) following and unbounded following) " +
              "from values (1, 1), (2, 2) as t(key, value)").collect()),
        condition = "DATATYPE_MISMATCH.GROUPS_FRAME_NULL_OFFSET",
        parameters = Map(
          "sqlExpr" -> ("\"GROUPS BETWEEN CAST(NULL AS INT) FOLLOWING AND " +
            "UNBOUNDED FOLLOWING\""),
          "location" -> "lower"),
        queryContext = Array(
          ExpectedContext(
            fragment = "(order by key groups between " +
              "cast(null as int) following and unbounded following)",
            start = 23,
            stop = 103)))
    }
  }

  test("GROUPS rejects negative literal, expression and parameter offsets") {
    for (singlePass <- Seq(false, true)) {
      withSQLConf(
        SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false",
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString) {
        for (offset <- Seq("-1", "-2147483648", "1 - 2", ":offset");
            boundary <- Seq(
              s"$offset preceding and current row",
              s"$offset following and unbounded following",
              s"unbounded preceding and $offset preceding",
              s"current row and $offset following")) {
          val error = intercept[AnalysisException] {
            spark.sql(
              s"select sum(v) over (order by k groups between $boundary) " +
                "from values (1, 10), (2, 20) as t(k, v)",
              Map("offset" -> -1)).collect()
          }
          assert(error.getCondition == "DATATYPE_MISMATCH.GROUPS_FRAME_NEGATIVE_OFFSET")
        }
      }
    }
  }

  test("GROUPS accepts zero and positive parameter offsets") {
    for (offset <- Seq(0, 1, Int.MaxValue)) {
      val query =
        "select sum(v) over (order by k groups between :offset preceding and " +
          "current row) from values (1, 10), (1, 20), (2, 30) as t(k, v)"
      checkAnswer(spark.sql(query, Map("offset" -> offset)),
        Seq(Row(30L), Row(30L), Row(if (offset == 0) 30L else 60L)))
    }
  }

  // Returns the Sort, Exchange, and WindowExec counts in the executed plan.
  private def planShape(df: DataFrame): (Int, Int, Int) = {
    val plan = df.queryExecution.executedPlan
    (plan.collect { case s: SortExec => s }.size,
      plan.collect { case e: Exchange => e }.size,
      plan.collect { case w: WindowExec => w }.size)
  }

  // No-offset GROUPS frames have the same results and plan shape as RANGE frames.
  private def checkGroupsFrameMatchesRange(frameBoundary: String): Unit = {
    withTempView("t") {
      // Include ties to exercise peer-group semantics.
      Seq((1, 10), (1, 15), (2, 20), (3, 25), (3, 30), (9, 40))
        .toDF("batch_id", "amount").createOrReplaceTempView("t")
      val groupsDf = spark.sql(
        s"select sum(amount) over (partition by batch_id % 2 order by batch_id " +
          s"groups $frameBoundary) as total from t")
      val rangeDf = spark.sql(
        s"select sum(amount) over (partition by batch_id % 2 order by batch_id " +
          s"range $frameBoundary) as total from t")
      checkAnswer(groupsDf, rangeDf)

      // Disable AQE to inspect operators directly. Build fresh DataFrames because executedPlan
      // is cached and the frames above were created with AQE enabled.
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val groupsShape = planShape(spark.sql(
          s"select sum(amount) over (partition by batch_id % 2 order by batch_id " +
            s"groups $frameBoundary) as total from t"))
        val rangeShape = planShape(spark.sql(
          s"select sum(amount) over (partition by batch_id % 2 order by batch_id " +
            s"range $frameBoundary) as total from t"))
        assert(groupsShape._1 > 0 && groupsShape._2 > 0 && groupsShape._3 > 0,
          s"expected non-zero Sort/Exchange/WindowExec counts, got $groupsShape")
        assert(groupsShape == rangeShape,
          s"GROUPS plan shape differs from RANGE plan shape for '$frameBoundary'")
      }

      // Physical RANGE execution must not change the logical frame representation.
      val frameSqls = groupsDf.queryExecution.analyzed.collect { case w: WindowNode =>
        w.windowExpressions.flatMap(_.collect { case e: WindowExpression =>
          e.windowSpec.frameSpecification.sql
        })
      }.flatten
      assert(frameSqls.nonEmpty, "expected at least one window frame in the analyzed plan")
      frameSqls.foreach { sql =>
        assert(sql.contains("GROUPS"), s"expected GROUPS in frame sql: $sql")
      }

      val explainText = groupsDf.queryExecution.explainString(ExtendedMode)
      assert(explainText.contains("GroupFrame"),
        s"expected GroupFrame in explain output:\n$explainText")
      assert(!explainText.contains("RangeFrame"),
        s"did not expect RangeFrame in explain output:\n$explainText")
    }
  }

  test("GROUPS between unbounded preceding and unbounded following " +
    "matches RANGE") {
    checkGroupsFrameMatchesRange("between unbounded preceding and unbounded following")
  }

  test("GROUPS between unbounded preceding and current row matches RANGE") {
    checkGroupsFrameMatchesRange("between unbounded preceding and current row")
  }

  test("GROUPS between current row and unbounded following matches RANGE") {
    checkGroupsFrameMatchesRange("between current row and unbounded following")
  }

  // Case 2: CURRENT ROW means the current row's entire peer group.
  test("GROUPS between current row and current row (test matrix case 2)") {
    withTempView("t") {
      Seq((1, 10), (1, 15), (2, 20), (3, 25), (3, 30), (9, 40))
        .toDF("batch_id", "amount").createOrReplaceTempView("t")
      checkAnswer(
        spark.sql(
          "select sum(amount) over (order by batch_id " +
            "groups between current row and current row) as total from t"),
        Seq(Row(25), Row(25), Row(20), Row(55), Row(55), Row(40)))
    }
  }

  // Case 6: NULLs form one peer group.
  test("GROUPS with NULL peer group (test matrix case 6)") {
    withTempView("t") {
      Seq((Some(1), 10), (Some(1), 15), (Some(2), 20), (None, 30))
        .toDF("batch_id", "amount").createOrReplaceTempView("t")
      checkAnswer(
        spark.sql(
          "select sum(amount) over (order by batch_id nulls first " +
            "groups between current row and current row) as total from t"),
        Seq(Row(25), Row(25), Row(20), Row(30)))
    }
  }

  // Exercise an offset GroupBoundOrdering with multiple NULL keys in one peer group.
  test("GROUPS offset frame with NULLS FIRST peer group") {
    withTempView("t") {
      Seq(
        (None, 1), (None, 2), (Some(1), 10), (Some(1), 15), (Some(2), 20),
        (Some(3), 25), (Some(3), 30), (Some(9), 40))
        .toDF("batch_id", "amount").createOrReplaceTempView("t")
      val groupsDf = spark.sql(
        "select batch_id, sum(amount) over (order by batch_id nulls first " +
          "groups between 1 preceding and current row) as total from t")
      // Peer groups are {null, null}, {1, 1}, {2}, {3, 3}, {9}.
      checkAnswer(
        groupsDf,
        Seq(
          Row(null, 3), Row(null, 3), Row(1, 28), Row(1, 28), Row(2, 45),
          Row(3, 75), Row(3, 75), Row(9, 95)))
      // RANGE over DENSE_RANK provides an equivalent peer-group oracle.
      checkAnswer(
        groupsDf,
        spark.sql(
          """
            |select batch_id, total from (
            |  select batch_id, amount, sum(amount) over (
            |    order by dense_rank() over (order by batch_id nulls first)
            |    range between 1 preceding and current row) as total
            |  from t
            |)
            |""".stripMargin))
    }
  }

  test("GROUPS offset frame with NULLS LAST peer group") {
    withTempView("t") {
      Seq(
        (Some(1), 10), (Some(1), 15), (Some(2), 20), (Some(3), 25), (Some(3), 30),
        (Some(9), 40), (None, 1), (None, 2))
        .toDF("batch_id", "amount").createOrReplaceTempView("t")
      val groupsDf = spark.sql(
        "select batch_id, sum(amount) over (order by batch_id nulls last " +
          "groups between 1 preceding and current row) as total from t")
      // Peer groups are {1, 1}, {2}, {3, 3}, {9}, {null, null}.
      checkAnswer(
        groupsDf,
        Seq(
          Row(1, 25), Row(1, 25), Row(2, 45), Row(3, 75),
          Row(3, 75), Row(9, 95), Row(null, 43), Row(null, 43)))
      checkAnswer(
        groupsDf,
        spark.sql(
          """
            |select batch_id, total from (
            |  select batch_id, amount, sum(amount) over (
            |    order by dense_rank() over (order by batch_id nulls last)
            |    range between 1 preceding and current row) as total
            |  from t
            |)
            |""".stripMargin))
    }
  }

  // Case 1: `n PRECEDING` counts peer groups, not rows or key distance.
  test("GROUPS between 1 preceding and current row (test matrix case 1)") {
    withTempView("t") {
      Seq((1, 10), (1, 15), (2, 20), (3, 25), (3, 30), (9, 40))
        .toDF("batch_id", "amount").createOrReplaceTempView("t")
      // Include batch_id to verify each total is assigned to the correct rows.
      checkAnswer(
        spark.sql(
          "select batch_id, sum(amount) over (order by batch_id " +
            "groups between 1 preceding and current row) as total from t"),
        Seq(
          Row(1, 25), Row(1, 25), Row(2, 45), Row(3, 75), Row(3, 75), Row(9, 95)))
    }
  }

  // GROUPS offsets remain integral and are not coerced to the ORDER BY type.
  private def specifiedFrame(df: DataFrame): SpecifiedWindowFrame = {
    val frames = df.queryExecution.analyzed.collect { case w: WindowNode =>
      w.windowExpressions.flatMap(_.collect {
        case e: WindowExpression => e.windowSpec.frameSpecification
      })
    }.flatten
    assert(frames.size === 1, s"expected exactly one window frame, got $frames")
    frames.head.asInstanceOf[SpecifiedWindowFrame]
  }

  test("GROUPS offset over a DATE order key is not coerced to an interval") {
    withTempView("t") {
      Seq(("2020-01-01", 10), ("2020-01-01", 15), ("2020-01-02", 20),
        ("2020-01-03", 25), ("2020-01-03", 30), ("2020-01-09", 40))
        .toDF("d", "amount")
        .selectExpr("CAST(d AS DATE) AS d", "amount")
        .createOrReplaceTempView("t")

      val groupsDf = spark.sql(
        "select amount, sum(amount) over (order by d " +
          "groups between 1 preceding and current row) as total from t")
      val groupsFrame = specifiedFrame(groupsDf)
      assert(groupsFrame.frameType === GroupFrame,
        s"expected GroupFrame, got ${groupsFrame.frameType}")
      assert(groupsFrame.lower.dataType == IntegerType,
        s"GROUPS offset must stay an integer, not be cast to the DATE order-key type " +
          s"or an interval, got ${groupsFrame.lower} : ${groupsFrame.lower.dataType}")
      val analyzedText = groupsDf.queryExecution.analyzed.toString
      assert(!analyzedText.toLowerCase(Locale.ROOT).contains("interval"),
        s"GROUPS offset must not be cast to an interval:\n$analyzedText")
      val optimizedText = groupsDf.queryExecution.optimizedPlan.toString
      assert(optimizedText.contains("GroupFrame"),
        s"expected GroupFrame to survive optimization:\n$optimizedText")
      assert(!optimizedText.toLowerCase(Locale.ROOT).contains("interval"),
        s"GROUPS offset must not be cast to an interval after optimization:\n$optimizedText")
      checkAnswer(groupsDf.select("total"),
        Seq(Row(25), Row(25), Row(45), Row(75), Row(75), Row(95)))

      // RANGE over a DATE key uses an interval boundary.
      val rangeDf = spark.sql(
        "select amount, sum(amount) over (order by d " +
          "range between interval 1 day preceding and current row) as total from t")
      val rangeFrame = specifiedFrame(rangeDf)
      assert(rangeFrame.frameType === RangeFrame,
        s"expected RangeFrame, got ${rangeFrame.frameType}")
      assert(rangeFrame.lower.dataType.isInstanceOf[DayTimeIntervalType],
        s"RANGE offset over a DATE order key should be an interval, " +
          s"got ${rangeFrame.lower} : ${rangeFrame.lower.dataType}")
    }
  }

  // With one row per peer group, offset GROUPS and ROWS plans are equivalent.
  test("GROUPS offset frame plan shape has no extra Sort/Exchange/WindowExec") {
    withTempView("t") {
      Seq((1, 10), (2, 15), (3, 20), (4, 25), (5, 30))
        .toDF("batch_id", "amount").createOrReplaceTempView("t")
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val groupsShape = planShape(spark.sql(
          "select sum(amount) over (order by batch_id " +
            "groups between 1 preceding and current row) as total from t"))
        val rowsShape = planShape(spark.sql(
          "select sum(amount) over (order by batch_id " +
            "rows between 1 preceding and current row) as total from t"))
        assert(groupsShape._1 == 1 && groupsShape._3 == 1,
          s"expected exactly one Sort and one WindowExec, got $groupsShape")
        assert(groupsShape == rowsShape,
          s"GROUPS offset plan shape differs from the equivalent ROWS plan shape: " +
            s"groups=$groupsShape rows=$rowsShape")
      }
    }
  }

  // GROUPS uses the generic aggregate path for offset window functions.
  test("first_value/nth_value over a GROUPS frame use peer-group semantics") {
    withTempView("t") {
      Seq((1, 10), (1, 15), (2, 20), (3, 25), (3, 30), (9, 40))
        .toDF("batch_id", "amount").createOrReplaceTempView("t")
      checkAnswer(
        spark.sql(
          "select batch_id, " +
            "first_value(amount) over (order by batch_id " +
            "  groups between 1 preceding and 1 following) as first_amt, " +
            "nth_value(amount, 2) over (order by batch_id " +
            "  groups between 1 preceding and 1 following) as second_amt " +
            "from t"),
        Seq(
          Row(1, 10, 15), Row(1, 10, 15), Row(2, 10, 15),
          Row(3, 20, 25), Row(3, 20, 25), Row(9, 25, 30)))
    }
  }

  // Case 3: `n FOLLOWING` advances one peer group.
  test("GROUPS between current row and 1 following (test matrix case 3)") {
    withTempView("t") {
      Seq((1, 10), (1, 15), (2, 20), (3, 25), (3, 30), (9, 40))
        .toDF("batch_id", "amount").createOrReplaceTempView("t")
      // Include batch_id to verify each total is assigned to the correct rows.
      checkAnswer(
        spark.sql(
          "select batch_id, sum(amount) over (order by batch_id " +
            "groups between current row and 1 following) as total from t"),
        Seq(
          Row(1, 45), Row(1, 45), Row(2, 75), Row(3, 95), Row(3, 95), Row(9, 40)))
    }
  }

  // Case 4: GROUPS offsets support multi-column ordering.
  test("GROUPS with multi-column ORDER BY and an offset (test matrix case 4)") {
    withTempView("t") {
      Seq(
        ("2024-01-01", 1, 10),
        ("2024-01-01", 1, 20),
        ("2024-01-02", 2, 30),
        ("2024-01-03", 3, 45),
        ("2024-01-03", 3, 45))
        .toDF("trade_date", "batch_id", "amount").createOrReplaceTempView("t")
      checkAnswer(
        spark.sql(
          "select sum(amount) over (order by trade_date, batch_id " +
            "groups between 1 preceding and current row) as total from t"),
        // Peer groups are {10, 20}, {30}, and {45, 45}.
        Seq(Row(30), Row(30), Row(60), Row(120), Row(120)))
    }
  }

  // Case 5: offsets count in window-ordering direction, not key-value direction.
  test("GROUPS with DESC order counts in window order (test matrix case 5)") {
    withTempView("t") {
      Seq((1, 10), (1, 15), (2, 20), (3, 25), (3, 30), (9, 40))
        .toDF("batch_id", "amount").createOrReplaceTempView("t")
      // Include batch_id to verify totals follow descending window order.
      checkAnswer(
        spark.sql(
          "select batch_id, sum(amount) over (order by batch_id desc " +
            "groups between 1 preceding and current row) as total from t"),
        Seq(
          Row(9, 40), Row(3, 95), Row(3, 95), Row(2, 75), Row(1, 45), Row(1, 45)))
    }
  }

  // 0 PRECEDING / 0 FOLLOWING must equal CURRENT ROW.
  test("GROUPS 0 PRECEDING / 0 FOLLOWING equal CURRENT ROW") {
    withTempView("t") {
      Seq((1, 10), (1, 15), (2, 20), (3, 25), (3, 30), (9, 40))
        .toDF("batch_id", "amount").createOrReplaceTempView("t")
      val current = spark.sql(
        "select sum(amount) over (order by batch_id " +
          "groups between current row and current row) as total from t")
      val zeroPreceding = spark.sql(
        "select sum(amount) over (order by batch_id " +
          "groups between 0 preceding and current row) as total from t")
      val zeroFollowing = spark.sql(
        "select sum(amount) over (order by batch_id " +
          "groups between current row and 0 following) as total from t")
      val bothZero = spark.sql(
        "select sum(amount) over (order by batch_id " +
          "groups between 0 preceding and 0 following) as total from t")
      checkAnswer(zeroPreceding, current)
      checkAnswer(zeroFollowing, current)
      checkAnswer(bothZero, current)
    }
  }

  // RANGE over DENSE_RANK is equivalent to GROUPS over the original ordering.
  private def denseRankRangeOracle(lower: String, upper: String): DataFrame = {
    spark.sql(
      s"""
         |select total from (
         |  select amount, sum(amount) over (
         |    order by dense_rank() over (order by batch_id)
         |    range between $lower and $upper) as total
         |  from t
         |)
         |""".stripMargin)
  }

  private def checkGroupsAgainstDenseRankOracle(lower: String, upper: String): Unit = {
    val groupsDf = spark.sql(
      s"select sum(amount) over (order by batch_id groups between $lower and $upper) " +
        "as total from t")
    checkAnswer(groupsDf, denseRankRangeOracle(lower, upper))
  }

  test("GROUPS boundary matrix matches the DENSE_RANK + RANGE oracle") {
    withTempView("t") {
      Seq((1, 10), (1, 15), (2, 20), (3, 25), (3, 30), (9, 40))
        .toDF("batch_id", "amount").createOrReplaceTempView("t")
      val lowers = Seq("unbounded preceding", "1 preceding", "current row", "1 following")
      val uppers = Seq("1 preceding", "current row", "1 following", "unbounded following")
      for (lower <- lowers; upper <- uppers) {
        // Only this combination is rejected statically; other reversed bounds yield empty frames.
        val invalid = (lower, upper) match {
          case ("1 following", "1 preceding") => true
          case _ => false
        }
        if (!invalid) {
          withClue(s"lower='$lower' upper='$upper'") {
            checkGroupsAgainstDenseRankOracle(lower, upper)
          }
        } else {
          withClue(s"lower='$lower' upper='$upper'") {
            checkError(
              exception = intercept[AnalysisException] {
                spark.sql(
                  "select sum(amount) over (order by batch_id groups between " +
                    "1 following and 1 preceding) as total from t").collect()
              },
              condition = "DATATYPE_MISMATCH.SPECIFIED_WINDOW_FRAME_WRONG_COMPARISON",
              parameters = Map(
                "sqlExpr" -> "\"GROUPS BETWEEN 1 FOLLOWING AND 1 PRECEDING\"",
                "comparison" -> "less than or equal"),
              queryContext = Array(
                ExpectedContext(
                  fragment = "(order by batch_id groups between " +
                    "1 following and 1 preceding)",
                  start = 24,
                  stop = 85)))
          }
        }
      }
    }
  }

  // Peer-shape edges.
  test("GROUPS peer-shape edge - one group for the whole partition") {
    withTempView("t") {
      Seq((1, 10), (1, 15), (1, 20)).toDF("batch_id", "amount").createOrReplaceTempView("t")
      checkAnswer(
        spark.sql(
          "select sum(amount) over (order by batch_id " +
            "groups between 1 preceding and 1 following) as total from t"),
        Seq(Row(45), Row(45), Row(45)))
    }
  }

  // One group per row: GROUPS must then equal ROWS.
  test("GROUPS peer-shape edge - one group per row equals ROWS") {
    withTempView("t") {
      Seq((1, 10), (2, 15), (3, 20), (4, 25), (5, 30))
        .toDF("batch_id", "amount").createOrReplaceTempView("t")
      val groupsDf = spark.sql(
        "select sum(amount) over (order by batch_id " +
          "groups between 1 preceding and 1 following) as total from t")
      val rowsDf = spark.sql(
        "select sum(amount) over (order by batch_id " +
          "rows between 1 preceding and 1 following) as total from t")
      checkAnswer(groupsDf, rowsDf)
    }
  }

  test("GROUPS peer-shape edge - a single row") {
    withTempView("t") {
      Seq((1, 10)).toDF("batch_id", "amount").createOrReplaceTempView("t")
      checkAnswer(
        spark.sql(
          "select sum(amount) over (order by batch_id " +
            "groups between 1 preceding and 1 following) as total from t"),
        Seq(Row(10)))
    }
  }

  test("GROUPS peer-shape edge - an empty partition") {
    withTempView("t") {
      Seq.empty[(Int, Int)].toDF("batch_id", "amount").createOrReplaceTempView("t")
      checkAnswer(
        spark.sql(
          "select sum(amount) over (order by batch_id " +
            "groups between 1 preceding and 1 following) as total from t"),
        Seq.empty[Row])
    }
  }

  // A frame entirely outside the partition is an empty frame, not an error: aggregates return
  // their empty-input value (SUM -> NULL, COUNT -> 0).
  test("GROUPS frame entirely outside the partition returns empty-input value") {
    withTempView("t") {
      Seq((1, 10), (2, 20), (3, 30)).toDF("batch_id", "amount").createOrReplaceTempView("t")
      checkAnswer(
        spark.sql(
          "select sum(amount) over (order by batch_id " +
            "groups between 5 following and 6 following) as sum_total, " +
            "count(amount) over (order by batch_id " +
            "groups between 5 following and 6 following) as count_total from t"),
        Seq(Row(null, 0), Row(null, 0), Row(null, 0)))
      checkAnswer(
        spark.sql(
          "select sum(amount) over (order by batch_id " +
            "groups between 6 preceding and 5 preceding) as sum_total, " +
            "count(amount) over (order by batch_id " +
            "groups between 6 preceding and 5 preceding) as count_total from t"),
        Seq(Row(null, 0), Row(null, 0), Row(null, 0)))
    }
  }

  // Compare random inputs, including NULL keys, with the DENSE_RANK plus RANGE oracle.
  test("GROUPS randomised differential oracle vs DENSE_RANK + RANGE") {
    val rand = new scala.util.Random(58980L)
    withTempView("t") {
      val rows = (1 to 200).map { _ =>
        val batchId = if (rand.nextInt(10) == 0) None else Some(rand.nextInt(5))
        (batchId, rand.nextInt(30))
      }
      rows.toDF("batch_id", "amount").createOrReplaceTempView("t")
      val boundaries = Seq(
        ("unbounded preceding", "current row"),
        ("2 preceding", "current row"),
        ("current row", "2 following"),
        ("1 preceding", "1 following"),
        ("current row", "unbounded following"))
      boundaries.foreach { case (lower, upper) =>
        withClue(s"lower='$lower' upper='$upper'") {
          checkGroupsAgainstDenseRankOracle(lower, upper)
        }
      }
    }
  }

  // Frames, and the peer-group cursors in their bound orderings, are reused across a task's
  // partitions. Every other GROUPS test runs on one partition, or on partitions of identical
  // group shape, so none would notice group structure carried over from the previous one.
  test("GROUPS over partitions with differing peer-group structure") {
    withTempView("t") {
      // pk=0: every row its own peer group. pk=1: one peer group covering the partition.
      // pk=2: mixed group sizes. pk=3: a single row.
      val rows =
        (0 until 12).map(i => (0, i, i + 1)) ++
          (0 until 12).map(i => (1, 7, i + 1)) ++
          Seq((2, 0, 1), (2, 0, 2), (2, 0, 3), (2, 1, 4), (2, 2, 5), (2, 2, 6), (2, 3, 7)) ++
          Seq((3, 0, 1))
      rows.toDF("pk", "batch_id", "amount").createOrReplaceTempView("t")
      val boundaries = Seq(
        ("unbounded preceding", "current row"),
        ("2 preceding", "current row"),
        ("current row", "2 following"),
        ("1 preceding", "1 following"),
        ("2 preceding", "1 preceding"),
        ("current row", "unbounded following"))
      // One shuffle partition puts all four window partitions through one frame instance.
      // `minPartitionRows = 8` also splits them across execution paths: the 12-row partitions
      // take the segment tree, the 7- and 1-row ones its fallback frame, which shares its
      // bound orderings.
      val configs = Seq(
        Seq(SQLConf.SHUFFLE_PARTITIONS.key -> "1"),
        Seq(SQLConf.SHUFFLE_PARTITIONS.key -> "1",
          SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true",
          SQLConf.WINDOW_SEGMENT_TREE_MIN_PARTITION_ROWS.key -> "8"))
      for (config <- configs; (lower, upper) <- boundaries) {
        withSQLConf(config: _*) {
          withClue(s"config=$config lower='$lower' upper='$upper'") {
            checkAnswer(
              spark.sql(
                s"select pk, sum(amount) over (partition by pk order by batch_id " +
                  s"groups between $lower and $upper) as total from t"),
              spark.sql(
                s"""
                   |select pk, total from (
                   |  select pk, sum(amount) over (
                   |    partition by pk
                   |    order by dense_rank() over (partition by pk order by batch_id)
                   |    range between $lower and $upper) as total
                   |  from t
                   |)
                   |""".stripMargin))
          }
        }
      }
    }
  }

  // Force multiple spills and compare with in-memory execution.
  test("GROUPS results are unaffected by partition spilling") {
    withTempView("t") {
      val rows = (1 to 500).map(i => (i % 40, i))
      rows.toDF("batch_id", "amount").createOrReplaceTempView("t")
      val query =
        "select sum(amount) over (order by batch_id " +
          "groups between 2 preceding and 2 following) as total from t"
      val noSpill = spark.sql(query).collect()
      val spilled = withSQLConf(
        SQLConf.WINDOW_EXEC_BUFFER_SPILL_THRESHOLD.key -> "25",
        SQLConf.WINDOW_EXEC_BUFFER_IN_MEMORY_THRESHOLD.key -> "1") {
        spark.sql(query).collect()
      }
      assert(spilled.sameElements(noSpill),
        s"spilled results differ from non-spilling results:\n" +
          s"no-spill: ${noSpill.mkString(",")}\nspilled: ${spilled.mkString(",")}")
    }
  }

  // Verify that GROUPS and RANGE use the same peer equality for special order-key values.
  test("GROUPS peer groups for float special values match RANGE CURRENT ROW") {
    withTempView("t") {
      Seq(0.0, -0.0, Double.NaN, Double.NaN, 1.0)
        .zipWithIndex.map { case (v, i) => (v, i) }
        .toDF("value", "amount").createOrReplaceTempView("t")
      // Signed zeros are peers, as are NaN values.
      checkAnswer(
        spark.sql(
          "select sum(amount) over (order by value " +
            "groups between current row and current row) as total from t"),
        spark.sql(
          "select sum(amount) over (order by value " +
            "range between current row and current row) as total from t"))
      // Check offset behavior against the DENSE_RANK plus RANGE oracle.
      checkAnswer(
        spark.sql(
          "select sum(amount) over (order by value " +
            "groups between 1 preceding and current row) as total from t"),
        spark.sql(
          """
            |select total from (
            |  select amount, sum(amount) over (
            |    order by dense_rank() over (order by value)
            |    range between 1 preceding and current row) as total
            |  from t
            |)
            |""".stripMargin))
    }
  }

  test("GROUPS peer groups for collated strings match RANGE CURRENT ROW") {
    withTempView("t") {
      Seq(("abc", 1), ("ABC", 2), ("abd", 3))
        .toDF("value", "amount").createOrReplaceTempView("t")
      spark.sql(
        "select value collate UTF8_LCASE as value, amount from t").createOrReplaceTempView("tc")
      checkAnswer(
        spark.sql(
          "select sum(amount) over (order by value " +
            "groups between current row and current row) as total from tc"),
        spark.sql(
          "select sum(amount) over (order by value " +
            "range between current row and current row) as total from tc"))
      checkAnswer(
        spark.sql(
          "select sum(amount) over (order by value " +
            "groups between 1 preceding and current row) as total from tc"),
        spark.sql(
          """
            |select total from (
            |  select amount, sum(amount) over (
            |    order by dense_rank() over (order by value)
            |    range between 1 preceding and current row) as total
            |  from tc
            |)
            |""".stripMargin))
    }
  }

  test("CREATE VIEW over a no-offset GROUPS query round-trips") {
    // Persistent views cannot reference temporary views.
    withTable("t") {
      withView("v") {
        Seq((1, 10), (1, 15), (2, 20), (3, 25), (3, 30), (9, 40))
          .toDF("batch_id", "amount").write.saveAsTable("t")
        spark.sql(
          "create view v as select batch_id, sum(amount) over (order by batch_id " +
            "groups between current row and current row) as total from t")
        val storedText =
          spark.sessionState.catalog.getTempViewOrPermanentTableMetadata(
            TableIdentifier("v")).viewText.get
        // View text preserves the keyword's original case.
        assert(storedText.toUpperCase(Locale.ROOT).contains("GROUPS"), storedText)
        // Re-create the view to verify that its stored text parses.
        withView("v2") {
          spark.sql(s"create view v2 as $storedText")
          checkAnswer(spark.table("v2"), spark.table("v"))
        }
        checkAnswer(
          spark.table("v").orderBy("batch_id", "total"),
          Seq(
            Row(1, 25), Row(1, 25), Row(2, 20), Row(3, 55), Row(3, 55), Row(9, 40)))
      }
    }
  }

  test("no regression in existing ROWS and RANGE frame results") {
    withTempView("t") {
      Seq((1, 10, 10), (2, 20, 20), (3, 30, 10))
        .toDF("key", "row_amount", "range_amount")
        .createOrReplaceTempView("t")
      checkAnswer(
        spark.sql(
          """
            |select
            |  sum(row_amount) over (
            |    order by key rows between unbounded preceding and current row) as rows_total,
            |  sum(range_amount) over (
            |    order by key range between unbounded preceding and current row) as range_total
            |from t
            |""".stripMargin),
        Row(10, 10) :: Row(30, 30) :: Row(60, 40) :: Nil)
    }
  }
}
