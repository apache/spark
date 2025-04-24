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

import org.apache.spark.sql.catalyst.expressions.{Literal, NonFoldableLiteral}
import org.apache.spark.sql.catalyst.optimizer.EliminateWindowPartitions
import org.apache.spark.sql.catalyst.plans.logical.{Window => WindowNode}
import org.apache.spark.sql.classic.ExpressionColumnNode
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.CalendarIntervalType

/**
 * Window frame testing for DataFrame API.
 */
class DataFrameWindowFramesSuite extends QueryTest with SharedSparkSession {
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
}
