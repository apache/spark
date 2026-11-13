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
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.CalendarIntervalType

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

  test("SPARK-61009: range frame offset that does not fit in the integral order type") {
    val df = spark.range(0, 6).select(
      $"id".cast("int").as("id"),
      ($"id" * 10).cast("tinyint").as("b"))
    Seq(true, false).foreach { ansiEnabled =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansiEnabled.toString) {
        // The frame of b holds the values in [b - 200, b].
        checkAnswer(
          df.select($"id",
            count("*").over(Window.orderBy($"b").rangeBetween(-200, Window.currentRow))),
          (0 until 6).map(i => Row(i, i + 1)))
        // The frame of b holds the values in [b, b + 100], and b + 100 overflows TINYINT.
        checkAnswer(
          df.select($"id",
            count("*").over(Window.orderBy($"b".desc).rangeBetween(-100, Window.currentRow))),
          (0 until 6).map(i => Row(i, 6 - i)))
      }
    }
  }

  test("SPARK-61009: range frames ordered by an integral type near the limits of the type") {
    val types: Seq[(String, BigInt, BigInt, BigInt => Any)] = Seq(
      ("tinyint", BigInt(Byte.MinValue), BigInt(Byte.MaxValue), _.toByte),
      ("smallint", BigInt(Short.MinValue), BigInt(Short.MaxValue), _.toShort),
      ("int", BigInt(Int.MinValue), BigInt(Int.MaxValue), _.toInt),
      ("bigint", BigInt(Long.MinValue), BigInt(Long.MaxValue), _.toLong))
    val offsets = Seq(1L, 100L, 200L, 70000L, 3000000000L, Long.MaxValue).map(BigInt(_))
    // The offsets combined with UNBOUNDED, so that the bounds overflow each of the types.
    val unboundedOffsets = Seq(200L, 3000000000L, Long.MaxValue).map(BigInt(_))
    val zero = Some(BigInt(0))
    // The (lower, upper) offsets of the frames: a negative offset is PRECEDING, a positive one
    // FOLLOWING, and None is UNBOUNDED.
    val frames: Seq[(Option[BigInt], Option[BigInt])] = offsets.flatMap { o =>
      Seq((Some(-o), zero), (zero, Some(o)), (Some(-o), Some(o)))
    } ++ unboundedOffsets.flatMap { o =>
      Seq((None, Some(o)), (None, Some(-o)), (Some(-o), None), (Some(o), None))
    } ++ Seq(
      (Some(BigInt(100)), Some(BigInt(200))),
      (Some(BigInt(-200)), Some(BigInt(-100))),
      (Some(BigInt(1)), Some(BigInt(Long.MaxValue))),
      (Some(-BigInt(Long.MaxValue)), Some(BigInt(-1))))
    def boundary(offset: Option[BigInt], unbounded: String): String = offset match {
      case None => unbounded
      case Some(o) if o < 0 => s"${-o} PRECEDING"
      case Some(o) if o > 0 => s"$o FOLLOWING"
      case _ => "CURRENT ROW"
    }
    def windows(order: String): Seq[String] = frames.map { case (lower, upper) =>
      s"(ORDER BY v $order RANGE BETWEEN ${boundary(lower, "UNBOUNDED PRECEDING")} AND " +
        s"${boundary(upper, "UNBOUNDED FOLLOWING")})"
    }
    // (order, ascending, nullsFirst)
    val orders = Seq(
      ("ASC NULLS FIRST", true, true),
      ("ASC NULLS LAST", true, false),
      ("DESC NULLS FIRST", false, true),
      ("DESC NULLS LAST", false, false))

    types.foreach { case (typeName, min, max, toType) =>
      val values = Seq(min, min + 1, BigInt(-100), BigInt(-1), BigInt(0), BigInt(1),
        BigInt(100), max - 1, max).filter(v => v >= min && v <= max).distinct
      val df = (values.map(v => Option(v.toLong)) :+ None).toDF("v")
        .select($"v".cast(typeName).as("v"))
      // The non-NULL values in the frame of a value, following the definition of a range frame,
      // and whether the frame holds the NULL value.
      def frame(
          lower: Option[BigInt],
          upper: Option[BigInt],
          ascending: Boolean,
          nullsFirst: Boolean,
          v: Option[BigInt]): (Seq[BigInt], Boolean) = v match {
        case Some(v) =>
          val from = if (ascending) lower.map(v + _) else upper.map(v - _)
          val to = if (ascending) upper.map(v + _) else lower.map(v - _)
          (values.filter(r => from.forall(r >= _) && to.forall(r <= _)),
            if (nullsFirst) lower.isEmpty else upper.isEmpty)
        case None =>
          // The bounds of a NULL value are NULL, so a bounded side of its frame ends at the
          // NULL values. The other values are in the frame only if the side of the frame where
          // they are sorted is unbounded.
          (if (if (nullsFirst) upper.isEmpty else lower.isEmpty) values else Nil, true)
      }
      withTempView("t") {
        df.createOrReplaceTempView("t")
        orders.foreach { case (order, ascending, nullsFirst) =>
          val rowFrames = (values.map(Option(_)) :+ None).map { v =>
            v.map(toType).orNull -> frames.map { case (lower, upper) =>
              frame(lower, upper, ascending, nullsFirst, v)
            }
          }
          def count(vs: Seq[BigInt], hasNull: Boolean): Long = vs.size + (if (hasNull) 1 else 0)
          def minMax(vs: Seq[BigInt]): Seq[Any] =
            Seq(vs.minOption.map(toType).orNull, vs.maxOption.map(toType).orNull)
          // Each query is analyzed and planned under the configuration it is checked with.
          def query: DataFrame = sql("SELECT v, " + windows(order).flatMap { w =>
            Seq(s"count(*) OVER $w", s"min(v) OVER $w", s"max(v) OVER $w")
          }.mkString(", ") + " FROM t")
          val expected = rowFrames.map { case (v, fs) =>
            Row.fromSeq(v +: fs.flatMap { case (vs, hasNull) =>
              count(vs, hasNull) +: minMax(vs)
            })
          }
          def countQuery: DataFrame =
            sql("SELECT v, " + windows(order).map(w => s"count(*) OVER $w").mkString(", ") +
              " FROM t")
          val expectedCounts = rowFrames.map { case (v, fs) =>
            Row.fromSeq(v +: fs.map { case (vs, hasNull) => count(vs, hasNull) })
          }
          def minMaxQuery: DataFrame = sql("SELECT v, " + windows(order).flatMap { w =>
            Seq(s"min(v) OVER $w", s"max(v) OVER $w")
          }.mkString(", ") + " FROM t")
          val expectedMinMax = rowFrames.map { case (v, fs) =>
            Row.fromSeq(v +: fs.flatMap { case (vs, _) => minMax(vs) })
          }
          Seq(true, false).foreach { ansiEnabled =>
            withSQLConf(SQLConf.ANSI_ENABLED.key -> ansiEnabled.toString) {
              checkAnswer(query, expected)
            }
          }
          // The segment tree frames and the monotonic deque frames (for min and max only) use the
          // same bounds.
          def checkFrames(df: DataFrame, expectedRows: Seq[Row], metric: String): Unit = {
            checkAnswer(df, expectedRows)
            val window = df.queryExecution.executedPlan.collectFirst { case w: WindowExec => w }
            assert(window.exists(_.metrics(metric).value > 0), s"$metric is 0")
          }
          withSQLConf(
              SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
              SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true",
              SQLConf.WINDOW_SEGMENT_TREE_MIN_PARTITION_ROWS.key -> "1") {
            checkFrames(countQuery, expectedCounts, "numSegmentTreeFrames")
          }
          withSQLConf(
              SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
              SQLConf.WINDOW_MONOTONIC_DEQUE_ENABLED.key -> "true") {
            checkFrames(minMaxQuery, expectedMinMax, "numMonotonicDequeFrames")
          }
        }
      }
    }
  }

  test("SPARK-61009: range frames with a NULL integral offset of a wider type than the order") {
    val frames = Seq(
      "{null} PRECEDING AND CURRENT ROW",
      "CURRENT ROW AND {null} FOLLOWING",
      "{null} PRECEDING AND {one} FOLLOWING",
      "{one} PRECEDING AND {null} FOLLOWING")
    val orders = Seq("ASC NULLS FIRST", "ASC NULLS LAST", "DESC NULLS FIRST", "DESC NULLS LAST")
    Seq[(String, Int => Any)](
        ("tinyint", _.toByte),
        ("smallint", _.toShort),
        ("int", identity)).foreach { case (typeName, toType) =>
      withTempView("t") {
        Seq(Some(1), Some(2), Some(5), None).toDF("v")
          .select($"v".cast(typeName).as("v"))
          .createOrReplaceTempView("t")
        // The bounds are NULL, so a bounded side of a frame ends at the NULL values.
        checkAnswer(
          sql("SELECT v, " + Seq(
            "ASC NULLS FIRST RANGE BETWEEN CAST(NULL AS BIGINT) PRECEDING AND CURRENT ROW",
            "ASC NULLS LAST RANGE BETWEEN CAST(NULL AS BIGINT) PRECEDING AND CURRENT ROW",
            "DESC NULLS FIRST RANGE BETWEEN CURRENT ROW AND CAST(NULL AS BIGINT) FOLLOWING",
            "DESC NULLS LAST RANGE BETWEEN CURRENT ROW AND CAST(NULL AS BIGINT) FOLLOWING"
          ).map(w => s"count(*) OVER (ORDER BY v $w)").mkString(", ") + " FROM t"),
          Seq(Row(null, 1L, 1L, 1L, 1L), Row(toType(1), 2L, 0L, 0L, 2L),
            Row(toType(2), 3L, 0L, 0L, 3L), Row(toType(5), 4L, 0L, 0L, 4L)))
        def query(nullOffset: String, oneOffset: String): DataFrame = {
          val windows = for (order <- orders; frame <- frames) yield {
            val bounds = frame.replace("{null}", nullOffset).replace("{one}", oneOffset)
            s"(ORDER BY v $order RANGE BETWEEN $bounds)"
          }
          sql("SELECT v, " + windows.flatMap { w =>
            Seq(s"count(*) OVER $w", s"min(v) OVER $w", s"max(v) OVER $w")
          }.mkString(", ") + " FROM t")
        }
        // A NULL offset makes the bounds NULL, whatever its type. The frames are checked against
        // the same frames with offsets of the order type.
        val expected = query(s"CAST(NULL AS $typeName)", s"CAST(1 AS $typeName)").collect()
        Seq(true, false).foreach { ansiEnabled =>
          withSQLConf(SQLConf.ANSI_ENABLED.key -> ansiEnabled.toString) {
            checkAnswer(query("CAST(NULL AS BIGINT)", "1"), expected.toSeq)
          }
        }
        withSQLConf(
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
            SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true",
            SQLConf.WINDOW_SEGMENT_TREE_MIN_PARTITION_ROWS.key -> "1",
            SQLConf.WINDOW_MONOTONIC_DEQUE_ENABLED.key -> "true") {
          checkAnswer(query("CAST(NULL AS BIGINT)", "1"), expected.toSeq)
        }
      }
    }
  }

  test("SPARK-61009: equal range frames with integral offsets of different types share a frame") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true",
        SQLConf.WINDOW_SEGMENT_TREE_MIN_PARTITION_ROWS.key -> "1") {
      Seq[(String, Int => Any)](
          ("tinyint", _.toByte),
          ("smallint", _.toShort),
          ("int", identity)).foreach { case (typeName, toType) =>
        val df = Seq(1, 2, 3, 4).toDF("v").select($"v".cast(typeName).as("v"))
        // The offsets of the frames have the order type, INT, BIGINT and BIGINT.
        val result = df.select(
          $"v",
          expr(s"count(*) OVER (ORDER BY v RANGE BETWEEN CAST(1 AS $typeName) PRECEDING AND " +
            "CURRENT ROW)"),
          expr("sum(v) OVER (ORDER BY v RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)"),
          expr("min(v) OVER (ORDER BY v RANGE BETWEEN CAST(1 AS BIGINT) PRECEDING AND " +
            "CURRENT ROW)"),
          max($"v").over(Window.orderBy($"v").rangeBetween(-1, Window.currentRow)))
        // The frame of v holds the values in [v - 1, v].
        checkAnswer(result, (1 to 4).map { v =>
          Row(toType(v), if (v == 1) 1L else 2L, (2 * v - 1).toLong, toType(1.max(v - 1)),
            toType(v))
        })
        val window = result.queryExecution.executedPlan.collectFirst { case w: WindowExec => w }
        assert(window.get.metrics("numSegmentTreeFrames").value == 1)
      }
    }
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
