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

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Window frame testing for DataFrame API.
 */
class DataFrameWindowFramesSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

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

    val e = intercept[AnalysisException](
      df.select(
        $"key",
        count("key").over(
          Window.partitionBy($"value").orderBy($"key").rowsBetween(0, 2147483648L))))
    assert(e.message.contains("Boundary end is not a valid integer: 2147483648"))
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
      errorClass = "DATATYPE_MISMATCH",
      errorSubClass = Some("RANGE_FRAME_MULTI_ORDER"),
      parameters = Map(
        "orderSpec" -> """key#\d+ ASC NULLS FIRST,value#\d+ ASC NULLS FIRST""",
        "sqlExpr" -> (""""\(ORDER BY key ASC NULLS FIRST, value ASC NULLS FIRST RANGE """ +
          """BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING\)"""")
      ),
      matchPVals = true
    )

    checkError(
      exception = intercept[AnalysisException](
        df.select(
          min("key").over(window.rangeBetween(-1, Window.unboundedFollowing)))
      ),
      errorClass = "DATATYPE_MISMATCH",
      errorSubClass = Some("RANGE_FRAME_MULTI_ORDER"),
      parameters = Map(
        "orderSpec" -> """key#\d+ ASC NULLS FIRST,value#\d+ ASC NULLS FIRST""",
        "sqlExpr" -> (""""\(ORDER BY key ASC NULLS FIRST, value ASC NULLS FIRST RANGE """ +
          """BETWEEN -1 FOLLOWING AND UNBOUNDED FOLLOWING\)"""")
      ),
      matchPVals = true
    )

    checkError(
      exception = intercept[AnalysisException](
        df.select(
          min("key").over(window.rangeBetween(-1, 1)))
      ),
      errorClass = "DATATYPE_MISMATCH",
      errorSubClass = Some("RANGE_FRAME_MULTI_ORDER"),
      parameters = Map(
        "orderSpec" -> """key#\d+ ASC NULLS FIRST,value#\d+ ASC NULLS FIRST""",
        "sqlExpr" -> (""""\(ORDER BY key ASC NULLS FIRST, value ASC NULLS FIRST RANGE """ +
          """BETWEEN -1 FOLLOWING AND 1 FOLLOWING\)"""")
      ),
      matchPVals = true
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
      errorClass = "DATATYPE_MISMATCH",
      errorSubClass = Some("SPECIFIED_WINDOW_FRAME_UNACCEPTED_TYPE"),
      parameters = Map(
        "location" -> "upper",
        "exprType" -> "\"STRING\"",
        "expectedType" -> ("(\"NUMERIC\" or \"INTERVAL DAY TO SECOND\" or \"INTERVAL YEAR " +
          "TO MONTH\" or \"INTERVAL\")"),
        "sqlExpr" -> "\"RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING\""
      )
    )

    checkError(
      exception = intercept[AnalysisException](
        df.select(
          min("value").over(window.rangeBetween(-1, Window.unboundedFollowing)))
      ),
      errorClass = "DATATYPE_MISMATCH",
      errorSubClass = Some("SPECIFIED_WINDOW_FRAME_UNACCEPTED_TYPE"),
      parameters = Map(
        "location" -> "lower",
        "exprType" -> "\"STRING\"",
        "expectedType" -> ("(\"NUMERIC\" or \"INTERVAL DAY TO SECOND\" or \"INTERVAL YEAR " +
          "TO MONTH\" or \"INTERVAL\")"),
        "sqlExpr" -> "\"RANGE BETWEEN -1 FOLLOWING AND UNBOUNDED FOLLOWING\""
      )
    )

    checkError(
      exception = intercept[AnalysisException](
        df.select(
          min("value").over(window.rangeBetween(-1, 1)))
      ),
      errorClass = "DATATYPE_MISMATCH",
      errorSubClass = Some("SPECIFIED_WINDOW_FRAME_UNACCEPTED_TYPE"),
      parameters = Map(
        "location" -> "lower",
        "exprType" -> "\"STRING\"",
        "expectedType" -> ("(\"NUMERIC\" or \"INTERVAL DAY TO SECOND\" or \"INTERVAL YEAR " +
          "TO MONTH\" or \"INTERVAL\")"),
        "sqlExpr" -> "\"RANGE BETWEEN -1 FOLLOWING AND 1 FOLLOWING\""
      )
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
}
