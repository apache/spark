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

import java.sql.{Date, Timestamp}

import org.apache.spark.TestUtils.{assertNotSpilled, assertSpilled}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

/**
 * Window function testing for DataFrame API.
 */
class DataFrameWindowFunctionsSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("Window partitionBy cardinality, no order by") {
    val df = Seq(("a", 1), ("a", 2), ("b", 4), ("b", 4)).toDF("key", "value")

    checkAnswer(
      df.select(
        sum("value").over(),
        sum("value").over(Window.partitionBy("key")),
        sum("value").over(Window.partitionBy("key", "value")),
        sum("value").over(Window.partitionBy("value", "key"))),
      Row(11, 3, 1, 1) :: Row(11, 3, 2, 2) :: Row(11, 8, 8, 8) :: Row(11, 8, 8, 8) :: Nil)
  }

  test("Null value in partition key") {
    val df = Seq(("a", 1), ("a", 2), (null, 4), (null, 8)).toDF("key", "value")

    checkAnswer(
      df.select(
        'value,
        sum("value").over(Window.partitionBy("key"))),
      Row(1, 3) :: Row(2, 3) :: Row(4, 12) :: Row(8, 12) :: Nil)
  }

  test("Same partitionBy multiple times") {
    val df = Seq(("a", 1), ("a", 2), ("b", 4), ("b", 8)).toDF("key", "value")

    checkAnswer(
      df.select(
        sum("value").over(Window.partitionBy("key", "key"))),
      Row(3) :: Row(3) :: Row(12) :: Row(12) :: Nil)
  }

  test("Multiple orderBy clauses") {
    val df = Seq(("a", "x", 1), ("a", "y", 2), ("b", "y", 3), ("b", "x", 4)).toDF("k1", "k2", "v")

    checkAnswer(
      df.select(
        'v,
        rank.over(Window.orderBy("k1", "k2")),
        rank.over(Window.orderBy("k2", "k1"))),
      Row(1, 1, 1) :: Row(2, 2, 3) :: Row(3, 4, 4) :: Row(4, 3, 2) :: Nil)
  }

  test("Multiple orderBy clauses with desc") {
    val df = Seq(("a", "x", 1), ("a", "y", 2), ("b", "y", 3), ("b", "x", 4)).toDF("k1", "k2", "v")

    checkAnswer(
      df.select(
        'v,
        rank.over(Window.orderBy($"k1".desc, $"k2")),
        rank.over(Window.orderBy($"k1", $"k2".desc)),
        rank.over(Window.orderBy($"k1".desc, $"k2".desc))),
      Row(1, 3, 2, 4) :: Row(2, 4, 1, 3) :: Row(3, 2, 3, 1) :: Row(4, 1, 4, 2) :: Nil)
  }

  test("Null values sorted to first by asc, last by desc ordering by default") {
    val df = Seq((null, 1), ("a", 2), ("b", 3)).toDF("key", "value")

    checkAnswer(
      df.select(
        'value,
        rank.over(Window.orderBy($"key")),
        rank.over(Window.orderBy($"key".desc))),
      Row(1, 1, 3) :: Row(2, 2, 2) :: Row(3, 3, 1) :: Nil)
  }

  test("Ordering of null values can be explicitly controlled") {
    val df = Seq((null, 1), ("a", 2), ("b", 3)).toDF("key", "value")

    checkAnswer(
      df.select(
        'value,
        rank.over(Window.orderBy($"key".asc_nulls_first)),
        rank.over(Window.orderBy($"key".asc_nulls_last)),
        rank.over(Window.orderBy($"key".desc_nulls_first)),
        rank.over(Window.orderBy($"key".desc_nulls_last))),
      Row(1, 1, 3, 1, 3) :: Row(2, 2, 1, 3, 2) :: Row(3, 3, 2, 2, 1) :: Nil)
  }

  test("Order by without frame defaults to range between unbounded_preceding - current_row") {
    val df = Seq(8, 2, 2, 1).toDF("value")

    checkAnswer(
      df.select(
        'value,
        sum("value").over(Window.orderBy('value))),
      Row(1, 1) :: Row(2, 5) :: Row(2, 5) :: Row(8, 13) :: Nil)
  }

  test("Without order and frame default is row btw unbounded_preceding - unbounded_following") {
    val df = Seq(8, 4, 2, 1).toDF("value")

    checkAnswer(
      df.select(
        'value,
        sum("value").over()),
      Row(1, 15) :: Row(2, 15) :: Row(4, 15) :: Row(8, 15) :: Nil)
  }

  test("Partitioning and ordering don't fail on empty dataframe") {
    val emptyDf = Seq(("k", 1)).toDF("key", "value").limit(0)

    checkAnswer(
      emptyDf.select(
        sum("value").over(Window.partitionBy('key)),
        sum("value").over(Window.orderBy('value))),
      Nil)
  }

  test("orderBy with expression using a column") {
    val df = Seq("a", "abc", "ab", "abcd").toDF("v")

    checkAnswer(
      df.select(
        'v,
        rank.over(
          Window.orderBy(length($"v")))),
      Row("a", 1) :: Row("ab", 2) :: Row("abc", 3) :: Row("abcd", 4) :: Nil)
  }

  test("orderBy with expression using multiple columns") {
    val df = Seq((2, 5), (1, 2), (4, 1)).toDF("a", "b")

    checkAnswer(
      df.select(
        'a,
        sum("a").over(
          Window.orderBy($"a" + $"b")),
        sum("a").over(
          Window.orderBy(($"a" + $"b").desc))),
      Row(1, 1, 7) :: Row(2, 7, 2) :: Row(4, 5, 6) :: Nil)
  }

  test("partitionBy with expression using multiple columns") {
    val df = Seq((2, 2), (1, 3), (4, 1)).toDF("a", "b")

    checkAnswer(
      df.select(
        'a,
        sum("a").over(
          Window.partitionBy($"a" + $"b"))),
      Row(1, 3) :: Row(2, 3) :: Row(4, 4) :: Nil)
  }

  test("ranking, analytic and aggregate functions in the same window") {
    val df = Seq((1, 4), (1, 2), (2, 8), (2, 16)).toDF("a", "b")
    val w = Window.partitionBy("a").orderBy("b")

    checkAnswer(
      df.select(
        'b,
        first("b").over(w),
        lag("b", 1).over(w),
        sum("b").over(w)),
      Row(2, 2, null, 2) :: Row(4, 2, 2, 6) :: Row(8, 8, null, 8) :: Row(16, 8, 8, 24) :: Nil)
  }

  test("partitionBy can use a window function") {
    val df = Seq(("a", 2), ("b", 1), ("c", 4), ("d", 8)).toDF("k", "v")

    checkAnswer(
      df.select(
        'k,
        sum("v").over(Window.partitionBy(rank.over(Window.orderBy("v")) mod 2))),
      Row("a", 10) :: Row("b", 5) :: Row("c", 5) :: Row("d", 10) :: Nil)
  }

  test("orderBy can use a window function") {
    val df = Seq(2, 1, 4, 3).toDF("v")

    checkAnswer(
      df.select(
        'v,
        rank.over(Window.orderBy(rank.over(Window.orderBy("v")).desc))),
      Row(1, 4) :: Row(2, 3) :: Row(3, 2) :: Row(4, 1) :: Nil)
  }

  test("filter by column produced by window function") {
    val df = Seq(("a", 1), ("a", 2), ("b", 4), ("b", 8)).toDF("k", "v")

    checkAnswer(
      df.select('v, avg('v).over(Window.partitionBy('k)).as("v_avg")).where('v > 'v_avg),
      Row(2, 1.5) :: Row(8, 6.0) :: Nil)
  }

  // it can be enabled when SPARK-16418 is resolved
  ignore("window functions in filter") {
    val df = Seq(("a", 1), ("a", 2), ("b", 4), ("b", 8)).toDF("k", "v")

    checkAnswer(
      df.select('v).where('v > avg('v).over(Window.partitionBy('k))),
      Row(2) :: Row(8) :: Nil)
  }

  test("partitionBy referring to a non-existent column causes failure") {
    val df = Seq(1).toDF("value")

    val e = intercept[AnalysisException](
      df.select(
        avg("value").over(Window.partitionBy($"key"))))
    assert(e.message.contains("cannot resolve '`key`' given input columns: [value]"))
  }

  test("orderBy referring to a non-existent column causes failure") {
    val df = Seq(1).toDF("value")

    val e = intercept[AnalysisException](
      df.select(
        avg("value").over(Window.orderBy($"key"))))
    assert(e.message.contains("cannot resolve '`key`' given input columns: [value]"))
  }

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

  test("rank functions in unspecific window") {
    val df = Seq((1, "1"), (2, "2"), (1, "2"), (2, "2")).toDF("key", "value")
    df.createOrReplaceTempView("window_table")
    checkAnswer(
      df.select(
        $"key",
        max("key").over(Window.partitionBy("value").orderBy("key")),
        min("key").over(Window.partitionBy("value").orderBy("key")),
        mean("key").over(Window.partitionBy("value").orderBy("key")),
        count("key").over(Window.partitionBy("value").orderBy("key")),
        sum("key").over(Window.partitionBy("value").orderBy("key")),
        ntile(2).over(Window.partitionBy("value").orderBy("key")),
        row_number().over(Window.partitionBy("value").orderBy("key")),
        dense_rank().over(Window.partitionBy("value").orderBy("key")),
        rank().over(Window.partitionBy("value").orderBy("key")),
        cume_dist().over(Window.partitionBy("value").orderBy("key")),
        percent_rank().over(Window.partitionBy("value").orderBy("key"))),
      Row(1, 1, 1, 1.0d, 1, 1, 1, 1, 1, 1, 1.0d, 0.0d) ::
      Row(1, 1, 1, 1.0d, 1, 1, 1, 1, 1, 1, 1.0d / 3.0d, 0.0d) ::
      Row(2, 2, 1, 5.0d / 3.0d, 3, 5, 1, 2, 2, 2, 1.0d, 0.5d) ::
      Row(2, 2, 1, 5.0d / 3.0d, 3, 5, 2, 3, 2, 2, 1.0d, 0.5d) :: Nil)
  }

  test("window function should fail if order by clause is not specified") {
    val df = Seq((1, "1"), (2, "2"), (1, "2"), (2, "2")).toDF("key", "value")
    val e = intercept[AnalysisException](
      // Here we missed .orderBy("key")!
      df.select(row_number().over(Window.partitionBy("value"))).collect())
    assert(e.message.contains("requires window to be ordered"))
  }

  test("statistical functions") {
    val df = Seq(("a", 1), ("a", 1), ("a", 2), ("a", 2), ("b", 4), ("b", 3), ("b", 2)).
      toDF("key", "value")
    val window = Window.partitionBy($"key")
    checkAnswer(
      df.select(
        $"key",
        var_pop($"value").over(window),
        var_samp($"value").over(window),
        approx_count_distinct($"value").over(window)),
      Seq.fill(4)(Row("a", 1.0d / 4.0d, 1.0d / 3.0d, 2))
      ++ Seq.fill(3)(Row("b", 2.0d / 3.0d, 1.0d, 3)))
  }

  test("window function with aggregates") {
    val df = Seq(("a", 1), ("a", 1), ("a", 2), ("a", 2), ("b", 4), ("b", 3), ("b", 2)).
      toDF("key", "value")
    val window = Window.orderBy()
    checkAnswer(
      df.groupBy($"key")
        .agg(
          sum($"value"),
          sum(sum($"value")).over(window) - sum($"value")),
      Seq(Row("a", 6, 9), Row("b", 9, 6)))
  }

  test("SPARK-16195 empty over spec") {
    val df = Seq(("a", 1), ("a", 1), ("a", 2), ("b", 2)).
      toDF("key", "value")
    df.createOrReplaceTempView("window_table")
    checkAnswer(
      df.select($"key", $"value", sum($"value").over(), avg($"value").over()),
      Seq(Row("a", 1, 6, 1.5), Row("a", 1, 6, 1.5), Row("a", 2, 6, 1.5), Row("b", 2, 6, 1.5)))
    checkAnswer(
      sql("select key, value, sum(value) over(), avg(value) over() from window_table"),
      Seq(Row("a", 1, 6, 1.5), Row("a", 1, 6, 1.5), Row("a", 2, 6, 1.5), Row("b", 2, 6, 1.5)))
  }

  test("window function with udaf") {
    val udaf = new UserDefinedAggregateFunction {
      def inputSchema: StructType = new StructType()
        .add("a", LongType)
        .add("b", LongType)

      def bufferSchema: StructType = new StructType()
        .add("product", LongType)

      def dataType: DataType = LongType

      def deterministic: Boolean = true

      def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = 0L
      }

      def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        if (!(input.isNullAt(0) || input.isNullAt(1))) {
          buffer(0) = buffer.getLong(0) + input.getLong(0) * input.getLong(1)
        }
      }

      def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      }

      def evaluate(buffer: Row): Any =
        buffer.getLong(0)
    }
    val df = Seq(
      ("a", 1, 1),
      ("a", 1, 5),
      ("a", 2, 10),
      ("a", 2, -1),
      ("b", 4, 7),
      ("b", 3, 8),
      ("b", 2, 4))
      .toDF("key", "a", "b")
    val window = Window.partitionBy($"key").orderBy($"a").rangeBetween(Long.MinValue, 0L)
    checkAnswer(
      df.select(
        $"key",
        $"a",
        $"b",
        udaf($"a", $"b").over(window)),
      Seq(
        Row("a", 1, 1, 6),
        Row("a", 1, 5, 6),
        Row("a", 2, 10, 24),
        Row("a", 2, -1, 24),
        Row("b", 4, 7, 60),
        Row("b", 3, 8, 32),
        Row("b", 2, 4, 8)))
  }

  test("null inputs") {
    val df = Seq(("a", 1), ("a", 1), ("a", 2), ("a", 2), ("b", 4), ("b", 3), ("b", 2))
      .toDF("key", "value")
    val window = Window.orderBy()
    checkAnswer(
      df.select(
        $"key",
        $"value",
        avg(lit(null)).over(window),
        sum(lit(null)).over(window)),
      Seq(
        Row("a", 1, null, null),
        Row("a", 1, null, null),
        Row("a", 2, null, null),
        Row("a", 2, null, null),
        Row("b", 4, null, null),
        Row("b", 3, null, null),
        Row("b", 2, null, null)))
  }

  test("last/first with ignoreNulls") {
    val nullStr: String = null
    val df = Seq(
      ("a", 0, nullStr),
      ("a", 1, "x"),
      ("a", 2, "y"),
      ("a", 3, "z"),
      ("a", 4, nullStr),
      ("b", 1, nullStr),
      ("b", 2, nullStr)).
      toDF("key", "order", "value")
    val window = Window.partitionBy($"key").orderBy($"order")
    checkAnswer(
      df.select(
        $"key",
        $"order",
        first($"value").over(window),
        first($"value", ignoreNulls = false).over(window),
        first($"value", ignoreNulls = true).over(window),
        last($"value").over(window),
        last($"value", ignoreNulls = false).over(window),
        last($"value", ignoreNulls = true).over(window)),
      Seq(
        Row("a", 0, null, null, null, null, null, null),
        Row("a", 1, null, null, "x", "x", "x", "x"),
        Row("a", 2, null, null, "x", "y", "y", "y"),
        Row("a", 3, null, null, "x", "z", "z", "z"),
        Row("a", 4, null, null, "x", null, null, "z"),
        Row("b", 1, null, null, null, null, null, null),
        Row("b", 2, null, null, null, null, null, null)))
  }

  test("SPARK-12989 ExtractWindowExpressions treats alias as regular attribute") {
    val src = Seq((0, 3, 5)).toDF("a", "b", "c")
      .withColumn("Data", struct("a", "b"))
      .drop("a")
      .drop("b")
    val winSpec = Window.partitionBy("Data.a", "Data.b").orderBy($"c".desc)
    val df = src.select($"*", max("c").over(winSpec) as "max")
    checkAnswer(df, Row(5, Row(0, 3), 5))
  }

  test("aggregation and rows between with unbounded + predicate pushdown") {
    val df = Seq((1, "1"), (2, "2"), (2, "3"), (1, "3"), (3, "2"), (4, "3")).toDF("key", "value")
    df.createOrReplaceTempView("window_table")
    val selectList = Seq($"key", $"value",
      last("key").over(
        Window.partitionBy($"value").orderBy($"key").rowsBetween(0, Long.MaxValue)),
      last("key").over(
        Window.partitionBy($"value").orderBy($"key").rowsBetween(Long.MinValue, 0)),
      last("key").over(Window.partitionBy($"value").orderBy($"key").rowsBetween(-1, 1)))

    checkAnswer(
      df.select(selectList: _*).where($"value" < "3"),
      Seq(Row(1, "1", 1, 1, 1), Row(2, "2", 3, 2, 3), Row(3, "2", 3, 3, 3)))
  }

  test("aggregation and range between with unbounded + predicate pushdown") {
    val df = Seq((5, "1"), (5, "2"), (4, "2"), (6, "2"), (3, "1"), (2, "2")).toDF("key", "value")
    df.createOrReplaceTempView("window_table")
    val selectList = Seq($"key", $"value",
      last("value").over(
        Window.partitionBy($"value").orderBy($"key").rangeBetween(-2, -1)).equalTo("2")
        .as("last_v"),
      avg("key").over(Window.partitionBy("value").orderBy("key").rangeBetween(Long.MinValue, 1))
        .as("avg_key1"),
      avg("key").over(Window.partitionBy("value").orderBy("key").rangeBetween(0, Long.MaxValue))
        .as("avg_key2"),
      avg("key").over(Window.partitionBy("value").orderBy("key").rangeBetween(-1, 1))
        .as("avg_key3"))

    checkAnswer(
      df.select(selectList: _*).where($"value" < 2),
      Seq(Row(3, "1", null, 3.0, 4.0, 3.0), Row(5, "1", false, 4.0, 5.0, 5.0)))
  }

  test("Window spill with less than the inMemoryThreshold") {
    val df = Seq((1, "1"), (2, "2"), (1, "3"), (2, "4")).toDF("key", "value")
    val window = Window.partitionBy($"key").orderBy($"value")

    withSQLConf(SQLConf.WINDOW_EXEC_BUFFER_IN_MEMORY_THRESHOLD.key -> "2",
      SQLConf.WINDOW_EXEC_BUFFER_SPILL_THRESHOLD.key -> "2") {
      assertNotSpilled(sparkContext, "select") {
        df.select($"key", sum("value").over(window)).collect()
      }
    }
  }

  test("Window spill with more than the inMemoryThreshold but less than the spillThreshold") {
    val df = Seq((1, "1"), (2, "2"), (1, "3"), (2, "4")).toDF("key", "value")
    val window = Window.partitionBy($"key").orderBy($"value")

    withSQLConf(SQLConf.WINDOW_EXEC_BUFFER_IN_MEMORY_THRESHOLD.key -> "1",
      SQLConf.WINDOW_EXEC_BUFFER_SPILL_THRESHOLD.key -> "2") {
      assertNotSpilled(sparkContext, "select") {
        df.select($"key", sum("value").over(window)).collect()
      }
    }
  }

  test("Window spill with more than the inMemoryThreshold and spillThreshold") {
    val df = Seq((1, "1"), (2, "2"), (1, "3"), (2, "4")).toDF("key", "value")
    val window = Window.partitionBy($"key").orderBy($"value")

    withSQLConf(SQLConf.WINDOW_EXEC_BUFFER_IN_MEMORY_THRESHOLD.key -> "1",
      SQLConf.WINDOW_EXEC_BUFFER_SPILL_THRESHOLD.key -> "1") {
      assertSpilled(sparkContext, "select") {
        df.select($"key", sum("value").over(window)).collect()
      }
    }
  }

  test("SPARK-21258: complex object in combination with spilling") {
    // Make sure we trigger the spilling path.
    withSQLConf(SQLConf.WINDOW_EXEC_BUFFER_IN_MEMORY_THRESHOLD.key -> "1",
      SQLConf.WINDOW_EXEC_BUFFER_SPILL_THRESHOLD.key -> "17") {
      val sampleSchema = new StructType().
        add("f0", StringType).
        add("f1", LongType).
        add("f2", ArrayType(new StructType().
          add("f20", StringType))).
        add("f3", ArrayType(new StructType().
          add("f30", StringType)))

      val w0 = Window.partitionBy("f0").orderBy("f1")
      val w1 = w0.rowsBetween(Long.MinValue, Long.MaxValue)

      val c0 = first(struct($"f2", $"f3")).over(w0) as "c0"
      val c1 = last(struct($"f2", $"f3")).over(w1) as "c1"

      val input =
        """{"f1":1497820153720,"f2":[{"f20":"x","f21":0}],"f3":[{"f30":"x","f31":0}]}
          |{"f1":1497802179638}
          |{"f1":1497802189347}
          |{"f1":1497802189593}
          |{"f1":1497802189597}
          |{"f1":1497802189599}
          |{"f1":1497802192103}
          |{"f1":1497802193414}
          |{"f1":1497802193577}
          |{"f1":1497802193709}
          |{"f1":1497802202883}
          |{"f1":1497802203006}
          |{"f1":1497802203743}
          |{"f1":1497802203834}
          |{"f1":1497802203887}
          |{"f1":1497802203893}
          |{"f1":1497802203976}
          |{"f1":1497820168098}
          |""".stripMargin.split("\n").toSeq

      import testImplicits._

      assertSpilled(sparkContext, "select") {
        spark.read.schema(sampleSchema).json(input.toDS()).select(c0, c1).foreach { _ => () }
      }
    }
  }
}
