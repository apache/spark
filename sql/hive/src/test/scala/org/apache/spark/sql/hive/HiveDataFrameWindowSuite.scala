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

package org.apache.spark.sql.hive

import org.apache.spark.sql.{Row, QueryTest}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.test.TestHiveSingleton

class HiveDataFrameWindowSuite extends QueryTest with TestHiveSingleton {
  import hiveContext.implicits._
  import hiveContext.sql

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

  test("lead") {
    val df = Seq((1, "1"), (2, "2"), (1, "1"), (2, "2")).toDF("key", "value")
    df.registerTempTable("window_table")

    checkAnswer(
      df.select(
        lead("value", 1).over(Window.partitionBy($"key").orderBy($"value"))),
      sql(
        """SELECT
          | lead(value) OVER (PARTITION BY key ORDER BY value)
          | FROM window_table""".stripMargin).collect())
  }

  test("lag") {
    val df = Seq((1, "1"), (2, "2"), (1, "1"), (2, "2")).toDF("key", "value")
    df.registerTempTable("window_table")

    checkAnswer(
      df.select(
        lag("value", 1).over(Window.partitionBy($"key").orderBy($"value"))),
      sql(
        """SELECT
          | lag(value) OVER (PARTITION BY key ORDER BY value)
          | FROM window_table""".stripMargin).collect())
  }

  test("lead with default value") {
    val df = Seq((1, "1"), (1, "1"), (2, "2"), (1, "1"),
                 (2, "2"), (1, "1"), (2, "2")).toDF("key", "value")
    df.registerTempTable("window_table")
    checkAnswer(
      df.select(
        lead("value", 2, "n/a").over(Window.partitionBy("key").orderBy("value"))),
      sql(
        """SELECT
          | lead(value, 2, "n/a") OVER (PARTITION BY key ORDER BY value)
          | FROM window_table""".stripMargin).collect())
  }

  test("lag with default value") {
    val df = Seq((1, "1"), (1, "1"), (2, "2"), (1, "1"),
                 (2, "2"), (1, "1"), (2, "2")).toDF("key", "value")
    df.registerTempTable("window_table")
    checkAnswer(
      df.select(
        lag("value", 2, "n/a").over(Window.partitionBy($"key").orderBy($"value"))),
      sql(
        """SELECT
          | lag(value, 2, "n/a") OVER (PARTITION BY key ORDER BY value)
          | FROM window_table""".stripMargin).collect())
  }

  test("rank functions in unspecific window") {
    val df = Seq((1, "1"), (2, "2"), (1, "2"), (2, "2")).toDF("key", "value")
    df.registerTempTable("window_table")
    checkAnswer(
      df.select(
        $"key",
        max("key").over(Window.partitionBy("value").orderBy("key")),
        min("key").over(Window.partitionBy("value").orderBy("key")),
        mean("key").over(Window.partitionBy("value").orderBy("key")),
        count("key").over(Window.partitionBy("value").orderBy("key")),
        sum("key").over(Window.partitionBy("value").orderBy("key")),
        ntile(2).over(Window.partitionBy("value").orderBy("key")),
        rowNumber().over(Window.partitionBy("value").orderBy("key")),
        denseRank().over(Window.partitionBy("value").orderBy("key")),
        rank().over(Window.partitionBy("value").orderBy("key")),
        cumeDist().over(Window.partitionBy("value").orderBy("key")),
        percentRank().over(Window.partitionBy("value").orderBy("key"))),
      sql(
        s"""SELECT
           |key,
           |max(key) over (partition by value order by key),
           |min(key) over (partition by value order by key),
           |avg(key) over (partition by value order by key),
           |count(key) over (partition by value order by key),
           |sum(key) over (partition by value order by key),
           |ntile(2) over (partition by value order by key),
           |row_number() over (partition by value order by key),
           |dense_rank() over (partition by value order by key),
           |rank() over (partition by value order by key),
           |cume_dist() over (partition by value order by key),
           |percent_rank() over (partition by value order by key)
           |FROM window_table""".stripMargin).collect())
  }

  test("aggregation and rows between") {
    val df = Seq((1, "1"), (2, "2"), (1, "1"), (2, "2")).toDF("key", "value")
    df.registerTempTable("window_table")
    checkAnswer(
      df.select(
        avg("key").over(Window.partitionBy($"value").orderBy($"key").rowsBetween(-1, 2))),
      sql(
        """SELECT
          | avg(key) OVER
          |   (PARTITION BY value ORDER BY key ROWS BETWEEN 1 preceding and 2 following)
          | FROM window_table""".stripMargin).collect())
  }

  test("aggregation and range betweens") {
    val df = Seq((1, "1"), (2, "2"), (1, "1"), (2, "2")).toDF("key", "value")
    df.registerTempTable("window_table")
    checkAnswer(
      df.select(
        avg("key").over(Window.partitionBy($"value").orderBy($"key").rangeBetween(-1, 1))),
      sql(
        """SELECT
          | avg(key) OVER
          |   (PARTITION BY value ORDER BY key RANGE BETWEEN 1 preceding and 1 following)
          | FROM window_table""".stripMargin).collect())
  }

  test("aggregation and rows betweens with unbounded") {
    val df = Seq((1, "1"), (2, "2"), (2, "3"), (1, "3"), (3, "2"), (4, "3")).toDF("key", "value")
    df.registerTempTable("window_table")
    checkAnswer(
      df.select(
        $"key",
        last("value").over(
          Window.partitionBy($"value").orderBy($"key").rowsBetween(0, Long.MaxValue)),
        last("value").over(
          Window.partitionBy($"value").orderBy($"key").rowsBetween(Long.MinValue, 0)),
        last("value").over(Window.partitionBy($"value").orderBy($"key").rowsBetween(-1, 3))),
      sql(
        """SELECT
          | key,
          | last_value(value) OVER
          |   (PARTITION BY value ORDER BY key ROWS between current row and unbounded following),
          | last_value(value) OVER
          |   (PARTITION BY value ORDER BY key ROWS between unbounded preceding and current row),
          | last_value(value) OVER
          |   (PARTITION BY value ORDER BY key ROWS between 1 preceding and 3 following)
          | FROM window_table""".stripMargin).collect())
  }

  test("aggregation and range betweens with unbounded") {
    val df = Seq((5, "1"), (5, "2"), (4, "2"), (6, "2"), (3, "1"), (2, "2")).toDF("key", "value")
    df.registerTempTable("window_table")
    checkAnswer(
      df.select(
        $"key",
        last("value").over(
          Window.partitionBy($"value").orderBy($"key").rangeBetween(-2, -1))
          .equalTo("2")
          .as("last_v"),
        avg("key").over(Window.partitionBy("value").orderBy("key").rangeBetween(Long.MinValue, 1))
          .as("avg_key1"),
        avg("key").over(Window.partitionBy("value").orderBy("key").rangeBetween(0, Long.MaxValue))
          .as("avg_key2"),
        avg("key").over(Window.partitionBy("value").orderBy("key").rangeBetween(-1, 0))
          .as("avg_key3")
      ),
      sql(
        """SELECT
          | key,
          | last_value(value) OVER
          |   (PARTITION BY value ORDER BY key RANGE BETWEEN 2 preceding and 1 preceding) == "2",
          | avg(key) OVER
          |   (PARTITION BY value ORDER BY key RANGE BETWEEN unbounded preceding and 1 following),
          | avg(key) OVER
          |   (PARTITION BY value ORDER BY key RANGE BETWEEN current row and unbounded following),
          | avg(key) OVER
          |   (PARTITION BY value ORDER BY key RANGE BETWEEN 1 preceding and current row)
          | FROM window_table""".stripMargin).collect())
  }

  test("reverse sliding range frame") {
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
    val window = Window.
      partitionBy($"category").
      orderBy($"revenue".desc).
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

  // This is here to illustrate the fact that reverse order also reverses offsets.
  test("reverse unbounded range frame") {
    val df = Seq(1, 2, 4, 3, 2, 1).
      map(Tuple1.apply).
      toDF("value")
    val window = Window.orderBy($"value".desc)
    checkAnswer(
      df.select(
        $"value",
        sum($"value").over(window.rangeBetween(Long.MinValue, 1)),
        sum($"value").over(window.rangeBetween(1, Long.MaxValue))),
      Row(1, 13, null) :: Row(2, 13, 2) :: Row(4, 7, 9) ::
        Row(3, 11, 6) :: Row(2, 13, 2) :: Row(1, 13, null) :: Nil)

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
}
