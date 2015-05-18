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
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.sql.hive.test.TestHive.implicits._

class HiveDataFrameWindowSuite extends QueryTest {

  test("reuse window partitionBy") {
    val df = Seq((1, "1"), (2, "2"), (1, "1"), (2, "2")).toDF("key", "value")
    val w = partitionBy("key").orderBy("value")

    checkAnswer(
      df.select(
        lead("key").over(w),
        lead("value").over(w)),
      Row(1, "1") :: Row(2, "2") :: Row(null, null) :: Row(null, null) :: Nil)
  }

  test("reuse window orderBy") {
    val df = Seq((1, "1"), (2, "2"), (1, "1"), (2, "2")).toDF("key", "value")
    val w = orderBy("value").partitionBy("key")

    checkAnswer(
      df.select(
        lead("key").over(w),
        lead("value").over(w)),
      Row(1, "1") :: Row(2, "2") :: Row(null, null) :: Row(null, null) :: Nil)
  }

  test("lead in window") {
    val df = Seq((1, "1"), (2, "2"), (1, "1"), (2, "2")).toDF("key", "value")
    df.registerTempTable("window_table")

    checkAnswer(
      df.select(
        lead("value").over(
          partitionBy($"key")
          .orderBy($"value"))),
      sql(
        """SELECT
          | lead(value) OVER (PARTITION BY key ORDER BY value)
          | FROM window_table""".stripMargin).collect())
  }

  test("lag in window") {
    val df = Seq((1, "1"), (2, "2"), (1, "1"), (2, "2")).toDF("key", "value")
    df.registerTempTable("window_table")

    checkAnswer(
      df.select(
        lag("value").over(
          partitionBy($"key")
          .orderBy($"value"))),
      sql(
        """SELECT
          | lag(value) OVER (PARTITION BY key ORDER BY value)
          | FROM window_table""".stripMargin).collect())
  }

  test("lead in window with default value") {
    val df = Seq((1, "1"), (1, "1"), (2, "2"), (1, "1"),
                 (2, "2"), (1, "1"), (2, "2")).toDF("key", "value")
    df.registerTempTable("window_table")
    checkAnswer(
      df.select(
        lead("value", 2, "n/a").over(
          partitionBy("key")
          .orderBy("value"))),
      sql(
        """SELECT
          | lead(value, 2, "n/a") OVER (PARTITION BY key ORDER BY value)
          | FROM window_table""".stripMargin).collect())
  }

  test("lag in window with default value") {
    val df = Seq((1, "1"), (1, "1"), (2, "2"), (1, "1"),
                 (2, "2"), (1, "1"), (2, "2")).toDF("key", "value")
    df.registerTempTable("window_table")
    checkAnswer(
      df.select(
        lag("value", 2, "n/a").over(
          partitionBy($"key")
          .orderBy($"value"))),
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
        max("key").over(
          partitionBy("value")
            .orderBy("key")),
        min("key").over(
          partitionBy("value")
            .orderBy("key")),
        mean("key").over(
          partitionBy("value")
            .orderBy("key")),
        count("key").over(
          partitionBy("value")
            .orderBy("key")),
        sum("key").over(
          partitionBy("value")
            .orderBy("key")),
        ntile("key").over(
          partitionBy("value")
            .orderBy("key")),
        ntile($"key").over(
          partitionBy("value")
            .orderBy("key")),
        rowNumber().over(
          partitionBy("value")
            .orderBy("key")),
        denseRank().over(
          partitionBy("value")
            .orderBy("key")),
        rank().over(
          partitionBy("value")
            .orderBy("key")),
        cumeDist().over(
          partitionBy("value")
            .orderBy("key")),
        percentRank().over(
          partitionBy("value")
            .orderBy("key"))),
      sql(
        s"""SELECT
           |key,
           |max(key) over (partition by value order by key),
           |min(key) over (partition by value order by key),
           |avg(key) over (partition by value order by key),
           |count(key) over (partition by value order by key),
           |sum(key) over (partition by value order by key),
           |ntile(key) over (partition by value order by key),
           |ntile(key) over (partition by value order by key),
           |row_number() over (partition by value order by key),
           |dense_rank() over (partition by value order by key),
           |rank() over (partition by value order by key),
           |cume_dist() over (partition by value order by key),
           |percent_rank() over (partition by value order by key)
           |FROM window_table""".stripMargin).collect)
  }

  test("aggregation in a row window") {
    val df = Seq((1, "1"), (2, "2"), (1, "1"), (2, "2")).toDF("key", "value")
    df.registerTempTable("window_table")
    checkAnswer(
      df.select(
        avg("key").over(
          partitionBy($"value")
            .orderBy($"key")
            .rows
            .between
            .preceding(1)
            .and
            .following(1))),
      sql(
        """SELECT
          | avg(key) OVER
          |   (PARTITION BY value ORDER BY key ROWS BETWEEN 1 preceding and 1 following)
          | FROM window_table""".stripMargin).collect())
  }

  test("aggregation in a Range window") {
    val df = Seq((1, "1"), (2, "2"), (1, "1"), (2, "2")).toDF("key", "value")
    df.registerTempTable("window_table")
    checkAnswer(
      df.select(
        avg("key").over(
          partitionBy($"value")
          .orderBy($"key")
          .range
          .between
          .preceding(1)
          .and
          .following(1))),
      sql(
        """SELECT
          | avg(key) OVER
          |   (PARTITION BY value ORDER BY key RANGE BETWEEN 1 preceding and 1 following)
          | FROM window_table""".stripMargin).collect())
  }

  test("Aggregate function in Row preceding Window") {
    val df = Seq((1, "1"), (2, "2"), (2, "3"), (1, "3"), (3, "3"), (4, "3")).toDF("key", "value")
    df.registerTempTable("window_table")
    checkAnswer(
      df.select(
        $"key",
        first("value").over(
          partitionBy($"value")
          .orderBy($"key")
          .rows
          .preceding(1)),
        first("value").over(
          partitionBy($"value")
            .orderBy($"key")
            .rows
            .between
            .preceding(2)
            .and
            .preceding(1))),
      sql(
        """SELECT
          | key,
          | first_value(value) OVER
          |   (PARTITION BY value ORDER BY key ROWS 1 preceding),
          | first_value(value) OVER
          |   (PARTITION BY value ORDER BY key ROWS between 2 preceding and 1 preceding)
          | FROM window_table""".stripMargin).collect())
  }

  test("Aggregate function in Row following Window") {
    val df = Seq((1, "1"), (2, "2"), (2, "3"), (1, "3"), (3, "2"), (4, "3")).toDF("key", "value")
    df.registerTempTable("window_table")
    checkAnswer(
      df.select(
        $"key",
        last("value").over(
          partitionBy($"value")
            .orderBy($"key")
            .rows
            .between
            .currentRow()
            .and
            .unboundedFollowing()),
        last("value").over(
          partitionBy($"value")
            .orderBy($"key")
            .rows
            .between
            .unboundedPreceding()
            .and
            .currentRow()),
        last("value").over(
          partitionBy($"value")
          .orderBy($"key")
          .rows
          .between
          .preceding(1)
          .and
          .following(1))),
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

  test("Multiple aggregate functions in row window") {
    val df = Seq((1, "1"), (1, "2"), (3, "2"), (2, "2"), (1, "1"), (2, "2")).toDF("key", "value")
    df.registerTempTable("window_table")
    checkAnswer(
      df.select(
        avg("key").over(
          partitionBy($"key")
            .orderBy($"value")
            .rows
            .preceding(1)),
        avg("key").over(
          partitionBy($"key")
            .orderBy($"value")
            .rows
            .between
            .currentRow
            .and
            .currentRow),
        avg("key").over(
          partitionBy($"key")
            .orderBy($"value")
            .rows
            .between
            .preceding(2)
            .and
            .preceding(1))),
      sql(
        """SELECT
          | avg(key) OVER
          |   (partition by key ORDER BY value rows 1 preceding),
          | avg(key) OVER
          |   (partition by key ORDER BY value rows between current row and current row),
          | avg(key) OVER
          |   (partition by key ORDER BY value rows between 2 preceding and 1 preceding)
          | FROM window_table""".stripMargin).collect())
  }

  test("Multiple aggregate functions in range window") {
    val df = Seq((1, "1"), (2, "2"), (2, "2"), (2, "2"), (1, "1"), (2, "2")).toDF("key", "value")
    df.registerTempTable("window_table")
    checkAnswer(
      df.select(
        $"key",
        last("value").over(
          partitionBy($"value")
            .orderBy($"key")
            .range
            .following(1))
          .equalTo("2")
          .as("last_v"),
        avg("key")
          .over(
            partitionBy("value")
              .orderBy("key")
              .range
              .between
              .preceding(2)
              .and
              .following(1))
          .as("avg_key1"),
        avg("key")
          .over(
            partitionBy("value")
              .orderBy("key")
              .range
              .between
              .currentRow
              .and
              .following(1))
          .as("avg_key2"),
        avg("key")
          .over(
            partitionBy("value")
              .orderBy("key")
              .range
              .between
              .preceding(1)
              .and
              .currentRow)
          .as("avg_key3")
      ),
      sql(
        """SELECT
          | key,
          | last_value(value) OVER
          |   (PARTITION BY value ORDER BY key RANGE 1 preceding) == "2",
          | avg(key) OVER
          |   (PARTITION BY value ORDER BY key RANGE BETWEEN 2 preceding and 1 following),
          | avg(key) OVER
          |   (PARTITION BY value ORDER BY key RANGE BETWEEN current row and 1 following),
          | avg(key) OVER
          |   (PARTITION BY value ORDER BY key RANGE BETWEEN 1 preceding and current row)
          | FROM window_table""".stripMargin).collect())
  }
}
