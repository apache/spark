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
    checkAnswer(
      df.select(
        lead("value").over(
          partitionBy($"key")
          .orderBy($"value"))),
      Row("1") :: Row("2") :: Row(null) :: Row(null) :: Nil)
  }

  test("lag in window") {
    val df = Seq((1, "1"), (2, "2"), (1, "1"), (2, "2")).toDF("key", "value")
    checkAnswer(
      df.select(
        lag("value").over(
          partitionBy($"key")
          .orderBy($"value"))),
      Row("1") :: Row("2") :: Row(null) :: Row(null) :: Nil)
  }

  test("lead in window with default value") {
    val df = Seq((1, "1"), (1, "1"), (2, "2"), (1, "1"),
                 (2, "2"), (1, "1"), (2, "2")).toDF("key", "value")
    checkAnswer(
      df.select(
        lead("value", 2, "n/a").over(
          partitionBy("key")
          .orderBy("value"))),
      Row("1") :: Row("1") :: Row("2") :: Row("n/a")
        :: Row("n/a") :: Row("n/a") :: Row("n/a") :: Nil)
  }

  test("lag in window with default value") {
    val df = Seq((1, "1"), (1, "1"), (2, "2"), (1, "1"),
                 (2, "2"), (1, "1"), (2, "2")).toDF("key", "value")
    checkAnswer(
      df.select(
        lag("value", 2, "n/a").over(
          partitionBy($"key")
          .orderBy($"value"))),
      Row("1") :: Row("1") :: Row("2") :: Row("n/a")
        :: Row("n/a") :: Row("n/a") :: Row("n/a") :: Nil)
  }

  test("rank functions in unspecific window") {
    val df = Seq((1, "1"), (2, "2"), (1, "1"), (2, "2")).toDF("key", "value")
    df.registerTempTable("window_table")
    checkAnswer(
      df.select(
        $"key",
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
      Row(1.0) :: Row(1.0) :: Row(2.0) :: Row(2.0) :: Nil)
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
      Row(1.0) :: Row(1.0) :: Row(2.0) :: Row(2.0) :: Nil)
  }

  test("Aggregate function in Row preceding Window") {
    val df = Seq((1, "1"), (2, "2"), (2, "3")).toDF("key", "value")
    df.registerTempTable("window_table")
    checkAnswer(
      df.select(
        $"key",
        first("value").over(
          partitionBy($"value")
          .orderBy($"key")
          .rows
          .preceding(1))),
        Row(1, "1") :: Row(2, "2") :: Row(2, "3") :: Nil)
  }

  test("Aggregate function in Row following Window") {
    val df = Seq((1, "1"), (2, "2"), (2, "3")).toDF("key", "value")
    df.registerTempTable("window_table")
    checkAnswer(
      df.select(
        $"key",
        last("value").over(
          partitionBy($"value")
          .orderBy($"key")
          .rows
          .following(1))),
        Row(1, "1") :: Row(2, "2") :: Row(2, "3") :: Nil)
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
      Row(1.0, 1.0, 1.0) ::
      Row(1.0, 1.0, 1.0) ::
      Row(1.0, 1.0, 1.0) ::
      Row(2.0, 2.0, 2.0) ::
      Row(2.0, 2.0, 2.0) ::
      Row(3.0, 3.0, 3.0) :: Nil)
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
      Row(1, false, 1.0, 1.0, 1.0) ::
      Row(1, false, 1.0, 1.0, 1.0) ::
      Row(2, true, 2.0, 2.0, 2.0) ::
      Row(2, true, 2.0, 2.0, 2.0) ::
      Row(2, true, 2.0, 2.0, 2.0) ::
      Row(2, true, 2.0, 2.0, 2.0) :: Nil)
  }
}
