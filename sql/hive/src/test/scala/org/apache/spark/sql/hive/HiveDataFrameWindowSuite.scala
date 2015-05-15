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

import org.apache.spark.sql.{AnalysisException, Row, QueryTest}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.test.TestHive.implicits._

class HiveDataFrameWindowSuite extends QueryTest {

  test("reuse window") {
    val df = Seq((1, "1"), (2, "2"), (1, "1"), (2, "2")).toDF("key", "value")
    val w = over.partitionBy("key").orderBy("value")

    checkAnswer(
      df.select(
        lead("key").over(w).toColumn,
        lead("value").over(w).toColumn),
      Row(1, "1") :: Row(2, "2") :: Row(null, null) :: Row(null, null) :: Nil)
  }

  test("lead in window") {
    val df = Seq((1, "1"), (2, "2"), (1, "1"), (2, "2")).toDF("key", "value")
    checkAnswer(
      df.select(
        lead("value").over
          .partitionBy($"key")
          .orderBy($"value")
          .toColumn),
      Row("1") :: Row("2") :: Row(null) :: Row(null) :: Nil)
  }

  test("lag in window") {
    val df = Seq((1, "1"), (2, "2"), (1, "1"), (2, "2")).toDF("key", "value")
    checkAnswer(
      df.select(
        lead("value").over
          .partitionBy($"key")
          .orderBy($"value")
          .toColumn),
      Row("1") :: Row("2") :: Row(null) :: Row(null) :: Nil)
  }

  test("lead in window with default value") {
    val df = Seq((1, "1"), (1, "1"), (2, "2"), (1, "1"),
                 (2, "2"), (1, "1"), (2, "2")).toDF("key", "value")
    checkAnswer(
      df.select(
        lead("value", 2, "n/a").over
          .partitionBy("key")
          .orderBy("value")
          .toColumn),
      Row("1") :: Row("1") :: Row("2") :: Row("n/a")
        :: Row("n/a") :: Row("n/a") :: Row("n/a") :: Nil)
  }

  test("lag in window with default value") {
    val df = Seq((1, "1"), (1, "1"), (2, "2"), (1, "1"),
                 (2, "2"), (1, "1"), (2, "2")).toDF("key", "value")
    checkAnswer(
      df.select(
        lead("value", 2, "n/a").over
          .partitionBy($"key")
          .orderBy($"value")
          .toColumn),
      Row("1") :: Row("1") :: Row("2") :: Row("n/a")
        :: Row("n/a") :: Row("n/a") :: Row("n/a") :: Nil)
  }

  test("aggregation in a Row window") {
    val df = Seq((1, "1"), (2, "2"), (1, "1"), (2, "2")).toDF("key", "value")
    checkAnswer(
      df.select(
        avg("key").over
          .partitionBy($"key")
          .orderBy($"value")
          .rows
          .preceding(1)
          .toColumn),
      Row(1.0) :: Row(1.0) :: Row(2.0) :: Row(2.0) :: Nil)
  }

  test("aggregation in a Range window") {
    val df = Seq((1, "1"), (2, "2"), (1, "1"), (2, "2")).toDF("key", "value")
    checkAnswer(
      df.select(
        avg("key").over
          .partitionBy($"value")
          .orderBy($"key")
          .between
          .preceding(1)
          .following(1)
          .toColumn),
      Row(1.0) :: Row(1.0) :: Row(2.0) :: Row(2.0) :: Nil)
  }

  test("multiple aggregate function in window") {
    val df = Seq((1, "1"), (2, "2"), (1, "1"), (2, "2")).toDF("key", "value")
    checkAnswer(
      df.select(
        avg("key").over
          .partitionBy($"value")
          .orderBy($"key")
          .rows
          .preceding(1).toColumn,
        sum("key").over
          .partitionBy($"value")
          .orderBy($"key")
          .between
          .preceding(1)
          .following(1)
          .toColumn),
      Row(1.0, 1) :: Row(1.0, 2) :: Row(2.0, 2) :: Row(2.0, 4) :: Nil)
  }

  test("Window function in Unspecified Window") {
    val df = Seq((1, "1"), (2, "2"), (2, "3")).toDF("key", "value")

    checkAnswer(
      df.select(
        $"key",
        first("value").over
          .partitionBy($"key")
          .toColumn),
      Row(1, "1") :: Row(2, "2") :: Row(2, "2") :: Nil)
  }

  test("Window function in Unspecified Window #2") {
    val df = Seq((1, "1"), (2, "2"), (2, "3")).toDF("key", "value")

    checkAnswer(
      df.select(
        $"key",
        first("value").over
          .partitionBy($"key")
          .orderBy($"value")
          .toColumn),
      Row(1, "1") :: Row(2, "2") :: Row(2, "2") :: Nil)
  }

  test("Aggregate function in Range Window") {
    val df = Seq((1, "1"), (2, "2"), (2, "3")).toDF("key", "value")

    checkAnswer(
      df.select(
        $"key",
        first("value").over
          .partitionBy($"value")
          .orderBy($"key")
          .between
          .preceding(1)
          .following(1)
          .toColumn),
      Row(1, "1") :: Row(2, "2") :: Row(2, "3") :: Nil)
  }

  test("Aggregate function in Row preceding Window") {
    val df = Seq((1, "1"), (2, "2"), (2, "3")).toDF("key", "value")
    checkAnswer(
      df.select(
        $"key",
        first("value").over
          .partitionBy($"value")
          .orderBy($"key")
          .rows
          .preceding(1)
          .toColumn),
        Row(1, "1") :: Row(2, "2") :: Row(2, "3") :: Nil)
  }

  test("Aggregate function in Row following Window") {
    val df = Seq((1, "1"), (2, "2"), (2, "3")).toDF("key", "value")
    checkAnswer(
      df.select(
        $"key",
        last("value").over
          .partitionBy($"value")
          .orderBy($"key")
          .rows
          .following(1)
          .toColumn),
        Row(1, "1") :: Row(2, "2") :: Row(2, "3") :: Nil)
  }

  test("Multiple aggregate functions") {
    val df = Seq((1, "1"), (2, "2"), (2, "3")).toDF("key", "value")
    checkAnswer(
      df.select(
        $"key",
        last("value").over
          .partitionBy($"value")
          .orderBy($"key")
          .rows
          .following(1)
          .toColumn
          .equalTo("2")
          .as("last_v"),
        avg("key")
          .over
          .partitionBy("value")
          .orderBy("key")
          .between
          .preceding(2)
          .following(1)
          .toColumn.as("avg_key")
      ),
      Row(1, false, 1.0) :: Row(2, false, 2.0) :: Row(2, true, 2.0) :: Nil)
  }
}
