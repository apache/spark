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

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.catalyst.plans.logical.Expand
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.StringType

class DataFrameSessionWindowingSuite
  extends QueryTest with SharedSQLContext with BeforeAndAfterEach {

  import testImplicits._

  private def withTempTable(f: String => Unit): Unit = {
    val tableName = "temp"
    Seq(
      ("2018-08-22 19:39:27", "a", 4),
      ("2018-08-22 19:39:34", "a", 1),
      ("2018-08-22 19:39:56", "a", 3),
      ("2018-08-22 19:39:56", "b", 2)
    ).toDF("time", "key", "value").createOrReplaceTempView(tableName)
    try {
      f(tableName)
    } finally {
      spark.catalog.dropTempView(tableName)
    }
  }

  test("session window in SQL with single key as session window key") {
    withTempTable { table =>
      val a = spark.sql(
        s"""select session_window, key, sum(value) as res
            | from $table group by session_window(time, "10 seconds"), key""".stripMargin)
      checkAnswer(
        spark.sql(
          s"""select session_window, key, sum(value) as res
             | from $table group by session_window(time, "10 seconds"), key""".stripMargin)
          .select($"session_window.start".cast(StringType),
            $"session_window.end".cast(StringType), $"key", $"res"),
        Seq(
          Row("2018-08-22 19:39:27", "2018-08-22 19:39:44", "a", 5),
          Row("2018-08-22 19:39:56", "2018-08-22 19:40:06", "a", 3),
          Row("2018-08-22 19:39:56", "2018-08-22 19:40:06", "b", 2)
        )
      )
    }
  }

  test("session window with single key as session window key") {
    val df = Seq(
      ("2018-08-22 19:39:27", "a", 4),
      ("2018-08-22 19:39:34", "a", 1),
      ("2018-08-22 19:39:56", "a", 3),
      ("2018-08-22 19:39:56", "b", 2)).toDF("time", "key", "value")
    checkAnswer(
      df.groupBy(session_window($"time", "10 seconds"), $"key")
        .agg(sum($"value").as("res"))
        .orderBy($"session_window.start".asc)
        .select($"session_window.start".cast(StringType),
          $"session_window.end".cast(StringType), $"key", $"res"),
      Seq(
        Row("2018-08-22 19:39:27", "2018-08-22 19:39:44", "a", 5),
        Row("2018-08-22 19:39:56", "2018-08-22 19:40:06", "a", 3),
        Row("2018-08-22 19:39:56", "2018-08-22 19:40:06", "b", 2))
    )
  }

  test("test session window exec with multi group by") {
    // key with b1 and a2 will be partitioned into same task
    val df = Seq(
      ("2018-08-22 19:39:34", "a", "2", 1),
      ("2018-08-22 19:39:56", "b", "1", 2),
      ("2018-08-22 19:39:56", "a", "2", 3),
      ("2018-08-22 19:39:27", "a", "1", 4),
      ("2018-08-22 19:39:27", "a", "2", 4),
      ("2018-08-22 19:39:34", "a", "1", 1),
      ("2018-08-22 19:39:56", "a", "1", 3),
      ("2018-08-22 19:39:56", "b", "2", 2)).toDF("time", "key1", "key2", "value")
    checkAnswer(
      df.groupBy(session_window($"time", "10 seconds"), $"key1", $"key2")
        .agg(sum($"value").as("res"))
        .orderBy($"session_window.start".asc, $"key1".asc, $"key2".asc)
        .select($"session_window.start".cast(StringType),
          $"session_window.end".cast(StringType), $"key1", $"key2", $"res"),
      Seq(
        Row("2018-08-22 19:39:27", "2018-08-22 19:39:44", "a", "1", 5),
        Row("2018-08-22 19:39:27", "2018-08-22 19:39:44", "a", "2", 5),
        Row("2018-08-22 19:39:56", "2018-08-22 19:40:06", "a", "1", 3),
        Row("2018-08-22 19:39:56", "2018-08-22 19:40:06", "a", "2", 3),
        Row("2018-08-22 19:39:56", "2018-08-22 19:40:06", "b", "1", 2),
        Row("2018-08-22 19:39:56", "2018-08-22 19:40:06", "b", "2", 2))
    )
  }

  test("test session window exec including multi keys in one partition") {
    withSQLConf("spark.sql.shuffle.partitions" -> "1") {
      // key with b1 and a2 will be partitioned into same task
      val df = Seq(
        ("2018-08-22 19:39:34", "a2", 1),
        ("2018-08-22 19:39:56", "b1", 2),
        ("2018-08-22 19:39:56", "a2", 3),
        ("2018-08-22 19:39:27", "a1", 4),
        ("2018-08-22 19:39:27", "a2", 4),
        ("2018-08-22 19:39:34", "a1", 1),
        ("2018-08-22 19:39:56", "a1", 3),
        ("2018-08-22 19:39:56", "b2", 2)).toDF("time", "key", "value")
      checkAnswer(
        df.groupBy(session_window($"time", "10 seconds"), $"key")
          .agg(sum($"value").as("res"))
          .orderBy($"session_window.start".asc, $"key".asc)
          .select($"session_window.start".cast(StringType),
            $"session_window.end".cast(StringType), $"key", $"res"),
        Seq(
          Row("2018-08-22 19:39:27", "2018-08-22 19:39:44", "a1", 5),
          Row("2018-08-22 19:39:27", "2018-08-22 19:39:44", "a2", 5),
          Row("2018-08-22 19:39:56", "2018-08-22 19:40:06", "a1", 3),
          Row("2018-08-22 19:39:56", "2018-08-22 19:40:06", "a2", 3),
          Row("2018-08-22 19:39:56", "2018-08-22 19:40:06", "b1", 2),
          Row("2018-08-22 19:39:56", "2018-08-22 19:40:06", "b2", 2))
      )
    }
  }

}
