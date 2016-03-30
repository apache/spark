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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext

class DataFrameTimeWindowingSuite extends QueryTest with SharedSQLContext {

  import testImplicits._

  test("tumbling windows") {
    // 2016-03-27 19:39:34 UTC, 2016-03-27 19:39:56 UTC, 2016-03-27 19:39:27 UTC
    val df = Seq(
      (1459103974L, 1, "a"),
      (1459103996L, 2, "a"),
      (1459103967L, 4, "b")).toDF("time", "value", "id")

    checkAnswer(
      df.groupBy(window($"time", "10 seconds"))
        .agg(count("*").as("counts"))
        .orderBy($"window.start".asc)
        .select("counts"),
      Seq(Row(1), Row(1), Row(1))
    )
  }

  test("explicit tumbling window") {
    val df = Seq(
      ("2016-03-27 19:39:34", 1, "a"),
      ("2016-03-27 19:39:56", 2, "a"),
      ("2016-03-27 19:39:27", 4, "b")).toDF("time", "value", "id")

    checkAnswer(
      df.groupBy(window($"time", "10 seconds", "10 seconds", "5 seconds"), $"id")
        .agg(count("*").as("counts"))
        .orderBy($"window.start".asc)
        .select("counts"),
      Seq(Row(1), Row(1), Row(1)))
  }

  test("?") {
    // 2016-03-27 19:39:34 UTC, 2016-03-27 19:39:56 UTC, 2016-03-27 19:39:27 UTC
    val df = Seq(
      (1459103974L, 1, "a"),
      (1459103996L, 2, "a"),
      (1459103967L, 4, "b")).toDF("time", "value", "id")
    checkAnswer(
      df.select(window($"time", "10 seconds"), $"value")
        .orderBy($"window.start".asc)
        .select("value"),
      Seq(Row(4), Row(1), Row(2))
    )
  }

  test("time windowing - sliding windows") {
    // 2016-03-27 19:39:34 UTC, 2016-03-27 19:39:56 UTC, 2016-03-27 19:39:27 UTC
    val df = Seq(
      (1459103974L, 1, "a"),
      (1459103996L, 2, "a"),
      (1459103967L, 4, "b")).toDF("time", "value", "id")

    checkAnswer(
      df.groupBy(window($"time", "10 seconds", "3 seconds", "0 second"))
        .agg(count("*").as("counts"))
        .orderBy($"window.start".asc)
        .select("counts"),
      // 2016-03-27 19:39:27 UTC -> 4 bins
      // 2016-03-27 19:39:34 UTC -> 3 bins
      // 2016-03-27 19:39:56 UTC -> 3 bins
      Seq(Row(1), Row(1), Row(1), Row(2), Row(1), Row(1), Row(1), Row(1), Row(1))
    )
    checkAnswer(
      df.select(window($"time", "10 seconds", "3 seconds", "0 second"), $"value")
        .orderBy($"window.start".asc, $"value".desc).select("value"),
      // 2016-03-27 19:39:27 UTC -> 4 bins
      // 2016-03-27 19:39:34 UTC -> 3 bins
      // 2016-03-27 19:39:56 UTC -> 3 bins
      Seq(Row(4), Row(4), Row(4), Row(4), Row(1), Row(1), Row(1), Row(2), Row(2), Row(2))
    )
  }

  test("esoteric time windowing use cases") {
    // 2016-03-27 19:39:34 UTC, 2016-03-27 19:39:56 UTC
    val df = Seq(
      (1459103974L, 1, Seq("a", "b")),
      (1459103996L, 2, Seq("a", "c", "d"))).toDF("time", "value", "ids")

    checkAnswer(
      df.select(window($"time", "10 seconds"), $"value", explode($"ids"))
        .orderBy($"window.start".asc).select("value"),
      // first window exploded to two rows for "a", and "b", second window exploded to 3 rows
      Seq(Row(1), Row(1), Row(2), Row(2), Row(2))
    )
  }

  test("string timestamps") {
    val df2 = Seq(
      ("2016-03-27 09:00:05", 1),
      ("2016-03-27 09:00:32", 2),
      (null, 3),
      (null, 4)).toDF("time", "value")

    checkDataset(
      df2
        .select(window($"time", "10 seconds"), $"value")
        .orderBy($"window.start".asc)
        .select("value")
        .as[Int],
      1, 2) // null columns are dropped
  }

  test("another test") {
    val df2 = Seq(
      ("2016-03-27 09:00:05", 1),
      ("2016-03-27 09:00:32", 2),
      (null, 3),
      (null, 4)).toDF("time", "value")

    val df3 = Seq(
      ("2016-03-27 09:00:02", 3),
      ("2016-03-27 09:00:35", 6)).toDF("time", "othervalue")


    checkAnswer(
      df2.select(window($"time", "10 seconds"), $"value").join(
        df3
            .select(window($"time", "10 seconds"), $"othervalue"), Seq("window")).groupBy("window")
          .agg((sum("value") + sum("othervalue")).as("total"))
          .orderBy($"window.start".asc).select("total"),
      Seq(Row(4), Row(8)))
  }

  test("negative timestamps") {
    val df4 = Seq((2L, 1), (12L, 2)).toDF("time", "value")
    checkAnswer(
      df4.select(window($"time", "10 seconds", "10 seconds", "5 seconds"), $"value")
          .orderBy($"window.start".asc).select("value"),
      Seq(Row(1), Row(2))
    )
  }

  ignore("multiple time windows in a single operator") {

  }

  test("aliased windows") {
    val df = Seq(
      ("2016-03-27 19:39:34", 1, Seq("a", "b")),
      ("2016-03-27 19:39:56", 2, Seq("a", "c", "d"))).toDF("time", "value", "ids")

    checkAnswer(
      df.select(window($"time", "10 seconds").as("time_window"), $"value")
          .orderBy($"time_window.start".asc)
          .select("value"),
      Seq(Row(1), Row(2))
    )
  }
}
