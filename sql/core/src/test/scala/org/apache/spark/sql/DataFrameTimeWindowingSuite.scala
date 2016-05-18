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

import java.util.TimeZone

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.StringType

class DataFrameTimeWindowingSuite extends QueryTest with SharedSQLContext with BeforeAndAfterEach {

  import testImplicits._

  override def beforeEach(): Unit = {
    super.beforeEach()
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
  }

  override def afterEach(): Unit = {
    super.beforeEach()
    TimeZone.setDefault(null)
  }

  test("tumbling window groupBy statement") {
    val df = Seq(
      ("2016-03-27 19:39:34", 1, "a"),
      ("2016-03-27 19:39:56", 2, "a"),
      ("2016-03-27 19:39:27", 4, "b")).toDF("time", "value", "id")
    checkAnswer(
      df.groupBy(window($"time", "10 seconds"))
        .agg(count("*").as("counts"))
        .orderBy($"window.start".asc)
        .select("counts"),
      Seq(Row(1), Row(1), Row(1))
    )
  }

  test("tumbling window groupBy statement with startTime") {
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

  test("tumbling window with multi-column projection") {
    val df = Seq(
      ("2016-03-27 19:39:34", 1, "a"),
      ("2016-03-27 19:39:56", 2, "a"),
      ("2016-03-27 19:39:27", 4, "b")).toDF("time", "value", "id")

    checkAnswer(
      df.select(window($"time", "10 seconds"), $"value")
        .orderBy($"window.start".asc)
        .select($"window.start".cast("string"), $"window.end".cast("string"), $"value"),
      Seq(
        Row("2016-03-27 19:39:20", "2016-03-27 19:39:30", 4),
        Row("2016-03-27 19:39:30", "2016-03-27 19:39:40", 1),
        Row("2016-03-27 19:39:50", "2016-03-27 19:40:00", 2)
      )
    )
  }

  test("sliding window grouping") {
    val df = Seq(
      ("2016-03-27 19:39:34", 1, "a"),
      ("2016-03-27 19:39:56", 2, "a"),
      ("2016-03-27 19:39:27", 4, "b")).toDF("time", "value", "id")

    checkAnswer(
      df.groupBy(window($"time", "10 seconds", "3 seconds", "0 second"))
        .agg(count("*").as("counts"))
        .orderBy($"window.start".asc)
        .select($"window.start".cast("string"), $"window.end".cast("string"), $"counts"),
      // 2016-03-27 19:39:27 UTC -> 4 bins
      // 2016-03-27 19:39:34 UTC -> 3 bins
      // 2016-03-27 19:39:56 UTC -> 3 bins
      Seq(
        Row("2016-03-27 19:39:18", "2016-03-27 19:39:28", 1),
        Row("2016-03-27 19:39:21", "2016-03-27 19:39:31", 1),
        Row("2016-03-27 19:39:24", "2016-03-27 19:39:34", 1),
        Row("2016-03-27 19:39:27", "2016-03-27 19:39:37", 2),
        Row("2016-03-27 19:39:30", "2016-03-27 19:39:40", 1),
        Row("2016-03-27 19:39:33", "2016-03-27 19:39:43", 1),
        Row("2016-03-27 19:39:48", "2016-03-27 19:39:58", 1),
        Row("2016-03-27 19:39:51", "2016-03-27 19:40:01", 1),
        Row("2016-03-27 19:39:54", "2016-03-27 19:40:04", 1))
    )
  }

  test("sliding window projection") {
    val df = Seq(
      ("2016-03-27 19:39:34", 1, "a"),
      ("2016-03-27 19:39:56", 2, "a"),
      ("2016-03-27 19:39:27", 4, "b")).toDF("time", "value", "id")

    checkAnswer(
      df.select(window($"time", "10 seconds", "3 seconds", "0 second"), $"value")
        .orderBy($"window.start".asc, $"value".desc).select("value"),
      // 2016-03-27 19:39:27 UTC -> 4 bins
      // 2016-03-27 19:39:34 UTC -> 3 bins
      // 2016-03-27 19:39:56 UTC -> 3 bins
      Seq(Row(4), Row(4), Row(4), Row(4), Row(1), Row(1), Row(1), Row(2), Row(2), Row(2))
    )
  }

  test("windowing combined with explode expression") {
    val df = Seq(
      ("2016-03-27 19:39:34", 1, Seq("a", "b")),
      ("2016-03-27 19:39:56", 2, Seq("a", "c", "d"))).toDF("time", "value", "ids")

    checkAnswer(
      df.select(window($"time", "10 seconds"), $"value", explode($"ids"))
        .orderBy($"window.start".asc).select("value"),
      // first window exploded to two rows for "a", and "b", second window exploded to 3 rows
      Seq(Row(1), Row(1), Row(2), Row(2), Row(2))
    )
  }

  test("null timestamps") {
    val df = Seq(
      ("2016-03-27 09:00:05", 1),
      ("2016-03-27 09:00:32", 2),
      (null, 3),
      (null, 4)).toDF("time", "value")

    checkDataset(
      df.select(window($"time", "10 seconds"), $"value")
        .orderBy($"window.start".asc)
        .select("value")
        .as[Int],
      1, 2) // null columns are dropped
  }

  test("time window joins") {
    val df = Seq(
      ("2016-03-27 09:00:05", 1),
      ("2016-03-27 09:00:32", 2),
      (null, 3),
      (null, 4)).toDF("time", "value")

    val df2 = Seq(
      ("2016-03-27 09:00:02", 3),
      ("2016-03-27 09:00:35", 6)).toDF("time", "othervalue")

    checkAnswer(
      df.select(window($"time", "10 seconds"), $"value").join(
        df2.select(window($"time", "10 seconds"), $"othervalue"), Seq("window"))
        .groupBy("window")
        .agg((sum("value") + sum("othervalue")).as("total"))
        .orderBy($"window.start".asc).select("total"),
      Seq(Row(4), Row(8)))
  }

  test("negative timestamps") {
    val df4 = Seq(
      ("1970-01-01 00:00:02", 1),
      ("1970-01-01 00:00:12", 2)).toDF("time", "value")
    checkAnswer(
      df4.select(window($"time", "10 seconds", "10 seconds", "5 seconds"), $"value")
        .orderBy($"window.start".asc)
        .select($"window.start".cast(StringType), $"window.end".cast(StringType), $"value"),
      Seq(
        Row("1969-12-31 23:59:55", "1970-01-01 00:00:05", 1),
        Row("1970-01-01 00:00:05", "1970-01-01 00:00:15", 2))
    )
  }

  test("multiple time windows in a single operator throws nice exception") {
    val df = Seq(
      ("2016-03-27 09:00:02", 3),
      ("2016-03-27 09:00:35", 6)).toDF("time", "value")
    val e = intercept[AnalysisException] {
      df.select(window($"time", "10 second"), window($"time", "15 second")).collect()
    }
    assert(e.getMessage.contains(
      "Multiple time window expressions would result in a cartesian product"))
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

  test("millisecond precision sliding windows") {
    val df = Seq(
      ("2016-03-27 09:00:00.41", 3),
      ("2016-03-27 09:00:00.62", 6),
      ("2016-03-27 09:00:00.715", 8)).toDF("time", "value")
    checkAnswer(
      df.groupBy(window($"time", "200 milliseconds", "40 milliseconds", "0 milliseconds"))
        .agg(count("*").as("counts"))
        .orderBy($"window.start".asc)
        .select($"window.start".cast(StringType), $"window.end".cast(StringType), $"counts"),
      Seq(
        Row("2016-03-27 09:00:00.24", "2016-03-27 09:00:00.44", 1),
        Row("2016-03-27 09:00:00.28", "2016-03-27 09:00:00.48", 1),
        Row("2016-03-27 09:00:00.32", "2016-03-27 09:00:00.52", 1),
        Row("2016-03-27 09:00:00.36", "2016-03-27 09:00:00.56", 1),
        Row("2016-03-27 09:00:00.4", "2016-03-27 09:00:00.6", 1),
        Row("2016-03-27 09:00:00.44", "2016-03-27 09:00:00.64", 1),
        Row("2016-03-27 09:00:00.48", "2016-03-27 09:00:00.68", 1),
        Row("2016-03-27 09:00:00.52", "2016-03-27 09:00:00.72", 2),
        Row("2016-03-27 09:00:00.56", "2016-03-27 09:00:00.76", 2),
        Row("2016-03-27 09:00:00.6", "2016-03-27 09:00:00.8", 2),
        Row("2016-03-27 09:00:00.64", "2016-03-27 09:00:00.84", 1),
        Row("2016-03-27 09:00:00.68", "2016-03-27 09:00:00.88", 1))
    )
  }

  private def withTempTable(f: String => Unit): Unit = {
    val tableName = "temp"
    Seq(
      ("2016-03-27 19:39:34", 1),
      ("2016-03-27 19:39:56", 2),
      ("2016-03-27 19:39:27", 4)).toDF("time", "value").createOrReplaceTempView(tableName)
    try {
      f(tableName)
    } finally {
      spark.catalog.dropTempView(tableName)
    }
  }

  test("time window in SQL with single string expression") {
    withTempTable { table =>
      checkAnswer(
        spark.sql(s"""select window(time, "10 seconds"), value from $table""")
          .select($"window.start".cast(StringType), $"window.end".cast(StringType), $"value"),
        Seq(
          Row("2016-03-27 19:39:20", "2016-03-27 19:39:30", 4),
          Row("2016-03-27 19:39:30", "2016-03-27 19:39:40", 1),
          Row("2016-03-27 19:39:50", "2016-03-27 19:40:00", 2)
        )
      )
    }
  }

  test("time window in SQL with with two expressions") {
    withTempTable { table =>
      checkAnswer(
        spark.sql(
          s"""select window(time, "10 seconds", 10000000), value from $table""")
          .select($"window.start".cast(StringType), $"window.end".cast(StringType), $"value"),
        Seq(
          Row("2016-03-27 19:39:20", "2016-03-27 19:39:30", 4),
          Row("2016-03-27 19:39:30", "2016-03-27 19:39:40", 1),
          Row("2016-03-27 19:39:50", "2016-03-27 19:40:00", 2)
        )
      )
    }
  }

  test("time window in SQL with with three expressions") {
    withTempTable { table =>
      checkAnswer(
        spark.sql(
          s"""select window(time, "10 seconds", 10000000, "5 seconds"), value from $table""")
          .select($"window.start".cast(StringType), $"window.end".cast(StringType), $"value"),
        Seq(
          Row("2016-03-27 19:39:25", "2016-03-27 19:39:35", 1),
          Row("2016-03-27 19:39:25", "2016-03-27 19:39:35", 4),
          Row("2016-03-27 19:39:55", "2016-03-27 19:40:05", 2)
        )
      )
    }
  }
}
