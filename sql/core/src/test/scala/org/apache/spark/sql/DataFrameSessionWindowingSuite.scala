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
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StringType

class DataFrameSessionWindowingSuite extends QueryTest with SharedSparkSession
  with BeforeAndAfterEach {

  import testImplicits._

  test("simple session window with record at window start") {
    val df = Seq(
      ("2016-03-27 19:39:30", 1, "a")).toDF("time", "value", "id")

    checkAnswer(
      df.groupBy(session_window($"time", "10 seconds"))
        .agg(count("*").as("counts"))
        .orderBy($"session_window.start".asc)
        .select($"session_window.start".cast("string"), $"session_window.end".cast("string"),
          $"counts"),
      Seq(
        Row("2016-03-27 19:39:30", "2016-03-27 19:39:40", 1)
      )
    )
  }

  test("session window groupBy statement") {
    val df = Seq(
      ("2016-03-27 19:39:34", 1, "a"),
      ("2016-03-27 19:39:56", 2, "a"),
      ("2016-03-27 19:39:27", 4, "b")).toDF("time", "value", "id")

    // session window handles sort while applying group by
    // whereas time window doesn't

    checkAnswer(
      df.groupBy(session_window($"time", "10 seconds"))
        .agg(count("*").as("counts"))
        .orderBy($"session_window.start".asc)
        .select("counts"),
      Seq(Row(2), Row(1))
    )
  }

  test("session window groupBy with multiple keys statement") {
    val df = Seq(
      ("2016-03-27 19:39:34", 1, "a"),
      ("2016-03-27 19:39:39", 1, "a"),
      ("2016-03-27 19:39:56", 2, "a"),
      ("2016-03-27 19:40:04", 2, "a"),
      ("2016-03-27 19:39:27", 4, "b")).toDF("time", "value", "id")

    // session window handles sort while applying group by
    // whereas time window doesn't

    // expected sessions
    // key "a" => (19:39:34 ~ 19:39:49) (19:39:56 ~ 19:40:14)
    // key "b" => (19:39:27 ~ 19:39:37)

    checkAnswer(
      df.groupBy(session_window($"time", "10 seconds"), 'id)
        .agg(count("*").as("counts"), sum("value").as("sum"))
        .orderBy($"session_window.start".asc)
        .selectExpr("CAST(session_window.start AS STRING)", "CAST(session_window.end AS STRING)",
          "id", "counts", "sum"),

      Seq(
        Row("2016-03-27 19:39:27", "2016-03-27 19:39:37", "b", 1, 4),
        Row("2016-03-27 19:39:34", "2016-03-27 19:39:49", "a", 2, 2),
        Row("2016-03-27 19:39:56", "2016-03-27 19:40:14", "a", 2, 4)
      )
    )
  }

  test("session window groupBy with multiple keys statement - one distinct") {
    val df = Seq(
      ("2016-03-27 19:39:34", 1, "a"),
      ("2016-03-27 19:39:39", 1, "a"),
      ("2016-03-27 19:39:56", 2, "a"),
      ("2016-03-27 19:40:04", 2, "a"),
      ("2016-03-27 19:39:27", 4, "b")).toDF("time", "value", "id")

    // session window handles sort while applying group by
    // whereas time window doesn't

    // expected sessions
    // key "a" => (19:39:34 ~ 19:39:49) (19:39:56 ~ 19:40:14)
    // key "b" => (19:39:27 ~ 19:39:37)

    checkAnswer(
      df.groupBy(session_window($"time", "10 seconds"), 'id)
        .agg(count("*").as("counts"), sum_distinct(col("value")).as("sum"))
        .orderBy($"session_window.start".asc)
        .selectExpr("CAST(session_window.start AS STRING)", "CAST(session_window.end AS STRING)",
          "id", "counts", "sum"),
      Seq(
        Row("2016-03-27 19:39:27", "2016-03-27 19:39:37", "b", 1, 4),
        Row("2016-03-27 19:39:34", "2016-03-27 19:39:49", "a", 2, 1),
        Row("2016-03-27 19:39:56", "2016-03-27 19:40:14", "a", 2, 2)
      )
    )
  }

  test("session window groupBy with multiple keys statement - two distinct") {
    val df = Seq(
      ("2016-03-27 19:39:34", 1, 2, "a"),
      ("2016-03-27 19:39:39", 1, 2, "a"),
      ("2016-03-27 19:39:56", 2, 4, "a"),
      ("2016-03-27 19:40:04", 2, 4, "a"),
      ("2016-03-27 19:39:27", 4, 8, "b")).toDF("time", "value", "value2", "id")

    // session window handles sort while applying group by
    // whereas time window doesn't

    // expected sessions
    // key "a" => (19:39:34 ~ 19:39:49) (19:39:56 ~ 19:40:14)
    // key "b" => (19:39:27 ~ 19:39:37)

    checkAnswer(
      df.groupBy(session_window($"time", "10 seconds"), 'id)
        .agg(sum_distinct(col("value")).as("sum"), sum_distinct(col("value2")).as("sum2"))
        .orderBy($"session_window.start".asc)
        .selectExpr("CAST(session_window.start AS STRING)", "CAST(session_window.end AS STRING)",
          "id", "sum", "sum2"),
      Seq(
        Row("2016-03-27 19:39:27", "2016-03-27 19:39:37", "b", 4, 8),
        Row("2016-03-27 19:39:34", "2016-03-27 19:39:49", "a", 1, 2),
        Row("2016-03-27 19:39:56", "2016-03-27 19:40:14", "a", 2, 4)
      )
    )
  }

  test("session window groupBy with multiple keys statement - keys overlapped with sessions") {
    val df = Seq(
      ("2016-03-27 19:39:34", 1, "a"),
      ("2016-03-27 19:39:39", 1, "b"),
      ("2016-03-27 19:39:40", 2, "a"),
      ("2016-03-27 19:39:45", 2, "b"),
      ("2016-03-27 19:39:27", 4, "b")).toDF("time", "value", "id")

    // session window handles sort while applying group by
    // whereas time window doesn't

    // expected sessions
    // a => (19:39:34 ~ 19:39:50)
    // b => (19:39:27 ~ 19:39:37), (19:39:39 ~ 19:39:55)

    checkAnswer(
      df.groupBy(session_window($"time", "10 seconds"), 'id)
        .agg(count("*").as("counts"), sum("value").as("sum"))
        .orderBy($"session_window.start".asc)
        .selectExpr("CAST(session_window.start AS STRING)", "CAST(session_window.end AS STRING)",
          "id", "counts", "sum"),

      Seq(
        Row("2016-03-27 19:39:27", "2016-03-27 19:39:37", "b", 1, 4),
        Row("2016-03-27 19:39:34", "2016-03-27 19:39:50", "a", 2, 3),
        Row("2016-03-27 19:39:39", "2016-03-27 19:39:55", "b", 2, 3)
      )
    )
  }

  test("session window with multi-column projection") {
    val df = Seq(
      ("2016-03-27 19:39:34", 1, "a"),
      ("2016-03-27 19:39:56", 2, "a"),
      ("2016-03-27 19:39:27", 4, "b")).toDF("time", "value", "id")
      .select(session_window($"time", "10 seconds"), $"value")
      .orderBy($"session_window.start".asc)
      .select($"session_window.start".cast("string"), $"session_window.end".cast("string"),
        $"value")

    val expands = df.queryExecution.optimizedPlan.find(_.isInstanceOf[Expand])
    assert(expands.isEmpty, "Session windows shouldn't require expand")

    checkAnswer(
      df,
      Seq(
        Row("2016-03-27 19:39:27", "2016-03-27 19:39:37", 4),
        Row("2016-03-27 19:39:34", "2016-03-27 19:39:44", 1),
        Row("2016-03-27 19:39:56", "2016-03-27 19:40:06", 2)
      )
    )
  }

  test("session window combined with explode expression") {
    val df = Seq(
      ("2016-03-27 19:39:34", 1, Seq("a", "b")),
      ("2016-03-27 19:39:56", 2, Seq("a", "c", "d"))).toDF("time", "value", "ids")

    checkAnswer(
      df.select(session_window($"time", "10 seconds"), $"value", explode($"ids"))
        .orderBy($"session_window.start".asc).select("value"),
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
      df.select(session_window($"time", "10 seconds"), $"value")
        .orderBy($"session_window.start".asc)
        .select("value")
        .as[Int],
      1, 2) // null columns are dropped
  }

  // NOTE: unlike time window, joining session windows without grouping
  // doesn't arrange session, so two rows will be joined only if session range is exactly same

  test("multiple session windows in a single operator throws nice exception") {
    val df = Seq(
      ("2016-03-27 09:00:02", 3),
      ("2016-03-27 09:00:35", 6)).toDF("time", "value")
    val e = intercept[AnalysisException] {
      df.select(session_window($"time", "10 second"), session_window($"time", "15 second"))
        .collect()
    }
    assert(e.getMessage.contains(
      "Multiple time/session window expressions would result in a cartesian product"))
  }

  test("aliased session windows") {
    val df = Seq(
      ("2016-03-27 19:39:34", 1, Seq("a", "b")),
      ("2016-03-27 19:39:56", 2, Seq("a", "c", "d"))).toDF("time", "value", "ids")

    checkAnswer(
      df.select(session_window($"time", "10 seconds").as("session_window"), $"value")
        .orderBy($"session_window.start".asc)
        .select("value"),
      Seq(Row(1), Row(2))
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
        spark.sql(s"""select session_window(time, "10 seconds"), value from $table""")
          .select($"session_window.start".cast(StringType), $"session_window.end".cast(StringType),
            $"value"),
        Seq(
          Row("2016-03-27 19:39:27", "2016-03-27 19:39:37", 4),
          Row("2016-03-27 19:39:34", "2016-03-27 19:39:44", 1),
          Row("2016-03-27 19:39:56", "2016-03-27 19:40:06", 2)
        )
      )
    }
  }
}
