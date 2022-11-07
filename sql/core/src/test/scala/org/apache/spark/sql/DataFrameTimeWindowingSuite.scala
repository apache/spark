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

import java.time.LocalDateTime

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Expand, Filter}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class DataFrameTimeWindowingSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  test("simple tumbling window with record at window start") {
    val df1 = Seq(("2016-03-27 19:39:30", 1, "a")).toDF("time", "value", "id")
    val df2 = Seq((LocalDateTime.parse("2016-03-27T19:39:30"), 1, "a")).toDF("time", "value", "id")

    Seq(df1, df2).foreach { df =>
      checkAnswer(
        df.groupBy(window($"time", "10 seconds"))
          .agg(count("*").as("counts"))
          .orderBy($"window.start".asc)
          .select($"window.start".cast("string"), $"window.end".cast("string"), $"counts"),
        Seq(
          Row("2016-03-27 19:39:30", "2016-03-27 19:39:40", 1)
        )
      )
    }
  }

  test("SPARK-21590: tumbling window using negative start time") {
    val df1 = Seq(
      ("2016-03-27 19:39:30", 1, "a"),
      ("2016-03-27 19:39:25", 2, "a")).toDF("time", "value", "id")
    val df2 = Seq((LocalDateTime.parse("2016-03-27T19:39:30"), 1, "a"),
      (LocalDateTime.parse("2016-03-27T19:39:25"), 2, "a")).toDF("time", "value", "id")

    Seq(df1, df2).foreach { df =>
      checkAnswer(
        df.groupBy(window($"time", "10 seconds", "10 seconds", "-5 seconds"))
          .agg(count("*").as("counts"))
          .orderBy($"window.start".asc)
          .select($"window.start".cast("string"), $"window.end".cast("string"), $"counts"),
        Seq(
          Row("2016-03-27 19:39:25", "2016-03-27 19:39:35", 2)
        )
      )
    }
  }

  test("tumbling window groupBy statement") {
    val df1 = Seq(
      ("2016-03-27 19:39:34", 1, "a"),
      ("2016-03-27 19:39:56", 2, "a"),
      ("2016-03-27 19:39:27", 4, "b")).toDF("time", "value", "id")
    val df2 = Seq(
      (LocalDateTime.parse("2016-03-27T19:39:34"), 1, "a"),
      (LocalDateTime.parse("2016-03-27T19:39:56"), 2, "a"),
      (LocalDateTime.parse("2016-03-27T19:39:27"), 4, "b")).toDF("time", "value", "id")

    Seq(df1, df2).foreach { df =>
      checkAnswer(
        df.groupBy(window($"time", "10 seconds"))
          .agg(count("*").as("counts"))
          .orderBy($"window.start".asc)
          .select("counts"),
        Seq(Row(1), Row(1), Row(1))
      )
    }
  }

  test("tumbling window groupBy statement with startTime") {
    val df1 = Seq(
      ("2016-03-27 19:39:34", 1, "a"),
      ("2016-03-27 19:39:56", 2, "a"),
      ("2016-03-27 19:39:27", 4, "b")).toDF("time", "value", "id")
    val df2 = Seq(
      (LocalDateTime.parse("2016-03-27T19:39:34"), 1, "a"),
      (LocalDateTime.parse("2016-03-27T19:39:56"), 2, "a"),
      (LocalDateTime.parse("2016-03-27T19:39:27"), 4, "b")).toDF("time", "value", "id")

    Seq(df1, df2).foreach { df =>
      checkAnswer(
        df.groupBy(window($"time", "10 seconds", "10 seconds", "5 seconds"), $"id")
          .agg(count("*").as("counts"))
          .orderBy($"window.start".asc)
          .select("counts"),
        Seq(Row(1), Row(1), Row(1))
      )
    }
  }

  test("SPARK-21590: tumbling window groupBy statement with negative startTime") {
    val df1 = Seq(
      ("2016-03-27 19:39:34", 1, "a"),
      ("2016-03-27 19:39:56", 2, "a"),
      ("2016-03-27 19:39:27", 4, "b")).toDF("time", "value", "id")
    val df2 = Seq(
      (LocalDateTime.parse("2016-03-27T19:39:34"), 1, "a"),
      (LocalDateTime.parse("2016-03-27T19:39:56"), 2, "a"),
      (LocalDateTime.parse("2016-03-27T19:39:27"), 4, "b")).toDF("time", "value", "id")

    Seq(df1, df2).foreach { df =>
      checkAnswer(
        df.groupBy(window($"time", "10 seconds", "10 seconds", "-5 seconds"), $"id")
          .agg(count("*").as("counts"))
          .orderBy($"window.start".asc)
          .select("counts"),
        Seq(Row(1), Row(1), Row(1))
      )
    }
  }

  test("tumbling window with multi-column projection") {
    val df1 = Seq(
      ("2016-03-27 19:39:34", 1, "a"),
      ("2016-03-27 19:39:56", 2, "a"),
      ("2016-03-27 19:39:27", 4, "b")).toDF("time", "value", "id")
      .select(window($"time", "10 seconds"), $"value")
      .orderBy($"window.start".asc)
      .select($"window.start".cast("string"), $"window.end".cast("string"), $"value")
    val df2 = Seq(
      (LocalDateTime.parse("2016-03-27T19:39:34"), 1, "a"),
      (LocalDateTime.parse("2016-03-27T19:39:56"), 2, "a"),
      (LocalDateTime.parse("2016-03-27T19:39:27"), 4, "b")).toDF("time", "value", "id")
      .select(window($"time", "10 seconds"), $"value")
      .orderBy($"window.start".asc)
      .select($"window.start".cast("string"), $"window.end".cast("string"), $"value")

    Seq(df1, df2).foreach { df =>
      val expands = df.queryExecution.optimizedPlan.find(_.isInstanceOf[Expand])
      assert(expands.isEmpty, "Tumbling windows shouldn't require expand")

      checkAnswer(
        df,
        Seq(
          Row("2016-03-27 19:39:20", "2016-03-27 19:39:30", 4),
          Row("2016-03-27 19:39:30", "2016-03-27 19:39:40", 1),
          Row("2016-03-27 19:39:50", "2016-03-27 19:40:00", 2)
        )
      )
    }
  }

  test("sliding window grouping") {
    val df1 = Seq(
      ("2016-03-27 19:39:34", 1, "a"),
      ("2016-03-27 19:39:56", 2, "a"),
      ("2016-03-27 19:39:27", 4, "b")).toDF("time", "value", "id")
    val df2 = Seq(
      (LocalDateTime.parse("2016-03-27T19:39:34"), 1, "a"),
      (LocalDateTime.parse("2016-03-27T19:39:56"), 2, "a"),
      (LocalDateTime.parse("2016-03-27T19:39:27"), 4, "b")).toDF("time", "value", "id")

    Seq(df1, df2).foreach { df =>
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
  }

  test("sliding window projection") {
    val df1 = Seq(
      ("2016-03-27 19:39:34", 1, "a"),
      ("2016-03-27 19:39:56", 2, "a"),
      ("2016-03-27 19:39:27", 4, "b")).toDF("time", "value", "id")
      .select(window($"time", "10 seconds", "3 seconds", "0 second"), $"value")
      .orderBy($"window.start".asc, $"value".desc).select("value")
    val df2 = Seq(
      (LocalDateTime.parse("2016-03-27T19:39:34"), 1, "a"),
      (LocalDateTime.parse("2016-03-27T19:39:56"), 2, "a"),
      (LocalDateTime.parse("2016-03-27T19:39:27"), 4, "b")).toDF("time", "value", "id")
      .select(window($"time", "10 seconds", "3 seconds", "0 second"), $"value")
      .orderBy($"window.start".asc, $"value".desc).select("value")

    Seq(df1, df2).foreach { df =>
      val expands = df.queryExecution.optimizedPlan.find(_.isInstanceOf[Expand])
      assert(expands.nonEmpty, "Sliding windows require expand")

      checkAnswer(
        df,
        // 2016-03-27 19:39:27 UTC -> 4 bins
        // 2016-03-27 19:39:34 UTC -> 3 bins
        // 2016-03-27 19:39:56 UTC -> 3 bins
        Seq(Row(4), Row(4), Row(4), Row(4), Row(1), Row(1), Row(1), Row(2), Row(2), Row(2))
      )
    }
  }

  test("windowing combined with explode expression") {
    val df1 = Seq(
      ("2016-03-27 19:39:34", 1, Seq("a", "b")),
      ("2016-03-27 19:39:56", 2, Seq("a", "c", "d"))).toDF("time", "value", "ids")
    val df2 = Seq(
      (LocalDateTime.parse("2016-03-27T19:39:34"), 1, Seq("a", "b")),
      (LocalDateTime.parse("2016-03-27T19:39:56"), 2, Seq("a", "c", "d"))).toDF(
"time", "value", "ids")

    Seq(df1, df2).foreach { df =>
      checkAnswer(
        df.select(window($"time", "10 seconds"), $"value", explode($"ids"))
          .orderBy($"window.start".asc).select("value"),
        // first window exploded to two rows for "a", and "b", second window exploded to 3 rows
        Seq(Row(1), Row(1), Row(2), Row(2), Row(2))
      )
    }
  }

  test("null timestamps") {
    val df1 = Seq(
      ("2016-03-27 09:00:05", 1),
      ("2016-03-27 09:00:32", 2),
      (null, 3),
      (null, 4)).toDF("time", "value")
    val df2 = Seq(
      (LocalDateTime.parse("2016-03-27T09:00:05"), 1),
      (LocalDateTime.parse("2016-03-27T09:00:32"), 2),
      (null, 3),
      (null, 4)).toDF("time", "value")

    Seq(df1, df2).foreach { df =>
      checkDataset(
        df.select(window($"time", "10 seconds"), $"value")
          .orderBy($"window.start".asc)
          .select("value")
          .as[Int],
        1, 2) // null columns are dropped
    }
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

    val df3 = Seq(
      (LocalDateTime.parse("2016-03-27T09:00:05"), 1),
      (LocalDateTime.parse("2016-03-27T09:00:32"), 2),
      (null, 3),
      (null, 4)).toDF("time", "value")

    val df4 = Seq(
      (LocalDateTime.parse("2016-03-27T09:00:02"), 3),
      (LocalDateTime.parse("2016-03-27T09:00:35"), 6)).toDF("time", "othervalue")

    Seq((df, df2), (df3, df4)).foreach { tuple =>
      checkAnswer(
        tuple._1.select(window($"time", "10 seconds"), $"value").join(
          tuple._2.select(window($"time", "10 seconds"), $"othervalue"), Seq("window"))
          .groupBy("window")
          .agg((sum("value") + sum("othervalue")).as("total"))
          .orderBy($"window.start".asc).select("total"),
        Seq(Row(4), Row(8))
      )
    }
  }

  test("negative timestamps") {
    val df1 = Seq(
      ("1970-01-01 00:00:02", 1),
      ("1970-01-01 00:00:12", 2)).toDF("time", "value")
    val df2 = Seq(
      (LocalDateTime.parse("1970-01-01T00:00:02"), 1),
      (LocalDateTime.parse("1970-01-01T00:00:12"), 2)).toDF("time", "value")

    Seq(df1, df2).foreach { df =>
      checkAnswer(
        df.select(window($"time", "10 seconds", "10 seconds", "5 seconds"), $"value")
          .orderBy($"window.start".asc)
          .select($"window.start".cast(StringType), $"window.end".cast(StringType), $"value"),
        Seq(
          Row("1969-12-31 23:59:55", "1970-01-01 00:00:05", 1),
          Row("1970-01-01 00:00:05", "1970-01-01 00:00:15", 2))
      )
    }
  }

  test("multiple time windows in a single operator throws nice exception") {
    val df1 = Seq(
      ("2016-03-27 09:00:02", 3),
      ("2016-03-27 09:00:35", 6)).toDF("time", "value")
    val df2 = Seq(
      (LocalDateTime.parse("2016-03-27T09:00:02"), 3),
      (LocalDateTime.parse("2016-03-27T09:00:35"), 6)).toDF("time", "value")

    Seq(df1, df2).foreach { df =>
      val e = intercept[AnalysisException] {
        df.select(window($"time", "10 second"), window($"time", "15 second")).collect()
      }
      assert(e.getMessage.contains(
        "Multiple time/session window expressions would result in a cartesian product"))
    }
  }

  test("aliased windows") {
    val df1 = Seq(
      ("2016-03-27 19:39:34", 1, Seq("a", "b")),
      ("2016-03-27 19:39:56", 2, Seq("a", "c", "d"))).toDF("time", "value", "ids")
    val df2 = Seq(
      (LocalDateTime.parse("2016-03-27T19:39:34"), 1, Seq("a", "b")),
      (LocalDateTime.parse("2016-03-27T19:39:56"), 2, Seq("a", "c", "d"))).toDF(
      "time", "value", "ids")

    Seq(df1, df2).foreach { df =>
      checkAnswer(
        df.select(window($"time", "10 seconds").as("time_window"), $"value")
          .orderBy($"time_window.start".asc)
          .select("value"),
        Seq(Row(1), Row(2))
      )
    }
  }

  test("millisecond precision sliding windows") {
    val df1 = Seq(
      ("2016-03-27 09:00:00.41", 3),
      ("2016-03-27 09:00:00.62", 6),
      ("2016-03-27 09:00:00.715", 8)).toDF("time", "value")
    val df2 = Seq(
      (LocalDateTime.parse("2016-03-27T09:00:00.41"), 3),
      (LocalDateTime.parse("2016-03-27T09:00:00.62"), 6),
      (LocalDateTime.parse("2016-03-27T09:00:00.715"), 8)).toDF("time", "value")

    Seq(df1, df2).foreach { df =>
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
  }

  private def withTempTable(f: String => Unit): Unit = {
    val tableName = "temp"
    val df1 = Seq(
      ("2016-03-27 19:39:34", 1),
      ("2016-03-27 19:39:56", 2),
      ("2016-03-27 19:39:27", 4)).toDF("time", "value")
    val df2 = Seq(
      (LocalDateTime.parse("2016-03-27T19:39:34"), 1),
      (LocalDateTime.parse("2016-03-27T19:39:56"), 2),
      (LocalDateTime.parse("2016-03-27T19:39:27"), 4)).toDF("time", "value")

    Seq(df1, df2).foreach { df =>
      df.createOrReplaceTempView(tableName)
      try {
        f(tableName)
      } finally {
        spark.catalog.dropTempView(tableName)
      }
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

  test("time window in SQL with two expressions") {
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

  test("time window in SQL with three expressions") {
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

  test("SPARK-21590: time window in SQL with three expressions including negative start time") {
    withTempTable { table =>
      checkAnswer(
        spark.sql(
          s"""select window(time, "10 seconds", 10000000, "-5 seconds"), value from $table""")
          .select($"window.start".cast(StringType), $"window.end".cast(StringType), $"value"),
        Seq(
          Row("2016-03-27 19:39:25", "2016-03-27 19:39:35", 1),
          Row("2016-03-27 19:39:25", "2016-03-27 19:39:35", 4),
          Row("2016-03-27 19:39:55", "2016-03-27 19:40:05", 2)
        )
      )
    }
  }

  test("SPARK-36091: Support TimestampNTZ type in expression TimeWindow") {
    val df1 = Seq(
      ("2016-03-27 19:39:30", 1, "a"),
      ("2016-03-27 19:39:25", 2, "a")).toDF("time", "value", "id")
    val df2 = Seq((LocalDateTime.parse("2016-03-27T19:39:30"), 1, "a"),
      (LocalDateTime.parse("2016-03-27T19:39:25"), 2, "a")).toDF("time", "value", "id")
    val type1 = StructType(
      Seq(StructField("start", TimestampType), StructField("end", TimestampType)))
    val type2 = StructType(
      Seq(StructField("start", TimestampNTZType), StructField("end", TimestampNTZType)))

    Seq((df1, type1), (df2, type2)).foreach { tuple =>
      val logicalPlan =
        tuple._1.groupBy(window($"time", "10 seconds", "10 seconds", "-5 seconds"))
          .agg(count("*").as("counts"))
          .orderBy($"window.start".asc)
          .select($"window.start".cast("string"), $"window.end".cast("string"), $"counts")
      val aggregate = logicalPlan.queryExecution.analyzed.children(0).children(0)
      assert(aggregate.isInstanceOf[Aggregate])
      val timeWindow = aggregate.asInstanceOf[Aggregate].groupingExpressions(0)
      assert(timeWindow.isInstanceOf[AttributeReference])
      val attributeReference = timeWindow.asInstanceOf[AttributeReference]
      assert(attributeReference.name == "window")
      assert(attributeReference.dataType == tuple._2)
    }
  }

  test("No need to filter windows when windowDuration is multiple of slideDuration") {
    val df1 = Seq(
      ("2022-02-15 19:39:34", 1, "a"),
      ("2022-02-15 19:39:56", 2, "a"),
      ("2022-02-15 19:39:27", 4, "b")).toDF("time", "value", "id")
      .select(window($"time", "9 seconds", "3 seconds", "0 second"), $"value")
      .orderBy($"window.start".asc, $"value".desc).select("value")
    val df2 = Seq(
      (LocalDateTime.parse("2022-02-15T19:39:34"), 1, "a"),
      (LocalDateTime.parse("2022-02-15T19:39:56"), 2, "a"),
      (LocalDateTime.parse("2022-02-15T19:39:27"), 4, "b")).toDF("time", "value", "id")
      .select(window($"time", "9 seconds", "3 seconds", "0 second"), $"value")
      .orderBy($"window.start".asc, $"value".desc).select("value")

    val df3 = Seq(
      ("2022-02-15 19:39:34", 1, "a"),
      ("2022-02-15 19:39:56", 2, "a"),
      ("2022-02-15 19:39:27", 4, "b")).toDF("time", "value", "id")
      .select(window($"time", "9 seconds", "3 seconds", "-2 second"), $"value")
      .orderBy($"window.start".asc, $"value".desc).select("value")
    val df4 = Seq(
      (LocalDateTime.parse("2022-02-15T19:39:34"), 1, "a"),
      (LocalDateTime.parse("2022-02-15T19:39:56"), 2, "a"),
      (LocalDateTime.parse("2022-02-15T19:39:27"), 4, "b")).toDF("time", "value", "id")
      .select(window($"time", "9 seconds", "3 seconds", "2 second"), $"value")
      .orderBy($"window.start".asc, $"value".desc).select("value")

    Seq(df1, df2, df3, df4).foreach { df =>
      val filter = df.queryExecution.optimizedPlan.find(_.isInstanceOf[Filter])
      assert(filter.isDefined)
      val exist = filter.get.constraints.filter(e =>
        e.toString.contains(">=") || e.toString.contains("<"))
      assert(exist.isEmpty, "No need to filter windows " +
        "when windowDuration is multiple of slideDuration")
    }
  }

  test("SPARK-38227: 'start' and 'end' fields should be nullable") {
    // We expect the fields in window struct as nullable since the dataType of TimeWindow defines
    // them as nullable. The rule 'TimeWindowing' should respect the dataType.
    val df1 = Seq(
      ("2016-03-27 09:00:05", 1),
      ("2016-03-27 09:00:32", 2)).toDF("time", "value")
    val df2 = Seq(
      (LocalDateTime.parse("2016-03-27T09:00:05"), 1),
      (LocalDateTime.parse("2016-03-27T09:00:32"), 2)).toDF("time", "value")

    def validateWindowColumnInSchema(schema: StructType, colName: String): Unit = {
      schema.find(_.name == colName) match {
        case Some(StructField(_, st: StructType, _, _)) =>
          assertFieldInWindowStruct(st, "start")
          assertFieldInWindowStruct(st, "end")

        case _ => fail("Failed to find suitable window column from DataFrame!")
      }
    }

    def assertFieldInWindowStruct(windowType: StructType, fieldName: String): Unit = {
      val field = windowType.fields.find(_.name == fieldName)
      assert(field.isDefined, s"'$fieldName' field should exist in window struct")
      assert(field.get.nullable, s"'$fieldName' field should be nullable")
    }

    for {
      df <- Seq(df1, df2)
      nullable <- Seq(true, false)
    } {
      val dfWithDesiredNullability = new DataFrame(df.queryExecution, RowEncoder(
        StructType(df.schema.fields.map(_.copy(nullable = nullable)))))
      // tumbling windows
      val windowedProject = dfWithDesiredNullability
        .select(window($"time", "10 seconds").as("window"), $"value")
      val schema = windowedProject.queryExecution.optimizedPlan.schema
      validateWindowColumnInSchema(schema, "window")

      // sliding windows
      val windowedProject2 = dfWithDesiredNullability
        .select(window($"time", "10 seconds", "3 seconds").as("window"),
        $"value")
      val schema2 = windowedProject2.queryExecution.optimizedPlan.schema
      validateWindowColumnInSchema(schema2, "window")
    }
  }

  test("window_time function on raw window column") {
    val df = Seq(
      ("2016-03-27 19:38:18"), ("2016-03-27 19:39:25")
    ).toDF("time")

    checkAnswer(
      df.select(window($"time", "10 seconds").as("window"))
        .select(
          $"window.end".cast("string"),
          window_time($"window").cast("string")
        ),
      Seq(
        Row("2016-03-27 19:38:20", "2016-03-27 19:38:19.999999"),
        Row("2016-03-27 19:39:30", "2016-03-27 19:39:29.999999")
      )
    )
  }

  test("2 window_time functions on raw window column") {
    val df = Seq(
      ("2016-03-27 19:38:18"), ("2016-03-27 19:39:25")
    ).toDF("time")

    val df2 = df
      .withColumn("time2", expr("time - INTERVAL 15 minutes"))
      .select(window($"time", "10 seconds").as("window1"), $"time2")
      .select($"window1", window($"time2", "10 seconds").as("window2"))

    checkAnswer(
      df2.select(
        $"window1.end".cast("string"),
        window_time($"window1").cast("string"),
        $"window2.end".cast("string"),
        window_time($"window2").cast("string")),
      Seq(
        Row("2016-03-27 19:38:20", "2016-03-27 19:38:19.999999",
            "2016-03-27 19:23:20", "2016-03-27 19:23:19.999999"),
        Row("2016-03-27 19:39:30", "2016-03-27 19:39:29.999999",
            "2016-03-27 19:24:30", "2016-03-27 19:24:29.999999"))
    )

    // check column names
    val df3 = df2
      .select(
        window_time($"window1").cast("string"),
        window_time($"window2").cast("string"),
        window_time($"window2").as("wt2_aliased").cast("string")
      )

    val schema = df3.schema

    assert(schema.fields.exists(_.name == "window_time(window1)"))
    assert(schema.fields.exists(_.name == "window_time(window2)"))
    assert(schema.fields.exists(_.name == "wt2_aliased"))
  }

  test("window_time function on agg output") {
    val df = Seq(
      ("2016-03-27 19:38:19", 1), ("2016-03-27 19:39:25", 2)
    ).toDF("time", "value")
    checkAnswer(
      df.groupBy(window($"time", "10 seconds"))
        .agg(count("*").as("counts"))
        .orderBy($"window.start".asc)
        .select(
          $"window.start".cast("string"),
          $"window.end".cast("string"),
          window_time($"window").cast("string"),
          $"counts"),
      Seq(
        Row("2016-03-27 19:38:10", "2016-03-27 19:38:20", "2016-03-27 19:38:19.999999", 1),
        Row("2016-03-27 19:39:20", "2016-03-27 19:39:30", "2016-03-27 19:39:29.999999", 1)
      )
    )
  }
}
