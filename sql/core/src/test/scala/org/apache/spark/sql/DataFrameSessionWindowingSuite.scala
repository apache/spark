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

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, GreaterThan}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Expand, Filter, LogicalPlan, Project}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

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
      df.groupBy(session_window($"time", "10 seconds"), $"id")
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
      df.groupBy(session_window($"time", "10 seconds"), $"id")
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
      df.groupBy(session_window($"time", "10 seconds"), $"id")
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
      df.groupBy(session_window($"time", "10 seconds"), $"id")
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
      ("2016-03-27 19:39:34", 1, "10 seconds"),
      ("2016-03-27 19:39:56", 2, "20 seconds"),
      ("2016-03-27 19:39:27", 4, "30 seconds")).toDF("time", "value", "duration")
      .createOrReplaceTempView(tableName)
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

  test("SPARK-36465: time window in SQL with dynamic string expression") {
    withTempTable { table =>
      checkAnswer(
        spark.sql(s"""select session_window(time, duration), value from $table""")
          .select($"session_window.start".cast(StringType), $"session_window.end".cast(StringType),
            $"value"),
        Seq(
          Row("2016-03-27 19:39:27", "2016-03-27 19:39:57", 4),
          Row("2016-03-27 19:39:34", "2016-03-27 19:39:44", 1),
          Row("2016-03-27 19:39:56", "2016-03-27 19:40:16", 2)
        )
      )
    }
  }

  test("SPARK-36465: Unsupported dynamic gap datatype") {
    withTempTable { table =>
      val err = intercept[AnalysisException] {
        spark.sql(s"""select session_window(time, 1.0), value from $table""")
          .select($"session_window.start".cast(StringType), $"session_window.end".cast(StringType),
            $"value")
      }
      assert(err.message.contains("Gap duration expression used in session window must be " +
        "CalendarIntervalType, but got DecimalType(2,1)"))
    }
  }

  test("SPARK-36465: time window in SQL with UDF as gap duration") {
    withTempTable { table =>

      spark.udf.register("gapDuration",
        (i: java.lang.Integer) => s"${i * 10} seconds")

      checkAnswer(
        spark.sql(s"""select session_window(time, gapDuration(value)), value from $table""")
          .select($"session_window.start".cast(StringType), $"session_window.end".cast(StringType),
            $"value"),
        Seq(
          Row("2016-03-27 19:39:27", "2016-03-27 19:40:07", 4),
          Row("2016-03-27 19:39:34", "2016-03-27 19:39:44", 1),
          Row("2016-03-27 19:39:56", "2016-03-27 19:40:16", 2)
        )
      )
    }
  }

  test("SPARK-36465: time window in SQL with conditional expression as gap duration") {
    withTempTable { table =>

      checkAnswer(
        spark.sql("select session_window(time, " +
          """case when value = 1 then "2 seconds" when value = 2 then "10 seconds" """ +
          s"""else "20 seconds" end), value from $table""")
          .select($"session_window.start".cast(StringType), $"session_window.end".cast(StringType),
            $"value"),
        Seq(
          Row("2016-03-27 19:39:27", "2016-03-27 19:39:47", 4),
          Row("2016-03-27 19:39:34", "2016-03-27 19:39:36", 1),
          Row("2016-03-27 19:39:56", "2016-03-27 19:40:06", 2)
        )
      )
    }
  }

  test("SPARK-36465: filter out events with negative/zero gap duration") {
    withTempTable { table =>

      spark.udf.register("gapDuration",
        (i: java.lang.Integer) => {
          if (i == 1) {
            "0 seconds"
          } else if (i == 2) {
            "-10 seconds"
          } else {
            "5 seconds"
          }
        })

      checkAnswer(
        spark.sql(s"""select session_window(time, gapDuration(value)), value from $table""")
          .groupBy($"session_window")
          .agg(count("*").as("counts"))
          .select($"session_window.start".cast("string"), $"session_window.end".cast("string"),
            $"counts"),
        Seq(Row("2016-03-27 19:39:27", "2016-03-27 19:39:32", 1))
      )
    }
  }

  test("SPARK-36465: filter out events with invalid gap duration.") {
    val df = Seq(
      ("2016-03-27 19:39:30", 1, "a")).toDF("time", "value", "id")

    checkAnswer(
      df.groupBy(session_window($"time", "x sec"))
        .agg(count("*").as("counts"))
        .orderBy($"session_window.start".asc)
        .select($"session_window.start".cast("string"), $"session_window.end".cast("string"),
          $"counts"),
      Seq()
    )

    withTempTable { table =>
      checkAnswer(
        spark.sql("select session_window(time, " +
          """case when value = 1 then "2 seconds" when value = 2 then "invalid gap duration" """ +
          s"""else "20 seconds" end), value from $table""")
          .select($"session_window.start".cast(StringType), $"session_window.end".cast(StringType),
            $"value"),
        Seq(
          Row("2016-03-27 19:39:27", "2016-03-27 19:39:47", 4),
          Row("2016-03-27 19:39:34", "2016-03-27 19:39:36", 1)
        )
      )
    }
  }

  test("SPARK-36724: Support timestamp_ntz as a type of time column for SessionWindow") {
    val df = Seq((LocalDateTime.parse("2016-03-27T19:39:30"), 1, "a"),
      (LocalDateTime.parse("2016-03-27T19:39:25"), 2, "a")).toDF("time", "value", "id")
    val aggDF =
      df.groupBy(session_window($"time", "10 seconds"))
        .agg(count("*").as("counts"))
        .orderBy($"session_window.start".asc)
        .select($"session_window.start".cast("string"),
          $"session_window.end".cast("string"), $"counts")

    val aggregate = aggDF.queryExecution.analyzed.children(0).children(0)
    assert(aggregate.isInstanceOf[Aggregate])

    val timeWindow = aggregate.asInstanceOf[Aggregate].groupingExpressions(0)
    assert(timeWindow.isInstanceOf[AttributeReference])

    val attributeReference = timeWindow.asInstanceOf[AttributeReference]
    assert(attributeReference.name == "session_window")

    val expectedSchema = StructType(
      Seq(StructField("start", TimestampNTZType), StructField("end", TimestampNTZType)))
    assert(attributeReference.dataType == expectedSchema)

    checkAnswer(aggDF, Seq(Row("2016-03-27 19:39:25", "2016-03-27 19:39:40", 2)))
  }

  test("SPARK-38227: 'start' and 'end' fields should be nullable") {
    // We expect the fields in window struct as nullable since the dataType of SessionWindow
    // defines them as nullable. The rule 'SessionWindowing' should respect the dataType.
    val df1 = Seq(
      ("hello", "2016-03-27 09:00:05", 1),
      ("structured", "2016-03-27 09:00:32", 2)).toDF("id", "time", "value")
    val df2 = Seq(
      ("world", LocalDateTime.parse("2016-03-27T09:00:05"), 1),
      ("spark", LocalDateTime.parse("2016-03-27T09:00:32"), 2)).toDF("id", "time", "value")

    val udf = spark.udf.register("gapDuration", (s: String) => {
      if (s == "hello") {
        "1 second"
      } else if (s == "structured") {
        // zero gap duration will be filtered out from aggregation
        "0 second"
      } else if (s == "world") {
        // negative gap duration will be filtered out from aggregation
        "-10 seconds"
      } else {
        "10 seconds"
      }
    })

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
      val dfWithDesiredNullability = new DataFrame(df.queryExecution, ExpressionEncoder(
        StructType(df.schema.fields.map(_.copy(nullable = nullable)))))
      // session window without dynamic gap
      val windowedProject = dfWithDesiredNullability
        .select(session_window($"time", "10 seconds").as("session"), $"value")
      val schema = windowedProject.queryExecution.optimizedPlan.schema
      validateWindowColumnInSchema(schema, "session")

      // session window with dynamic gap
      val windowedProject2 = dfWithDesiredNullability
        .select(session_window($"time", udf($"id")).as("session"), $"value")
      val schema2 = windowedProject2.queryExecution.optimizedPlan.schema
      validateWindowColumnInSchema(schema2, "session")
    }
  }

  test("SPARK-38349: No need to filter events when gapDuration greater than 0") {
    // negative gap duration
    check("-5 seconds", true, "Need to filter events when gap duration less than 0")

    // positive gap duration
    check("5 seconds", false, "No need to filter events when gap duration greater than 0")

    // invalid gap duration
    check("x seconds", true, "Need to filter events when gap duration invalid")

    // dynamic gap duration
    check(when(col("time").equalTo("1"), "5 seconds")
      .when(col("time").equalTo("2"), "10 seconds")
      .otherwise("10 seconds"), true, "Need to filter events when gap duration dynamically")

    def check(
        gapDuration: Any,
        expectTimeRange: Boolean,
        assertHintMsg: String): Unit = {
      val data = Seq(
        ("2016-03-27 19:39:30", 1, "a")).toDF("time", "value", "id")
      val df = if (gapDuration.isInstanceOf[String]) {
        data.groupBy(session_window($"time", gapDuration.asInstanceOf[String]))
      } else {
        data.groupBy(session_window($"time", gapDuration.asInstanceOf[Column]))
      }
      val aggregate = df.agg(count("*").as("counts"))
        .select($"session_window.start".cast("string"), $"session_window.end".cast("string"),
          $"counts")

      checkFilterCondition(aggregate.queryExecution.logical, expectTimeRange, assertHintMsg)
    }

    def checkFilterCondition(
        logicalPlan: LogicalPlan,
        expectTimeRange: Boolean,
        assertHintMsg: String): Unit = {
      val filter = logicalPlan.find { plan =>
        plan.isInstanceOf[Filter] && plan.children.head.isInstanceOf[Project]
      }
      assert(filter.isDefined)
      val exist = filter.get.expressions.flatMap { expr =>
        expr.collect { case gt: GreaterThan => gt }
      }
      if (expectTimeRange) {
        assert(exist.nonEmpty, assertHintMsg)
      } else {
        assert(exist.isEmpty, assertHintMsg)
      }
    }
  }

  test("SPARK-49836 using window fn with window as parameter should preserve parent operator") {
    withTempView("clicks") {
      val df = Seq(
        // small window: [00:00, 01:00), user1, 2
        ("2024-09-30 00:00:00", "user1"), ("2024-09-30 00:00:30", "user1"),
        // small window: [01:00, 02:00), user2, 2
        ("2024-09-30 00:01:00", "user2"), ("2024-09-30 00:01:30", "user2"),
        // small window: [03:00, 04:00), user1, 1
        ("2024-09-30 00:03:30", "user1"),
        // small window: [11:00, 12:00), user1, 3
        ("2024-09-30 00:11:00", "user1"), ("2024-09-30 00:11:30", "user1"),
        ("2024-09-30 00:11:45", "user1")
      ).toDF("eventTime", "userId")

      // session window: (01:00, 09:00), user1, 3 / (02:00, 07:00), user2, 2 /
      //   (12:00, 12:05), user1, 3

      df.createOrReplaceTempView("clicks")

      val aggregatedData = spark.sql(
        """
          |SELECT
          |  userId,
          |  avg(cpu_large.numClicks) AS clicksPerSession
          |FROM
          |(
          |  SELECT
          |    session_window(small_window, '5 minutes') AS session,
          |    userId,
          |    sum(numClicks) AS numClicks
          |  FROM
          |  (
          |    SELECT
          |      window(eventTime, '1 minute') AS small_window,
          |      userId,
          |      count(*) AS numClicks
          |    FROM clicks
          |    GROUP BY window, userId
          |  ) cpu_small
          |  GROUP BY session_window, userId
          |) cpu_large
          |GROUP BY userId
          |""".stripMargin)

      checkAnswer(
        aggregatedData,
        Seq(Row("user1", 3), Row("user2", 2))
      )
    }
  }
}
