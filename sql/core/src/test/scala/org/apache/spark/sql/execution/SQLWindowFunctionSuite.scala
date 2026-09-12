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

package org.apache.spark.sql.execution

import org.apache.spark.TestUtils.{assertNotSpilled, assertSpilled}
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.internal.SQLConf.{ADAPTIVE_EXECUTION_ENABLED, WINDOW_EXEC_BUFFER_IN_MEMORY_THRESHOLD, WINDOW_EXEC_BUFFER_SIZE_SPILL_THRESHOLD, WINDOW_EXEC_BUFFER_SPILL_THRESHOLD, WINDOW_EXEC_DISTINCT_HASH_FALLBACK_THRESHOLD}
import org.apache.spark.sql.test.SharedSparkSession

case class WindowData(month: Int, area: String, product: Int)


/**
 * Test suite for SQL window functions.
 */
class SQLWindowFunctionSuite extends SharedSparkSession {

  import testImplicits._

  test("window function: udaf with aggregate expression") {
    val data = Seq(
      WindowData(1, "a", 5),
      WindowData(2, "a", 6),
      WindowData(3, "b", 7),
      WindowData(4, "b", 8),
      WindowData(5, "c", 9),
      WindowData(6, "c", 10)
    )
    withTempView("windowData") {
      sparkContext.parallelize(data).toDF().createOrReplaceTempView("windowData")

      checkAnswer(
        sql(
          """
            |select area, sum(product), sum(sum(product)) over (partition by area)
            |from windowData group by month, area
          """.stripMargin),
        Seq(
          ("a", 5, 11),
          ("a", 6, 11),
          ("b", 7, 15),
          ("b", 8, 15),
          ("c", 9, 19),
          ("c", 10, 19)
        ).map(i => Row(i._1, i._2, i._3)))

      checkAnswer(
        sql(
          """
            |select area, sum(product) - 1, sum(sum(product)) over (partition by area)
            |from windowData group by month, area
          """.stripMargin),
        Seq(
          ("a", 4, 11),
          ("a", 5, 11),
          ("b", 6, 15),
          ("b", 7, 15),
          ("c", 8, 19),
          ("c", 9, 19)
        ).map(i => Row(i._1, i._2, i._3)))

      checkAnswer(
        sql(
          """
            |select area, sum(product), sum(product) / sum(sum(product)) over (partition by area)
            |from windowData group by month, area
          """.stripMargin),
        Seq(
          ("a", 5, 5d/11),
          ("a", 6, 6d/11),
          ("b", 7, 7d/15),
          ("b", 8, 8d/15),
          ("c", 10, 10d/19),
          ("c", 9, 9d/19)
        ).map(i => Row(i._1, i._2, i._3)))

      checkAnswer(
        sql(
          """
            |select area, sum(product), sum(product) / sum(sum(product) - 1) over
            |(partition by area)
            |from windowData group by month, area
          """.stripMargin),
        Seq(
          ("a", 5, 5d/9),
          ("a", 6, 6d/9),
          ("b", 7, 7d/13),
          ("b", 8, 8d/13),
          ("c", 10, 10d/17),
          ("c", 9, 9d/17)
        ).map(i => Row(i._1, i._2, i._3)))
    }
  }

  test("window function: refer column in inner select block") {
    val data = Seq(
      WindowData(1, "a", 5),
      WindowData(2, "a", 6),
      WindowData(3, "b", 7),
      WindowData(4, "b", 8),
      WindowData(5, "c", 9),
      WindowData(6, "c", 10)
    )
    withTempView("windowData") {
      sparkContext.parallelize(data).toDF().createOrReplaceTempView("windowData")

      checkAnswer(
        sql(
          """
            |select area, rank() over (partition by area order by tmp.month) + tmp.tmp1 as c1
            |from (select month, area, product, 1 as tmp1 from windowData) tmp
          """.stripMargin),
        Seq(
          ("a", 2),
          ("a", 3),
          ("b", 2),
          ("b", 3),
          ("c", 2),
          ("c", 3)
        ).map(i => Row(i._1, i._2)))
    }
  }

  test("window function: partition and order expressions") {
    val data = Seq(
      WindowData(1, "a", 5),
      WindowData(2, "a", 6),
      WindowData(3, "b", 7),
      WindowData(4, "b", 8),
      WindowData(5, "c", 9),
      WindowData(6, "c", 10)
    )
    withTempView("windowData") {
      sparkContext.parallelize(data).toDF().createOrReplaceTempView("windowData")

      checkAnswer(
        sql(
          """
            |select month, area, product, sum(product + 1) over (partition by 1 order by 2)
            |from windowData
          """.stripMargin),
        Seq(
          (1, "a", 5, 51),
          (2, "a", 6, 51),
          (3, "b", 7, 51),
          (4, "b", 8, 51),
          (5, "c", 9, 51),
          (6, "c", 10, 51)
        ).map(i => Row(i._1, i._2, i._3, i._4)))

      checkAnswer(
        sql(
          """
            |select month, area, product, sum(product)
            |over (partition by month % 2 order by 10 - product)
            |from windowData
          """.stripMargin),
        Seq(
          (1, "a", 5, 21),
          (2, "a", 6, 24),
          (3, "b", 7, 16),
          (4, "b", 8, 18),
          (5, "c", 9, 9),
          (6, "c", 10, 10)
        ).map(i => Row(i._1, i._2, i._3, i._4)))
    }
  }

  test("window function: distinct should not be silently ignored") {
    val data = Seq(
      WindowData(1, "a", 5),
      WindowData(2, "a", 6),
      WindowData(3, "b", 7),
      WindowData(4, "b", 8),
      WindowData(5, "c", 9),
      WindowData(6, "c", 10)
    )
    withTempView("windowData") {
      sparkContext.parallelize(data).toDF().createOrReplaceTempView("windowData")

      val e = intercept[AnalysisException] {
        sql(
          """
            |select month, area, product, sum(distinct product + 1) over (
            |  partition by 1 order by 2 rows between current row and current row)
            |from windowData
          """.stripMargin)
      }
      assert(e.getMessage.contains("Unsupported DISTINCT window function"))
    }
  }

  test("window function: distinct rejects unorderable inputs") {
    val e = intercept[AnalysisException] {
      sql("SELECT count(DISTINCT map('key', id)) OVER () FROM range(1)")
    }
    assert(e.getCondition === "DISTINCT_WINDOW_FUNCTION_UNSUPPORTED")
  }

  test("window function: distinct aggregates with an unbounded preceding frame") {
    val data = Seq(
      (1, 0, 10, "a", 10),
      (1, 1, 20, "a", 10),
      (1, 2, 20, "b", 20),
      (1, 3, 20, null.asInstanceOf[String], 30),
      (1, 4, 30, "c", 30),
      (2, 5, 5, "b", 5),
      (2, 6, 5, "b", 5),
      (2, 7, 6, "a", 6)
    ).toDF("k", "id", "v", "x", "amount")

    withTempView("distinctWindowData") {
      data.createOrReplaceTempView("distinctWindowData")

      checkAnswer(
        sql(
          """
            |SELECT k, id,
            |  count(DISTINCT x) OVER (PARTITION BY k ORDER BY v) AS range_count,
            |  count(DISTINCT x) OVER (
            |    PARTITION BY k ORDER BY v, id
            |    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rows_count,
            |  count(DISTINCT x) OVER (
            |    PARTITION BY k ORDER BY v, id
            |    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS preceding_count,
            |  count(DISTINCT x) OVER (
            |    PARTITION BY k ORDER BY v, id
            |    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS following_count,
            |  count(DISTINCT x) OVER (PARTITION BY k) AS partition_count,
            |  sum(DISTINCT amount) OVER (PARTITION BY k ORDER BY v) AS range_sum,
            |  avg(DISTINCT amount) OVER (PARTITION BY k ORDER BY v) AS range_avg,
            |  sort_array(collect_list(DISTINCT amount) OVER (
            |    PARTITION BY k ORDER BY v)) AS range_values
            |FROM distinctWindowData
          """.stripMargin),
        Seq(
          Row(1, 0, 1L, 1L, 0L, 1L, 3L, 10L, 10.0, Seq(10)),
          Row(1, 1, 2L, 1L, 1L, 2L, 3L, 60L, 20.0, Seq(10, 20, 30)),
          Row(1, 2, 2L, 2L, 1L, 2L, 3L, 60L, 20.0, Seq(10, 20, 30)),
          Row(1, 3, 2L, 2L, 2L, 3L, 3L, 60L, 20.0, Seq(10, 20, 30)),
          Row(1, 4, 3L, 3L, 2L, 3L, 3L, 60L, 20.0, Seq(10, 20, 30)),
          Row(2, 5, 1L, 1L, 0L, 1L, 2L, 5L, 5.0, Seq(5)),
          Row(2, 6, 1L, 1L, 1L, 2L, 2L, 5L, 5.0, Seq(5)),
          Row(2, 7, 2L, 2L, 1L, 2L, 2L, 11L, 5.5, Seq(5, 6))
        ))
    }
  }

  test("window function: count distinct with a range offset, filter, and multiple columns") {
    withSQLConf(WINDOW_EXEC_BUFFER_IN_MEMORY_THRESHOLD.key -> "1") {
      val data = Seq(
        (0, 10, "a", 1, true),
        (1, 20, "a", 1, true),
        (2, 20, "b", 1, false),
        (3, 20, "b", 2, true),
        (4, 30, "c", 3, true)
      ).toDF("id", "v", "x", "y", "selected")

      withTempView("distinctWindowData") {
        data.createOrReplaceTempView("distinctWindowData")

        checkAnswer(
          sql(
            """
              |SELECT id,
              |  count(DISTINCT x) OVER (
              |    ORDER BY v RANGE BETWEEN UNBOUNDED PRECEDING AND 5 PRECEDING)
              |      AS preceding_count,
              |  count(DISTINCT x) FILTER (WHERE selected) OVER (
              |    ORDER BY v RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
              |      AS filtered_count,
              |  count(DISTINCT x, y) OVER (
              |    ORDER BY v RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS tuple_count
              |FROM distinctWindowData
            """.stripMargin),
          Seq(
            Row(0, 0L, 1L, 1L),
            Row(1, 1L, 2L, 3L),
            Row(2, 1L, 2L, 3L),
            Row(3, 1L, 2L, 3L),
            Row(4, 2L, 3L, 4L)
          ))
      }
    }
  }

  test("window function: count distinct falls back from hash and sorter spills") {
    withSQLConf(
      WINDOW_EXEC_BUFFER_IN_MEMORY_THRESHOLD.key -> "1000",
      WINDOW_EXEC_BUFFER_SPILL_THRESHOLD.key -> "5",
      WINDOW_EXEC_DISTINCT_HASH_FALLBACK_THRESHOLD.key -> "2") {
      val result = sql(
        """
          |SELECT max(distinct_count)
          |FROM (
          |  SELECT count(DISTINCT id % 3) OVER (
          |    ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS distinct_count
          |  FROM range(100)
          |)
        """.stripMargin)
      assertSpilled(sparkContext, "count distinct window hash fallback") {
        checkAnswer(result, Row(3L))
      }
    }
  }

  test("window function: unbounded distinct frame skips the event sorter") {
    withSQLConf(
      ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      WINDOW_EXEC_BUFFER_IN_MEMORY_THRESHOLD.key -> "1000",
      WINDOW_EXEC_BUFFER_SPILL_THRESHOLD.key -> "5",
      WINDOW_EXEC_DISTINCT_HASH_FALLBACK_THRESHOLD.key -> Int.MaxValue.toString) {
      val result = sql(
        """
          |SELECT max(distinct_count)
          |FROM (
          |  SELECT count(DISTINCT id) OVER () AS distinct_count
          |  FROM range(100)
          |)
        """.stripMargin)
      assertNotSpilled(sparkContext, "unbounded distinct window without an event sorter") {
        checkAnswer(result, Row(100L))
      }
      val window = result.queryExecution.executedPlan.collectFirst {
        case window: WindowExec => window
      }.get
      assert(window.metrics("spillSize").value == 0)
    }
  }

  test("window function: unbounded distinct frame falls back by size") {
    withSQLConf(
      WINDOW_EXEC_BUFFER_IN_MEMORY_THRESHOLD.key -> "1000",
      WINDOW_EXEC_BUFFER_SPILL_THRESHOLD.key -> Int.MaxValue.toString,
      WINDOW_EXEC_BUFFER_SIZE_SPILL_THRESHOLD.key -> "1",
      WINDOW_EXEC_DISTINCT_HASH_FALLBACK_THRESHOLD.key -> Int.MaxValue.toString) {
      val result = sql(
        """
          |SELECT id,
          |  count(DISTINCT id % 3) OVER () AS distinct_count,
          |  sum(DISTINCT id % 3) OVER () AS distinct_sum,
          |  sort_array(collect_list(DISTINCT id % 3) OVER ()) AS distinct_values
          |FROM range(20)
        """.stripMargin)
      checkAnswer(
        result,
        Seq.tabulate(20)(id => Row(id.toLong, 3L, 3L, Seq(0L, 1L, 2L))))
    }
  }

  test("window function: distinct event sorter spill size is reported") {
    withSQLConf(
      ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      WINDOW_EXEC_BUFFER_IN_MEMORY_THRESHOLD.key -> "1000",
      WINDOW_EXEC_BUFFER_SPILL_THRESHOLD.key -> "5",
      WINDOW_EXEC_DISTINCT_HASH_FALLBACK_THRESHOLD.key -> Int.MaxValue.toString) {
      val result = sql(
        """
          |SELECT max(distinct_count)
          |FROM (
          |  SELECT count(DISTINCT id) OVER (
          |    ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS distinct_count
          |  FROM range(100)
          |)
        """.stripMargin)
      checkAnswer(result, Row(100L))
      val window = result.queryExecution.executedPlan.collectFirst {
        case window: WindowExec => window
      }.get
      assert(window.metrics("spillSize").value > 0)
    }
  }

  test("window function: count distinct stays in hash at the fallback threshold") {
    withSQLConf(
      WINDOW_EXEC_BUFFER_IN_MEMORY_THRESHOLD.key -> "1000",
      WINDOW_EXEC_BUFFER_SPILL_THRESHOLD.key -> "5",
      WINDOW_EXEC_DISTINCT_HASH_FALLBACK_THRESHOLD.key -> "2") {
      val result = sql(
        """
          |SELECT max(distinct_count)
          |FROM (
          |  SELECT count(DISTINCT id % 2) OVER (
          |    ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS distinct_count
          |  FROM range(100)
          |)
        """.stripMargin)
      assertNotSpilled(sparkContext, "count distinct window at hash fallback threshold") {
        checkAnswer(result, Row(2L))
      }
    }
  }

  test("window function: distinct handles binary-unstable collation across spills") {
    withSQLConf(WINDOW_EXEC_BUFFER_SPILL_THRESHOLD.key -> "1") {
      checkAnswer(
        sql(
          """
            |SELECT id,
            |  count(DISTINCT value COLLATE UTF8_LCASE) OVER (
            |    ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
            |  collect_list(DISTINCT value COLLATE UTF8_LCASE) OVER (
            |    ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            |FROM VALUES (0, 'a'), (1, 'A'), (2, 'b'), (3, 'B') AS data(id, value)
            |ORDER BY id
          """.stripMargin),
        Seq(
          Row(0, 1L, Seq("a")),
          Row(1, 1L, Seq("a")),
          Row(2, 2L, Seq("a", "b")),
          Row(3, 2L, Seq("a", "b"))))
    }
  }

  test("window function: distinct keeps variable-length inputs across row reuse") {
    withSQLConf(WINDOW_EXEC_BUFFER_SPILL_THRESHOLD.key -> "1") {
      checkAnswer(
        sql(
          """
            |SELECT id,
            |  max(DISTINCT value) OVER (
            |    ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING),
            |  first(DISTINCT value) OVER (
            |    ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING),
            |  max(DISTINCT value) OVER ()
            |FROM VALUES (0, 'z'), (1, 'a'), (2, 'b') AS data(id, value)
            |ORDER BY id
          """.stripMargin),
        Seq(
          Row(0, "z", "z", "z"),
          Row(1, "z", "z", "z"),
          Row(2, "z", "z", "z")))
    }
  }

  test("window function: distinct aggregates use normalized floating-point inputs") {
    val rows = sql(
      """
        |SELECT id, collect_list(DISTINCT value) OVER (
        |  ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS values
        |FROM VALUES
        |  (0, CAST('-0.0' AS DOUBLE)),
        |  (1, CAST('0.0' AS DOUBLE)) AS data(id, value)
        |ORDER BY id
      """.stripMargin).collect()

    val rawBits = rows.map { row =>
      row.getSeq[Double](1).map(java.lang.Double.doubleToRawLongBits)
    }
    assert(rawBits === Seq(Seq(0L), Seq(0L)))
  }

  test("window function: distinct aggregates share frames by key and filter") {
    checkAnswer(
      sql(
        """
          |SELECT id,
          |  count(DISTINCT value) FILTER (WHERE selected) OVER (
          |    ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS count_value,
          |  sum(DISTINCT value) FILTER (WHERE selected) OVER (
          |    ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sum_value,
          |  avg(DISTINCT value) FILTER (WHERE selected) OVER (
          |    ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS avg_value,
          |  sort_array(collect_list(DISTINCT value) FILTER (WHERE selected) OVER (
          |    ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS values,
          |  count(DISTINCT value) FILTER (WHERE NOT selected) OVER (
          |    ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS other_count
          |FROM VALUES
          |  (0, 1, true),
          |  (1, 1, true),
          |  (2, 2, false),
          |  (3, 3, true) AS data(id, value, selected)
          |ORDER BY id
        """.stripMargin),
      Seq(
        Row(0, 1L, 1L, 1.0, Seq(1), 0L),
        Row(1, 1L, 1L, 1.0, Seq(1), 0L),
        Row(2, 1L, 1L, 1.0, Seq(1), 1L),
        Row(3, 2L, 4L, 2.0, Seq(1, 3), 1L)))
  }

  test("window function: distinct imperative aggregates share a frame") {
    checkAnswer(
      sql(
        """
          |SELECT id,
          |  listagg(DISTINCT value, ',') OVER (
          |    ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS concatenated,
          |  sort_array(collect_list(DISTINCT value) OVER (
          |    ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS values
          |FROM VALUES
          |  (0, 'a'),
          |  (1, 'a'),
          |  (2, 'b') AS data(id, value)
          |ORDER BY id
        """.stripMargin),
      Seq(
        Row(0, "a", Seq("a")),
        Row(1, "a", Seq("a")),
        Row(2, "a,b", Seq("a", "b"))))
  }

  test("window function: listagg distinct reuses its ordering argument as a key") {
    checkAnswer(
      sql(
        """
          |SELECT id,
          |  listagg(DISTINCT value) WITHIN GROUP (ORDER BY value) OVER () AS concatenated
          |FROM VALUES (0, 'b'), (1, 'a'), (2, 'b') AS data(id, value)
          |ORDER BY id
        """.stripMargin),
      Seq(Row(0, "ab"), Row(1, "ab"), Row(2, "ab")))
  }

  test("window function: distinct foldable inputs share an empty-key frame") {
    withSQLConf(
      WINDOW_EXEC_BUFFER_SPILL_THRESHOLD.key -> "1",
      WINDOW_EXEC_DISTINCT_HASH_FALLBACK_THRESHOLD.key -> "0") {
      val result = sql(
        """
          |SELECT id,
          |  count(DISTINCT 1) OVER (
          |    ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS count_one,
          |  count(DISTINCT CAST(NULL AS INT)) OVER (
          |    ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS count_null,
          |  sum(DISTINCT 2) OVER (
          |    ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sum_two
          |FROM range(3)
          |ORDER BY id
        """.stripMargin)
      assertSpilled(sparkContext, "distinct window with an empty key") {
        checkAnswer(
          result,
          Seq(
            Row(0L, 1L, 0L, 2L),
            Row(1L, 1L, 0L, 2L),
            Row(2L, 1L, 0L, 2L)))
      }
    }
  }

  test("window function: distinct works when the window input buffer spills") {
    withSQLConf(
      WINDOW_EXEC_BUFFER_IN_MEMORY_THRESHOLD.key -> "1",
      WINDOW_EXEC_BUFFER_SPILL_THRESHOLD.key -> "2") {
      val result = sql(
        """
          |SELECT max(distinct_count)
          |FROM (
          |  SELECT count(DISTINCT 1) OVER (
          |    ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS distinct_count
          |  FROM range(100)
          |)
        """.stripMargin)
      assertSpilled(sparkContext, "distinct window with spilled input buffer") {
        checkAnswer(result, Row(1L))
      }
    }
  }

  test("window function: expressions in arguments of a window functions") {
    val data = Seq(
      WindowData(1, "a", 5),
      WindowData(2, "a", 6),
      WindowData(3, "b", 7),
      WindowData(4, "b", 8),
      WindowData(5, "c", 9),
      WindowData(6, "c", 10)
    )
    withTempView("windowData") {
      sparkContext.parallelize(data).toDF().createOrReplaceTempView("windowData")

      checkAnswer(
        sql(
          """
            |select month, area, month % 2,
            |lag(product, 1 + 1, product) over (partition by month % 2 order by area)
            |from windowData
          """.stripMargin),
        Seq(
          (1, "a", 1, 5),
          (2, "a", 0, 6),
          (3, "b", 1, 7),
          (4, "b", 0, 8),
          (5, "c", 1, 5),
          (6, "c", 0, 6)
        ).map(i => Row(i._1, i._2, i._3, i._4)))
    }
  }


  test("window function: Sorting columns are not in Project") {
    val data = Seq(
      WindowData(1, "d", 10),
      WindowData(2, "a", 6),
      WindowData(3, "b", 7),
      WindowData(4, "b", 8),
      WindowData(5, "c", 9),
      WindowData(6, "c", 11)
    )
    withTempView("windowData") {
      sparkContext.parallelize(data).toDF().createOrReplaceTempView("windowData")

      checkAnswer(
        sql("select month, product, sum(product + 1) over() from windowData order by area"),
        Seq(
          (2, 6, 57),
          (3, 7, 57),
          (4, 8, 57),
          (5, 9, 57),
          (6, 11, 57),
          (1, 10, 57)
        ).map(i => Row(i._1, i._2, i._3)))

      checkAnswer(
        sql(
          """
            |select area, rank() over (partition by area order by tmp.month) + tmp.tmp1 as c1
            |from (select month, area, product as p, 1 as tmp1 from windowData) tmp order by p
          """.stripMargin),
        Seq(
          ("a", 2),
          ("b", 2),
          ("b", 3),
          ("c", 2),
          ("d", 2),
          ("c", 3)
        ).map(i => Row(i._1, i._2)))

      checkAnswer(
        sql(
          """
            |select area, rank() over (partition by area order by month) as c1
            |from windowData group by product, area, month order by product, area
          """.stripMargin),
        Seq(
          ("a", 1),
          ("b", 1),
          ("b", 2),
          ("c", 1),
          ("d", 1),
          ("c", 2)
        ).map(i => Row(i._1, i._2)))

      checkAnswer(
        sql(
          """
            |select area, sum(product) / sum(sum(product)) over (partition by area) as c1
            |from windowData group by area, month order by month, c1
          """.stripMargin),
        Seq(
          ("d", 1.0),
          ("a", 1.0),
          ("b", 0.4666666666666667),
          ("b", 0.5333333333333333),
          ("c", 0.45),
          ("c", 0.55)
        ).map(i => Row(i._1, i._2)))
    }
  }

  // todo: fix this test case by reimplementing the function ResolveAggregateFunctions
  ignore("window function: Pushing aggregate Expressions in Sort to Aggregate") {
    val data = Seq(
      WindowData(1, "d", 10),
      WindowData(2, "a", 6),
      WindowData(3, "b", 7),
      WindowData(4, "b", 8),
      WindowData(5, "c", 9),
      WindowData(6, "c", 11)
    )
    withTempView("windowData") {
      sparkContext.parallelize(data).toDF().createOrReplaceTempView("windowData")

      checkAnswer(
        sql(
          """
            |select area, sum(product) over () as c from windowData
            |where product > 3 group by area, product
            |having avg(month) > 0 order by avg(month), product
          """.stripMargin),
        Seq(
          ("a", 51),
          ("b", 51),
          ("b", 51),
          ("c", 51),
          ("c", 51),
          ("d", 51)
        ).map(i => Row(i._1, i._2)))
    }
  }

  test("window function: multiple window expressions in a single expression") {
    val nums = sparkContext.parallelize(1 to 10).map(x => (x, x % 2)).toDF("x", "y")
    nums.createOrReplaceTempView("nums")

    val expected =
      Row(1, 1, 1, 55, 1, 57) ::
        Row(0, 2, 3, 55, 2, 60) ::
        Row(1, 3, 6, 55, 4, 65) ::
        Row(0, 4, 10, 55, 6, 71) ::
        Row(1, 5, 15, 55, 9, 79) ::
        Row(0, 6, 21, 55, 12, 88) ::
        Row(1, 7, 28, 55, 16, 99) ::
        Row(0, 8, 36, 55, 20, 111) ::
        Row(1, 9, 45, 55, 25, 125) ::
        Row(0, 10, 55, 55, 30, 140) :: Nil

    val actual = sql(
      """
        |SELECT
        |  y,
        |  x,
        |  sum(x) OVER w1 AS running_sum,
        |  sum(x) OVER w2 AS total_sum,
        |  sum(x) OVER w3 AS running_sum_per_y,
        |  ((sum(x) OVER w1) + (sum(x) OVER w2) + (sum(x) OVER w3)) as combined2
        |FROM nums
        |WINDOW w1 AS (ORDER BY x ROWS BETWEEN UnBOUNDED PRECEDiNG AND CuRRENT RoW),
        |       w2 AS (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOuNDED FoLLOWING),
        |       w3 AS (PARTITION BY y ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      """.stripMargin)

    checkAnswer(actual, expected)

    spark.catalog.dropTempView("nums")
  }

  test("window function: multiple window expressions specified by range in a single expression") {
    val nums = sparkContext.parallelize(1 to 10).map(x => (x, x % 2)).toDF("x", "y")
    nums.createOrReplaceTempView("nums")
    withTempView("nums") {
      val expected =
        Row(1, 1, 1, 4, null, 8, 25) ::
          Row(1, 3, 4, 9, 1, 12, 24) ::
          Row(1, 5, 9, 15, 4, 16, 21) ::
          Row(1, 7, 16, 21, 8, 9, 16) ::
          Row(1, 9, 25, 16, 12, null, 9) ::
          Row(0, 2, 2, 6, null, 10, 30) ::
          Row(0, 4, 6, 12, 2, 14, 28) ::
          Row(0, 6, 12, 18, 6, 18, 24) ::
          Row(0, 8, 20, 24, 10, 10, 18) ::
          Row(0, 10, 30, 18, 14, null, 10) ::
          Nil

      val actual = sql(
        """
          |SELECT
          |  y,
          |  x,
          |  sum(x) over w1 as history_sum,
          |  sum(x) over w2 as period_sum1,
          |  sum(x) over w3 as period_sum2,
          |  sum(x) over w4 as period_sum3,
          |  sum(x) over w5 as future_sum
          |FROM nums
          |WINDOW
          |  w1 AS (PARTITION BY y ORDER BY x RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
          |  w2 AS (PARTITION BY y ORDER BY x RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING),
          |  w3 AS (PARTITION BY y ORDER BY x RANGE BETWEEN 4 PRECEDING AND 2 PRECEDING ),
          |  w4 AS (PARTITION BY y ORDER BY x RANGE BETWEEN 2 FOLLOWING AND 4 FOLLOWING),
          |  w5 AS (PARTITION BY y ORDER BY x RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
        """.stripMargin
      )
      checkAnswer(actual, expected)
    }
  }

  test("SPARK-7595: Window will cause resolve failed with self join") {
    checkAnswer(sql(
      """
        |with
        | v0 as (select 0 as key, 1 as value),
        | v1 as (select key, count(value) over (partition by key) cnt_val from v0),
        | v2 as (select v1.key, v1_lag.cnt_val from v1 cross join v1 v1_lag
        |        where v1.key = v1_lag.key)
        | select key, cnt_val from v2 order by key limit 1
      """.stripMargin), Row(0, 1))
  }

  test("SPARK-16633: lead/lag should return the default value if the offset row does not exist") {
    checkAnswer(sql(
      """
        |SELECT
        |  lag(123, 100, 321) OVER (ORDER BY id) as lag,
        |  lead(123, 100, 321) OVER (ORDER BY id) as lead
        |FROM (SELECT 1 as id) tmp
      """.stripMargin),
      Row(321, 321))

    checkAnswer(sql(
      """
        |SELECT
        |  lag(123, 100, a) OVER (ORDER BY id) as lag,
        |  lead(123, 100, a) OVER (ORDER BY id) as lead
        |FROM (SELECT 1 as id, 2 as a) tmp
      """.stripMargin),
      Row(2, 2))
  }

  test("lead/lag should respect null values") {
    checkAnswer(sql(
      """
        |SELECT
        |  b,
        |  lag(a, 1, 321) OVER (ORDER BY b) as lag,
        |  lead(a, 1, 321) OVER (ORDER BY b) as lead
        |FROM (SELECT cast(null as int) as a, 1 as b
        |      UNION ALL
        |      select cast(null as int) as id, 2 as b) tmp
      """.stripMargin),
      Row(1, 321, null) :: Row(2, null, 321) :: Nil)

    checkAnswer(sql(
      """
        |SELECT
        |  b,
        |  lag(a, 1, c) OVER (ORDER BY b) as lag,
        |  lead(a, 1, c) OVER (ORDER BY b) as lead
        |FROM (SELECT cast(null as int) as a, 1 as b, 3 as c
        |      UNION ALL
        |      select cast(null as int) as id, 2 as b, 4 as c) tmp
      """.stripMargin),
      Row(1, 3, null) :: Row(2, null, 4) :: Nil)
  }

  test("test with low buffer spill threshold") {
    val nums = sparkContext.parallelize(1 to 10).map(x => (x, x % 2)).toDF("x", "y")
    nums.createOrReplaceTempView("nums")

    val expected =
      Row(1, 1, 1) ::
        Row(0, 2, 3) ::
        Row(1, 3, 6) ::
        Row(0, 4, 10) ::
        Row(1, 5, 15) ::
        Row(0, 6, 21) ::
        Row(1, 7, 28) ::
        Row(0, 8, 36) ::
        Row(1, 9, 45) ::
        Row(0, 10, 55) :: Nil

    val actual = sql(
      """
        |SELECT y, x, sum(x) OVER w1 AS running_sum
        |FROM nums
        |WINDOW w1 AS (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDiNG AND CURRENT RoW)
      """.stripMargin)

    withSQLConf(WINDOW_EXEC_BUFFER_IN_MEMORY_THRESHOLD.key -> "1",
      WINDOW_EXEC_BUFFER_SPILL_THRESHOLD.key -> "2") {
      assertSpilled(sparkContext, "test with low buffer spill threshold") {
        checkAnswer(actual, expected)
      }
    }

    spark.catalog.dropTempView("nums")
  }

  test("sql parameters in window frame clause") {
    val data = Seq(
      WindowData(1, "d", 10),
      WindowData(2, "a", 6),
      WindowData(3, "b", 7),
      WindowData(4, "b", 8),
      WindowData(5, "c", 9),
      WindowData(6, "c", 11)
    )
    val expected = Seq(
      Row(11),
      Row(12),
      Row(15),
      Row(6),
      Row(6),
      Row(9)
    )

    withTempView("windowData") {
      sparkContext.parallelize(data).toDF().createOrReplaceTempView("windowData")

      // Named parameters.
      val namedParamSql = """
        |SELECT
        |  SUM(month) OVER (ORDER BY month ROWS BETWEEN CURRENT ROW AND :param1 FOLLOWING)
        |FROM windowData
      """.stripMargin
      checkAnswer(spark.sql(namedParamSql, Map("param1" -> 2)), expected)

      // Positional parameters.
      val postParamSql = """
        |SELECT
        |  SUM(month) OVER (ORDER BY month ROWS BETWEEN CURRENT ROW AND ? FOLLOWING)
        |FROM windowData
      """.stripMargin
      checkAnswer(spark.sql(postParamSql, Array(2)), expected)

      // Wrong type of parameter.
      val e = intercept[AnalysisException] {
        spark.sql(namedParamSql, Map("param1" -> "abc")).collect()
      }
      assert(e.errorClass.contains("DATATYPE_MISMATCH.SPECIFIED_WINDOW_FRAME_UNACCEPTED_TYPE"))
    }
  }
}
