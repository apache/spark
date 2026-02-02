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

import org.apache.spark.TestUtils.assertSpilled
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.internal.SQLConf.{WINDOW_EXEC_BUFFER_IN_MEMORY_THRESHOLD, WINDOW_EXEC_BUFFER_SPILL_THRESHOLD}
import org.apache.spark.sql.test.SharedSparkSession

case class WindowData(month: Int, area: String, product: Int)


/**
 * Test suite for SQL window functions.
 */
class SQLWindowFunctionSuite extends QueryTest with SharedSparkSession {

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
            |select month, area, product, sum(distinct product + 1) over (partition by 1 order by 2)
            |from windowData
          """.stripMargin)
      }
      assert(e.getMessage.contains("Distinct window functions are not supported"))
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
