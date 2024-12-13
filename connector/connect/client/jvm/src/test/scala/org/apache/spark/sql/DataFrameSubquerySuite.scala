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
import org.apache.spark.sql.test.{QueryTest, RemoteSparkSession}

class DataFrameSubquerySuite extends QueryTest with RemoteSparkSession {
  import testImplicits._

  private def table1() = {
    sql("CREATE VIEW t1(c1, c2) AS VALUES (0, 1), (1, 2)")
    spark.table("t1")
  }

  private def table2() = {
    sql("CREATE VIEW t2(c1, c2) AS VALUES (0, 2), (0, 3)")
    spark.table("t2")
  }

  private def table3() = {
    sql(
      "CREATE VIEW t3(c1, c2) AS " +
        "VALUES (0, ARRAY(0, 1)), (1, ARRAY(2)), (2, ARRAY()), (null, ARRAY(4))")
    spark.table("t3")
  }

  test("lateral join with single column select") {
    withView("t1", "t2") {
      val t1 = table1()
      val t2 = table2()

      checkAnswer(
        t1.lateralJoin(spark.range(1).select($"c1".outer())).toDF("c1", "c2", "c3"),
        sql("SELECT * FROM t1, LATERAL (SELECT c1)").toDF("c1", "c2", "c3"))
      checkAnswer(
        t1.lateralJoin(t2.select($"c1")).toDF("c1", "c2", "c3"),
        sql("SELECT * FROM t1, LATERAL (SELECT c1 FROM t2)").toDF("c1", "c2", "c3"))
      checkAnswer(
        t1.lateralJoin(t2.select($"t1.c1".outer())).toDF("c1", "c2", "c3"),
        sql("SELECT * FROM t1, LATERAL (SELECT t1.c1 FROM t2)").toDF("c1", "c2", "c3"))
      checkAnswer(
        t1.lateralJoin(t2.select($"t1.c1".outer() + $"t2.c1")).toDF("c1", "c2", "c3"),
        sql("SELECT * FROM t1, LATERAL (SELECT t1.c1 + t2.c1 FROM t2)").toDF("c1", "c2", "c3"))
    }
  }

  test("lateral join with different join types") {
    withView("t1") {
      val t1 = table1()

      checkAnswer(
        t1.lateralJoin(
          spark.range(1).select(($"c1".outer() + $"c2".outer()).as("c3")),
          $"c2" === $"c3"),
        sql("SELECT * FROM t1 JOIN LATERAL (SELECT c1 + c2 AS c3) ON c2 = c3"))
      checkAnswer(
        t1.lateralJoin(
          spark.range(1).select(($"c1".outer() + $"c2".outer()).as("c3")),
          $"c2" === $"c3",
          "left"),
        sql("SELECT * FROM t1 LEFT JOIN LATERAL (SELECT c1 + c2 AS c3) ON c2 = c3"))
      checkAnswer(
        t1.lateralJoin(spark.range(1).select(($"c1".outer() + $"c2".outer()).as("c3")), "cross"),
        sql("SELECT * FROM t1 CROSS JOIN LATERAL (SELECT c1 + c2 AS c3)"))
    }
  }

  test("lateral join with correlated equality / non-equality predicates") {
    withView("t1", "t2") {
      val t1 = table1()
      val t2 = table2()

      checkAnswer(
        t1.lateralJoin(t2.where($"t1.c1".outer() === $"t2.c1").select($"c2"))
          .toDF("c1", "c2", "c3"),
        sql("SELECT * FROM t1, LATERAL (SELECT c2 FROM t2 WHERE t1.c1 = t2.c1)")
          .toDF("c1", "c2", "c3"))
      checkAnswer(
        t1.lateralJoin(t2.where($"t1.c1".outer() < $"t2.c1").select($"c2"))
          .toDF("c1", "c2", "c3"),
        sql("SELECT * FROM t1, LATERAL (SELECT c2 FROM t2 WHERE t1.c1 < t2.c1)")
          .toDF("c1", "c2", "c3"))
    }
  }

  test("lateral join with aggregation and correlated non-equality predicates") {
    withView("t1", "t2") {
      val t1 = table1()
      val t2 = table2()

      checkAnswer(
        t1.lateralJoin(t2.where($"t1.c2".outer() < $"t2.c2").select(max($"c2").as("m"))),
        sql("SELECT * FROM t1, LATERAL (SELECT max(c2) AS m FROM t2 WHERE t1.c2 < t2.c2)"))
    }
  }

  test("lateral join can reference preceding FROM clause items") {
    withView("t1", "t2") {
      val t1 = table1()
      val t2 = table2()

      checkAnswer(
        t1.join(t2)
          .lateralJoin(spark.range(1).select($"t1.c2".outer() + $"t2.c2".outer()))
          .toDF("c1", "c2", "c3", "c4", "c5"),
        sql("SELECT * FROM t1 JOIN t2 JOIN LATERAL (SELECT t1.c2 + t2.c2)")
          .toDF("c1", "c2", "c3", "c4", "c5"))
    }
  }

  test("multiple lateral joins") {
    withView("t1") {
      val t1 = table1()

      checkAnswer(
        t1.lateralJoin(spark.range(1).select(($"c1".outer() + $"c2".outer()).as("a")))
          .lateralJoin(spark.range(1).select(($"c1".outer() - $"c2".outer()).as("b")))
          .lateralJoin(spark.range(1).select(($"a".outer() * $"b".outer()).as("c"))),
        sql("""
            |SELECT * FROM t1,
            |LATERAL (SELECT c1 + c2 AS a),
            |LATERAL (SELECT c1 - c2 AS b),
            |LATERAL (SELECT a * b AS c)
            |""".stripMargin))
    }
  }

  test("lateral join in between regular joins") {
    withView("t1", "t2") {
      val t1 = table1()
      val t2 = table2()

      checkAnswer(
        t1.lateralJoin(t2.where($"t1.c1".outer() === $"t2.c1").select($"c2"), "left")
          .join(t1.as("t3"), $"t2.c2" === $"t3.c2", "left")
          .toDF("c1", "c2", "c3", "c4", "c5"),
        sql("""
            |SELECT * FROM t1
            |LEFT OUTER JOIN LATERAL (SELECT c2 FROM t2 WHERE t1.c1 = t2.c1) s
            |LEFT OUTER JOIN t1 t3 ON s.c2 = t3.c2
            |""".stripMargin)
          .toDF("c1", "c2", "c3", "c4", "c5"))
    }
  }

  test("nested lateral joins") {
    withView("t1", "t2") {
      val t1 = table1()
      val t2 = table2()

      checkAnswer(
        t1.lateralJoin(t2.lateralJoin(spark.range(1).select($"c1".outer())))
          .toDF("c1", "c2", "c3", "c4", "c5"),
        sql("SELECT * FROM t1, LATERAL (SELECT * FROM t2, LATERAL (SELECT c1))")
          .toDF("c1", "c2", "c3", "c4", "c5"))
      checkAnswer(
        t1.lateralJoin(
          spark
            .range(1)
            .select(($"c1".outer() + lit(1)).as("c1"))
            .lateralJoin(spark.range(1).select($"c1".outer())))
          .toDF("c1", "c2", "c3", "c4"),
        sql(
          "SELECT * FROM t1, LATERAL (SELECT * FROM (SELECT c1 + 1 AS c1), LATERAL (SELECT c1))")
          .toDF("c1", "c2", "c3", "c4"))
    }
  }

  test("lateral join with table-valued functions") {
    withView("t1", "t3") {
      val t1 = table1()
      val t3 = table3()

      checkAnswer(t1.lateralJoin(spark.tvf.range(3)), sql("SELECT * FROM t1, LATERAL RANGE(3)"))
      checkAnswer(
        t1.lateralJoin(spark.tvf.explode(array($"c1".outer(), $"c2".outer()))),
        sql("SELECT * FROM t1, LATERAL EXPLODE(ARRAY(c1, c2)) t2(c3)"))
      checkAnswer(
        t3.lateralJoin(spark.tvf.explode_outer($"c2".outer())),
        sql("SELECT * FROM t3, LATERAL EXPLODE_OUTER(c2) t2(v)"))
      checkAnswer(
        spark.tvf
          .explode(array(lit(1), lit(2)))
          .toDF("v")
          .lateralJoin(spark.range(1).select($"v".outer() + 1)),
        sql("SELECT * FROM EXPLODE(ARRAY(1, 2)) t(v), LATERAL (SELECT v + 1)"))
    }
  }

  test("lateral join with table-valued functions and join conditions") {
    withView("t1", "t3") {
      val t1 = table1()
      val t3 = table3()

      checkAnswer(
        t1.lateralJoin(spark.tvf.explode(array($"c1".outer(), $"c2".outer())), $"c1" === $"col"),
        sql("SELECT * FROM t1 JOIN LATERAL EXPLODE(ARRAY(c1, c2)) t(c3) ON t1.c1 = c3"))
      checkAnswer(
        t3.lateralJoin(spark.tvf.explode($"c2".outer()), $"c1" === $"col"),
        sql("SELECT * FROM t3 JOIN LATERAL EXPLODE(c2) t(c3) ON t3.c1 = c3"))
      checkAnswer(
        t3.lateralJoin(spark.tvf.explode($"c2".outer()), $"c1" === $"col", "left"),
        sql("SELECT * FROM t3 LEFT JOIN LATERAL EXPLODE(c2) t(c3) ON t3.c1 = c3"))
    }
  }
}
