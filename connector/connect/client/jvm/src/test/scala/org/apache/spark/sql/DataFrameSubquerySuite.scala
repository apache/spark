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

import org.apache.spark.SparkRuntimeException
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.{QueryTest, RemoteSparkSession}

class DataFrameSubquerySuite extends QueryTest with RemoteSparkSession {
  import testImplicits._

  val row = identity[(java.lang.Integer, java.lang.Double)](_)

  lazy val l = Seq(
    row((1, 2.0)),
    row((1, 2.0)),
    row((2, 1.0)),
    row((2, 1.0)),
    row((3, 3.0)),
    row((null, null)),
    row((null, 5.0)),
    row((6, null))).toDF("a", "b")

  lazy val r = Seq(
    row((2, 3.0)),
    row((2, 3.0)),
    row((3, 2.0)),
    row((4, 1.0)),
    row((null, null)),
    row((null, 5.0)),
    row((6, null))).toDF("c", "d")

  override def beforeAll(): Unit = {
    super.beforeAll()
    l.createOrReplaceTempView("l")
    r.createOrReplaceTempView("r")
  }

  test("noop outer()") {
    checkAnswer(spark.range(1).select($"id".outer()), Row(0))
    checkError(
      intercept[AnalysisException](spark.range(1).select($"outer_col".outer()).collect()),
      "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      parameters = Map("objectName" -> "`outer_col`", "proposal" -> "`id`"),
      context = ExpectedContext(fragment = "$", callSitePattern = getCurrentClassCallSitePattern))
  }

  test("simple uncorrelated scalar subquery") {
    checkAnswer(
      spark.range(1).select(spark.range(1).select(lit(1)).scalar().as("b")),
      sql("select (select 1 as b) as b"))

    checkAnswer(
      spark
        .range(1)
        .select(
          spark.range(1).select(spark.range(1).select(lit(1)).scalar() + 1).scalar() + lit(1)),
      sql("select (select (select 1) + 1) + 1"))

    // string type
    checkAnswer(
      spark.range(1).select(spark.range(1).select(lit("s")).scalar().as("b")),
      sql("select (select 's' as s) as b"))
  }

  test("uncorrelated scalar subquery should return null if there is 0 rows") {
    checkAnswer(
      spark.range(1).select(spark.range(1).select(lit("s")).limit(0).scalar().as("b")),
      sql("select (select 's' as s limit 0) as b"))
  }

  test("uncorrelated scalar subquery on a DataFrame generated query") {
    withTempView("subqueryData") {
      val df = Seq((1, "one"), (2, "two"), (3, "three")).toDF("key", "value")
      df.createOrReplaceTempView("subqueryData")

      checkAnswer(
        spark
          .range(1)
          .select(
            spark
              .table("subqueryData")
              .select($"key")
              .where($"key" > 2)
              .orderBy($"key")
              .limit(1)
              .scalar() + lit(1)),
        sql("select (select key from subqueryData where key > 2 order by key limit 1) + 1"))

      checkAnswer(
        spark.range(1).select(-spark.table("subqueryData").select(max($"key")).scalar()),
        sql("select -(select max(key) from subqueryData)"))

      checkAnswer(
        spark.range(1).select(spark.table("subqueryData").select($"value").limit(0).scalar()),
        sql("select (select value from subqueryData limit 0)"))

      checkAnswer(
        spark
          .range(1)
          .select(
            spark
              .table("subqueryData")
              .where($"key" === spark.table("subqueryData").select(max($"key")).scalar() - lit(1))
              .select(min($"value"))
              .scalar()),
        sql(
          "select (select min(value) from subqueryData" +
            " where key = (select max(key) from subqueryData) - 1)"))
    }
  }

  test("correlated scalar subquery in SELECT with outer() function") {
    val df1 = spark.table("l").as("t1")
    val df2 = spark.table("l").as("t2")
    // We can use the `.outer()` function to wrap either the outer column, or the entire condition,
    // or the SQL string of the condition.
    Seq($"t1.a" === $"t2.a".outer(), ($"t1.a" === $"t2.a").outer(), expr("t1.a = t2.a").outer())
      .foreach { cond =>
        checkAnswer(
          df1.select($"a", df2.where(cond).select(sum($"b")).scalar().as("sum_b")),
          sql("select a, (select sum(b) from l t1 where t1.a = t2.a) sum_b from l t2"))
      }
  }

  test("correlated scalar subquery in WHERE with outer() function") {
    // We can use the `.outer()` function to wrap either the outer column, or the entire condition,
    // or the SQL string of the condition.
    Seq($"a".outer() === $"c", ($"a" === $"c").outer(), expr("a = c").outer()).foreach { cond =>
      checkAnswer(
        spark.table("l").where($"b" < spark.table("r").where(cond).select(max($"d")).scalar()),
        sql("select * from l where b < (select max(d) from r where a = c)"))
    }
  }

  test("EXISTS predicate subquery with outer() function") {
    // We can use the `.outer()` function to wrap either the outer column, or the entire condition,
    // or the SQL string of the condition.
    Seq($"a".outer() === $"c", ($"a" === $"c").outer(), expr("a = c").outer()).foreach { cond =>
      checkAnswer(
        spark.table("l").where(spark.table("r").where(cond).exists()),
        sql("select * from l where exists (select * from r where l.a = r.c)"))

      checkAnswer(
        spark.table("l").where(spark.table("r").where(cond).exists() && $"a" <= lit(2)),
        sql("select * from l where exists (select * from r where l.a = r.c) and l.a <= 2"))
    }
  }

  test("SPARK-15677: Queries against local relations with scalar subquery in Select list") {
    withTempView("t1", "t2") {
      Seq((1, 1), (2, 2)).toDF("c1", "c2").createOrReplaceTempView("t1")
      Seq((1, 1), (2, 2)).toDF("c1", "c2").createOrReplaceTempView("t2")

      checkAnswer(
        spark.table("t1").select(spark.range(1).select(lit(1).as("col")).scalar()),
        sql("SELECT (select 1 as col) from t1"))

      checkAnswer(
        spark.table("t1").select(spark.table("t2").select(max($"c1")).scalar()),
        sql("SELECT (select max(c1) from t2) from t1"))

      checkAnswer(
        spark.table("t1").select(lit(1) + spark.range(1).select(lit(1).as("col")).scalar()),
        sql("SELECT 1 + (select 1 as col) from t1"))

      checkAnswer(
        spark.table("t1").select($"c1", spark.table("t2").select(max($"c1")).scalar() + $"c2"),
        sql("SELECT c1, (select max(c1) from t2) + c2 from t1"))

      checkAnswer(
        spark
          .table("t1")
          .select(
            $"c1",
            spark.table("t2").where($"t1.c2".outer() === $"t2.c2").select(max($"c1")).scalar()),
        sql("SELECT c1, (select max(c1) from t2 where t1.c2 = t2.c2) from t1"))
    }
  }

  test("NOT EXISTS predicate subquery") {
    checkAnswer(
      spark.table("l").where(!spark.table("r").where($"a".outer() === $"c").exists()),
      sql("select * from l where not exists (select * from r where l.a = r.c)"))

    checkAnswer(
      spark
        .table("l")
        .where(!spark.table("r").where($"a".outer() === $"c" && $"b".outer() < $"d").exists()),
      sql("select * from l where not exists (select * from r where l.a = r.c and l.b < r.d)"))
  }

  test("EXISTS predicate subquery within OR") {
    checkAnswer(
      spark
        .table("l")
        .where(spark.table("r").where($"a".outer() === $"c").exists() ||
          spark.table("r").where($"a".outer() === $"c").exists()),
      sql(
        "select * from l where exists (select * from r where l.a = r.c)" +
          " or exists (select * from r where l.a = r.c)"))

    checkAnswer(
      spark
        .table("l")
        .where(!spark.table("r").where($"a".outer() === $"c" && $"b".outer() < $"d").exists() ||
          !spark.table("r").where($"a".outer() === $"c").exists()),
      sql(
        "select * from l where not exists (select * from r where l.a = r.c and l.b < r.d)" +
          " or not exists (select * from r where l.a = r.c)"))
  }

  test("correlated scalar subquery in select (null safe equal)") {
    val df1 = spark.table("l").as("t1")
    val df2 = spark.table("l").as("t2")
    checkAnswer(
      df1.select(
        $"a",
        df2.where($"t2.a" <=> $"t1.a".outer()).select(sum($"b")).scalar().as("sum_b")),
      sql("select a, (select sum(b) from l t2 where t2.a <=> t1.a) sum_b from l t1"))
  }

  test("correlated scalar subquery in aggregate") {
    checkAnswer(
      spark
        .table("l")
        .groupBy(
          $"a",
          spark.table("r").where($"a".outer() === $"c").select(sum($"d")).scalar().as("sum_d"))
        .agg(Map.empty[String, String]),
      sql("select a, (select sum(d) from r where a = c) sum_d from l l1 group by 1, 2"))
  }

  test("SPARK-34269: correlated subquery with view in aggregate's grouping expression") {
    withTable("tr") {
      withView("vr") {
        r.write.saveAsTable("tr")
        sql("create view vr as select * from tr")
        checkAnswer(
          spark
            .table("l")
            .groupBy(
              $"a",
              spark
                .table("vr")
                .where($"a".outer() === $"c")
                .select(sum($"d"))
                .scalar()
                .as("sum_d"))
            .agg(Map.empty[String, String]),
          sql("select a, (select sum(d) from vr where a = c) sum_d from l l1 group by 1, 2"))
      }
    }
  }

  test("non-aggregated correlated scalar subquery") {
    val df1 = spark.table("l").as("t1")
    val df2 = spark.table("l").as("t2")
    val exception1 = intercept[SparkRuntimeException] {
      df1
        .select($"a", df2.where($"t1.a" === $"t2.a".outer()).select($"b").scalar().as("sum_b"))
        .collect()
    }
    checkError(exception1, condition = "SCALAR_SUBQUERY_TOO_MANY_ROWS")
  }

  test("non-equal correlated scalar subquery") {
    val df1 = spark.table("l").as("t1")
    val df2 = spark.table("l").as("t2")
    checkAnswer(
      df1.select(
        $"a",
        df2.where($"t2.a" < $"t1.a".outer()).select(sum($"b")).scalar().as("sum_b")),
      sql("select a, (select sum(b) from l t2 where t2.a < t1.a) sum_b from l t1"))
  }

  test("disjunctive correlated scalar subquery") {
    checkAnswer(
      spark
        .table("l")
        .where(
          spark
            .table("r")
            .where(($"a".outer() === $"c" && $"d" === 2.0) ||
              ($"a".outer() === $"c" && $"d" === 1.0))
            .select(count(lit(1)))
            .scalar() > 0)
        .select($"a"),
      sql("""
            |select a
            |from   l
            |where  (select count(*)
            |        from   r
            |        where (a = c and d = 2.0) or (a = c and d = 1.0)) > 0
        """.stripMargin))
  }

  test("correlated scalar subquery with missing outer reference") {
    checkAnswer(
      spark
        .table("l")
        .select($"a", spark.table("r").where($"c" === $"a").select(sum($"d")).scalar()),
      sql("select a, (select sum(d) from r where c = a) from l"))
  }

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

  test("lateral join with star expansion") {
    withView("t1", "t2") {
      val t1 = table1()
      val t2 = table2()

      checkAnswer(
        t1.lateralJoin(spark.range(1).select().select($"*")),
        sql("SELECT * FROM t1, LATERAL (SELECT *)"))
      checkAnswer(
        t1.lateralJoin(t2.select($"*")).toDF("c1", "c2", "c3", "c4"),
        sql("SELECT * FROM t1, LATERAL (SELECT * FROM t2)").toDF("c1", "c2", "c3", "c4"))
      checkAnswer(
        t1.lateralJoin(t2.select($"t1.*".outer(), $"t2.*"))
          .toDF("c1", "c2", "c3", "c4", "c5", "c6"),
        sql("SELECT * FROM t1, LATERAL (SELECT t1.*, t2.* FROM t2)")
          .toDF("c1", "c2", "c3", "c4", "c5", "c6"))
      checkAnswer(
        t1.lateralJoin(t2.alias("t1").select($"t1.*")).toDF("c1", "c2", "c3", "c4"),
        sql("SELECT * FROM t1, LATERAL (SELECT t1.* FROM t2 AS t1)").toDF("c1", "c2", "c3", "c4"))
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

  test("lateral join with subquery alias") {
    withView("t1") {
      val t1 = table1()

      checkAnswer(
        t1.lateralJoin(spark.range(1).select($"c1".outer(), $"c2".outer()).toDF("a", "b").as("s"))
          .select("a", "b"),
        sql("SELECT a, b FROM t1, LATERAL (SELECT c1, c2) s(a, b)"))
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
        t1.lateralJoin(t2.where($"t1.c1".outer() === $"t2.c1").select($"c2").as("s"), "left")
          .join(t1.as("t3"), $"s.c2" === $"t3.c2", "left")
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

  test("scalar subquery inside lateral join") {
    withView("t1", "t2") {
      val t1 = table1()
      val t2 = table2()

      // uncorrelated
      checkAnswer(
        t1.lateralJoin(spark.range(1).select($"c2".outer(), t2.select(min($"c2")).scalar()))
          .toDF("c1", "c2", "c3", "c4"),
        sql("SELECT * FROM t1, LATERAL (SELECT c2, (SELECT MIN(c2) FROM t2))")
          .toDF("c1", "c2", "c3", "c4"))

      // correlated
      checkAnswer(
        t1.lateralJoin(
          spark
            .range(1)
            .select($"c1".outer().as("a"))
            .select(t2.where($"c1" === $"a".outer()).select(sum($"c2")).scalar())),
        sql("""
              |SELECT * FROM t1, LATERAL (
              |    SELECT (SELECT SUM(c2) FROM t2 WHERE c1 = a) FROM (SELECT c1 AS a)
              |)
              |""".stripMargin))
    }
  }

  test("lateral join inside subquery") {
    withView("t1", "t2") {
      val t1 = table1()
      val t2 = table2()

      // uncorrelated
      checkAnswer(
        t1.where(
          $"c1" === t2
            .lateralJoin(spark.range(1).select($"c1".outer().as("a")))
            .select(min($"a"))
            .scalar()),
        sql("SELECT * FROM t1 WHERE c1 = (SELECT MIN(a) FROM t2, LATERAL (SELECT c1 AS a))"))
      // correlated
      checkAnswer(
        t1.where(
          $"c1" === t2
            .lateralJoin(spark.range(1).select($"c1".outer().as("a")))
            .where($"c1" === $"t1.c1".outer())
            .select(min($"a"))
            .scalar()),
        sql(
          "SELECT * FROM t1 " +
            "WHERE c1 = (SELECT MIN(a) FROM t2, LATERAL (SELECT c1 AS a) WHERE c1 = t1.c1)"))
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

  test("subquery with generator / table-valued functions") {
    withView("t1") {
      val t1 = table1()

      checkAnswer(
        spark.range(1).select(explode(t1.select(collect_list("c2")).scalar())),
        sql("SELECT EXPLODE((SELECT COLLECT_LIST(c2) FROM t1))"))
      checkAnswer(
        spark.tvf.explode(t1.select(collect_list("c2")).scalar()),
        sql("SELECT * FROM EXPLODE((SELECT COLLECT_LIST(c2) FROM t1))"))
    }
  }

  test("subquery in join condition") {
    withView("t1", "t2") {
      val t1 = table1()
      val t2 = table2()

      checkAnswer(
        t1.join(t2, $"t1.c1" === t1.select(max("c1")).scalar()).toDF("c1", "c2", "c3", "c4"),
        sql("SELECT * FROM t1 JOIN t2 ON t1.c1 = (SELECT MAX(c1) FROM t1)")
          .toDF("c1", "c2", "c3", "c4"))
    }
  }

  test("subquery in unpivot") {
    withView("t1", "t2") {
      val t1 = table1()
      val t2 = table2()

      checkError(
        intercept[AnalysisException] {
          t1.unpivot(Array(t2.exists()), "c1", "c2").collect()
        },
        "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.UNSUPPORTED_IN_EXISTS_SUBQUERY",
        parameters = Map("treeNode" -> "(?s)'Unpivot.*"),
        matchPVals = true,
        queryContext = Array(
          ExpectedContext(fragment = "exists", callSitePattern = getCurrentClassCallSitePattern)))
      checkError(
        intercept[AnalysisException] {
          t1.unpivot(Array($"c1"), Array(t2.exists()), "c1", "c2").collect()
        },
        "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.UNSUPPORTED_IN_EXISTS_SUBQUERY",
        parameters = Map("treeNode" -> "(?s)Expand.*"),
        matchPVals = true,
        queryContext = Array(
          ExpectedContext(fragment = "exists", callSitePattern = getCurrentClassCallSitePattern)))
    }
  }

  test("subquery in transpose") {
    withView("t1") {
      val t1 = table1()

      checkError(
        intercept[AnalysisException] {
          t1.transpose(t1.select(max("c1")).scalar()).collect()
        },
        "TRANSPOSE_INVALID_INDEX_COLUMN",
        parameters = Map("reason" -> "Index column must be an atomic attribute"))
    }
  }

  test("subquery in withColumns") {
    withView("t1") {
      val t1 = table1()

      checkAnswer(
        t1.withColumn(
          "scalar",
          spark
            .range(1)
            .select($"c1".outer() + $"c2".outer())
            .scalar()),
        t1.select($"*", ($"c1" + $"c2").as("scalar")))

      checkAnswer(
        t1.withColumn(
          "scalar",
          spark
            .range(1)
            .withColumn("c1", $"c1".outer())
            .select($"c1" + $"c2".outer())
            .scalar()),
        t1.select($"*", ($"c1" + $"c2").as("scalar")))

      checkAnswer(
        t1.withColumn(
          "scalar",
          spark
            .range(1)
            .select($"c1".outer().as("c1"))
            .withColumn("c2", $"c2".outer())
            .select($"c1" + $"c2")
            .scalar()),
        t1.select($"*", ($"c1" + $"c2").as("scalar")))
    }
  }

  test("subquery in withColumnsRenamed") {
    withView("t1") {
      val t1 = table1()

      checkAnswer(
        t1.withColumn(
          "scalar",
          spark
            .range(1)
            .select($"c1".outer().as("c1"), $"c2".outer().as("c2"))
            .withColumnsRenamed(Map("c1" -> "x", "c2" -> "y"))
            .select($"x" + $"y")
            .scalar()),
        t1.select($"*", ($"c1".as("x") + $"c2".as("y")).as("scalar")))
    }
  }

  test("subquery in drop") {
    withView("t1") {
      val t1 = table1()

      checkAnswer(t1.drop(spark.range(1).select(lit("c1")).scalar()), t1)
    }
  }

  test("subquery in repartition") {
    withView("t1") {
      val t1 = table1()

      checkAnswer(t1.repartition(spark.range(1).select(lit(1)).scalar()), t1)
    }
  }
}
