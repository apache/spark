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
import org.apache.spark.sql.test.SharedSparkSession

class DataFrameSubquerySuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  setupTestData()

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

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    l.createOrReplaceTempView("l")
    r.createOrReplaceTempView("r")
  }

  test("unanalyzable expression") {
    val exception = intercept[AnalysisException] {
      spark.range(1).select($"id" === $"id".outer()).schema
    }
    checkError(
      exception,
      condition = "UNANALYZABLE_EXPRESSION",
      parameters = Map("expr" -> "\"outer(id)\""),
      queryContext =
        Array(ExpectedContext(fragment = "outer", callSitePattern = getCurrentClassCallSitePattern))
    )
  }

  test("simple uncorrelated scalar subquery") {
    checkAnswer(
      spark.range(1).select(
        spark.range(1).select(lit(1)).scalar().as("b")
      ),
      sql("select (select 1 as b) as b")
    )

    checkAnswer(
      spark.range(1).select(
        spark.range(1).select(spark.range(1).select(lit(1)).scalar() + 1).scalar() + lit(1)
      ),
      sql("select (select (select 1) + 1) + 1")
    )

    // string type
    checkAnswer(
      spark.range(1).select(
        spark.range(1).select(lit("s")).scalar().as("b")
      ),
      sql("select (select 's' as s) as b")
    )
  }

  test("uncorrelated scalar subquery should return null if there is 0 rows") {
    checkAnswer(
      spark.range(1).select(
        spark.range(1).select(lit("s")).limit(0).scalar().as("b")
      ),
      sql("select (select 's' as s limit 0) as b")
    )
  }

  test("uncorrelated scalar subquery on a DataFrame generated query") {
    withTempView("subqueryData") {
      val df = Seq((1, "one"), (2, "two"), (3, "three")).toDF("key", "value")
      df.createOrReplaceTempView("subqueryData")

      checkAnswer(
        spark.range(1).select(
          spark.table("subqueryData")
            .select($"key").where($"key" > 2).orderBy($"key").limit(1).scalar() + lit(1)
        ),
        sql("select (select key from subqueryData where key > 2 order by key limit 1) + 1")
      )

      checkAnswer(
        spark.range(1).select(
          -spark.table("subqueryData").select(max($"key")).scalar()
        ),
        sql("select -(select max(key) from subqueryData)")
      )

      checkAnswer(
        spark.range(1).select(
          spark.table("subqueryData").select($"value").limit(0).scalar()
        ),
        sql("select (select value from subqueryData limit 0)")
      )

      checkAnswer(
        spark.range(1).select(
          spark.table("subqueryData")
            .where(
              $"key" === spark.table("subqueryData").select(max($"key")).scalar() - lit(1)
            ).select(
              min($"value")
            ).scalar()
        ),
        sql("select (select min(value) from subqueryData" +
          " where key = (select max(key) from subqueryData) - 1)")
      )
    }
  }

  test("SPARK-15677: Queries against local relations with scalar subquery in Select list") {
    withTempView("t1", "t2") {
      Seq((1, 1), (2, 2)).toDF("c1", "c2").createOrReplaceTempView("t1")
      Seq((1, 1), (2, 2)).toDF("c1", "c2").createOrReplaceTempView("t2")

      checkAnswer(
        spark.table("t1").select(
          spark.range(1).select(lit(1).as("col")).scalar()
        ),
        sql("SELECT (select 1 as col) from t1")
      )

      checkAnswer(
        spark.table("t1").select(
          spark.table("t2").select(max($"c1")).scalar()
        ),
        sql("SELECT (select max(c1) from t2) from t1")
      )

      checkAnswer(
        spark.table("t1").select(
          lit(1) + spark.range(1).select(lit(1).as("col")).scalar()
        ),
        sql("SELECT 1 + (select 1 as col) from t1")
      )

      checkAnswer(
        spark.table("t1").select(
          $"c1",
          spark.table("t2").select(max($"c1")).scalar() + $"c2"
        ),
        sql("SELECT c1, (select max(c1) from t2) + c2 from t1")
      )

      checkAnswer(
        spark.table("t1").select(
          $"c1",
          spark.table("t2").where($"t1.c2".outer() === $"t2.c2").select(max($"c1")).scalar()
        ),
        sql("SELECT c1, (select max(c1) from t2 where t1.c2 = t2.c2) from t1")
      )
    }
  }

  test("EXISTS predicate subquery") {
    checkAnswer(
      spark.table("l").where(
        spark.table("r").where($"a".outer() === $"c").exists()
      ),
      sql("select * from l where exists (select * from r where l.a = r.c)")
    )

    checkAnswer(
      spark.table("l").where(
        spark.table("r").where($"a".outer() === $"c").exists() && $"a" <= lit(2)
      ),
      sql("select * from l where exists (select * from r where l.a = r.c) and l.a <= 2")
    )
  }

  test("NOT EXISTS predicate subquery") {
    checkAnswer(
      spark.table("l").where(
        !spark.table("r").where($"a".outer() === $"c").exists()
      ),
      sql("select * from l where not exists (select * from r where l.a = r.c)")
    )

    checkAnswer(
      spark.table("l").where(
        !spark.table("r").where($"a".outer() === $"c" && $"b".outer() < $"d").exists()
      ),
      sql("select * from l where not exists (select * from r where l.a = r.c and l.b < r.d)")
    )
  }

  test("EXISTS predicate subquery within OR") {
    checkAnswer(
      spark.table("l").where(
        spark.table("r").where($"a".outer() === $"c").exists() ||
        spark.table("r").where($"a".outer() === $"c").exists()
      ),
      sql("select * from l where exists (select * from r where l.a = r.c)" +
        " or exists (select * from r where l.a = r.c)")
    )

    checkAnswer(
      spark.table("l").where(
        !spark.table("r").where($"a".outer() === $"c" && $"b".outer() < $"d").exists() ||
        !spark.table("r").where($"a".outer() === $"c").exists()
      ),
      sql("select * from l where not exists (select * from r where l.a = r.c and l.b < r.d)" +
        " or not exists (select * from r where l.a = r.c)")
    )
  }

  test("correlated scalar subquery in where") {
    checkAnswer(
      spark.table("l").where(
        $"b" < spark.table("r").where($"a".outer() === $"c").select(max($"d")).scalar()
      ),
      sql("select * from l where b < (select max(d) from r where a = c)")
    )
  }

  test("correlated scalar subquery in select") {
    checkAnswer(
      spark.table("l").select(
        $"a",
        spark.table("l").where($"a" === $"a".outer()).select(sum($"b")).scalar().as("sum_b")
      ),
      sql("select a, (select sum(b) from l l2 where l2.a = l1.a) sum_b from l l1")
    )
  }

  test("correlated scalar subquery in select (null safe)") {
    checkAnswer(
      spark.table("l").select(
        $"a",
        spark.table("l").where($"a" <=> $"a".outer()).select(sum($"b")).scalar().as("sum_b")
      ),
      sql("select a, (select sum(b) from l l2 where l2.a <=> l1.a) sum_b from l l1")
    )
  }

  test("correlated scalar subquery in aggregate") {
    checkAnswer(
      spark.table("l").groupBy(
        $"a",
        spark.table("r").where($"a".outer() === $"c").select(sum($"d")).scalar().as("sum_d")
      ).agg(Map.empty[String, String]),
      sql("select a, (select sum(d) from r where a = c) sum_d from l l1 group by 1, 2")
    )
  }

  test("SPARK-34269: correlated subquery with view in aggregate's grouping expression") {
    withTable("tr") {
      withView("vr") {
        r.write.saveAsTable("tr")
        sql("create view vr as select * from tr")
        checkAnswer(
          spark.table("l").groupBy(
            $"a",
            spark.table("vr").where($"a".outer() === $"c").select(sum($"d")).scalar().as("sum_d")
          ).agg(Map.empty[String, String]),
          sql("select a, (select sum(d) from vr where a = c) sum_d from l l1 group by 1, 2")
        )
      }
    }
  }

  test("non-aggregated correlated scalar subquery") {
    val exception1 = intercept[SparkRuntimeException] {
      spark.table("l").select(
        $"a",
        spark.table("l").where($"a" === $"a".outer()).select($"b").scalar().as("sum_b")
      ).collect()
    }
    checkError(
      exception1,
      condition = "SCALAR_SUBQUERY_TOO_MANY_ROWS"
    )
  }

  test("non-equal correlated scalar subquery") {
    checkAnswer(
      spark.table("l").select(
        $"a",
        spark.table("l").where($"a" < $"a".outer()).select(sum($"b")).scalar().as("sum_b")
      ),
      sql("select a, (select sum(b) from l l2 where l2.a < l1.a) sum_b from l l1")
    )
  }

  test("disjunctive correlated scalar subquery") {
    checkAnswer(
      spark.table("l").where(
        spark.table("r").where(
          ($"a".outer() === $"c" && $"d" === 2.0) ||
            ($"a".outer() === $"c" && $"d" === 1.0)
        ).select(count(lit(1))).scalar() > 0
      ).select($"a"),
      sql("""
            |select a
            |from   l
            |where  (select count(*)
            |        from   r
            |        where (a = c and d = 2.0) or (a = c and d = 1.0)) > 0
        """.stripMargin)
    )
  }

  test("correlated scalar subquery with outer reference errors") {
    // Missing `outer()`
    val exception1 = intercept[AnalysisException] {
      spark.table("l").select(
        $"a",
        spark.table("r").where($"c" === $"a").select(sum($"d")).scalar()
      ).collect()
    }
    checkError(
      exception1,
      condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      parameters = Map("objectName" -> "`a`", "proposal" -> "`c`, `d`"),
      queryContext =
        Array(ExpectedContext(fragment = "$", callSitePattern = getCurrentClassCallSitePattern))
    )

    // Extra `outer()`
    val exception2 = intercept[AnalysisException] {
      spark.table("l").select(
        $"a",
        spark.table("r").where($"c".outer() === $"a".outer()).select(sum($"d")).scalar()
      ).collect()
    }
    checkError(
      exception2,
      condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      parameters = Map("objectName" -> "`c`", "proposal" -> "`a`, `b`"),
      queryContext =
        Array(ExpectedContext(fragment = "outer", callSitePattern = getCurrentClassCallSitePattern))
    )

    // Missing `outer()` for another outer
    val exception3 = intercept[AnalysisException] {
      spark.table("l").select(
        $"a",
        spark.table("r").where($"b" === $"a".outer()).select(sum($"d")).scalar()
      ).collect()
    }
    checkError(
      exception3,
      condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      parameters = Map("objectName" -> "`b`", "proposal" -> "`c`, `d`"),
      queryContext =
        Array(ExpectedContext(fragment = "$", callSitePattern = getCurrentClassCallSitePattern))
    )
  }
}
