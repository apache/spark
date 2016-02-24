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

package org.apache.spark.sql.hive

import scala.util.control.NonFatal

import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SQLTestUtils

class LogicalPlanToSQLSuite extends SQLBuilderTest with SQLTestUtils {
  import testImplicits._

  protected override def beforeAll(): Unit = {
    sql("DROP TABLE IF EXISTS parquet_t0")
    sql("DROP TABLE IF EXISTS parquet_t1")
    sql("DROP TABLE IF EXISTS parquet_t2")
    sql("DROP TABLE IF EXISTS t0")

    sqlContext.range(10).write.saveAsTable("parquet_t0")
    sql("CREATE TABLE t0 AS SELECT * FROM parquet_t0")

    sqlContext
      .range(10)
      .select('id as 'key, concat(lit("val_"), 'id) as 'value)
      .write
      .saveAsTable("parquet_t1")

    sqlContext
      .range(10)
      .select('id as 'a, 'id as 'b, 'id as 'c, 'id as 'd)
      .write
      .saveAsTable("parquet_t2")
  }

  override protected def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS parquet_t0")
    sql("DROP TABLE IF EXISTS parquet_t1")
    sql("DROP TABLE IF EXISTS parquet_t2")
    sql("DROP TABLE IF EXISTS t0")
  }

  private def checkHiveQl(hiveQl: String): Unit = {
    val df = sql(hiveQl)

    val convertedSQL = try new SQLBuilder(df).toSQL catch {
      case NonFatal(e) =>
        fail(
          s"""Cannot convert the following HiveQL query plan back to SQL query string:
             |
             |# Original HiveQL query string:
             |$hiveQl
             |
             |# Resolved query plan:
             |${df.queryExecution.analyzed.treeString}
           """.stripMargin)
    }

    try {
      checkAnswer(sql(convertedSQL), df)
    } catch { case cause: Throwable =>
      fail(
        s"""Failed to execute converted SQL string or got wrong answer:
           |
           |# Converted SQL query string:
           |$convertedSQL
           |
           |# Original HiveQL query string:
           |$hiveQl
           |
           |# Resolved query plan:
           |${df.queryExecution.analyzed.treeString}
         """.stripMargin,
        cause)
    }
  }

  test("in") {
    checkHiveQl("SELECT id FROM parquet_t0 WHERE id IN (1, 2, 3)")
  }

  test("not in") {
    checkHiveQl("SELECT id FROM t0 WHERE id NOT IN (1, 2, 3)")
  }

  test("not like") {
    checkHiveQl("SELECT id FROM t0 WHERE id + 5 NOT LIKE '1%'")
  }

  test("aggregate function in having clause") {
    checkHiveQl("SELECT COUNT(value) FROM parquet_t1 GROUP BY key HAVING MAX(key) > 0")
  }

  test("aggregate function in order by clause") {
    checkHiveQl("SELECT COUNT(value) FROM parquet_t1 GROUP BY key ORDER BY MAX(key)")
  }

  // When there are multiple aggregate functions in ORDER BY clause, all of them are extracted into
  // Aggregate operator and aliased to the same name "aggOrder".  This is OK for normal query
  // execution since these aliases have different expression ID.  But this introduces name collision
  // when converting resolved plans back to SQL query strings as expression IDs are stripped.
  test("aggregate function in order by clause with multiple order keys") {
    checkHiveQl("SELECT COUNT(value) FROM parquet_t1 GROUP BY key ORDER BY key, MAX(key)")
  }

  test("type widening in union") {
    checkHiveQl("SELECT id FROM parquet_t0 UNION ALL SELECT CAST(id AS INT) AS id FROM parquet_t0")
  }

  test("union distinct") {
    checkHiveQl("SELECT * FROM t0 UNION SELECT * FROM t0")
  }

  // Parser is unable to parse the following query:
  // SELECT  `u_1`.`id`
  // FROM (((SELECT  `t0`.`id` FROM `default`.`t0`)
  // UNION ALL (SELECT  `t0`.`id` FROM `default`.`t0`))
  // UNION ALL (SELECT  `t0`.`id` FROM `default`.`t0`)) AS u_1
  ignore("three-child union") {
    checkHiveQl(
      """
        |SELECT id FROM parquet_t0
        |UNION ALL SELECT id FROM parquet_t0
        |UNION ALL SELECT id FROM parquet_t0
      """.stripMargin)
  }


  test("intersect") {
    checkHiveQl("SELECT * FROM t0 INTERSECT SELECT * FROM t0")
  }

  test("except") {
    checkHiveQl("SELECT * FROM t0 EXCEPT SELECT * FROM t0")
  }

  test("self join") {
    checkHiveQl("SELECT x.key FROM parquet_t1 x JOIN parquet_t1 y ON x.key = y.key")
  }

  test("self join with group by") {
    checkHiveQl(
      "SELECT x.key, COUNT(*) FROM parquet_t1 x JOIN parquet_t1 y ON x.key = y.key group by x.key")
  }

  test("case") {
    checkHiveQl("SELECT CASE WHEN id % 2 > 0 THEN 0 WHEN id % 2 = 0 THEN 1 END FROM parquet_t0")
  }

  test("case with else") {
    checkHiveQl("SELECT CASE WHEN id % 2 > 0 THEN 0 ELSE 1 END FROM parquet_t0")
  }

  test("case with key") {
    checkHiveQl("SELECT CASE id WHEN 0 THEN 'foo' WHEN 1 THEN 'bar' END FROM parquet_t0")
  }

  test("case with key and else") {
    checkHiveQl("SELECT CASE id WHEN 0 THEN 'foo' WHEN 1 THEN 'bar' ELSE 'baz' END FROM parquet_t0")
  }

  test("select distinct without aggregate functions") {
    checkHiveQl("SELECT DISTINCT id FROM parquet_t0")
  }

  test("cluster by") {
    checkHiveQl("SELECT id FROM parquet_t0 CLUSTER BY id")
  }

  test("distribute by") {
    checkHiveQl("SELECT id FROM parquet_t0 DISTRIBUTE BY id")
  }

  test("distribute by with sort by") {
    checkHiveQl("SELECT id FROM parquet_t0 DISTRIBUTE BY id SORT BY id")
  }

  test("distinct aggregation") {
    checkHiveQl("SELECT COUNT(DISTINCT id) FROM parquet_t0")
  }

  test("TABLESAMPLE") {
    // Project [id#2L]
    // +- Sample 0.0, 1.0, false, ...
    //    +- Subquery s
    //       +- Subquery parquet_t0
    //          +- Relation[id#2L] ParquetRelation
    checkHiveQl("SELECT s.id FROM parquet_t0 TABLESAMPLE(100 PERCENT) s")

    // Project [id#2L]
    // +- Sample 0.0, 1.0, false, ...
    //    +- Subquery parquet_t0
    //       +- Relation[id#2L] ParquetRelation
    checkHiveQl("SELECT * FROM parquet_t0 TABLESAMPLE(100 PERCENT)")

    // Project [id#21L]
    // +- Sample 0.0, 1.0, false, ...
    //    +- MetastoreRelation default, t0, Some(s)
    checkHiveQl("SELECT s.id FROM t0 TABLESAMPLE(100 PERCENT) s")

    // Project [id#24L]
    // +- Sample 0.0, 1.0, false, ...
    //    +- MetastoreRelation default, t0, None
    checkHiveQl("SELECT * FROM t0 TABLESAMPLE(100 PERCENT)")

    // When a sampling fraction is not 100%, the returned results are random.
    // Thus, added an always-false filter here to check if the generated plan can be successfully
    // executed.
    checkHiveQl("SELECT s.id FROM parquet_t0 TABLESAMPLE(0.1 PERCENT) s WHERE 1=0")
    checkHiveQl("SELECT * FROM parquet_t0 TABLESAMPLE(0.1 PERCENT) WHERE 1=0")
  }

  // TODO Enable this
  // Query plans transformed by DistinctAggregationRewriter are not recognized yet
  ignore("multi-distinct columns") {
    checkHiveQl("SELECT a, COUNT(DISTINCT b), COUNT(DISTINCT c), SUM(d) FROM parquet_t2 GROUP BY a")
  }

  test("persisted data source relations") {
    Seq("orc", "json", "parquet").foreach { format =>
      val tableName = s"${format}_parquet_t0"
      withTable(tableName) {
        sqlContext.range(10).write.format(format).saveAsTable(tableName)
        checkHiveQl(s"SELECT id FROM $tableName")
      }
    }
  }

  test("plans with non-SQL expressions") {
    sqlContext.udf.register("foo", (_: Int) * 2)
    intercept[UnsupportedOperationException](new SQLBuilder(sql("SELECT foo(id) FROM t0")).toSQL)
  }

  test("named expression in column names shouldn't be quoted") {
    def checkColumnNames(query: String, expectedColNames: String*): Unit = {
      checkHiveQl(query)
      assert(sql(query).columns === expectedColNames)
    }

    // Attributes
    checkColumnNames(
      """SELECT * FROM (
        |  SELECT 1 AS a, 2 AS b, 3 AS `we``ird`
        |) s
      """.stripMargin,
      "a", "b", "we`ird"
    )

    checkColumnNames(
      """SELECT x.a, y.a, x.b, y.b
        |FROM (SELECT 1 AS a, 2 AS b) x
        |INNER JOIN (SELECT 1 AS a, 2 AS b) y
        |ON x.a = y.a
      """.stripMargin,
      "a", "a", "b", "b"
    )

    // String literal
    checkColumnNames(
      "SELECT 'foo', '\"bar\\''",
      "foo", "\"bar\'"
    )

    // Numeric literals (should have CAST or suffixes in column names)
    checkColumnNames(
      "SELECT 1Y, 2S, 3, 4L, 5.1, 6.1D",
      "1", "2", "3", "4", "5.1", "6.1"
    )

    // Aliases
    checkColumnNames(
      "SELECT 1 AS a",
      "a"
    )

    // Complex type extractors
    checkColumnNames(
      """SELECT
        |  a.f1, b[0].f1, b.f1, c["foo"], d[0]
        |FROM (
        |  SELECT
        |    NAMED_STRUCT("f1", 1, "f2", "foo") AS a,
        |    ARRAY(NAMED_STRUCT("f1", 1, "f2", "foo")) AS b,
        |    MAP("foo", 1) AS c,
        |    ARRAY(1) AS d
        |) s
      """.stripMargin,
      "f1", "b[0].f1", "f1", "c[foo]", "d[0]"
    )
  }
}
