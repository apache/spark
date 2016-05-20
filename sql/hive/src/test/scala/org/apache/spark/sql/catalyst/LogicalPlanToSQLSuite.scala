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

package org.apache.spark.sql.catalyst

import scala.util.control.NonFatal

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SQLTestUtils

class LogicalPlanToSQLSuite extends SQLBuilderTest with SQLTestUtils {
  import testImplicits._

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    sql("DROP TABLE IF EXISTS parquet_t0")
    sql("DROP TABLE IF EXISTS parquet_t1")
    sql("DROP TABLE IF EXISTS parquet_t2")
    sql("DROP TABLE IF EXISTS t0")

    spark.range(10).write.saveAsTable("parquet_t0")
    sql("CREATE TABLE t0 AS SELECT * FROM parquet_t0")

    spark
      .range(10)
      .select('id as 'key, concat(lit("val_"), 'id) as 'value)
      .write
      .saveAsTable("parquet_t1")

    spark
      .range(10)
      .select('id as 'a, 'id as 'b, 'id as 'c, 'id as 'd)
      .write
      .saveAsTable("parquet_t2")

    def createArray(id: Column): Column = {
      when(id % 3 === 0, lit(null)).otherwise(array('id, 'id + 1))
    }

    spark
      .range(10)
      .select(
        createArray('id).as("arr"),
        array(array('id), createArray('id)).as("arr2"),
        lit("""{"f1": "1", "f2": "2", "f3": 3}""").as("json"),
        'id
      )
      .write
      .saveAsTable("parquet_t3")
  }

  override protected def afterAll(): Unit = {
    try {
      sql("DROP TABLE IF EXISTS parquet_t0")
      sql("DROP TABLE IF EXISTS parquet_t1")
      sql("DROP TABLE IF EXISTS parquet_t2")
      sql("DROP TABLE IF EXISTS parquet_t3")
      sql("DROP TABLE IF EXISTS t0")
    } finally {
      super.afterAll()
    }
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
           """.stripMargin, e)
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
         """.stripMargin, cause)
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

  test("three-child union") {
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

  test("rollup/cube #1") {
    // Original logical plan:
    //   Aggregate [(key#17L % cast(5 as bigint))#47L,grouping__id#46],
    //             [(count(1),mode=Complete,isDistinct=false) AS cnt#43L,
    //              (key#17L % cast(5 as bigint))#47L AS _c1#45L,
    //              grouping__id#46 AS _c2#44]
    //   +- Expand [List(key#17L, value#18, (key#17L % cast(5 as bigint))#47L, 0),
    //              List(key#17L, value#18, null, 1)],
    //             [key#17L,value#18,(key#17L % cast(5 as bigint))#47L,grouping__id#46]
    //      +- Project [key#17L,
    //                  value#18,
    //                  (key#17L % cast(5 as bigint)) AS (key#17L % cast(5 as bigint))#47L]
    //         +- Subquery t1
    //            +- Relation[key#17L,value#18] ParquetRelation
    // Converted SQL:
    //   SELECT count( 1) AS `cnt`,
    //          (`t1`.`key` % CAST(5 AS BIGINT)),
    //          grouping_id() AS `_c2`
    //   FROM `default`.`t1`
    //   GROUP BY (`t1`.`key` % CAST(5 AS BIGINT))
    //   GROUPING SETS (((`t1`.`key` % CAST(5 AS BIGINT))), ())
    checkHiveQl(
      "SELECT count(*) as cnt, key%5, grouping_id() FROM parquet_t1 GROUP BY key % 5 WITH ROLLUP")
    checkHiveQl(
      "SELECT count(*) as cnt, key%5, grouping_id() FROM parquet_t1 GROUP BY key % 5 WITH CUBE")
  }

  test("rollup/cube #2") {
    checkHiveQl("SELECT key, value, count(value) FROM parquet_t1 GROUP BY key, value WITH ROLLUP")
    checkHiveQl("SELECT key, value, count(value) FROM parquet_t1 GROUP BY key, value WITH CUBE")
  }

  test("rollup/cube #3") {
    checkHiveQl(
      "SELECT key, count(value), grouping_id() FROM parquet_t1 GROUP BY key, value WITH ROLLUP")
    checkHiveQl(
      "SELECT key, count(value), grouping_id() FROM parquet_t1 GROUP BY key, value WITH CUBE")
  }

  test("rollup/cube #4") {
    checkHiveQl(
      s"""
        |SELECT count(*) as cnt, key % 5 as k1, key - 5 as k2, grouping_id() FROM parquet_t1
        |GROUP BY key % 5, key - 5 WITH ROLLUP
      """.stripMargin)
    checkHiveQl(
      s"""
        |SELECT count(*) as cnt, key % 5 as k1, key - 5 as k2, grouping_id() FROM parquet_t1
        |GROUP BY key % 5, key - 5 WITH CUBE
      """.stripMargin)
  }

  test("rollup/cube #5") {
    checkHiveQl(
      s"""
        |SELECT count(*) AS cnt, key % 5 AS k1, key - 5 AS k2, grouping_id(key % 5, key - 5) AS k3
        |FROM (SELECT key, key%2, key - 5 FROM parquet_t1) t GROUP BY key%5, key-5
        |WITH ROLLUP
      """.stripMargin)
    checkHiveQl(
      s"""
        |SELECT count(*) AS cnt, key % 5 AS k1, key - 5 AS k2, grouping_id(key % 5, key - 5) AS k3
        |FROM (SELECT key, key % 2, key - 5 FROM parquet_t1) t GROUP BY key % 5, key - 5
        |WITH CUBE
      """.stripMargin)
  }

  test("rollup/cube #6") {
    checkHiveQl("SELECT a, b, sum(c) FROM parquet_t2 GROUP BY ROLLUP(a, b) ORDER BY a, b")
    checkHiveQl("SELECT a, b, sum(c) FROM parquet_t2 GROUP BY CUBE(a, b) ORDER BY a, b")
    checkHiveQl("SELECT a, b, sum(a) FROM parquet_t2 GROUP BY ROLLUP(a, b) ORDER BY a, b")
    checkHiveQl("SELECT a, b, sum(a) FROM parquet_t2 GROUP BY CUBE(a, b) ORDER BY a, b")
    checkHiveQl("SELECT a + b, b, sum(a - b) FROM parquet_t2 GROUP BY a + b, b WITH ROLLUP")
    checkHiveQl("SELECT a + b, b, sum(a - b) FROM parquet_t2 GROUP BY a + b, b WITH CUBE")
  }

  test("rollup/cube #7") {
    checkHiveQl("SELECT a, b, grouping_id(a, b) FROM parquet_t2 GROUP BY cube(a, b)")
    checkHiveQl("SELECT a, b, grouping(b) FROM parquet_t2 GROUP BY cube(a, b)")
    checkHiveQl("SELECT a, b, grouping(a) FROM parquet_t2 GROUP BY cube(a, b)")
  }

  test("rollup/cube #8") {
    // grouping_id() is part of another expression
    checkHiveQl(
      s"""
         |SELECT hkey AS k1, value - 5 AS k2, hash(grouping_id()) AS hgid
         |FROM (SELECT hash(key) as hkey, key as value FROM parquet_t1) t GROUP BY hkey, value-5
         |WITH ROLLUP
      """.stripMargin)
    checkHiveQl(
      s"""
         |SELECT hkey AS k1, value - 5 AS k2, hash(grouping_id()) AS hgid
         |FROM (SELECT hash(key) as hkey, key as value FROM parquet_t1) t GROUP BY hkey, value-5
         |WITH CUBE
      """.stripMargin)
  }

  test("rollup/cube #9") {
    // self join is used as the child node of ROLLUP/CUBE with replaced quantifiers
    checkHiveQl(
      s"""
         |SELECT t.key - 5, cnt, SUM(cnt)
         |FROM (SELECT x.key, COUNT(*) as cnt
         |FROM parquet_t1 x JOIN parquet_t1 y ON x.key = y.key GROUP BY x.key) t
         |GROUP BY cnt, t.key - 5
         |WITH ROLLUP
      """.stripMargin)
    checkHiveQl(
      s"""
         |SELECT t.key - 5, cnt, SUM(cnt)
         |FROM (SELECT x.key, COUNT(*) as cnt
         |FROM parquet_t1 x JOIN parquet_t1 y ON x.key = y.key GROUP BY x.key) t
         |GROUP BY cnt, t.key - 5
         |WITH CUBE
      """.stripMargin)
  }

  test("grouping sets #1") {
    checkHiveQl(
      s"""
         |SELECT count(*) AS cnt, key % 5 AS k1, key - 5 AS k2, grouping_id() AS k3
         |FROM (SELECT key, key % 2, key - 5 FROM parquet_t1) t GROUP BY key % 5, key - 5
         |GROUPING SETS (key % 5, key - 5)
      """.stripMargin)
  }

  test("grouping sets #2") {
    checkHiveQl(
      "SELECT a, b, sum(c) FROM parquet_t2 GROUP BY a, b GROUPING SETS (a, b) ORDER BY a, b")
    checkHiveQl(
      "SELECT a, b, sum(c) FROM parquet_t2 GROUP BY a, b GROUPING SETS (a) ORDER BY a, b")
    checkHiveQl(
      "SELECT a, b, sum(c) FROM parquet_t2 GROUP BY a, b GROUPING SETS (b) ORDER BY a, b")
    checkHiveQl(
      "SELECT a, b, sum(c) FROM parquet_t2 GROUP BY a, b GROUPING SETS (()) ORDER BY a, b")
    checkHiveQl(
      s"""
         |SELECT a, b, sum(c) FROM parquet_t2 GROUP BY a, b
         |GROUPING SETS ((), (a), (a, b)) ORDER BY a, b
      """.stripMargin)
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

  test("SPARK-13720: sort by after having") {
    checkHiveQl("SELECT COUNT(value) FROM parquet_t1 GROUP BY key HAVING MAX(key) > 0 SORT BY key")
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

  test("multi-distinct columns") {
    checkHiveQl("SELECT a, COUNT(DISTINCT b), COUNT(DISTINCT c), SUM(d) FROM parquet_t2 GROUP BY a")
  }

  test("persisted data source relations") {
    Seq("orc", "json", "parquet").foreach { format =>
      val tableName = s"${format}_parquet_t0"
      withTable(tableName) {
        spark.range(10).write.format(format).saveAsTable(tableName)
        checkHiveQl(s"SELECT id FROM $tableName")
      }
    }
  }

  test("script transformation - schemaless") {
    checkHiveQl("SELECT TRANSFORM (a, b, c, d) USING 'cat' FROM parquet_t2")
    checkHiveQl("SELECT TRANSFORM (*) USING 'cat' FROM parquet_t2")
  }

  test("script transformation - alias list") {
    checkHiveQl("SELECT TRANSFORM (a, b, c, d) USING 'cat' AS (d1, d2, d3, d4) FROM parquet_t2")
  }

  test("script transformation - alias list with type") {
    checkHiveQl(
      """FROM
        |(FROM parquet_t1 SELECT TRANSFORM(key, value) USING 'cat' AS (thing1 int, thing2 string)) t
        |SELECT thing1 + 1
      """.stripMargin)
  }

  test("script transformation - row format delimited clause with only one format property") {
    checkHiveQl(
      """SELECT TRANSFORM (key) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        |USING 'cat' AS (tKey) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        |FROM parquet_t1
      """.stripMargin)
  }

  test("script transformation - row format delimited clause with multiple format properties") {
    checkHiveQl(
      """SELECT TRANSFORM (key)
        |ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\t'
        |USING 'cat' AS (tKey)
        |ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\t'
        |FROM parquet_t1
      """.stripMargin)
  }

  test("script transformation - row format serde clauses with SERDEPROPERTIES") {
    checkHiveQl(
      """SELECT TRANSFORM (key, value)
        |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        |WITH SERDEPROPERTIES('field.delim' = '|')
        |USING 'cat' AS (tKey, tValue)
        |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        |WITH SERDEPROPERTIES('field.delim' = '|')
        |FROM parquet_t1
      """.stripMargin)
  }

  test("script transformation - row format serde clauses without SERDEPROPERTIES") {
    checkHiveQl(
      """SELECT TRANSFORM (key, value)
        |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        |USING 'cat' AS (tKey, tValue)
        |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        |FROM parquet_t1
      """.stripMargin)
  }

  test("plans with non-SQL expressions") {
    spark.udf.register("foo", (_: Int) * 2)
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

  test("window basic") {
    checkHiveQl("SELECT MAX(value) OVER (PARTITION BY key % 3) FROM parquet_t1")
    checkHiveQl(
      """
         |SELECT key, value, ROUND(AVG(key) OVER (), 2)
         |FROM parquet_t1 ORDER BY key
      """.stripMargin)
    checkHiveQl(
      """
         |SELECT value, MAX(key + 1) OVER (PARTITION BY key % 5 ORDER BY key % 7) AS max
         |FROM parquet_t1
      """.stripMargin)
  }

  test("multiple window functions in one expression") {
    checkHiveQl(
      """
        |SELECT
        |  MAX(key) OVER (ORDER BY key DESC, value) / MIN(key) OVER (PARTITION BY key % 3)
        |FROM parquet_t1
      """.stripMargin)
  }

  test("regular expressions and window functions in one expression") {
    checkHiveQl("SELECT MAX(key) OVER (PARTITION BY key % 3) + key FROM parquet_t1")
  }

  test("aggregate functions and window functions in one expression") {
    checkHiveQl("SELECT MAX(c) + COUNT(a) OVER () FROM parquet_t2 GROUP BY a, b")
  }

  test("window with different window specification") {
    checkHiveQl(
      """
         |SELECT key, value,
         |DENSE_RANK() OVER (ORDER BY key, value) AS dr,
         |MAX(value) OVER (PARTITION BY key ORDER BY key ASC) AS max
         |FROM parquet_t1
      """.stripMargin)
  }

  test("window with the same window specification with aggregate + having") {
    checkHiveQl(
      """
         |SELECT key, value,
         |MAX(value) OVER (PARTITION BY key % 5 ORDER BY key DESC) AS max
         |FROM parquet_t1 GROUP BY key, value HAVING key > 5
      """.stripMargin)
  }

  test("window with the same window specification with aggregate functions") {
    checkHiveQl(
      """
         |SELECT key, value,
         |MAX(value) OVER (PARTITION BY key % 5 ORDER BY key) AS max
         |FROM parquet_t1 GROUP BY key, value
      """.stripMargin)
  }

  test("window with the same window specification with aggregate") {
    checkHiveQl(
      """
         |SELECT key, value,
         |DENSE_RANK() OVER (DISTRIBUTE BY key SORT BY key, value) AS dr,
         |COUNT(key)
         |FROM parquet_t1 GROUP BY key, value
      """.stripMargin)
  }

  test("window with the same window specification without aggregate and filter") {
    checkHiveQl(
      """
         |SELECT key, value,
         |DENSE_RANK() OVER (DISTRIBUTE BY key SORT BY key, value) AS dr,
         |COUNT(key) OVER(DISTRIBUTE BY key SORT BY key, value) AS ca
         |FROM parquet_t1
      """.stripMargin)
  }

  test("window clause") {
    checkHiveQl(
      """
         |SELECT key, MAX(value) OVER w1 AS MAX, MIN(value) OVER w2 AS min
         |FROM parquet_t1
         |WINDOW w1 AS (PARTITION BY key % 5 ORDER BY key), w2 AS (PARTITION BY key % 6)
      """.stripMargin)
  }

  test("special window functions") {
    checkHiveQl(
      """
        |SELECT
        |  RANK() OVER w,
        |  PERCENT_RANK() OVER w,
        |  DENSE_RANK() OVER w,
        |  ROW_NUMBER() OVER w,
        |  NTILE(10) OVER w,
        |  CUME_DIST() OVER w,
        |  LAG(key, 2) OVER w,
        |  LEAD(key, 2) OVER w
        |FROM parquet_t1
        |WINDOW w AS (PARTITION BY key % 5 ORDER BY key)
      """.stripMargin)
  }

  test("window with join") {
    checkHiveQl(
      """
        |SELECT x.key, MAX(y.key) OVER (PARTITION BY x.key % 5 ORDER BY x.key)
        |FROM parquet_t1 x JOIN parquet_t1 y ON x.key = y.key
      """.stripMargin)
  }

  test("join 2 tables and aggregate function in having clause") {
    checkHiveQl(
      """
        |SELECT COUNT(a.value), b.KEY, a.KEY
        |FROM parquet_t1 a, parquet_t1 b
        |GROUP BY a.KEY, b.KEY
        |HAVING MAX(a.KEY) > 0
      """.stripMargin)
  }

  test("generator in project list without FROM clause") {
    checkHiveQl("SELECT EXPLODE(ARRAY(1,2,3))")
    checkHiveQl("SELECT EXPLODE(ARRAY(1,2,3)) AS val")
  }

  test("generator in project list with non-referenced table") {
    checkHiveQl("SELECT EXPLODE(ARRAY(1,2,3)) FROM t0")
    checkHiveQl("SELECT EXPLODE(ARRAY(1,2,3)) AS val FROM t0")
  }

  test("generator in project list with referenced table") {
    checkHiveQl("SELECT EXPLODE(arr) FROM parquet_t3")
    checkHiveQl("SELECT EXPLODE(arr) AS val FROM parquet_t3")
  }

  test("generator in project list with non-UDTF expressions") {
    checkHiveQl("SELECT EXPLODE(arr), id FROM parquet_t3")
    checkHiveQl("SELECT EXPLODE(arr) AS val, id as a FROM parquet_t3")
  }

  test("generator in lateral view") {
    checkHiveQl("SELECT val, id FROM parquet_t3 LATERAL VIEW EXPLODE(arr) exp AS val")
    checkHiveQl("SELECT val, id FROM parquet_t3 LATERAL VIEW OUTER EXPLODE(arr) exp AS val")
  }

  test("generator in lateral view with ambiguous names") {
    checkHiveQl(
      """
        |SELECT exp.id, parquet_t3.id
        |FROM parquet_t3
        |LATERAL VIEW EXPLODE(arr) exp AS id
      """.stripMargin)
    checkHiveQl(
      """
        |SELECT exp.id, parquet_t3.id
        |FROM parquet_t3
        |LATERAL VIEW OUTER EXPLODE(arr) exp AS id
      """.stripMargin)
  }

  test("use JSON_TUPLE as generator") {
    checkHiveQl(
      """
        |SELECT c0, c1, c2
        |FROM parquet_t3
        |LATERAL VIEW JSON_TUPLE(json, 'f1', 'f2', 'f3') jt
      """.stripMargin)
    checkHiveQl(
      """
        |SELECT a, b, c
        |FROM parquet_t3
        |LATERAL VIEW JSON_TUPLE(json, 'f1', 'f2', 'f3') jt AS a, b, c
      """.stripMargin)
  }

  test("nested generator in lateral view") {
    checkHiveQl(
      """
        |SELECT val, id
        |FROM parquet_t3
        |LATERAL VIEW EXPLODE(arr2) exp1 AS nested_array
        |LATERAL VIEW EXPLODE(nested_array) exp1 AS val
      """.stripMargin)

    checkHiveQl(
      """
        |SELECT val, id
        |FROM parquet_t3
        |LATERAL VIEW EXPLODE(arr2) exp1 AS nested_array
        |LATERAL VIEW OUTER EXPLODE(nested_array) exp1 AS val
      """.stripMargin)
  }

  test("generate with other operators") {
    checkHiveQl(
      """
        |SELECT EXPLODE(arr) AS val, id
        |FROM parquet_t3
        |WHERE id > 2
        |ORDER BY val, id
        |LIMIT 5
      """.stripMargin)

    checkHiveQl(
      """
        |SELECT val, id
        |FROM parquet_t3
        |LATERAL VIEW EXPLODE(arr2) exp1 AS nested_array
        |LATERAL VIEW EXPLODE(nested_array) exp1 AS val
        |WHERE val > 2
        |ORDER BY val, id
        |LIMIT 5
      """.stripMargin)
  }

  test("filter after subquery") {
    checkHiveQl("SELECT a FROM (SELECT key + 1 AS a FROM parquet_t1) t WHERE a > 5")
  }

  test("SPARK-14933 - select parquet table") {
    withTable("parquet_t") {
      sql(
        """
          |create table parquet_t (c1 int, c2 string)
          |stored as parquet select 1, 'abc'
        """.stripMargin)

      checkHiveQl("select * from parquet_t")
    }
  }

  test("SPARK-14933 - select orc table") {
    withTable("orc_t") {
      sql(
        """
          |create table orc_t (c1 int, c2 string)
          |stored as orc select 1, 'abc'
        """.stripMargin)

      checkHiveQl("select * from orc_t")
    }
  }
}
