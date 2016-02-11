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
    sql("DROP TABLE IF EXISTS t0")
    sql("DROP TABLE IF EXISTS t1")
    sql("DROP TABLE IF EXISTS t2")
    sqlContext.range(10).write.saveAsTable("t0")

    sqlContext
      .range(10)
      .select('id as 'key, concat(lit("val_"), 'id) as 'value)
      .write
      .saveAsTable("t1")

    sqlContext.range(10).select('id as 'a, 'id as 'b, 'id as 'c, 'id as 'd).write.saveAsTable("t2")
  }

  override protected def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS t0")
    sql("DROP TABLE IF EXISTS t1")
    sql("DROP TABLE IF EXISTS t2")
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
    checkHiveQl("SELECT id FROM t0 WHERE id IN (1, 2, 3)")
  }

  test("aggregate function in having clause") {
    checkHiveQl("SELECT COUNT(value) FROM t1 GROUP BY key HAVING MAX(key) > 0")
  }

  test("aggregate function in order by clause") {
    checkHiveQl("SELECT COUNT(value) FROM t1 GROUP BY key ORDER BY MAX(key)")
  }

  // When there are multiple aggregate functions in ORDER BY clause, all of them are extracted into
  // Aggregate operator and aliased to the same name "aggOrder".  This is OK for normal query
  // execution since these aliases have different expression ID.  But this introduces name collision
  // when converting resolved plans back to SQL query strings as expression IDs are stripped.
  test("aggregate function in order by clause with multiple order keys") {
    checkHiveQl("SELECT COUNT(value) FROM t1 GROUP BY key ORDER BY key, MAX(key)")
  }

  test("type widening in union") {
    checkHiveQl("SELECT id FROM t0 UNION ALL SELECT CAST(id AS INT) AS id FROM t0")
  }

  test("self join") {
    checkHiveQl("SELECT x.key FROM t1 x JOIN t1 y ON x.key = y.key")
  }

  test("self join with group by") {
    checkHiveQl("SELECT x.key, COUNT(*) FROM t1 x JOIN t1 y ON x.key = y.key group by x.key")
  }

  test("three-child union") {
    checkHiveQl("SELECT id FROM t0 UNION ALL SELECT id FROM t0 UNION ALL SELECT id FROM t0")
  }

  test("case") {
    checkHiveQl("SELECT CASE WHEN id % 2 > 0 THEN 0 WHEN id % 2 = 0 THEN 1 END FROM t0")
  }

  test("case with else") {
    checkHiveQl("SELECT CASE WHEN id % 2 > 0 THEN 0 ELSE 1 END FROM t0")
  }

  test("case with key") {
    checkHiveQl("SELECT CASE id WHEN 0 THEN 'foo' WHEN 1 THEN 'bar' END FROM t0")
  }

  test("case with key and else") {
    checkHiveQl("SELECT CASE id WHEN 0 THEN 'foo' WHEN 1 THEN 'bar' ELSE 'baz' END FROM t0")
  }

  test("select distinct without aggregate functions") {
    checkHiveQl("SELECT DISTINCT id FROM t0")
  }

  test("cluster by") {
    checkHiveQl("SELECT id FROM t0 CLUSTER BY id")
  }

  test("distribute by") {
    checkHiveQl("SELECT id FROM t0 DISTRIBUTE BY id")
  }

  test("distribute by with sort by") {
    checkHiveQl("SELECT id FROM t0 DISTRIBUTE BY id SORT BY id")
  }

  test("distinct aggregation") {
    checkHiveQl("SELECT COUNT(DISTINCT id) FROM t0")
  }

  // TODO Enable this
  // Query plans transformed by DistinctAggregationRewriter are not recognized yet
  ignore("multi-distinct columns") {
    checkHiveQl("SELECT a, COUNT(DISTINCT b), COUNT(DISTINCT c), SUM(d) FROM t2 GROUP BY a")
  }

  test("persisted data source relations") {
    Seq("orc", "json", "parquet").foreach { format =>
      val tableName = s"${format}_t0"
      withTable(tableName) {
        sqlContext.range(10).write.format(format).saveAsTable(tableName)
        checkHiveQl(s"SELECT id FROM $tableName")
      }
    }
  }
}
