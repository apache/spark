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

  /**
   * Compare the generated SQL with the expected answer string.
   * Note that there exists a normalization for both arguments for the convenience.
   * - Remove the id from the generated attributes, e.g., `gen_attr_1` -> `gen_attr`.
   * - Multiple spaces are replaced into a single space.
   * - New line characters are replaced into space.
   * - Trim the left and right finally.
   */
  private def checkSQLStructure(convertedSQL: String, expectedString: String): Unit = {
    val normalizedGenSQL = convertedSQL
      .replaceAll("`gen_attr_\\d+`", "`gen_attr`")
      .replaceAll("  ", " ")
    assert(normalizedGenSQL == expectedString.replaceAll("[ \n]+", " ").trim())
  }

  private def checkHiveQl(hiveQl: String, expectedString: String = null): Unit = {
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

    if (expectedString != null) checkSQLStructure(convertedSQL, expectedString)

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
    checkHiveQl("SELECT id FROM parquet_t0 WHERE id IN (1, 2, 3)",
      """
        |SELECT `gen_attr` AS `id`
        |FROM (SELECT `gen_attr`
        |      FROM (SELECT `id` AS `gen_attr`
        |            FROM `default`.`parquet_t0`)
        |            AS gen_subquery_0
        |      WHERE (CAST(`gen_attr` AS BIGINT)
        |             IN (CAST(1 AS BIGINT), CAST(2 AS BIGINT), CAST(3 AS BIGINT))))
        |      AS parquet_t0
      """.stripMargin)
  }

  test("not in") {
    checkHiveQl("SELECT id FROM t0 WHERE id NOT IN (1, 2, 3)",
      """
        |SELECT `gen_attr` AS `id`
        |FROM (SELECT `gen_attr`
        |      FROM (SELECT `id` AS `gen_attr` FROM `default`.`t0`) AS gen_subquery_0
        |      WHERE (NOT (CAST(`gen_attr` AS BIGINT)
        |                  IN (CAST(1 AS BIGINT), CAST(2 AS BIGINT), CAST(3 AS BIGINT)))))
        |      AS t0
      """.stripMargin)
  }

  test("not like") {
    checkHiveQl("SELECT id FROM t0 WHERE id + 5 NOT LIKE '1%'",
      """
        |SELECT `gen_attr` AS `id`
        |FROM (SELECT `gen_attr`
        |      FROM (SELECT `id` AS `gen_attr`
        |            FROM `default`.`t0`)
        |            AS gen_subquery_0
        |      WHERE (NOT CAST((`gen_attr` + CAST(5 AS BIGINT)) AS STRING) LIKE "1%"))
        |      AS t0
      """.stripMargin)
  }

  test("aggregate function in having clause") {
    checkHiveQl("SELECT COUNT(value) FROM parquet_t1 GROUP BY key HAVING MAX(key) > 0",
      """
        |SELECT `gen_attr` AS `count(value)`
        |FROM (SELECT `gen_attr`
        |      FROM (SELECT count(`gen_attr`) AS `gen_attr`, max(`gen_attr`) AS `gen_attr`
        |            FROM (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |                  FROM `default`.`parquet_t1`)
        |                  AS gen_subquery_0
        |            GROUP BY `gen_attr`
        |            HAVING (`gen_attr` > CAST(0 AS BIGINT)))
        |            AS gen_subquery_1)
        |      AS gen_subquery_2
      """.stripMargin)
  }

  test("aggregate function in order by clause") {
    checkHiveQl("SELECT COUNT(value) FROM parquet_t1 GROUP BY key ORDER BY MAX(key)",
      """
        |SELECT `gen_attr` AS `count(value)`
        |FROM (SELECT `gen_attr`
        |      FROM (SELECT count(`gen_attr`) AS `gen_attr`, max(`gen_attr`) AS `gen_attr`
        |            FROM (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |                  FROM `default`.`parquet_t1`)
        |                  AS gen_subquery_0
        |            GROUP BY `gen_attr`
        |            ORDER BY `gen_attr` ASC)
        |            AS gen_subquery_1)
        |      AS gen_subquery_2
      """.stripMargin)
  }

  // When there are multiple aggregate functions in ORDER BY clause, all of them are extracted into
  // Aggregate operator and aliased to the same name "aggOrder".  This is OK for normal query
  // execution since these aliases have different expression ID.  But this introduces name collision
  // when converting resolved plans back to SQL query strings as expression IDs are stripped.
  test("aggregate function in order by clause with multiple order keys") {
    checkHiveQl("SELECT COUNT(value) FROM parquet_t1 GROUP BY key ORDER BY key, MAX(key)",
      """
        |SELECT `gen_attr` AS `count(value)`
        |FROM (SELECT `gen_attr`
        |      FROM (SELECT count(`gen_attr`) AS `gen_attr`, `gen_attr` AS `gen_attr`,
        |                   max(`gen_attr`) AS `gen_attr`
        |            FROM (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |                  FROM `default`.`parquet_t1`)
        |                  AS gen_subquery_0
        |            GROUP BY `gen_attr`
        |            ORDER BY `gen_attr` ASC, `gen_attr` ASC)
        |            AS gen_subquery_1)
        |      AS gen_subquery_2
      """.stripMargin)
  }

  test("type widening in union") {
    checkHiveQl("SELECT id FROM parquet_t0 UNION ALL SELECT CAST(id AS INT) AS id FROM parquet_t0",
      """
        |SELECT `gen_attr` AS `id`
        |FROM ((SELECT `gen_attr`
        |       FROM (SELECT `id` AS `gen_attr`
        |             FROM `default`.`parquet_t0`)
        |             AS gen_subquery_0)
        |      UNION ALL
        |      (SELECT CAST(CAST(`gen_attr` AS INT) AS BIGINT) AS `gen_attr`
        |       FROM (SELECT `id` AS `gen_attr`
        |             FROM `default`.`parquet_t0`)
        |             AS gen_subquery_1))
        |     AS parquet_t0
      """.stripMargin)
  }

  test("union distinct") {
    checkHiveQl("SELECT * FROM t0 UNION SELECT * FROM t0",
      """
        |SELECT `gen_attr` AS `id`
        |FROM ((SELECT `gen_attr`
        |       FROM (SELECT `id` AS `gen_attr`
        |             FROM `default`.`t0`)
        |             AS gen_subquery_0)
        |      UNION DISTINCT
        |      (SELECT `gen_attr`
        |       FROM (SELECT `id` AS `gen_attr`
        |             FROM `default`.`t0`)
        |             AS gen_subquery_1))
        |     AS t0
      """.stripMargin)
  }

  test("three-child union") {
    checkHiveQl(
      """
        |SELECT id FROM parquet_t0
        |UNION ALL SELECT id FROM parquet_t0
        |UNION ALL SELECT id FROM parquet_t0
      """.stripMargin,
      """
        |SELECT `gen_attr` AS `id`
        |FROM ((SELECT `gen_attr`
        |       FROM (SELECT `id` AS `gen_attr`
        |             FROM `default`.`parquet_t0`)
        |             AS gen_subquery_0)
        |      UNION ALL
        |      (SELECT `gen_attr`
        |       FROM (SELECT `id` AS `gen_attr`
        |             FROM `default`.`parquet_t0`)
        |             AS gen_subquery_1)
        |      UNION ALL
        |      (SELECT `gen_attr`
        |       FROM (SELECT `id` AS `gen_attr`
        |             FROM `default`.`parquet_t0`)
        |             AS gen_subquery_2))
        |     AS parquet_t0
      """.stripMargin)
  }

  test("intersect") {
    checkHiveQl("SELECT * FROM t0 INTERSECT SELECT * FROM t0",
      """
        |SELECT `gen_attr` AS `id`
        |FROM ((SELECT `gen_attr`
        |       FROM (SELECT `id` AS `gen_attr`
        |             FROM `default`.`t0`)
        |             AS gen_subquery_0 )
        |      INTERSECT
        |      ( SELECT `gen_attr`
        |       FROM (SELECT `id` AS `gen_attr`
        |             FROM `default`.`t0`)
        |             AS gen_subquery_1))
        |     AS t0
      """.stripMargin)
  }

  test("except") {
    checkHiveQl("SELECT * FROM t0 EXCEPT SELECT * FROM t0",
      """
        |SELECT `gen_attr` AS `id`
        |FROM ((SELECT `gen_attr`
        |       FROM (SELECT `id` AS `gen_attr`
        |             FROM `default`.`t0`)
        |             AS gen_subquery_0 )
        |      EXCEPT ( SELECT `gen_attr`
        |              FROM (SELECT `id` AS `gen_attr`
        |                    FROM `default`.`t0`)
        |                    AS gen_subquery_1))
        |     AS t0
      """.stripMargin)
  }

  test("self join") {
    checkHiveQl("SELECT x.key FROM parquet_t1 x JOIN parquet_t1 y ON x.key = y.key",
      """
        |SELECT `gen_attr` AS `key`
        |FROM (SELECT `gen_attr`
        |      FROM (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |            FROM `default`.`parquet_t1`)
        |            AS gen_subquery_0
        |     INNER JOIN (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |                 FROM `default`.`parquet_t1`)
        |                 AS gen_subquery_1
        |     ON (`gen_attr` = `gen_attr`))
        |     AS x
      """.stripMargin)
  }

  test("self join with group by") {
    checkHiveQl(
      "SELECT x.key, COUNT(*) FROM parquet_t1 x JOIN parquet_t1 y ON x.key = y.key group by x.key",
      """
        |SELECT `gen_attr` AS `key`, `gen_attr` AS `count(1)`
        |FROM (SELECT `gen_attr`, count(1) AS `gen_attr`
        |      FROM (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |            FROM `default`.`parquet_t1`)
        |            AS gen_subquery_0
        |     INNER JOIN (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |                 FROM `default`.`parquet_t1`)
        |                 AS gen_subquery_1
        |     ON (`gen_attr` = `gen_attr`)
        |     GROUP BY `gen_attr`)
        |     AS x
      """.stripMargin)
  }

  test("case") {
    // scalastyle:off line.size.limit
    checkHiveQl("SELECT CASE WHEN id % 2 > 0 THEN 0 WHEN id % 2 = 0 THEN 1 END FROM parquet_t0",
      """
        |SELECT `gen_attr` AS `CASE WHEN ((id % CAST(2 AS BIGINT)) > CAST(0 AS BIGINT)) THEN 0 WHEN ((id % CAST(2 AS BIGINT)) = CAST(0 AS BIGINT)) THEN 1 END`
        |FROM (SELECT CASE WHEN ((`gen_attr` % CAST(2 AS BIGINT)) > CAST(0 AS BIGINT)) THEN 0
        |                  WHEN ((`gen_attr` % CAST(2 AS BIGINT)) = CAST(0 AS BIGINT)) THEN 1
        |             END AS `gen_attr`
        |      FROM (SELECT `id` AS `gen_attr`
        |            FROM `default`.`parquet_t0`)
        |            AS gen_subquery_0)
        |      AS gen_subquery_1
      """.stripMargin)
    // scalastyle:on line.size.limit
  }

  test("case with else") {
    checkHiveQl("SELECT CASE WHEN id % 2 > 0 THEN 0 ELSE 1 END FROM parquet_t0",
      """
        |SELECT `gen_attr`
        |       AS `CASE WHEN ((id % CAST(2 AS BIGINT)) > CAST(0 AS BIGINT)) THEN 0 ELSE 1 END`
        |FROM (SELECT CASE WHEN ((`gen_attr` % CAST(2 AS BIGINT)) > CAST(0 AS BIGINT)) THEN 0
        |                  ELSE 1
        |             END AS `gen_attr`
        |      FROM (SELECT `id` AS `gen_attr`
        |            FROM `default`.`parquet_t0`)
        |            AS gen_subquery_0)
        |      AS gen_subquery_1
      """.stripMargin)
  }

  test("case with key") {
    // scalastyle:off line.size.limit
    checkHiveQl("SELECT CASE id WHEN 0 THEN 'foo' WHEN 1 THEN 'bar' END FROM parquet_t0",
      """
        |SELECT `gen_attr` AS `CASE WHEN (id = CAST(0 AS BIGINT)) THEN foo WHEN (id = CAST(1 AS BIGINT)) THEN bar END`
        |FROM (SELECT CASE WHEN (`gen_attr` = CAST(0 AS BIGINT)) THEN "foo"
        |                  WHEN (`gen_attr` = CAST(1 AS BIGINT)) THEN "bar"
        |             END AS `gen_attr`
        |      FROM (SELECT `id` AS `gen_attr`
        |            FROM `default`.`parquet_t0`)
        |            AS gen_subquery_0)
        |      AS gen_subquery_1
      """.stripMargin)
    // scalastyle:on line.size.limit
  }

  test("case with key and else") {
    // scalastyle:off line.size.limit
    checkHiveQl("SELECT CASE id WHEN 0 THEN 'foo' WHEN 1 THEN 'bar' ELSE 'baz' END FROM parquet_t0",
      """
        |SELECT `gen_attr` AS `CASE WHEN (id = CAST(0 AS BIGINT)) THEN foo WHEN (id = CAST(1 AS BIGINT)) THEN bar ELSE baz END`
        |FROM (SELECT CASE WHEN (`gen_attr` = CAST(0 AS BIGINT)) THEN "foo"
        |                  WHEN (`gen_attr` = CAST(1 AS BIGINT)) THEN "bar"
        |                  ELSE "baz"
        |             END AS `gen_attr`
        |     FROM
        |     (SELECT `id` AS `gen_attr`
        |      FROM `default`.`parquet_t0`)
        |      AS gen_subquery_0)
        |     AS gen_subquery_1
      """.stripMargin)
    // scalastyle:on line.size.limit
  }

  test("select distinct without aggregate functions") {
    checkHiveQl("SELECT DISTINCT id FROM parquet_t0",
      """
        |SELECT `gen_attr` AS `id`
        |FROM (SELECT DISTINCT `gen_attr`
        |      FROM (SELECT `id` AS `gen_attr`
        |            FROM `default`.`parquet_t0`)
        |      AS gen_subquery_0)
        |     AS parquet_t0
      """.stripMargin)
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
      "SELECT count(*) as cnt, key%5, grouping_id() FROM parquet_t1 GROUP BY key % 5 WITH ROLLUP",
      """
        |SELECT `gen_attr` AS `cnt`, `gen_attr` AS `(key % CAST(5 AS BIGINT))`,
        |       `gen_attr` AS `grouping_id()`
        |FROM (SELECT count(1) AS `gen_attr`, (`gen_attr` % CAST(5 AS BIGINT)) AS `gen_attr`,
        |             grouping_id() AS `gen_attr`
        |      FROM (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |            FROM `default`.`parquet_t1`)
        |            AS gen_subquery_0
        |      GROUP BY (`gen_attr` % CAST(5 AS BIGINT))
        |      GROUPING SETS(((`gen_attr` % CAST(5 AS BIGINT))), ()))
        |      AS gen_subquery_1
      """.stripMargin)

    checkHiveQl(
      "SELECT count(*) as cnt, key%5, grouping_id() FROM parquet_t1 GROUP BY key % 5 WITH CUBE",
      """
        |SELECT `gen_attr` AS `cnt`, `gen_attr` AS `(key % CAST(5 AS BIGINT))`,
        |       `gen_attr` AS `grouping_id()`
        |FROM (SELECT count(1) AS `gen_attr`, (`gen_attr` % CAST(5 AS BIGINT)) AS `gen_attr`,
        |             grouping_id() AS `gen_attr`
        |      FROM (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |            FROM `default`.`parquet_t1`)
        |            AS gen_subquery_0
        |      GROUP BY (`gen_attr` % CAST(5 AS BIGINT))
        |      GROUPING SETS(((`gen_attr` % CAST(5 AS BIGINT))), ()))
        |      AS gen_subquery_1
      """.stripMargin)
  }

  test("rollup/cube #2") {
    checkHiveQl("SELECT key, value, count(value) FROM parquet_t1 GROUP BY key, value WITH ROLLUP",
      """
        |SELECT `gen_attr` AS `key`, `gen_attr` AS `value`, `gen_attr` AS `count(value)`
        |FROM (SELECT `gen_attr` AS `gen_attr`, `gen_attr` AS `gen_attr`,
        |             count(`gen_attr`) AS `gen_attr`
        |      FROM (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |            FROM `default`.`parquet_t1`)
        |            AS gen_subquery_0
        |      GROUP BY `gen_attr`, `gen_attr`
        |      GROUPING SETS((`gen_attr`, `gen_attr`), (`gen_attr`), ()))
        |      AS gen_subquery_1
      """.stripMargin)

    checkHiveQl("SELECT key, value, count(value) FROM parquet_t1 GROUP BY key, value WITH CUBE",
      """
        |SELECT `gen_attr` AS `key`, `gen_attr` AS `value`, `gen_attr` AS `count(value)`
        |FROM (SELECT `gen_attr` AS `gen_attr`, `gen_attr` AS `gen_attr`,
        |             count(`gen_attr`) AS `gen_attr`
        |      FROM (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |            FROM `default`.`parquet_t1`)
        |            AS gen_subquery_0
        |      GROUP BY `gen_attr`, `gen_attr`
        |      GROUPING SETS((`gen_attr`, `gen_attr`), (`gen_attr`), (`gen_attr`), ()))
        |      AS gen_subquery_1
      """.stripMargin)
  }

  test("rollup/cube #3") {
    checkHiveQl(
      "SELECT key, count(value), grouping_id() FROM parquet_t1 GROUP BY key, value WITH ROLLUP",
      """
        |SELECT `gen_attr` AS `key`, `gen_attr` AS `count(value)`, `gen_attr` AS `grouping_id()`
        |FROM (SELECT `gen_attr` AS `gen_attr`, count(`gen_attr`) AS `gen_attr`,
        |             grouping_id() AS `gen_attr`
        |      FROM (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |            FROM `default`.`parquet_t1`)
        |            AS gen_subquery_0
        |      GROUP BY `gen_attr`, `gen_attr`
        |      GROUPING SETS((`gen_attr`, `gen_attr`), (`gen_attr`), ()))
        |      AS gen_subquery_1
      """.stripMargin)

    checkHiveQl(
      "SELECT key, count(value), grouping_id() FROM parquet_t1 GROUP BY key, value WITH CUBE",
      """
        |SELECT `gen_attr` AS `key`, `gen_attr` AS `count(value)`, `gen_attr` AS `grouping_id()`
        |FROM (SELECT `gen_attr` AS `gen_attr`, count(`gen_attr`) AS `gen_attr`,
        |             grouping_id() AS `gen_attr`
        |      FROM (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |            FROM `default`.`parquet_t1`)
        |            AS gen_subquery_0
        |      GROUP BY `gen_attr`, `gen_attr`
        |      GROUPING SETS((`gen_attr`, `gen_attr`), (`gen_attr`), (`gen_attr`), ()))
        |      AS gen_subquery_1
      """.stripMargin)
  }

  test("rollup/cube #4") {
    checkHiveQl(
      s"""
        |SELECT count(*) as cnt, key % 5 as k1, key - 5 as k2, grouping_id() FROM parquet_t1
        |GROUP BY key % 5, key - 5 WITH ROLLUP
      """.stripMargin,
      """
        |SELECT `gen_attr` AS `cnt`, `gen_attr` AS `k1`, `gen_attr` AS `k2`,
        |       `gen_attr` AS `grouping_id()`
        |FROM (SELECT count(1) AS `gen_attr`, (`gen_attr` % CAST(5 AS BIGINT)) AS `gen_attr`,
        |             (`gen_attr` - CAST(5 AS BIGINT)) AS `gen_attr`, grouping_id() AS `gen_attr`
        |      FROM (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |            FROM `default`.`parquet_t1`)
        |            AS gen_subquery_0
        |      GROUP BY (`gen_attr` % CAST(5 AS BIGINT)), (`gen_attr` - CAST(5 AS BIGINT))
        |      GROUPING SETS(((`gen_attr` % CAST(5 AS BIGINT)), (`gen_attr` - CAST(5 AS BIGINT))),
        |                    ((`gen_attr` % CAST(5 AS BIGINT))), ()))
        |      AS gen_subquery_1
      """.stripMargin)

    checkHiveQl(
      s"""
        |SELECT count(*) as cnt, key % 5 as k1, key - 5 as k2, grouping_id() FROM parquet_t1
        |GROUP BY key % 5, key - 5 WITH CUBE
      """.stripMargin,
      """
        |SELECT `gen_attr` AS `cnt`, `gen_attr` AS `k1`, `gen_attr` AS `k2`,
        |       `gen_attr` AS `grouping_id()`
        |FROM (SELECT count(1) AS `gen_attr`, (`gen_attr` % CAST(5 AS BIGINT)) AS `gen_attr`,
        |             (`gen_attr` - CAST(5 AS BIGINT)) AS `gen_attr`, grouping_id() AS `gen_attr`
        |      FROM (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |            FROM `default`.`parquet_t1`)
        |            AS gen_subquery_0
        |      GROUP BY (`gen_attr` % CAST(5 AS BIGINT)), (`gen_attr` - CAST(5 AS BIGINT))
        |      GROUPING SETS(((`gen_attr` % CAST(5 AS BIGINT)), (`gen_attr` - CAST(5 AS BIGINT))),
        |                    ((`gen_attr` % CAST(5 AS BIGINT))), ((`gen_attr` - CAST(5 AS BIGINT))),
        |                    ()))
        |      AS gen_subquery_1
      """.stripMargin)
  }

  test("rollup/cube #5") {
    checkHiveQl(
      s"""
        |SELECT count(*) AS cnt, key % 5 AS k1, key - 5 AS k2, grouping_id(key % 5, key - 5) AS k3
        |FROM (SELECT key, key%2, key - 5 FROM parquet_t1) t GROUP BY key%5, key-5
        |WITH ROLLUP
      """.stripMargin,
      """
        |SELECT `gen_attr` AS `cnt`, `gen_attr` AS `k1`, `gen_attr` AS `k2`, `gen_attr` AS `k3`
        |FROM (SELECT count(1) AS `gen_attr`, (`gen_attr` % CAST(5 AS BIGINT)) AS `gen_attr`,
        |             (`gen_attr` - CAST(5 AS BIGINT)) AS `gen_attr`, grouping_id() AS `gen_attr`
        |      FROM (SELECT `gen_attr`, (`gen_attr` % CAST(2 AS BIGINT)) AS `gen_attr`,
        |                   (`gen_attr` - CAST(5 AS BIGINT)) AS `gen_attr`
        |            FROM (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |                  FROM `default`.`parquet_t1`)
        |                  AS gen_subquery_0)
        |            AS t
        |      GROUP BY (`gen_attr` % CAST(5 AS BIGINT)), (`gen_attr` - CAST(5 AS BIGINT))
        |      GROUPING SETS(((`gen_attr` % CAST(5 AS BIGINT)), (`gen_attr` - CAST(5 AS BIGINT))),
        |                    ((`gen_attr` % CAST(5 AS BIGINT))), ()))
        |      AS gen_subquery_1
      """.stripMargin)

    checkHiveQl(
      s"""
        |SELECT count(*) AS cnt, key % 5 AS k1, key - 5 AS k2, grouping_id(key % 5, key - 5) AS k3
        |FROM (SELECT key, key % 2, key - 5 FROM parquet_t1) t GROUP BY key % 5, key - 5
        |WITH CUBE
      """.stripMargin,
      """
        |SELECT `gen_attr` AS `cnt`, `gen_attr` AS `k1`, `gen_attr` AS `k2`, `gen_attr` AS `k3`
        |FROM (SELECT count(1) AS `gen_attr`, (`gen_attr` % CAST(5 AS BIGINT)) AS `gen_attr`,
        |             (`gen_attr` - CAST(5 AS BIGINT)) AS `gen_attr`, grouping_id() AS `gen_attr`
        |      FROM (SELECT `gen_attr`, (`gen_attr` % CAST(2 AS BIGINT)) AS `gen_attr`,
        |                   (`gen_attr` - CAST(5 AS BIGINT)) AS `gen_attr`
        |            FROM (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |                  FROM `default`.`parquet_t1`)
        |                  AS gen_subquery_0)
        |            AS t
        |      GROUP BY (`gen_attr` % CAST(5 AS BIGINT)), (`gen_attr` - CAST(5 AS BIGINT))
        |      GROUPING SETS(((`gen_attr` % CAST(5 AS BIGINT)), (`gen_attr` - CAST(5 AS BIGINT))),
        |                    ((`gen_attr` % CAST(5 AS BIGINT))), ((`gen_attr` - CAST(5 AS BIGINT))),
        |                    ()))
        |      AS gen_subquery_1
      """.stripMargin)
  }

  test("rollup/cube #6") {
    checkHiveQl("SELECT a, b, sum(c) FROM parquet_t2 GROUP BY ROLLUP(a, b) ORDER BY a, b",
      """
        |SELECT `gen_attr` AS `a`, `gen_attr` AS `b`, `gen_attr` AS `sum(c)`
        |FROM (SELECT `gen_attr` AS `gen_attr`, `gen_attr` AS `gen_attr`,
        |             sum(`gen_attr`) AS `gen_attr`
        |      FROM (SELECT `a` AS `gen_attr`, `b` AS `gen_attr`, `c` AS `gen_attr`,
        |                   `d` AS `gen_attr`
        |            FROM `default`.`parquet_t2`)
        |            AS gen_subquery_0
        |      GROUP BY `gen_attr`, `gen_attr`
        |      GROUPING SETS((`gen_attr`, `gen_attr`), (`gen_attr`), ())
        |      ORDER BY `gen_attr` ASC, `gen_attr` ASC)
        |      AS gen_subquery_1
      """.stripMargin)

    checkHiveQl("SELECT a, b, sum(c) FROM parquet_t2 GROUP BY CUBE(a, b) ORDER BY a, b",
      """
        |SELECT `gen_attr` AS `a`, `gen_attr` AS `b`, `gen_attr` AS `sum(c)`
        |FROM (SELECT `gen_attr` AS `gen_attr`, `gen_attr` AS `gen_attr`,
        |             sum(`gen_attr`) AS `gen_attr`
        |      FROM (SELECT `a` AS `gen_attr`, `b` AS `gen_attr`, `c` AS `gen_attr`,
        |                   `d` AS `gen_attr`
        |            FROM `default`.`parquet_t2`)
        |            AS gen_subquery_0
        |      GROUP BY `gen_attr`, `gen_attr`
        |      GROUPING SETS((`gen_attr`, `gen_attr`), (`gen_attr`), (`gen_attr`), ())
        |      ORDER BY `gen_attr` ASC, `gen_attr` ASC)
        |      AS gen_subquery_1
      """.stripMargin)

    checkHiveQl("SELECT a, b, sum(a) FROM parquet_t2 GROUP BY ROLLUP(a, b) ORDER BY a, b",
      """
        |SELECT `gen_attr` AS `a`, `gen_attr` AS `b`, `gen_attr` AS `sum(a)`
        |FROM (SELECT `gen_attr` AS `gen_attr`, `gen_attr` AS `gen_attr`,
        |             sum(`gen_attr`) AS `gen_attr`
        |      FROM (SELECT `a` AS `gen_attr`, `b` AS `gen_attr`, `c` AS `gen_attr`,
        |                   `d` AS `gen_attr`
        |            FROM `default`.`parquet_t2`)
        |            AS gen_subquery_0
        |      GROUP BY `gen_attr`, `gen_attr`
        |      GROUPING SETS((`gen_attr`, `gen_attr`), (`gen_attr`), ())
        |      ORDER BY `gen_attr` ASC, `gen_attr` ASC)
        |      AS gen_subquery_1
      """.stripMargin)

    checkHiveQl("SELECT a, b, sum(a) FROM parquet_t2 GROUP BY CUBE(a, b) ORDER BY a, b",
      """
        |SELECT `gen_attr` AS `a`, `gen_attr` AS `b`, `gen_attr` AS `sum(a)`
        |FROM (SELECT `gen_attr` AS `gen_attr`, `gen_attr` AS `gen_attr`,
        |             sum(`gen_attr`) AS `gen_attr`
        |      FROM (SELECT `a` AS `gen_attr`, `b` AS `gen_attr`, `c` AS `gen_attr`,
        |                   `d` AS `gen_attr`
        |            FROM `default`.`parquet_t2`)
        |            AS gen_subquery_0
        |      GROUP BY `gen_attr`, `gen_attr`
        |      GROUPING SETS((`gen_attr`, `gen_attr`), (`gen_attr`), (`gen_attr`), ())
        |      ORDER BY `gen_attr` ASC, `gen_attr` ASC) AS gen_subquery_1
      """.stripMargin)

    checkHiveQl("SELECT a + b, b, sum(a - b) FROM parquet_t2 GROUP BY a + b, b WITH ROLLUP",
      """
        |SELECT `gen_attr` AS `(a + b)`, `gen_attr` AS `b`, `gen_attr` AS `sum((a - b))`
        |FROM (SELECT (`gen_attr` + `gen_attr`) AS `gen_attr`, `gen_attr` AS `gen_attr`,
        |             sum((`gen_attr` - `gen_attr`)) AS `gen_attr`
        |      FROM (SELECT `a` AS `gen_attr`, `b` AS `gen_attr`, `c` AS `gen_attr`,
        |                   `d` AS `gen_attr`
        |            FROM `default`.`parquet_t2`)
        |            AS gen_subquery_0
        |      GROUP BY (`gen_attr` + `gen_attr`), `gen_attr`
        |      GROUPING SETS(((`gen_attr` + `gen_attr`), `gen_attr`),
        |                    ((`gen_attr` + `gen_attr`)), ()))
        |      AS gen_subquery_1
      """.stripMargin)
    checkHiveQl("SELECT a + b, b, sum(a - b) FROM parquet_t2 GROUP BY a + b, b WITH CUBE")
  }

  test("rollup/cube #7") {
    checkHiveQl("SELECT a, b, grouping_id(a, b) FROM parquet_t2 GROUP BY cube(a, b)",
      """
        |SELECT `gen_attr` AS `a`, `gen_attr` AS `b`, `gen_attr` AS `grouping_id(a, b)`
        |FROM (SELECT `gen_attr` AS `gen_attr`, `gen_attr` AS `gen_attr`,
        |             grouping_id() AS `gen_attr`
        |      FROM (SELECT `a` AS `gen_attr`, `b` AS `gen_attr`, `c` AS `gen_attr`,
        |                   `d` AS `gen_attr`
        |            FROM `default`.`parquet_t2`)
        |            AS gen_subquery_0
        |      GROUP BY `gen_attr`, `gen_attr`
        |      GROUPING SETS((`gen_attr`, `gen_attr`), (`gen_attr`), (`gen_attr`), ()))
        |      AS gen_subquery_1
      """.stripMargin)

    checkHiveQl("SELECT a, b, grouping(b) FROM parquet_t2 GROUP BY cube(a, b)",
      """
        |SELECT `gen_attr` AS `a`, `gen_attr` AS `b`, `gen_attr` AS `grouping(b)`
        |FROM (SELECT `gen_attr` AS `gen_attr`, `gen_attr` AS `gen_attr`,
        |             grouping(`gen_attr`) AS `gen_attr`
        |      FROM (SELECT `a` AS `gen_attr`, `b` AS `gen_attr`, `c` AS `gen_attr`,
        |                   `d` AS `gen_attr`
        |            FROM `default`.`parquet_t2`)
        |            AS gen_subquery_0
        |      GROUP BY `gen_attr`, `gen_attr`
        |      GROUPING SETS((`gen_attr`, `gen_attr`), (`gen_attr`), (`gen_attr`), ()))
        |      AS gen_subquery_1
      """.stripMargin)

    checkHiveQl("SELECT a, b, grouping(a) FROM parquet_t2 GROUP BY cube(a, b)",
      """
        |SELECT `gen_attr` AS `a`, `gen_attr` AS `b`, `gen_attr` AS `grouping(a)`
        |FROM (SELECT `gen_attr` AS `gen_attr`, `gen_attr` AS `gen_attr`,
        |             grouping(`gen_attr`) AS `gen_attr`
        |      FROM (SELECT `a` AS `gen_attr`, `b` AS `gen_attr`, `c` AS `gen_attr`,
        |                   `d` AS `gen_attr`
        |            FROM `default`.`parquet_t2`)
        |            AS gen_subquery_0
        |      GROUP BY `gen_attr`, `gen_attr`
        |      GROUPING SETS((`gen_attr`, `gen_attr`), (`gen_attr`), (`gen_attr`), ()))
        |      AS gen_subquery_1
      """.stripMargin)
  }

  test("rollup/cube #8") {
    // grouping_id() is part of another expression
    checkHiveQl(
      s"""
         |SELECT hkey AS k1, value - 5 AS k2, hash(grouping_id()) AS hgid
         |FROM (SELECT hash(key) as hkey, key as value FROM parquet_t1) t GROUP BY hkey, value-5
         |WITH ROLLUP
      """.stripMargin,
      """
        |SELECT `gen_attr` AS `k1`, `gen_attr` AS `k2`, `gen_attr` AS `hgid`
        |FROM (SELECT `gen_attr` AS `gen_attr`, (`gen_attr` - CAST(5 AS BIGINT)) AS `gen_attr`,
        |             hash(grouping_id()) AS `gen_attr`
        |      FROM (SELECT hash(`gen_attr`) AS `gen_attr`, `gen_attr` AS `gen_attr`
        |            FROM (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |                  FROM `default`.`parquet_t1`)
        |                  AS gen_subquery_0)
        |            AS t
        |      GROUP BY `gen_attr`, (`gen_attr` - CAST(5 AS BIGINT))
        |      GROUPING SETS((`gen_attr`, (`gen_attr` - CAST(5 AS BIGINT))), (`gen_attr`), ()))
        |      AS gen_subquery_1
      """.stripMargin)

    checkHiveQl(
      s"""
         |SELECT hkey AS k1, value - 5 AS k2, hash(grouping_id()) AS hgid
         |FROM (SELECT hash(key) as hkey, key as value FROM parquet_t1) t GROUP BY hkey, value-5
         |WITH CUBE
      """.stripMargin,
      """
        |SELECT `gen_attr` AS `k1`, `gen_attr` AS `k2`, `gen_attr` AS `hgid`
        |FROM (SELECT `gen_attr` AS `gen_attr`, (`gen_attr` - CAST(5 AS BIGINT)) AS `gen_attr`,
        |             hash(grouping_id()) AS `gen_attr`
        |      FROM (SELECT hash(`gen_attr`) AS `gen_attr`, `gen_attr` AS `gen_attr`
        |            FROM (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |                  FROM `default`.`parquet_t1`)
        |                  AS gen_subquery_0)
        |            AS t
        |      GROUP BY `gen_attr`, (`gen_attr` - CAST(5 AS BIGINT))
        |      GROUPING SETS((`gen_attr`, (`gen_attr` - CAST(5 AS BIGINT))), (`gen_attr`),
        |                   ((`gen_attr` - CAST(5 AS BIGINT))), ()))
        |      AS gen_subquery_1
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
      """.stripMargin,
      """
        |SELECT `gen_attr` AS `(key - CAST(5 AS BIGINT))`, `gen_attr` AS `cnt`,
        |       `gen_attr` AS `sum(cnt)`
        |FROM (SELECT (`gen_attr` - CAST(5 AS BIGINT)) AS `gen_attr`, `gen_attr` AS `gen_attr`,
        |             sum(`gen_attr`) AS `gen_attr`
        |      FROM (SELECT `gen_attr`, count(1) AS `gen_attr`
        |            FROM (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |                  FROM `default`.`parquet_t1`)
        |                  AS gen_subquery_0
        |           INNER JOIN (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |                       FROM `default`.`parquet_t1`)
        |                       AS gen_subquery_1
        |           ON (`gen_attr` = `gen_attr`)
        |           GROUP BY `gen_attr`)
        |           AS t
        |      GROUP BY `gen_attr`, (`gen_attr` - CAST(5 AS BIGINT))
        |      GROUPING SETS((`gen_attr`, (`gen_attr` - CAST(5 AS BIGINT))), (`gen_attr`), ()))
        |      AS gen_subquery_2
      """.stripMargin)

    checkHiveQl(
      s"""
         |SELECT t.key - 5, cnt, SUM(cnt)
         |FROM (SELECT x.key, COUNT(*) as cnt
         |FROM parquet_t1 x JOIN parquet_t1 y ON x.key = y.key GROUP BY x.key) t
         |GROUP BY cnt, t.key - 5
         |WITH CUBE
      """.stripMargin,
      """
        |SELECT `gen_attr` AS `(key - CAST(5 AS BIGINT))`, `gen_attr` AS `cnt`,
        |       `gen_attr` AS `sum(cnt)`
        |FROM (SELECT (`gen_attr` - CAST(5 AS BIGINT)) AS `gen_attr`, `gen_attr` AS `gen_attr`,
        |             sum(`gen_attr`) AS `gen_attr`
        |      FROM (SELECT `gen_attr`, count(1) AS `gen_attr`
        |            FROM (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |                  FROM `default`.`parquet_t1`)
        |                  AS gen_subquery_0
        |           INNER JOIN (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |                       FROM `default`.`parquet_t1`)
        |                       AS gen_subquery_1
        |           ON (`gen_attr` = `gen_attr`)
        |           GROUP BY `gen_attr`)
        |           AS t
        |      GROUP BY `gen_attr`, (`gen_attr` - CAST(5 AS BIGINT))
        |      GROUPING SETS((`gen_attr`, (`gen_attr` - CAST(5 AS BIGINT))), (`gen_attr`),
        |                   ((`gen_attr` - CAST(5 AS BIGINT))), ()))
        |      AS gen_subquery_2
      """.stripMargin)
  }

  test("grouping sets #1") {
    checkHiveQl(
      s"""
         |SELECT count(*) AS cnt, key % 5 AS k1, key - 5 AS k2, grouping_id() AS k3
         |FROM (SELECT key, key % 2, key - 5 FROM parquet_t1) t GROUP BY key % 5, key - 5
         |GROUPING SETS (key % 5, key - 5)
      """.stripMargin,
      """
        |SELECT `gen_attr` AS `cnt`, `gen_attr` AS `k1`, `gen_attr` AS `k2`, `gen_attr` AS `k3`
        |FROM (SELECT count(1) AS `gen_attr`, (`gen_attr` % CAST(5 AS BIGINT)) AS `gen_attr`,
        |             (`gen_attr` - CAST(5 AS BIGINT)) AS `gen_attr`, grouping_id() AS `gen_attr`
        |      FROM (SELECT `gen_attr`, (`gen_attr` % CAST(2 AS BIGINT)) AS `gen_attr`,
        |                   (`gen_attr` - CAST(5 AS BIGINT)) AS `gen_attr`
        |            FROM (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |                  FROM `default`.`parquet_t1`)
        |                  AS gen_subquery_0)
        |            AS t
        |      GROUP BY (`gen_attr` % CAST(5 AS BIGINT)), (`gen_attr` - CAST(5 AS BIGINT))
        |      GROUPING SETS(((`gen_attr` % CAST(5 AS BIGINT))),
        |                    ((`gen_attr` - CAST(5 AS BIGINT)))))
        |      AS gen_subquery_1
      """.stripMargin)
  }

  test("grouping sets #2") {
    checkHiveQl(
      "SELECT a, b, sum(c) FROM parquet_t2 GROUP BY a, b GROUPING SETS (a, b) ORDER BY a, b",
      """
        |SELECT `gen_attr` AS `a`, `gen_attr` AS `b`, `gen_attr` AS `sum(c)`
        |FROM (SELECT `gen_attr` AS `gen_attr`, `gen_attr` AS `gen_attr`,
        |             sum(`gen_attr`) AS `gen_attr`
        |      FROM (SELECT `a` AS `gen_attr`, `b` AS `gen_attr`, `c` AS `gen_attr`,
        |                   `d` AS `gen_attr`
        |            FROM `default`.`parquet_t2`)
        |            AS gen_subquery_0
        |      GROUP BY `gen_attr`, `gen_attr`
        |      GROUPING SETS((`gen_attr`), (`gen_attr`))
        |      ORDER BY `gen_attr` ASC, `gen_attr` ASC)
        |      AS gen_subquery_1
      """.stripMargin)

    checkHiveQl(
      "SELECT a, b, sum(c) FROM parquet_t2 GROUP BY a, b GROUPING SETS (a) ORDER BY a, b",
      """
        |SELECT `gen_attr` AS `a`, `gen_attr` AS `b`, `gen_attr` AS `sum(c)`
        |FROM (SELECT `gen_attr` AS `gen_attr`, `gen_attr` AS `gen_attr`,
        |             sum(`gen_attr`) AS `gen_attr`
        |      FROM (SELECT `a` AS `gen_attr`, `b` AS `gen_attr`, `c` AS `gen_attr`,
        |                   `d` AS `gen_attr`
        |            FROM `default`.`parquet_t2`)
        |            AS gen_subquery_0
        |      GROUP BY `gen_attr`, `gen_attr`
        |      GROUPING SETS((`gen_attr`))
        |      ORDER BY `gen_attr` ASC, `gen_attr` ASC)
        |      AS gen_subquery_1
      """.stripMargin)

    checkHiveQl(
      "SELECT a, b, sum(c) FROM parquet_t2 GROUP BY a, b GROUPING SETS (b) ORDER BY a, b",
      """
        |SELECT `gen_attr` AS `a`, `gen_attr` AS `b`, `gen_attr` AS `sum(c)`
        |FROM (SELECT `gen_attr` AS `gen_attr`, `gen_attr` AS `gen_attr`,
        |             sum(`gen_attr`) AS `gen_attr`
        |      FROM (SELECT `a` AS `gen_attr`, `b` AS `gen_attr`, `c` AS `gen_attr`,
        |                   `d` AS `gen_attr`
        |            FROM `default`.`parquet_t2`)
        |            AS gen_subquery_0
        |      GROUP BY `gen_attr`, `gen_attr`
        |      GROUPING SETS((`gen_attr`))
        |      ORDER BY `gen_attr` ASC, `gen_attr` ASC)
        |      AS gen_subquery_1
      """.stripMargin)

    checkHiveQl(
      "SELECT a, b, sum(c) FROM parquet_t2 GROUP BY a, b GROUPING SETS (()) ORDER BY a, b",
      """
        |SELECT `gen_attr` AS `a`, `gen_attr` AS `b`, `gen_attr` AS `sum(c)`
        |FROM (SELECT `gen_attr` AS `gen_attr`, `gen_attr` AS `gen_attr`,
        |             sum(`gen_attr`) AS `gen_attr`
        |      FROM (SELECT `a` AS `gen_attr`, `b` AS `gen_attr`, `c` AS `gen_attr`,
        |                   `d` AS `gen_attr`
        |            FROM `default`.`parquet_t2`)
        |            AS gen_subquery_0
        |      GROUP BY `gen_attr`, `gen_attr`
        |      GROUPING SETS(())
        |      ORDER BY `gen_attr` ASC, `gen_attr` ASC)
        |      AS gen_subquery_1
      """.stripMargin)

    checkHiveQl(
      s"""
         |SELECT a, b, sum(c) FROM parquet_t2 GROUP BY a, b
         |GROUPING SETS ((), (a), (a, b)) ORDER BY a, b
      """.stripMargin,
      """
        |SELECT `gen_attr` AS `a`, `gen_attr` AS `b`, `gen_attr` AS `sum(c)`
        |FROM (SELECT `gen_attr` AS `gen_attr`, `gen_attr` AS `gen_attr`,
        |             sum(`gen_attr`) AS `gen_attr`
        |      FROM (SELECT `a` AS `gen_attr`, `b` AS `gen_attr`, `c` AS `gen_attr`,
        |                   `d` AS `gen_attr`
        |            FROM `default`.`parquet_t2`)
        |            AS gen_subquery_0
        |      GROUP BY `gen_attr`, `gen_attr`
        |      GROUPING SETS((), (`gen_attr`), (`gen_attr`, `gen_attr`))
        |      ORDER BY `gen_attr` ASC, `gen_attr` ASC)
        |      AS gen_subquery_1
      """.stripMargin)
  }

  test("cluster by") {
    checkHiveQl("SELECT id FROM parquet_t0 CLUSTER BY id",
      """
        |SELECT `gen_attr` AS `id`
        |FROM (SELECT `gen_attr`
        |      FROM (SELECT `id` AS `gen_attr`
        |            FROM `default`.`parquet_t0`)
        |            AS gen_subquery_0
        |      CLUSTER BY `gen_attr`)
        |      AS parquet_t0
      """.stripMargin)
  }

  test("distribute by") {
    checkHiveQl("SELECT id FROM parquet_t0 DISTRIBUTE BY id",
      """
        |SELECT `gen_attr` AS `id`
        |FROM (SELECT `gen_attr`
        |      FROM (SELECT `id` AS `gen_attr`
        |            FROM `default`.`parquet_t0`)
        |            AS gen_subquery_0
        |      DISTRIBUTE BY `gen_attr`)
        |      AS parquet_t0
      """.stripMargin)
  }

  test("distribute by with sort by") {
    checkHiveQl("SELECT id FROM parquet_t0 DISTRIBUTE BY id SORT BY id",
      """
        |SELECT `gen_attr` AS `id`
        |FROM (SELECT `gen_attr`
        |      FROM (SELECT `id` AS `gen_attr`
        |            FROM `default`.`parquet_t0`)
        |            AS gen_subquery_0
        |      CLUSTER BY `gen_attr`)
        |      AS parquet_t0
      """.stripMargin)
  }

  test("SPARK-13720: sort by after having") {
    checkHiveQl("SELECT COUNT(value) FROM parquet_t1 GROUP BY key HAVING MAX(key) > 0 SORT BY key",
      """
        |SELECT `gen_attr` AS `count(value)`
        |FROM (SELECT `gen_attr`
        |      FROM (SELECT `gen_attr`, `gen_attr`
        |            FROM (SELECT count(`gen_attr`) AS `gen_attr`, max(`gen_attr`) AS `gen_attr`,
        |                         `gen_attr`
        |                  FROM (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |                        FROM `default`.`parquet_t1`)
        |                        AS gen_subquery_0
        |                  GROUP BY `gen_attr`
        |                  HAVING (`gen_attr` > CAST(0 AS BIGINT)))
        |                  AS gen_subquery_1
        |            SORT BY `gen_attr` ASC)
        |            AS gen_subquery_2)
        |      AS gen_subquery_3
      """.stripMargin)
  }

  test("distinct aggregation") {
    checkHiveQl("SELECT COUNT(DISTINCT id) FROM parquet_t0",
      """
        |SELECT `gen_attr` AS `count(DISTINCT id)`
        |FROM (SELECT count(DISTINCT `gen_attr`) AS `gen_attr`
        |      FROM (SELECT `id` AS `gen_attr`
        |            FROM `default`.`parquet_t0`)
        |            AS gen_subquery_0)
        |      AS gen_subquery_1
      """.stripMargin)
  }

  test("TABLESAMPLE") {
    // Project [id#2L]
    // +- Sample 0.0, 1.0, false, ...
    //    +- Subquery s
    //       +- Subquery parquet_t0
    //          +- Relation[id#2L] ParquetRelation
    checkHiveQl("SELECT s.id FROM parquet_t0 TABLESAMPLE(100 PERCENT) s",
      """
        |SELECT `gen_attr` AS `id`
        |FROM (SELECT `gen_attr`
        |      FROM (SELECT `id` AS `gen_attr`
        |            FROM `default`.`parquet_t0`
        |            TABLESAMPLE(100.0 PERCENT))
        |            AS gen_subquery_0)
        |      AS s
      """.stripMargin)

    // Project [id#2L]
    // +- Sample 0.0, 1.0, false, ...
    //    +- Subquery parquet_t0
    //       +- Relation[id#2L] ParquetRelation
    checkHiveQl("SELECT * FROM parquet_t0 TABLESAMPLE(100 PERCENT)",
      """
        |SELECT `gen_attr` AS `id`
        |FROM (SELECT `gen_attr`
        |      FROM (SELECT `id` AS `gen_attr`
        |            FROM `default`.`parquet_t0`
        |            TABLESAMPLE(100.0 PERCENT))
        |            AS gen_subquery_0)
        |      AS parquet_t0
      """.stripMargin)

    // Project [id#21L]
    // +- Sample 0.0, 1.0, false, ...
    //    +- MetastoreRelation default, t0, Some(s)
    checkHiveQl("SELECT s.id FROM t0 TABLESAMPLE(100 PERCENT) s",
      """
        |SELECT `gen_attr` AS `id`
        |FROM (SELECT `gen_attr`
        |      FROM (SELECT `id` AS `gen_attr`
        |            FROM `default`.`t0`
        |            TABLESAMPLE(100.0 PERCENT))
        |            AS gen_subquery_0)
        |      AS s
      """.stripMargin)

    // Project [id#24L]
    // +- Sample 0.0, 1.0, false, ...
    //    +- MetastoreRelation default, t0, None
    checkHiveQl("SELECT * FROM t0 TABLESAMPLE(100 PERCENT)",
      """
        |SELECT `gen_attr` AS `id`
        |FROM (SELECT `gen_attr`
        |      FROM (SELECT `id` AS `gen_attr`
        |            FROM `default`.`t0`
        |            TABLESAMPLE(100.0 PERCENT))
        |            AS gen_subquery_0)
        |      AS t0
      """.stripMargin)

    // When a sampling fraction is not 100%, the returned results are random.
    // Thus, added an always-false filter here to check if the generated plan can be successfully
    // executed.
    checkHiveQl("SELECT s.id FROM parquet_t0 TABLESAMPLE(0.1 PERCENT) s WHERE 1=0",
      """
        |SELECT `gen_attr` AS `id`
        |FROM (SELECT `gen_attr`
        |      FROM (SELECT `id` AS `gen_attr`
        |            FROM `default`.`parquet_t0`
        |            TABLESAMPLE(0.1 PERCENT))
        |            AS gen_subquery_0
        |      WHERE (1 = 0))
        |      AS s
      """.stripMargin)

    checkHiveQl("SELECT * FROM parquet_t0 TABLESAMPLE(0.1 PERCENT) WHERE 1=0",
      """
        |SELECT `gen_attr` AS `id`
        |FROM (SELECT `gen_attr`
        |      FROM (SELECT `id` AS `gen_attr`
        |            FROM `default`.`parquet_t0`
        |            TABLESAMPLE(0.1 PERCENT))
        |            AS gen_subquery_0
        |      WHERE (1 = 0))
        |      AS parquet_t0
      """.stripMargin)
  }

  test("multi-distinct columns") {
    checkHiveQl("SELECT a, COUNT(DISTINCT b), COUNT(DISTINCT c), SUM(d) FROM parquet_t2 GROUP BY a",
      """
        |SELECT `gen_attr` AS `a`, `gen_attr` AS `count(DISTINCT b)`,
        |       `gen_attr` AS `count(DISTINCT c)`, `gen_attr` AS `sum(d)`
        |FROM (SELECT `gen_attr`, count(DISTINCT `gen_attr`) AS `gen_attr`,
        |             count(DISTINCT `gen_attr`) AS `gen_attr`, sum(`gen_attr`) AS `gen_attr`
        |      FROM (SELECT `a` AS `gen_attr`, `b` AS `gen_attr`, `c` AS `gen_attr`,
        |                   `d` AS `gen_attr`
        |            FROM `default`.`parquet_t2`)
        |            AS gen_subquery_0
        |      GROUP BY `gen_attr`)
        |      AS parquet_t2
      """.stripMargin)
  }

  test("persisted data source relations") {
    Seq("orc", "json", "parquet").foreach { format =>
      val tableName = s"${format}_parquet_t0"
      withTable(tableName) {
        spark.range(10).write.format(format).saveAsTable(tableName)
        checkHiveQl(s"SELECT id FROM $tableName",
          s"""
            |SELECT `gen_attr` AS `id`
            |FROM (SELECT `gen_attr`
            |      FROM (SELECT `id` AS `gen_attr`
            |            FROM `default`.`$tableName`)
            |            AS gen_subquery_0)
            |      AS $tableName
          """.stripMargin)
      }
    }
  }

  test("script transformation - schemaless") {
    checkHiveQl("SELECT TRANSFORM (a, b, c, d) USING 'cat' FROM parquet_t2",
      """
        |SELECT `gen_attr` AS `key`, `gen_attr` AS `value`
        |FROM (SELECT TRANSFORM (`gen_attr`, `gen_attr`, `gen_attr`, `gen_attr`)
        |      USING 'cat'
        |      AS (`gen_attr` string, `gen_attr` string)
        |      FROM (SELECT `a` AS `gen_attr`, `b` AS `gen_attr`, `c` AS `gen_attr`,
        |                   `d` AS `gen_attr`
        |            FROM `default`.`parquet_t2`)
        |            AS gen_subquery_0)
        |      AS gen_subquery_1
      """.stripMargin)

    checkHiveQl("SELECT TRANSFORM (*) USING 'cat' FROM parquet_t2",
      """
        |SELECT `gen_attr` AS `key`, `gen_attr` AS `value`
        |FROM (SELECT TRANSFORM (`gen_attr`, `gen_attr`, `gen_attr`, `gen_attr`)
        |      USING 'cat'
        |      AS (`gen_attr` string, `gen_attr` string)
        |      FROM (SELECT `a` AS `gen_attr`, `b` AS `gen_attr`, `c` AS `gen_attr`,
        |                   `d` AS `gen_attr`
        |            FROM `default`.`parquet_t2`)
        |            AS gen_subquery_0)
        |      AS gen_subquery_1
      """.stripMargin)
  }

  test("script transformation - alias list") {
    // scalastyle:off
    checkHiveQl("SELECT TRANSFORM (a, b, c, d) USING 'cat' AS (d1, d2, d3, d4) FROM parquet_t2",
      """
        |SELECT `gen_attr` AS `d1`, `gen_attr` AS `d2`, `gen_attr` AS `d3`, `gen_attr` AS `d4`
        |FROM (SELECT TRANSFORM (`gen_attr`, `gen_attr`, `gen_attr`, `gen_attr`)
        |      ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        |      WITH SERDEPROPERTIES('field.delim' = '	')
        |      USING 'cat' AS (`gen_attr` string, `gen_attr` string, `gen_attr` string,
        |                      `gen_attr` string)
        |      ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        |      WITH SERDEPROPERTIES('field.delim' = '	')
        |      FROM (SELECT `a` AS `gen_attr`, `b` AS `gen_attr`, `c` AS `gen_attr`,
        |                   `d` AS `gen_attr`
        |            FROM `default`.`parquet_t2`)
        |            AS gen_subquery_0)
        |      AS gen_subquery_1
      """.stripMargin)
    // scalastyle:on
  }

  test("script transformation - alias list with type") {
    // scalastyle:off
    checkHiveQl(
      """FROM
        |(FROM parquet_t1 SELECT TRANSFORM(key, value) USING 'cat' AS (thing1 int, thing2 string)) t
        |SELECT thing1 + 1
      """.stripMargin,
      """
        |SELECT `gen_attr` AS `(thing1 + 1)`
        |FROM (SELECT (`gen_attr` + 1) AS `gen_attr`
        |      FROM (SELECT TRANSFORM (`gen_attr`, `gen_attr`)
        |            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        |            WITH SERDEPROPERTIES('field.delim' = '	')
        |            USING 'cat' AS (`gen_attr` int, `gen_attr` string)
        |            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        |            WITH SERDEPROPERTIES('field.delim' = '	')
        |            FROM (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |                  FROM `default`.`parquet_t1`)
        |                  AS gen_subquery_0)
        |            AS t)
        |      AS gen_subquery_1
      """.stripMargin)
    // scalastyle:on
  }

  test("script transformation - row format delimited clause with only one format property") {
    checkHiveQl(
      """SELECT TRANSFORM (key) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        |USING 'cat' AS (tKey) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        |FROM parquet_t1
      """.stripMargin,
      """
        |SELECT `gen_attr` AS `tKey`
        |FROM (SELECT TRANSFORM (`gen_attr`)
        |      ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        |      USING 'cat' AS (`gen_attr` string)
        |      ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        |      FROM (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |            FROM `default`.`parquet_t1`)
        |            AS gen_subquery_0)
        |      AS gen_subquery_1
      """.stripMargin)
  }

  test("script transformation - row format delimited clause with multiple format properties") {
    checkHiveQl(
      """SELECT TRANSFORM (key)
        |ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\t'
        |USING 'cat' AS (tKey)
        |ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\t'
        |FROM parquet_t1
      """.stripMargin,
      """
        |SELECT `gen_attr` AS `tKey`
        |FROM (SELECT TRANSFORM (`gen_attr`)
        |      ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\t'
        |      USING 'cat' AS (`gen_attr` string)
        |      ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\t'
        |      FROM (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |            FROM `default`.`parquet_t1`)
        |            AS gen_subquery_0)
        |      AS gen_subquery_1
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
      """.stripMargin,
      """
        |SELECT `gen_attr` AS `tKey`, `gen_attr` AS `tValue`
        |FROM (SELECT TRANSFORM (`gen_attr`, `gen_attr`)
        |      ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        |      WITH SERDEPROPERTIES('field.delim' = '|')
        |      USING 'cat' AS (`gen_attr` string, `gen_attr` string)
        |      ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        |      WITH SERDEPROPERTIES('field.delim' = '|')
        |      FROM (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |            FROM `default`.`parquet_t1`)
        |            AS gen_subquery_0)
        |      AS gen_subquery_1
      """.stripMargin)
  }

  test("script transformation - row format serde clauses without SERDEPROPERTIES") {
    checkHiveQl(
      """SELECT TRANSFORM (key, value)
        |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        |USING 'cat' AS (tKey, tValue)
        |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        |FROM parquet_t1
      """.stripMargin,
      """
        |SELECT `gen_attr` AS `tKey`, `gen_attr` AS `tValue`
        |FROM (SELECT TRANSFORM (`gen_attr`, `gen_attr`)
        |      ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        |      USING 'cat' AS (`gen_attr` string, `gen_attr` string)
        |      ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        |      FROM (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |            FROM `default`.`parquet_t1`)
        |            AS gen_subquery_0)
        |      AS gen_subquery_1
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
    // scalastyle:off line.size.limit
    checkHiveQl("SELECT MAX(value) OVER (PARTITION BY key % 3) FROM parquet_t1",
      """
        |SELECT `gen_attr` AS `max(value) OVER (PARTITION BY (key % CAST(3 AS BIGINT)) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)`
        |FROM (SELECT `gen_attr`
        |      FROM (SELECT gen_subquery_1.`gen_attr`, gen_subquery_1.`gen_attr`,
        |                   max(`gen_attr`)
        |                   OVER (PARTITION BY `gen_attr`  ROWS BETWEEN UNBOUNDED PRECEDING AND
        |                                                               UNBOUNDED FOLLOWING)
        |                   AS `gen_attr`
        |            FROM (SELECT `gen_attr`, (`gen_attr` % CAST(3 AS BIGINT)) AS `gen_attr`
        |                  FROM (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |                        FROM `default`.`parquet_t1`)
        |                        AS gen_subquery_0)
        |                  AS gen_subquery_1)
        |            AS gen_subquery_2)
        |      AS gen_subquery_3
      """.stripMargin)

    checkHiveQl(
      """
         |SELECT key, value, ROUND(AVG(key) OVER (), 2)
         |FROM parquet_t1 ORDER BY key
      """.stripMargin,
      """
        |SELECT `gen_attr` AS `key`, `gen_attr` AS `value`, `gen_attr` AS `round(avg(key) OVER ( ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 2)`
        |FROM (SELECT `gen_attr`, `gen_attr`, round(`gen_attr`, 2) AS `gen_attr`
        |      FROM (SELECT gen_subquery_1.`gen_attr`, gen_subquery_1.`gen_attr`,
        |                   avg(`gen_attr`) OVER ( ROWS BETWEEN UNBOUNDED PRECEDING AND
        |                                                       UNBOUNDED FOLLOWING) AS `gen_attr`
        |            FROM (SELECT `gen_attr`, `gen_attr`
        |                  FROM (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |                        FROM `default`.`parquet_t1`)
        |                        AS gen_subquery_0)
        |                  AS gen_subquery_1)
        |            AS gen_subquery_2
        |      ORDER BY `gen_attr` ASC)
        |      AS parquet_t1
      """.stripMargin)

    checkHiveQl(
      """
         |SELECT value, MAX(key + 1) OVER (PARTITION BY key % 5 ORDER BY key % 7) AS max
         |FROM parquet_t1
      """.stripMargin,
      """
        |SELECT `gen_attr` AS `value`, `gen_attr` AS `max`
        |FROM (SELECT `gen_attr`, `gen_attr`
        |      FROM (SELECT gen_subquery_1.`gen_attr`, gen_subquery_1.`gen_attr`,
        |                   gen_subquery_1.`gen_attr`, gen_subquery_1.`gen_attr`,
        |                   max(`gen_attr`) OVER (PARTITION BY `gen_attr` ORDER BY `gen_attr`
        |                                         ASC RANGE BETWEEN UNBOUNDED PRECEDING AND
        |                                                           CURRENT ROW) AS `gen_attr`
        |            FROM (SELECT `gen_attr`, (`gen_attr` + CAST(1 AS BIGINT)) AS `gen_attr`,
        |                         (`gen_attr` % CAST(5 AS BIGINT)) AS `gen_attr`,
        |                         (`gen_attr` % CAST(7 AS BIGINT)) AS `gen_attr`
        |                  FROM (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |                        FROM `default`.`parquet_t1`)
        |                        AS gen_subquery_0)
        |                  AS gen_subquery_1)
        |            AS gen_subquery_2)
        |      AS parquet_t1
      """.stripMargin)
    // scalastyle:on line.size.limit
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
    // scalastyle:off line.size.limit
    checkHiveQl("SELECT MAX(key) OVER (PARTITION BY key % 3) + key FROM parquet_t1",
      """
        |SELECT `gen_attr` AS `(max(key) OVER (PARTITION BY (key % CAST(3 AS BIGINT)) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) + key)`
        |FROM (SELECT (`gen_attr` + `gen_attr`) AS `gen_attr`
        |      FROM (SELECT gen_subquery_1.`gen_attr`, gen_subquery_1.`gen_attr`,
        |                   max(`gen_attr`) OVER (PARTITION BY `gen_attr`
        |                                         ROWS BETWEEN UNBOUNDED PRECEDING AND
        |                                                      UNBOUNDED FOLLOWING) AS `gen_attr`
        |            FROM (SELECT `gen_attr`, (`gen_attr` % CAST(3 AS BIGINT)) AS `gen_attr`
        |                  FROM (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |                        FROM `default`.`parquet_t1`)
        |                        AS gen_subquery_0)
        |                  AS gen_subquery_1)
        |            AS gen_subquery_2)
        |      AS gen_subquery_3
      """.stripMargin)
    // scalastyle:on line.size.limit
  }

  test("aggregate functions and window functions in one expression") {
    // scalastyle:off line.size.limit
    checkHiveQl("SELECT MAX(c) + COUNT(a) OVER () FROM parquet_t2 GROUP BY a, b",
      """
        |SELECT `gen_attr` AS `(max(c) + count(a) OVER ( ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))`
        |FROM (SELECT (`gen_attr` + `gen_attr`) AS `gen_attr`
        |      FROM (SELECT gen_subquery_1.`gen_attr`, gen_subquery_1.`gen_attr`,
        |                   count(`gen_attr`) OVER ( ROWS BETWEEN UNBOUNDED PRECEDING AND
        |                                                         UNBOUNDED FOLLOWING) AS `gen_attr`
        |            FROM (SELECT max(`gen_attr`) AS `gen_attr`, `gen_attr`
        |                  FROM (SELECT `a` AS `gen_attr`, `b` AS `gen_attr`, `c` AS `gen_attr`,
        |                               `d` AS `gen_attr`
        |                        FROM `default`.`parquet_t2`)
        |                        AS gen_subquery_0
        |                  GROUP BY `gen_attr`, `gen_attr`)
        |                  AS gen_subquery_1)
        |            AS gen_subquery_2)
        |      AS gen_subquery_3
      """.stripMargin)
    // scalastyle:on line.size.limit
  }

  test("window with different window specification") {
    checkHiveQl(
      """
         |SELECT key, value,
         |DENSE_RANK() OVER (ORDER BY key, value) AS dr,
         |MAX(value) OVER (PARTITION BY key ORDER BY key ASC) AS max
         |FROM parquet_t1
      """.stripMargin,
      """
        |SELECT `gen_attr` AS `key`, `gen_attr` AS `value`, `gen_attr` AS `dr`, `gen_attr` AS `max`
        |FROM (SELECT `gen_attr`, `gen_attr`, `gen_attr`, `gen_attr`
        |      FROM (SELECT gen_subquery_2.`gen_attr`, gen_subquery_2.`gen_attr`,
        |                   gen_subquery_2.`gen_attr`,
        |                   DENSE_RANK() OVER ( ORDER BY `gen_attr` ASC, `gen_attr` ASC
        |                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS `gen_attr`
        |            FROM (SELECT gen_subquery_1.`gen_attr`, gen_subquery_1.`gen_attr`,
        |                         max(`gen_attr`) OVER (PARTITION BY `gen_attr`
        |                                               ORDER BY `gen_attr` ASC
        |                                               RANGE BETWEEN UNBOUNDED PRECEDING AND
        |                                                             CURRENT ROW) AS `gen_attr`
        |                  FROM (SELECT `gen_attr`, `gen_attr`
        |                        FROM (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |                              FROM `default`.`parquet_t1`)
        |                              AS gen_subquery_0)
        |                        AS gen_subquery_1)
        |                  AS gen_subquery_2)
        |            AS gen_subquery_3)
        |      AS parquet_t1
      """.stripMargin)
  }

  test("window with the same window specification with aggregate + having") {
    checkHiveQl(
      """
         |SELECT key, value,
         |MAX(value) OVER (PARTITION BY key % 5 ORDER BY key DESC) AS max
         |FROM parquet_t1 GROUP BY key, value HAVING key > 5
      """.stripMargin,
      """
        |SELECT `gen_attr` AS `key`, `gen_attr` AS `value`, `gen_attr` AS `max`
        |FROM (SELECT `gen_attr`, `gen_attr`, `gen_attr`
        |      FROM (SELECT gen_subquery_1.`gen_attr`, gen_subquery_1.`gen_attr`,
        |                   gen_subquery_1.`gen_attr`,
        |                   max(`gen_attr`) OVER (PARTITION BY `gen_attr` ORDER BY `gen_attr` DESC
        |                                         RANGE BETWEEN UNBOUNDED PRECEDING AND
        |                                                       CURRENT ROW) AS `gen_attr`
        |            FROM (SELECT `gen_attr`, `gen_attr`,
        |                         (`gen_attr` % CAST(5 AS BIGINT)) AS `gen_attr`
        |                  FROM (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |                        FROM `default`.`parquet_t1`)
        |                        AS gen_subquery_0
        |                  GROUP BY `gen_attr`, `gen_attr`
        |                  HAVING (`gen_attr` > CAST(5 AS BIGINT)))
        |                  AS gen_subquery_1)
        |            AS gen_subquery_2)
        |      AS parquet_t1
      """.stripMargin)
  }

  test("window with the same window specification with aggregate functions") {
    checkHiveQl(
      """
         |SELECT key, value,
         |MAX(value) OVER (PARTITION BY key % 5 ORDER BY key) AS max
         |FROM parquet_t1 GROUP BY key, value
      """.stripMargin,
      """
        |SELECT `gen_attr` AS `key`, `gen_attr` AS `value`, `gen_attr` AS `max`
        |FROM (SELECT `gen_attr`, `gen_attr`, `gen_attr`
        |      FROM (SELECT gen_subquery_1.`gen_attr`, gen_subquery_1.`gen_attr`,
        |                   gen_subquery_1.`gen_attr`,
        |                   max(`gen_attr`) OVER (PARTITION BY `gen_attr` ORDER BY `gen_attr` ASC
        |                                         RANGE BETWEEN UNBOUNDED PRECEDING AND
        |                                         CURRENT ROW) AS `gen_attr`
        |            FROM (SELECT `gen_attr`, `gen_attr`,
        |                         (`gen_attr` % CAST(5 AS BIGINT)) AS `gen_attr`
        |                  FROM (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |                        FROM `default`.`parquet_t1`)
        |                        AS gen_subquery_0
        |                  GROUP BY `gen_attr`, `gen_attr`)
        |                  AS gen_subquery_1)
        |            AS gen_subquery_2)
        |      AS parquet_t1
      """.stripMargin)
  }

  test("window with the same window specification with aggregate") {
    checkHiveQl(
      """
         |SELECT key, value,
         |DENSE_RANK() OVER (DISTRIBUTE BY key SORT BY key, value) AS dr,
         |COUNT(key)
         |FROM parquet_t1 GROUP BY key, value
      """.stripMargin,
      """
        |SELECT `gen_attr` AS `key`, `gen_attr` AS `value`, `gen_attr` AS `dr`,
        |       `gen_attr` AS `count(key)`
        |FROM (SELECT `gen_attr`, `gen_attr`, `gen_attr`, `gen_attr`
        |      FROM (SELECT gen_subquery_1.`gen_attr`, gen_subquery_1.`gen_attr`,
        |                   gen_subquery_1.`gen_attr`,
        |                   DENSE_RANK() OVER (PARTITION BY `gen_attr`
        |                                      ORDER BY `gen_attr` ASC, `gen_attr` ASC
        |                                      ROWS BETWEEN UNBOUNDED PRECEDING AND
        |                                                   CURRENT ROW) AS `gen_attr`
        |            FROM (SELECT `gen_attr`, `gen_attr`, count(`gen_attr`) AS `gen_attr`
        |                  FROM (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |                        FROM `default`.`parquet_t1`)
        |                        AS gen_subquery_0
        |                  GROUP BY `gen_attr`, `gen_attr`)
        |                  AS gen_subquery_1)
        |            AS gen_subquery_2)
        |      AS parquet_t1
      """.stripMargin)
  }

  test("window with the same window specification without aggregate and filter") {
    checkHiveQl(
      """
         |SELECT key, value,
         |DENSE_RANK() OVER (DISTRIBUTE BY key SORT BY key, value) AS dr,
         |COUNT(key) OVER(DISTRIBUTE BY key SORT BY key, value) AS ca
         |FROM parquet_t1
      """.stripMargin,
      """
        |SELECT `gen_attr` AS `key`, `gen_attr` AS `value`, `gen_attr` AS `dr`, `gen_attr` AS `ca`
        |FROM (SELECT `gen_attr`, `gen_attr`, `gen_attr`, `gen_attr`
        |      FROM (SELECT gen_subquery_1.`gen_attr`, gen_subquery_1.`gen_attr`,
        |                   DENSE_RANK() OVER (PARTITION BY `gen_attr`
        |                                      ORDER BY `gen_attr` ASC, `gen_attr` ASC
        |                                      ROWS BETWEEN UNBOUNDED PRECEDING AND
        |                                                   CURRENT ROW) AS `gen_attr`,
        |                   count(`gen_attr`) OVER (PARTITION BY `gen_attr`
        |                                           ORDER BY `gen_attr` ASC, `gen_attr` ASC
        |                                           RANGE BETWEEN UNBOUNDED PRECEDING AND
        |                                                         CURRENT ROW) AS `gen_attr`
        |            FROM (SELECT `gen_attr`, `gen_attr`
        |                  FROM (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |                        FROM `default`.`parquet_t1`)
        |                        AS gen_subquery_0)
        |                  AS gen_subquery_1)
        |            AS gen_subquery_2)
        |      AS parquet_t1
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
    // scalastyle:off line.size.limit
    checkHiveQl(
      """
        |SELECT x.key, MAX(y.key) OVER (PARTITION BY x.key % 5 ORDER BY x.key)
        |FROM parquet_t1 x JOIN parquet_t1 y ON x.key = y.key
      """.stripMargin,
      """
        |SELECT `gen_attr` AS `key`, `gen_attr` AS `max(key) OVER (PARTITION BY (key % CAST(5 AS BIGINT)) ORDER BY key ASC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`
        |FROM (SELECT `gen_attr`, `gen_attr`
        |      FROM (SELECT gen_subquery_2.`gen_attr`, gen_subquery_2.`gen_attr`,
        |                   gen_subquery_2.`gen_attr`,
        |                   max(`gen_attr`) OVER (PARTITION BY `gen_attr` ORDER BY `gen_attr` ASC
        |                                         RANGE BETWEEN UNBOUNDED PRECEDING AND
        |                                                       CURRENT ROW) AS `gen_attr`
        |            FROM (SELECT `gen_attr`, `gen_attr`,
        |                         (`gen_attr` % CAST(5 AS BIGINT)) AS `gen_attr`
        |                  FROM (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |                        FROM `default`.`parquet_t1`)
        |                        AS gen_subquery_0
        |                  INNER JOIN (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |                              FROM `default`.`parquet_t1`)
        |                              AS gen_subquery_1
        |                  ON (`gen_attr` = `gen_attr`))
        |                  AS gen_subquery_2)
        |            AS gen_subquery_3)
        |      AS x
      """.stripMargin)
    // scalastyle:on line.size.limit
  }

  test("join 2 tables and aggregate function in having clause") {
    checkHiveQl(
      """
        |SELECT COUNT(a.value), b.KEY, a.KEY
        |FROM parquet_t1 a, parquet_t1 b
        |GROUP BY a.KEY, b.KEY
        |HAVING MAX(a.KEY) > 0
      """.stripMargin,
      """
        |SELECT `gen_attr` AS `count(value)`, `gen_attr` AS `KEY`, `gen_attr` AS `KEY`
        |FROM (SELECT `gen_attr`, `gen_attr`, `gen_attr`
        |      FROM (SELECT count(`gen_attr`) AS `gen_attr`, `gen_attr`, `gen_attr`,
        |                   max(`gen_attr`) AS `gen_attr`
        |            FROM (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |                  FROM `default`.`parquet_t1`)
        |                  AS gen_subquery_0
        |            INNER JOIN (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |                        FROM `default`.`parquet_t1`)
        |                        AS gen_subquery_1
        |            GROUP BY `gen_attr`, `gen_attr`
        |            HAVING (`gen_attr` > CAST(0 AS BIGINT)))
        |            AS gen_subquery_2)
        |      AS gen_subquery_3
      """.stripMargin)
  }

  test("generator in project list without FROM clause") {
    checkHiveQl("SELECT EXPLODE(ARRAY(1,2,3))",
      """
        |SELECT `gen_attr` AS `col`
        |FROM (SELECT `gen_attr`
        |      FROM (SELECT 1) gen_subquery_1
        |      LATERAL VIEW explode(array(1, 2, 3)) gen_subquery_2 AS `gen_attr`)
        |      AS gen_subquery_0
      """.stripMargin)

    checkHiveQl("SELECT EXPLODE(ARRAY(1,2,3)) AS val",
      """
        |SELECT `gen_attr` AS `val`
        |FROM (SELECT `gen_attr`
        |      FROM (SELECT 1) gen_subquery_1
        |      LATERAL VIEW explode(array(1, 2, 3)) gen_subquery_2 AS `gen_attr`)
        |      AS gen_subquery_0
      """.stripMargin)
  }

  test("generator in project list with non-referenced table") {
    checkHiveQl("SELECT EXPLODE(ARRAY(1,2,3)) FROM t0",
      """
        |SELECT `gen_attr` AS `col`
        |FROM (SELECT `gen_attr`
        |      FROM (SELECT `id` AS `gen_attr`
        |            FROM `default`.`t0`)
        |            AS gen_subquery_0
        |      LATERAL VIEW explode(array(1, 2, 3)) gen_subquery_2 AS `gen_attr`)
        |      AS gen_subquery_1
      """.stripMargin)

    checkHiveQl("SELECT EXPLODE(ARRAY(1,2,3)) AS val FROM t0",
      """
        |SELECT `gen_attr` AS `val`
        |FROM (SELECT `gen_attr`
        |      FROM (SELECT `id` AS `gen_attr`
        |            FROM `default`.`t0`)
        |            AS gen_subquery_0
        |      LATERAL VIEW explode(array(1, 2, 3)) gen_subquery_2 AS `gen_attr`)
        |      AS gen_subquery_1
      """.stripMargin)
  }

  test("generator in project list with referenced table") {
    checkHiveQl("SELECT EXPLODE(arr) FROM parquet_t3",
      """
        |SELECT `gen_attr` AS `col`
        |FROM (SELECT `gen_attr`
        |      FROM (SELECT `arr` AS `gen_attr`, `arr2` AS `gen_attr`, `json` AS `gen_attr`,
        |                   `id` AS `gen_attr`
        |            FROM `default`.`parquet_t3`)
        |            AS gen_subquery_0
        |      LATERAL VIEW explode(`gen_attr`) gen_subquery_2 AS `gen_attr`)
        |      AS gen_subquery_1
      """.stripMargin)

    checkHiveQl("SELECT EXPLODE(arr) AS val FROM parquet_t3",
      """
        |SELECT `gen_attr` AS `val`
        |FROM (SELECT `gen_attr`
        |      FROM (SELECT `arr` AS `gen_attr`, `arr2` AS `gen_attr`, `json` AS `gen_attr`,
        |                   `id` AS `gen_attr`
        |            FROM `default`.`parquet_t3`)
        |            AS gen_subquery_0
        |      LATERAL VIEW explode(`gen_attr`) gen_subquery_2 AS `gen_attr`)
        |      AS gen_subquery_1
      """.stripMargin)
  }

  test("generator in project list with non-UDTF expressions") {
    checkHiveQl("SELECT EXPLODE(arr), id FROM parquet_t3",
      """
        |SELECT `gen_attr` AS `col`, `gen_attr` AS `id`
        |FROM (SELECT `gen_attr`, `gen_attr`
        |      FROM (SELECT `arr` AS `gen_attr`, `arr2` AS `gen_attr`, `json` AS `gen_attr`,
        |                   `id` AS `gen_attr`
        |            FROM `default`.`parquet_t3`)
        |            AS gen_subquery_0
        |      LATERAL VIEW explode(`gen_attr`) gen_subquery_1 AS `gen_attr`)
        |      AS parquet_t3
      """.stripMargin)

    checkHiveQl("SELECT EXPLODE(arr) AS val, id as a FROM parquet_t3",
      """
        |SELECT `gen_attr` AS `val`, `gen_attr` AS `a`
        |FROM (SELECT `gen_attr`, `gen_attr` AS `gen_attr`
        |      FROM (SELECT `arr` AS `gen_attr`, `arr2` AS `gen_attr`, `json` AS `gen_attr`,
        |                   `id` AS `gen_attr`
        |            FROM `default`.`parquet_t3`)
        |            AS gen_subquery_0
        |      LATERAL VIEW explode(`gen_attr`) gen_subquery_2 AS `gen_attr`)
        |      AS gen_subquery_1
      """.stripMargin)
  }

  test("generator in lateral view") {
    checkHiveQl("SELECT val, id FROM parquet_t3 LATERAL VIEW EXPLODE(arr) exp AS val",
      """
        |SELECT `gen_attr` AS `val`, `gen_attr` AS `id`
        |FROM (SELECT `gen_attr`, `gen_attr`
        |      FROM (SELECT `arr` AS `gen_attr`, `arr2` AS `gen_attr`, `json` AS `gen_attr`,
        |                   `id` AS `gen_attr`
        |            FROM `default`.`parquet_t3`)
        |            AS gen_subquery_0
        |      LATERAL VIEW explode(`gen_attr`) gen_subquery_2 AS `gen_attr`)
        |      AS gen_subquery_1
      """.stripMargin)

    checkHiveQl("SELECT val, id FROM parquet_t3 LATERAL VIEW OUTER EXPLODE(arr) exp AS val",
      """
        |SELECT `gen_attr` AS `val`, `gen_attr` AS `id`
        |FROM (SELECT `gen_attr`, `gen_attr`
        |      FROM (SELECT `arr` AS `gen_attr`, `arr2` AS `gen_attr`, `json` AS `gen_attr`,
        |                   `id` AS `gen_attr`
        |            FROM `default`.`parquet_t3`)
        |            AS gen_subquery_0
        |      LATERAL VIEW OUTER explode(`gen_attr`) gen_subquery_2 AS `gen_attr`)
        |      AS gen_subquery_1
      """.stripMargin)
  }

  test("generator in lateral view with ambiguous names") {
    checkHiveQl(
      """
        |SELECT exp.id, parquet_t3.id
        |FROM parquet_t3
        |LATERAL VIEW EXPLODE(arr) exp AS id
      """.stripMargin,
      """
        |SELECT `gen_attr` AS `id`, `gen_attr` AS `id`
        |FROM (SELECT `gen_attr`, `gen_attr`
        |      FROM (SELECT `arr` AS `gen_attr`, `arr2` AS `gen_attr`, `json` AS `gen_attr`,
        |                   `id` AS `gen_attr`
        |            FROM `default`.`parquet_t3`)
        |            AS gen_subquery_0
        |      LATERAL VIEW explode(`gen_attr`) gen_subquery_2 AS `gen_attr`)
        |      AS gen_subquery_1
      """.stripMargin)

    checkHiveQl(
      """
        |SELECT exp.id, parquet_t3.id
        |FROM parquet_t3
        |LATERAL VIEW OUTER EXPLODE(arr) exp AS id
      """.stripMargin,
      """
        |SELECT `gen_attr` AS `id`, `gen_attr` AS `id`
        |FROM (SELECT `gen_attr`, `gen_attr`
        |      FROM (SELECT `arr` AS `gen_attr`, `arr2` AS `gen_attr`, `json` AS `gen_attr`,
        |                   `id` AS `gen_attr`
        |            FROM `default`.`parquet_t3`)
        |            AS gen_subquery_0
        |      LATERAL VIEW OUTER explode(`gen_attr`) gen_subquery_2 AS `gen_attr`)
        |      AS gen_subquery_1
      """.stripMargin)
  }

  test("use JSON_TUPLE as generator") {
    checkHiveQl(
      """
        |SELECT c0, c1, c2
        |FROM parquet_t3
        |LATERAL VIEW JSON_TUPLE(json, 'f1', 'f2', 'f3') jt
      """.stripMargin,
      """
        |SELECT `gen_attr` AS `c0`, `gen_attr` AS `c1`, `gen_attr` AS `c2`
        |FROM (SELECT `gen_attr`, `gen_attr`, `gen_attr`
        |      FROM (SELECT `arr` AS `gen_attr`, `arr2` AS `gen_attr`, `json` AS `gen_attr`,
        |                   `id` AS `gen_attr`
        |            FROM `default`.`parquet_t3`)
        |            AS gen_subquery_0
        |      LATERAL VIEW json_tuple(`gen_attr`, "f1", "f2", "f3")
        |                   gen_subquery_1 AS `gen_attr`, `gen_attr`, `gen_attr`)
        |      AS jt
      """.stripMargin)

    checkHiveQl(
      """
        |SELECT a, b, c
        |FROM parquet_t3
        |LATERAL VIEW JSON_TUPLE(json, 'f1', 'f2', 'f3') jt AS a, b, c
      """.stripMargin,
      """
        |SELECT `gen_attr` AS `a`, `gen_attr` AS `b`, `gen_attr` AS `c`
        |FROM (SELECT `gen_attr`, `gen_attr`, `gen_attr`
        |      FROM (SELECT `arr` AS `gen_attr`, `arr2` AS `gen_attr`, `json` AS `gen_attr`,
        |                   `id` AS `gen_attr`
        |            FROM `default`.`parquet_t3`)
        |            AS gen_subquery_0
        |      LATERAL VIEW json_tuple(`gen_attr`, "f1", "f2", "f3")
        |                   gen_subquery_1 AS `gen_attr`, `gen_attr`, `gen_attr`)
        |      AS jt
      """.stripMargin)
  }

  test("nested generator in lateral view") {
    checkHiveQl(
      """
        |SELECT val, id
        |FROM parquet_t3
        |LATERAL VIEW EXPLODE(arr2) exp1 AS nested_array
        |LATERAL VIEW EXPLODE(nested_array) exp1 AS val
      """.stripMargin,
      """
        |SELECT `gen_attr` AS `val`, `gen_attr` AS `id`
        |FROM (SELECT `gen_attr`, `gen_attr`
        |      FROM (SELECT `arr` AS `gen_attr`, `arr2` AS `gen_attr`, `json` AS `gen_attr`,
        |                   `id` AS `gen_attr`
        |            FROM `default`.`parquet_t3`)
        |            AS gen_subquery_0
        |      LATERAL VIEW explode(`gen_attr`) gen_subquery_2 AS `gen_attr`
        |      LATERAL VIEW explode(`gen_attr`) gen_subquery_3 AS `gen_attr`)
        |      AS gen_subquery_1
      """.stripMargin)

    checkHiveQl(
      """
        |SELECT val, id
        |FROM parquet_t3
        |LATERAL VIEW EXPLODE(arr2) exp1 AS nested_array
        |LATERAL VIEW OUTER EXPLODE(nested_array) exp1 AS val
      """.stripMargin,
      """
        |SELECT `gen_attr` AS `val`, `gen_attr` AS `id`
        |FROM (SELECT `gen_attr`, `gen_attr`
        |      FROM (SELECT `arr` AS `gen_attr`, `arr2` AS `gen_attr`, `json` AS `gen_attr`,
        |                   `id` AS `gen_attr`
        |            FROM `default`.`parquet_t3`)
        |            AS gen_subquery_0
        |      LATERAL VIEW explode(`gen_attr`) gen_subquery_2 AS `gen_attr`
        |      LATERAL VIEW OUTER explode(`gen_attr`) gen_subquery_3 AS `gen_attr`)
        |      AS gen_subquery_1
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
      """.stripMargin,
      """
        |SELECT `gen_attr` AS `val`, `gen_attr` AS `id`
        |FROM (SELECT `gen_attr`, `gen_attr`
        |      FROM (SELECT gen_subquery_0.`gen_attr`, gen_subquery_0.`gen_attr`,
        |                   gen_subquery_0.`gen_attr`, gen_subquery_0.`gen_attr`
        |            FROM (SELECT `arr` AS `gen_attr`, `arr2` AS `gen_attr`, `json` AS `gen_attr`,
        |                         `id` AS `gen_attr`
        |                  FROM `default`.`parquet_t3`)
        |                  AS gen_subquery_0
        |            WHERE (`gen_attr` > CAST(2 AS BIGINT)))
        |            AS gen_subquery_1
        |      LATERAL VIEW explode(`gen_attr`) gen_subquery_2 AS `gen_attr`
        |      ORDER BY `gen_attr` ASC, `gen_attr` ASC LIMIT 5)
        |      AS parquet_t3
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
      """.stripMargin,
      """
        |SELECT `gen_attr` AS `val`, `gen_attr` AS `id`
        |FROM (SELECT `gen_attr`, `gen_attr`
        |      FROM (SELECT `arr` AS `gen_attr`, `arr2` AS `gen_attr`, `json` AS `gen_attr`,
        |                   `id` AS `gen_attr`
        |            FROM `default`.`parquet_t3`)
        |            AS gen_subquery_0
        |      LATERAL VIEW explode(`gen_attr`) gen_subquery_2 AS `gen_attr`
        |      LATERAL VIEW explode(`gen_attr`) gen_subquery_3 AS `gen_attr`
        |      WHERE (`gen_attr` > CAST(2 AS BIGINT))
        |      ORDER BY `gen_attr` ASC, `gen_attr` ASC LIMIT 5)
        |      AS gen_subquery_1
      """.stripMargin)
  }

  test("filter after subquery") {
    checkHiveQl("SELECT a FROM (SELECT key + 1 AS a FROM parquet_t1) t WHERE a > 5",
      """
        |SELECT `gen_attr` AS `a`
        |FROM (SELECT `gen_attr`
        |      FROM (SELECT (`gen_attr` + CAST(1 AS BIGINT)) AS `gen_attr`
        |            FROM (SELECT `key` AS `gen_attr`, `value` AS `gen_attr`
        |                  FROM `default`.`parquet_t1`)
        |                  AS gen_subquery_0) AS t
        |            WHERE (`gen_attr` > CAST(5 AS BIGINT)))
        |      AS t
      """.stripMargin)
  }

  test("SPARK-14933 - select parquet table") {
    withTable("parquet_t") {
      sql("create table parquet_t stored as parquet as select 1 as c1, 'abc' as c2")
      checkHiveQl("select * from parquet_t",
        """
          |SELECT `gen_attr` AS `c1`, `gen_attr` AS `c2`
          |FROM (SELECT `gen_attr`, `gen_attr`
          |      FROM (SELECT `c1` AS `gen_attr`, `c2` AS `gen_attr`
          |            FROM `default`.`parquet_t`)
          |            AS gen_subquery_0)
          |      AS parquet_t
        """.stripMargin)
    }
  }

  test("SPARK-14933 - select orc table") {
    withTable("orc_t") {
      sql("create table orc_t stored as orc as select 1 as c1, 'abc' as c2")
      checkHiveQl("select * from orc_t",
        """
          |SELECT `gen_attr` AS `c1`, `gen_attr` AS `c2`
          |FROM (SELECT `gen_attr`, `gen_attr`
          |      FROM (SELECT `c1` AS `gen_attr`, `c2` AS `gen_attr`
          |            FROM `default`.`orc_t`)
          |            AS gen_subquery_0)
          |      AS orc_t
        """.stripMargin)
    }
  }
}
