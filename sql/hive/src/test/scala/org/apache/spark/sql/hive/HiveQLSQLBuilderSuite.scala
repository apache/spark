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

import java.sql.Timestamp

import org.apache.spark.sql.catalyst.expressions.{Literal, Alias}
import org.apache.spark.sql.catalyst.plans.logical.{OneRowRelation, Project}
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.functions._

class HiveQLSQLBuilderSuite extends SQLBuilderTest with SQLTestUtils {
  import testImplicits._

  protected override def beforeAll(): Unit = {
    sqlContext.range(10).write.saveAsTable("t0")

    sqlContext
      .range(10)
      .select('id as 'key, concat(lit("val_"), 'id) as 'value)
      .write
      .saveAsTable("t1")
  }

  override protected def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS t0")
  }

  private def checkHiveQl(hiveQl: String): Unit = {
    val df = sql(hiveQl)
    val convertedSQL = new SQLBuilder(df).toSQL

    if (convertedSQL.isEmpty) {
      fail(
        s"""Cannot convert the following HiveQL query plan back to SQL query string:
           |
           |Original HiveQL query string:
           |$hiveQl
           |
           |Resolved query plan:
           |${df.queryExecution.analyzed.treeString}
         """.stripMargin)
    }

    checkAnswer(sql(convertedSQL.get), df)
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

  // TODO Fix name collision introduced by ResolveAggregateFunction analysis rule
  // When there are multiple aggregate functions in ORDER BY clause, all of them are extracted into
  // Aggregate operator and aliased to the same name "aggOrder".  This is OK for normal query
  // execution since these aliases have different expression ID.  But this introduces name collision
  // when converting resolved plans back to SQL query strings as expression IDs are stripped.
  ignore("aggregate function in order by clause with multiple order keys") {
    checkHiveQl("SELECT COUNT(value) FROM t1 GROUP BY key ORDER BY key, MAX(key)")
  }

  test("type widening in union") {
    checkHiveQl("SELECT id FROM t0 UNION ALL SELECT CAST(id AS INT) AS id FROM t0")
  }
}
