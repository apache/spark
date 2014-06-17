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

package org.apache.spark.sql.hive.execution

import scala.util.Try

import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.sql.{SchemaRDD, execution, Row}

/**
 * A set of test cases expressed in Hive QL that are not covered by the tests included in the hive distribution.
 */
class HiveQuerySuite extends HiveComparisonTest {

  createQueryTest("between",
    "SELECT * FROM src WHERE key Between 1 and 2")

  createQueryTest("div",
    "SELECT 1 DIV 2, 1 div 2, 1 dIv 2 FROM src LIMIT 1")

  test("Query expressed in SQL") {
    assert(sql("SELECT 1").collect() === Array(Seq(1)))
  }

  test("Query expressed in HiveQL") {
    hql("FROM src SELECT key").collect()
    hiveql("FROM src SELECT key").collect()
  }

  createQueryTest("Constant Folding Optimization for AVG_SUM_COUNT",
    "SELECT AVG(0), SUM(0), COUNT(null), COUNT(value) FROM src GROUP BY key")

  createQueryTest("Simple Average",
    "SELECT AVG(key) FROM src")

  createQueryTest("Simple Average + 1",
    "SELECT AVG(key) + 1.0 FROM src")

  createQueryTest("Simple Average + 1 with group",
    "SELECT AVG(key) + 1.0, value FROM src group by value")

  createQueryTest("string literal",
    "SELECT 'test' FROM src")

  createQueryTest("Escape sequences",
    """SELECT key, '\\\t\\' FROM src WHERE key = 86""")

  createQueryTest("IgnoreExplain",
    """EXPLAIN SELECT key FROM src""")

  createQueryTest("trivial join where clause",
    "SELECT * FROM src a JOIN src b WHERE a.key = b.key")

  createQueryTest("trivial join ON clause",
    "SELECT * FROM src a JOIN src b ON a.key = b.key")

  createQueryTest("small.cartesian",
    "SELECT a.key, b.key FROM (SELECT key FROM src WHERE key < 1) a JOIN (SELECT key FROM src WHERE key = 2) b")

  createQueryTest("length.udf",
    "SELECT length(\"test\") FROM src LIMIT 1")

  createQueryTest("partitioned table scan",
    "SELECT ds, hr, key, value FROM srcpart")

  createQueryTest("hash",
    "SELECT hash('test') FROM src LIMIT 1")

  createQueryTest("create table as",
    """
      |CREATE TABLE createdtable AS SELECT * FROM src;
      |SELECT * FROM createdtable
    """.stripMargin)

  createQueryTest("create table as with db name",
    """
      |CREATE DATABASE IF NOT EXISTS testdb;
      |CREATE TABLE testdb.createdtable AS SELECT * FROM default.src;
      |SELECT * FROM testdb.createdtable;
      |DROP DATABASE IF EXISTS testdb CASCADE
    """.stripMargin)

  createQueryTest("insert table with db name",
    """
      |CREATE DATABASE IF NOT EXISTS testdb;
      |CREATE TABLE testdb.createdtable like default.src;
      |INSERT INTO TABLE testdb.createdtable SELECT * FROM default.src;
      |SELECT * FROM testdb.createdtable;
      |DROP DATABASE IF EXISTS testdb CASCADE
    """.stripMargin)

  createQueryTest("insert into and insert overwrite",
    """
      |CREATE TABLE createdtable like src;
      |INSERT INTO TABLE createdtable SELECT * FROM src;
      |INSERT INTO TABLE createdtable SELECT * FROM src1;
      |SELECT * FROM createdtable;
      |INSERT OVERWRITE TABLE createdtable SELECT * FROM src WHERE key = 86;
      |SELECT * FROM createdtable;
    """.stripMargin)

  createQueryTest("transform",
    "SELECT TRANSFORM (key) USING 'cat' AS (tKey) FROM src")

  createQueryTest("LIKE",
    "SELECT * FROM src WHERE value LIKE '%1%'")

  createQueryTest("DISTINCT",
    "SELECT DISTINCT key, value FROM src")

  ignore("empty aggregate input") {
    createQueryTest("empty aggregate input",
      "SELECT SUM(key) FROM (SELECT * FROM src LIMIT 0) a")
  }

  createQueryTest("lateral view1",
    "SELECT tbl.* FROM src LATERAL VIEW explode(array(1,2)) tbl as a")

  createQueryTest("lateral view2",
    "SELECT * FROM src LATERAL VIEW explode(array(1,2)) tbl")


  createQueryTest("lateral view3",
    "FROM src SELECT key, D.* lateral view explode(array(key+3, key+4)) D as CX")

  createQueryTest("lateral view4",
    """
      |create table src_lv1 (key string, value string);
      |create table src_lv2 (key string, value string);
      |
      |FROM src
      |insert overwrite table src_lv1 SELECT key, D.* lateral view explode(array(key+3, key+4)) D as CX
      |insert overwrite table src_lv2 SELECT key, D.* lateral view explode(array(key+3, key+4)) D as CX
    """.stripMargin)

  createQueryTest("lateral view5",
    "FROM src SELECT explode(array(key+3, key+4))")

  createQueryTest("lateral view6",
    "SELECT * FROM src LATERAL VIEW explode(map(key+3,key+4)) D as k, v")

  test("sampling") {
    hql("SELECT * FROM src TABLESAMPLE(0.1 PERCENT) s")
  }

  test("SchemaRDD toString") {
    hql("SHOW TABLES").toString
    hql("SELECT * FROM src").toString
  }

  createQueryTest("case statements with key #1",
    "SELECT (CASE 1 WHEN 2 THEN 3 END) FROM src where key < 15")

  createQueryTest("case statements with key #2",
    "SELECT (CASE key WHEN 2 THEN 3 ELSE 0 END) FROM src WHERE key < 15")

  createQueryTest("case statements with key #3",
    "SELECT (CASE key WHEN 2 THEN 3 WHEN NULL THEN 4 END) FROM src WHERE key < 15")

  createQueryTest("case statements with key #4",
    "SELECT (CASE key WHEN 2 THEN 3 WHEN NULL THEN 4 ELSE 0 END) FROM src WHERE key < 15")

  createQueryTest("case statements WITHOUT key #1",
    "SELECT (CASE WHEN key > 2 THEN 3 END) FROM src WHERE key < 15")

  createQueryTest("case statements WITHOUT key #2",
    "SELECT (CASE WHEN key > 2 THEN 3 ELSE 4 END) FROM src WHERE key < 15")

  createQueryTest("case statements WITHOUT key #3",
    "SELECT (CASE WHEN key > 2 THEN 3 WHEN 2 > key THEN 2 END) FROM src WHERE key < 15")

  createQueryTest("case statements WITHOUT key #4",
    "SELECT (CASE WHEN key > 2 THEN 3 WHEN 2 > key THEN 2 ELSE 0 END) FROM src WHERE key < 15")

  test("implement identity function using case statement") {
    val actual = hql("SELECT (CASE key WHEN key THEN key END) FROM src").collect().toSet
    val expected = hql("SELECT key FROM src").collect().toSet
    assert(actual === expected)
  }

  // TODO: adopt this test when Spark SQL has the functionality / framework to report errors.
  // See https://github.com/apache/spark/pull/1055#issuecomment-45820167 for a discussion.
  ignore("non-boolean conditions in a CaseWhen are illegal") {
    intercept[Exception] {
      hql("SELECT (CASE WHEN key > 2 THEN 3 WHEN 1 THEN 2 ELSE 0 END) FROM src").collect()
    }
  }

  private val explainCommandClassName =
    classOf[execution.ExplainCommand].getSimpleName.stripSuffix("$")

  def isExplanation(result: SchemaRDD) = {
    val explanation = result.select('plan).collect().map { case Row(plan: String) => plan }
    explanation.size > 1 && explanation.head.startsWith(explainCommandClassName)
  }

  test("SPARK-1704: Explain commands as a SchemaRDD") {
    hql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")

    val rdd = hql("explain select key, count(value) from src group by key")
    assert(isExplanation(rdd))

    TestHive.reset()
  }

  test("Query Hive native command execution result") {
    val tableName = "test_native_commands"

    assertResult(0) {
      hql(s"DROP TABLE IF EXISTS $tableName").count()
    }

    assertResult(0) {
      hql(s"CREATE TABLE $tableName(key INT, value STRING)").count()
    }

    assert(
      hql("SHOW TABLES")
        .select('result)
        .collect()
        .map(_.getString(0))
        .contains(tableName))

    assertResult(Array(Array("key", "int", "None"), Array("value", "string", "None"))) {
      hql(s"DESCRIBE $tableName")
        .select('result)
        .collect()
        .map(_.getString(0).split("\t").map(_.trim))
    }

    assert(isExplanation(hql(s"EXPLAIN SELECT key, COUNT(*) FROM $tableName GROUP BY key")))

    TestHive.reset()
  }

  test("Exactly once semantics for DDL and command statements") {
    val tableName = "test_exactly_once"
    val q0 = hql(s"CREATE TABLE $tableName(key INT, value STRING)")

    // If the table was not created, the following assertion would fail
    assert(Try(table(tableName)).isSuccess)

    // If the CREATE TABLE command got executed again, the following assertion would fail
    assert(Try(q0.count()).isSuccess)
  }

  test("parse HQL set commands") {
    // Adapted from its SQL counterpart.
    val testKey = "spark.sql.key.usedfortestonly"
    val testVal = "val0,val_1,val2.3,my_table"

    hql(s"set $testKey=$testVal")
    assert(get(testKey, testVal + "_") == testVal)

    hql("set mapred.reduce.tasks=20")
    assert(get("mapred.reduce.tasks", "0") == "20")
    hql("set mapred.reduce.tasks = 40")
    assert(get("mapred.reduce.tasks", "0") == "40")

    hql(s"set $testKey=$testVal")
    assert(get(testKey, "0") == testVal)

    hql(s"set $testKey=")
    assert(get(testKey, "0") == "")
  }

  test("SET commands semantics for a HiveContext") {
    // Adapted from its SQL counterpart.
    val testKey = "spark.sql.key.usedfortestonly"
    val testVal = "test.val.0"
    val nonexistentKey = "nonexistent"
    def rowsToPairs(rows: Array[Row]) = rows.map { case Row(key: String, value: String) =>
      key -> value
    }

    clear()

    // "set" itself returns all config variables currently specified in SQLConf.
    assert(hql("SET").collect().size == 0)

    assertResult(Array(testKey -> testVal)) {
      rowsToPairs(hql(s"SET $testKey=$testVal").collect())
    }

    assert(hiveconf.get(testKey, "") == testVal)
    assertResult(Array(testKey -> testVal)) {
      rowsToPairs(hql("SET").collect())
    }

    hql(s"SET ${testKey + testKey}=${testVal + testVal}")
    assert(hiveconf.get(testKey + testKey, "") == testVal + testVal)
    assertResult(Array(testKey -> testVal, (testKey + testKey) -> (testVal + testVal))) {
      rowsToPairs(hql("SET").collect())
    }

    // "set key"
    assertResult(Array(testKey -> testVal)) {
      rowsToPairs(hql(s"SET $testKey").collect())
    }

    assertResult(Array(nonexistentKey -> "<undefined>")) {
      rowsToPairs(hql(s"SET $nonexistentKey").collect())
    }

    // Assert that sql() should have the same effects as hql() by repeating the above using sql().
    clear()
    assert(sql("SET").collect().size == 0)

    assertResult(Array(testKey -> testVal)) {
      rowsToPairs(sql(s"SET $testKey=$testVal").collect())
    }

    assert(hiveconf.get(testKey, "") == testVal)
    assertResult(Array(testKey -> testVal)) {
      rowsToPairs(sql("SET").collect())
    }

    sql(s"SET ${testKey + testKey}=${testVal + testVal}")
    assert(hiveconf.get(testKey + testKey, "") == testVal + testVal)
    assertResult(Array(testKey -> testVal, (testKey + testKey) -> (testVal + testVal))) {
      rowsToPairs(sql("SET").collect())
    }

    assertResult(Array(testKey -> testVal)) {
      rowsToPairs(sql(s"SET $testKey").collect())
    }

    assertResult(Array(nonexistentKey -> "<undefined>")) {
      rowsToPairs(sql(s"SET $nonexistentKey").collect())
    }

    clear()
  }

  // Put tests that depend on specific Hive settings before these last two test,
  // since they modify /clear stuff.

}
