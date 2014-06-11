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

import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.sql.hive.test.TestHive

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

  test("SPARK-1704: Explain commands as a SchemaRDD") {
    hql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
    val rdd = hql("explain select key, count(value) from src group by key")
    assert(rdd.collect().size == 1)
    assert(rdd.toString.contains("ExplainCommand"))
    assert(rdd.filter(row => row.toString.contains("ExplainCommand")).collect().size == 0,
      "actual contents of the result should be the plans of the query to be explained")
    TestHive.reset()
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
    var testVal = "test.val.0"
    val nonexistentKey = "nonexistent"
    def fromRows(row: Array[Row]): Array[String] = row.map(_.getString(0))

    clear()

    // "set" itself returns all config variables currently specified in SQLConf.
    assert(hql("set").collect().size == 0)

    // "set key=val"
    hql(s"SET $testKey=$testVal")
    assert(fromRows(hql("SET").collect()) sameElements Array(s"$testKey=$testVal"))
    assert(hiveconf.get(testKey, "") == testVal)

    hql(s"SET ${testKey + testKey}=${testVal + testVal}")
    assert(fromRows(hql("SET").collect()) sameElements
      Array(
        s"$testKey=$testVal",
        s"${testKey + testKey}=${testVal + testVal}"))
    assert(hiveconf.get(testKey + testKey, "") == testVal + testVal)

    // "set key"
    assert(fromRows(hql(s"SET $testKey").collect()) sameElements
      Array(s"$testKey=$testVal"))
    assert(fromRows(hql(s"SET $nonexistentKey").collect()) sameElements
      Array(s"$nonexistentKey is undefined"))

    // Assert that sql() should have the same effects as hql() by repeating the above using sql().
    clear()
    assert(sql("set").collect().size == 0)

    sql(s"SET $testKey=$testVal")
    assert(fromRows(sql("SET").collect()) sameElements Array(s"$testKey=$testVal"))
    assert(hiveconf.get(testKey, "") == testVal)

    sql(s"SET ${testKey + testKey}=${testVal + testVal}")
    assert(fromRows(sql("SET").collect()) sameElements
      Array(
        s"$testKey=$testVal",
        s"${testKey + testKey}=${testVal + testVal}"))
    assert(hiveconf.get(testKey + testKey, "") == testVal + testVal)

    assert(fromRows(sql(s"SET $testKey").collect()) sameElements
      Array(s"$testKey=$testVal"))
    assert(fromRows(sql(s"SET $nonexistentKey").collect()) sameElements
      Array(s"$nonexistentKey is undefined"))
  }

  // Put tests that depend on specific Hive settings before these last two test,
  // since they modify /clear stuff.

}
