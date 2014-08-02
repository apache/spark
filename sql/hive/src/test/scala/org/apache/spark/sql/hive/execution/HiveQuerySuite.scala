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

import org.apache.spark.sql.{SchemaRDD, Row}
import org.apache.spark.sql.hive._
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.sql.{Row, SchemaRDD}

case class TestData(a: Int, b: String)

/**
 * A set of test cases expressed in Hive QL that are not covered by the tests included in the hive distribution.
 */
class HiveQuerySuite extends HiveComparisonTest {

  createQueryTest("single case",
    """SELECT case when true then 1 else 2 end FROM src LIMIT 1""")

  createQueryTest("double case",
    """SELECT case when 1 = 2 then 1 when 2 = 2 then 3 else 2 end FROM src LIMIT 1""")

  createQueryTest("case else null",
    """SELECT case when 1 = 2 then 1 when 2 = 2 then 3 else null end FROM src LIMIT 1""")

  createQueryTest("having no references",
    "SELECT key FROM src GROUP BY key HAVING COUNT(*) > 1")

  createQueryTest("boolean = number",
    """
      |SELECT
      |  1 = true, 1L = true, 1Y = true, true = 1, true = 1L, true = 1Y,
      |  0 = true, 0L = true, 0Y = true, true = 0, true = 0L, true = 0Y,
      |  1 = false, 1L = false, 1Y = false, false = 1, false = 1L, false = 1Y,
      |  0 = false, 0L = false, 0Y = false, false = 0, false = 0L, false = 0Y,
      |  2 = true, 2L = true, 2Y = true, true = 2, true = 2L, true = 2Y,
      |  2 = false, 2L = false, 2Y = false, false = 2, false = 2L, false = 2Y
      |FROM src LIMIT 1
    """.stripMargin)

  test("CREATE TABLE AS runs once") {
    hql("CREATE TABLE foo AS SELECT 1 FROM src LIMIT 1").collect()
    assert(hql("SELECT COUNT(*) FROM foo").collect().head.getLong(0) === 1,
      "Incorrect number of rows in created table")
  }

  createQueryTest("between",
    "SELECT * FROM src WHERE key Between 1 and 2")

  createQueryTest("div",
    "SELECT 1 DIV 2, 1 div 2, 1 dIv 2, 100 DIV 51, 100 DIV 49 FROM src LIMIT 1")

  createQueryTest("division",
    "SELECT 2 / 1, 1 / 2, 1 / 3, 1 / COUNT(*) FROM src LIMIT 1")

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

  createQueryTest("case sensitivity: Hive table",
    "SELECT srcalias.KEY, SRCALIAS.value FROM sRc SrCAlias WHERE SrCAlias.kEy < 15")

  test("case sensitivity: registered table") {
    val testData: SchemaRDD =
      TestHive.sparkContext.parallelize(
        TestData(1, "str1") ::
        TestData(2, "str2") :: Nil)
    testData.registerAsTable("REGisteredTABle")

    assertResult(Array(Array(2, "str2"))) {
      hql("SELECT tablealias.A, TABLEALIAS.b FROM reGisteredTABle TableAlias " +
        "WHERE TableAliaS.a > 1").collect()
    }
  }

  def isExplanation(result: SchemaRDD) = {
    val explanation = result.select('plan).collect().map { case Row(plan: String) => plan }
    explanation.size > 1 && explanation.head.startsWith("Physical execution plan")
  }

  test("SPARK-1704: Explain commands as a SchemaRDD") {
    hql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")

    val rdd = hql("explain select key, count(value) from src group by key")
    assert(isExplanation(rdd))

    TestHive.reset()
  }

  test("SPARK-2180: HAVING support in GROUP BY clauses (positive)") {
    val fixture = List(("foo", 2), ("bar", 1), ("foo", 4), ("bar", 3))
      .zipWithIndex.map {case Pair(Pair(value, attr), key) => HavingRow(key, value, attr)}
    TestHive.sparkContext.parallelize(fixture).registerAsTable("having_test")
    val results =
      hql("SELECT value, max(attr) AS attr FROM having_test GROUP BY value HAVING attr > 3")
      .collect()
      .map(x => Pair(x.getString(0), x.getInt(1)))

    assert(results === Array(Pair("foo", 4)))
    TestHive.reset()
  }

  test("SPARK-2180: HAVING with non-boolean clause raises no exceptions") {
    hql("select key, count(*) c from src group by key having c").collect()
  }

  test("SPARK-2225: turn HAVING without GROUP BY into a simple filter") {
    assert(hql("select key from src having key > 490").collect().size < 100)
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

  test("DESCRIBE commands") {
    hql(s"CREATE TABLE test_describe_commands1 (key INT, value STRING) PARTITIONED BY (dt STRING)")

    hql(
      """FROM src INSERT OVERWRITE TABLE test_describe_commands1 PARTITION (dt='2008-06-08')
        |SELECT key, value
      """.stripMargin)

    // Describe a table
    assertResult(
      Array(
        Array("key", "int", null),
        Array("value", "string", null),
        Array("dt", "string", null),
        Array("# Partition Information", "", ""),
        Array("# col_name", "data_type", "comment"),
        Array("dt", "string", null))
    ) {
      hql("DESCRIBE test_describe_commands1")
        .select('col_name, 'data_type, 'comment)
        .collect()
    }

    // Describe a table with a fully qualified table name
    assertResult(
      Array(
        Array("key", "int", null),
        Array("value", "string", null),
        Array("dt", "string", null),
        Array("# Partition Information", "", ""),
        Array("# col_name", "data_type", "comment"),
        Array("dt", "string", null))
    ) {
      hql("DESCRIBE default.test_describe_commands1")
        .select('col_name, 'data_type, 'comment)
        .collect()
    }

    // Describe a column is a native command
    assertResult(Array(Array("value", "string", "from deserializer"))) {
      hql("DESCRIBE test_describe_commands1 value")
        .select('result)
        .collect()
        .map(_.getString(0).split("\t").map(_.trim))
    }

    // Describe a column is a native command
    assertResult(Array(Array("value", "string", "from deserializer"))) {
      hql("DESCRIBE default.test_describe_commands1 value")
        .select('result)
        .collect()
        .map(_.getString(0).split("\t").map(_.trim))
    }

    // Describe a partition is a native command
    assertResult(
      Array(
        Array("key", "int", "None"),
        Array("value", "string", "None"),
        Array("dt", "string", "None"),
        Array("", "", ""),
        Array("# Partition Information", "", ""),
        Array("# col_name", "data_type", "comment"),
        Array("", "", ""),
        Array("dt", "string", "None"))
    ) {
      hql("DESCRIBE test_describe_commands1 PARTITION (dt='2008-06-08')")
        .select('result)
        .collect()
        .map(_.getString(0).split("\t").map(_.trim))
    }

    // Describe a registered temporary table.
    val testData: SchemaRDD =
      TestHive.sparkContext.parallelize(
        TestData(1, "str1") ::
        TestData(1, "str2") :: Nil)
    testData.registerAsTable("test_describe_commands2")

    assertResult(
      Array(
        Array("# Registered as a temporary table", null, null),
        Array("a", "IntegerType", null),
        Array("b", "StringType", null))
    ) {
      hql("DESCRIBE test_describe_commands2")
        .select('col_name, 'data_type, 'comment)
        .collect()
    }
  }

  test("SPARK-2263: Insert Map<K, V> values") {
    hql("CREATE TABLE m(value MAP<INT, STRING>)")
    hql("INSERT OVERWRITE TABLE m SELECT MAP(key, value) FROM src LIMIT 10")
    hql("SELECT * FROM m").collect().zip(hql("SELECT * FROM src LIMIT 10").collect()).map {
      case (Row(map: Map[_, _]), Row(key: Int, value: String)) =>
        assert(map.size === 1)
        assert(map.head === (key, value))
    }
  }

  test("parse HQL set commands") {
    // Adapted from its SQL counterpart.
    val testKey = "spark.sql.key.usedfortestonly"
    val testVal = "val0,val_1,val2.3,my_table"

    hql(s"set $testKey=$testVal")
    assert(get(testKey, testVal + "_") == testVal)

    hql("set some.property=20")
    assert(get("some.property", "0") == "20")
    hql("set some.property = 40")
    assert(get("some.property", "0") == "40")

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

    clear()

    // "set" itself returns all config variables currently specified in SQLConf.
    assert(hql("SET").collect().size == 0)

    assertResult(Array(s"$testKey=$testVal")) {
      hql(s"SET $testKey=$testVal").collect().map(_.getString(0))
    }

    assert(hiveconf.get(testKey, "") == testVal)
    assertResult(Array(s"$testKey=$testVal")) {
      hql(s"SET $testKey=$testVal").collect().map(_.getString(0))
    }

    hql(s"SET ${testKey + testKey}=${testVal + testVal}")
    assert(hiveconf.get(testKey + testKey, "") == testVal + testVal)
    assertResult(Array(s"$testKey=$testVal", s"${testKey + testKey}=${testVal + testVal}")) {
      hql(s"SET").collect().map(_.getString(0))
    }

    // "set key"
    assertResult(Array(s"$testKey=$testVal")) {
      hql(s"SET $testKey").collect().map(_.getString(0))
    }

    assertResult(Array(s"$nonexistentKey=<undefined>")) {
      hql(s"SET $nonexistentKey").collect().map(_.getString(0))
    }

    // Assert that sql() should have the same effects as hql() by repeating the above using sql().
    clear()
    assert(sql("SET").collect().size == 0)

    assertResult(Array(s"$testKey=$testVal")) {
      sql(s"SET $testKey=$testVal").collect().map(_.getString(0))
    }

    assert(hiveconf.get(testKey, "") == testVal)
    assertResult(Array(s"$testKey=$testVal")) {
      sql("SET").collect().map(_.getString(0))
    }

    sql(s"SET ${testKey + testKey}=${testVal + testVal}")
    assert(hiveconf.get(testKey + testKey, "") == testVal + testVal)
    assertResult(Array(s"$testKey=$testVal", s"${testKey + testKey}=${testVal + testVal}")) {
      sql("SET").collect().map(_.getString(0))
    }

    assertResult(Array(s"$testKey=$testVal")) {
      sql(s"SET $testKey").collect().map(_.getString(0))
    }

    assertResult(Array(s"$nonexistentKey=<undefined>")) {
      sql(s"SET $nonexistentKey").collect().map(_.getString(0))
    }

    clear()
  }

  // Put tests that depend on specific Hive settings before these last two test,
  // since they modify /clear stuff.
}

// for SPARK-2180 test
case class HavingRow(key: Int, value: String, attr: Int)
