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

import java.io.File
import java.util.{Locale, TimeZone}

import org.scalatest.BeforeAndAfter

import scala.util.Try

import org.apache.hadoop.hive.conf.HiveConf.ConfVars

import org.apache.spark.{SparkFiles, SparkException}
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.hive._
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.sql.{SQLConf, Row, SchemaRDD}

case class TestData(a: Int, b: String)

/**
 * A set of test cases expressed in Hive QL that are not covered by the tests included in the hive distribution.
 */
class HiveQuerySuite extends HiveComparisonTest with BeforeAndAfter {
  private val originalTimeZone = TimeZone.getDefault
  private val originalLocale = Locale.getDefault

  override def beforeAll() {
    TestHive.cacheTables = true
    // Timezone is fixed to America/Los_Angeles for those timezone sensitive tests (timestamp_*)
    TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"))
    // Add Locale setting
    Locale.setDefault(Locale.US)
  }

  override def afterAll() {
    TestHive.cacheTables = false
    TimeZone.setDefault(originalTimeZone)
    Locale.setDefault(originalLocale)
  }

  test("SPARK-4908: concurent hive native commands") {
    (1 to 100).par.map { _ =>
      sql("USE default")
      sql("SHOW TABLES")
    }
  }
  
  createQueryTest("! operator",
    """
      |SELECT a FROM (
      |  SELECT 1 AS a FROM src LIMIT 1 UNION ALL
      |  SELECT 2 AS a FROM src LIMIT 1) table
      |WHERE !(a>1)
    """.stripMargin)

  createQueryTest("constant object inspector for generic udf",
    """SELECT named_struct(
      lower("AA"), "10",
      repeat(lower("AA"), 3), "11",
      lower(repeat("AA", 3)), "12",
      printf("Bb%d", 12), "13",
      repeat(printf("s%d", 14), 2), "14") FROM src LIMIT 1""")

  createQueryTest("NaN to Decimal",
    "SELECT CAST(CAST('NaN' AS DOUBLE) AS DECIMAL(1,1)) FROM src LIMIT 1")

  createQueryTest("constant null testing",
    """SELECT
      |IF(FALSE, CAST(NULL AS STRING), CAST(1 AS STRING)) AS COL1,
      |IF(TRUE, CAST(NULL AS STRING), CAST(1 AS STRING)) AS COL2,
      |IF(FALSE, CAST(NULL AS INT), CAST(1 AS INT)) AS COL3,
      |IF(TRUE, CAST(NULL AS INT), CAST(1 AS INT)) AS COL4,
      |IF(FALSE, CAST(NULL AS DOUBLE), CAST(1 AS DOUBLE)) AS COL5,
      |IF(TRUE, CAST(NULL AS DOUBLE), CAST(1 AS DOUBLE)) AS COL6,
      |IF(FALSE, CAST(NULL AS BOOLEAN), CAST(1 AS BOOLEAN)) AS COL7,
      |IF(TRUE, CAST(NULL AS BOOLEAN), CAST(1 AS BOOLEAN)) AS COL8,
      |IF(FALSE, CAST(NULL AS BIGINT), CAST(1 AS BIGINT)) AS COL9,
      |IF(TRUE, CAST(NULL AS BIGINT), CAST(1 AS BIGINT)) AS COL10,
      |IF(FALSE, CAST(NULL AS FLOAT), CAST(1 AS FLOAT)) AS COL11,
      |IF(TRUE, CAST(NULL AS FLOAT), CAST(1 AS FLOAT)) AS COL12,
      |IF(FALSE, CAST(NULL AS SMALLINT), CAST(1 AS SMALLINT)) AS COL13,
      |IF(TRUE, CAST(NULL AS SMALLINT), CAST(1 AS SMALLINT)) AS COL14,
      |IF(FALSE, CAST(NULL AS TINYINT), CAST(1 AS TINYINT)) AS COL15,
      |IF(TRUE, CAST(NULL AS TINYINT), CAST(1 AS TINYINT)) AS COL16,
      |IF(FALSE, CAST(NULL AS BINARY), CAST("1" AS BINARY)) AS COL17,
      |IF(TRUE, CAST(NULL AS BINARY), CAST("1" AS BINARY)) AS COL18,
      |IF(FALSE, CAST(NULL AS DATE), CAST("1970-01-01" AS DATE)) AS COL19,
      |IF(TRUE, CAST(NULL AS DATE), CAST("1970-01-01" AS DATE)) AS COL20,
      |IF(FALSE, CAST(NULL AS TIMESTAMP), CAST(1 AS TIMESTAMP)) AS COL21,
      |IF(TRUE, CAST(NULL AS TIMESTAMP), CAST(1 AS TIMESTAMP)) AS COL22,
      |IF(FALSE, CAST(NULL AS DECIMAL), CAST(1 AS DECIMAL)) AS COL23,
      |IF(TRUE, CAST(NULL AS DECIMAL), CAST(1 AS DECIMAL)) AS COL24
      |FROM src LIMIT 1""".stripMargin)

  createQueryTest("constant array",
  """
    |SELECT sort_array(
    |  sort_array(
    |    array("hadoop distributed file system",
    |          "enterprise databases", "hadoop map-reduce")))
    |FROM src LIMIT 1;
  """.stripMargin)

  createQueryTest("count distinct 0 values",
    """
      |SELECT COUNT(DISTINCT a) FROM (
      |  SELECT 'a' AS a FROM src LIMIT 0) table
    """.stripMargin)

  createQueryTest("count distinct 1 value strings",
    """
      |SELECT COUNT(DISTINCT a) FROM (
      |  SELECT 'a' AS a FROM src LIMIT 1 UNION ALL
      |  SELECT 'b' AS a FROM src LIMIT 1) table
    """.stripMargin)

  createQueryTest("count distinct 1 value",
    """
      |SELECT COUNT(DISTINCT a) FROM (
      |  SELECT 1 AS a FROM src LIMIT 1 UNION ALL
      |  SELECT 1 AS a FROM src LIMIT 1) table
    """.stripMargin)

  createQueryTest("count distinct 2 values",
    """
      |SELECT COUNT(DISTINCT a) FROM (
      |  SELECT 1 AS a FROM src LIMIT 1 UNION ALL
      |  SELECT 2 AS a FROM src LIMIT 1) table
    """.stripMargin)

  createQueryTest("count distinct 2 values including null",
    """
      |SELECT COUNT(DISTINCT a, 1) FROM (
      |  SELECT 1 AS a FROM src LIMIT 1 UNION ALL
      |  SELECT 1 AS a FROM src LIMIT 1 UNION ALL
      |  SELECT null AS a FROM src LIMIT 1) table
    """.stripMargin)

  createQueryTest("count distinct 1 value + null",
  """
    |SELECT COUNT(DISTINCT a) FROM (
    |  SELECT 1 AS a FROM src LIMIT 1 UNION ALL
    |  SELECT 1 AS a FROM src LIMIT 1 UNION ALL
    |  SELECT null AS a FROM src LIMIT 1) table
  """.stripMargin)

  createQueryTest("count distinct 1 value long",
    """
      |SELECT COUNT(DISTINCT a) FROM (
      |  SELECT 1L AS a FROM src LIMIT 1 UNION ALL
      |  SELECT 1L AS a FROM src LIMIT 1) table
    """.stripMargin)

  createQueryTest("count distinct 2 values long",
    """
      |SELECT COUNT(DISTINCT a) FROM (
      |  SELECT 1L AS a FROM src LIMIT 1 UNION ALL
      |  SELECT 2L AS a FROM src LIMIT 1) table
    """.stripMargin)

  createQueryTest("count distinct 1 value + null long",
    """
      |SELECT COUNT(DISTINCT a) FROM (
      |  SELECT 1L AS a FROM src LIMIT 1 UNION ALL
      |  SELECT 1L AS a FROM src LIMIT 1 UNION ALL
      |  SELECT null AS a FROM src LIMIT 1) table
    """.stripMargin)

  createQueryTest("null case",
    "SELECT case when(true) then 1 else null end FROM src LIMIT 1")

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
    sql("CREATE TABLE foo AS SELECT 1 FROM src LIMIT 1").collect()
    assert(sql("SELECT COUNT(*) FROM foo").collect().head.getLong(0) === 1,
      "Incorrect number of rows in created table")
  }

  createQueryTest("between",
    "SELECT * FROM src WHERE key Between 1 and 2")

  createQueryTest("div",
    "SELECT 1 DIV 2, 1 div 2, 1 dIv 2, 100 DIV 51, 100 DIV 49 FROM src LIMIT 1")

  // Jdk version leads to different query output for double, so not use createQueryTest here
  test("division") {
    val res = sql("SELECT 2 / 1, 1 / 2, 1 / 3, 1 / COUNT(*) FROM src LIMIT 1").collect().head
    Seq(2.0, 0.5, 0.3333333333333333, 0.002).zip(res).foreach( x =>
      assert(x._1 == x._2.asInstanceOf[Double]))
  }

  createQueryTest("modulus",
    "SELECT 11 % 10, IF((101.1 % 100.0) BETWEEN 1.01 AND 1.11, \"true\", \"false\"), (101 / 2) % 10 FROM src LIMIT 1")

  test("Query expressed in SQL") {
    setConf("spark.sql.dialect", "sql")
    assert(sql("SELECT 1").collect() === Array(Seq(1)))
    setConf("spark.sql.dialect", "hiveql")
  }

  test("Query expressed in HiveQL") {
    sql("FROM src SELECT key").collect()
  }

  test("Query with constant folding the CAST") {
    sql("SELECT CAST(CAST('123' AS binary) AS binary) FROM src LIMIT 1").collect()
  }

  createQueryTest("Constant Folding Optimization for AVG_SUM_COUNT",
    "SELECT AVG(0), SUM(0), COUNT(null), COUNT(value) FROM src GROUP BY key")

  createQueryTest("Cast Timestamp to Timestamp in UDF",
    """
       | SELECT DATEDIFF(CAST(value AS timestamp), CAST('2002-03-21 00:00:00' AS timestamp))
       | FROM src LIMIT 1
    """.stripMargin)

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

  createQueryTest("empty aggregate input",
    "SELECT SUM(key) FROM (SELECT * FROM src LIMIT 0) a")

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
    sql("SELECT * FROM src TABLESAMPLE(0.1 PERCENT) s")
  }

  test("SchemaRDD toString") {
    sql("SHOW TABLES").toString
    sql("SELECT * FROM src").toString
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

  // Jdk version leads to different query output for double, so not use createQueryTest here
  test("timestamp cast #1") {
    val res = sql("SELECT CAST(CAST(1 AS TIMESTAMP) AS DOUBLE) FROM src LIMIT 1").collect().head
    assert(0.001 == res.getDouble(0))
  }

  createQueryTest("timestamp cast #2",
    "SELECT CAST(CAST(1.2 AS TIMESTAMP) AS DOUBLE) FROM src LIMIT 1")

  createQueryTest("timestamp cast #3",
    "SELECT CAST(CAST(1200 AS TIMESTAMP) AS INT) FROM src LIMIT 1")

  createQueryTest("timestamp cast #4",
    "SELECT CAST(CAST(1.2 AS TIMESTAMP) AS DOUBLE) FROM src LIMIT 1")

  createQueryTest("timestamp cast #5",
    "SELECT CAST(CAST(-1 AS TIMESTAMP) AS DOUBLE) FROM src LIMIT 1")

  createQueryTest("timestamp cast #6",
    "SELECT CAST(CAST(-1.2 AS TIMESTAMP) AS DOUBLE) FROM src LIMIT 1")

  createQueryTest("timestamp cast #7",
    "SELECT CAST(CAST(-1200 AS TIMESTAMP) AS INT) FROM src LIMIT 1")

  createQueryTest("timestamp cast #8",
    "SELECT CAST(CAST(-1.2 AS TIMESTAMP) AS DOUBLE) FROM src LIMIT 1")

  createQueryTest("select null from table",
    "SELECT null FROM src LIMIT 1")

  test("predicates contains an empty AttributeSet() references") {
    sql(
      """
        |SELECT a FROM (
        |  SELECT 1 AS a FROM src LIMIT 1 ) table
        |WHERE abs(20141202) is not null
      """.stripMargin).collect()
  }

  test("implement identity function using case statement") {
    val actual = sql("SELECT (CASE key WHEN key THEN key END) FROM src")
      .map { case Row(i: Int) => i }
      .collect()
      .toSet

    val expected = sql("SELECT key FROM src")
      .map { case Row(i: Int) => i }
      .collect()
      .toSet

    assert(actual === expected)
  }

  // TODO: adopt this test when Spark SQL has the functionality / framework to report errors.
  // See https://github.com/apache/spark/pull/1055#issuecomment-45820167 for a discussion.
  ignore("non-boolean conditions in a CaseWhen are illegal") {
    intercept[Exception] {
      sql("SELECT (CASE WHEN key > 2 THEN 3 WHEN 1 THEN 2 ELSE 0 END) FROM src").collect()
    }
  }

  createQueryTest("case sensitivity when query Hive table",
    "SELECT srcalias.KEY, SRCALIAS.value FROM sRc SrCAlias WHERE SrCAlias.kEy < 15")

  test("case sensitivity: registered table") {
    val testData =
      TestHive.sparkContext.parallelize(
        TestData(1, "str1") ::
        TestData(2, "str2") :: Nil)
    testData.registerTempTable("REGisteredTABle")

    assertResult(Array(Array(2, "str2"))) {
      sql("SELECT tablealias.A, TABLEALIAS.b FROM reGisteredTABle TableAlias " +
        "WHERE TableAliaS.a > 1").collect()
    }
  }

  def isExplanation(result: SchemaRDD) = {
    val explanation = result.select('plan).collect().map { case Row(plan: String) => plan }
    explanation.contains("== Physical Plan ==")
  }

  test("SPARK-1704: Explain commands as a SchemaRDD") {
    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")

    val rdd = sql("explain select key, count(value) from src group by key")
    assert(isExplanation(rdd))

    TestHive.reset()
  }

  test("SPARK-2180: HAVING support in GROUP BY clauses (positive)") {
    val fixture = List(("foo", 2), ("bar", 1), ("foo", 4), ("bar", 3))
      .zipWithIndex.map {case Pair(Pair(value, attr), key) => HavingRow(key, value, attr)}
    TestHive.sparkContext.parallelize(fixture).registerTempTable("having_test")
    val results =
      sql("SELECT value, max(attr) AS attr FROM having_test GROUP BY value HAVING attr > 3")
      .collect()
      .map(x => Pair(x.getString(0), x.getInt(1)))

    assert(results === Array(Pair("foo", 4)))
    TestHive.reset()
  }

  test("SPARK-2180: HAVING with non-boolean clause raises no exceptions") {
    sql("select key, count(*) c from src group by key having c").collect()
  }

  test("SPARK-2225: turn HAVING without GROUP BY into a simple filter") {
    assert(sql("select key from src having key > 490").collect().size < 100)
  }

  test("Query Hive native command execution result") {
    val tableName = "test_native_commands"

    assertResult(0) {
      sql(s"DROP TABLE IF EXISTS $tableName").count()
    }

    assertResult(0) {
      sql(s"CREATE TABLE $tableName(key INT, value STRING)").count()
    }

    assert(
      sql("SHOW TABLES")
        .select('result)
        .collect()
        .map(_.getString(0))
        .contains(tableName))

    assert(isExplanation(sql(s"EXPLAIN SELECT key, COUNT(*) FROM $tableName GROUP BY key")))

    TestHive.reset()
  }

  test("Exactly once semantics for DDL and command statements") {
    val tableName = "test_exactly_once"
    val q0 = sql(s"CREATE TABLE $tableName(key INT, value STRING)")

    // If the table was not created, the following assertion would fail
    assert(Try(table(tableName)).isSuccess)

    // If the CREATE TABLE command got executed again, the following assertion would fail
    assert(Try(q0.count()).isSuccess)
  }

  test("DESCRIBE commands") {
    sql(s"CREATE TABLE test_describe_commands1 (key INT, value STRING) PARTITIONED BY (dt STRING)")

    sql(
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
      sql("DESCRIBE test_describe_commands1")
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
      sql("DESCRIBE default.test_describe_commands1")
        .select('col_name, 'data_type, 'comment)
        .collect()
    }

    // Describe a column is a native command
    assertResult(Array(Array("value", "string", "from deserializer"))) {
      sql("DESCRIBE test_describe_commands1 value")
        .select('result)
        .collect()
        .map(_.getString(0).split("\t").map(_.trim))
    }

    // Describe a column is a native command
    assertResult(Array(Array("value", "string", "from deserializer"))) {
      sql("DESCRIBE default.test_describe_commands1 value")
        .select('result)
        .collect()
        .map(_.getString(0).split("\t").map(_.trim))
    }

    // Describe a partition is a native command
    assertResult(
      Array(
        Array("key", "int"),
        Array("value", "string"),
        Array("dt", "string"),
        Array(""),
        Array("# Partition Information"),
        Array("# col_name", "data_type", "comment"),
        Array(""),
        Array("dt", "string"))
    ) {
      sql("DESCRIBE test_describe_commands1 PARTITION (dt='2008-06-08')")
        .select('result)
        .collect()
        .map(_.getString(0).replaceAll("None", "").trim.split("\t").map(_.trim))
    }

    // Describe a registered temporary table.
    val testData =
      TestHive.sparkContext.parallelize(
        TestData(1, "str1") ::
        TestData(1, "str2") :: Nil)
    testData.registerTempTable("test_describe_commands2")

    assertResult(
      Array(
        Array("a", "IntegerType", null),
        Array("b", "StringType", null))
    ) {
      sql("DESCRIBE test_describe_commands2")
        .select('col_name, 'data_type, 'comment)
        .collect()
    }
  }

  test("SPARK-2263: Insert Map<K, V> values") {
    sql("CREATE TABLE m(value MAP<INT, STRING>)")
    sql("INSERT OVERWRITE TABLE m SELECT MAP(key, value) FROM src LIMIT 10")
    sql("SELECT * FROM m").collect().zip(sql("SELECT * FROM src LIMIT 10").collect()).map {
      case (Row(map: Map[_, _]), Row(key: Int, value: String)) =>
        assert(map.size === 1)
        assert(map.head === (key, value))
    }
  }

  test("ADD JAR command") {
    val testJar = TestHive.getHiveFile("data/files/TestSerDe.jar").getCanonicalPath
    sql("CREATE TABLE alter1(a INT, b INT)")
    intercept[Exception] {
      sql(
        """ALTER TABLE alter1 SET SERDE 'org.apache.hadoop.hive.serde2.TestSerDe'
          |WITH serdeproperties('s1'='9')
        """.stripMargin)
    }
    // Now only verify 0.12.0, and ignore other versions due to binary compatibility
    // current TestSerDe.jar is from 0.12.0
    if (HiveShim.version == "0.12.0") {
      sql(s"ADD JAR $testJar")
      sql(
        """ALTER TABLE alter1 SET SERDE 'org.apache.hadoop.hive.serde2.TestSerDe'
          |WITH serdeproperties('s1'='9')
        """.stripMargin)
    }
    sql("DROP TABLE alter1")
  }

  test("ADD FILE command") {
    val testFile = TestHive.getHiveFile("data/files/v1.txt").getCanonicalFile
    sql(s"ADD FILE $testFile")

    val checkAddFileRDD = sparkContext.parallelize(1 to 2, 1).mapPartitions { _ =>
      Iterator.single(new File(SparkFiles.get("v1.txt")).canRead)
    }

    assert(checkAddFileRDD.first())
  }

  case class LogEntry(filename: String, message: String)
  case class LogFile(name: String)

  createQueryTest("dynamic_partition",
    """
      |DROP TABLE IF EXISTS dynamic_part_table;
      |CREATE TABLE dynamic_part_table(intcol INT) PARTITIONED BY (partcol1 INT, partcol2 INT);
      |
      |SET hive.exec.dynamic.partition.mode=nonstrict;
      |
      |INSERT INTO TABLE dynamic_part_table PARTITION(partcol1, partcol2)
      |SELECT 1, 1, 1 FROM src WHERE key=150;
      |
      |INSERT INTO TABLE dynamic_part_table PARTITION(partcol1, partcol2)
      |SELECT 1, NULL, 1 FROM src WHERE key=150;
      |
      |INSERT INTO TABLE dynamic_part_table PARTITION(partcol1, partcol2)
      |SELECT 1, 1, NULL FROM src WHERE key=150;
      |
      |INSERT INTO TABLe dynamic_part_table PARTITION(partcol1, partcol2)
      |SELECT 1, NULL, NULL FROM src WHERE key=150;
      |
      |DROP TABLE IF EXISTS dynamic_part_table;
    """.stripMargin)

  test("Dynamic partition folder layout") {
    sql("DROP TABLE IF EXISTS dynamic_part_table")
    sql("CREATE TABLE dynamic_part_table(intcol INT) PARTITIONED BY (partcol1 INT, partcol2 INT)")
    sql("SET hive.exec.dynamic.partition.mode=nonstrict")

    val data = Map(
      Seq("1", "1") -> 1,
      Seq("1", "NULL") -> 2,
      Seq("NULL", "1") -> 3,
      Seq("NULL", "NULL") -> 4)

    data.foreach { case (parts, value) =>
      sql(
        s"""INSERT INTO TABLE dynamic_part_table PARTITION(partcol1, partcol2)
           |SELECT $value, ${parts.mkString(", ")} FROM src WHERE key=150
         """.stripMargin)

      val partFolder = Seq("partcol1", "partcol2")
        .zip(parts)
        .map { case (k, v) =>
          if (v == "NULL") {
            s"$k=${ConfVars.DEFAULTPARTITIONNAME.defaultVal}"
          } else {
            s"$k=$v"
          }
        }
        .mkString("/")

      // Loads partition data to a temporary table to verify contents
      val path = s"$warehousePath/dynamic_part_table/$partFolder/part-00000"

      sql("DROP TABLE IF EXISTS dp_verify")
      sql("CREATE TABLE dp_verify(intcol INT)")
      sql(s"LOAD DATA LOCAL INPATH '$path' INTO TABLE dp_verify")

      assert(sql("SELECT * FROM dp_verify").collect() === Array(Row(value)))
    }
  }

  test("Partition spec validation") {
    sql("DROP TABLE IF EXISTS dp_test")
    sql("CREATE TABLE dp_test(key INT, value STRING) PARTITIONED BY (dp INT, sp INT)")
    sql("SET hive.exec.dynamic.partition.mode=strict")

    // Should throw when using strict dynamic partition mode without any static partition
    intercept[SparkException] {
      sql(
        """INSERT INTO TABLE dp_test PARTITION(dp)
          |SELECT key, value, key % 5 FROM src
        """.stripMargin)
    }

    sql("SET hive.exec.dynamic.partition.mode=nonstrict")

    // Should throw when a static partition appears after a dynamic partition
    intercept[SparkException] {
      sql(
        """INSERT INTO TABLE dp_test PARTITION(dp, sp = 1)
          |SELECT key, value, key % 5 FROM src
        """.stripMargin)
    }
  }

  test("SPARK-3414 regression: should store analyzed logical plan when registering a temp table") {
    sparkContext.makeRDD(Seq.empty[LogEntry]).registerTempTable("rawLogs")
    sparkContext.makeRDD(Seq.empty[LogFile]).registerTempTable("logFiles")

    sql(
      """
      SELECT name, message
      FROM rawLogs
      JOIN (
        SELECT name
        FROM logFiles
      ) files
      ON rawLogs.filename = files.name
      """).registerTempTable("boom")

    // This should be successfully analyzed
    sql("SELECT * FROM boom").queryExecution.analyzed
  }

  test("SPARK-3810: PreInsertionCasts static partitioning support") {
    val analyzedPlan = {
      loadTestTable("srcpart")
      sql("DROP TABLE IF EXISTS withparts")
      sql("CREATE TABLE withparts LIKE srcpart")
      sql("INSERT INTO TABLE withparts PARTITION(ds='1', hr='2') SELECT key, value FROM src")
        .queryExecution.analyzed
    }

    assertResult(1, "Duplicated project detected\n" + analyzedPlan) {
      analyzedPlan.collect {
        case _: Project => ()
      }.size
    }
  }

  test("SPARK-3810: PreInsertionCasts dynamic partitioning support") {
    val analyzedPlan = {
      loadTestTable("srcpart")
      sql("DROP TABLE IF EXISTS withparts")
      sql("CREATE TABLE withparts LIKE srcpart")
      sql("SET hive.exec.dynamic.partition.mode=nonstrict")

      sql("CREATE TABLE IF NOT EXISTS withparts LIKE srcpart")
      sql("INSERT INTO TABLE withparts PARTITION(ds, hr) SELECT key, value FROM src")
        .queryExecution.analyzed
    }

    assertResult(1, "Duplicated project detected\n" + analyzedPlan) {
      analyzedPlan.collect {
        case _: Project => ()
      }.size
    }
  }

  test("parse HQL set commands") {
    // Adapted from its SQL counterpart.
    val testKey = "spark.sql.key.usedfortestonly"
    val testVal = "val0,val_1,val2.3,my_table"

    sql(s"set $testKey=$testVal")
    assert(getConf(testKey, testVal + "_") == testVal)

    sql("set some.property=20")
    assert(getConf("some.property", "0") == "20")
    sql("set some.property = 40")
    assert(getConf("some.property", "0") == "40")

    sql(s"set $testKey=$testVal")
    assert(getConf(testKey, "0") == testVal)

    sql(s"set $testKey=")
    assert(getConf(testKey, "0") == "")
  }

  test("SET commands semantics for a HiveContext") {
    // Adapted from its SQL counterpart.
    val testKey = "spark.sql.key.usedfortestonly"
    val testVal = "test.val.0"
    val nonexistentKey = "nonexistent"
    val KV = "([^=]+)=([^=]*)".r
    def collectResults(rdd: SchemaRDD): Set[(String, String)] =
      rdd.collect().map {
        case Row(key: String, value: String) => key -> value
        case Row(KV(key, value)) => key -> value
      }.toSet
    clear()

    // "SET" itself returns all config variables currently specified in SQLConf.
    // TODO: Should we be listing the default here always? probably...
    assert(sql("SET").collect().size == 0)

    assertResult(Set(testKey -> testVal)) {
      collectResults(sql(s"SET $testKey=$testVal"))
    }

    assert(hiveconf.get(testKey, "") == testVal)
    assertResult(Set(testKey -> testVal))(collectResults(sql("SET")))
    assertResult(Set(testKey -> testVal))(collectResults(sql("SET -v")))

    sql(s"SET ${testKey + testKey}=${testVal + testVal}")
    assert(hiveconf.get(testKey + testKey, "") == testVal + testVal)
    assertResult(Set(testKey -> testVal, (testKey + testKey) -> (testVal + testVal))) {
      collectResults(sql("SET"))
    }
    assertResult(Set(testKey -> testVal, (testKey + testKey) -> (testVal + testVal))) {
      collectResults(sql("SET -v"))
    }

    // "SET key"
    assertResult(Set(testKey -> testVal)) {
      collectResults(sql(s"SET $testKey"))
    }

    assertResult(Set(nonexistentKey -> "<undefined>")) {
      collectResults(sql(s"SET $nonexistentKey"))
    }

    clear()
  }

  createQueryTest("select from thrift based table",
    "SELECT * from src_thrift")

  // Put tests that depend on specific Hive settings before these last two test,
  // since they modify /clear stuff.
}

// for SPARK-2180 test
case class HavingRow(key: Int, value: String, attr: Int)
