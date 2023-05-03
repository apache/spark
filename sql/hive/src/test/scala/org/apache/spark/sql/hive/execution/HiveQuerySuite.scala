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
import java.net.URI
import java.nio.file.Files
import java.sql.Timestamp

import scala.util.Try

import org.apache.commons.lang3.{JavaVersion, SystemUtils}
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkFiles, TestUtils}
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.execution.joins.BroadcastNestedLoopJoinExec
import org.apache.spark.sql.hive.HiveUtils.{builtinHiveVersion => hiveVersion}
import org.apache.spark.sql.hive.test.{HiveTestJars, TestHive}
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.tags.SlowHiveTest

case class TestData(a: Int, b: String)

/**
 * A set of test cases expressed in Hive QL that are not covered by the tests
 * included in the hive distribution.
 */
@SlowHiveTest
class HiveQuerySuite extends HiveComparisonTest with SQLTestUtils with BeforeAndAfter {
  import org.apache.spark.sql.hive.test.TestHive.implicits._

  private val originalCrossJoinEnabled = TestHive.conf.crossJoinEnabled

  def spark: SparkSession = sparkSession

  override def beforeAll(): Unit = {
    super.beforeAll()
    TestHive.setCacheTables(true)
    // Ensures that cross joins are enabled so that we can test them
    TestHive.setConf(SQLConf.CROSS_JOINS_ENABLED, true)
  }

  override def afterAll(): Unit = {
    try {
      TestHive.setCacheTables(false)
      sql("DROP TEMPORARY FUNCTION IF EXISTS udtf_count2")
      TestHive.setConf(SQLConf.CROSS_JOINS_ENABLED, originalCrossJoinEnabled)
    } finally {
      super.afterAll()
    }
  }

  private def assertUnsupportedFeature(
      body: => Unit,
      message: String,
      expectedContext: ExpectedContext): Unit = {
    checkError(
      exception = intercept[ParseException] {
        body
      },
      errorClass = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" -> message),
      context = expectedContext)
  }

  // Testing the Broadcast based join for cartesian join (cross join)
  // We assume that the Broadcast Join Threshold will work since the src is a small table
  private val spark_10484_1 = """
                                | SELECT a.key, b.key
                                | FROM src a LEFT JOIN src b WHERE a.key > b.key + 300
                                | ORDER BY b.key, a.key
                                | LIMIT 20
                              """.stripMargin
  private val spark_10484_2 = """
                                | SELECT a.key, b.key
                                | FROM src a RIGHT JOIN src b WHERE a.key > b.key + 300
                                | ORDER BY a.key, b.key
                                | LIMIT 20
                              """.stripMargin
  private val spark_10484_3 = """
                                | SELECT a.key, b.key
                                | FROM src a FULL OUTER JOIN src b WHERE a.key > b.key + 300
                                | ORDER BY a.key, b.key
                                | LIMIT 20
                              """.stripMargin
  private val spark_10484_4 = """
                                | SELECT a.key, b.key
                                | FROM src a JOIN src b WHERE a.key > b.key + 300
                                | ORDER BY a.key, b.key
                                | LIMIT 20
                              """.stripMargin

  createQueryTest("SPARK-10484 Optimize the Cartesian (Cross) Join with broadcast based JOIN #1",
    spark_10484_1)

  createQueryTest("SPARK-10484 Optimize the Cartesian (Cross) Join with broadcast based JOIN #2",
    spark_10484_2)

  createQueryTest("SPARK-10484 Optimize the Cartesian (Cross) Join with broadcast based JOIN #3",
    spark_10484_3)

  createQueryTest("SPARK-10484 Optimize the Cartesian (Cross) Join with broadcast based JOIN #4",
    spark_10484_4)

  test("SPARK-10484 Optimize the Cartesian (Cross) Join with broadcast based JOIN") {
    def assertBroadcastNestedLoopJoin(sqlText: String): Unit = {
      assert(sql(sqlText).queryExecution.sparkPlan.collect {
        case _: BroadcastNestedLoopJoinExec => 1
      }.nonEmpty)
    }

    assertBroadcastNestedLoopJoin(spark_10484_1)
    assertBroadcastNestedLoopJoin(spark_10484_2)
    assertBroadcastNestedLoopJoin(spark_10484_3)
    assertBroadcastNestedLoopJoin(spark_10484_4)
  }

  createQueryTest("insert table with generator with column name",
    """
      |  CREATE TABLE gen_tmp (key Int);
      |  INSERT OVERWRITE TABLE gen_tmp
      |    SELECT explode(array(1,2,3)) AS val FROM src LIMIT 3;
      |  SELECT key FROM gen_tmp ORDER BY key ASC;
    """.stripMargin)

  createQueryTest("insert table with generator with multiple column names",
    """
      |  CREATE TABLE gen_tmp (key Int, value String);
      |  INSERT OVERWRITE TABLE gen_tmp
      |    SELECT explode(map(key, value)) as (k1, k2) FROM src LIMIT 3;
      |  SELECT key, value FROM gen_tmp ORDER BY key, value ASC;
    """.stripMargin)

  createQueryTest("insert table with generator without column name",
    """
      |  CREATE TABLE gen_tmp (key Int);
      |  INSERT OVERWRITE TABLE gen_tmp
      |    SELECT explode(array(1,2,3)) FROM src LIMIT 3;
      |  SELECT key FROM gen_tmp ORDER BY key ASC;
    """.stripMargin)

  test("multiple generators in projection") {
    checkError(
      exception = intercept[AnalysisException] {
        sql("SELECT explode(array(key, key)), explode(array(key, key)) FROM src").collect()
      },
      errorClass = "UNSUPPORTED_GENERATOR.MULTI_GENERATOR",
      parameters = Map(
        "clause" -> "SELECT",
        "num" -> "2",
        "generators" -> "\"explode(array(key, key))\", \"explode(array(key, key))\""))

    checkError(
      exception = intercept[AnalysisException] {
        sql("SELECT explode(array(key, key)) as k1, explode(array(key, key)) FROM src").collect()
      },
      errorClass = "UNSUPPORTED_GENERATOR.MULTI_GENERATOR",
      parameters = Map(
        "clause" -> "SELECT",
        "num" -> "2",
        "generators" -> "\"explode(array(key, key))\", \"explode(array(key, key))\""))
  }

  createQueryTest("! operator",
    """
      |SELECT a FROM (
      |  SELECT 1 AS a UNION ALL SELECT 2 AS a) t
      |WHERE !(a>1)
    """.stripMargin)

  createQueryTest("constant object inspector for generic udf",
    """SELECT named_struct(
      lower("AA"), "10",
      repeat(lower("AA"), 3), "11",
      lower(repeat("AA", 3)), "12",
      printf("bb%d", 12), "13",
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
      |IF(TRUE, CAST(NULL AS TIMESTAMP), CAST('1969-12-31 16:00:01' AS TIMESTAMP)) AS COL21,
      |IF(FALSE, CAST(NULL AS DECIMAL), CAST(1 AS DECIMAL)) AS COL22,
      |IF(TRUE, CAST(NULL AS DECIMAL), CAST(1 AS DECIMAL)) AS COL23
      |FROM src LIMIT 1""".stripMargin)

  test("constant null testing timestamp") {
    val r1 = sql(
      """
        |SELECT IF(FALSE, CAST(NULL AS TIMESTAMP),
        |CAST('1969-12-31 16:00:01' AS TIMESTAMP)) AS COL20
      """.stripMargin).collect().head
    assert(new Timestamp(1000) == r1.getTimestamp(0))
  }

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

  createQueryTest("no from clause",
    "SELECT 1, +1, -1")

  if (!conf.ansiEnabled) {
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
  }

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
    Seq(2.0, 0.5, 0.3333333333333333, 0.002).zip(res.toSeq).foreach( x =>
      assert(x._1 == x._2.asInstanceOf[Double]))
  }

  createQueryTest("modulus",
    "SELECT 11 % 10, IF((101.1 % 100.0) BETWEEN 1.01 AND 1.11, \"true\", \"false\"), " +
      "(101 / 2) % 10 FROM src LIMIT 1")

  test("Query expressed in HiveQL") {
    sql("FROM src SELECT key").collect()
  }

  test("Query with constant folding the CAST") {
    sql("SELECT CAST(CAST('123' AS binary) AS binary) FROM src LIMIT 1").collect()
  }

  createQueryTest("Constant Folding Optimization for AVG_SUM_COUNT",
    "SELECT AVG(0), SUM(0), COUNT(null), COUNT(value) FROM src GROUP BY key")

  if (!conf.ansiEnabled) {
    createQueryTest("Cast Timestamp to Timestamp in UDF",
      """
        | SELECT DATEDIFF(CAST(value AS timestamp), CAST('2002-03-21 00:00:00' AS timestamp))
        | FROM src LIMIT 1
    """.stripMargin)
  }

  createQueryTest("Date comparison test 1",
    """
      | SELECT
      | CAST(CAST('1970-01-01 22:00:00' AS timestamp) AS date) ==
      | CAST(CAST('1970-01-01 23:00:00' AS timestamp) AS date)
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

  createQueryTest("length.udf",
    "SELECT length(\"test\") FROM src LIMIT 1")

  createQueryTest("partitioned table scan",
    "SELECT ds, hr, key, value FROM srcpart")

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

  createQueryTest("create table as with db name within backticks",
    """
      |CREATE DATABASE IF NOT EXISTS testdb;
      |CREATE TABLE `testdb`.`createdtable` AS SELECT * FROM default.src;
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

  test("SPARK-7270: consider dynamic partition when comparing table output") {
    withTable("test_partition", "ptest") {
      sql(s"CREATE TABLE test_partition (a STRING) PARTITIONED BY (b BIGINT, c STRING)")
      sql(s"CREATE TABLE ptest (a STRING, b BIGINT, c STRING)")

      val analyzedPlan = sql(
        """
        |INSERT OVERWRITE table test_partition PARTITION (b=1, c)
        |SELECT 'a', 'c' from ptest
      """.stripMargin).queryExecution.analyzed

      assertResult(false, "Incorrect cast detected\n" + analyzedPlan) {
      var hasCast = false
        analyzedPlan.collect {
          case p: Project => p.transformExpressionsUp { case c: Cast => hasCast = true; c }
        }
        hasCast
      }
    }
  }

  // Some tests suing script transformation are skipped as it requires `/bin/bash` which
  // can be missing or differently located.
  createQueryTest("transform",
    "SELECT TRANSFORM (key) USING 'cat' AS (tKey) FROM src",
    skip = !TestUtils.testCommandAvailable("/bin/bash"))

  createQueryTest("schema-less transform",
    """
      |SELECT TRANSFORM (key, value) USING 'cat' FROM src;
      |SELECT TRANSFORM (*) USING 'cat' FROM src;
    """.stripMargin,
    skip = !TestUtils.testCommandAvailable("/bin/bash"))

  val delimiter = "'\t'"

  createQueryTest("transform with custom field delimiter",
    s"""
      |SELECT TRANSFORM (key) ROW FORMAT DELIMITED FIELDS TERMINATED BY ${delimiter}
      |USING 'cat' AS (tKey) ROW FORMAT DELIMITED FIELDS TERMINATED BY ${delimiter} FROM src;
    """.stripMargin.replaceAll("\n", " "),
    skip = !TestUtils.testCommandAvailable("/bin/bash"))

  createQueryTest("transform with custom field delimiter2",
    s"""
      |SELECT TRANSFORM (key, value) ROW FORMAT DELIMITED FIELDS TERMINATED BY ${delimiter}
      |USING 'cat' ROW FORMAT DELIMITED FIELDS TERMINATED BY ${delimiter} FROM src;
    """.stripMargin.replaceAll("\n", " "),
    skip = !TestUtils.testCommandAvailable("/bin/bash"))

  createQueryTest("transform with custom field delimiter3",
    s"""
      |SELECT TRANSFORM (*) ROW FORMAT DELIMITED FIELDS TERMINATED BY ${delimiter}
      |USING 'cat' ROW FORMAT DELIMITED FIELDS TERMINATED BY ${delimiter} FROM src;
    """.stripMargin.replaceAll("\n", " "),
    skip = !TestUtils.testCommandAvailable("/bin/bash"))

  createQueryTest("transform with SerDe",
    """
      |SELECT TRANSFORM (key, value) ROW FORMAT SERDE
      |'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
      |USING 'cat' AS (tKey, tValue) ROW FORMAT SERDE
      |'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' FROM src;
    """.stripMargin.replaceAll(System.lineSeparator(), " "),
    skip = !TestUtils.testCommandAvailable("/bin/bash"))

  test("transform with SerDe2") {
    assume(TestUtils.testCommandAvailable("/bin/bash"))
    withTable("small_src") {
      sql("CREATE TABLE small_src(key INT, value STRING)")
      sql("INSERT OVERWRITE TABLE small_src SELECT key, value FROM src LIMIT 10")

      val expected = sql("SELECT key FROM small_src").collect().head
      val res = sql(
        """
        |SELECT TRANSFORM (key) ROW FORMAT SERDE
        |'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
        |WITH SERDEPROPERTIES ('avro.schema.literal'='{"namespace":
        |"testing.hive.avro.serde","name": "src","type": "record","fields":
        |[{"name":"key","type":"int"}]}') USING 'cat' AS (tKey INT) ROW FORMAT SERDE
        |'org.apache.hadoop.hive.serde2.avro.AvroSerDe' WITH SERDEPROPERTIES
        |('avro.schema.literal'='{"namespace": "testing.hive.avro.serde","name":
        |"src","type": "record","fields": [{"name":"key","type":"int"}]}')
        |FROM small_src
      """.stripMargin.replaceAll(System.lineSeparator(), " ")).collect().head

      assert(expected(0) === res(0))
    }
  }

  createQueryTest("transform with SerDe3",
    """
      |SELECT TRANSFORM (*) ROW FORMAT SERDE
      |'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES
      |('serialization.last.column.takes.rest'='true') USING 'cat' AS (tKey, tValue)
      |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
      |WITH SERDEPROPERTIES ('serialization.last.column.takes.rest'='true') FROM src;
    """.stripMargin.replaceAll(System.lineSeparator(), " "),
    skip = !TestUtils.testCommandAvailable("/bin/bash"))

  createQueryTest("transform with SerDe4",
    """
      |SELECT TRANSFORM (*) ROW FORMAT SERDE
      |'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES
      |('serialization.last.column.takes.rest'='true') USING 'cat' ROW FORMAT SERDE
      |'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES
      |('serialization.last.column.takes.rest'='true') FROM src;
    """.stripMargin.replaceAll(System.lineSeparator(), " "),
    skip = !TestUtils.testCommandAvailable("/bin/bash"))

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

  // scalastyle:off
  createQueryTest("lateral view4",
    """
      |create table src_lv1 (key string, value string);
      |create table src_lv2 (key string, value string);
      |
      |FROM src
      |insert overwrite table src_lv1 SELECT key, D.* lateral view explode(array(key+3, key+4)) D as CX
      |insert overwrite table src_lv2 SELECT key, D.* lateral view explode(array(key+3, key+4)) D as CX
    """.stripMargin)
  // scalastyle:on

  createQueryTest("lateral view5",
    "FROM src SELECT explode(array(key+3, key+4))")

  createQueryTest("lateral view6",
    "SELECT * FROM src LATERAL VIEW explode(map(key+3,key+4)) D as k, v")

  createQueryTest("Specify the udtf output",
    "SELECT d FROM (SELECT explode(array(1,1)) d FROM src LIMIT 1) t")

  if (!conf.ansiEnabled) {
    createQueryTest("SPARK-9034 Reflect field names defined in GenericUDTF #1",
      "SELECT col FROM (SELECT explode(array(key,value)) FROM src LIMIT 1) t")
  }

  createQueryTest("SPARK-9034 Reflect field names defined in GenericUDTF #2",
    "SELECT key,value FROM (SELECT explode(map(key,value)) FROM src LIMIT 1) t")

  test("sampling") {
    sql("SELECT * FROM src TABLESAMPLE(0.1 PERCENT) s")
    sql("SELECT * FROM src TABLESAMPLE(100 PERCENT) s")
  }

  test("DataFrame toString") {
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
    val res = sql("SELECT CAST(TIMESTAMP_SECONDS(1) AS DOUBLE) FROM src LIMIT 1").collect().head
    assert(1 == res.getDouble(0))
  }

  test("timestamp cast #2") {
    val res = sql("SELECT CAST(TIMESTAMP_SECONDS(-1) AS DOUBLE) FROM src LIMIT 1").collect().head
    assert(-1 == res.get(0))
  }

  createQueryTest("timestamp cast #3",
    "SELECT CAST(TIMESTAMP_SECONDS(1.2) AS DOUBLE) FROM src LIMIT 1")

  createQueryTest("timestamp cast #4",
    "SELECT CAST(TIMESTAMP_SECONDS(-1.2) AS DOUBLE) FROM src LIMIT 1")

  test("timestamp cast #5") {
    val res = sql("SELECT CAST(TIMESTAMP_SECONDS(1200) AS INT) FROM src LIMIT 1").collect().head
    assert(1200 == res.getInt(0))
  }

  test("timestamp cast #6") {
    val res = sql("SELECT CAST(TIMESTAMP_SECONDS(-1200) AS INT) FROM src LIMIT 1").collect().head
    assert(-1200 == res.getInt(0))
  }

  createQueryTest("select null from table",
    "SELECT null FROM src LIMIT 1")

  createQueryTest("CTE feature #1",
    "with q1 as (select key from src) select * from q1 where key = 5")

  createQueryTest("CTE feature #2",
    """with q1 as (select * from src where key= 5),
      |q2 as (select * from src s2 where key = 4)
      |select value from q1 union all select value from q2
    """.stripMargin)

  createQueryTest("CTE feature #3",
    """with q1 as (select key from src)
      |from q1
      |select * where key = 4
    """.stripMargin)

  // test get_json_object again Hive, because the HiveCompatibilitySuite cannot handle result
  // with newline in it.
  createQueryTest("get_json_object #1",
    "SELECT get_json_object(src_json.json, '$') FROM src_json")

  createQueryTest("get_json_object #2",
    "SELECT get_json_object(src_json.json, '$.owner'), get_json_object(src_json.json, '$.store')" +
      " FROM src_json")

  createQueryTest("get_json_object #3",
    "SELECT get_json_object(src_json.json, '$.store.bicycle'), " +
      "get_json_object(src_json.json, '$.store.book') FROM src_json")

  createQueryTest("get_json_object #4",
    "SELECT get_json_object(src_json.json, '$.store.book[0]'), " +
      "get_json_object(src_json.json, '$.store.book[*]') FROM src_json")

  createQueryTest("get_json_object #5",
    "SELECT get_json_object(src_json.json, '$.store.book[0].category'), " +
      "get_json_object(src_json.json, '$.store.book[*].category'), " +
      "get_json_object(src_json.json, '$.store.book[*].isbn'), " +
      "get_json_object(src_json.json, '$.store.book[*].reader') FROM src_json")

  createQueryTest("get_json_object #6",
    "SELECT get_json_object(src_json.json, '$.store.book[*].reader[0].age'), " +
      "get_json_object(src_json.json, '$.store.book[*].reader[*].age') FROM src_json")

  createQueryTest("get_json_object #7",
    "SELECT get_json_object(src_json.json, '$.store.basket[0][1]'), " +
      "get_json_object(src_json.json, '$.store.basket[*]'), " +
      // Hive returns wrong result with [*][0], so this expression is change to make test pass
      "get_json_object(src_json.json, '$.store.basket[0][0]'), " +
      "get_json_object(src_json.json, '$.store.basket[0][*]'), " +
      "get_json_object(src_json.json, '$.store.basket[*][*]'), " +
      "get_json_object(src_json.json, '$.store.basket[0][2].b'), " +
      "get_json_object(src_json.json, '$.store.basket[0][*].b') FROM src_json")

  createQueryTest("get_json_object #8",
    "SELECT get_json_object(src_json.json, '$.non_exist_key'), " +
      "get_json_object(src_json.json, '$..no_recursive'), " +
      "get_json_object(src_json.json, '$.store.book[10]'), " +
      "get_json_object(src_json.json, '$.store.book[0].non_exist_key'), " +
      "get_json_object(src_json.json, '$.store.basket[*].non_exist_key'), " +
      "get_json_object(src_json.json, '$.store.basket[0][*].non_exist_key') FROM src_json")

  createQueryTest("get_json_object #9",
    "SELECT get_json_object(src_json.json, '$.zip code') FROM src_json")

  createQueryTest("get_json_object #10",
    "SELECT get_json_object(src_json.json, '$.fb:testid') FROM src_json")

  test("predicates contains an empty AttributeSet() references") {
    sql(
      """
        |SELECT a FROM (
        |  SELECT 1 AS a FROM src LIMIT 1 ) t
        |WHERE abs(20141202) is not null
      """.stripMargin).collect()
  }

  test("implement identity function using case statement") {
    val actual = sql("SELECT (CASE key WHEN key THEN key END) FROM src")
      .rdd
      .map { case Row(i: Int) => i }
      .collect()
      .toSet

    val expected = sql("SELECT key FROM src")
      .rdd
      .map { case Row(i: Int) => i }
      .collect()
      .toSet

    assert(actual === expected)
  }

  // TODO: adopt this test when Spark SQL has the functionality / framework to report errors.
  // See https://github.com/apache/spark/pull/1055#issuecomment-45820167 for a discussion.
  ignore("non-boolean conditions in a CaseWhen are illegal") {
    checkError(
      exception = intercept[AnalysisException] {
        sql("SELECT (CASE WHEN key > 2 THEN 3 WHEN 1 THEN 2 ELSE 0 END) FROM src").collect()
      },
      errorClass = null,
      parameters = Map.empty)
  }

  createQueryTest("case sensitivity when query Hive table",
    "SELECT srcalias.KEY, SRCALIAS.value FROM sRc SrCAlias WHERE SrCAlias.kEy < 15")

  test("case sensitivity: created temporary view") {
    withTempView("REGisteredTABle") {
      val testData =
        TestHive.sparkContext.parallelize(
          TestData(1, "str1") ::
          TestData(2, "str2") :: Nil)
      testData.toDF().createOrReplaceTempView("REGisteredTABle")

      assertResult(Array(Row(2, "str2"))) {
        sql("SELECT tablealias.A, TABLEALIAS.b FROM reGisteredTABle TableAlias " +
          "WHERE TableAliaS.a > 1").collect()
      }
    }
  }

  def isExplanation(result: DataFrame): Boolean = {
    val explanation = result.select("plan").collect().map { case Row(plan: String) => plan }
    explanation.head.startsWith("== Physical Plan ==")
  }

  test("SPARK-1704: Explain commands as a DataFrame") {
    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")

    val df = sql("explain select key, count(value) from src group by key")
    assert(isExplanation(df))

    TestHive.reset()
  }

  test("SPARK-2180: HAVING support in GROUP BY clauses (positive)") {
    withTempView("having_test") {
      val fixture = List(("foo", 2), ("bar", 1), ("foo", 4), ("bar", 3))
        .zipWithIndex.map {case ((value, attr), key) => HavingRow(key, value, attr)}
      TestHive.sparkContext.parallelize(fixture).toDF().createOrReplaceTempView("having_test")
      val results =
        sql("SELECT value, max(attr) AS attr FROM having_test GROUP BY value HAVING attr > 3")
        .collect()
        .map(x => (x.getString(0), x.getInt(1)))

      assert(results === Array(("foo", 4)))
      TestHive.reset()
    }
  }

  test("SPARK-2180: HAVING with non-boolean clause raises no exceptions") {
    sql("select key, count(*) c from src group by key having c").collect()
  }

  test("union/except/intersect") {
    assertResult(Array(Row(1), Row(1))) {
      sql("select 1 as a union all select 1 as a").collect()
    }
    assertResult(Array(Row(1))) {
      sql("select 1 as a union distinct select 1 as a").collect()
    }
    assertResult(Array(Row(1))) {
      sql("select 1 as a union select 1 as a").collect()
    }
    assertResult(Array()) {
      sql("select 1 as a except select 1 as a").collect()
    }
    assertResult(Array(Row(1))) {
      sql("select 1 as a intersect select 1 as a").collect()
    }
  }

  test("SPARK-5383 alias for udfs with multi output columns") {
    assert(
      sql("select stack(2, key, value, key, value) as (a, b) from src limit 5")
        .collect()
        .size == 5)

    assert(
      sql("select a, b from (select stack(2, key, value, key, value) as (a, b) from src) t limit 5")
        .collect()
        .size == 5)
  }

  test("SPARK-5367: resolve star expression in udf") {
    assert(sql("select concat(*) from src limit 5").collect().size == 5)
    assert(sql("select concat(key, *) from src limit 5").collect().size == 5)
    if (!conf.ansiEnabled) {
      assert(sql("select array(*) from src limit 5").collect().size == 5)
      assert(sql("select array(key, *) from src limit 5").collect().size == 5)
    }
  }

  test("Exactly once semantics for DDL and command statements") {
    val tableName = "test_exactly_once"
    withTable(tableName) {
      val q0 = sql(s"CREATE TABLE $tableName(key INT, value STRING)")

      // If the table was not created, the following assertion would fail
      assert(Try(table(tableName)).isSuccess)

      // If the CREATE TABLE command got executed again, the following assertion would fail
      assert(Try(q0.count()).isSuccess)
    }
  }

  test("SPARK-2263: Insert Map<K, V> values") {
    withTable("m") {
      sql("CREATE TABLE m(value MAP<INT, STRING>)")
      sql("INSERT OVERWRITE TABLE m SELECT MAP(key, value) FROM src LIMIT 10")
      sql("SELECT * FROM m").collect().zip(sql("SELECT * FROM src LIMIT 10").collect()).foreach {
        case (Row(map: Map[_, _]), Row(key: Int, value: String)) =>
          assert(map.size === 1)
          assert(map.head === ((key, value)))
      }
    }
  }

  test("ADD JAR command") {
    sql("CREATE TABLE alter1(a INT, b INT)")
    checkError(
      exception = intercept[AnalysisException] {
        sql(
          """ALTER TABLE alter1 SET SERDE 'org.apache.hadoop.hive.serde2.TestSerDe'
            |WITH serdeproperties('s1'='9')""".stripMargin)
      },
      errorClass = null,
      parameters = Map.empty)
    sql("DROP TABLE alter1")
  }

  test("ADD JAR command 2") {
    // this is a test case from mapjoin_addjar.q
    val testJar = HiveTestJars.getHiveHcatalogCoreJar().toURI
    val testData = TestHive.getHiveFile("data/files/sample.json").toURI
    sql(s"ADD JAR $testJar")
    sql(
      """CREATE TABLE t1(a string, b string)
      |ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'""".stripMargin)
    sql(s"""LOAD DATA LOCAL INPATH "$testData" INTO TABLE t1""")
    sql("select * from src join t1 on src.key = t1.a")
    sql("DROP TABLE t1")
    assert(sql("list jars").
      filter(_.getString(0).contains(HiveTestJars.getHiveHcatalogCoreJar().getName)).count() > 0)
    assert(sql("list jar").
      filter(_.getString(0).contains(HiveTestJars.getHiveHcatalogCoreJar().getName)).count() > 0)
    val testJar2 = TestHive.getHiveFile("TestUDTF.jar").getCanonicalPath
    sql(s"ADD JAR $testJar2")
    assert(sql(s"list jar $testJar").count() == 1)
  }

  test("SPARK-34955: ADD JAR should treat paths which contains white spaces") {
    withTempDir { dir =>
      val file = File.createTempFile("someprefix1", "somesuffix1", dir)
      Files.write(file.toPath, "test_file1".getBytes)
      val jarFile = new File(dir, "test file.jar")
      TestUtils.createJar(Seq(file), jarFile)
      sql(s"ADD JAR '${jarFile.getAbsolutePath}'")
      assert(sql("LIST JARS").
        filter(_.getString(0).contains(s"${jarFile.getName}".replace(" ", "%20"))).count() > 0)
    }
  }

  test("CREATE TEMPORARY FUNCTION") {
    val funcJar = TestHive.getHiveFile("TestUDTF.jar")
    val jarURL = funcJar.toURI.toURL
    sql(s"ADD JAR $jarURL")
    sql(
      """CREATE TEMPORARY FUNCTION udtf_count2 AS
        |'org.apache.spark.sql.hive.execution.GenericUDTFCount2'
      """.stripMargin)
    assert(sql("DESCRIBE FUNCTION udtf_count2").count > 1)
    sql("DROP TEMPORARY FUNCTION udtf_count2")
  }

  test("ADD FILE command") {
    val testFile = TestHive.getHiveFile("data/files/v1.txt").toURI
    sql(s"ADD FILE $testFile")

    val checkAddFileRDD = sparkContext.parallelize(1 to 2, 1).mapPartitions { _ =>
      Iterator.single(new File(SparkFiles.get("v1.txt")).canRead)
    }

    assert(checkAddFileRDD.first())
    assert(sql("list files").
      filter(_.getString(0).contains("data/files/v1.txt")).count() > 0)
    assert(sql("list file").
      filter(_.getString(0).contains("data/files/v1.txt")).count() > 0)
    assert(sql(s"list file $testFile").count() == 1)
  }

  test("ADD ARCHIVE/LIST ARCHIVES commands") {
    withTempDir { dir =>
      val file1 = File.createTempFile("someprefix1", "somesuffix1", dir)
      val file2 = File.createTempFile("someprefix2", "somesuffix2", dir)

      Files.write(file1.toPath, "file1".getBytes)
      Files.write(file2.toPath, "file2".getBytes)

      val zipFile = new File(dir, "test.zip")
      val jarFile = new File(dir, "test.jar")
      TestUtils.createJar(Seq(file1), zipFile)
      TestUtils.createJar(Seq(file2), jarFile)

      sql(s"ADD ARCHIVE ${zipFile.getAbsolutePath}#foo")
      sql(s"ADD ARCHIVE ${jarFile.getAbsolutePath}#bar")

      val checkAddArchive =
        sparkContext.parallelize(
          Seq(
            "foo",
            s"foo/${file1.getName}",
            "nonexistence",
            "bar",
            s"bar/${file2.getName}"), 1).map { name =>
          val file = new File(SparkFiles.get(name))
          val contents =
            if (file.isFile) {
              Some(String.join("", new String(Files.readAllBytes(file.toPath))))
            } else {
              None
            }
          (name, file.canRead, contents)
        }.collect()

      assert(checkAddArchive(0) === ("foo", true, None))
      assert(checkAddArchive(1) === (s"foo/${file1.getName}", true, Some("file1")))
      assert(checkAddArchive(2) === ("nonexistence", false, None))
      assert(checkAddArchive(3) === ("bar", true, None))
      assert(checkAddArchive(4) === (s"bar/${file2.getName}", true, Some("file2")))
      assert(sql("list archives").
        filter(_.getString(0).contains(s"${zipFile.getAbsolutePath}")).count() > 0)
      assert(sql("list archive").
        filter(_.getString(0).contains(s"${jarFile.getAbsolutePath}")).count() > 0)
      assert(sql(s"list archive ${zipFile.getAbsolutePath}").count() === 1)
      assert(sql(s"list archives ${zipFile.getAbsolutePath} nonexistence").count() === 1)
      assert(sql(s"list archives ${zipFile.getAbsolutePath} " +
        s"${jarFile.getAbsolutePath}").count === 2)
    }
  }

  test("ADD ARCHIVE/List ARCHIVES commands - unsupported archive formats") {
    withTempDir { dir =>
      val file1 = File.createTempFile("someprefix1", "somesuffix1", dir)
      val file2 = File.createTempFile("someprefix2", "somesuffix2", dir)

      Files.write(file1.toPath, "file1".getBytes)
      Files.write(file2.toPath, "file2".getBytes)

      // Emulate unsupported archive formats with .bz2 and .xz suffix.
      val bz2File = new File(dir, "test.bz2")
      val xzFile = new File(dir, "test.xz")
      TestUtils.createJar(Seq(file1), bz2File)
      TestUtils.createJar(Seq(file2), xzFile)

      sql(s"ADD ARCHIVE ${bz2File.getAbsolutePath}#foo")
      sql(s"ADD ARCHIVE ${xzFile.getAbsolutePath}#bar")

      val checkAddArchive =
        sparkContext.parallelize(
          Seq(
            "foo",
            "bar"), 1).map { name =>
          val file = new File(SparkFiles.get(name))
          val contents =
            if (file.isFile) {
              Some(Files.readAllBytes(file.toPath).toSeq)
            } else {
              None
            }
          (name, file.canRead, contents)
        }.collect()

      assert(checkAddArchive(0) === ("foo", true, Some(Files.readAllBytes(bz2File.toPath).toSeq)))
      assert(checkAddArchive(1) === ("bar", true, Some(Files.readAllBytes(xzFile.toPath).toSeq)))
      assert(sql("list archives").
        filter(_.getString(0).contains(s"${bz2File.getAbsolutePath}")).count() > 0)
      assert(sql("list archive").
        filter(_.getString(0).contains(s"${xzFile.getAbsolutePath}")).count() > 0)
      assert(sql(s"list archive ${bz2File.getAbsolutePath}").count() === 1)
      assert(sql(s"list archives ${bz2File.getAbsolutePath} " +
        s"${xzFile.getAbsolutePath}").count === 2)
    }
  }

  test("SPARK-35105: ADD FILES command with multiple files") {
    withTempDir { dir =>
      val file1 = File.createTempFile("someprefix1", "somesuffix1", dir)
      val file2 = File.createTempFile("someprefix2", "somesuffix 2", dir)
      val file3 = File.createTempFile("someprefix3", "somesuffix 3", dir)
      val file4 = File.createTempFile("someprefix4", "somesuffix4", dir)

      Files.write(file1.toPath, "file1".getBytes)
      Files.write(file2.toPath, "file2".getBytes)
      Files.write(file3.toPath, "file3".getBytes)
      Files.write(file4.toPath, "file3".getBytes)

      sql(s"ADD FILE ${file1.getAbsolutePath} '${file2.getAbsoluteFile}'")
      sql(s"""ADD FILES "${file3.getAbsolutePath}" ${file4.getAbsoluteFile}""")
      val listFiles = sql(s"LIST FILES ${file1.getAbsolutePath} " +
        s"'${file2.getAbsolutePath}' '${file3.getAbsolutePath}' ${file4.getAbsolutePath}")
      assert(listFiles.count === 4)
      assert(listFiles.filter(_.getString(0).contains(file1.getName)).count() === 1)
      assert(listFiles.filter(
        _.getString(0).contains(file2.getName.replace(" ", "%20"))).count() === 1)
      assert(listFiles.filter(
        _.getString(0).contains(file3.getName.replace(" ", "%20"))).count() === 1)
      assert(listFiles.filter(_.getString(0).contains(file4.getName)).count() === 1)
    }
  }

  test("SPARK-35105: ADD JARS command with multiple files") {
    withTempDir { dir =>
      val file1 = new File(dir, "test1.txt")
      val file2 = new File(dir, "test2.txt")
      val file3 = new File(dir, "test3.txt")
      val file4 = new File(dir, "test4.txt")

      Files.write(file1.toPath, "file1".getBytes)
      Files.write(file2.toPath, "file2".getBytes)
      Files.write(file3.toPath, "file3".getBytes)
      Files.write(file4.toPath, "file4".getBytes)

      val jarFile1 = File.createTempFile("someprefix1", "somesuffix 1", dir)
      val jarFile2 = File.createTempFile("someprefix2", "somesuffix2", dir)
      val jarFile3 = File.createTempFile("someprefix3", "somesuffix3", dir)
      val jarFile4 = File.createTempFile("someprefix4", "somesuffix 4", dir)

      TestUtils.createJar(Seq(file1), jarFile1)
      TestUtils.createJar(Seq(file2), jarFile2)
      TestUtils.createJar(Seq(file3), jarFile3)
      TestUtils.createJar(Seq(file4), jarFile4)

      sql(s"""ADD JAR "${jarFile1.getAbsolutePath}" ${jarFile2.getAbsoluteFile}""")
      sql(s"ADD JARS ${jarFile3.getAbsolutePath} '${jarFile4.getAbsoluteFile}'")
      val listFiles = sql(s"LIST JARS '${jarFile1.getAbsolutePath}' " +
        s"${jarFile2.getAbsolutePath} ${jarFile3.getAbsolutePath} '${jarFile4.getAbsoluteFile}'")
      assert(listFiles.count === 4)
      assert(listFiles.filter(
        _.getString(0).contains(jarFile1.getName.replace(" ", "%20"))).count() === 1)
      assert(listFiles.filter(_.getString(0).contains(jarFile2.getName)).count() === 1)
      assert(listFiles.filter(_.getString(0).contains(jarFile3.getName)).count() === 1)
      assert(listFiles.filter(
        _.getString(0).contains(jarFile4.getName.replace(" ", "%20"))).count() === 1)
    }
  }

  test("SPARK-35105: ADD ARCHIVES command with multiple files") {
    withTempDir { dir =>
      val file1 = new File(dir, "test1.txt")
      val file2 = new File(dir, "test2.txt")
      val file3 = new File(dir, "test3.txt")
      val file4 = new File(dir, "test4.txt")

      Files.write(file1.toPath, "file1".getBytes)
      Files.write(file2.toPath, "file2".getBytes)
      Files.write(file3.toPath, "file3".getBytes)
      Files.write(file4.toPath, "file4".getBytes)

      val jarFile1 = File.createTempFile("someprefix1", "somesuffix1", dir)
      val jarFile2 = File.createTempFile("someprefix2", "somesuffix 2", dir)
      val jarFile3 = File.createTempFile("someprefix3", "somesuffix3", dir)
      val jarFile4 = File.createTempFile("someprefix4", "somesuffix 4", dir)

      TestUtils.createJar(Seq(file1), jarFile1)
      TestUtils.createJar(Seq(file2), jarFile2)
      TestUtils.createJar(Seq(file3), jarFile3)
      TestUtils.createJar(Seq(file4), jarFile4)

      sql(s"""ADD ARCHIVE ${jarFile1.getAbsolutePath} "${jarFile2.getAbsoluteFile}"""")
      sql(s"ADD ARCHIVES ${jarFile3.getAbsolutePath} '${jarFile4.getAbsoluteFile}'")
      val listFiles = sql(s"LIST ARCHIVES ${jarFile1.getAbsolutePath} " +
        s"'${jarFile2.getAbsolutePath}' ${jarFile3.getAbsolutePath} '${jarFile4.getAbsolutePath}'")
      assert(listFiles.count === 4)
      assert(listFiles.filter(_.getString(0).contains(jarFile1.getName)).count() === 1)
      assert(listFiles.filter(
        _.getString(0).contains(jarFile2.getName.replace(" ", "%20"))).count() === 1)
      assert(listFiles.filter(_.getString(0).contains(jarFile3.getName)).count() === 1)
      assert(listFiles.filter(
        _.getString(0).contains(jarFile4.getName.replace(" ", "%20"))).count() === 1)
    }
  }

  test("SPARK-34977: LIST FILES/JARS/ARCHIVES should handle multiple quoted path arguments") {
    withTempDir { dir =>
      val file1 = File.createTempFile("someprefix1", "somesuffix1", dir)
      val file2 = File.createTempFile("someprefix2", "somesuffix2", dir)
      val file3 = File.createTempFile("someprefix3", "somesuffix 3", dir)

      Files.write(file1.toPath, "file1".getBytes)
      Files.write(file2.toPath, "file2".getBytes)
      Files.write(file3.toPath, "file3".getBytes)

      sql(s"ADD FILE ${file1.getAbsolutePath}")
      sql(s"ADD FILE ${file2.getAbsolutePath}")
      sql(s"ADD FILE '${file3.getAbsolutePath}'")
      val listFiles = sql("LIST FILES " +
        s"""'${file1.getAbsolutePath}' ${file2.getAbsolutePath} "${file3.getAbsolutePath}"""")

      assert(listFiles.count === 3)
      assert(listFiles.filter(_.getString(0).contains(file1.getName)).count() === 1)
      assert(listFiles.filter(_.getString(0).contains(file2.getName)).count() === 1)
      assert(listFiles.filter(
        _.getString(0).contains(file3.getName.replace(" ", "%20"))).count() === 1)

      val file4 = File.createTempFile("someprefix4", "somesuffix4", dir)
      val file5 = File.createTempFile("someprefix5", "somesuffix5", dir)
      val file6 = File.createTempFile("someprefix6", "somesuffix6", dir)
      Files.write(file4.toPath, "file4".getBytes)
      Files.write(file5.toPath, "file5".getBytes)
      Files.write(file6.toPath, "file6".getBytes)

      val jarFile1 = new File(dir, "test1.jar")
      val jarFile2 = new File(dir, "test2.jar")
      val jarFile3 = new File(dir, "test 3.jar")
      TestUtils.createJar(Seq(file4), jarFile1)
      TestUtils.createJar(Seq(file5), jarFile2)
      TestUtils.createJar(Seq(file6), jarFile3)

      sql(s"ADD ARCHIVE ${jarFile1.getAbsolutePath}")
      sql(s"ADD ARCHIVE ${jarFile2.getAbsolutePath}#foo")
      sql(s"ADD ARCHIVE '${jarFile3.getAbsolutePath}'")
      val listArchives = sql(s"LIST ARCHIVES '${jarFile1.getAbsolutePath}' " +
        s"""${jarFile2.getAbsolutePath} "${jarFile3.getAbsolutePath}"""")

      assert(listArchives.count === 3)
      assert(listArchives.filter(_.getString(0).contains(jarFile1.getName)).count() === 1)
      assert(listArchives.filter(_.getString(0).contains(jarFile2.getName)).count() === 1)
      assert(listArchives.filter(
        _.getString(0).contains(jarFile3.getName.replace(" ", "%20"))).count() === 1)

      val file7 = File.createTempFile("someprefix7", "somesuffix7", dir)
      val file8 = File.createTempFile("someprefix8", "somesuffix8", dir)
      val file9 = File.createTempFile("someprefix9", "somesuffix9", dir)
      Files.write(file4.toPath, "file7".getBytes)
      Files.write(file5.toPath, "file8".getBytes)
      Files.write(file6.toPath, "file9".getBytes)

      val jarFile4 = new File(dir, "test4.jar")
      val jarFile5 = new File(dir, "test5.jar")
      val jarFile6 = new File(dir, "test 6.jar")
      TestUtils.createJar(Seq(file7), jarFile4)
      TestUtils.createJar(Seq(file8), jarFile5)
      TestUtils.createJar(Seq(file9), jarFile6)

      sql(s"ADD JAR ${jarFile4.getAbsolutePath}")
      sql(s"ADD JAR ${jarFile5.getAbsolutePath}")
      sql(s"ADD JAR '${jarFile6.getAbsolutePath}'")
      val listJars = sql(s"LIST JARS '${jarFile4.getAbsolutePath}' " +
        s"""${jarFile5.getAbsolutePath} "${jarFile6.getAbsolutePath}"""")
      assert(listJars.count === 3)
      assert(listJars.filter(_.getString(0).contains(jarFile4.getName)).count() === 1)
      assert(listJars.filter(_.getString(0).contains(jarFile5.getName)).count() === 1)
      assert(listJars.filter(
        _.getString(0).contains(jarFile6.getName.replace(" ", "%20"))).count() === 1)
    }
  }

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

  ignore("Dynamic partition folder layout") {
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
            s"$k=${ConfVars.DEFAULTPARTITIONNAME.defaultStrVal}"
          } else {
            s"$k=$v"
          }
        }
        .mkString("/")

      // Loads partition data to a temporary table to verify contents
      val warehousePathFile = new URI(sparkSession.getWarehousePath()).getPath
      val path = s"$warehousePathFile/dynamic_part_table/$partFolder/part-00000"

      sql("DROP TABLE IF EXISTS dp_verify")
      sql("CREATE TABLE dp_verify(intcol INT)")
      sql(s"LOAD DATA LOCAL INPATH '$path' INTO TABLE dp_verify")

      assert(sql("SELECT * FROM dp_verify").collect() === Array(Row(value)))
    }
  }

  test("SPARK-5592: get java.net.URISyntaxException when dynamic partitioning") {
    sql("""
      |create table sc as select *
      |from (select '2011-01-11', '2011-01-11+14:18:26' from src tablesample (1 rows)
      |union all
      |select '2011-01-11', '2011-01-11+15:18:26' from src tablesample (1 rows)
      |union all
      |select '2011-01-11', '2011-01-11+16:18:26' from src tablesample (1 rows) ) s
    """.stripMargin)
    sql("create table sc_part (key string) partitioned by (ts string) stored as rcfile")
    sql("set hive.exec.dynamic.partition=true")
    sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sql("insert overwrite table sc_part partition(ts) select * from sc")
    sql("drop table sc_part")
  }

  test("Partition spec validation") {
    sql("DROP TABLE IF EXISTS dp_test")
    sql("CREATE TABLE dp_test(key INT, value STRING) PARTITIONED BY (dp INT, sp INT)")
    sql("SET hive.exec.dynamic.partition.mode=strict")

    // Should throw when using strict dynamic partition mode without any static partition
    checkError(
      exception = intercept[AnalysisException] {
        sql(
          """INSERT INTO TABLE dp_test PARTITION(dp)
            |SELECT key, value, key % 5 FROM src""".stripMargin)
      },
      errorClass = "_LEGACY_ERROR_TEMP_1168",
      parameters = Map(
        "tableName" -> "`spark_catalog`.`default`.`dp_test`",
        "targetColumns" -> "4",
        "insertedColumns" -> "3",
        "staticPartCols" -> "0"))

    sql("SET hive.exec.dynamic.partition.mode=nonstrict")

    // Should throw when a static partition appears after a dynamic partition
    checkError(
      exception = intercept[AnalysisException] {
        sql(
          """INSERT INTO TABLE dp_test PARTITION(dp, sp = 1)
            |SELECT key, value, key % 5 FROM src""".stripMargin)
      },
      errorClass = null,
      parameters = Map.empty)
  }

  test("SPARK-3414 regression: should store analyzed logical plan when creating a temporary view") {
    withTempView("rawLogs", "logFiles", "boom") {
      sparkContext.makeRDD(Seq.empty[LogEntry]).toDF().createOrReplaceTempView("rawLogs")
      sparkContext.makeRDD(Seq.empty[LogFile]).toDF().createOrReplaceTempView("logFiles")

      sql(
        """
        SELECT name, message
        FROM rawLogs
        JOIN (
          SELECT name
          FROM logFiles
        ) files
        ON rawLogs.filename = files.name
        """).createOrReplaceTempView("boom")

      // This should be successfully analyzed
      sql("SELECT * FROM boom").queryExecution.analyzed
    }
  }

  test("SPARK-3810: PreprocessTableInsertion static partitioning support") {
    val analyzedPlan = {
      loadTestTable("srcpart")
      sql("DROP TABLE IF EXISTS withparts")
      sql("CREATE TABLE withparts LIKE srcpart")
      sql("INSERT INTO TABLE withparts PARTITION(ds='1', hr='2') SELECT key, value FROM src")
        .queryExecution.analyzed
      }

    assertResult(1, "Duplicated project detected\n" + analyzedPlan) {
      analyzedPlan.collect {
        case i: InsertIntoHiveTable => i.query.collect { case p: Project => () }.size
      }.sum
    }
  }

  test("SPARK-3810: PreprocessTableInsertion dynamic partitioning support") {
    val analyzedPlan = {
      loadTestTable("srcpart")
      sql("DROP TABLE IF EXISTS withparts")
      sql("CREATE TABLE withparts LIKE srcpart")
      sql("SET hive.exec.dynamic.partition.mode=nonstrict")

      sql("CREATE TABLE IF NOT EXISTS withparts LIKE srcpart")
      sql("INSERT INTO TABLE withparts PARTITION(ds, hr) SELECT key, value, '1', '2' FROM src")
        .queryExecution.analyzed
    }

    assertResult(2, "Duplicated project detected\n" + analyzedPlan) {
      analyzedPlan.collect {
        case i: InsertIntoHiveTable => i.query.collect { case p: Project => () }.size
      }.sum
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

  test("current_database with multiple sessions") {
    sql("create database a")
    sql("use a")
    val s2 = newSession()
    s2.sql("create database b")
    s2.sql("use b")

    assert(sql("select current_database()").first() === Row("a"))
    assert(s2.sql("select current_database()").first() === Row("b"))

    try {
      sql("create table test_a(key INT, value STRING)")
      s2.sql("create table test_b(key INT, value STRING)")

      sql("select * from test_a")
      checkError(
        exception = intercept[AnalysisException] {
          sql("select * from test_b")
        },
        errorClass = "TABLE_OR_VIEW_NOT_FOUND",
        parameters = Map("relationName" -> "`test_b`"),
        context = ExpectedContext(
          fragment = "test_b",
          start = 14,
          stop = 19))

      sql("select * from b.test_b")

      s2.sql("select * from test_b")
      checkError(
        exception = intercept[AnalysisException] {
          s2.sql("select * from test_a")
        },
        errorClass = "TABLE_OR_VIEW_NOT_FOUND",
        parameters = Map("relationName" -> "`test_a`"),
        context = ExpectedContext(
          fragment = "test_a",
          start = 14,
          stop = 19))
      s2.sql("select * from a.test_a")
    } finally {
      sql("DROP TABLE IF EXISTS test_a")
      s2.sql("DROP TABLE IF EXISTS test_b")
    }

  }

  test("use database") {
    val currentDatabase = sql("select current_database()").first().getString(0)

    sql("CREATE DATABASE hive_test_db")
    sql("USE hive_test_db")
    assert("hive_test_db" == sql("select current_database()").first().getString(0))

    assert("hive_test_db" == sql("select current_schema()").first().getString(0))

    checkError(
      exception = intercept[AnalysisException] {
        sql("USE not_existing_db")
      },
      errorClass = "SCHEMA_NOT_FOUND",
      parameters = Map("schemaName" -> "`not_existing_db`"))

    sql(s"USE $currentDatabase")
    assert(currentDatabase == sql("select current_database()").first().getString(0))
  }

  test("lookup hive UDF in another thread") {
    checkError(
      exception = intercept[AnalysisException] {
        range(1).selectExpr("not_a_udf()")
      },
      errorClass = "UNRESOLVED_ROUTINE",
      sqlState = None,
      parameters = Map(
        "routineName" -> "`not_a_udf`",
        "searchPath" -> "[`system`.`builtin`, `system`.`session`, `spark_catalog`.`a`]"),
      context = ExpectedContext(
        fragment = "not_a_udf()",
        start = 0,
        stop = 10))

    var success = false
    val t = new Thread("test") {
      override def run(): Unit = {
        checkError(
          exception = intercept[AnalysisException] {
            range(1).selectExpr("not_a_udf()")
          },
          errorClass = "UNRESOLVED_ROUTINE",
          sqlState = None,
          parameters = Map(
            "routineName" -> "`not_a_udf`",
            "searchPath" -> "[`system`.`builtin`, `system`.`session`, `spark_catalog`.`a`]"),
          context = ExpectedContext(
            fragment = "not_a_udf()",
            start = 0,
            stop = 10))
        success = true
      }
    }
    t.start()
    t.join()
    assert(success)
  }

  createQueryTest("select from thrift based table",
    "SELECT * from src_thrift")

  // Put tests that depend on specific Hive settings before these last two test,
  // since they modify /clear stuff.

  test("role management commands are not supported") {
    assertUnsupportedFeature(
      sql("CREATE ROLE my_role"),
      "CREATE ROLE",
      ExpectedContext(fragment = "CREATE ROLE my_role", start = 0, stop = 18))
    assertUnsupportedFeature(
      sql("DROP ROLE my_role"),
      "DROP ROLE",
      ExpectedContext(fragment = "DROP ROLE my_role", start = 0, stop = 16))
    assertUnsupportedFeature(
      sql("SHOW CURRENT ROLES"),
      "SHOW CURRENT ROLES",
      ExpectedContext(fragment = "SHOW CURRENT ROLES", start = 0, stop = 17))
    assertUnsupportedFeature(
      sql("SHOW ROLES"),
      "SHOW ROLES",
      ExpectedContext(fragment = "SHOW ROLES", start = 0, stop = 9))
    assertUnsupportedFeature(
      sql("SHOW GRANT"),
      "SHOW GRANT",
      ExpectedContext(fragment = "SHOW GRANT", start = 0, stop = 9))
    assertUnsupportedFeature(
      sql("SHOW ROLE GRANT USER my_principal"),
      "SHOW ROLE GRANT",
      ExpectedContext(fragment = "SHOW ROLE GRANT USER my_principal", start = 0, stop = 32))
    assertUnsupportedFeature(
      sql("SHOW PRINCIPALS my_role"),
      "SHOW PRINCIPALS",
      ExpectedContext(fragment = "SHOW PRINCIPALS my_role", start = 0, stop = 22))
    assertUnsupportedFeature(
      sql("SET ROLE my_role"),
      "SET ROLE",
      ExpectedContext(fragment = "SET ROLE my_role", start = 0, stop = 15))
    assertUnsupportedFeature(
      sql("GRANT my_role TO USER my_user"),
      "GRANT",
      ExpectedContext(fragment = "GRANT my_role TO USER my_user", start = 0, stop = 28))
    assertUnsupportedFeature(
      sql("GRANT ALL ON my_table TO USER my_user"),
      "GRANT",
      ExpectedContext(fragment = "GRANT ALL ON my_table TO USER my_user", start = 0, stop = 36))
    assertUnsupportedFeature(
      sql("REVOKE my_role FROM USER my_user"),
      "REVOKE",
      ExpectedContext(fragment = "REVOKE my_role FROM USER my_user", start = 0, stop = 31))
    assertUnsupportedFeature(
      sql("REVOKE ALL ON my_table FROM USER my_user"),
      "REVOKE",
      ExpectedContext(fragment = "REVOKE ALL ON my_table FROM USER my_user", start = 0, stop = 39))
  }

  test("import/export commands are not supported") {
    assertUnsupportedFeature(
      sql("IMPORT TABLE my_table FROM 'my_path'"),
      "IMPORT TABLE",
      ExpectedContext(fragment = "IMPORT TABLE my_table FROM 'my_path'", start = 0, stop = 35))
    assertUnsupportedFeature(
      sql("EXPORT TABLE my_table TO 'my_path'"),
      "EXPORT TABLE",
      ExpectedContext(fragment = "EXPORT TABLE my_table TO 'my_path'", start = 0, stop = 33))
  }

  test("some show commands are not supported") {
    assertUnsupportedFeature(
      sql("SHOW COMPACTIONS"),
      "SHOW COMPACTIONS",
      ExpectedContext(fragment = "SHOW COMPACTIONS", start = 0, stop = 15))
    assertUnsupportedFeature(
      sql("SHOW TRANSACTIONS"),
      "SHOW TRANSACTIONS",
      ExpectedContext(fragment = "SHOW TRANSACTIONS", start = 0, stop = 16))
    assertUnsupportedFeature(
      sql("SHOW INDEXES ON my_table"),
      "SHOW INDEXES",
      ExpectedContext(fragment = "SHOW INDEXES ON my_table", start = 0, stop = 23))
    assertUnsupportedFeature(
      sql("SHOW LOCKS my_table"),
      "SHOW LOCKS",
      ExpectedContext(fragment = "SHOW LOCKS my_table", start = 0, stop = 18))
  }

  test("lock/unlock table and database commands are not supported") {
    assertUnsupportedFeature(
      sql("LOCK TABLE my_table SHARED"),
      "LOCK TABLE",
      ExpectedContext(fragment = "LOCK TABLE my_table SHARED", start = 0, stop = 25))
    assertUnsupportedFeature(
      sql("UNLOCK TABLE my_table"),
      "UNLOCK TABLE",
      ExpectedContext(fragment = "UNLOCK TABLE my_table", start = 0, stop = 20))
    assertUnsupportedFeature(
      sql("LOCK DATABASE my_db SHARED"),
      "LOCK DATABASE",
      ExpectedContext(fragment = "LOCK DATABASE my_db SHARED", start = 0, stop = 25))
    assertUnsupportedFeature(
      sql("UNLOCK DATABASE my_db"),
      "UNLOCK DATABASE",
      ExpectedContext(fragment = "UNLOCK DATABASE my_db", start = 0, stop = 20))
  }

  test("alter index command is not supported") {
    val sql1 = "ALTER INDEX my_index ON my_table REBUILD"
    assertUnsupportedFeature(
      sql(sql1),
      "ALTER INDEX",
      ExpectedContext(fragment = sql1, start = 0, stop = 39))
    val sql2 = "ALTER INDEX my_index ON my_table set IDXPROPERTIES (\"prop1\"=\"val1_new\")"
    assertUnsupportedFeature(
      sql(sql2),
      "ALTER INDEX",
      ExpectedContext(fragment = sql2, start = 0, stop = 70))
  }

  test("create/drop macro commands are not supported") {
    val sql1 = "CREATE TEMPORARY MACRO SIGMOID (x DOUBLE) 1.0 / (1.0 + EXP(-x))"
    assertUnsupportedFeature(
      sql(sql1),
      "CREATE TEMPORARY MACRO",
      ExpectedContext(fragment = sql1, start = 0, stop = 62))
    val sql2 = "DROP TEMPORARY MACRO SIGMOID"
    assertUnsupportedFeature(
      sql(sql2),
      "DROP TEMPORARY MACRO",
      ExpectedContext(fragment = sql2, start = 0, stop = 27))
  }

  test("dynamic partitioning is allowed when hive.exec.dynamic.partition.mode is nonstrict") {
    val modeConfKey = "hive.exec.dynamic.partition.mode"
    withTable("with_parts") {
      sql("CREATE TABLE with_parts(key INT) PARTITIONED BY (p INT)")

      withSQLConf(modeConfKey -> "nonstrict") {
        sql("INSERT OVERWRITE TABLE with_parts partition(p) select 1, 2")
        assert(spark.table("with_parts").filter($"p" === 2).collect().head == Row(1, 2))
      }

      // Turn off style check since the following test is to modify hadoop configuration on purpose.
      // scalastyle:off hadoopconfiguration
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      // scalastyle:on hadoopconfiguration

      val originalValue = hadoopConf.get(modeConfKey, "nonstrict")
      try {
        hadoopConf.set(modeConfKey, "nonstrict")
        sql("INSERT OVERWRITE TABLE with_parts partition(p) select 3, 4")
        assert(spark.table("with_parts").filter($"p" === 4).collect().head == Row(3, 4))
      } finally {
        hadoopConf.set(modeConfKey, originalValue)
      }
    }
  }

  test("SPARK-28054: Unable to insert partitioned table when partition name is upper case") {
    withSQLConf("hive.exec.dynamic.partition.mode" -> "nonstrict") {
      withTable("spark_28054_test") {
        sql("CREATE TABLE spark_28054_test (KEY STRING, VALUE STRING) PARTITIONED BY (DS STRING)")

        sql("INSERT INTO TABLE spark_28054_test PARTITION(DS) SELECT 'k' KEY, 'v' VALUE, '1' DS")

        assertResult(Array(Row("k", "v", "1"))) {
          sql("SELECT * from spark_28054_test").collect()
        }

        sql("INSERT INTO TABLE spark_28054_test PARTITION(ds) SELECT 'k' key, 'v' value, '2' ds")
        assertResult(Array(Row("k", "v", "1"), Row("k", "v", "2"))) {
          sql("SELECT * from spark_28054_test").collect()
        }
      }
    }
  }

  // This test case is moved from HiveCompatibilitySuite to make it easy to test with JDK 11.
  test("udf_radians") {
    withSQLConf("hive.fetch.task.conversion" -> "more") {
      val result = sql("select radians(57.2958) FROM src tablesample (1 rows)").collect()
      if (SystemUtils.isJavaVersionAtLeast(JavaVersion.JAVA_9)) {
        assertResult(Array(Row(1.0000003575641672))) (result)
      } else {
        assertResult(Array(Row(1.000000357564167))) (result)
      }

      assertResult(Array(Row(2.4999991485811655))) {
        sql("select radians(143.2394) FROM src tablesample (1 rows)").collect()
      }
    }
  }

  test("SPARK-33084: Add jar support Ivy URI in SQL") {
    val testData = TestHive.getHiveFile("data/files/sample.json").toURI
    withTable("t") {
      // hive-catalog-core has some transitive dependencies which dont exist on maven central
      // and hence cannot be found in the test environment or are non-jar (.pom) which cause
      // failures in tests. Use transitive=false as it should be good enough to test the Ivy
      // support in Hive ADD JAR
      sql(s"ADD JAR ivy://org.apache.hive.hcatalog:hive-hcatalog-core:$hiveVersion" +
        "?transitive=false")
      sql(
        """CREATE TABLE t(a string, b string)
          |ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'""".stripMargin)
      sql(s"""LOAD DATA LOCAL INPATH "$testData" INTO TABLE t""")
      sql("SELECT * FROM src JOIN t on src.key = t.a")
      assert(sql("LIST JARS").filter(_.getString(0).contains(
        s"org.apache.hive.hcatalog_hive-hcatalog-core-$hiveVersion.jar")).count() > 0)
      assert(sql("LIST JAR").
        filter(_.getString(0).contains(
          s"org.apache.hive.hcatalog_hive-hcatalog-core-$hiveVersion.jar")).count() > 0)
    }
  }
}

// for SPARK-2180 test
case class HavingRow(key: Int, value: String, attr: Int)

case class LogEntry(filename: String, message: String)
case class LogFile(name: String)
