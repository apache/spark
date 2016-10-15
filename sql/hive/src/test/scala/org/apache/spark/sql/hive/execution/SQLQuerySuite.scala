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

import java.sql.{Date, Timestamp}

import scala.sys.process.{Process, ProcessLogger}
import scala.util.Try

import org.apache.hadoop.fs.Path

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, FunctionRegistry, NoSuchPartitionException}
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.{HiveUtils, MetastoreRelation}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

case class Nested1(f1: Nested2)
case class Nested2(f2: Nested3)
case class Nested3(f3: Int)

case class NestedArray2(b: Seq[Int])
case class NestedArray1(a: NestedArray2)

case class Order(
    id: Int,
    make: String,
    `type`: String,
    price: Int,
    pdate: String,
    customer: String,
    city: String,
    state: String,
    month: Int)

/**
 * A collection of hive query tests where we generate the answers ourselves instead of depending on
 * Hive to generate them (in contrast to HiveQuerySuite).  Often this is because the query is
 * valid, but Hive currently cannot execute it.
 */
class SQLQuerySuite extends QueryTest with SQLTestUtils with TestHiveSingleton {
  import hiveContext._
  import spark.implicits._

  test("script") {
    val scriptFilePath = getTestResourcePath("test_script.sh")
    if (testCommandAvailable("bash") && testCommandAvailable("echo | sed")) {
      val df = Seq(("x1", "y1", "z1"), ("x2", "y2", "z2")).toDF("c1", "c2", "c3")
      df.createOrReplaceTempView("script_table")
      val query1 = sql(
        s"""
          |SELECT col1 FROM (from(SELECT c1, c2, c3 FROM script_table) tempt_table
          |REDUCE c1, c2, c3 USING 'bash $scriptFilePath' AS
          |(col1 STRING, col2 STRING)) script_test_table""".stripMargin)
      checkAnswer(query1, Row("x1_y1") :: Row("x2_y2") :: Nil)
    }
    // else skip this test
  }

  test("UDTF") {
    withUserDefinedFunction("udtf_count2" -> true) {
      sql(s"ADD JAR ${hiveContext.getHiveFile("TestUDTF.jar").getCanonicalPath()}")
      // The function source code can be found at:
      // https://cwiki.apache.org/confluence/display/Hive/DeveloperGuide+UDTF
      sql(
        """
          |CREATE TEMPORARY FUNCTION udtf_count2
          |AS 'org.apache.spark.sql.hive.execution.GenericUDTFCount2'
        """.stripMargin)

      checkAnswer(
        sql("SELECT key, cc FROM src LATERAL VIEW udtf_count2(value) dd AS cc"),
        Row(97, 500) :: Row(97, 500) :: Nil)

      checkAnswer(
        sql("SELECT udtf_count2(a) FROM (SELECT 1 AS a FROM src LIMIT 3) t"),
        Row(3) :: Row(3) :: Nil)
    }
  }

  test("permanent UDTF") {
    withUserDefinedFunction("udtf_count_temp" -> false) {
      sql(
        s"""
          |CREATE FUNCTION udtf_count_temp
          |AS 'org.apache.spark.sql.hive.execution.GenericUDTFCount2'
          |USING JAR '${hiveContext.getHiveFile("TestUDTF.jar").getCanonicalPath()}'
        """.stripMargin)

      checkAnswer(
        sql("SELECT key, cc FROM src LATERAL VIEW udtf_count_temp(value) dd AS cc"),
        Row(97, 500) :: Row(97, 500) :: Nil)

      checkAnswer(
        sql("SELECT udtf_count_temp(a) FROM (SELECT 1 AS a FROM src LIMIT 3) t"),
        Row(3) :: Row(3) :: Nil)
    }
  }

  test("SPARK-6835: udtf in lateral view") {
    val df = Seq((1, 1)).toDF("c1", "c2")
    df.createOrReplaceTempView("table1")
    val query = sql("SELECT c1, v FROM table1 LATERAL VIEW stack(3, 1, c1 + 1, c1 + 2) d AS v")
    checkAnswer(query, Row(1, 1) :: Row(1, 2) :: Row(1, 3) :: Nil)
  }

  test("SPARK-13651: generator outputs shouldn't be resolved from its child's output") {
    withTempView("src") {
      Seq(("id1", "value1")).toDF("key", "value").createOrReplaceTempView("src")
      val query =
        sql("SELECT genoutput.* FROM src " +
          "LATERAL VIEW explode(map('key1', 100, 'key2', 200)) genoutput AS key, value")
      checkAnswer(query, Row("key1", 100) :: Row("key2", 200) :: Nil)
    }
  }

  test("SPARK-6851: Self-joined converted parquet tables") {
    val orders = Seq(
      Order(1, "Atlas", "MTB", 234, "2015-01-07", "John D", "Pacifica", "CA", 20151),
      Order(3, "Swift", "MTB", 285, "2015-01-17", "John S", "Redwood City", "CA", 20151),
      Order(4, "Atlas", "Hybrid", 303, "2015-01-23", "Jones S", "San Mateo", "CA", 20151),
      Order(7, "Next", "MTB", 356, "2015-01-04", "Jane D", "Daly City", "CA", 20151),
      Order(10, "Next", "YFlikr", 187, "2015-01-09", "John D", "Fremont", "CA", 20151),
      Order(11, "Swift", "YFlikr", 187, "2015-01-23", "John D", "Hayward", "CA", 20151),
      Order(2, "Next", "Hybrid", 324, "2015-02-03", "Jane D", "Daly City", "CA", 20152),
      Order(5, "Next", "Street", 187, "2015-02-08", "John D", "Fremont", "CA", 20152),
      Order(6, "Atlas", "Street", 154, "2015-02-09", "John D", "Pacifica", "CA", 20152),
      Order(8, "Swift", "Hybrid", 485, "2015-02-19", "John S", "Redwood City", "CA", 20152),
      Order(9, "Atlas", "Split", 303, "2015-02-28", "Jones S", "San Mateo", "CA", 20152))

    val orderUpdates = Seq(
      Order(1, "Atlas", "MTB", 434, "2015-01-07", "John D", "Pacifica", "CA", 20151),
      Order(11, "Swift", "YFlikr", 137, "2015-01-23", "John D", "Hayward", "CA", 20151))

    orders.toDF.createOrReplaceTempView("orders1")
    orderUpdates.toDF.createOrReplaceTempView("orderupdates1")

    sql(
      """CREATE TABLE orders(
        |  id INT,
        |  make String,
        |  type String,
        |  price INT,
        |  pdate String,
        |  customer String,
        |  city String)
        |PARTITIONED BY (state STRING, month INT)
        |STORED AS PARQUET
      """.stripMargin)

    sql(
      """CREATE TABLE orderupdates(
        |  id INT,
        |  make String,
        |  type String,
        |  price INT,
        |  pdate String,
        |  customer String,
        |  city String)
        |PARTITIONED BY (state STRING, month INT)
        |STORED AS PARQUET
      """.stripMargin)

    sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sql("INSERT INTO TABLE orders PARTITION(state, month) SELECT * FROM orders1")
    sql("INSERT INTO TABLE orderupdates PARTITION(state, month) SELECT * FROM orderupdates1")

    checkAnswer(
      sql(
        """
          |select orders.state, orders.month
          |from orders
          |join (
          |  select distinct orders.state,orders.month
          |  from orders
          |  join orderupdates
          |    on orderupdates.id = orders.id) ao
          |  on ao.state = orders.state and ao.month = orders.month
        """.stripMargin),
      (1 to 6).map(_ => Row("CA", 20151)))
  }

  test("show functions") {
    val allBuiltinFunctions = FunctionRegistry.builtin.listFunction().toSet[String].toList.sorted
    val allFunctions = sql("SHOW functions").collect().map(r => r(0))
    allBuiltinFunctions.foreach { f =>
      assert(allFunctions.contains(f))
    }
    withTempDatabase { db =>
      def createFunction(names: Seq[String]): Unit = {
        names.foreach { name =>
          sql(
            s"""
              |CREATE TEMPORARY FUNCTION $name
              |AS '${classOf[PairUDF].getName}'
            """.stripMargin)
        }
      }
      def dropFunction(names: Seq[String]): Unit = {
        names.foreach { name =>
          sql(s"DROP TEMPORARY FUNCTION $name")
        }
      }
      createFunction(Seq("temp_abs", "temp_weekofyear", "temp_sha", "temp_sha1", "temp_sha2"))

      checkAnswer(sql("SHOW functions temp_abs"), Row("temp_abs"))
      checkAnswer(sql("SHOW functions 'temp_abs'"), Row("temp_abs"))
      checkAnswer(sql(s"SHOW functions $db.temp_abs"), Row("temp_abs"))
      checkAnswer(sql(s"SHOW functions `$db`.`temp_abs`"), Row("temp_abs"))
      checkAnswer(sql(s"SHOW functions `$db`.`temp_abs`"), Row("temp_abs"))
      checkAnswer(sql("SHOW functions `a function doens't exist`"), Nil)
      checkAnswer(sql("SHOW functions `temp_weekofyea*`"), Row("temp_weekofyear"))

      // this probably will failed if we add more function with `sha` prefixing.
      checkAnswer(
        sql("SHOW functions `temp_sha*`"),
        List(Row("temp_sha"), Row("temp_sha1"), Row("temp_sha2")))

      // Test '|' for alternation.
      checkAnswer(
        sql("SHOW functions 'temp_sha*|temp_weekofyea*'"),
        List(Row("temp_sha"), Row("temp_sha1"), Row("temp_sha2"), Row("temp_weekofyear")))

      dropFunction(Seq("temp_abs", "temp_weekofyear", "temp_sha", "temp_sha1", "temp_sha2"))
    }
  }

  test("describe functions - built-in functions") {
    checkKeywordsExist(sql("describe function extended upper"),
      "Function: upper",
      "Class: org.apache.spark.sql.catalyst.expressions.Upper",
      "Usage: upper(str) - Returns str with all characters changed to uppercase",
      "Extended Usage:",
      "> SELECT upper('SparkSql')",
      "'SPARKSQL'")

    checkKeywordsExist(sql("describe functioN Upper"),
      "Function: upper",
      "Class: org.apache.spark.sql.catalyst.expressions.Upper",
      "Usage: upper(str) - Returns str with all characters changed to uppercase")

    checkKeywordsNotExist(sql("describe functioN Upper"),
      "Extended Usage")

    checkKeywordsExist(sql("describe functioN abcadf"),
      "Function: abcadf not found.")

    checkKeywordsExist(sql("describe functioN  `~`"),
      "Function: ~",
      "Class: org.apache.spark.sql.catalyst.expressions.BitwiseNot",
      "Usage: ~ b - Bitwise NOT.")

    // Hard coded describe functions
    checkKeywordsExist(sql("describe function  `<>`"),
      "Function: <>",
      "Usage: a <> b - Returns TRUE if a is not equal to b")

    checkKeywordsExist(sql("describe function  `!=`"),
      "Function: !=",
      "Usage: a != b - Returns TRUE if a is not equal to b")

    checkKeywordsExist(sql("describe function  `between`"),
      "Function: between",
      "Usage: a [NOT] BETWEEN b AND c - evaluate if a is [not] in between b and c")

    checkKeywordsExist(sql("describe function  `case`"),
      "Function: case",
      "Usage: CASE a WHEN b THEN c [WHEN d THEN e]* [ELSE f] END - " +
        "When a = b, returns c; when a = d, return e; else return f")
  }

  test("describe functions - user defined functions") {
    withUserDefinedFunction("udtf_count" -> false) {
      sql(
        s"""
           |CREATE FUNCTION udtf_count
           |AS 'org.apache.spark.sql.hive.execution.GenericUDTFCount2'
           |USING JAR '${hiveContext.getHiveFile("TestUDTF.jar").getCanonicalPath()}'
        """.stripMargin)

      checkKeywordsExist(sql("describe function udtf_count"),
        "Function: default.udtf_count",
        "Class: org.apache.spark.sql.hive.execution.GenericUDTFCount2",
        "Usage: N/A")

      checkAnswer(
        sql("SELECT udtf_count(a) FROM (SELECT 1 AS a FROM src LIMIT 3) t"),
        Row(3) :: Row(3) :: Nil)

      checkKeywordsExist(sql("describe function udtf_count"),
        "Function: default.udtf_count",
        "Class: org.apache.spark.sql.hive.execution.GenericUDTFCount2",
        "Usage: N/A")
    }
  }

  test("describe functions - temporary user defined functions") {
    withUserDefinedFunction("udtf_count_temp" -> true) {
      sql(
        s"""
           |CREATE TEMPORARY FUNCTION udtf_count_temp
           |AS 'org.apache.spark.sql.hive.execution.GenericUDTFCount2'
           |USING JAR '${hiveContext.getHiveFile("TestUDTF.jar").getCanonicalPath()}'
        """.stripMargin)

      checkKeywordsExist(sql("describe function udtf_count_temp"),
        "Function: udtf_count_temp",
        "Class: org.apache.spark.sql.hive.execution.GenericUDTFCount2",
        "Usage: N/A")

      checkAnswer(
        sql("SELECT udtf_count_temp(a) FROM (SELECT 1 AS a FROM src LIMIT 3) t"),
        Row(3) :: Row(3) :: Nil)

      checkKeywordsExist(sql("describe function udtf_count_temp"),
        "Function: udtf_count_temp",
        "Class: org.apache.spark.sql.hive.execution.GenericUDTFCount2",
        "Usage: N/A")
    }
  }

  test("describe partition") {
    withTable("partitioned_table") {
      sql("CREATE TABLE partitioned_table (a STRING, b INT) PARTITIONED BY (c STRING, d STRING)")
      sql("ALTER TABLE partitioned_table ADD PARTITION (c='Us', d=1)")

      checkKeywordsExist(sql("DESC partitioned_table PARTITION (c='Us', d=1)"),
        "# Partition Information",
        "# col_name")

      checkKeywordsExist(sql("DESC EXTENDED partitioned_table PARTITION (c='Us', d=1)"),
        "# Partition Information",
        "# col_name",
        "Detailed Partition Information CatalogPartition(",
        "Partition Values: [Us, 1]",
        "Storage(Location:",
        "Partition Parameters")

      checkKeywordsExist(sql("DESC FORMATTED partitioned_table PARTITION (c='Us', d=1)"),
        "# Partition Information",
        "# col_name",
        "# Detailed Partition Information",
        "Partition Value:",
        "Database:",
        "Table:",
        "Location:",
        "Partition Parameters:",
        "# Storage Information")
    }
  }

  test("describe partition - error handling") {
    withTable("partitioned_table", "datasource_table") {
      sql("CREATE TABLE partitioned_table (a STRING, b INT) PARTITIONED BY (c STRING, d STRING)")
      sql("ALTER TABLE partitioned_table ADD PARTITION (c='Us', d=1)")

      val m = intercept[NoSuchPartitionException] {
        sql("DESC partitioned_table PARTITION (c='Us', d=2)")
      }.getMessage()
      assert(m.contains("Partition not found in table"))

      val m2 = intercept[AnalysisException] {
        sql("DESC partitioned_table PARTITION (c='Us')")
      }.getMessage()
      assert(m2.contains("Partition spec is invalid"))

      val m3 = intercept[ParseException] {
        sql("DESC partitioned_table PARTITION (c='Us', d)")
      }.getMessage()
      assert(m3.contains("PARTITION specification is incomplete: `d`"))

      spark
        .range(1).select('id as 'a, 'id as 'b, 'id as 'c, 'id as 'd).write
        .partitionBy("d")
        .saveAsTable("datasource_table")
      val m4 = intercept[AnalysisException] {
        sql("DESC datasource_table PARTITION (d=2)")
      }.getMessage()
      assert(m4.contains("DESC PARTITION is not allowed on a datasource table"))

      val m5 = intercept[AnalysisException] {
        spark.range(10).select('id as 'a, 'id as 'b).createTempView("view1")
        sql("DESC view1 PARTITION (c='Us', d=1)")
      }.getMessage()
      assert(m5.contains("DESC PARTITION is not allowed on a temporary view"))

      withView("permanent_view") {
        val m = intercept[AnalysisException] {
          sql("CREATE VIEW permanent_view AS SELECT * FROM partitioned_table")
          sql("DESC permanent_view PARTITION (c='Us', d=1)")
        }.getMessage()
        assert(m.contains("DESC PARTITION is not allowed on a view"))
      }
    }
  }

  test("SPARK-5371: union with null and sum") {
    val df = Seq((1, 1)).toDF("c1", "c2")
    df.createOrReplaceTempView("table1")

    val query = sql(
      """
        |SELECT
        |  MIN(c1),
        |  MIN(c2)
        |FROM (
        |  SELECT
        |    SUM(c1) c1,
        |    NULL c2
        |  FROM table1
        |  UNION ALL
        |  SELECT
        |    NULL c1,
        |    SUM(c2) c2
        |  FROM table1
        |) a
      """.stripMargin)
    checkAnswer(query, Row(1, 1) :: Nil)
  }

  test("CTAS with WITH clause") {
    val df = Seq((1, 1)).toDF("c1", "c2")
    df.createOrReplaceTempView("table1")

    sql(
      """
        |CREATE TABLE with_table1 AS
        |WITH T AS (
        |  SELECT *
        |  FROM table1
        |)
        |SELECT *
        |FROM T
      """.stripMargin)
    val query = sql("SELECT * FROM with_table1")
    checkAnswer(query, Row(1, 1) :: Nil)
  }

  test("explode nested Field") {
    Seq(NestedArray1(NestedArray2(Seq(1, 2, 3)))).toDF.createOrReplaceTempView("nestedArray")
    checkAnswer(
      sql("SELECT ints FROM nestedArray LATERAL VIEW explode(a.b) a AS ints"),
      Row(1) :: Row(2) :: Row(3) :: Nil)

    checkAnswer(
      sql("SELECT `ints` FROM nestedArray LATERAL VIEW explode(a.b) `a` AS `ints`"),
      Row(1) :: Row(2) :: Row(3) :: Nil)

    checkAnswer(
      sql("SELECT `a`.`ints` FROM nestedArray LATERAL VIEW explode(a.b) `a` AS `ints`"),
      Row(1) :: Row(2) :: Row(3) :: Nil)

    checkAnswer(
      sql(
        """
          |SELECT `weird``tab`.`weird``col`
          |FROM nestedArray
          |LATERAL VIEW explode(a.b) `weird``tab` AS `weird``col`
        """.stripMargin),
      Row(1) :: Row(2) :: Row(3) :: Nil)
  }

  test("SPARK-4512 Fix attribute reference resolution error when using SORT BY") {
    checkAnswer(
      sql("SELECT * FROM (SELECT key + key AS a FROM src SORT BY value) t ORDER BY t.a"),
      sql("SELECT key + key as a FROM src ORDER BY a").collect().toSeq
    )
  }

  def checkRelation(
      tableName: String,
      isDataSourceParquet: Boolean,
      format: String,
      userSpecifiedLocation: Option[String] = None): Unit = {
    val relation = EliminateSubqueryAliases(
      sessionState.catalog.lookupRelation(TableIdentifier(tableName)))
    val catalogTable =
      sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
    relation match {
      case LogicalRelation(r: HadoopFsRelation, _, _) =>
        if (!isDataSourceParquet) {
          fail(
            s"${classOf[MetastoreRelation].getCanonicalName} is expected, but found " +
              s"${HadoopFsRelation.getClass.getCanonicalName}.")
        }
        userSpecifiedLocation match {
          case Some(location) =>
            assert(r.options("path") === location)
          case None => // OK.
        }
        assert(catalogTable.provider.get === format)

      case r: MetastoreRelation =>
        if (isDataSourceParquet) {
          fail(
            s"${HadoopFsRelation.getClass.getCanonicalName} is expected, but found " +
              s"${classOf[MetastoreRelation].getCanonicalName}.")
        }
        userSpecifiedLocation match {
          case Some(location) =>
            assert(r.catalogTable.storage.locationUri.get === location)
          case None => // OK.
        }
        // Also make sure that the format is the desired format.
        assert(catalogTable.storage.inputFormat.get.toLowerCase.contains(format))
    }

    // When a user-specified location is defined, the table type needs to be EXTERNAL.
    val actualTableType = catalogTable.tableType
    userSpecifiedLocation match {
      case Some(location) =>
        assert(actualTableType === CatalogTableType.EXTERNAL)
      case None =>
        assert(actualTableType === CatalogTableType.MANAGED)
    }
  }

  test("CTAS without serde without location") {
    val originalConf = sessionState.conf.convertCTAS

    setConf(SQLConf.CONVERT_CTAS, true)

    val defaultDataSource = sessionState.conf.defaultDataSourceName
    try {
      sql("CREATE TABLE ctas1 AS SELECT key k, value FROM src ORDER BY k, value")
      sql("CREATE TABLE IF NOT EXISTS ctas1 AS SELECT key k, value FROM src ORDER BY k, value")
      val message = intercept[AnalysisException] {
        sql("CREATE TABLE ctas1 AS SELECT key k, value FROM src ORDER BY k, value")
      }.getMessage
      assert(message.contains("already exists"))
      checkRelation("ctas1", true, defaultDataSource)
      sql("DROP TABLE ctas1")

      // Specifying database name for query can be converted to data source write path
      // is not allowed right now.
      sql("CREATE TABLE default.ctas1 AS SELECT key k, value FROM src ORDER BY k, value")
      checkRelation("ctas1", true, defaultDataSource)
      sql("DROP TABLE ctas1")

      sql("CREATE TABLE ctas1 stored as textfile" +
          " AS SELECT key k, value FROM src ORDER BY k, value")
      checkRelation("ctas1", false, "text")
      sql("DROP TABLE ctas1")

      sql("CREATE TABLE ctas1 stored as sequencefile" +
            " AS SELECT key k, value FROM src ORDER BY k, value")
      checkRelation("ctas1", false, "sequence")
      sql("DROP TABLE ctas1")

      sql("CREATE TABLE ctas1 stored as rcfile AS SELECT key k, value FROM src ORDER BY k, value")
      checkRelation("ctas1", false, "rcfile")
      sql("DROP TABLE ctas1")

      sql("CREATE TABLE ctas1 stored as orc AS SELECT key k, value FROM src ORDER BY k, value")
      checkRelation("ctas1", false, "orc")
      sql("DROP TABLE ctas1")

      sql("CREATE TABLE ctas1 stored as parquet AS SELECT key k, value FROM src ORDER BY k, value")
      checkRelation("ctas1", false, "parquet")
      sql("DROP TABLE ctas1")
    } finally {
      setConf(SQLConf.CONVERT_CTAS, originalConf)
      sql("DROP TABLE IF EXISTS ctas1")
    }
  }

  test("CTAS without serde with location") {
    withSQLConf(SQLConf.CONVERT_CTAS.key -> "true") {
      withTempDir { dir =>
        val defaultDataSource = sessionState.conf.defaultDataSourceName

        val tempLocation = dir.getCanonicalPath
        sql(s"CREATE TABLE ctas1 LOCATION 'file:$tempLocation/c1'" +
          " AS SELECT key k, value FROM src ORDER BY k, value")
        checkRelation("ctas1", true, defaultDataSource, Some(s"file:$tempLocation/c1"))
        sql("DROP TABLE ctas1")

        sql(s"CREATE TABLE ctas1 LOCATION 'file:$tempLocation/c2'" +
          " AS SELECT key k, value FROM src ORDER BY k, value")
        checkRelation("ctas1", true, defaultDataSource, Some(s"file:$tempLocation/c2"))
        sql("DROP TABLE ctas1")

        sql(s"CREATE TABLE ctas1 stored as textfile LOCATION 'file:$tempLocation/c3'" +
          " AS SELECT key k, value FROM src ORDER BY k, value")
        checkRelation("ctas1", false, "text", Some(s"file:$tempLocation/c3"))
        sql("DROP TABLE ctas1")

        sql(s"CREATE TABLE ctas1 stored as sequenceFile LOCATION 'file:$tempLocation/c4'" +
          " AS SELECT key k, value FROM src ORDER BY k, value")
        checkRelation("ctas1", false, "sequence", Some(s"file:$tempLocation/c4"))
        sql("DROP TABLE ctas1")

        sql(s"CREATE TABLE ctas1 stored as rcfile LOCATION 'file:$tempLocation/c5'" +
          " AS SELECT key k, value FROM src ORDER BY k, value")
        checkRelation("ctas1", false, "rcfile", Some(s"file:$tempLocation/c5"))
        sql("DROP TABLE ctas1")
      }
    }
  }

  test("CTAS with serde") {
    sql("CREATE TABLE ctas1 AS SELECT key k, value FROM src ORDER BY k, value").collect()
    sql(
      """CREATE TABLE ctas2
        | ROW FORMAT SERDE "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe"
        | WITH SERDEPROPERTIES("serde_p1"="p1","serde_p2"="p2")
        | STORED AS RCFile
        | TBLPROPERTIES("tbl_p1"="p11", "tbl_p2"="p22")
        | AS
        |   SELECT key, value
        |   FROM src
        |   ORDER BY key, value""".stripMargin).collect()
    sql(
      """CREATE TABLE ctas3
        | ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\012'
        | STORED AS textfile AS
        |   SELECT key, value
        |   FROM src
        |   ORDER BY key, value""".stripMargin).collect()

    // the table schema may like (key: integer, value: string)
    sql(
      """CREATE TABLE IF NOT EXISTS ctas4 AS
        | SELECT 1 AS key, value FROM src LIMIT 1""".stripMargin).collect()
    // do nothing cause the table ctas4 already existed.
    sql(
      """CREATE TABLE IF NOT EXISTS ctas4 AS
        | SELECT key, value FROM src ORDER BY key, value""".stripMargin).collect()

    checkAnswer(
      sql("SELECT k, value FROM ctas1 ORDER BY k, value"),
      sql("SELECT key, value FROM src ORDER BY key, value").collect().toSeq)
    checkAnswer(
      sql("SELECT key, value FROM ctas2 ORDER BY key, value"),
      sql(
        """
          SELECT key, value
          FROM src
          ORDER BY key, value""").collect().toSeq)
    checkAnswer(
      sql("SELECT key, value FROM ctas3 ORDER BY key, value"),
      sql(
        """
          SELECT key, value
          FROM src
          ORDER BY key, value""").collect().toSeq)
    intercept[AnalysisException] {
      sql(
        """CREATE TABLE ctas4 AS
          | SELECT key, value FROM src ORDER BY key, value""".stripMargin).collect()
    }
    checkAnswer(
      sql("SELECT key, value FROM ctas4 ORDER BY key, value"),
      sql("SELECT key, value FROM ctas4 LIMIT 1").collect().toSeq)

    /*
    Disabled because our describe table does not output the serde information right now.
    checkKeywordsExist(sql("DESC EXTENDED ctas2"),
      "name:key", "type:string", "name:value", "ctas2",
      "org.apache.hadoop.hive.ql.io.RCFileInputFormat",
      "org.apache.hadoop.hive.ql.io.RCFileOutputFormat",
      "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe",
      "serde_p1=p1", "serde_p2=p2", "tbl_p1=p11", "tbl_p2=p22", "MANAGED_TABLE"
    )
    */

    sql(
      """CREATE TABLE ctas5
        | STORED AS parquet AS
        |   SELECT key, value
        |   FROM src
        |   ORDER BY key, value""".stripMargin).collect()

    /*
    Disabled because our describe table does not output the serde information right now.
    withSQLConf(HiveUtils.CONVERT_METASTORE_PARQUET.key -> "false") {
      checkKeywordsExist(sql("DESC EXTENDED ctas5"),
        "name:key", "type:string", "name:value", "ctas5",
        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
        "MANAGED_TABLE"
      )
    }
    */

    // use the Hive SerDe for parquet tables
    withSQLConf(HiveUtils.CONVERT_METASTORE_PARQUET.key -> "false") {
      checkAnswer(
        sql("SELECT key, value FROM ctas5 ORDER BY key, value"),
        sql("SELECT key, value FROM src ORDER BY key, value").collect().toSeq)
    }
  }

  test("specifying the column list for CTAS") {
    withTempView("mytable1") {
      Seq((1, "111111"), (2, "222222")).toDF("key", "value").createOrReplaceTempView("mytable1")
      withTable("gen__tmp") {
        sql("create table gen__tmp as select key as a, value as b from mytable1")
        checkAnswer(
          sql("SELECT a, b from gen__tmp"),
          sql("select key, value from mytable1").collect())
      }

      withTable("gen__tmp") {
        val e = intercept[AnalysisException] {
          sql("create table gen__tmp(a int, b string) as select key, value from mytable1")
        }.getMessage
        assert(e.contains("Schema may not be specified in a Create Table As Select (CTAS)"))
      }

      withTable("gen__tmp") {
        val e = intercept[AnalysisException] {
          sql(
            """
              |CREATE TABLE gen__tmp
              |PARTITIONED BY (key string)
              |AS SELECT key, value FROM mytable1
            """.stripMargin)
        }.getMessage
        assert(e.contains("A Create Table As Select (CTAS) statement is not allowed to " +
          "create a partitioned table using Hive's file formats"))
      }
    }
  }

  test("command substitution") {
    sql("set tbl=src")
    checkAnswer(
      sql("SELECT key FROM ${hiveconf:tbl} ORDER BY key, value limit 1"),
      sql("SELECT key FROM src ORDER BY key, value limit 1").collect().toSeq)

    sql("set spark.sql.variable.substitute=false") // disable the substitution
    sql("set tbl2=src")
    intercept[Exception] {
      sql("SELECT key FROM ${hiveconf:tbl2} ORDER BY key, value limit 1").collect()
    }

    sql("set spark.sql.variable.substitute=true") // enable the substitution
    checkAnswer(
      sql("SELECT key FROM ${hiveconf:tbl2} ORDER BY key, value limit 1"),
      sql("SELECT key FROM src ORDER BY key, value limit 1").collect().toSeq)
  }

  test("ordering not in select") {
    checkAnswer(
      sql("SELECT key FROM src ORDER BY value"),
      sql("SELECT key FROM (SELECT key, value FROM src ORDER BY value) a").collect().toSeq)
  }

  test("ordering not in agg") {
    checkAnswer(
      sql("SELECT key FROM src GROUP BY key, value ORDER BY value"),
      sql("""
        SELECT key
        FROM (
          SELECT key, value
          FROM src
          GROUP BY key, value
          ORDER BY value) a""").collect().toSeq)
  }

  test("double nested data") {
    sparkContext.parallelize(Nested1(Nested2(Nested3(1))) :: Nil)
      .toDF().createOrReplaceTempView("nested")
    checkAnswer(
      sql("SELECT f1.f2.f3 FROM nested"),
      Row(1))

    sql("CREATE TABLE test_ctas_1234 AS SELECT * from nested")
    checkAnswer(
      sql("SELECT * FROM test_ctas_1234"),
      sql("SELECT * FROM nested").collect().toSeq)

    intercept[AnalysisException] {
      sql("CREATE TABLE test_ctas_1234 AS SELECT * from notexists").collect()
    }
  }

  test("test CTAS") {
    sql("CREATE TABLE test_ctas_123 AS SELECT key, value FROM src")
    checkAnswer(
      sql("SELECT key, value FROM test_ctas_123 ORDER BY key"),
      sql("SELECT key, value FROM src ORDER BY key").collect().toSeq)
  }

  test("SPARK-4825 save join to table") {
    val testData = sparkContext.parallelize(1 to 10).map(i => TestData(i, i.toString)).toDF()
    sql("CREATE TABLE test1 (key INT, value STRING)")
    testData.write.mode(SaveMode.Append).insertInto("test1")
    sql("CREATE TABLE test2 (key INT, value STRING)")
    testData.write.mode(SaveMode.Append).insertInto("test2")
    testData.write.mode(SaveMode.Append).insertInto("test2")
    sql("CREATE TABLE test AS SELECT COUNT(a.value) FROM test1 a JOIN test2 b ON a.key = b.key")
    checkAnswer(
      table("test"),
      sql("SELECT COUNT(a.value) FROM test1 a JOIN test2 b ON a.key = b.key").collect().toSeq)
  }

  test("SPARK-3708 Backticks aren't handled correctly is aliases") {
    checkAnswer(
      sql("SELECT k FROM (SELECT `key` AS `k` FROM src) a"),
      sql("SELECT `key` FROM src").collect().toSeq)
  }

  test("SPARK-3834 Backticks not correctly handled in subquery aliases") {
    checkAnswer(
      sql("SELECT a.key FROM (SELECT key FROM src) `a`"),
      sql("SELECT `key` FROM src").collect().toSeq)
  }

  test("SPARK-3814 Support Bitwise & operator") {
    checkAnswer(
      sql("SELECT case when 1&1=1 then 1 else 0 end FROM src"),
      sql("SELECT 1 FROM src").collect().toSeq)
  }

  test("SPARK-3814 Support Bitwise | operator") {
    checkAnswer(
      sql("SELECT case when 1|0=1 then 1 else 0 end FROM src"),
      sql("SELECT 1 FROM src").collect().toSeq)
  }

  test("SPARK-3814 Support Bitwise ^ operator") {
    checkAnswer(
      sql("SELECT case when 1^0=1 then 1 else 0 end FROM src"),
      sql("SELECT 1 FROM src").collect().toSeq)
  }

  test("SPARK-3814 Support Bitwise ~ operator") {
    checkAnswer(
      sql("SELECT case when ~1=-2 then 1 else 0 end FROM src"),
      sql("SELECT 1 FROM src").collect().toSeq)
  }

  test("SPARK-4154 Query does not work if it has 'not between' in Spark SQL and HQL") {
    checkAnswer(sql("SELECT key FROM src WHERE key not between 0 and 10 order by key"),
      sql("SELECT key FROM src WHERE key between 11 and 500 order by key").collect().toSeq)
  }

  test("SPARK-2554 SumDistinct partial aggregation") {
    checkAnswer(sql("SELECT sum( distinct key) FROM src group by key order by key"),
      sql("SELECT distinct key FROM src order by key").collect().toSeq)
  }

  test("SPARK-4963 DataFrame sample on mutable row return wrong result") {
    sql("SELECT * FROM src WHERE key % 2 = 0")
      .sample(withReplacement = false, fraction = 0.3)
      .createOrReplaceTempView("sampled")
    (1 to 10).foreach { i =>
      checkAnswer(
        sql("SELECT * FROM sampled WHERE key % 2 = 1"),
        Seq.empty[Row])
    }
  }

  test("SPARK-4699 SparkSession with Hive Support should be case insensitive by default") {
    checkAnswer(
      sql("SELECT KEY FROM Src ORDER BY value"),
      sql("SELECT key FROM src ORDER BY value").collect().toSeq)
  }

  test("SPARK-5284 Insert into Hive throws NPE when a inner complex type field has a null value") {
    val schema = StructType(
      StructField("s",
        StructType(
          StructField("innerStruct", StructType(StructField("s1", StringType, true) :: Nil)) ::
            StructField("innerArray", ArrayType(IntegerType), true) ::
            StructField("innerMap", MapType(StringType, IntegerType)) :: Nil), true) :: Nil)
    val row = Row(Row(null, null, null))

    val rowRdd = sparkContext.parallelize(row :: Nil)

    spark.createDataFrame(rowRdd, schema).createOrReplaceTempView("testTable")

    sql(
      """CREATE TABLE nullValuesInInnerComplexTypes
        |  (s struct<innerStruct: struct<s1:string>,
        |            innerArray:array<int>,
        |            innerMap: map<string, int>>)
      """.stripMargin).collect()

    sql(
      """
        |INSERT OVERWRITE TABLE nullValuesInInnerComplexTypes
        |SELECT * FROM testTable
      """.stripMargin)

    checkAnswer(
      sql("SELECT * FROM nullValuesInInnerComplexTypes"),
      Row(Row(null, null, null))
    )

    sql("DROP TABLE nullValuesInInnerComplexTypes")
    dropTempTable("testTable")
  }

  test("SPARK-4296 Grouping field with Hive UDF as sub expression") {
    val rdd = sparkContext.makeRDD( """{"a": "str", "b":"1", "c":"1970-01-01 00:00:00"}""" :: Nil)
    read.json(rdd).createOrReplaceTempView("data")
    checkAnswer(
      sql("SELECT concat(a, '-', b), year(c) FROM data GROUP BY concat(a, '-', b), year(c)"),
      Row("str-1", 1970))

    dropTempTable("data")

    read.json(rdd).createOrReplaceTempView("data")
    checkAnswer(sql("SELECT year(c) + 1 FROM data GROUP BY year(c) + 1"), Row(1971))

    dropTempTable("data")
  }

  test("resolve udtf in projection #1") {
    val rdd = sparkContext.makeRDD((1 to 5).map(i => s"""{"a":[$i, ${i + 1}]}"""))
    read.json(rdd).createOrReplaceTempView("data")
    val df = sql("SELECT explode(a) AS val FROM data")
    val col = df("val")
  }

  test("resolve udtf in projection #2") {
    val rdd = sparkContext.makeRDD((1 to 2).map(i => s"""{"a":[$i, ${i + 1}]}"""))
    read.json(rdd).createOrReplaceTempView("data")
    checkAnswer(sql("SELECT explode(map(1, 1)) FROM data LIMIT 1"), Row(1, 1) :: Nil)
    checkAnswer(sql("SELECT explode(map(1, 1)) as (k1, k2) FROM data LIMIT 1"), Row(1, 1) :: Nil)
    intercept[AnalysisException] {
      sql("SELECT explode(map(1, 1)) as k1 FROM data LIMIT 1")
    }

    intercept[AnalysisException] {
      sql("SELECT explode(map(1, 1)) as (k1, k2, k3) FROM data LIMIT 1")
    }
  }

  // TGF with non-TGF in project is allowed in Spark SQL, but not in Hive
  test("TGF with non-TGF in projection") {
    val rdd = sparkContext.makeRDD( """{"a": "1", "b":"1"}""" :: Nil)
    read.json(rdd).createOrReplaceTempView("data")
    checkAnswer(
      sql("SELECT explode(map(a, b)) as (k1, k2), a, b FROM data"),
      Row("1", "1", "1", "1") :: Nil)
  }

  test("logical.Project should not be resolved if it contains aggregates or generators") {
    // This test is used to test the fix of SPARK-5875.
    // The original issue was that Project's resolved will be true when it contains
    // AggregateExpressions or Generators. However, in this case, the Project
    // is not in a valid state (cannot be executed). Because of this bug, the analysis rule of
    // PreInsertionCasts will actually start to work before ImplicitGenerate and then
    // generates an invalid query plan.
    val rdd = sparkContext.makeRDD((1 to 5).map(i => s"""{"a":[$i, ${i + 1}]}"""))
    read.json(rdd).createOrReplaceTempView("data")
    val originalConf = sessionState.conf.convertCTAS
    setConf(SQLConf.CONVERT_CTAS, false)

    try {
      sql("CREATE TABLE explodeTest (key bigInt)")
      table("explodeTest").queryExecution.analyzed match {
        case metastoreRelation: MetastoreRelation => // OK
        case _ =>
          fail("To correctly test the fix of SPARK-5875, explodeTest should be a MetastoreRelation")
      }

      sql(s"INSERT OVERWRITE TABLE explodeTest SELECT explode(a) AS val FROM data")
      checkAnswer(
        sql("SELECT key from explodeTest"),
        (1 to 5).flatMap(i => Row(i) :: Row(i + 1) :: Nil)
      )

      sql("DROP TABLE explodeTest")
      dropTempTable("data")
    } finally {
      setConf(SQLConf.CONVERT_CTAS, originalConf)
    }
  }

  test("sanity test for SPARK-6618") {
    (1 to 100).par.map { i =>
      val tableName = s"SPARK_6618_table_$i"
      sql(s"CREATE TABLE $tableName (col1 string)")
      sessionState.catalog.lookupRelation(TableIdentifier(tableName))
      table(tableName)
      tables()
      sql(s"DROP TABLE $tableName")
    }
  }

  test("SPARK-5203 union with different decimal precision") {
    Seq.empty[(java.math.BigDecimal, java.math.BigDecimal)]
      .toDF("d1", "d2")
      .select($"d1".cast(DecimalType(10, 5)).as("d"))
      .createOrReplaceTempView("dn")

    sql("select d from dn union all select d * 2 from dn")
      .queryExecution.analyzed
  }

  test("Star Expansion - script transform") {
    val data = (1 to 100000).map { i => (i, i, i) }
    data.toDF("d1", "d2", "d3").createOrReplaceTempView("script_trans")
    assert(100000 === sql("SELECT TRANSFORM (*) USING 'cat' FROM script_trans").count())
  }

  test("test script transform for stdout") {
    val data = (1 to 100000).map { i => (i, i, i) }
    data.toDF("d1", "d2", "d3").createOrReplaceTempView("script_trans")
    assert(100000 ===
      sql("SELECT TRANSFORM (d1, d2, d3) USING 'cat' AS (a,b,c) FROM script_trans").count())
  }

  test("test script transform for stderr") {
    val data = (1 to 100000).map { i => (i, i, i) }
    data.toDF("d1", "d2", "d3").createOrReplaceTempView("script_trans")
    assert(0 ===
      sql("SELECT TRANSFORM (d1, d2, d3) USING 'cat 1>&2' AS (a,b,c) FROM script_trans").count())
  }

  test("test script transform data type") {
    val data = (1 to 5).map { i => (i, i) }
    data.toDF("key", "value").createOrReplaceTempView("test")
    checkAnswer(
      sql("""FROM
          |(FROM test SELECT TRANSFORM(key, value) USING 'cat' AS (`thing1` int, thing2 string)) t
          |SELECT thing1 + 1
        """.stripMargin), (2 to 6).map(i => Row(i)))
  }

  test("Sorting columns are not in Generate") {
    withTempView("data") {
      spark.range(1, 5)
        .select(array($"id", $"id" + 1).as("a"), $"id".as("b"), (lit(10) - $"id").as("c"))
        .createOrReplaceTempView("data")

      // case 1: missing sort columns are resolvable if join is true
      checkAnswer(
        sql("SELECT explode(a) AS val, b FROM data WHERE b < 2 order by val, c"),
        Row(1, 1) :: Row(2, 1) :: Nil)

      // case 2: missing sort columns are resolvable if join is false
      checkAnswer(
        sql("SELECT explode(a) AS val FROM data order by val, c"),
        Seq(1, 2, 2, 3, 3, 4, 4, 5).map(i => Row(i)))

      // case 3: missing sort columns are resolvable if join is true and outer is true
      checkAnswer(
        sql(
          """
            |SELECT C.val, b FROM data LATERAL VIEW OUTER explode(a) C as val
            |where b < 2 order by c, val, b
          """.stripMargin),
        Row(1, 1) :: Row(2, 1) :: Nil)
    }
  }

  test("test case key when") {
    (1 to 5).map(i => (i, i.toString)).toDF("k", "v").createOrReplaceTempView("t")
    checkAnswer(
      sql("SELECT CASE k WHEN 2 THEN 22 WHEN 4 THEN 44 ELSE 0 END, v FROM t"),
      Row(0, "1") :: Row(22, "2") :: Row(0, "3") :: Row(44, "4") :: Row(0, "5") :: Nil)
  }

  test("SPARK-7269 Check analysis failed in case in-sensitive") {
    Seq(1, 2, 3).map { i =>
      (i.toString, i.toString)
    }.toDF("key", "value").createOrReplaceTempView("df_analysis")
    sql("SELECT kEy from df_analysis group by key").collect()
    sql("SELECT kEy+3 from df_analysis group by key+3").collect()
    sql("SELECT kEy+3, a.kEy, A.kEy from df_analysis A group by key").collect()
    sql("SELECT cast(kEy+1 as Int) from df_analysis A group by cast(key+1 as int)").collect()
    sql("SELECT cast(kEy+1 as Int) from df_analysis A group by key+1").collect()
    sql("SELECT 2 from df_analysis A group by key+1").collect()
    intercept[AnalysisException] {
      sql("SELECT kEy+1 from df_analysis group by key+3")
    }
    intercept[AnalysisException] {
      sql("SELECT cast(key+2 as Int) from df_analysis A group by cast(key+1 as int)")
    }
  }

  test("Cast STRING to BIGINT") {
    checkAnswer(sql("SELECT CAST('775983671874188101' as BIGINT)"), Row(775983671874188101L))
  }

  test("dynamic partition value test") {
    try {
      sql("set hive.exec.dynamic.partition.mode=nonstrict")
      // date
      sql("drop table if exists dynparttest1")
      sql("create table dynparttest1 (value int) partitioned by (pdate date)")
      sql(
        """
          |insert into table dynparttest1 partition(pdate)
          | select count(*), cast('2015-05-21' as date) as pdate from src
        """.stripMargin)
      checkAnswer(
        sql("select * from dynparttest1"),
        Seq(Row(500, java.sql.Date.valueOf("2015-05-21"))))

      // decimal
      sql("drop table if exists dynparttest2")
      sql("create table dynparttest2 (value int) partitioned by (pdec decimal(5, 1))")
      sql(
        """
          |insert into table dynparttest2 partition(pdec)
          | select count(*), cast('100.12' as decimal(5, 1)) as pdec from src
        """.stripMargin)
      checkAnswer(
        sql("select * from dynparttest2"),
        Seq(Row(500, new java.math.BigDecimal("100.1"))))
    } finally {
      sql("drop table if exists dynparttest1")
      sql("drop table if exists dynparttest2")
      sql("set hive.exec.dynamic.partition.mode=strict")
    }
  }

  test("Call add jar in a different thread (SPARK-8306)") {
    @volatile var error: Option[Throwable] = None
    val thread = new Thread {
      override def run() {
        // To make sure this test works, this jar should not be loaded in another place.
        sql(
          s"ADD JAR ${hiveContext.getHiveFile("hive-contrib-0.13.1.jar").getCanonicalPath()}")
        try {
          sql(
            """
              |CREATE TEMPORARY FUNCTION example_max
              |AS 'org.apache.hadoop.hive.contrib.udaf.example.UDAFExampleMax'
            """.stripMargin)
        } catch {
          case throwable: Throwable =>
            error = Some(throwable)
        }
      }
    }
    thread.start()
    thread.join()
    error match {
      case Some(throwable) =>
        fail("CREATE TEMPORARY FUNCTION should not fail.", throwable)
      case None => // OK
    }
  }

  test("SPARK-6785: HiveQuerySuite - Date comparison test 2") {
    checkAnswer(
      sql("SELECT CAST(CAST(0 AS timestamp) AS date) > CAST(0 AS timestamp) FROM src LIMIT 1"),
      Row(false))
  }

  test("SPARK-6785: HiveQuerySuite - Date cast") {
    // new Date(0) == 1970-01-01 00:00:00.0 GMT == 1969-12-31 16:00:00.0 PST
    checkAnswer(
      sql(
        """
          | SELECT
          | CAST(CAST(0 AS timestamp) AS date),
          | CAST(CAST(CAST(0 AS timestamp) AS date) AS string),
          | CAST(0 AS timestamp),
          | CAST(CAST(0 AS timestamp) AS string),
          | CAST(CAST(CAST('1970-01-01 23:00:00' AS timestamp) AS date) AS timestamp)
          | FROM src LIMIT 1
        """.stripMargin),
      Row(
        Date.valueOf("1969-12-31"),
        String.valueOf("1969-12-31"),
        Timestamp.valueOf("1969-12-31 16:00:00"),
        String.valueOf("1969-12-31 16:00:00"),
        Timestamp.valueOf("1970-01-01 00:00:00")))

  }

  test("SPARK-8588 HiveTypeCoercion.inConversion fires too early") {
    val df =
      createDataFrame(Seq((1, "2014-01-01"), (2, "2015-01-01"), (3, "2016-01-01")))
    df.toDF("id", "datef").createOrReplaceTempView("test_SPARK8588")
    checkAnswer(
      sql(
        """
          |select id, concat(year(datef))
          |from test_SPARK8588 where concat(year(datef), ' year') in ('2015 year', '2014 year')
        """.stripMargin),
      Row(1, "2014") :: Row(2, "2015") :: Nil
    )
    dropTempTable("test_SPARK8588")
  }

  test("SPARK-9371: fix the support for special chars in column names for hive context") {
    read.json(sparkContext.makeRDD(
      """{"a": {"c.b": 1}, "b.$q": [{"a@!.q": 1}], "q.w": {"w.i&": [1]}}""" :: Nil))
      .createOrReplaceTempView("t")

    checkAnswer(sql("SELECT a.`c.b`, `b.$q`[0].`a@!.q`, `q.w`.`w.i&`[0] FROM t"), Row(1, 1, 1))
  }

  test("Convert hive interval term into Literal of CalendarIntervalType") {
    checkAnswer(sql("select interval '10-9' year to month"),
      Row(CalendarInterval.fromString("interval 10 years 9 months")))
    checkAnswer(sql("select interval '20 15:40:32.99899999' day to second"),
      Row(CalendarInterval.fromString("interval 2 weeks 6 days 15 hours 40 minutes " +
        "32 seconds 99 milliseconds 899 microseconds")))
    checkAnswer(sql("select interval '30' year"),
      Row(CalendarInterval.fromString("interval 30 years")))
    checkAnswer(sql("select interval '25' month"),
      Row(CalendarInterval.fromString("interval 25 months")))
    checkAnswer(sql("select interval '-100' day"),
      Row(CalendarInterval.fromString("interval -14 weeks -2 days")))
    checkAnswer(sql("select interval '40' hour"),
      Row(CalendarInterval.fromString("interval 1 days 16 hours")))
    checkAnswer(sql("select interval '80' minute"),
      Row(CalendarInterval.fromString("interval 1 hour 20 minutes")))
    checkAnswer(sql("select interval '299.889987299' second"),
      Row(CalendarInterval.fromString(
        "interval 4 minutes 59 seconds 889 milliseconds 987 microseconds")))
  }

  test("specifying database name for a temporary table is not allowed") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val df = sparkContext.parallelize(1 to 10).map(i => (i, i.toString)).toDF("num", "str")
      df
        .write
        .format("parquet")
        .save(path)

      // We don't support creating a temporary table while specifying a database
      val message = intercept[AnalysisException] {
        spark.sql(
          s"""
          |CREATE TEMPORARY TABLE db.t
          |USING parquet
          |OPTIONS (
          |  path '$path'
          |)
        """.stripMargin)
      }.getMessage

      // If you use backticks to quote the name then it's OK.
      spark.sql(
        s"""
          |CREATE TEMPORARY TABLE `db.t`
          |USING parquet
          |OPTIONS (
          |  path '$path'
          |)
        """.stripMargin)
      checkAnswer(spark.table("`db.t`"), df)
    }
  }

  test("SPARK-10593 same column names in lateral view") {
    val df = spark.sql(
    """
      |select
      |insideLayer2.json as a2
      |from (select '{"layer1": {"layer2": "text inside layer 2"}}' json) test
      |lateral view json_tuple(json, 'layer1') insideLayer1 as json
      |lateral view json_tuple(insideLayer1.json, 'layer2') insideLayer2 as json
    """.stripMargin
    )

    checkAnswer(df, Row("text inside layer 2") :: Nil)
  }

  ignore("SPARK-10310: " +
    "script transformation using default input/output SerDe and record reader/writer") {
    spark
      .range(5)
      .selectExpr("id AS a", "id AS b")
      .createOrReplaceTempView("test")

    val scriptFilePath = getTestResourcePath("data")
    checkAnswer(
      sql(
        s"""FROM(
          |  FROM test SELECT TRANSFORM(a, b)
          |  USING 'python $scriptFilePath/scripts/test_transform.py "\t"'
          |  AS (c STRING, d STRING)
          |) t
          |SELECT c
        """.stripMargin),
      (0 until 5).map(i => Row(i + "#")))
  }

  ignore("SPARK-10310: script transformation using LazySimpleSerDe") {
    spark
      .range(5)
      .selectExpr("id AS a", "id AS b")
      .createOrReplaceTempView("test")

    val scriptFilePath = getTestResourcePath("data")
    val df = sql(
      s"""FROM test
        |SELECT TRANSFORM(a, b)
        |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        |WITH SERDEPROPERTIES('field.delim' = '|')
        |USING 'python $scriptFilePath/scripts/test_transform.py "|"'
        |AS (c STRING, d STRING)
        |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        |WITH SERDEPROPERTIES('field.delim' = '|')
      """.stripMargin)

    checkAnswer(df, (0 until 5).map(i => Row(i + "#", i + "#")))
  }

  test("SPARK-10741: Sort on Aggregate using parquet") {
    withTable("test10741") {
      withTempView("src") {
        Seq("a" -> 5, "a" -> 9, "b" -> 6).toDF("c1", "c2").createOrReplaceTempView("src")
        sql("CREATE TABLE test10741 STORED AS PARQUET AS SELECT * FROM src")
      }

      checkAnswer(sql(
        """
          |SELECT c1, AVG(c2) AS c_avg
          |FROM test10741
          |GROUP BY c1
          |HAVING (AVG(c2) > 5) ORDER BY c1
        """.stripMargin), Row("a", 7.0) :: Row("b", 6.0) :: Nil)

      checkAnswer(sql(
        """
          |SELECT c1, AVG(c2) AS c_avg
          |FROM test10741
          |GROUP BY c1
          |ORDER BY AVG(c2)
        """.stripMargin), Row("b", 6.0) :: Row("a", 7.0) :: Nil)
    }
  }

  test("run sql directly on files - parquet") {
    val df = spark.range(100).toDF()
    withTempPath(f => {
      df.write.parquet(f.getCanonicalPath)
      // data source type is case insensitive
      checkAnswer(sql(s"select id from Parquet.`${f.getCanonicalPath}`"),
        df)
      checkAnswer(sql(s"select id from `org.apache.spark.sql.parquet`.`${f.getCanonicalPath}`"),
        df)
      checkAnswer(sql(s"select a.id from parquet.`${f.getCanonicalPath}` as a"),
        df)
    })
  }

  test("run sql directly on files - orc") {
    val df = spark.range(100).toDF()
    withTempPath(f => {
      df.write.orc(f.getCanonicalPath)
      // data source type is case insensitive
      checkAnswer(sql(s"select id from ORC.`${f.getCanonicalPath}`"),
        df)
      checkAnswer(sql(s"select id from `org.apache.spark.sql.hive.orc`.`${f.getCanonicalPath}`"),
        df)
      checkAnswer(sql(s"select a.id from orc.`${f.getCanonicalPath}` as a"),
        df)
    })
  }

  test("run sql directly on files - csv") {
    val df = spark.range(100).toDF()
    withTempPath(f => {
      df.write.csv(f.getCanonicalPath)
      // data source type is case insensitive
      checkAnswer(sql(s"select cast(_c0 as int) id from CSV.`${f.getCanonicalPath}`"),
        df)
      checkAnswer(
        sql(s"select cast(_c0 as int) id from `com.databricks.spark.csv`.`${f.getCanonicalPath}`"),
        df)
      checkAnswer(sql(s"select cast(a._c0 as int) id from csv.`${f.getCanonicalPath}` as a"),
        df)
    })
  }

  test("run sql directly on files - json") {
    val df = spark.range(100).toDF()
    withTempPath(f => {
      df.write.json(f.getCanonicalPath)
      // data source type is case insensitive
      checkAnswer(sql(s"select id from jsoN.`${f.getCanonicalPath}`"),
        df)
      checkAnswer(sql(s"select id from `org.apache.spark.sql.json`.`${f.getCanonicalPath}`"),
        df)
      checkAnswer(sql(s"select a.id from json.`${f.getCanonicalPath}` as a"),
        df)
    })
  }

  test("SPARK-8976 Wrong Result for Rollup #1") {
    checkAnswer(sql(
      "SELECT count(*) AS cnt, key % 5, grouping_id() FROM src GROUP BY key%5 WITH ROLLUP"),
      Seq(
        (113, 3, 0),
        (91, 0, 0),
        (500, null, 1),
        (84, 1, 0),
        (105, 2, 0),
        (107, 4, 0)
      ).map(i => Row(i._1, i._2, i._3)))
  }

  test("SPARK-8976 Wrong Result for Rollup #2") {
    checkAnswer(sql(
      """
        |SELECT count(*) AS cnt, key % 5 AS k1, key-5 AS k2, grouping_id() AS k3
        |FROM src GROUP BY key%5, key-5
        |WITH ROLLUP ORDER BY cnt, k1, k2, k3 LIMIT 10
      """.stripMargin),
      Seq(
        (1, 0, 5, 0),
        (1, 0, 15, 0),
        (1, 0, 25, 0),
        (1, 0, 60, 0),
        (1, 0, 75, 0),
        (1, 0, 80, 0),
        (1, 0, 100, 0),
        (1, 0, 140, 0),
        (1, 0, 145, 0),
        (1, 0, 150, 0)
      ).map(i => Row(i._1, i._2, i._3, i._4)))
  }

  test("SPARK-8976 Wrong Result for Rollup #3") {
    checkAnswer(sql(
      """
        |SELECT count(*) AS cnt, key % 5 AS k1, key-5 AS k2, grouping_id() AS k3
        |FROM (SELECT key, key%2, key - 5 FROM src) t GROUP BY key%5, key-5
        |WITH ROLLUP ORDER BY cnt, k1, k2, k3 LIMIT 10
      """.stripMargin),
      Seq(
        (1, 0, 5, 0),
        (1, 0, 15, 0),
        (1, 0, 25, 0),
        (1, 0, 60, 0),
        (1, 0, 75, 0),
        (1, 0, 80, 0),
        (1, 0, 100, 0),
        (1, 0, 140, 0),
        (1, 0, 145, 0),
        (1, 0, 150, 0)
      ).map(i => Row(i._1, i._2, i._3, i._4)))
  }

  test("SPARK-8976 Wrong Result for CUBE #1") {
    checkAnswer(sql(
      "SELECT count(*) AS cnt, key % 5, grouping_id() FROM src GROUP BY key%5 WITH CUBE"),
      Seq(
        (113, 3, 0),
        (91, 0, 0),
        (500, null, 1),
        (84, 1, 0),
        (105, 2, 0),
        (107, 4, 0)
      ).map(i => Row(i._1, i._2, i._3)))
  }

  test("SPARK-8976 Wrong Result for CUBE #2") {
    checkAnswer(sql(
      """
        |SELECT count(*) AS cnt, key % 5 AS k1, key-5 AS k2, grouping_id() AS k3
        |FROM (SELECT key, key%2, key - 5 FROM src) t GROUP BY key%5, key-5
        |WITH CUBE ORDER BY cnt, k1, k2, k3 LIMIT 10
      """.stripMargin),
    Seq(
      (1, null, -3, 2),
      (1, null, -1, 2),
      (1, null, 3, 2),
      (1, null, 4, 2),
      (1, null, 5, 2),
      (1, null, 6, 2),
      (1, null, 12, 2),
      (1, null, 14, 2),
      (1, null, 15, 2),
      (1, null, 22, 2)
    ).map(i => Row(i._1, i._2, i._3, i._4)))
  }

  test("SPARK-8976 Wrong Result for GroupingSet") {
    checkAnswer(sql(
      """
        |SELECT count(*) AS cnt, key % 5 AS k1, key-5 AS k2, grouping_id() AS k3
        |FROM (SELECT key, key%2, key - 5 FROM src) t GROUP BY key%5, key-5
        |GROUPING SETS (key%5, key-5) ORDER BY cnt, k1, k2, k3 LIMIT 10
      """.stripMargin),
    Seq(
      (1, null, -3, 2),
      (1, null, -1, 2),
      (1, null, 3, 2),
      (1, null, 4, 2),
      (1, null, 5, 2),
      (1, null, 6, 2),
      (1, null, 12, 2),
      (1, null, 14, 2),
      (1, null, 15, 2),
      (1, null, 22, 2)
    ).map(i => Row(i._1, i._2, i._3, i._4)))
  }

  test("SPARK-10562: partition by column with mixed case name") {
    withTable("tbl10562") {
      val df = Seq(2012 -> "a").toDF("Year", "val")
      df.write.partitionBy("Year").saveAsTable("tbl10562")
      checkAnswer(sql("SELECT year FROM tbl10562"), Row(2012))
      checkAnswer(sql("SELECT Year FROM tbl10562"), Row(2012))
      checkAnswer(sql("SELECT yEAr FROM tbl10562"), Row(2012))
      checkAnswer(sql("SELECT val FROM tbl10562 WHERE Year > 2015"), Nil)
      checkAnswer(sql("SELECT val FROM tbl10562 WHERE Year == 2012"), Row("a"))
    }
  }

  test("SPARK-11453: append data to partitioned table") {
    withTable("tbl11453") {
      Seq("1" -> "10", "2" -> "20").toDF("i", "j")
        .write.partitionBy("i").saveAsTable("tbl11453")

      Seq("3" -> "30").toDF("i", "j")
        .write.mode(SaveMode.Append).partitionBy("i").saveAsTable("tbl11453")
      checkAnswer(
        spark.read.table("tbl11453").select("i", "j").orderBy("i"),
        Row("1", "10") :: Row("2", "20") :: Row("3", "30") :: Nil)

      // make sure case sensitivity is correct.
      Seq("4" -> "40").toDF("i", "j")
        .write.mode(SaveMode.Append).partitionBy("I").saveAsTable("tbl11453")
      checkAnswer(
        spark.read.table("tbl11453").select("i", "j").orderBy("i"),
        Row("1", "10") :: Row("2", "20") :: Row("3", "30") :: Row("4", "40") :: Nil)
    }
  }

  test("SPARK-11590: use native json_tuple in lateral view") {
    checkAnswer(sql(
      """
        |SELECT a, b
        |FROM (SELECT '{"f1": "value1", "f2": 12}' json) test
        |LATERAL VIEW json_tuple(json, 'f1', 'f2') jt AS a, b
      """.stripMargin), Row("value1", "12"))

    // we should use `c0`, `c1`... as the name of fields if no alias is provided, to follow hive.
    checkAnswer(sql(
      """
        |SELECT c0, c1
        |FROM (SELECT '{"f1": "value1", "f2": 12}' json) test
        |LATERAL VIEW json_tuple(json, 'f1', 'f2') jt
      """.stripMargin), Row("value1", "12"))

    // we can also use `json_tuple` in project list.
    checkAnswer(sql(
      """
        |SELECT json_tuple(json, 'f1', 'f2')
        |FROM (SELECT '{"f1": "value1", "f2": 12}' json) test
      """.stripMargin), Row("value1", "12"))

    // we can also mix `json_tuple` with other project expressions.
    checkAnswer(sql(
      """
        |SELECT json_tuple(json, 'f1', 'f2'), 3.14, str
        |FROM (SELECT '{"f1": "value1", "f2": 12}' json, 'hello' as str) test
      """.stripMargin), Row("value1", "12", BigDecimal("3.14"), "hello"))
  }

  test("multi-insert with lateral view") {
    withTempView("t1") {
      spark.range(10)
        .select(array($"id", $"id" + 1).as("arr"), $"id")
        .createOrReplaceTempView("source")
      withTable("dest1", "dest2") {
        sql("CREATE TABLE dest1 (i INT)")
        sql("CREATE TABLE dest2 (i INT)")
        sql(
          """
            |FROM source
            |INSERT OVERWRITE TABLE dest1
            |SELECT id
            |WHERE id > 3
            |INSERT OVERWRITE TABLE dest2
            |select col LATERAL VIEW EXPLODE(arr) exp AS col
            |WHERE col > 3
          """.stripMargin)

        checkAnswer(
          spark.table("dest1"),
          sql("SELECT id FROM source WHERE id > 3"))
        checkAnswer(
          spark.table("dest2"),
          sql("SELECT col FROM source LATERAL VIEW EXPLODE(arr) exp AS col WHERE col > 3"))
      }
    }
  }

  test("derived from Hive query file: drop_database_removes_partition_dirs.q") {
    // This test verifies that if a partition exists outside a table's current location when the
    // database is dropped the partition's location is dropped as well.
    sql("DROP database if exists test_database CASCADE")
    sql("CREATE DATABASE test_database")
    val previousCurrentDB = sessionState.catalog.getCurrentDatabase
    sql("USE test_database")
    sql("drop table if exists test_table")

    val tempDir = System.getProperty("test.tmp.dir")
    assert(tempDir != null, "TestHive should set test.tmp.dir.")

    sql(
      """
        |CREATE TABLE test_table (key int, value STRING)
        |PARTITIONED BY (part STRING)
        |STORED AS RCFILE
        |LOCATION 'file:${system:test.tmp.dir}/drop_database_removes_partition_dirs_table'
      """.stripMargin)
    sql(
      """
        |ALTER TABLE test_table ADD PARTITION (part = '1')
        |LOCATION 'file:${system:test.tmp.dir}/drop_database_removes_partition_dirs_table2/part=1'
      """.stripMargin)
    sql(
      """
        |INSERT OVERWRITE TABLE test_table PARTITION (part = '1')
        |SELECT * FROM default.src
      """.stripMargin)
     checkAnswer(
       sql("select part, key, value from test_table"),
       sql("select '1' as part, key, value from default.src")
     )
    val path = new Path(
      new Path(s"file:$tempDir"),
      "drop_database_removes_partition_dirs_table2")
    val fs = path.getFileSystem(sparkContext.hadoopConfiguration)
    // The partition dir is not empty.
    assert(fs.listStatus(new Path(path, "part=1")).nonEmpty)

    sql(s"USE $previousCurrentDB")
    sql("DROP DATABASE test_database CASCADE")

    // This table dir should not exist after we drop the entire database with the mode
    // of CASCADE. This probably indicates a Hive bug, which returns the wrong table
    // root location. So, the table's directory still there. We should change the condition
    // to fs.exists(path) after we handle fs operations.
    assert(
      fs.exists(path),
      "Thank you for making the changes of letting Spark SQL handle filesystem operations " +
        "for DDL commands. Originally, Hive metastore does not delete the table root directory " +
        "for this case. Now, please change this condition to !fs.exists(path).")
  }

  test("derived from Hive query file: drop_table_removes_partition_dirs.q") {
    // This test verifies that if a partition exists outside the table's current location when the
    // table is dropped the partition's location is dropped as well.
    sql("drop table if exists test_table")

    val tempDir = System.getProperty("test.tmp.dir")
    assert(tempDir != null, "TestHive should set test.tmp.dir.")

    sql(
      """
        |CREATE TABLE test_table (key int, value STRING)
        |PARTITIONED BY (part STRING)
        |STORED AS RCFILE
        |LOCATION 'file:${system:test.tmp.dir}/drop_table_removes_partition_dirs_table2'
      """.stripMargin)
    sql(
      """
        |ALTER TABLE test_table ADD PARTITION (part = '1')
        |LOCATION 'file:${system:test.tmp.dir}/drop_table_removes_partition_dirs_table2/part=1'
      """.stripMargin)
    sql(
      """
        |INSERT OVERWRITE TABLE test_table PARTITION (part = '1')
        |SELECT * FROM default.src
      """.stripMargin)
    checkAnswer(
      sql("select part, key, value from test_table"),
      sql("select '1' as part, key, value from src")
    )
    val path = new Path(new Path(s"file:$tempDir"), "drop_table_removes_partition_dirs_table2")
    val fs = path.getFileSystem(sparkContext.hadoopConfiguration)
    // The partition dir is not empty.
    assert(fs.listStatus(new Path(path, "part=1")).nonEmpty)

    sql("drop table test_table")
    assert(fs.exists(path), "This is an external table, so the data should not have been dropped")
  }

  test("select partitioned table") {
    val table = "table_with_partition"
    withTable(table) {
      sql(
        s"""
           |CREATE TABLE $table(c1 string)
           |PARTITIONED BY (p1 string,p2 string,p3 string,p4 string,p5 string)
         """.stripMargin)
      sql(
        s"""
           |INSERT OVERWRITE TABLE $table
           |PARTITION (p1='a',p2='b',p3='c',p4='d',p5='e')
           |SELECT 'blarr'
         """.stripMargin)

      // project list is the same order of paritioning columns in table definition
      checkAnswer(
        sql(s"SELECT p1, p2, p3, p4, p5, c1 FROM $table"),
        Row("a", "b", "c", "d", "e", "blarr") :: Nil)

      // project list does not have the same order of paritioning columns in table definition
      checkAnswer(
        sql(s"SELECT p2, p3, p4, p1, p5, c1 FROM $table"),
        Row("b", "c", "d", "a", "e", "blarr") :: Nil)

      // project list contains partial partition columns in table definition
      checkAnswer(
        sql(s"SELECT p2, p1, p5, c1 FROM $table"),
        Row("b", "a", "e", "blarr") :: Nil)
    }
  }

  test("SPARK-14981: DESC not supported for sorting columns") {
    withTable("t") {
      val cause = intercept[ParseException] {
        sql(
          """CREATE TABLE t USING PARQUET
            |OPTIONS (PATH '/path/to/file')
            |CLUSTERED BY (a) SORTED BY (b DESC) INTO 2 BUCKETS
            |AS SELECT 1 AS a, 2 AS b
          """.stripMargin
        )
      }

      assert(cause.getMessage.contains("Column ordering must be ASC, was 'DESC'"))
    }
  }

  test("insert into datasource table") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(i INT, j STRING) USING parquet")
      Seq(1 -> "a").toDF("i", "j").write.mode("overwrite").insertInto("tbl")
      checkAnswer(sql("SELECT * FROM tbl"), Row(1, "a"))
    }
  }

  test("spark-15557 promote string test") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(c1 string, c2 string)")
      sql("insert into tbl values ('3', '2.3')")
      checkAnswer(
        sql("select (cast (99 as decimal(19,6)) + cast('3' as decimal)) * cast('2.3' as decimal)"),
        Row(204.0)
      )
      checkAnswer(
        sql("select (cast(99 as decimal(19,6)) + '3') *'2.3' from tbl"),
        Row(234.6)
      )
      checkAnswer(
        sql("select (cast(99 as decimal(19,6)) + c1) * c2 from tbl"),
        Row(234.6)
      )
    }
  }

  test("SPARK-15752 optimize metadata only query for hive table") {
    withSQLConf(SQLConf.OPTIMIZER_METADATA_ONLY.key -> "true") {
      withTable("data_15752", "srcpart_15752", "srctext_15752") {
        val df = Seq((1, "2"), (3, "4")).toDF("key", "value")
        df.createOrReplaceTempView("data_15752")
        sql(
          """
            |CREATE TABLE srcpart_15752 (col1 INT, col2 STRING)
            |PARTITIONED BY (partcol1 INT, partcol2 STRING) STORED AS parquet
          """.stripMargin)
        for (partcol1 <- Seq(0, 1); partcol2 <- Seq("a", "b")) {
          sql(
            s"""
              |INSERT OVERWRITE TABLE srcpart_15752
              |PARTITION (partcol1='$partcol1', partcol2='$partcol2')
              |select key, value from data_15752
            """.stripMargin)
        }
        checkAnswer(
          sql("select partcol1 from srcpart_15752 group by partcol1"),
          Row(0) :: Row(1) :: Nil)
        checkAnswer(
          sql("select partcol1 from srcpart_15752 where partcol1 = 1 group by partcol1"),
          Row(1))
        checkAnswer(
          sql("select partcol1, count(distinct partcol2) from srcpart_15752 group by partcol1"),
          Row(0, 2) :: Row(1, 2) :: Nil)
        checkAnswer(
          sql("select partcol1, count(distinct partcol2) from srcpart_15752 where partcol1 = 1 " +
            "group by partcol1"),
          Row(1, 2) :: Nil)
        checkAnswer(sql("select distinct partcol1 from srcpart_15752"), Row(0) :: Row(1) :: Nil)
        checkAnswer(sql("select distinct partcol1 from srcpart_15752 where partcol1 = 1"), Row(1))
        checkAnswer(
          sql("select distinct col from (select partcol1 + 1 as col from srcpart_15752 " +
            "where partcol1 = 1) t"),
          Row(2))
        checkAnswer(sql("select distinct partcol1 from srcpart_15752 where partcol1 = 1"), Row(1))
        checkAnswer(sql("select max(partcol1) from srcpart_15752"), Row(1))
        checkAnswer(sql("select max(partcol1) from srcpart_15752 where partcol1 = 1"), Row(1))
        checkAnswer(sql("select max(partcol1) from (select partcol1 from srcpart_15752) t"), Row(1))
        checkAnswer(
          sql("select max(col) from (select partcol1 + 1 as col from srcpart_15752 " +
            "where partcol1 = 1) t"),
          Row(2))

        sql(
          """
            |CREATE TABLE srctext_15752 (col1 INT, col2 STRING)
            |PARTITIONED BY (partcol1 INT, partcol2 STRING) STORED AS textfile
          """.stripMargin)
        for (partcol1 <- Seq(0, 1); partcol2 <- Seq("a", "b")) {
          sql(
            s"""
              |INSERT OVERWRITE TABLE srctext_15752
              |PARTITION (partcol1='$partcol1', partcol2='$partcol2')
              |select key, value from data_15752
            """.stripMargin)
        }
        checkAnswer(
          sql("select partcol1 from srctext_15752 group by partcol1"),
          Row(0) :: Row(1) :: Nil)
        checkAnswer(
          sql("select partcol1 from srctext_15752 where partcol1 = 1 group by partcol1"),
          Row(1))
        checkAnswer(
          sql("select partcol1, count(distinct partcol2) from srctext_15752 group by partcol1"),
          Row(0, 2) :: Row(1, 2) :: Nil)
        checkAnswer(
          sql("select partcol1, count(distinct partcol2) from srctext_15752  where partcol1 = 1 " +
            "group by partcol1"),
          Row(1, 2) :: Nil)
        checkAnswer(sql("select distinct partcol1 from srctext_15752"), Row(0) :: Row(1) :: Nil)
        checkAnswer(sql("select distinct partcol1 from srctext_15752 where partcol1 = 1"), Row(1))
        checkAnswer(
          sql("select distinct col from (select partcol1 + 1 as col from srctext_15752 " +
            "where partcol1 = 1) t"),
          Row(2))
        checkAnswer(sql("select max(partcol1) from srctext_15752"), Row(1))
        checkAnswer(sql("select max(partcol1) from srctext_15752 where partcol1 = 1"), Row(1))
        checkAnswer(sql("select max(partcol1) from (select partcol1 from srctext_15752) t"), Row(1))
        checkAnswer(
          sql("select max(col) from (select partcol1 + 1 as col from srctext_15752 " +
            "where partcol1 = 1) t"),
          Row(2))
      }
    }
  }

  test("SPARK-17354: Partitioning by dates/timestamps works with Parquet vectorized reader") {
    withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true") {
      sql(
        """CREATE TABLE order(id INT)
          |PARTITIONED BY (pd DATE, pt TIMESTAMP)
          |STORED AS PARQUET
        """.stripMargin)

      sql("set hive.exec.dynamic.partition.mode=nonstrict")
      sql(
        """INSERT INTO TABLE order PARTITION(pd, pt)
          |SELECT 1 AS id, CAST('1990-02-24' AS DATE) AS pd, CAST('1990-02-24' AS TIMESTAMP) AS pt
        """.stripMargin)
      val actual = sql("SELECT * FROM order")
      val expected = sql(
        "SELECT 1 AS id, CAST('1990-02-24' AS DATE) AS pd, CAST('1990-02-24' AS TIMESTAMP) AS pt")
      checkAnswer(actual, expected)
      sql("DROP TABLE order")
    }
  }

  test("SPARK-17108: Fix BIGINT and INT comparison failure in spark sql") {
    sql("create table t1(a map<bigint, array<string>>)")
    sql("select * from t1 where a[1] is not null")

    sql("create table t2(a map<int, array<string>>)")
    sql("select * from t2 where a[1] is not null")

    sql("create table t3(a map<bigint, array<string>>)")
    sql("select * from t3 where a[1L] is not null")
  }

  def testCommandAvailable(command: String): Boolean = {
    val attempt = Try(Process(command).run(ProcessLogger(_ => ())).exitValue())
    attempt.isSuccess && attempt.get == 0
  }
}
