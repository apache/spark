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
import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}
import java.util.{Locale, Set}

import com.google.common.io.Files
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.{SparkException, TestUtils}
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.catalog.{CatalogTableType, CatalogUtils, HiveTableRelation}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.connector.catalog.CatalogManager.SESSION_CATALOG_NAME
import org.apache.spark.sql.execution.{SparkPlanInfo, TestUncaughtExceptionHandler}
import org.apache.spark.sql.execution.adaptive.{DisableAdaptiveExecutionSuite, EnableAdaptiveExecutionSuite}
import org.apache.spark.sql.execution.command.{InsertIntoDataSourceDirCommand, LoadDataCommand}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.{HiveExternalCatalog, HiveUtils}
import org.apache.spark.sql.hive.test.{HiveTestJars, TestHiveSingleton}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf.GLOBAL_TEMP_DATABASE
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types._
import org.apache.spark.tags.SlowHiveTest

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
abstract class SQLQuerySuiteBase extends QueryTest with SQLTestUtils with TestHiveSingleton {
  import hiveContext._
  import spark.implicits._

  test("query global temp view") {
    val df = Seq(1).toDF("i1")
    df.createGlobalTempView("tbl1")
    val global_temp_db = spark.conf.get(GLOBAL_TEMP_DATABASE.key)
    checkAnswer(spark.sql(s"select * from ${global_temp_db}.tbl1"), Row(1))
    spark.sql(s"drop view ${global_temp_db}.tbl1")
  }

  test("non-existent global temp view") {
    val global_temp_db = spark.conf.get(GLOBAL_TEMP_DATABASE.key)
    val e = intercept[AnalysisException] {
      spark.sql(s"select * from ${global_temp_db}.nonexistentview")
    }
    checkErrorTableNotFound(e, s"`${global_temp_db}`.`nonexistentview`",
      ExpectedContext(s"${global_temp_db}.nonexistentview", 14,
        13 + s"${global_temp_db}.nonexistentview".length))
  }

  test("script") {
    withTempView("script_table") {
      assume(TestUtils.testCommandAvailable("/bin/bash"))
      assume(TestUtils.testCommandAvailable("echo"))
      assume(TestUtils.testCommandAvailable("sed"))
      val scriptFilePath = getTestResourcePath("test_script.sh")
      val df = Seq(("x1", "y1", "z1"), ("x2", "y2", "z2")).toDF("c1", "c2", "c3")
      df.createOrReplaceTempView("script_table")
      val query1 = sql(
        s"""
          |SELECT col1 FROM (from(SELECT c1, c2, c3 FROM script_table) tempt_table
          |REDUCE c1, c2, c3 USING 'bash $scriptFilePath' AS
          |(col1 STRING, col2 STRING)) script_test_table""".stripMargin)
      checkAnswer(query1, Row("x1_y1") :: Row("x2_y2") :: Nil)
    }
  }

  test("SPARK-6835: udtf in lateral view") {
    withTempView("table1") {
      val df = Seq((1, 1)).toDF("c1", "c2")
      df.createOrReplaceTempView("table1")
      val query = sql("SELECT c1, v FROM table1 LATERAL VIEW stack(3, 1, c1 + 1, c1 + 2) d AS v")
      checkAnswer(query, Row(1, 1) :: Row(1, 2) :: Row(1, 3) :: Nil)
    }
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
    withTempView("orders1", "orderupdates1") {
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

      orders.toDF().createOrReplaceTempView("orders1")
      orderUpdates.toDF().createOrReplaceTempView("orderupdates1")

      withTable("orders", "orderupdates") {
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
        withSQLConf("hive.exec.dynamic.partition.mode" -> "nonstrict") {
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
      }
    }
  }

  test("describe functions - built-in functions") {
    checkKeywordsExist(sql("describe function extended upper"),
      "Function: upper",
      "Class: org.apache.spark.sql.catalyst.expressions.Upper",
      "Usage: upper(str) - Returns `str` with all characters changed to uppercase",
      "Extended Usage:",
      "Examples:",
      "> SELECT upper('SparkSql');",
      "SPARKSQL")

    checkKeywordsExist(sql("describe functioN Upper"),
      "Function: upper",
      "Class: org.apache.spark.sql.catalyst.expressions.Upper",
      "Usage: upper(str) - Returns `str` with all characters changed to uppercase")

    checkKeywordsNotExist(sql("describe functioN Upper"),
      "Extended Usage")

    val sqlText = "describe functioN abcadf"
    checkError(
      exception = intercept[AnalysisException](sql(sqlText)),
      condition = "UNRESOLVED_ROUTINE",
      parameters = Map(
        "routineName" -> "`abcadf`",
        "searchPath" -> "[`system`.`builtin`, `system`.`session`, `spark_catalog`.`default`]"),
      context = ExpectedContext(
        fragment = "abcadf",
        start = 18,
        stop = 23))

    checkKeywordsExist(sql("describe functioN  `~`"),
      "Function: ~",
      "Class: org.apache.spark.sql.catalyst.expressions.BitwiseNot",
      "Usage: ~ expr - Returns the result of bitwise NOT of `expr`.")

    // Hard coded describe functions
    checkKeywordsExist(sql("describe function  `<>`"),
      "Function: <>",
      "Usage: expr1 <> expr2 - Returns true if `expr1` is not equal to `expr2`")

    checkKeywordsExist(sql("describe function  `!=`"),
      "Function: !=",
      "Usage: expr1 != expr2 - Returns true if `expr1` is not equal to `expr2`")

    checkKeywordsExist(sql("describe function  `between`"),
      "Function: between",
      "input [NOT] between lower AND upper - " +
        "evaluate if `input` is [not] in between `lower` and `upper`")

    checkKeywordsExist(sql("describe function  `case`"),
      "Function: case",
      "Usage: CASE expr1 WHEN expr2 THEN expr3 " +
        "[WHEN expr4 THEN expr5]* [ELSE expr6] END - " +
        "When `expr1` = `expr2`, returns `expr3`; " +
        "when `expr1` = `expr4`, return `expr5`; else return `expr6`")
  }

  test("describe functions - user defined functions") {
    withUserDefinedFunction("udtf_count" -> false) {
      sql(
        s"""
           |CREATE FUNCTION udtf_count
           |AS 'org.apache.spark.sql.hive.execution.GenericUDTFCount2'
           |USING JAR '${hiveContext.getHiveFile("TestUDTF.jar").toURI}'
        """.stripMargin)

      checkKeywordsExist(sql("describe function udtf_count"),
        s"Function: $SESSION_CATALOG_NAME.default.udtf_count",
        "Class: org.apache.spark.sql.hive.execution.GenericUDTFCount2",
        "Usage: N/A")

      checkAnswer(
        sql("SELECT udtf_count(a) FROM (SELECT 1 AS a FROM src LIMIT 3) t"),
        Row(3) :: Row(3) :: Nil)

      checkKeywordsExist(sql("describe function udtf_count"),
        s"Function: $SESSION_CATALOG_NAME.default.udtf_count",
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
           |USING JAR '${hiveContext.getHiveFile("TestUDTF.jar").toURI}'
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

  test("SPARK-5371: union with null and sum") {
    withTempView("table1") {
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
  }

  test("CTAS with WITH clause") {
    withTempView("table1") {
      val df = Seq((1, 1)).toDF("c1", "c2")
      df.createOrReplaceTempView("table1")
      withTable("with_table1") {
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
    }
  }

  test("explode nested Field") {
    withTempView("nestedArray") {
      Seq(NestedArray1(NestedArray2(Seq(1, 2, 3)))).toDF().createOrReplaceTempView("nestedArray")
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
  }

  test("SPARK-4512 Fix attribute reference resolution error when using SORT BY") {
    checkAnswer(
      sql("SELECT * FROM (SELECT key + key AS a FROM src SORT BY value) t ORDER BY t.a"),
      sql("SELECT key + key as a FROM src ORDER BY a").collect().toSeq
    )
  }

  def checkRelation(
      tableName: String,
      isDataSourceTable: Boolean,
      format: String,
      userSpecifiedLocation: Option[String] = None): Unit = {
    var relation: LogicalPlan = null
    withSQLConf(
      HiveUtils.CONVERT_METASTORE_PARQUET.key -> "false",
      HiveUtils.CONVERT_METASTORE_ORC.key -> "false") {
      relation = EliminateSubqueryAliases(spark.table(tableName).queryExecution.analyzed)
    }
    val catalogTable =
      sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
    relation match {
      case LogicalRelation(r: HadoopFsRelation, _, _, _) =>
        if (!isDataSourceTable) {
          fail(
            s"${classOf[HiveTableRelation].getCanonicalName} is expected, but found " +
              s"${HadoopFsRelation.getClass.getCanonicalName}.")
        }
        userSpecifiedLocation match {
          case Some(location) =>
            assert(r.options("path") === location)
          case None => // OK.
        }
        assert(catalogTable.provider.get === format)

      case r: HiveTableRelation =>
        if (isDataSourceTable) {
          fail(
            s"${HadoopFsRelation.getClass.getCanonicalName} is expected, but found " +
              s"${classOf[HiveTableRelation].getCanonicalName}.")
        }
        userSpecifiedLocation match {
          case Some(location) =>
            assert(r.tableMeta.location === CatalogUtils.stringToURI(location))
          case None => // OK.
        }
        // Also make sure that the format and serde are as desired.
        assert(catalogTable.storage.inputFormat.get.toLowerCase(Locale.ROOT).contains(format))
        assert(catalogTable.storage.outputFormat.get.toLowerCase(Locale.ROOT).contains(format))
        val serde = catalogTable.storage.serde.get
        format match {
          case "sequence" | "text" => assert(serde.contains("LazySimpleSerDe"))
          case "rcfile" => assert(serde.contains("LazyBinaryColumnarSerDe"))
          case _ => assert(serde.toLowerCase(Locale.ROOT).contains(format))
        }
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
    withSQLConf(SQLConf.CONVERT_CTAS.key -> "true") {
      val defaultDataSource = sessionState.conf.defaultDataSourceName
      withTable("ctas1") {
        sql("CREATE TABLE ctas1 AS SELECT key k, value FROM src ORDER BY k, value")
        sql("CREATE TABLE IF NOT EXISTS ctas1 AS SELECT key k, value FROM src ORDER BY k, value")
        val e = intercept[AnalysisException] {
          sql("CREATE TABLE ctas1 AS SELECT key k, value FROM src ORDER BY k, value")
        }
        checkErrorTableAlreadyExists(e, "`spark_catalog`.`default`.`ctas1`")
        checkRelation("ctas1", isDataSourceTable = true, defaultDataSource)
      }

      // Specifying database name for query can be converted to data source write path
      // is not allowed right now.
      withTable("ctas1") {
        sql("CREATE TABLE default.ctas1 AS SELECT key k, value FROM src ORDER BY k, value")
        checkRelation("ctas1", isDataSourceTable = true, defaultDataSource)
      }

      withTable("ctas1") {
        sql("CREATE TABLE ctas1 stored as textfile" +
          " AS SELECT key k, value FROM src ORDER BY k, value")
        checkRelation("ctas1", isDataSourceTable = false, "text")
      }

      withTable("ctas1") {
        sql("CREATE TABLE ctas1 stored as sequencefile" +
          " AS SELECT key k, value FROM src ORDER BY k, value")
        checkRelation("ctas1", isDataSourceTable = false, "sequence")
      }

      withTable("ctas1") {
        sql("CREATE TABLE ctas1 stored as rcfile AS SELECT key k, value FROM src ORDER BY k, value")
        checkRelation("ctas1", isDataSourceTable = false, "rcfile")
      }

      withTable("ctas1") {
        sql("CREATE TABLE ctas1 stored as orc AS SELECT key k, value FROM src ORDER BY k, value")
        checkRelation("ctas1", isDataSourceTable = false, "orc")
      }

      withTable("ctas1") {
        sql(
          """
            |CREATE TABLE ctas1 stored as parquet
            |AS SELECT key k, value FROM src ORDER BY k, value
          """.stripMargin)
        checkRelation("ctas1", isDataSourceTable = false, "parquet")
      }
    }
  }

  test("CTAS with default fileformat") {
    val table = "ctas1"
    val ctas = s"CREATE TABLE IF NOT EXISTS $table SELECT key k, value FROM src"
    Seq("orc", "parquet").foreach { dataSourceFormat =>
      withSQLConf(
        SQLConf.CONVERT_CTAS.key -> "true",
        SQLConf.DEFAULT_DATA_SOURCE_NAME.key -> dataSourceFormat,
        "hive.default.fileformat" -> "textfile") {
        withTable(table) {
          sql(ctas)
          // The default datasource file format is controlled by `spark.sql.sources.default`.
          // This testcase verifies that setting `hive.default.fileformat` has no impact on
          // the target table's fileformat in case of CTAS.
          checkRelation(tableName = table, isDataSourceTable = true, format = dataSourceFormat)
        }
      }
    }
  }

  test("CTAS without serde with location") {
    withSQLConf(SQLConf.CONVERT_CTAS.key -> "true") {
      withTempDir { dir =>
        val defaultDataSource = sessionState.conf.defaultDataSourceName

        val tempLocation = dir.toURI.getPath.stripSuffix("/")
        withTable("ctas1") {
          sql(s"CREATE TABLE ctas1 LOCATION 'file:$tempLocation/c1'" +
            " AS SELECT key k, value FROM src ORDER BY k, value")
          checkRelation(
            "ctas1", isDataSourceTable = true, defaultDataSource, Some(s"file:$tempLocation/c1"))
        }

        withTable("ctas1") {
          sql(s"CREATE TABLE ctas1 LOCATION 'file:$tempLocation/c2'" +
            " AS SELECT key k, value FROM src ORDER BY k, value")
          checkRelation(
            "ctas1", isDataSourceTable = true, defaultDataSource, Some(s"file:$tempLocation/c2"))
        }

        withTable("ctas1") {
          sql(s"CREATE TABLE ctas1 stored as textfile LOCATION 'file:$tempLocation/c3'" +
            " AS SELECT key k, value FROM src ORDER BY k, value")
          checkRelation(
            "ctas1", isDataSourceTable = false, "text", Some(s"file:$tempLocation/c3"))
        }

        withTable("ctas1") {
          sql(s"CREATE TABLE ctas1 stored as sequenceFile LOCATION 'file:$tempLocation/c4'" +
            " AS SELECT key k, value FROM src ORDER BY k, value")
          checkRelation(
            "ctas1", isDataSourceTable = false, "sequence", Some(s"file:$tempLocation/c4"))
        }

        withTable("ctas1") {
          sql(s"CREATE TABLE ctas1 stored as rcfile LOCATION 'file:$tempLocation/c5'" +
            " AS SELECT key k, value FROM src ORDER BY k, value")
          checkRelation(
            "ctas1", isDataSourceTable = false, "rcfile", Some(s"file:$tempLocation/c5"))
        }
      }
    }
  }

  test("SPARK-28551: CTAS Hive Table should be with non-existent or empty location") {
    def executeCTASWithNonEmptyLocation(tempLocation: String): Unit = {
      sql(s"CREATE TABLE ctas1(id string) stored as rcfile LOCATION '$tempLocation/ctas1'")
      sql("INSERT INTO TABLE ctas1 SELECT 'A' ")
      sql(s"""CREATE TABLE ctas_with_existing_location stored as rcfile LOCATION
           |'$tempLocation' AS SELECT key k, value FROM src ORDER BY k, value""".stripMargin)
    }

    Seq(false, true).foreach { convertCTASFlag =>
      Seq(false, true).foreach { allowNonEmptyDirFlag =>
        withSQLConf(
          SQLConf.CONVERT_CTAS.key -> convertCTASFlag.toString,
          SQLConf.ALLOW_NON_EMPTY_LOCATION_IN_CTAS.key -> allowNonEmptyDirFlag.toString) {
          withTempDir { dir =>
            val tempLocation = dir.toURI.toString
            withTable("ctas1", "ctas_with_existing_location") {
              if (allowNonEmptyDirFlag == false) {
                val m = intercept[AnalysisException] {
                  // should not overwrite table location of table ctas1
                  executeCTASWithNonEmptyLocation(tempLocation)
                }.getMessage
                assert(m.contains("CREATE-TABLE-AS-SELECT cannot create " +
                  "table with location to a non-empty directory"))
              } else {
                executeCTASWithNonEmptyLocation(tempLocation)
              }
            }
          }
        }
      }
    }
  }

  test("CTAS with serde") {
    withTable("ctas1", "ctas2", "ctas3", "ctas4", "ctas5") {
      sql("CREATE TABLE ctas1 AS SELECT key k, value FROM src ORDER BY k, value")
      sql(
        """CREATE TABLE ctas2
          | ROW FORMAT SERDE "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe"
          | WITH SERDEPROPERTIES("serde_p1"="p1","serde_p2"="p2")
          | STORED AS RCFile
          | TBLPROPERTIES("tbl_p1"="p11", "tbl_p2"="p22")
          | AS
          |   SELECT key, value
          |   FROM src
          |   ORDER BY key, value""".stripMargin)

      val storageCtas2 = spark.sessionState.catalog.
        getTableMetadata(TableIdentifier("ctas2")).storage
      assert(storageCtas2.inputFormat == Some("org.apache.hadoop.hive.ql.io.RCFileInputFormat"))
      assert(storageCtas2.outputFormat == Some("org.apache.hadoop.hive.ql.io.RCFileOutputFormat"))
      assert(storageCtas2.serde == Some("org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe"))

      sql(
        """CREATE TABLE ctas3
          | ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\012'
          | STORED AS textfile AS
          |   SELECT key, value
          |   FROM src
          |   ORDER BY key, value""".stripMargin)

      // the table schema may like (key: integer, value: string)
      sql(
        """CREATE TABLE IF NOT EXISTS ctas4 AS
          | SELECT 1 AS key, value FROM src LIMIT 1""".stripMargin)
      // do nothing cause the table ctas4 already existed.
      sql(
        """CREATE TABLE IF NOT EXISTS ctas4 AS
          | SELECT key, value FROM src ORDER BY key, value""".stripMargin)

      checkAnswer(
        sql("SELECT k, value FROM ctas1 ORDER BY k, value"),
        sql("SELECT key, value FROM src ORDER BY key, value"))
      checkAnswer(
        sql("SELECT key, value FROM ctas2 ORDER BY key, value"),
        sql(
          """
          SELECT key, value
          FROM src
          ORDER BY key, value"""))
      checkAnswer(
        sql("SELECT key, value FROM ctas3 ORDER BY key, value"),
        sql(
          """
          SELECT key, value
          FROM src
          ORDER BY key, value"""))
      intercept[AnalysisException] {
        sql(
          """CREATE TABLE ctas4 AS
            | SELECT key, value FROM src ORDER BY key, value""".stripMargin)
      }
      checkAnswer(
        sql("SELECT key, value FROM ctas4 ORDER BY key, value"),
        sql("SELECT key, value FROM ctas4 LIMIT 1").collect().toSeq)

      sql(
        """CREATE TABLE ctas5
          | STORED AS parquet AS
          |   SELECT key, value
          |   FROM src
          |   ORDER BY key, value""".stripMargin)
      val storageCtas5 = spark.sessionState.catalog.
        getTableMetadata(TableIdentifier("ctas5")).storage
      assert(storageCtas5.inputFormat ==
        Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"))
      assert(storageCtas5.outputFormat ==
        Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"))
      assert(storageCtas5.serde ==
        Some("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"))


      // use the Hive SerDe for parquet tables
      withSQLConf(HiveUtils.CONVERT_METASTORE_PARQUET.key -> "false") {
        checkAnswer(
          sql("SELECT key, value FROM ctas5 ORDER BY key, value"),
          sql("SELECT key, value FROM src ORDER BY key, value"))
      }
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
        val e = intercept[ParseException] {
          sql("create table gen__tmp(a int, b string) as select key, value from mytable1")
        }.getMessage
        assert(e.contains("Schema may not be specified in a Create Table As Select (CTAS)"))
      }

      withTable("gen__tmp") {
        val e = intercept[ParseException] {
          sql(
            """
              |CREATE TABLE gen__tmp
              |PARTITIONED BY (key string)
              |AS SELECT key, value FROM mytable1
            """.stripMargin)
        }.getMessage
        assert(e.contains("Partition column types may not be specified in Create Table As Select"))
      }
    }
  }

  test("command substitution") {
    withSQLConf("tbl" -> "src") {
      checkAnswer(
        sql("SELECT key FROM ${hiveconf:tbl} ORDER BY key, value limit 1"),
        sql("SELECT key FROM src ORDER BY key, value limit 1").collect().toSeq)
    }

    withSQLConf("tbl2" -> "src", "spark.sql.variable.substitute" -> "false") {
      intercept[Exception] {
        sql("SELECT key FROM ${hiveconf:tbl2} ORDER BY key, value limit 1").collect()
      }
    }

    withSQLConf("tbl2" -> "src", "spark.sql.variable.substitute" -> "true") {
      checkAnswer(
        sql("SELECT key FROM ${hiveconf:tbl2} ORDER BY key, value limit 1"),
        sql("SELECT key FROM src ORDER BY key, value limit 1").collect().toSeq)
    }
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
    withTempView("nested") {
      withTable("test_ctas_1234") {
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
          sql("CREATE TABLE test_ctas_1234 AS SELECT * from nonexistent").collect()
        }
      }
    }
  }

  test("test CTAS") {
    withTable("test_ctas_123") {
      sql("CREATE TABLE test_ctas_123 AS SELECT key, value FROM src")
      checkAnswer(
        sql("SELECT key, value FROM test_ctas_123 ORDER BY key"),
        sql("SELECT key, value FROM src ORDER BY key").collect().toSeq)
    }
  }

  test("SPARK-4825 save join to table") {
    withTable("test1", "test2", "test") {
      val testData = sparkContext.parallelize(1 to 10).map(i => TestData(i, i.toString)).toDF()
      sql("CREATE TABLE test1 (key INT, value STRING)")
      testData.write.mode(SaveMode.Append).insertInto("test1")
      sql("CREATE TABLE test2 (key INT, value STRING)")
      testData.write.mode(SaveMode.Append).insertInto("test2")
      testData.write.mode(SaveMode.Append).insertInto("test2")
      sql("CREATE TABLE test USING hive AS " +
        "SELECT COUNT(a.value) FROM test1 a JOIN test2 b ON a.key = b.key")
      checkAnswer(
        table("test"),
        sql("SELECT COUNT(a.value) FROM test1 a JOIN test2 b ON a.key = b.key").collect().toSeq)
    }
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
    withTempView("sampled") {
      sql("SELECT * FROM src WHERE key % 2 = 0")
        .sample(withReplacement = false, fraction = 0.3)
        .createOrReplaceTempView("sampled")
      (1 to 10).foreach { i =>
        checkAnswer(
          sql("SELECT * FROM sampled WHERE key % 2 = 1"),
          Seq.empty[Row])
      }
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
    val ds = Seq("""{"a": "str", "b":"1", "c":"1970-01-01 00:00:00"}""").toDS()
    read.json(ds).createOrReplaceTempView("data")
    checkAnswer(
      sql("SELECT concat(a, '-', b), year(c) FROM data GROUP BY concat(a, '-', b), year(c)"),
      Row("str-1", 1970))

    dropTempTable("data")

    read.json(ds).createOrReplaceTempView("data")
    checkAnswer(sql("SELECT year(c) + 1 FROM data GROUP BY year(c) + 1"), Row(1971))

    dropTempTable("data")
  }

  test("resolve udtf in projection #1") {
    withTempView("data") {
      val ds = (1 to 5).map(i => s"""{"a":[$i, ${i + 1}]}""").toDS()
      read.json(ds).createOrReplaceTempView("data")
      sql("SELECT explode(a) AS val FROM data")
    }
  }

  test("resolve udtf in projection #2") {
    withTempView("data") {
      val ds = (1 to 2).map(i => s"""{"a":[$i, ${i + 1}]}""").toDS()
      read.json(ds).createOrReplaceTempView("data")
      checkAnswer(sql("SELECT explode(map(1, 1)) FROM data LIMIT 1"), Row(1, 1) :: Nil)
      checkAnswer(sql("SELECT explode(map(1, 1)) as (k1, k2) FROM data LIMIT 1"), Row(1, 1) :: Nil)
      intercept[AnalysisException] {
        sql("SELECT explode(map(1, 1)) as k1 FROM data LIMIT 1")
      }

      intercept[AnalysisException] {
        sql("SELECT explode(map(1, 1)) as (k1, k2, k3) FROM data LIMIT 1")
      }
    }
  }

  // TGF with non-TGF in project is allowed in Spark SQL, but not in Hive
  test("TGF with non-TGF in projection") {
    withTempView("data") {
      val ds = Seq("""{"a": "1", "b":"1"}""").toDS()
      read.json(ds).createOrReplaceTempView("data")
      checkAnswer(
        sql("SELECT explode(map(a, b)) as (k1, k2), a, b FROM data"),
        Row("1", "1", "1", "1") :: Nil)
    }
  }

  test("logical.Project should not be resolved if it contains aggregates or generators") {
    // This test is used to test the fix of SPARK-5875.
    // The original issue was that Project's resolved will be true when it contains
    // AggregateExpressions or Generators. However, in this case, the Project
    // is not in a valid state (cannot be executed). Because of this bug, the analysis rule of
    // PreInsertionCasts will actually start to work before ImplicitGenerate and then
    // generates an invalid query plan.
    val ds = (1 to 5).map(i => s"""{"a":[$i, ${i + 1}]}""").toDS()
    read.json(ds).createOrReplaceTempView("data")

    withSQLConf(SQLConf.CONVERT_CTAS.key -> "false") {
      sql("CREATE TABLE explodeTest (key bigInt) USING hive")
      table("explodeTest").queryExecution.analyzed match {
        case SubqueryAlias(_, r: HiveTableRelation) => // OK
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
    }
  }

  test("sanity test for SPARK-6618") {
    val threads: Seq[Thread] = (1 to 10).map { i =>
      new Thread("test-thread-" + i) {
        override def run(): Unit = {
          val tableName = s"SPARK_6618_table_$i"
          sql(s"CREATE TABLE $tableName (col1 string)")
          sessionState.catalog.lookupRelation(TableIdentifier(tableName))
          table(tableName)
          tables()
          sql(s"DROP TABLE $tableName")
        }
      }
    }
    threads.foreach(_.start())
    threads.foreach(_.join(10000))
  }

  test("SPARK-5203 union with different decimal precision") {
    withTempView("dn") {
      Seq.empty[(java.math.BigDecimal, java.math.BigDecimal)]
        .toDF("d1", "d2")
        .select($"d1".cast(DecimalType(10, 5)).as("d"))
        .createOrReplaceTempView("dn")

      sql("select d from dn union all select d * 2 from dn")
        .queryExecution.analyzed
    }
  }

  test("Star Expansion - script transform") {
    withTempView("script_trans") {
      assume(TestUtils.testCommandAvailable("/bin/bash"))
      val data = (1 to 100000).map { i => (i, i, i) }
      data.toDF("d1", "d2", "d3").createOrReplaceTempView("script_trans")
      assert(100000 === sql("SELECT TRANSFORM (*) USING 'cat' FROM script_trans").count())
    }
  }

  test("test script transform for stdout") {
    withTempView("script_trans") {
      assume(TestUtils.testCommandAvailable("/bin/bash"))
      val data = (1 to 100000).map { i => (i, i, i) }
      data.toDF("d1", "d2", "d3").createOrReplaceTempView("script_trans")
      assert(100000 ===
        sql("SELECT TRANSFORM (d1, d2, d3) USING 'cat' AS (a,b,c) FROM script_trans").count())
    }
  }

  test("test script transform for stderr") {
    withTempView("script_trans") {
      assume(TestUtils.testCommandAvailable("/bin/bash"))
      val data = (1 to 100000).map { i => (i, i, i) }
      data.toDF("d1", "d2", "d3").createOrReplaceTempView("script_trans")
      assert(0 ===
        sql("SELECT TRANSFORM (d1, d2, d3) USING 'cat 1>&2' AS (a,b,c) FROM script_trans").count())
    }
  }

  test("test script transform data type") {
    withTempView("test") {
      assume(TestUtils.testCommandAvailable("/bin/bash"))
      val data = (1 to 5).map { i => (i, i) }
      data.toDF("key", "value").createOrReplaceTempView("test")
      checkAnswer(
        sql(
          """FROM
            |(FROM test SELECT TRANSFORM(key, value) USING 'cat' AS (`thing1` int, thing2 string)) t
            |SELECT thing1 + 1
          """.stripMargin), (2 to 6).map(i => Row(i)))
    }
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
    withTempView("t") {
      (1 to 5).map(i => (i, i.toString)).toDF("k", "v").createOrReplaceTempView("t")
      checkAnswer(
        sql("SELECT CASE k WHEN 2 THEN 22 WHEN 4 THEN 44 ELSE 0 END, v FROM t"),
        Row(0, "1") :: Row(22, "2") :: Row(0, "3") :: Row(44, "4") :: Row(0, "5") :: Nil)
    }
  }

  test("SPARK-7269 Check analysis failed in case in-sensitive") {
    withTempView("df_analysis") {
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
  }

  test("Cast STRING to BIGINT") {
    checkAnswer(sql("SELECT CAST('775983671874188101' as BIGINT)"), Row(775983671874188101L))
  }

  test("dynamic partition value test") {
    withTable("dynparttest1", "dynparttest2") {
      withSQLConf("hive.exec.dynamic.partition.mode" -> "nonstrict") {
        // date
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
        sql("create table dynparttest2 (value int) partitioned by (pdec decimal(5, 1))")
        sql(
          """
             |insert into table dynparttest2 partition(pdec)
             | select count(*), cast('100.12' as decimal(5, 1)) as pdec from src
          """.stripMargin)
        checkAnswer(
            sql("select * from dynparttest2"),
            Seq(Row(500, new java.math.BigDecimal("100.1"))))
      }
    }
  }

  test("Call add jar in a different thread (SPARK-8306)") {
    @volatile var error: Option[Throwable] = None
    val thread = new Thread {
      override def run(): Unit = {
        // To make sure this test works, this jar should not be loaded in another place.
        sql(
          s"ADD JAR ${HiveTestJars.getHiveContribJar().getCanonicalPath}")
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
      sql("SELECT CAST(timestamp_seconds(0) AS date) > timestamp_seconds(0) FROM src LIMIT 1"),
      Row(false))
  }

  test("SPARK-6785: HiveQuerySuite - Date cast") {
    // new Date(0) == 1970-01-01 00:00:00.0 GMT == 1969-12-31 16:00:00.0 PST
    checkAnswer(
      sql(
        """
          | SELECT
          | CAST(timestamp_seconds(0) AS date),
          | CAST(CAST(timestamp_seconds(0) AS date) AS string),
          | timestamp_seconds(0),
          | CAST(timestamp_seconds(0) AS string),
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
    withTempView("t") {
      val ds = Seq("""{"a": {"c.b": 1}, "b.$q": [{"a@!.q": 1}], "q.w": {"w.i&": [1]}}""").toDS()
      read.json(ds).createOrReplaceTempView("t")

      checkAnswer(sql("SELECT a.`c.b`, `b.$q`[0].`a@!.q`, `q.w`.`w.i&`[0] FROM t"), Row(1, 1, 1))
    }
  }

  test("specifying database name for a temporary view is not allowed") {
    withTempPath { dir =>
      withTempView("db.t") {
        val path = dir.toURI.toString
        val df = sparkContext.parallelize(1 to 10).map(i => (i, i.toString)).toDF("num", "str")
        df
          .write
          .format("parquet")
          .save(path)

        // We don't support creating a temporary table while specifying a database
        intercept[ParseException] {
          spark.sql(
            s"""
              |CREATE TEMPORARY VIEW db.t
              |USING parquet
              |OPTIONS (
              |  path '$path'
              |)
             """.stripMargin)
        }

        // If you use backticks to quote the name then it's OK.
        spark.sql(
          s"""
            |CREATE TEMPORARY VIEW `db.t`
            |USING parquet
            |OPTIONS (
            |  path '$path'
            |)
           """.stripMargin)
        checkAnswer(spark.table("`db.t`"), df)
      }
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
    withTempView("test") {
      spark
        .range(5)
        .selectExpr("id AS a", "id AS b")
        .createOrReplaceTempView("test")

      val scriptFilePath = getTestResourcePath("data")
      checkAnswer(
        sql(
          s"""FROM(
            |  FROM test SELECT TRANSFORM(a, b)
            |  USING 'python3 $scriptFilePath/scripts/test_transform.py "\t"'
            |  AS (c STRING, d STRING)
            |) t
            |SELECT c
          """.stripMargin),
        (0 until 5).map(i => Row(s"$i#")))
    }
  }

  ignore("SPARK-10310: script transformation using LazySimpleSerDe") {
    withTempView("test") {
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
          |USING 'python3 $scriptFilePath/scripts/test_transform.py "|"'
          |AS (c STRING, d STRING)
          |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
          |WITH SERDEPROPERTIES('field.delim' = '|')
        """.stripMargin)

      checkAnswer(df, (0 until 5).map(i => Row(s"$i#", s"$i#")))
    }
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

  test("SPARK-44520: invalid path for support direct query shall throw correct exception") {
    checkError(
      exception = intercept[AnalysisException] {
        sql(s"select id from parquet.`invalid_path`")
      },
      condition = "PATH_NOT_FOUND",
      parameters = Map("path" -> "file.*invalid_path"),
      matchPVals = true
    )
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

  test("run sql directly on files - hive") {
    withTempPath(f => {
      spark.range(100).toDF().write.parquet(f.getCanonicalPath)

      checkError(
        exception = intercept[AnalysisException] {
          sql(s"select id from hive.`${f.getCanonicalPath}`")
        },
        condition = "UNSUPPORTED_DATASOURCE_FOR_DIRECT_QUERY",
        parameters = Map("dataSourceType" -> "hive"),
        context = ExpectedContext(s"hive.`${f.getCanonicalPath}`",
          15, 21 + f.getCanonicalPath.length)
      )

      // data source type is case insensitive
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"select id from HIVE.`${f.getCanonicalPath}`")
        },
        condition = "UNSUPPORTED_DATASOURCE_FOR_DIRECT_QUERY",
        parameters = Map("dataSourceType" -> "HIVE"),
        context = ExpectedContext(s"HIVE.`${f.getCanonicalPath}`",
          15, 21 + f.getCanonicalPath.length)
      )
    })
  }

  test("SPARK-8976 Wrong Result for Rollup #1") {
    Seq("grouping_id()", "grouping__id").foreach { gid =>
      checkAnswer(sql(
        s"SELECT count(*) AS cnt, key % 5, $gid FROM src GROUP BY key%5 WITH ROLLUP"),
        Seq(
          (113, 3, 0),
          (91, 0, 0),
          (500, null, 1),
          (84, 1, 0),
          (105, 2, 0),
          (107, 4, 0)
        ).map(i => Row(i._1, i._2, i._3)))
    }
  }

  test("SPARK-8976 Wrong Result for Rollup #2") {
    Seq("grouping_id()", "grouping__id").foreach { gid =>
      checkAnswer(sql(
        s"""
          |SELECT count(*) AS cnt, key % 5 AS k1, key-5 AS k2, $gid AS k3
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
  }

  test("SPARK-8976 Wrong Result for Rollup #3") {
    Seq("grouping_id()", "grouping__id").foreach { gid =>
      checkAnswer(sql(
        s"""
          |SELECT count(*) AS cnt, key % 5 AS k1, key-5 AS k2, $gid AS k3
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
  }

  test("SPARK-8976 Wrong Result for CUBE #1") {
    Seq("grouping_id()", "grouping__id").foreach { gid =>
      checkAnswer(sql(
        s"SELECT count(*) AS cnt, key % 5, $gid FROM src GROUP BY key%5 WITH CUBE"),
        Seq(
          (113, 3, 0),
          (91, 0, 0),
          (500, null, 1),
          (84, 1, 0),
          (105, 2, 0),
          (107, 4, 0)
        ).map(i => Row(i._1, i._2, i._3)))
    }
  }

  test("SPARK-8976 Wrong Result for CUBE #2") {
    Seq("grouping_id()", "grouping__id").foreach { gid =>
      checkAnswer(sql(
        s"""
          |SELECT count(*) AS cnt, key % 5 AS k1, key-5 AS k2, $gid AS k3
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
  }

  test("SPARK-8976 Wrong Result for GroupingSet") {
    Seq("grouping_id()", "grouping__id").foreach { gid =>
      checkAnswer(sql(
        s"""
          |SELECT count(*) AS cnt, key % 5 AS k1, key-5 AS k2, $gid AS k3
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
  }

  ignore("SPARK-10562: partition by column with mixed case name") {
    withTable("tbl10562") {
      val df = Seq(2012 -> "a").toDF("Year", "val")
      df.write.partitionBy("Year").saveAsTable("tbl10562")
      checkAnswer(sql("SELECT year FROM tbl10562"), Row(2012))
      checkAnswer(sql("SELECT Year FROM tbl10562"), Row(2012))
      checkAnswer(sql("SELECT yEAr FROM tbl10562"), Row(2012))
// TODO(ekl) this is causing test flakes [SPARK-18167], but we think the issue is derby specific
//      checkAnswer(sql("SELECT val FROM tbl10562 WHERE Year > 2015"), Nil)
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
    withTempView("source") {
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

      // project list is the same order of partitioning columns in table definition
      checkAnswer(
        sql(s"SELECT p1, p2, p3, p4, p5, c1 FROM $table"),
        Row("a", "b", "c", "d", "e", "blarr") :: Nil)

      // project list does not have the same order of partitioning columns in table definition
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
      checkError(
        exception = intercept[ParseException] {
          sql(
            """CREATE TABLE t USING PARQUET
              |OPTIONS (PATH '/path/to/file')
              |CLUSTERED BY (a) SORTED BY (b DESC) INTO 2 BUCKETS
              |AS SELECT 1 AS a, 2 AS b
            """.stripMargin)
        },
        condition = "_LEGACY_ERROR_TEMP_0035",
        parameters = Map("message" -> "Column ordering must be ASC, was 'DESC'"),
        context = ExpectedContext(
          fragment = "CLUSTERED BY (a) SORTED BY (b DESC) INTO 2 BUCKETS",
          start = 60,
          stop = 109))
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
    withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true",
      "hive.exec.dynamic.partition.mode" -> "nonstrict") {
      sql(
        """CREATE TABLE order(id INT)
          |PARTITIONED BY (pd DATE, pt TIMESTAMP)
          |STORED AS PARQUET
        """.stripMargin)

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
    withTable("t1", "t2", "t3") {
      sql("create table t1(a map<bigint, array<string>>)")
      sql("select * from t1 where a[1] is not null")

      sql("create table t2(a map<int, array<string>>)")
      sql("select * from t2 where a[1] is not null")

      sql("create table t3(a map<bigint, array<string>>)")
      sql("select * from t3 where a[1L] is not null")
    }
  }

  test("SPARK-17796 Support wildcard character in filename for LOAD DATA LOCAL INPATH") {
    withTempDir { dir =>
      val path = dir.toURI.toString.stripSuffix("/")
      val dirPath = dir.getAbsoluteFile
      for (i <- 1 to 3) {
        Files.write(s"$i", new File(dirPath, s"part-r-0000$i"), StandardCharsets.UTF_8)
      }
      for (i <- 5 to 7) {
        Files.write(s"$i", new File(dirPath, s"part-s-0000$i"), StandardCharsets.UTF_8)
      }

      withTable("load_t") {
        sql("CREATE TABLE load_t (a STRING) USING hive")
        sql(s"LOAD DATA LOCAL INPATH '$path/*part-r*' INTO TABLE load_t")
        checkAnswer(sql("SELECT * FROM load_t"), Seq(Row("1"), Row("2"), Row("3")))

        val m = intercept[AnalysisException] {
          sql("LOAD DATA LOCAL INPATH '/non-exist-folder/*part*' INTO TABLE load_t")
        }.getMessage
        assert(m.contains("LOAD DATA input path does not exist"))
      }
    }
  }

  test("SPARK-23425 Test LOAD DATA LOCAL INPATH with space in file name") {
    withTempDir { dir =>
      val path = dir.toURI.toString.stripSuffix("/")
      val dirPath = dir.getAbsoluteFile
      for (i <- 1 to 3) {
        Files.write(s"$i", new File(dirPath, s"part-r-0000 $i"), StandardCharsets.UTF_8)
      }
      withTable("load_t") {
        sql("CREATE TABLE load_t (a STRING) USING hive")
        sql(s"LOAD DATA LOCAL INPATH '$path/part-r-0000 1' INTO TABLE load_t")
        checkAnswer(sql("SELECT * FROM load_t"), Seq(Row("1")))
      }
    }
  }

  test("Support wildcard character in folderlevel for LOAD DATA LOCAL INPATH") {
    withTempDir { dir =>
      val path = dir.toURI.toString.stripSuffix("/")
      val dirPath = dir.getAbsoluteFile
      for (i <- 1 to 3) {
        Files.write(s"$i", new File(dirPath, s"part-r-0000$i"), StandardCharsets.UTF_8)
      }
      withTable("load_t") {
        sql("CREATE TABLE load_t (a STRING) USING hive")
        sql(s"LOAD DATA LOCAL INPATH '${
          path.substring(0, path.length - 1)
            .concat("*")
        }/' INTO TABLE load_t")
        checkAnswer(sql("SELECT * FROM load_t"), Seq(Row("1"), Row("2"), Row("3")))
        val m = intercept[AnalysisException] {
          sql(s"LOAD DATA LOCAL INPATH '${
            path.substring(0, path.length - 1).concat("_invalid_dir") concat ("*")
          }/' INTO TABLE load_t")
        }.getMessage
        assert(m.contains("LOAD DATA input path does not exist"))
      }
    }
  }

  test("SPARK-17796 Support wildcard '?'char in middle as part of local file path") {
    withTempDir { dir =>
      val path = dir.toURI.toString.stripSuffix("/")
      val dirPath = dir.getAbsoluteFile
      for (i <- 1 to 3) {
        Files.write(s"$i", new File(dirPath, s"part-r-0000$i"), StandardCharsets.UTF_8)
      }
      withTable("load_t1") {
        sql("CREATE TABLE load_t1 (a STRING) USING hive")
        sql(s"LOAD DATA LOCAL INPATH '$path/part-r-0000?' INTO TABLE load_t1")
        checkAnswer(sql("SELECT * FROM load_t1"), Seq(Row("1"), Row("2"), Row("3")))
      }
    }
  }

  test("SPARK-17796 Support wildcard '?'char in start as part of local file path") {
    withTempDir { dir =>
      val path = dir.toURI.toString.stripSuffix("/")
      val dirPath = dir.getAbsoluteFile
      for (i <- 1 to 3) {
        Files.write(s"$i", new File(dirPath, s"part-r-0000$i"), StandardCharsets.UTF_8)
      }
      withTable("load_t2") {
        sql("CREATE TABLE load_t2 (a STRING) USING hive")
        sql(s"LOAD DATA LOCAL INPATH '$path/?art-r-00001' INTO TABLE load_t2")
        checkAnswer(sql("SELECT * FROM load_t2"), Seq(Row("1")))
      }
    }
  }

  test("SPARK-28084 check for case insensitive property of partition column name in load command") {
    withTempDir { dir =>
      val path = dir.toURI.toString.stripSuffix("/")
      val dirPath = dir.getAbsoluteFile
      Files.append("1", new File(dirPath, "part-r-000011"), StandardCharsets.UTF_8)
      withTable("part_table") {
        withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
          sql(
            """
              |CREATE TABLE part_table (c STRING)
              |STORED AS textfile
              |PARTITIONED BY (d STRING)
            """.stripMargin)
          sql(s"LOAD DATA LOCAL INPATH '$path/part-r-000011' " +
            "INTO TABLE part_table PARTITION(D ='1')")
          checkAnswer(sql("SELECT * FROM part_table"), Seq(Row("1", "1")))
        }
      }
    }
  }

  test("SPARK-25738: defaultFs can have a port") {
    val defaultURI = new URI("hdfs://fizz.buzz.com:8020")
    val r = LoadDataCommand.makeQualified(defaultURI, new Path("/foo/bar"), new Path("/flim/flam"))
    assert(r === new Path("hdfs://fizz.buzz.com:8020/flim/flam"))
  }

  test("Insert overwrite with partition") {
    withTable("tableWithPartition") {
      sql(
        """
          |CREATE TABLE tableWithPartition (key int, value STRING)
          |PARTITIONED BY (part STRING)
        """.stripMargin)
      sql(
        """
          |INSERT OVERWRITE TABLE tableWithPartition PARTITION (part = '1')
          |SELECT * FROM default.src
        """.stripMargin)
       checkAnswer(
         sql("SELECT part, key, value FROM tableWithPartition"),
         sql("SELECT '1' AS part, key, value FROM default.src")
       )

      sql(
        """
          |INSERT OVERWRITE TABLE tableWithPartition PARTITION (part = '1')
          |SELECT * FROM VALUES (1, "one"), (2, "two"), (3, null) AS data(key, value)
        """.stripMargin)
      checkAnswer(
        sql("SELECT part, key, value FROM tableWithPartition"),
        sql(
          """
            |SELECT '1' AS part, key, value FROM VALUES
            |(1, "one"), (2, "two"), (3, null) AS data(key, value)
          """.stripMargin)
      )
    }
  }

  test("SPARK-19292: filter with partition columns should be case-insensitive on Hive tables") {
    withTable("tbl") {
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
        sql("CREATE TABLE tbl(i int, j int) USING hive PARTITIONED BY (j)")
        sql("INSERT INTO tbl PARTITION(j=10) SELECT 1")
        checkAnswer(spark.table("tbl"), Row(1, 10))

        checkAnswer(sql("SELECT i, j FROM tbl WHERE J=10"), Row(1, 10))
        checkAnswer(spark.table("tbl").filter($"J" === 10), Row(1, 10))
      }
    }
  }

  test("column resolution scenarios with hive table") {
    val currentDb = spark.catalog.currentDatabase
    withTempDatabase { db1 =>
      try {
        spark.catalog.setCurrentDatabase(db1)
        spark.sql("CREATE TABLE t1(i1 int) STORED AS parquet")
        spark.sql("INSERT INTO t1 VALUES(1)")
        checkAnswer(spark.sql(s"SELECT $db1.t1.i1 FROM t1"), Row(1))
        checkAnswer(spark.sql(s"SELECT $db1.t1.i1 FROM $db1.t1"), Row(1))
        checkAnswer(spark.sql(s"SELECT $db1.t1.* FROM $db1.t1"), Row(1))
      } finally {
        spark.catalog.setCurrentDatabase(currentDb)
      }
    }
  }

  test("SPARK-17409: Do Not Optimize Query in CTAS (Hive Serde Table) More Than Once") {
    withTable("bar") {
      withTempView("foo") {
        sql("select 0 as id").createOrReplaceTempView("foo")
        // If we optimize the query in CTAS more than once, the following saveAsTable will fail
        // with the error: `GROUP BY position 0 is not in select list (valid range is [1, 1])`
        sql("SELECT * FROM foo group by id").toDF().write.format("hive").saveAsTable("bar")
        checkAnswer(spark.table("bar"), Row(0) :: Nil)
        val tableMetadata = spark.sessionState.catalog.getTableMetadata(TableIdentifier("bar"))
        assert(tableMetadata.provider == Some("hive"), "the expected table is a Hive serde table")
      }
    }
  }

  test("SPARK-19912 String literals should be escaped for Hive metastore partition pruning") {
    withTable("spark_19912") {
      Seq(
        (1, "p1", "q1"),
        (2, "'", "q2"),
        (3, "\"", "q3"),
        (4, "p1\" and q=\"q1", "q4")
      ).toDF("a", "p", "q").write.partitionBy("p", "q").saveAsTable("spark_19912")

      val table = spark.table("spark_19912")
      checkAnswer(table.filter($"p" === "'").select($"a"), Row(2))
      checkAnswer(table.filter($"p" === "\"").select($"a"), Row(3))
      checkAnswer(table.filter($"p" === "p1\" and q=\"q1").select($"a"), Row(4))
    }
  }

  test("SPARK-21721: Clear FileSystem deleterOnExit cache if path is successfully removed") {
    val table = "test21721"
    withTable(table) {
      val deleteOnExitField = classOf[FileSystem].getDeclaredField("deleteOnExit")
      deleteOnExitField.setAccessible(true)

      val fs = FileSystem.get(spark.sessionState.newHadoopConf())
      val setOfPath = deleteOnExitField.get(fs).asInstanceOf[Set[Path]]

      val testData = sparkContext.parallelize(1 to 10).map(i => TestData(i, i.toString)).toDF()
      sql(s"CREATE TABLE $table (key INT, value STRING)")
      val pathSizeToDeleteOnExit = setOfPath.size()

      (0 to 10).foreach(_ => testData.write.mode(SaveMode.Append).insertInto(table))

      assert(setOfPath.size() == pathSizeToDeleteOnExit)
    }
  }

  test("SPARK-32889: ORC table column name supports special characters") {
    // "," is not allowed since cannot create a table having a column whose name
    // contains commas in Hive metastore.
    Seq("$", ";", "{", "}", "(", ")", "\n", "\t", "=", " ", "a b").foreach { name =>
      val source = "ORC"
      Seq(s"CREATE TABLE t32889(`$name` INT) USING $source",
          s"CREATE TABLE t32889 STORED AS $source AS SELECT 1 `$name`",
          s"CREATE TABLE t32889 USING $source AS SELECT 1 `$name`",
          s"CREATE TABLE t32889(`$name` INT) USING hive OPTIONS (fileFormat '$source')")
      .foreach { command =>
        withTable("t32889") {
          sql(command)
          assertResult(name)(
            sessionState.catalog.getTableMetadata(TableIdentifier("t32889")).schema.fields(0).name)
        }
      }

      withTable("t32889") {
        sql(s"CREATE TABLE t32889(`col` INT) USING $source")
        sql(s"ALTER TABLE t32889 ADD COLUMNS(`$name` INT)")
        assertResult(name)(
          sessionState.catalog.getTableMetadata(TableIdentifier("t32889")).schema.fields(1).name)
      }
    }
  }

  Seq("orc", "parquet").foreach { format =>
    test(s"SPARK-18355 Read data from a hive table with a new column - $format") {
      val client =
        spark.sharedState.externalCatalog.unwrapped.asInstanceOf[HiveExternalCatalog].client

      Seq("true", "false").foreach { value =>
        withSQLConf(
          HiveUtils.CONVERT_METASTORE_ORC.key -> value,
          HiveUtils.CONVERT_METASTORE_PARQUET.key -> value) {
          withTempDatabase { db =>
            client.runSqlHive(
              s"""
                 |CREATE TABLE $db.t(
                 |  click_id string,
                 |  search_id string,
                 |  uid bigint)
                 |PARTITIONED BY (
                 |  ts string,
                 |  hour string)
                 |STORED AS $format
              """.stripMargin)

            client.runSqlHive(
              s"""
                 |INSERT INTO TABLE $db.t
                 |PARTITION (ts = '98765', hour = '01')
                 |VALUES (12, 2, 12345)
              """.stripMargin
            )

            checkAnswer(
              sql(s"SELECT click_id, search_id, uid, ts, hour FROM $db.t"),
              Row("12", "2", 12345, "98765", "01"))

            client.runSqlHive(s"ALTER TABLE $db.t ADD COLUMNS (dummy string)")

            checkAnswer(
              sql(s"SELECT click_id, search_id FROM $db.t"),
              Row("12", "2"))

            checkAnswer(
              sql(s"SELECT search_id, click_id FROM $db.t"),
              Row("2", "12"))

            checkAnswer(
              sql(s"SELECT search_id FROM $db.t"),
              Row("2"))

            checkAnswer(
              sql(s"SELECT dummy, click_id FROM $db.t"),
              Row(null, "12"))

            checkAnswer(
              sql(s"SELECT click_id, search_id, uid, dummy, ts, hour FROM $db.t"),
              Row("12", "2", 12345, null, "98765", "01"))
          }
        }
      }
    }
  }

  test("SPARK-24085 scalar subquery in partitioning expression") {
    Seq("orc", "parquet").foreach { format =>
      Seq(true, false).foreach { isConverted =>
        withSQLConf(
          HiveUtils.CONVERT_METASTORE_ORC.key -> s"$isConverted",
          HiveUtils.CONVERT_METASTORE_PARQUET.key -> s"$isConverted",
          "hive.exec.dynamic.partition.mode" -> "nonstrict") {
          withTable(format) {
            withTempPath { tempDir =>
              sql(
                s"""
                  |CREATE TABLE ${format} (id_value string)
                  |PARTITIONED BY (id_type string)
                  |LOCATION '${tempDir.toURI}'
                  |STORED AS ${format}
                """.stripMargin)
              sql(s"insert into $format values ('1','a')")
              sql(s"insert into $format values ('2','a')")
              sql(s"insert into $format values ('3','b')")
              sql(s"insert into $format values ('4','b')")
              checkAnswer(
                sql(s"SELECT * FROM $format WHERE id_type = (SELECT 'b')"),
                Row("3", "b") :: Row("4", "b") :: Nil)
            }
          }
        }
      }
    }
  }

  test("SPARK-26181 hasMinMaxStats method of ColumnStatsMap is not correct") {
    withSQLConf(SQLConf.CBO_ENABLED.key -> "true") {
      withTable("all_null") {
        sql("create table all_null (attr1 int, attr2 int)")
        sql("insert into all_null values (null, null)")
        sql("analyze table all_null compute statistics for columns attr1, attr2")
        // check if the stats can be calculated without Cast exception.
        sql("select * from all_null where attr1 < 1").queryExecution.stringWithStats
        sql("select * from all_null where attr1 < attr2").queryExecution.stringWithStats
      }
    }
  }

  test("SPARK-26709: OptimizeMetadataOnlyQuery does not handle empty records correctly") {
    Seq(true, false).foreach { enableOptimizeMetadataOnlyQuery =>
      // This test case is only for file source V1. As the rule OptimizeMetadataOnlyQuery is
      // disabled by default, we can skip testing file source v2 in current stage.
      withSQLConf(SQLConf.OPTIMIZER_METADATA_ONLY.key -> enableOptimizeMetadataOnlyQuery.toString,
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
        withTable("t") {
          sql("CREATE TABLE t (col1 INT, p1 INT) USING PARQUET PARTITIONED BY (p1)")
          sql("INSERT INTO TABLE t PARTITION (p1 = 5) SELECT ID FROM range(1, 1)")
          if (enableOptimizeMetadataOnlyQuery) {
            // The result is wrong if we enable the configuration.
            checkAnswer(sql("SELECT MAX(p1) FROM t"), Row(5))
          } else {
            checkAnswer(sql("SELECT MAX(p1) FROM t"), Row(null))
          }
          checkAnswer(sql("SELECT MAX(col1) FROM t"), Row(null))
        }
      }
    }
  }

  test("SPARK-25158: " +
    "Executor accidentally exit because ScriptTransformationWriterThread throw Exception") {
    assume(TestUtils.testCommandAvailable("python3"))
    withTempView("test") {
      val defaultUncaughtExceptionHandler = Thread.getDefaultUncaughtExceptionHandler
      try {
        val uncaughtExceptionHandler = new TestUncaughtExceptionHandler
        Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler)

        // Use a bad udf to generate failed inputs.
        val badUDF = org.apache.spark.sql.functions.udf((x: Int) => {
          if (x < 1) x
          else throw new RuntimeException("Failed to produce data.")
        })
        spark
          .range(5)
          .select(badUDF($"id").as("a"))
          .createOrReplaceTempView("test")
        val scriptFilePath = getTestResourcePath("data")
        val e = intercept[SparkException] {
          sql(
            s"""FROM test SELECT TRANSFORM(a)
               |USING 'python3 $scriptFilePath/scripts/test_transform.py "\t"'
             """.stripMargin).collect()
        }
        assert(e.getMessage.contains("Failed to produce data."))
        assert(uncaughtExceptionHandler.exception.isEmpty)
      } finally {
        Thread.setDefaultUncaughtExceptionHandler(defaultUncaughtExceptionHandler)
      }
    }
  }

  test("SPARK-29295: insert overwrite external partition should not have old data") {
    Seq("true", "false").foreach { convertParquet =>
      withTable("test") {
        withTempDir { f =>
          sql("CREATE EXTERNAL TABLE test(id int) PARTITIONED BY (name string) STORED AS " +
            s"PARQUET LOCATION '${f.getAbsolutePath}'")

          withSQLConf(HiveUtils.CONVERT_METASTORE_PARQUET.key -> convertParquet) {
            sql("INSERT OVERWRITE TABLE test PARTITION(name='n1') SELECT 1")
            sql("ALTER TABLE test DROP PARTITION(name='n1')")
            sql("INSERT OVERWRITE TABLE test PARTITION(name='n1') SELECT 2")
            checkAnswer(sql("SELECT id FROM test WHERE name = 'n1' ORDER BY id"),
              Array(Row(2)))
          }
        }
      }
    }
  }

  test("SPARK-29295: dynamic insert overwrite external partition should not have old data") {
    Seq("true", "false").foreach { convertParquet =>
      withTable("test") {
        withTempDir { f =>
          sql("CREATE EXTERNAL TABLE test(id int) PARTITIONED BY (p1 string, p2 string) " +
            s"STORED AS PARQUET LOCATION '${f.getAbsolutePath}'")

          withSQLConf(HiveUtils.CONVERT_METASTORE_PARQUET.key -> convertParquet,
            "hive.exec.dynamic.partition.mode" -> "nonstrict") {
            sql(
              """
                |INSERT OVERWRITE TABLE test PARTITION(p1='n1', p2)
                |SELECT * FROM VALUES (1, 'n2'), (2, 'n3') AS t(id, p2)
              """.stripMargin)
            checkAnswer(sql("SELECT id FROM test WHERE p1 = 'n1' and p2 = 'n2' ORDER BY id"),
              Array(Row(1)))
            checkAnswer(sql("SELECT id FROM test WHERE p1 = 'n1' and p2 = 'n3' ORDER BY id"),
              Array(Row(2)))

            sql("INSERT OVERWRITE TABLE test PARTITION(p1='n1', p2) SELECT 4, 'n4'")
            checkAnswer(sql("SELECT id FROM test WHERE p1 = 'n1' and p2 = 'n4' ORDER BY id"),
              Array(Row(4)))

            sql("ALTER TABLE test DROP PARTITION(p1='n1',p2='n2')")
            sql("ALTER TABLE test DROP PARTITION(p1='n1',p2='n3')")

            sql(
              """
                |INSERT OVERWRITE TABLE test PARTITION(p1='n1', p2)
                |SELECT * FROM VALUES (5, 'n2'), (6, 'n3') AS t(id, p2)
              """.stripMargin)
            checkAnswer(sql("SELECT id FROM test WHERE p1 = 'n1' and p2 = 'n2' ORDER BY id"),
              Array(Row(5)))
            checkAnswer(sql("SELECT id FROM test WHERE p1 = 'n1' and p2 = 'n3' ORDER BY id"),
              Array(Row(6)))
            // Partition not overwritten should not be deleted.
            checkAnswer(sql("SELECT id FROM test WHERE p1 = 'n1' and p2 = 'n4' ORDER BY id"),
              Array(Row(4)))
          }
        }
      }

      withTable("test") {
        withTempDir { f =>
          sql("CREATE EXTERNAL TABLE test(id int) PARTITIONED BY (p1 string, p2 string) " +
            s"STORED AS PARQUET LOCATION '${f.getAbsolutePath}'")

          withSQLConf(HiveUtils.CONVERT_METASTORE_PARQUET.key -> convertParquet,
            "hive.exec.dynamic.partition.mode" -> "nonstrict") {
            // We should unescape partition value.
            sql("INSERT OVERWRITE TABLE test PARTITION(p1='n1', p2) SELECT 1, '/'")
            sql("ALTER TABLE test DROP PARTITION(p1='n1',p2='/')")
            sql("INSERT OVERWRITE TABLE test PARTITION(p1='n1', p2) SELECT 2, '/'")
            checkAnswer(sql("SELECT id FROM test WHERE p1 = 'n1' and p2 = '/' ORDER BY id"),
              Array(Row(2)))
          }
        }
      }
    }
  }

  test("partition pruning should handle date correctly") {
    withSQLConf(SQLConf.OPTIMIZER_INSET_CONVERSION_THRESHOLD.key -> "2") {
      withTable("t") {
        sql("CREATE TABLE t (i INT) PARTITIONED BY (j DATE)")
        sql("INSERT INTO t PARTITION(j='1990-11-11') SELECT 1")
        checkAnswer(sql("SELECT i, CAST(j AS STRING) FROM t"), Row(1, "1990-11-11"))
        checkAnswer(
          sql(
            """
              |SELECT i, CAST(j AS STRING)
              |FROM t
              |WHERE j IN (DATE'1990-11-10', DATE'1990-11-11', DATE'1990-11-12')
              |""".stripMargin),
          Row(1, "1990-11-11"))
      }
    }
  }

  test("SPARK-31522: hive metastore related configurations should be static") {
    Seq("spark.sql.hive.metastore.version",
      "spark.sql.hive.metastore.jars",
      "spark.sql.hive.metastore.sharedPrefixes",
      "spark.sql.hive.metastore.barrierPrefixes").foreach { key =>
      val e = intercept[AnalysisException](sql(s"set $key=abc"))
      assert(e.getMessage.contains("Cannot modify the value of a static config"))
    }
  }

  test("SPARK-29295: dynamic partition map parsed from partition path should be case insensitive") {
    withTable("t") {
      withSQLConf("hive.exec.dynamic.partition" -> "true",
        "hive.exec.dynamic.partition.mode" -> "nonstrict") {
        withTempDir { loc =>
          sql(s"CREATE TABLE t(c1 INT) PARTITIONED BY(P1 STRING) LOCATION '${loc.getAbsolutePath}'")
          sql("INSERT OVERWRITE TABLE t PARTITION(P1) VALUES(1, 'caseSensitive')")
          checkAnswer(sql("select * from t"), Row(1, "caseSensitive"))
        }
      }
    }
  }

  test("SPARK-32668: HiveGenericUDTF initialize UDTF should use StructObjectInspector method") {
    withUserDefinedFunction("udtf_stack1" -> true, "udtf_stack2" -> true) {
      sql(
        s"""
           |CREATE TEMPORARY FUNCTION udtf_stack1
           |AS 'org.apache.spark.sql.hive.execution.UDTFStack'
           |USING JAR '${hiveContext.getHiveFile("SPARK-21101-1.0.jar").toURI}'
        """.stripMargin)
      sql(
        s"""
           |CREATE TEMPORARY FUNCTION udtf_stack2
           |AS 'org.apache.spark.sql.hive.execution.UDTFStack2'
           |USING JAR '${hiveContext.getHiveFile("SPARK-21101-1.0.jar").toURI}'
        """.stripMargin)

      Seq("udtf_stack1", "udtf_stack2").foreach { udf =>
        checkAnswer(
          sql(s"SELECT $udf(2, 'A', 10, date '2015-01-01', 'B', 20, date '2016-01-01')"),
          Seq(Row("A", 10, Date.valueOf("2015-01-01")),
            Row("B", 20, Date.valueOf("2016-01-01"))))
      }
    }
  }

  test("SPARK-36197: Use PartitionDesc instead of TableDesc for reading hive partitioned tables") {
    withTempDir { dir =>
      val t1Loc = s"file:///$dir/t1"
      val t2Loc = s"file:///$dir/t2"
      withTable("t1", "t2") {
        hiveClient.runSqlHive(
          s"create table t1(id int) partitioned by(pid int) stored as avro location '$t1Loc'")
        hiveClient.runSqlHive("insert into t1 partition(pid=1) select 2")
        hiveClient.runSqlHive(
          s"create table t2(id int) partitioned by(pid int) stored as textfile location '$t2Loc'")
        hiveClient.runSqlHive("insert into t2 partition(pid=2) select 2")
        hiveClient.runSqlHive(s"alter table t1 add partition (pid=2) location '$t2Loc/pid=2'")
        hiveClient.runSqlHive("alter table t1 partition(pid=2) SET FILEFORMAT textfile")
        checkAnswer(sql("select pid, id from t1 order by pid"), Seq(Row(1, 2), Row(2, 2)))
      }
    }
  }

  test("SPARK-36905: read hive views without without explicit column names") {
    withTable("t1") {
      withView("test_view") {
        hiveClient.runSqlHive("create table t1 stored as avro as select 2 as id")
        hiveClient.runSqlHive("create view test_view as select 1, id + 1 from t1")
        checkAnswer(sql("select * from test_view"), Seq(Row(1, 3)))
      }
    }
  }

  test("SPARK-37196: HiveDecimal Precision Scale match failed should return null") {
    withTempDir { dir =>
      withSQLConf(HiveUtils.CONVERT_METASTORE_PARQUET.key -> "false") {
        withTable("test_precision") {
          val df = sql(s"SELECT 'dummy' AS name, ${"1" * 20}.${"2" * 18} AS value")
          df.write.mode("Overwrite").parquet(dir.getAbsolutePath)
          sql(
            s"""
               |CREATE EXTERNAL TABLE test_precision(name STRING, value DECIMAL(18,6))
               |STORED AS PARQUET LOCATION '${dir.getAbsolutePath}'
               |""".stripMargin)
          checkAnswer(sql("SELECT * FROM test_precision"), Row("dummy", null))
        }
      }
    }
  }

  test("SPARK-37217: Dynamic partitions should fail quickly " +
    "when writing to external tables to prevent data deletion") {
    withTable("test") {
      withTempDir { f =>
        sql("CREATE EXTERNAL TABLE test(id int) PARTITIONED BY (p1 string, p2 string) " +
          s"STORED AS PARQUET LOCATION '${f.getAbsolutePath}'")

        withSQLConf(HiveUtils.CONVERT_METASTORE_PARQUET.key -> "false",
          "hive.exec.dynamic.partition.mode" -> "nonstrict") {
          val insertSQL = """
              |INSERT OVERWRITE TABLE test PARTITION(p1='n1', p2)
              |SELECT * FROM VALUES (1, 'n2'), (2, 'n3'), (3, 'n4') AS t(id, p2)
              """.stripMargin
          withSQLConf("hive.exec.max.dynamic.partitions" -> "2") {
            val e = intercept[SparkException] {
              sql(insertSQL)
            }
            assert(e.getMessage.contains("Number of dynamic partitions created"))
          }

          withSQLConf("hive.exec.max.dynamic.partitions" -> "3") {
            sql(insertSQL)
          }
        }
      }
    }
  }

  test("SPARK-38215: Hive Insert Dir should use data source if it is convertible") {
    withTempView("p") {
      Seq(1, 2, 3).toDF("id").createOrReplaceTempView("p")

      Seq("orc", "parquet").foreach { format =>
        Seq(true, false).foreach { isConverted =>
          withSQLConf(
            HiveUtils.CONVERT_METASTORE_ORC.key -> s"$isConverted",
            HiveUtils.CONVERT_METASTORE_PARQUET.key -> s"$isConverted") {
            Seq(true, false).foreach { isConvertedCtas =>
              withSQLConf(HiveUtils.CONVERT_METASTORE_INSERT_DIR.key -> s"$isConvertedCtas") {
                withTempDir { dir =>
                  val df = sql(
                    s"""
                       |INSERT OVERWRITE LOCAL DIRECTORY '${dir.getAbsolutePath}'
                       |STORED AS $format
                       |SELECT 1
                  """.stripMargin)
                  val insertIntoDSDir = df.queryExecution.analyzed.collect {
                    case _: InsertIntoDataSourceDirCommand => true
                  }.headOption
                  val insertIntoHiveDir = df.queryExecution.analyzed.collect {
                    case _: InsertIntoHiveDirCommand => true
                  }.headOption
                  if (isConverted && isConvertedCtas) {
                    assert(insertIntoDSDir.nonEmpty)
                    assert(insertIntoHiveDir.isEmpty)
                  } else {
                    assert(insertIntoDSDir.isEmpty)
                    assert(insertIntoHiveDir.nonEmpty)
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  test("SPARK-38717: Handle Hive's bucket spec case preserving behaviour") {
    withTable("t") {
      sql(
        s"""
           |CREATE TABLE t(
           |  c STRING,
           |  B_C STRING
           |)
           |PARTITIONED BY (p_c STRING)
           |CLUSTERED BY (B_C) INTO 4 BUCKETS
           |STORED AS PARQUET
           |""".stripMargin)
      val df = sql("SELECT * FROM t")
      checkAnswer(df, Seq.empty[Row])
    }
  }

  test("SPARK-46388: HiveAnalysis convert InsertIntoStatement to InsertIntoHiveTable " +
    "iff child resolved") {
    withTable("t") {
      sql("CREATE TABLE t (a STRING)")
      checkError(
        exception = intercept[AnalysisException](sql("INSERT INTO t SELECT a*2 FROM t where b=1")),
        condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        sqlState = None,
        parameters = Map("objectName" -> "`b`", "proposal" -> "`a`"),
        context = ExpectedContext(
          fragment = "b",
          start = 38,
          stop = 38) )
      checkError(
        exception = intercept[AnalysisException](
          sql("INSERT INTO t SELECT cast(a as short) FROM t where b=1")),
        condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        sqlState = None,
        parameters = Map("objectName" -> "`b`", "proposal" -> "`a`"),
        context = ExpectedContext(
          fragment = "b",
          start = 51,
          stop = 51))
    }
  }
}

@SlowHiveTest
class SQLQuerySuite extends SQLQuerySuiteBase with DisableAdaptiveExecutionSuite {
  import spark.implicits._

  test("SPARK-36421: Validate all SQL configs to prevent from wrong use for ConfigEntry") {
    val df = spark.sql("set -v").select("Meaning")
    assert(df.collect().forall(!_.getString(0).contains("ConfigEntry")))
  }

  test("SPARK-25271: Hive ctas commands should use data source if it is convertible") {
    withTempView("p") {
      Seq(1, 2, 3).toDF("id").createOrReplaceTempView("p")

      Seq("orc", "parquet").foreach { format =>
        Seq(true, false).foreach { isConverted =>
          withSQLConf(
            HiveUtils.CONVERT_METASTORE_ORC.key -> s"$isConverted",
            HiveUtils.CONVERT_METASTORE_PARQUET.key -> s"$isConverted") {
            Seq(true, false).foreach { isConvertedCtas =>
              withSQLConf(HiveUtils.CONVERT_METASTORE_CTAS.key -> s"$isConvertedCtas") {

                val targetTable = "targetTable"
                withTable(targetTable) {
                  var commands: Seq[SparkPlanInfo] = Seq.empty
                  val listener = new SparkListener {
                    override def onOtherEvent(event: SparkListenerEvent): Unit = {
                      event match {
                        case start: SparkListenerSQLExecutionStart =>
                          commands = commands ++ Seq(start.sparkPlanInfo)
                        case _ => // ignore other events
                      }
                    }
                  }
                  spark.sparkContext.addSparkListener(listener)
                  try {
                    sql(s"CREATE TABLE $targetTable STORED AS $format AS SELECT id FROM p")
                    checkAnswer(sql(s"SELECT id FROM $targetTable"),
                      Row(1) :: Row(2) :: Row(3) :: Nil)
                    spark.sparkContext.listenerBus.waitUntilEmpty()
                    assert(commands.size == 3)
                    assert(commands.head.nodeName == "Execute CreateHiveTableAsSelectCommand")

                    val v1WriteCommand = commands(1)
                    if (isConverted && isConvertedCtas) {
                      assert(v1WriteCommand.nodeName == "Execute InsertIntoHadoopFsRelationCommand")
                    } else {
                      assert(v1WriteCommand.nodeName == "Execute InsertIntoHiveTable")
                    }
                  } finally {
                    spark.sparkContext.removeSparkListener(listener)
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
@SlowHiveTest
class SQLQuerySuiteAE extends SQLQuerySuiteBase with EnableAdaptiveExecutionSuite

