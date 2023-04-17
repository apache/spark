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

package org.apache.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.test.SharedSparkSession

class TemporaryTableSuite extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  private val sparkScratch = "/tmp/spark-scratch"

  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(StaticSQLConf.SCRATCH_DIR, sparkScratch)

  override def afterEach(): Unit = {
    try {
      spark.sessionState.catalog.dropAllTempTables()
    } finally {
      super.afterEach()
    }
  }

  test("Scratch dir is set and temporary tables are supported") {
    assert(spark.sessionState.catalog.sessionDir.contains(sparkScratch))
    assert(spark.sessionState.conf.enableTemporayTable)
  }

  test("create temporary table across different sessions") {
    val sparkSession1 = spark.newSession()
    val sparkSession2 = spark.newSession()

    val t1 = TableIdentifier("t1")

    sparkSession1.sql("CREATE TEMPORARY TABLE t1 AS SELECT * FROM range(2)")
    sparkSession2.sql("CREATE TEMPORARY TABLE t1 AS SELECT * FROM range(3)")

    assert(sparkSession1.sessionState.catalog.getTableMetadata(t1).isTemporary)
    assert(sparkSession1.sessionState.catalog.getTableMetadata(t1).tableType ===
      CatalogTableType.TEMPORARY)
    assert(sparkSession1.sessionState.catalog.getTableMetadata(t1).location.toString
      .contains(sparkScratch))
    assert(sparkSession2.sessionState.catalog.getTableMetadata(t1).isTemporary)
    assert(sparkSession2.sessionState.catalog.getTableMetadata(t1).tableType ===
      CatalogTableType.TEMPORARY)
    assert(sparkSession2.sessionState.catalog.getTableMetadata(t1).location.toString
      .contains(sparkScratch))

    checkAnswer(sparkSession1.table("t1"), Seq(Row(0), Row(1)))
    checkAnswer(sparkSession2.table("t1"), Seq(Row(0), Row(1), Row(2)))
  }

  test("drop temporary table") {
    withDatabase("db1") {
      withTable("t1", "db1.t1") {
        sql("create temporary table t1 using parquet as select 1")
        checkAnswer(spark.table("t1"), Seq(Row(1)))
        assert(spark.sessionState.catalog.tempTableExists(TableIdentifier("t1")))
        assert(spark.sessionState.catalog.tableExists(TableIdentifier("t1")))
        sql("drop table t1")
        assert(!spark.sessionState.catalog.tempTableExists(TableIdentifier("t1")))
        assert(!spark.sessionState.catalog.tableExists(TableIdentifier("t1")))

        sql("CREATE DATABASE db1")
        sql("create temporary table db1.t1 using parquet as select 1")
        checkAnswer(spark.table("db1.t1"), Seq(Row(1)))
        assert(spark.sessionState.catalog.tempTableExists(TableIdentifier("t1", Some("db1"))))
        assert(spark.sessionState.catalog.tableExists(TableIdentifier("t1", Some("db1"))))
        sql("drop table db1.t1")
        assert(!spark.sessionState.catalog.tempTableExists(TableIdentifier("t1", Some("db1"))))
        assert(!spark.sessionState.catalog.tempTableExists(TableIdentifier("t1", Some("db1"))))
        assert(!spark.sessionState.catalog.tableExists(TableIdentifier("t1", Some("db1"))))
      }
    }
  }

  test("create temp table in database") {
    withDatabase("db1") {
      spark.sql("create database db1")
      val sparkSession = spark.newSession()
      sparkSession.sql("create temporary table db1.t1 as select 1 as id")
      checkAnswer(sparkSession.table("db1.t1"), Seq(Row(1)))
      sparkSession.sql("use db1")
      checkAnswer(sparkSession.table("t1"), Seq(Row(1)))
    }
  }

  test("analyze temporary table") {
    spark.sql("CREATE TEMPORARY TABLE t1 AS SELECT * FROM range(100)")
    spark.sql("ANALYZE TABLE t1 COMPUTE STATISTICS FOR ALL COLUMNS")

    val t1 = TableIdentifier("t1")

    val stats = spark.sessionState.catalog.getTableMetadata(t1).stats
    assert(stats.nonEmpty)
    assert(stats.get.rowCount.contains(100))
    assert(stats.get.colStats.contains("id"))
  }

  test("create temporary table and insert values") {
    spark.sql("CREATE TEMPORARY TABLE t1(id bigint)")
    spark.sql("INSERT INTO t1 SELECT * FROM range(2)")
    spark.sql("INSERT INTO t1 VALUES (2), (3)")
    spark.sql("INSERT INTO t1(id) VALUES (4), (5)")

    checkAnswer(spark.table("t1"), Seq(Row(0), Row(1), Row(2), Row(3), Row(4), Row(5)))
  }

  test("temporary table and temporary view") {
    withTempView("t1") {
      spark.sql("CREATE TEMPORARY VIEW t1 AS SELECT * FROM range(2)")
      val e1 = intercept[TableAlreadyExistsException] {
        spark.sql("CREATE TEMPORARY TABLE t1 AS SELECT * FROM range(3)")
      }
      assert(e1.getMessage.contains("TABLE_OR_VIEW_ALREADY_EXISTS"))
    }
  }

  test("temporary table and non-temporary table") {
    spark.sql("CREATE TABLE t1 using parquet AS SELECT * FROM range(2)")
    spark.sql("CREATE TEMPORARY TABLE t1 using parquet AS SELECT * FROM range(3)")
    checkAnswer(spark.table("t1"), Seq(Row(0), Row(1), Row(2)))
    spark.sql("DROP TABLE t1") // drop temporary table
    checkAnswer(spark.table("t1"), Seq(Row(0), Row(1)))
    spark.sql("DROP TABLE t1") // drop non-temporary table
    assert(!spark.sessionState.catalog.tableExists(TableIdentifier("t1")))
  }

  test("show create temporary table") {
    spark.sql("CREATE TEMPORARY TABLE t1 using ORC AS SELECT id FROM range(5)")
    checkKeywordsExist(spark.sql("SHOW CREATE TABLE t1"), "CREATE TEMPORARY TABLE default.t1")
  }

  test("create temporary table like") {
    withTable("t3") {
      spark.sql("CREATE TEMPORARY TABLE t1(id int)")
      spark.sql("CREATE TEMPORARY TABLE t2 LIKE t1")
      spark.sql("CREATE TABLE t3 LIKE t2 USING parquet")
      assert(spark.sessionState.catalog.tempTableExists(TableIdentifier("t1")))
      assert(spark.sessionState.catalog.tempTableExists(TableIdentifier("t2")))
      assert(!spark.sessionState.catalog.tempTableExists(TableIdentifier("t3")))
    }
  }

  test("show tables with temporary tables") {
    withDatabase("db1") {
      withTable("tt1", "tt2", "db1.tt1", "db1.tt2") {
        sql("create database db1")
        sql("create temporary table tt1 using parquet as select 1")
        sql("create table tt2 using parquet as select 1")
        sql("create temporary table db1.tt1 using parquet as select 1")
        sql("create table db1.tt2 using parquet as select 1")

        checkAnswer(sql("show tables in db1"),
          Seq(Row("db1", "tt1", true), Row("db1", "tt2", false)))
        checkAnswer(sql("show tables"),
          Seq(Row("default", "tt1", true), Row("default", "tt2", false)))
      }
    }
  }

  test("create temporary table without using should use parquet by default") {
    def checkTemporaryTableProvider(name: String, provider: String): Unit = {
      assert(spark.sessionState.catalog.getTableMetadata(TableIdentifier(name)).provider ===
        Some(provider))
      assert(spark.sessionState.catalog.getTableMetadata(TableIdentifier(name)).tableType
        === CatalogTableType.TEMPORARY)
    }

    withTable("t1", "t2", "t3", "t4") {
      spark.sql("CREATE TEMPORARY TABLE t1 AS SELECT 1")
      checkTemporaryTableProvider("t1", "parquet")
      spark.sql("CREATE TEMPORARY TABLE t2 (id int)")
      checkTemporaryTableProvider("t2", "parquet")
      spark.sql("CREATE TEMPORARY TABLE t3 LIKE t2")
      checkTemporaryTableProvider("t3", "parquet")
      spark.sql("CREATE TEMPORARY TABLE t4 using ORC AS SELECT 1")
      checkTemporaryTableProvider("t4", "ORC")
    }
  }

  test("Create temporary table using data source") {
    withTable("tt1", "sameName", "`bad.name`") {
      sql("create temporary table tt1 (id int) using parquet")
      sql("insert into table tt1 values (1)")
      assert(spark.table("tt1").collect === Array(Row(1)))

      withTempView("same") {
        sql("create temporary view same as select 1")
        val e = intercept[AnalysisException](
          sql("create temporary table same (id int) using parquet"))
        assert(e.message.contains("`spark_catalog`.`default`.`same` because it already exists"))
      }

      withTempView("same") {
        sql("create temporary table same using parquet as select 1")
        val e = intercept[AnalysisException](
          sql("create temporary view same (id int) using parquet"))
        assert(e.message.contains("the temporary view `same` because it already exists"))
      }

      val e = intercept[AnalysisException](
        sql("create temporary table `bad.name` (id int) using parquet"))
      assert(e.message.contains("`bad.name` is not a valid name for tables/databases."))
    }
  }

  test("Create temporary table using data source as select") {
    withTable("tt1", "sameName", "`bad.name`") {
      sql("create temporary table tt1 using parquet as select 1")
      assert(spark.table("tt1").collect === Array(Row(1)))

      withTempView("same") {
        sql("create temporary view same as select 1")
        val e = intercept[AnalysisException](
          sql("create temporary table same using parquet as select 1"))
        assert(e.message.contains("`spark_catalog`.`default`.`same` because it already exists"))
      }

      withTempView("same") {
        sql("create temporary table same using parquet as select 1")
        val e = intercept[AnalysisException](
          sql("create temporary view same as select 1"))
        assert(e.message.contains("temporary view `same` because it already exists"))
      }

      val e = intercept[AnalysisException](
        sql("create temporary table `bad.name` using parquet as select 1"))
      assert(e.message.contains("`bad.name` is not a valid name for tables/databases."))
    }
  }

  test("Create a same name temporary table and permanent table") {
    withDatabase("db1") {
      sql("create database db1")
      withTable("same1", "same2", "same3", "same4", "same5", "same6") {
        withTable("same1", "same2", "same3", "same4", "same5", "same6") {
          sql("create temporary table same1 using parquet as select 'temp_table'")
          sql("create table same1 using parquet as select 'table'")

          sql("create temporary table same2 (key int) using parquet")
          sql("create table same2 (key int) using parquet")

          sql("create temporary table db1.same3 (key int) using parquet")
          sql("create table same3 (key int) using parquet")
          sql("create table db1.same3 (key int) using parquet")

          // create table then create temporary table
          sql("create table same4 using parquet as select 'table'")
          sql("create temporary table same4 using parquet as select 'temp_table'")

          sql("create table same5 (key int) using parquet")
          sql("create temporary table same5 (key int) using parquet")

          sql("create table db1.same6 (key int) using parquet")
          sql("create temporary table same6 (key int) using parquet")
          sql("create temporary table db1.same6 (key int) using parquet")
        }
      }
    }
  }

  // test unsupported command
  test("Create temporary table with location is not allowed") {
    withTable("tt1") {
      val e = intercept[AnalysisException](
        sql(
          """
            |create temporary table tt1 (id int) using parquet
            |LOCATION '/PATH'
            |""".stripMargin))
      assert(e.message.contains("specify LOCATION on temporary table"))
    }
  }

  test("Create temporary partitioned table") {
    val e1 = intercept[ParseException] {
      spark.newSession().sql(
        """
          |CREATE temporary TABLE t1 using parquet partitioned BY (part) AS
          |SELECT id,
          |       id % 2 AS part
          |FROM   range(10)
          |""".stripMargin)
    }
    assert(e1.getMessage.contains("Operation not allowed"))

    withTable("t1") {
      withTable("tt1") {
        sql("CREATE TABLE t1 (id int, dt int) USING parquet PARTITIONED BY (dt)")
        val e2 = intercept[AnalysisException] {
          sql("CREATE TEMPORARY TABLE tt1 LIKE t1 USING PARQUET")
        }
        assert(e2.getMessage.contains("The feature is not supported"))
      }
    }
  }

  test("Unsupported commands") {
    def testUnsupportedCommand(sqlStr: String): Unit = {
      val e = intercept[AnalysisException] {
        spark.sql(sqlStr)
      }
      assert(e.getMessage.contains("The feature is not supported"))
    }

    spark.sql("CREATE TEMPORARY TABLE t1 using parquet AS SELECT * FROM range(3)")

    testUnsupportedCommand("ALTER TABLE t1 SET TBLPROPERTIES ('key1' = 'val1')")
    testUnsupportedCommand("ALTER TABLE t1 UNSET TBLPROPERTIES IF EXISTS ('key1')")
    testUnsupportedCommand("ALTER TABLE t1 RENAME COLUMN id TO user_id")
    testUnsupportedCommand("ALTER TABLE t1 ADD COLUMNS (name string)")
    testUnsupportedCommand("ALTER TABLE t1 SET serdeproperties ('test'='test')")
    testUnsupportedCommand("ALTER TABLE t1 SET LOCATION '/tmp/spark'")
    testUnsupportedCommand("ALTER TABLE t1 RENAME TO t2")
    testUnsupportedCommand("ALTER TABLE t1 ADD PARTITION (part = '1')")
    testUnsupportedCommand("ALTER TABLE t1 DROP PARTITION (a='1', b='2')")
    testUnsupportedCommand("ALTER TABLE t1 PARTITION (a='1') RENAME TO PARTITION (a='100')")
    testUnsupportedCommand("ALTER TABLE t1 CHANGE COLUMN id TYPE bigint")
    testUnsupportedCommand("ALTER TABLE t1 SET SERDE 'whatever'")
  }
}
