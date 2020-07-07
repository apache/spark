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

package org.apache.spark.sql.execution.command

import java.io.File

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

class TemporaryTableSuite extends QueryTest with SharedSparkSession {

  override def afterEach(): Unit = {
    try {
      // drop all databases, tables and functions after each test
      spark.sessionState.catalog.reset()
    } finally {
      Utils.deleteRecursively(new File(spark.sessionState.conf.warehousePath))
      super.afterEach()
    }
  }

  test("create temporary table using data source") {
    withTempTable("tt1", "sameName", "`bad.name`") {
      sql("create temporary table tt1 (id int) using parquet")
      sql("insert into table tt1 values (1)")
      assert(spark.table("tt1").collect === Array(Row(1)))

      withTempView("same") {
        sql("create temporary view same as select 1")
        val e = intercept[AnalysisException] (
          sql("create temporary table same (id int) using parquet"))
        assert(e.message.contains("Temporary view 'same' already exists"))
      }

      withTempView("same") {
        sql("create temporary table same using parquet as select 1")
        val e = intercept[AnalysisException](
          sql("create temporary view same (id int) using parquet"))
        assert(e.message.contains("Temporary table 'default.same' already exists"))
      }

      val e = intercept[AnalysisException](
        sql("create temporary table `bad.name` (id int) using parquet"))
      assert(e.message.contains("`bad.name` is not a valid name for tables/databases."))
    }
  }

  test("create temporary table using data source as select") {
    withTempTable("tt1", "sameName", "`bad.name`") {
      sql("create temporary table tt1 using parquet as select 1")
      assert(spark.table("tt1").collect === Array(Row(1)))

      withTempView("same") {
        sql("create temporary view same as select 1")
        val e = intercept[AnalysisException] (
          sql("create temporary table same using parquet as select 1"))
        assert(e.message.contains("Temporary view 'same' already exists"))
      }

      withTempView("same") {
        sql("create temporary table same using parquet as select 1")
        val e = intercept[AnalysisException](
          sql("create temporary view same as select 1"))
        assert(e.message.contains("Temporary table 'default.same' already exists"))
      }

      val e = intercept[AnalysisException](
        sql("create temporary table `bad.name` using parquet as select 1"))
      assert(e.message.contains("`bad.name` is not a valid name for tables/databases."))
    }
  }

  test("create temporary table with location is not allowed") {
    withTempTable("tt1") {
      val e = intercept[AnalysisException](
        sql(
          """
            |create temporary table tt1 (id int) using parquet
            |LOCATION '/PATH'
            |""".stripMargin))
      assert(e.message.contains("specify LOCATION in temporary table"))
    }
  }

  test("create a same name temporary table and permanent table") {
    withDatabase("db1") {
      sql("create database db1")
      withTable("same1", "same2", "same3", "same4", "same5", "same6") {
        withTempTable("same1", "same2", "same3", "same4", "same5", "same6") {
          // create temporary table then create table
          sql("create temporary table same using parquet as select 'temp_table'")
          var e = intercept[AnalysisException](
            sql("create table same using parquet as select 'table'")
          ).getMessage
          assert(e.contains("already exists"))

          sql("create temporary table same2 (key int) using parquet")
          e = intercept[AnalysisException](
            sql("create table same2 (key int) using parquet")
          ).getMessage
          assert(e.contains("already exists"))

          sql("create temporary table db1.same3 (key int) using parquet")
          sql("create table same3 (key int) using parquet")
          e = intercept[AnalysisException](
            sql("create table db1.same3 (key int) using parquet")
          ).getMessage
          assert(e.contains("already exists"))

          // create table then create temporary table
          sql("create table same4 using parquet as select 'table'")
          e = intercept[AnalysisException](
            sql("create temporary table same4 using parquet as select 'temp_table'")
          ).getMessage
          assert(e.contains("already exists"))

          sql("create table same5 (key int) using parquet")
          e = intercept[AnalysisException](
            sql("create temporary table same5 (key int) using parquet")
          ).getMessage
          assert(e.contains("already exists"))

          sql("create table db1.same6 (key int) using parquet")
          sql("create temporary table same6 (key int) using parquet")
          e = intercept[AnalysisException](
            sql("create temporary table db1.same6 (key int) using parquet")
          ).getMessage
          assert(e.contains("already exists"))
        }
      }
    }
  }

  test("drop temporary table") {
    withDatabase("db1") {
      withTempTable("t1", "db1.t1") {
        sql("create temporary table t1 using parquet as select 1")
        assert(spark.table("t1").collect === Array(Row(1)))
        sql("drop table t1")
        assert(spark.sessionState.catalog.getTempTable("t1").isEmpty)

        sql("CREATE DATABASE db1")
        sql("create temporary table db1.t1 using parquet as select 1")
        assert(spark.table("db1.t1").collect === Array(Row(1)))
        sql("drop table db1.t1")
        assert(spark.sessionState.catalog.getTempTable("db1.t1").isEmpty)
      }
    }
  }


  test("create temporary partition table is not allowed") {
    withTempTable("t1") {
      val e = intercept[ParseException] {
        sql("create temporary table t1 (a int, b int) " +
          "using parquet partitioned by (b)")
      }.getMessage
      assert(e.contains("Operation not allowed"))
    }
  }

  test("insert into temporary table") {
    withDatabase("dba") {
      sql("CREATE DATABASE dba")
      withTempTable("dba.tt1", "tt1", "tt2") {
        sql("CREATE TEMPORARY TABLE dba.tt1 (key int) USING PARQUET")
        sql("CREATE TEMPORARY TABLE tt1 USING PARQUET AS SELECT 1")
        checkAnswer(spark.table("dba.tt1"), Seq())
        checkAnswer(spark.table("tt1"), Seq(Row(1)))

        sql("INSERT INTO TABLE dba.tt1 SELECT * FROM tt1")
        sql("INSERT OVERWRITE TABLE tt1 VALUES (2)")
        checkAnswer(spark.table("dba.tt1"), Seq(Row(1)))
        checkAnswer(spark.table("tt1"), Seq(Row(2)))
      }
    }
  }

  test("show tables with temporary tables") {
    withDatabase("db1") {
      withTempTable("tt1", "tt2", "db1.tt1", "db1.tt2") {
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

  private def withStaticSQLConf(pairs: (String, String)*)(f: => Unit): Unit = {
    pairs.foreach { case (k, v) =>
      SQLConf.get.setConfString(k, v)
    }
    try f finally {
      pairs.foreach { case (k, _) =>
        SQLConf.get.unsetConf(k)
      }
    }
  }

  test("check temporary table limitation") {
    withTempView("tt1", "tt2", "tt3") {
      withStaticSQLConf(StaticSQLConf.TEMPORARY_TABLE_MAX_SIZE_PER_SESSION.key -> "0") {
        sql("CREATE TEMPORARY TABLE tt1 (key int) USING PARQUET")
        val e = intercept[AnalysisException] {
          sql("CREATE TEMPORARY TABLE tt2 USING PARQUET AS SELECT 1")
        }.getMessage
        assert(e.contains("The total size of temporary tables in this session has reached"))
      }
      withStaticSQLConf(StaticSQLConf.TEMPORARY_TABLE_MAX_SIZE_PER_SESSION.key -> "1000") {
        sql("CREATE TEMPORARY TABLE tt2 USING PARQUET AS SELECT 1")
      }
      withStaticSQLConf(StaticSQLConf.TEMPORARY_TABLE_MAX_NUM_PER_SESSION.key -> "2") {
        val e = intercept[AnalysisException] {
          sql("CREATE TEMPORARY TABLE tt3 USING PARQUET AS SELECT 1")
        }.getMessage
        assert(e.contains("The total number of temporary tables in this session has reached"))
      }
      withStaticSQLConf(StaticSQLConf.TEMPORARY_TABLE_MAX_NUM_PER_SESSION.key -> "3") {
        sql("CREATE TEMPORARY TABLE tt3 USING PARQUET AS SELECT 1")
      }
    }
  }
}
