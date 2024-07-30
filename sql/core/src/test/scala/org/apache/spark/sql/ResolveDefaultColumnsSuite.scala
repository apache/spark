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

import org.apache.spark.SparkRuntimeException
import org.apache.spark.sql.test.SharedSparkSession

class ResolveDefaultColumnsSuite extends QueryTest with SharedSparkSession {
  test("column without default value defined (null as default)") {
    withTable("t") {
      sql("create table t(c1 timestamp, c2 timestamp) using parquet")

      // INSERT with user-defined columns
      sql("insert into t (c2) values (timestamp'2020-12-31')")
      checkAnswer(spark.table("t"),
        sql("select null, timestamp'2020-12-31'").collect().head)
      sql("truncate table t")
      sql("insert into t (c1) values (timestamp'2020-12-31')")
      checkAnswer(spark.table("t"),
        sql("select timestamp'2020-12-31', null").collect().head)

      // INSERT without user-defined columns
      sql("truncate table t")
      checkError(
        exception = intercept[AnalysisException] {
          sql("insert into t values (timestamp'2020-12-31')")
        },
        errorClass = "INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS",
        parameters = Map(
          "tableName" -> "`spark_catalog`.`default`.`t`",
          "tableColumns" -> "`c1`, `c2`",
          "dataColumns" -> "`col1`"))
    }
  }

  test("column with default value defined") {
    withTable("t") {
      sql("create table t(c1 timestamp DEFAULT timestamp'2020-01-01', " +
        "c2 timestamp DEFAULT timestamp'2020-01-01') using parquet")

      // INSERT with user-defined columns
      sql("insert into t (c1) values (timestamp'2020-12-31')")
      checkAnswer(spark.table("t"),
        sql("select timestamp'2020-12-31', timestamp'2020-01-01'").collect().head)
      sql("truncate table t")
      sql("insert into t (c2) values (timestamp'2020-12-31')")
      checkAnswer(spark.table("t"),
        sql("select timestamp'2020-01-01', timestamp'2020-12-31'").collect().head)

      // INSERT without user-defined columns
      sql("truncate table t")
      checkError(
        exception = intercept[AnalysisException] {
          sql("insert into t values (timestamp'2020-12-31')")
        },
        errorClass = "INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS",
        parameters = Map(
          "tableName" -> "`spark_catalog`.`default`.`t`",
          "tableColumns" -> "`c1`, `c2`",
          "dataColumns" -> "`col1`"))
    }
  }

  test("INSERT into partitioned tables") {
    withTable("t") {
      sql("create table t(c1 int, c2 int, c3 int, c4 int) using parquet partitioned by (c3, c4)")

      // INSERT without static partitions
      checkError(
        exception = intercept[AnalysisException] {
          sql("insert into t values (1, 2, 3)")
        },
        errorClass = "INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS",
        parameters = Map(
          "tableName" -> "`spark_catalog`.`default`.`t`",
          "tableColumns" -> "`c1`, `c2`, `c3`, `c4`",
          "dataColumns" -> "`col1`, `col2`, `col3`"))

      // INSERT without static partitions but with column list
      sql("truncate table t")
      sql("insert into t (c2, c1, c4) values (1, 2, 3)")
      checkAnswer(spark.table("t"), Row(2, 1, null, 3))

      // INSERT with static partitions
      sql("truncate table t")
      checkError(
        exception = intercept[AnalysisException] {
          sql("insert into t partition(c3=3, c4=4) values (1)")
        },
        errorClass = "INSERT_PARTITION_COLUMN_ARITY_MISMATCH",
        parameters = Map(
          "tableName" -> "`spark_catalog`.`default`.`t`",
          "tableColumns" -> "`c1`, `c2`, `c3`, `c4`",
          "dataColumns" -> "`col1`",
          "staticPartCols" -> "`c3`, `c4`"))

      // INSERT with static partitions and with column list
      sql("truncate table t")
      sql("insert into t partition(c3=3, c4=4) (c2) values (1)")
      checkAnswer(spark.table("t"), Row(null, 1, 3, 4))

      // INSERT with partial static partitions
      sql("truncate table t")
      checkError(
        exception = intercept[AnalysisException] {
          sql("insert into t partition(c3=3, c4) values (1, 2)")
        },
        errorClass = "INSERT_PARTITION_COLUMN_ARITY_MISMATCH",
        parameters = Map(
          "tableName" -> "`spark_catalog`.`default`.`t`",
          "tableColumns" -> "`c1`, `c2`, `c3`, `c4`",
          "dataColumns" -> "`col1`, `col2`",
          "staticPartCols" -> "`c3`"))

      // INSERT with partial static partitions and with column list is not allowed
      intercept[AnalysisException](sql("insert into t partition(c3=3, c4) (c1) values (1, 4)"))
    }
  }

  test("SPARK-43085: Column DEFAULT assignment for target tables with multi-part names") {
    withDatabase("demos") {
      sql("create database demos")
      withTable("demos.test_ts") {
        sql("create table demos.test_ts (id int, ts timestamp) using parquet")
        sql("insert into demos.test_ts(ts) values (timestamp'2023-01-01')")
        checkAnswer(spark.table("demos.test_ts"),
          sql("select null, timestamp'2023-01-01'"))
      }
      withTable("demos.test_ts") {
        sql("create table demos.test_ts (id int, ts timestamp) using parquet")
        sql("use database demos")
        sql("insert into test_ts(ts) values (timestamp'2023-01-01')")
        checkAnswer(spark.table("demos.test_ts"),
          sql("select null, timestamp'2023-01-01'"))
      }
    }
  }

  test("SPARK-43313: Column default values with implicit coercion from provided values") {
    withDatabase("demos") {
      sql("create database demos")
      withTable("demos.test_ts") {
        // If the provided default value is a literal of a wider type than the target column, but
        // the literal value fits within the narrower type, just coerce it for convenience.
        sql(
          """create table demos.test_ts (
            |a int default 42L,
            |b timestamp_ntz default '2022-01-02',
            |c date default '2022-01-03',
            |f float default 0D
            |) using parquet""".stripMargin)
        sql("insert into demos.test_ts(a) values (default)")
        checkAnswer(spark.table("demos.test_ts"),
          sql("select 42, timestamp_ntz'2022-01-02', date'2022-01-03', 0f"))
        // If the provided default value is a literal of a different type than the target column
        // such that no coercion is possible, throw an error.
        checkError(
          exception = intercept[AnalysisException] {
            sql("create table demos.test_ts_other (a int default 'abc') using parquet")
          },
          errorClass = "INVALID_DEFAULT_VALUE.DATA_TYPE",
          parameters = Map(
            "statement" -> "CREATE TABLE",
            "colName" -> "`a`",
            "expectedType" -> "\"INT\"",
            "defaultValue" -> "'abc'",
            "actualType" -> "\"STRING\""))
        checkError(
          exception = intercept[AnalysisException] {
            sql("create table demos.test_ts_other (a timestamp default 'invalid') using parquet")
          },
          errorClass = "INVALID_DEFAULT_VALUE.DATA_TYPE",
          parameters = Map(
            "statement" -> "CREATE TABLE",
            "colName" -> "`a`",
            "expectedType" -> "\"TIMESTAMP\"",
            "defaultValue" -> "'invalid'",
            "actualType" -> "\"STRING\""))
        checkError(
          exception = intercept[AnalysisException] {
            sql("create table demos.test_ts_other (a boolean default 'true') using parquet")
          },
          errorClass = "INVALID_DEFAULT_VALUE.DATA_TYPE",
          parameters = Map(
            "statement" -> "CREATE TABLE",
            "colName" -> "`a`",
            "expectedType" -> "\"BOOLEAN\"",
            "defaultValue" -> "'true'",
            "actualType" -> "\"STRING\""))
        checkError(
          exception = intercept[AnalysisException] {
            sql("create table demos.test_ts_other (a int default true) using parquet")
          },
          errorClass = "INVALID_DEFAULT_VALUE.DATA_TYPE",
          parameters = Map(
            "statement" -> "CREATE TABLE",
            "colName" -> "`a`",
            "expectedType" -> "\"INT\"",
            "defaultValue" -> "true",
            "actualType" -> "\"BOOLEAN\""))
      }
    }
  }

  test("SPARK-46949: DDL with valid default char/varchar values") {
    withTable("t") {
      val ddl =
        s"""
           |CREATE TABLE t(
           |  key int,
           |  v VARCHAR(6) DEFAULT 'apache',
           |  c CHAR(5) DEFAULT 'spark')
           |USING parquet""".stripMargin
      sql(ddl)
      sql("INSERT INTO t (key) VALUES(1)")
      checkAnswer(sql("select * from t"), Row(1, "apache", "spark"))
    }
  }

  test("SPARK-46949: DDL with invalid default char/varchar values") {
    Seq("CHAR", "VARCHAR").foreach { typeName =>
      checkError(
        exception = intercept[SparkRuntimeException](
          sql(s"CREATE TABLE t(c $typeName(3) DEFAULT 'spark') USING parquet")),
        errorClass = "EXCEED_LIMIT_LENGTH",
        parameters = Map("limit" -> "3"))
    }
  }

  test("SPARK-46949: DDL with default char/varchar values need padding") {
    withTable("t") {
      val ddl =
        s"""
           |CREATE TABLE t(
           |  key int,
           |  v VARCHAR(6) DEFAULT 'apache',
           |  c CHAR(6) DEFAULT 'spark')
           |USING parquet""".stripMargin
      sql(ddl)
      sql("INSERT INTO t (key) VALUES(1)")
      checkAnswer(sql("select * from t"), Row(1, "apache", "spark "))
    }
  }

  test("SPARK-46958: timestamp should have timezone for resolvable if default values castable") {
    val defaults = Seq("timestamp '2018-11-17'", "CAST(timestamp '2018-11-17' AS STRING)")
    defaults.foreach { default =>
      withTable("t") {
        sql(s"CREATE TABLE t(key int, c STRING DEFAULT $default) " +
          s"USING parquet")
        sql("INSERT INTO t (key) VALUES(1)")
        checkAnswer(sql("select * from t"), Row(1, "2018-11-17 00:00:00"))
      }
    }
  }

  test("SPARK-46958: timestamp should have timezone for resolvable when default values fit") {
    withTable("t") {
      // If the provided default value is a literal of a wider type than the target column, but
      // the literal value fits within the narrower type, just coerce it for convenience.
      sql(s"CREATE TABLE t(key int, c timestamp DEFAULT '2018-11-17 13:33:33') " +
        s"USING parquet")
      sql("INSERT INTO t (key) VALUES(1)")
      checkAnswer(sql("select CAST(c as STRING) from t"), Row("2018-11-17 13:33:33"))
    }
  }

  test("SPARK-48033: default columns using runtime replaceable expression works") {
    withTable("t") {
      sql("CREATE TABLE t(v VARIANT DEFAULT parse_json('1')) USING PARQUET")
      sql("INSERT INTO t VALUES(DEFAULT)")
      checkAnswer(sql("select v from t"), sql("select parse_json('1')").collect())
    }
  }

  test("SPARK-49054: Create table with current_user() default") {
    val tableName = "test_current_user"
    val user = spark.sparkContext.sparkUser
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName(i int, s string default current_user()) USING parquet")
      sql(s"INSERT INTO $tableName (i) VALUES ((0))")
      checkAnswer(sql(s"SELECT * FROM $tableName"), Seq(Row(0, user)))
    }
  }

  test("SPARK-49054: Alter table with current_user() default") {
    val tableName = "test_current_user"
    val user = spark.sparkContext.sparkUser
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName(i int, s string) USING parquet")
      sql(s"ALTER TABLE $tableName ALTER COLUMN s SET DEFAULT current_user()")
      sql(s"INSERT INTO $tableName (i) VALUES ((0))")
      checkAnswer(sql(s"SELECT * FROM $tableName"), Seq(Row(0, user)))
    }
  }
}
