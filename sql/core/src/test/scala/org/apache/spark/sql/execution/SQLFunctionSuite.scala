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

package org.apache.spark.sql.execution

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Test suite for SQL user-defined functions (UDFs).
 */
class SQLFunctionSuite extends SharedSparkSession {
  import testImplicits._

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    Seq((0, 1), (1, 2)).toDF("a", "b").createOrReplaceTempView("t")
  }

  test("SQL scalar function") {
    withUserDefinedFunction("area" -> false) {
      sql(
        """
          |CREATE FUNCTION area(width DOUBLE, height DOUBLE)
          |RETURNS DOUBLE
          |RETURN width * height
          |""".stripMargin)
      checkAnswer(sql("SELECT area(1, 2)"), Row(2))
      checkAnswer(sql("SELECT area(a, b) FROM t"), Seq(Row(0), Row(2)))
    }
  }

  test("SQL scalar function with subquery in the function body") {
    withUserDefinedFunction("foo" -> false) {
      withTable("tbl") {
        sql("CREATE TABLE tbl AS SELECT * FROM VALUES (1, 2), (1, 3), (2, 3) t(a, b)")
        sql(
          """
            |CREATE FUNCTION foo(x INT) RETURNS INT
            |RETURN SELECT SUM(b) FROM tbl WHERE x = a;
            |""".stripMargin)
        checkAnswer(sql("SELECT foo(1)"), Row(5))
        checkAnswer(sql("SELECT foo(a) FROM t"), Seq(Row(null), Row(5)))
      }
    }
  }

  test("SQL table function") {
    withUserDefinedFunction("foo" -> false) {
      sql(
        """
          |CREATE FUNCTION foo(x INT)
          |RETURNS TABLE(a INT)
          |RETURN SELECT x + 1 AS x1
          |""".stripMargin)
      checkAnswer(sql("SELECT * FROM foo(1)"), Row(2))
      checkAnswer(sql(
        """
          |SELECT t2.a FROM VALUES (1, 2), (3, 4) t1(a, b), LATERAL foo(a) t2
          |""".stripMargin), Seq(Row(2), Row(4)))
    }
  }

  test("SQL scalar function with default value") {
    withUserDefinedFunction("bar" -> false) {
      sql(
        """
          |CREATE FUNCTION bar(x INT DEFAULT 7)
          |RETURNS INT
          |RETURN x + 1
          |""".stripMargin)
      checkAnswer(sql("SELECT bar()"), Row(8))
      checkAnswer(sql("SELECT bar(1)"), Row(2))
    }
  }


  test("SQL UDF in higher-order function should fail with clear error message") {
    withUserDefinedFunction("test_lower_udf" -> false) {
      sql(
        """
          |CREATE FUNCTION test_lower_udf(s STRING)
          |RETURNS STRING
          |RETURN lower(s)
          |""".stripMargin)
      checkError(
        exception = intercept[AnalysisException] {
          sql("SELECT transform(array('A', 'B', 'C'), x -> test_lower_udf(x))").collect()
        },
        condition = "UNSUPPORTED_FEATURE.LAMBDA_FUNCTION_WITH_SQL_UDF",
        parameters = Map("funcName" -> "spark_catalog.default.test_lower_udf"),
        context = ExpectedContext(
          fragment = "test_lower_udf(x)",
          start = 44,
          stop = 60
        )
      )
    }
  }

  test("SPARK-56639: SQL function uses frozen SQL path") {
    withSQLConf(SQLConf.PATH_ENABLED.key -> "true") {
      withDatabase("path_func_db_a", "path_func_db_b") {
        withTable("path_func_db_a.frozen_t", "path_func_db_b.frozen_t") {
          withUserDefinedFunction("frozen_fn" -> false) {
            sql("USE default")
            sql("CREATE DATABASE path_func_db_a")
            sql("CREATE DATABASE path_func_db_b")
            sql("CREATE TABLE path_func_db_a.frozen_t USING parquet AS SELECT 10 AS id")
            sql("CREATE TABLE path_func_db_b.frozen_t USING parquet AS SELECT 20 AS id")
            try {
              sql("SET PATH = spark_catalog.path_func_db_a, system.builtin")
              sql(
                """
                  |CREATE FUNCTION frozen_fn()
                  |RETURNS INT
                  |RETURN (SELECT MAX(id) FROM frozen_t)
                  |""".stripMargin)
              sql("SET PATH = spark_catalog.path_func_db_b, system.builtin")

              checkAnswer(sql("SELECT MAX(id) FROM frozen_t"), Row(20))
              checkAnswer(sql("SELECT default.frozen_fn()"), Row(10))
            } finally {
              sql("SET PATH = DEFAULT_PATH")
            }
          }
        }
      }
    }
  }

  test("SPARK-56639: SQL table function uses frozen SQL path") {
    withSQLConf(SQLConf.PATH_ENABLED.key -> "true") {
      withDatabase("path_tvf_db_a", "path_tvf_db_b") {
        withTable("path_tvf_db_a.frozen_t", "path_tvf_db_b.frozen_t") {
          withUserDefinedFunction("frozen_tvf" -> false) {
            sql("USE default")
            sql("CREATE DATABASE path_tvf_db_a")
            sql("CREATE DATABASE path_tvf_db_b")
            sql("CREATE TABLE path_tvf_db_a.frozen_t USING parquet AS SELECT 100 AS id")
            sql("CREATE TABLE path_tvf_db_b.frozen_t USING parquet AS SELECT 200 AS id")
            try {
              sql("SET PATH = spark_catalog.path_tvf_db_a, system.builtin")
              sql(
                """
                  |CREATE FUNCTION frozen_tvf()
                  |RETURNS TABLE(id INT)
                  |RETURN SELECT MAX(id) AS id FROM frozen_t
                  |""".stripMargin)
              sql("SET PATH = spark_catalog.path_tvf_db_b, system.builtin")

              checkAnswer(sql("SELECT MAX(id) FROM frozen_t"), Row(200))
              checkAnswer(sql("SELECT * FROM default.frozen_tvf()"), Row(100))
            } finally {
              sql("SET PATH = DEFAULT_PATH")
            }
          }
        }
      }
    }
  }

  // Regression guard: frozen resolution path must not leak into CURRENT_SCHEMA/CURRENT_PATH.
  test("SPARK-56639: current_schema/current_path in SQL functions use invoker context") {
    withSQLConf(SQLConf.PATH_ENABLED.key -> "true") {
      withDatabase("path_ctx_fn_a", "path_ctx_fn_b") {
        withUserDefinedFunction("path_ctx_fn_a.f_scalar_ctx" -> false,
          "path_ctx_fn_a.f_table_ctx" -> false) {
          sql("CREATE DATABASE path_ctx_fn_a")
          sql("CREATE DATABASE path_ctx_fn_b")
          try {
            sql("USE path_ctx_fn_a")
            sql(
              """
                |CREATE FUNCTION path_ctx_fn_a.f_scalar_ctx()
                |RETURNS STRING
                |RETURN concat(current_schema(), '::', current_path())
                |""".stripMargin)
            sql(
              """
                |CREATE FUNCTION path_ctx_fn_a.f_table_ctx()
                |RETURNS TABLE(cs STRING, cp STRING)
                |RETURN SELECT current_schema() AS cs, current_path() AS cp
                |""".stripMargin)

            sql("USE path_ctx_fn_b")
            sql("SET PATH = DEFAULT_PATH")

            val scalar = sql("SELECT path_ctx_fn_a.f_scalar_ctx()").head().getString(0)
            assert(scalar.startsWith("path_ctx_fn_b::"),
              s"Expected scalar function to use invoker current_schema, got: $scalar")
            assert(scalar.contains("path_ctx_fn_b"),
              s"Expected scalar function to use invoker current_path, got: $scalar")
            assert(!scalar.contains("path_ctx_fn_a"),
              s"Did not expect creator schema in scalar function context, got: $scalar")

            val table = sql("SELECT cs, cp FROM path_ctx_fn_a.f_table_ctx()").head()
            val tableSchema = table.getString(0)
            val tablePath = table.getString(1)
            assert(tableSchema == "path_ctx_fn_b",
              s"Expected table function to use invoker current_schema, got: $tableSchema")
            assert(tablePath.contains("path_ctx_fn_b"),
              s"Expected table function to use invoker current_path, got: $tablePath")
            assert(!tablePath.contains("path_ctx_fn_a"),
              s"Did not expect creator schema in table function context, got: $tablePath")
          } finally {
            sql("SET PATH = DEFAULT_PATH")
            sql("USE default")
          }
        }
      }
    }
  }
}
