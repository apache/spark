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

import java.text.SimpleDateFormat
import java.util.Locale

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

  test("describe SQL scalar functions") {
    withUserDefinedFunction("foo" -> true, "bar" -> true, "area" -> false) {
      // Temporary function
      sql(
        """
          |CREATE TEMPORARY FUNCTION foo() RETURNS int
          |COMMENT 'function foo' RETURN 1
          |""".stripMargin)
      checkKeywordsExist(sql("describe function foo"),
        "Function:", "foo",
        "Type:", "SCALAR",
        "Input:", "()",
        "Returns:", "INT")
      checkKeywordsExist(sql("describe function extended foo"),
        "Deterministic: true",
        "Data Access:", "CONTAINS SQL",
        "Comment:", "function foo",
        "Create Time:",
        "Body:", "1")
      sql(
        """
          |CREATE TEMPORARY FUNCTION bar(x int default 8,
          |y int default substr('8hello', 1, 1) comment 'var_y')
          |RETURNS int COMMENT 'function bar' RETURN x + y
          |""".stripMargin)
      checkKeywordsExist(sql("describe function bar"),
        "Function:", "bar",
        "Input:", "x INT", "y INT",
        "Returns:", "INT")
      checkKeywordsExist(sql("describe function extended bar"),
        "Input:", "x INT DEFAULT 8", "y INT DEFAULT substr('8hello', 1, 1) 'var_y'",
        "Comment:", "function bar",
        "Deterministic: true",
        "Data Access:", "CONTAINS SQL",
        "Body:", "x + y")
      // Permanent function
      val beforeMs = System.currentTimeMillis()
      sql(
        """
          |CREATE FUNCTION area(width double comment 'width', height double comment 'height')
          |RETURNS double
          |COMMENT 'compute area'
          |DETERMINISTIC
          |RETURN width * height
          |""".stripMargin)
      val afterMs = System.currentTimeMillis()
      checkKeywordsExist(sql("describe function area"),
        "Function:", "default.area",
        "Type:", "SCALAR",
        "Input:", "width  DOUBLE", "height DOUBLE",
        "Returns:", "DOUBLE")
      val extendedRows = sql("describe function extended area").collect()
      checkKeywordsExist(sql("describe function extended area"),
        "Input:", "width  DOUBLE 'width'", "height DOUBLE 'height'",
        "Comment:", "compute area",
        "Deterministic: true",
        "Data Access:", "CONTAINS SQL",
        "Create Time:",
        "Body:", "width * height")
      // Verify the rendered Create Time falls within a small window around the
      // CREATE FUNCTION call, i.e. the timestamp set at CREATE time was preserved
      // (and not silently overwritten by a later cache-build / metadata-load).
      val createTimeRow = extendedRows.map(_.getString(0))
        .find(_.startsWith("Create Time:"))
        .getOrElse(fail("DESCRIBE FUNCTION EXTENDED is missing the Create Time row"))
      val tsStr = createTimeRow.split("Create Time:", 2)(1).trim
      // Date.toString() format -- explicit Locale.ENGLISH avoids parser drift on
      // build hosts whose default locale is not English.
      val sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.ENGLISH)
      val parsedMs = sdf.parse(tsStr).getTime
      // Date.toString() truncates to seconds; use a 2-second slop on each side.
      val slopMs = 2000L
      assert(parsedMs >= beforeMs - slopMs,
        s"Create Time '$tsStr' is before CREATE FUNCTION (beforeMs=$beforeMs)")
      assert(parsedMs <= afterMs + slopMs,
        s"Create Time '$tsStr' is after DESCRIBE FUNCTION (afterMs=$afterMs)")
    }
  }

  test("describe SQL table functions") {
    withUserDefinedFunction("foo" -> false) {
      sql(
        """
          |CREATE FUNCTION foo(x INT) RETURNS TABLE (a INT, b STRING)
          |COMMENT 'table function foo' RETURN SELECT x, x
          |""".stripMargin)
      checkKeywordsExist(sql("describe function foo"),
        "Function:", "foo",
        "Type:", "TABLE",
        "Input:", "x INT",
        "Returns:", "a INT", "b STRING")
      checkKeywordsExist(sql("describe function extended foo"),
        "Comment:", "table function foo",
        "Deterministic: true",
        "Data Access:", "CONTAINS SQL",
        "Create Time:",
        "Body:", "SELECT x, x")
    }
  }

  test("describe SQL functions with derived routine characteristics") {
    withUserDefinedFunction("foo" -> false, "bar" -> false, "baz" -> false) {
      withTable("tbl_for_describe") {
        sql("CREATE TABLE tbl_for_describe AS SELECT 1 AS x")
        sql("CREATE FUNCTION foo() RETURNS TABLE(x INT) RETURN SELECT * FROM tbl_for_describe")
        sql("CREATE FUNCTION bar() RETURNS DOUBLE RETURN SELECT SUM(x) + rand() FROM foo()")
        sql("CREATE FUNCTION baz() RETURNS INT NOT DETERMINISTIC READS SQL DATA RETURN 1")
        checkKeywordsExist(sql("DESCRIBE FUNCTION EXTENDED foo"),
          "Deterministic: true",
          "Data Access:", "READS SQL DATA")
        checkKeywordsExist(sql("DESCRIBE FUNCTION EXTENDED bar"),
          "Deterministic: false",
          "Data Access:", "READS SQL DATA")
        // Do not overwrite user-specified routine characteristics.
        checkKeywordsExist(sql("DESCRIBE FUNCTION EXTENDED baz"),
          "Deterministic: false",
          "Data Access:", "READS SQL DATA")
      }
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
              // DESCRIBE FUNCTION EXTENDED renders the frozen creator path,
              // not the invoker's current PATH. SqlPathFormat.formatForDisplay
              // back-ticks identifiers only when needed, so plain ASCII
              // identifiers appear unquoted.
              checkKeywordsExist(sql("DESCRIBE FUNCTION EXTENDED default.frozen_fn"),
                "SQL Path:",
                "spark_catalog.path_func_db_a, system.builtin")
              checkKeywordsNotExist(sql("DESCRIBE FUNCTION EXTENDED default.frozen_fn"),
                "path_func_db_b")
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

  // Run each SPARK-56945 case under both conf values: legacy=true asserts the pre-fix
  // (default) snapshot is eager, freezing the placeholder's hard-coded nullable = true;
  // legacy=false asserts the deferred snapshot preserves the body's actual non-null
  // nullability. df.schema("x").nullable must therefore equal `legacy`.
  Seq(true, false).foreach { legacy =>
    test(s"SPARK-56945: CTE column nullability tracks " +
        s"LEGACY_EAGER_CTE_SNAPSHOT_WITH_SQL_UDF=$legacy") {
      withSQLConf(SQLConf.LEGACY_EAGER_CTE_SNAPSHOT_WITH_SQL_UDF.key -> legacy.toString) {
        withUserDefinedFunction("non_null_one" -> false, "wrap_int" -> false) {
          sql("CREATE FUNCTION non_null_one() RETURNS INT RETURN 1")
          sql("CREATE FUNCTION wrap_int(x INT) RETURNS INT RETURN x")
          val df = sql(
            "WITH cte AS (SELECT wrap_int(non_null_one()) AS x) SELECT * FROM cte")
          assert(df.schema("x").nullable == legacy,
            s"Expected nullable = $legacy under legacy=$legacy, " +
              s"got schema ${df.schema.treeString}")
          checkAnswer(df, Row(1))
        }
      }
    }

    test(s"SPARK-56945: nested CTE schema settles across multiple ResolveSQLFunctions passes " +
        s"(legacy=$legacy)") {
      withSQLConf(SQLConf.LEGACY_EAGER_CTE_SNAPSHOT_WITH_SQL_UDF.key -> legacy.toString) {
        withUserDefinedFunction("non_null_one" -> false, "wrap_int" -> false) {
          sql("CREATE FUNCTION non_null_one() RETURNS INT RETURN 1")
          sql("CREATE FUNCTION wrap_int(x INT) RETURNS INT RETURN x")
          // 3-level nesting needs three ResolveSQLFunctions iterations to fully inline:
          // iter 1 rewrites non_null_one(), iter 2 rewrites the inner wrap_int(...), iter 3
          // rewrites the outer wrap_int(...). With legacy=false the CTE substitution must
          // remain deferred through both intermediate iterations and snapshot only after
          // the last one; with legacy=true it snapshots in iter 1 and freezes nullable=true.
          val df = sql(
            "WITH cte AS (SELECT wrap_int(wrap_int(non_null_one())) AS x) SELECT * FROM cte")
          assert(df.schema("x").nullable == legacy,
            s"Expected nullable = $legacy under legacy=$legacy, " +
              s"got schema ${df.schema.treeString}")
          checkAnswer(df, Row(1))
        }
      }
    }

    test(s"SPARK-56945: persisted VIEW captures schema at CREATE under legacy=$legacy and " +
        s"stays frozen on read") {
      withUserDefinedFunction("non_null_one" -> false, "wrap_int" -> false) {
        sql("CREATE FUNCTION non_null_one() RETURNS INT RETURN 1")
        sql("CREATE FUNCTION wrap_int(x INT) RETURNS INT RETURN x")
        withView("cte_udf_view") {
          // Snapshot the catalog schema under the create-time conf.
          withSQLConf(SQLConf.LEGACY_EAGER_CTE_SNAPSHOT_WITH_SQL_UDF.key -> legacy.toString) {
            sql(
              "CREATE VIEW cte_udf_view AS " +
                "WITH cte AS (SELECT wrap_int(non_null_one()) AS x) SELECT * FROM cte")
          }
          // The stored schema reflects the create-time conf and stays frozen regardless of
          // the read-time conf: flipping the conf at read time does not re-derive it.
          Seq(true, false).foreach { readLegacy =>
            withSQLConf(
                SQLConf.LEGACY_EAGER_CTE_SNAPSHOT_WITH_SQL_UDF.key -> readLegacy.toString) {
              val schema = spark.table("cte_udf_view").schema
              assert(schema("x").nullable == legacy,
                s"Expected stored view 'x' nullable = $legacy under create-time legacy=$legacy " +
                  s"(read-time legacy=$readLegacy), got schema ${schema.treeString}")
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
