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
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.SQLFunctionExpression
import org.apache.spark.sql.catalyst.catalog.SQLFunction
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.connector.catalog.CatalogManager
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

  test("SPARK-59970: a SQL function does not inherit the IDENTIFIER-clause variables of the " +
    "functions it calls") {
    withUserDefinedFunction("ident_callee" -> false, "ident_caller" -> false) {
      sql("DECLARE OR REPLACE VARIABLE ident_col STRING DEFAULT 'a'")
      try {
        // The callee may only be created with the legacy flag; its dependency on `ident_col` is
        // its own and is not persisted.
        withSQLConf(SQLConf.VARIABLES_UNDER_IDENTIFIER_IN_VIEW.key -> "true") {
          sql("CREATE FUNCTION ident_callee(x INT) RETURNS INT " +
            "RETURN (SELECT IDENTIFIER(ident_col) FROM VALUES (1) AS T(a))")
        }
        checkAnswer(sql("SELECT ident_callee(0)"), Row(1))
        // Expanding the callee must not attribute `ident_col` to the caller: creating the caller
        // succeeds without the flag. (The variable must still exist, since the callee's body is
        // inlined and its IDENTIFIER clause is evaluated during this analysis.)
        sql("CREATE FUNCTION ident_caller(x INT) RETURNS INT RETURN ident_callee(x) + 1")
        // A direct IDENTIFIER read in the caller's own body is still rejected.
        checkError(
          exception = intercept[AnalysisException] {
            sql("CREATE FUNCTION ident_caller2(x INT) RETURNS INT " +
              "RETURN ident_callee(x) + (SELECT IDENTIFIER(ident_col) FROM VALUES (1) AS T(a))")
          },
          condition = "INVALID_TEMP_OBJ_REFERENCE",
          parameters = Map(
            "obj" -> "FUNCTION",
            "objName" -> "`default`.`ident_caller2`",
            "tempObj" -> "VARIABLE",
            "tempObjName" -> "`ident_col`"))
        checkAnswer(sql("SELECT ident_caller(0)"), Row(2))
      } finally {
        sql("DROP TEMPORARY VARIABLE IF EXISTS ident_col")
      }
    }
  }

  test("SPARK-59970: IDENTIFIER-clause variables are stored for a temporary function only") {
    val variableNamesKey = "referredTempVariableNames"
    withUserDefinedFunction("ident_temp_fn" -> true, "ident_perm_fn" -> false) {
      sql("DECLARE OR REPLACE VARIABLE ident_col STRING DEFAULT 'a'")
      try {
        // A temporary function records the variable in its properties, as it does a direct
        // reference. The property is informational and is not consulted when the body is
        // re-analyzed: a function body resolves variables from the caller's context.
        sql("CREATE TEMPORARY FUNCTION ident_temp_fn(x INT) RETURNS INT " +
          "RETURN (SELECT IDENTIFIER(ident_col) FROM VALUES (1) AS T(a))")
        // Temporary functions are registered under the qualified `system.session` namespace.
        val tempFn = spark.sessionState.functionRegistry
          .lookupFunctionBuilder(FunctionIdentifier(
            "ident_temp_fn",
            Some(CatalogManager.SESSION_NAMESPACE),
            Some(CatalogManager.SYSTEM_CATALOG_NAME))).get
          .apply(Seq(Literal(0))) match {
            case f: SQLFunctionExpression => f.function
            case other => fail(s"unexpected builder result: $other")
          }
        assert(tempFn.properties(variableNamesKey) === """[["ident_col"]]""")

        // A persistent function created under the legacy flag stores no temporary variable, as a
        // persisted view does not: the dependency is tolerated, not persisted.
        withSQLConf(SQLConf.VARIABLES_UNDER_IDENTIFIER_IN_VIEW.key -> "true") {
          sql("CREATE FUNCTION ident_perm_fn(x INT) RETURNS INT " +
            "RETURN (SELECT IDENTIFIER(ident_col) FROM VALUES (1) AS T(a))")
        }
        val permFn = SQLFunction.fromCatalogFunction(
          spark.sessionState.catalog.getFunctionMetadata(
            FunctionIdentifier("ident_perm_fn", Some("default"))),
          spark.sessionState.sqlParser)
        assert(permFn.properties(variableNamesKey) === "[]")
      } finally {
        sql("DROP TEMPORARY VARIABLE IF EXISTS ident_col")
      }
    }
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

  test("SPARK-58779: SQL table function argument referencing a CTE") {
    withUserDefinedFunction("my_tvf" -> false) {
      sql(
        """
          |CREATE FUNCTION my_tvf(a INT, b INT)
          |RETURNS TABLE(x INT)
          |RETURN SELECT a + b AS x
          |""".stripMargin)
      // The table function is called with scalar-subquery arguments that reference a CTE.
      // ResolveSQLTableFunctions runs checkAnalysis (which runs InlineCTE) on the resolved
      // function plan, which contains a CTERelationRef whose CTERelationDef lives in the outer
      // WithCTE scope. Previously the unguarded cteMap(ref.cteId) lookup threw
      // NoSuchElementException.
      checkAnswer(sql(
        """
          |WITH cte AS (SELECT 1 AS c1, 2 AS c2)
          |SELECT * FROM my_tvf((SELECT c1 FROM cte), (SELECT c2 FROM cte))
          |""".stripMargin), Row(3))
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

  test("SPARK-52719: scalar SQL UDF as TVF argument") {
    withUserDefinedFunction("scalar_udf" -> false, "table_func" -> false) {
      sql("CREATE FUNCTION scalar_udf(x INT) RETURNS INT RETURN x + 1")
      sql(
        """CREATE FUNCTION table_func(x INT)
          |RETURNS TABLE(a INT)
          |RETURN SELECT x
          |""".stripMargin)
      checkAnswer(sql("SELECT * FROM table_func(scalar_udf(1))"), Row(2))
    }
  }

  test("SPARK-52719: table-valued SQL function in TVF argument is still rejected") {
    withUserDefinedFunction("tvf_inner" -> false, "tvf_outer" -> false) {
      sql(
        """CREATE FUNCTION tvf_inner(x INT)
          |RETURNS TABLE(a INT)
          |RETURN SELECT x
          |""".stripMargin)
      sql(
        """CREATE FUNCTION tvf_outer(x INT)
          |RETURNS TABLE(a INT)
          |RETURN SELECT x
          |""".stripMargin)
      checkError(
        exception = intercept[AnalysisException] {
          sql("SELECT * FROM tvf_outer(tvf_inner(1))").collect()
        },
        condition = "NOT_A_SCALAR_FUNCTION",
        parameters = Map(
          "functionName" -> "`spark_catalog`.`default`.`tvf_inner`"),
        context = ExpectedContext(
          fragment = "tvf_inner(1)",
          start = 24,
          stop = 35)
      )
    }
  }

  test("SPARK-52719: nested scalar UDFs in TVF argument") {
    withUserDefinedFunction("add_one" -> false, "tvf" -> false) {
      sql("CREATE FUNCTION add_one(x INT) RETURNS INT RETURN x + 1")
      sql(
        """CREATE FUNCTION tvf(x INT)
          |RETURNS TABLE(a INT)
          |RETURN SELECT x
          |""".stripMargin)
      checkAnswer(sql("SELECT * FROM tvf(add_one(add_one(1)))"), Row(3))
    }
  }

  test("SPARK-52719: scalar UDF mixed with literals in TVF arguments") {
    withUserDefinedFunction("add_one" -> false, "multi_tvf" -> false) {
      sql("CREATE FUNCTION add_one(x INT) RETURNS INT RETURN x + 1")
      sql(
        """CREATE FUNCTION multi_tvf(x INT, y INT)
          |RETURNS TABLE(a INT)
          |RETURN SELECT x + y
          |""".stripMargin)
      checkAnswer(sql("SELECT * FROM multi_tvf(add_one(3), 10)"), Row(14))
    }
  }

  test("SPARK-52719: scalar UDF returning NULL in TVF argument") {
    withUserDefinedFunction("null_udf" -> false, "tvf" -> false) {
      sql("CREATE FUNCTION null_udf(x INT) RETURNS INT RETURN CAST(NULL AS INT)")
      sql(
        """CREATE FUNCTION tvf(x INT)
          |RETURNS TABLE(a INT)
          |RETURN SELECT x
          |""".stripMargin)
      checkAnswer(sql("SELECT * FROM tvf(null_udf(1))"), Row(null))
    }
  }

  test("SPARK-60805: a SQL function body can call system-qualified functions") {
    withUserDefinedFunction("perm_q" -> false, "perm_q_tbl" -> false) {
      Seq("builtin.abs(-1)", "system.builtin.abs(-1)", "SYSTEM.Builtin.abs(-1)").foreach { call =>
        sql(s"CREATE OR REPLACE FUNCTION perm_q() RETURNS INT RETURN $call")
        checkAnswer(sql("SELECT perm_q()"), Row(1))
        sql(s"CREATE OR REPLACE FUNCTION perm_q_tbl() RETURNS TABLE (a INT) RETURN SELECT $call")
        checkAnswer(sql("SELECT * FROM perm_q_tbl()"), Row(1))
      }
      // A builtin.-qualified name that exists nowhere is an unresolved routine.
      val e = intercept[AnalysisException] {
        sql("CREATE OR REPLACE FUNCTION perm_q() RETURNS INT RETURN builtin.nope()")
      }
      assert(e.getCondition == "UNRESOLVED_ROUTINE", e.getMessage)
      // A system-qualified built-in table function, too.
      Seq("builtin.range(3)", "system.builtin.range(3)").foreach { call =>
        sql(s"CREATE OR REPLACE FUNCTION perm_q_tbl() RETURNS TABLE (id BIGINT) " +
          s"RETURN SELECT * FROM $call")
        checkAnswer(sql("SELECT * FROM perm_q_tbl()"), Seq(Row(0), Row(1), Row(2)))
      }
    }
    // A catalog-qualified persistent call works too, and is checked for cyclic references.
    withUserDefinedFunction("perm_in" -> false, "perm_out" -> false) {
      sql("CREATE FUNCTION perm_in() RETURNS INT RETURN 5")
      sql("CREATE FUNCTION perm_out() RETURNS INT RETURN spark_catalog.default.perm_in()")
      checkAnswer(sql("SELECT perm_out()"), Row(5))
      val e = intercept[AnalysisException] {
        sql("CREATE OR REPLACE FUNCTION perm_in() RETURNS INT " +
          "RETURN spark_catalog.default.perm_out()")
      }
      assert(e.getCondition == "CYCLIC_FUNCTION_REFERENCE", e.getMessage)
    }
    withUserDefinedFunction("tbl_in" -> false, "tbl_out" -> false) {
      sql("CREATE FUNCTION tbl_in() RETURNS TABLE (a INT) RETURN SELECT 1")
      sql("CREATE FUNCTION tbl_out() RETURNS TABLE (a INT) " +
        "RETURN SELECT * FROM SPARK_CATALOG.default.tbl_in()")
      checkAnswer(sql("SELECT * FROM tbl_out()"), Row(1))
      val e = intercept[AnalysisException] {
        sql("CREATE OR REPLACE FUNCTION tbl_in() RETURNS TABLE (a INT) " +
          "RETURN SELECT * FROM spark_catalog.default.tbl_out()")
      }
      assert(e.getCondition == "CYCLIC_FUNCTION_REFERENCE", e.getMessage)
    }
    // A session-qualified call is checked for cyclic references like an unqualified one.
    withUserDefinedFunction("temp_c" -> true, "temp_c_tbl" -> true) {
      sql("CREATE TEMPORARY FUNCTION temp_c() RETURNS INT RETURN 1")
      sql("CREATE TEMPORARY FUNCTION temp_c_tbl() RETURNS TABLE (a INT) RETURN SELECT 1")
      Seq(
        "CREATE OR REPLACE TEMPORARY FUNCTION temp_c() RETURNS INT RETURN session.temp_c()",
        "CREATE OR REPLACE TEMPORARY FUNCTION temp_c() RETURNS INT " +
          "RETURN system.session.temp_c()",
        "CREATE OR REPLACE TEMPORARY FUNCTION temp_c_tbl() RETURNS TABLE (a INT) " +
          "RETURN SELECT * FROM session.temp_c_tbl()"
      ).foreach { ddl =>
        val e = intercept[AnalysisException](sql(ddl))
        assert(e.getCondition == "CYCLIC_FUNCTION_REFERENCE", s"$ddl: ${e.getMessage}")
      }
    }
  }

  test("SPARK-60805: a permanent function cannot reference a temp function or temp TVF") {
    withUserDefinedFunction("temp_fn" -> true, "temp_caller" -> true, "temp_tbl" -> true,
        "temp_tbl_caller" -> true) {
      sql("CREATE TEMPORARY FUNCTION temp_fn() RETURNS INT RETURN 1")
      sql("CREATE TEMPORARY FUNCTION temp_tbl() RETURNS TABLE (a INT) RETURN SELECT 1")
      // A temporary function may call another temporary function through `system.session.`.
      sql("CREATE TEMPORARY FUNCTION temp_caller() RETURNS INT RETURN system.session.temp_fn()")
      checkAnswer(sql("SELECT temp_caller()"), Row(1))
      sql("CREATE TEMPORARY FUNCTION temp_tbl_caller() RETURNS TABLE (a INT) " +
        "RETURN SELECT * FROM system.session.temp_tbl()")
      checkAnswer(sql("SELECT * FROM temp_tbl_caller()"), Row(1))
      Seq(
        "CREATE FUNCTION perm_fn() RETURNS INT RETURN session.temp_fn()",
        "CREATE FUNCTION perm_fn() RETURNS INT RETURN system.session.temp_fn()",
        "CREATE FUNCTION perm_fn() RETURNS INT RETURN IDENTIFIER('temp_fn')()",
        "CREATE FUNCTION perm_fn() RETURNS INT RETURN IDENTIFIER('session.temp_fn')() + 1",
        "CREATE FUNCTION perm_fn() RETURNS INT RETURN IDENTIFIER(concat('temp', '_fn'))()",
        "CREATE FUNCTION perm_fn() RETURNS INT RETURN IDENTIFIER(lower('TEMP_FN'))()",
        "CREATE FUNCTION perm_fn() RETURNS INT RETURN (SELECT a FROM temp_tbl())",
        "CREATE FUNCTION perm_fn() RETURNS INT RETURN (SELECT a FROM IDENTIFIER('temp_tbl')())",
        "CREATE FUNCTION perm_fn() RETURNS TABLE (a INT) RETURN SELECT * FROM temp_tbl()",
        "CREATE FUNCTION perm_fn() RETURNS TABLE (a INT) RETURN SELECT * FROM session.temp_tbl()",
        "CREATE FUNCTION perm_fn() RETURNS TABLE (a INT) " +
          "RETURN SELECT * FROM system.session.temp_tbl()",
        "CREATE FUNCTION perm_fn() RETURNS INT RETURN (SELECT a FROM session.temp_tbl())"
      ).foreach { ddl =>
        val e = intercept[AnalysisException](sql(ddl))
        assert(e.getCondition == "INVALID_TEMP_OBJ_REFERENCE", s"$ddl: ${e.getMessage}")
      }
      withUserDefinedFunction("temp_built" -> true, "perm_built" -> false) {
        sql("CREATE TEMPORARY FUNCTION temp_built() RETURNS INT " +
          "RETURN IDENTIFIER(concat('temp', '_fn'))()")
        checkAnswer(sql("SELECT temp_built()"), Row(1))
        sql("CREATE FUNCTION perm_built() RETURNS INT RETURN IDENTIFIER(concat('ab', 's'))(-1)")
        checkAnswer(sql("SELECT perm_built()"), Row(1))
      }
      // The legacy flag re-allows only the forms earlier versions accepted at creation.
      val legacyKey = SQLConf.LEGACY_ALLOW_TEMP_FUNCTION_IN_PERSISTED_SQL_FUNCTION.key
      withUserDefinedFunction("perm_legacy" -> false, "perm_legacy_tbl" -> false,
          "perm_outer" -> false) {
        withSQLConf(legacyKey -> "true") {
          Seq("temp_tbl()", "IDENTIFIER('temp_tbl')()").foreach { call =>
            sql("CREATE OR REPLACE FUNCTION perm_legacy_tbl() RETURNS TABLE (a INT) " +
              s"RETURN SELECT * FROM $call")
            checkAnswer(sql("SELECT * FROM perm_legacy_tbl()"), Row(1))
          }
          Seq("IDENTIFIER('temp_fn')()", "IDENTIFIER(concat('temp', '_fn'))()").foreach { call =>
            sql(s"CREATE OR REPLACE FUNCTION perm_legacy() RETURNS INT RETURN $call")
            checkAnswer(sql("SELECT perm_legacy()"), Row(1))
          }
          Seq(
            "RETURNS INT RETURN temp_fn()",
            "RETURNS INT RETURN session.temp_fn()",
            "RETURNS INT RETURN system.session.temp_fn()",
            "RETURNS TABLE (a INT) RETURN SELECT * FROM session.temp_tbl()",
            "RETURNS TABLE (a INT) RETURN SELECT * FROM system.session.temp_tbl()"
          ).foreach { body =>
            val e = intercept[AnalysisException] {
              sql(s"CREATE OR REPLACE FUNCTION perm_legacy_rej() $body")
            }
            assert(e.getCondition == "INVALID_TEMP_OBJ_REFERENCE", s"$body: ${e.getMessage}")
          }
        }
        // Without the flag, calling a function that depends on a temp TVF is rejected too.
        val e = intercept[AnalysisException] {
          sql("CREATE FUNCTION perm_outer() RETURNS TABLE (a INT) " +
            "RETURN SELECT * FROM perm_legacy_tbl()")
        }
        assert(e.getCondition == "INVALID_TEMP_OBJ_REFERENCE", e.getMessage)
        assert(e.getMessageParameters.get("tempObjName") == "`temp_tbl`", e.getMessage)
        val scalar = intercept[AnalysisException] {
          sql("CREATE FUNCTION perm_outer() RETURNS INT RETURN (SELECT a FROM perm_legacy_tbl())")
        }
        assert(scalar.getCondition == "INVALID_TEMP_OBJ_REFERENCE", scalar.getMessage)
        assert(scalar.getMessageParameters.get("tempObjName") == "`temp_tbl`", scalar.getMessage)
      }
    }
  }

  test("SPARK-60805: a temp TVF counts only when resolution binds it") {
    // A same-named temp function does not make a built-in table function call a temp reference.
    withUserDefinedFunction("perm_range" -> false, "perm_explode" -> false) {
      try {
        sql("CREATE TEMPORARY FUNCTION range() RETURNS TABLE (a INT) RETURN SELECT 1")
        sql("CREATE TEMPORARY FUNCTION explode(x INT) RETURNS INT RETURN x")
        sql("CREATE FUNCTION perm_range() RETURNS TABLE (id BIGINT) RETURN SELECT * FROM range(3)")
        checkAnswer(sql("SELECT * FROM perm_range()"), Seq(Row(0), Row(1), Row(2)))
        sql("CREATE FUNCTION perm_explode() RETURNS TABLE (col INT) " +
          "RETURN SELECT * FROM explode(array(1, 2))")
        checkAnswer(sql("SELECT * FROM perm_explode()"), Seq(Row(1), Row(2)))
      } finally {
        sql("DROP TEMPORARY FUNCTION IF EXISTS range")
        sql("DROP TEMPORARY FUNCTION IF EXISTS explode")
      }
    }
    // Nor does it when a persistent function owns the call.
    withUserDefinedFunction("perm_last" -> false) {
      try {
        sql("CREATE FUNCTION default.last_tvf() RETURNS TABLE (a INT) RETURN SELECT 5")
        sql("CREATE TEMPORARY FUNCTION last_tvf() RETURNS TABLE (a INT) RETURN SELECT 9")
        withSQLConf(SQLConf.SESSION_FUNCTION_RESOLUTION_ORDER.key -> "last") {
          sql("CREATE FUNCTION perm_last() RETURNS TABLE (a INT) RETURN SELECT * FROM last_tvf()")
          checkAnswer(sql("SELECT * FROM perm_last()"), Row(5))
        }
      } finally {
        sql("DROP TEMPORARY FUNCTION IF EXISTS last_tvf")
        sql("DROP FUNCTION IF EXISTS default.last_tvf")
      }
    }
    // A temp TVF called through IDENTIFIER is a temp reference.
    withUserDefinedFunction("temp_tbl" -> true, "temp_ident" -> true, "perm_ident" -> false) {
      sql("CREATE TEMPORARY FUNCTION temp_tbl() RETURNS TABLE (a INT) RETURN SELECT 1")
      Seq("temp_tbl", "TEMP_TBL", "session.temp_tbl", "system.session.temp_tbl").foreach { ident =>
        val call = s"IDENTIFIER('$ident')()"
        sql("CREATE OR REPLACE TEMPORARY FUNCTION temp_ident() RETURNS TABLE (a INT) " +
          s"RETURN SELECT * FROM $call")
        checkAnswer(sql("SELECT * FROM temp_ident()"), Row(1))
        val e = intercept[AnalysisException] {
          sql(s"CREATE FUNCTION perm_ident() RETURNS TABLE (a INT) RETURN SELECT * FROM $call")
        }
        assert(e.getCondition == "INVALID_TEMP_OBJ_REFERENCE", s"$call: ${e.getMessage}")
        assert(e.getMessageParameters.get("tempObjName").equalsIgnoreCase("`temp_tbl`"),
          e.getMessage)
      }
    }
  }

  test("SPARK-60805: builtin./session. calls fall back to a same-named persistent schema") {
    Seq("builtin", "session").foreach { schema =>
      withDatabase(schema) {
        sql(s"CREATE DATABASE $schema")
        sql(s"CREATE FUNCTION $schema.pf() RETURNS INT RETURN 1")
        sql(s"CREATE FUNCTION $schema.pg() RETURNS INT RETURN $schema.pf()")
        sql(s"CREATE FUNCTION $schema.pt() RETURNS TABLE (a INT) RETURN SELECT 1")
        checkAnswer(sql(s"SELECT $schema.pg()"), Row(1))
        Seq(
          s"CREATE OR REPLACE FUNCTION $schema.pf() RETURNS INT RETURN $schema.pf()",
          s"CREATE OR REPLACE FUNCTION $schema.pf() RETURNS INT RETURN $schema.pg()",
          s"CREATE OR REPLACE FUNCTION $schema.pt() RETURNS TABLE (a INT) " +
            s"RETURN SELECT * FROM $schema.pt()"
        ).foreach { ddl =>
          val e = intercept[AnalysisException](sql(ddl))
          assert(e.getCondition == "CYCLIC_FUNCTION_REFERENCE", s"$ddl: ${e.getMessage}")
        }
      }
    }
  }

  test("SPARK-60805: session. calls follow spark.sql.legacy.persistentCatalogFirst") {
    // With no same-named persistent schema, builtin./session. still reach the system namespace.
    withUserDefinedFunction("temp_fn" -> true, "perm_legacy" -> false) {
      sql("CREATE TEMPORARY FUNCTION temp_fn() RETURNS INT RETURN 1")
      withSQLConf(SQLConf.PERSISTENT_CATALOG_FIRST.key -> "true") {
        sql("CREATE FUNCTION perm_legacy() RETURNS INT RETURN builtin.abs(-1)")
        checkAnswer(sql("SELECT perm_legacy()"), Row(1))
        val e = intercept[AnalysisException] {
          sql("CREATE OR REPLACE FUNCTION perm_legacy() RETURNS INT RETURN session.temp_fn()")
        }
        assert(e.getCondition == "INVALID_TEMP_OBJ_REFERENCE", e.getMessage)
      }
    }
    withDatabase("session") {
      sql("CREATE DATABASE session")
      sql("CREATE FUNCTION session.sf() RETURNS INT RETURN 3")
      withUserDefinedFunction("sf" -> true, "perm_sf" -> false) {
        sql("CREATE TEMPORARY FUNCTION sf() RETURNS INT RETURN 7")
        val createPerm = "CREATE OR REPLACE FUNCTION perm_sf() RETURNS INT RETURN session.sf()"
        // By default the temp function owns `session.sf`.
        val e = intercept[AnalysisException](sql(createPerm))
        assert(e.getCondition == "INVALID_TEMP_OBJ_REFERENCE", e.getMessage)
        // Earlier versions took it to be the persistent function, as the legacy flag does.
        withSQLConf(SQLConf.LEGACY_ALLOW_TEMP_FUNCTION_IN_PERSISTED_SQL_FUNCTION.key -> "true") {
          sql(createPerm)
        }
        withSQLConf(SQLConf.PERSISTENT_CATALOG_FIRST.key -> "true") {
          // The persistent function owns it.
          sql(createPerm)
          checkAnswer(sql("SELECT perm_sf()"), Row(3))
          val cycle = intercept[AnalysisException] {
            sql("CREATE OR REPLACE FUNCTION session.sf() RETURNS INT RETURN session.sf()")
          }
          assert(cycle.getCondition == "CYCLIC_FUNCTION_REFERENCE", cycle.getMessage)
        }
      }
      // The same precedence for a table function.
      sql("CREATE FUNCTION session.pt() RETURNS TABLE (a INT) RETURN SELECT 3")
      withUserDefinedFunction("pt" -> true, "perm_pt" -> false) {
        sql("CREATE TEMPORARY FUNCTION pt() RETURNS TABLE (a INT) RETURN SELECT 7")
        val createPerm = "CREATE OR REPLACE FUNCTION perm_pt() RETURNS TABLE (a INT) " +
          "RETURN SELECT * FROM session.pt()"
        val e = intercept[AnalysisException](sql(createPerm))
        assert(e.getCondition == "INVALID_TEMP_OBJ_REFERENCE", e.getMessage)
        val cycle = intercept[AnalysisException] {
          sql("CREATE OR REPLACE TEMPORARY FUNCTION pt() RETURNS TABLE (a INT) " +
            "RETURN SELECT * FROM session.pt()")
        }
        assert(cycle.getCondition == "CYCLIC_FUNCTION_REFERENCE", cycle.getMessage)
        withSQLConf(SQLConf.LEGACY_ALLOW_TEMP_FUNCTION_IN_PERSISTED_SQL_FUNCTION.key -> "true") {
          sql(createPerm)
        }
        withSQLConf(SQLConf.PERSISTENT_CATALOG_FIRST.key -> "true") {
          sql(createPerm)
          checkAnswer(sql("SELECT * FROM perm_pt()"), Row(3))
        }
      }
    }
  }

  test("SPARK-60805: a call to another catalog's function is left to analysis") {
    val query = intercept[AnalysisException](sql("SELECT no_such_cat.ns.f()"))
    val create = intercept[AnalysisException] {
      sql("CREATE FUNCTION perm_cat() RETURNS INT RETURN no_such_cat.ns.f()")
    }
    assert(create.getCondition == query.getCondition, create.getMessage)
    assert(create.getCondition != "IDENTIFIER_TOO_MANY_NAME_PARTS", create.getMessage)
  }

  test("SPARK-60805: a catalog-qualified cycle is detected under case-sensitive analysis") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      withUserDefinedFunction("cs_in" -> false, "cs_out" -> false) {
        sql("CREATE FUNCTION cs_in() RETURNS INT RETURN 1")
        sql("CREATE FUNCTION cs_out() RETURNS INT RETURN SPARK_CATALOG.default.cs_in()")
        checkAnswer(sql("SELECT cs_out()"), Row(1))
        val e = intercept[AnalysisException] {
          sql("CREATE OR REPLACE FUNCTION cs_in() RETURNS INT " +
            "RETURN SPARK_CATALOG.default.cs_out()")
        }
        assert(e.getCondition == "CYCLIC_FUNCTION_REFERENCE", e.getMessage)
      }
      withUserDefinedFunction("cs_tin" -> false, "cs_tout" -> false) {
        sql("CREATE FUNCTION cs_tin() RETURNS TABLE (a INT) RETURN SELECT 1")
        sql("CREATE FUNCTION cs_tout() RETURNS TABLE (a INT) " +
          "RETURN SELECT * FROM SPARK_CATALOG.default.cs_tin()")
        val e = intercept[AnalysisException] {
          sql("CREATE OR REPLACE FUNCTION cs_tin() RETURNS TABLE (a INT) " +
            "RETURN SELECT * FROM SPARK_CATALOG.default.cs_tout()")
        }
        assert(e.getCondition == "CYCLIC_FUNCTION_REFERENCE", e.getMessage)
      }
    }
  }
}
