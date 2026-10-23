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

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ExecuteImmediateEndToEndSuite extends SharedSparkSession {

  test("SPARK-47033: EXECUTE IMMEDIATE USING does not recognize session variable names") {
    try {
      spark.sql("DECLARE parm = 'Hello';")

      val originalQuery = spark.sql(
        "EXECUTE IMMEDIATE 'SELECT :parm' USING system.session.parm AS parm;")
      val newQuery = spark.sql("EXECUTE IMMEDIATE 'SELECT :parm' USING system.session.parm;")

      assert(originalQuery.columns sameElements newQuery.columns)

      checkAnswer(originalQuery, newQuery.collect().toIndexedSeq)
    } finally {
      spark.sql("DROP TEMPORARY VARIABLE IF EXISTS parm;")
    }
  }

  test("SQL Scripting not supported inside EXECUTE IMMEDIATE") {
    val executeImmediateText = "EXECUTE IMMEDIATE 'BEGIN SELECT 1; END'"
    checkError(
      exception = intercept[AnalysisException ] {
        spark.sql(executeImmediateText)
      },
      condition = "SQL_SCRIPT_IN_EXECUTE_IMMEDIATE",
      parameters = Map("sqlString" -> "BEGIN SELECT 1; END"))
  }

  test("EXECUTE IMMEDIATE resolves session variables in body") {
    withSessionVariable("v1", "v2") {
      spark.sql("DECLARE v1 = 42")
      spark.sql("DECLARE v2 = 99")
      checkAnswer(spark.sql("EXECUTE IMMEDIATE 'SELECT system.session.v1, v2'"), Row(42, 99))
    }
  }

  test("EXECUTE IMMEDIATE resolves session variables inside script") {
    withSessionVariable("v1", "v2") {
      spark.sql("DECLARE v1 = 10")
      spark.sql("DECLARE v2 = 20")
      val result = spark.sql(
        """
          |BEGIN
          |  DECLARE v3 = 1;
          |  EXECUTE IMMEDIATE 'SELECT system.session.v1, v2';
          |END
          |""".stripMargin)
      checkAnswer(result, Row(10, 20))
    }
  }

  test("EXECUTE IMMEDIATE does not resolve local variables") {
    val result = intercept[AnalysisException] {
      spark.sql(
        """
          |BEGIN
          |  DECLARE v1 = 5;
          |  EXECUTE IMMEDIATE 'SELECT v1';
          |END
          |""".stripMargin)
    }
    checkError(
      exception = result,
      condition = "UNRESOLVED_COLUMN.WITHOUT_SUGGESTION",
      sqlState = "42703",
      parameters = Map("objectName" -> "`v1`"),
      context = ExpectedContext(
        objectType = "EXECUTE IMMEDIATE",
        objectName = "",
        startIndex = 7,
        stopIndex = 8,
        fragment = "v1"))
  }

  test("EXECUTE IMMEDIATE does not resolve local variables in a command payload") {
    withTable("ei_local_cmd") {
      spark.sql("CREATE TABLE ei_local_cmd (id INT) USING parquet")
      // A deferred command payload is analyzed with local variables hidden, just like a query
      // payload, and that analysis is not re-run at the execution level. Referencing a local
      // variable must therefore fail at analysis.
      val result = intercept[AnalysisException] {
        spark.sql(
          """
            |BEGIN
            |  DECLARE v1 = 5;
            |  EXECUTE IMMEDIATE 'INSERT INTO ei_local_cmd SELECT v1';
            |END
            |""".stripMargin)
      }
      checkError(
        exception = result,
        condition = "UNRESOLVED_COLUMN.WITHOUT_SUGGESTION",
        sqlState = "42703",
        parameters = Map("objectName" -> "`v1`"),
        context = ExpectedContext(
          objectType = "EXECUTE IMMEDIATE",
          objectName = "",
          startIndex = 32,
          stopIndex = 33,
          fragment = "v1"))
    }
  }

  test("EXECUTE IMMEDIATE resolves local variable in USING clause") {
    val result = spark.sql(
      """
        |BEGIN
        |  DECLARE v1 = 5;
        |  EXECUTE IMMEDIATE 'SELECT ?' USING v1;
        |END
        |""".stripMargin)
    checkAnswer(result, Row(5))
  }

  test("EXECUTE IMMEDIATE resolves session var in body and local var in USING") {
    withSessionVariable("v1") {
      spark.sql("DECLARE v1 = 10")
      val result = spark.sql(
        """
          |BEGIN
          |  DECLARE v2 = 20;
          |  EXECUTE IMMEDIATE 'SELECT system.session.v1, ?' USING v2;
          |END
          |""".stripMargin)
      checkAnswer(result, Row(10, 20))
    }
  }

  test("EXECUTE IMMEDIATE fails when local var referenced in body alongside session var") {
    withSessionVariable("v1") {
      spark.sql("DECLARE v1 = 10")
      val e = intercept[AnalysisException] {
        spark.sql(
          """
            |BEGIN
            |  DECLARE v2 = 20;
            |  EXECUTE IMMEDIATE 'SELECT v1, v2';
            |END
            |""".stripMargin)
      }
      checkError(
        exception = e,
        condition = "UNRESOLVED_COLUMN.WITHOUT_SUGGESTION",
        sqlState = "42703",
        parameters = Map("objectName" -> "`v2`"),
        context = ExpectedContext(
          objectType = "EXECUTE IMMEDIATE",
          objectName = "",
          startIndex = 11,
          stopIndex = 12,
          fragment = "v2"))
    }
  }

  test("EXPLAIN EXECUTE IMMEDIATE does not execute the command payload") {
    withTable("execute_immediate_explain") {
      spark.sql("CREATE TABLE execute_immediate_explain (id INT) USING parquet")
      // EXPLAIN analyzes the payload but must not run it: command execution is deferred to the
      // execution level, so the DROP should have no effect here.
      spark.sql("EXPLAIN EXECUTE IMMEDIATE 'DROP TABLE execute_immediate_explain'").collect()
      assert(spark.catalog.tableExists("execute_immediate_explain"),
        "EXPLAIN must not execute the EXECUTE IMMEDIATE command payload")
    }
  }

  test("EXECUTE IMMEDIATE executes the command payload when run") {
    withTable("execute_immediate_run") {
      spark.sql("CREATE TABLE execute_immediate_run (id INT) USING parquet")
      spark.sql("EXECUTE IMMEDIATE 'DROP TABLE execute_immediate_run'")
      assert(!spark.catalog.tableExists("execute_immediate_run"),
        "EXECUTE IMMEDIATE must execute the command payload")
    }
  }

  test("EXECUTE IMMEDIATE runs a command payload exactly once") {
    withTable("execute_immediate_once") {
      spark.sql("CREATE TABLE execute_immediate_once (id INT) USING parquet")
      // ExecuteImmediateExec is the sole executor of the payload; a double execution would insert
      // the row twice. Asserting exactly one row guards the single-execution invariant.
      spark.sql("EXECUTE IMMEDIATE 'INSERT INTO execute_immediate_once VALUES (?)' USING 1")
      checkAnswer(spark.table("execute_immediate_once"), Row(1))
    }
  }

  test("EXPLAIN shows the EXECUTE IMMEDIATE command payload node") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      val plan = spark.sql("EXPLAIN EXECUTE IMMEDIATE 'SET spark.sql.ansi.enabled=true'")
        .collect().map(_.getString(0)).mkString("\n")
      // The physical node renders as "ExecuteImmediate" (TreeNode.nodeName strips the "Exec"
      // suffix); assert it appears together with its supervised payload, which EXPLAIN surfaces via
      // innerChildren.
      assert(plan.contains("ExecuteImmediate") && plan.contains("SetCommand"),
        s"EXPLAIN should show the ExecuteImmediate node wrapping its payload, but was:\n$plan")
      // EXPLAIN must analyze but not run the SET, so the conf stays at its pre-EXPLAIN value;
      // otherwise it would pollute later tests in this suite.
      assert(spark.conf.get(SQLConf.ANSI_ENABLED.key) == "false",
        "EXPLAIN must not execute the EXECUTE IMMEDIATE SET payload")
    }
  }

  test("EXECUTE IMMEDIATE runs a nested command payload exactly once") {
    withTable("ei_nested") {
      spark.sql("CREATE TABLE ei_nested (id INT) USING parquet")
      // The inner statement is itself an EXECUTE IMMEDIATE command, so the payload is a nested
      // ExecuteImmediateCommand: ExecuteImmediateExec.run runs it via QueryExecution.runCommand,
      // which plans it to another ExecuteImmediateExec. Asserting exactly one row guards the
      // single-execution invariant through the recursive deferral.
      spark.sql(
        """EXECUTE IMMEDIATE 'EXECUTE IMMEDIATE \'INSERT INTO ei_nested VALUES (1)\''""")
      checkAnswer(spark.table("ei_nested"), Row(1))
    }
  }

  test("EXECUTE IMMEDIATE runs a deferred command payload inside a SQL script") {
    withTable("ei_script") {
      spark.sql("CREATE TABLE ei_script (id INT) USING parquet")
      // A command payload deferred to the execution level must still run inside a BEGIN...END
      // script frame, where variable hiding and the scripting context are set up during analysis.
      spark.sql(
        """
          |BEGIN
          |  EXECUTE IMMEDIATE 'INSERT INTO ei_script VALUES (1)';
          |END
          |""".stripMargin).collect()
      checkAnswer(spark.table("ei_script"), Row(1))
    }
  }

  test("EXPLAIN EXECUTE IMMEDIATE does not run a parameterized command payload") {
    withTable("ei_explain_param") {
      spark.sql("CREATE TABLE ei_explain_param (id INT) USING parquet")
      val plan = spark.sql(
        "EXPLAIN EXECUTE IMMEDIATE 'INSERT INTO ei_explain_param VALUES (?)' USING 1")
        .collect().map(_.getString(0)).mkString("\n")
      assert(plan.contains("ExecuteImmediate"),
        s"EXPLAIN should show the ExecuteImmediate node, but was:\n$plan")
      // EXPLAIN analyzes and binds the parameter but must not run the command payload.
      checkAnswer(spark.table("ei_explain_param"), Seq.empty[Row])
    }
  }

  test("EXPLAIN EXECUTE IMMEDIATE splices a query payload instead of wrapping it") {
    val plan = spark.sql("EXPLAIN EXECUTE IMMEDIATE 'SELECT 1'")
      .collect().map(_.getString(0)).mkString("\n")
    // A query payload is spliced directly, so no ExecuteImmediate node wraps it (unlike a
    // command payload); EXPLAIN renders the query plan itself.
    assert(plan.nonEmpty && !plan.contains("ExecuteImmediate"),
      s"query payloads should be spliced, not wrapped, but was:\n$plan")
  }

  test("EXECUTE IMMEDIATE runtime error in a deferred command references the dynamic SQL") {
    withTable("ei_ctx_src", "ei_ctx_dst") {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
        spark.sql("CREATE TABLE ei_ctx_src (v INT) USING parquet")
        spark.sql("INSERT INTO ei_ctx_src VALUES (0)")
        spark.sql("CREATE TABLE ei_ctx_dst (r BIGINT) USING parquet")
        // The command payload runs at the execution level, not during analysis. A runtime failure
        // inside it must still carry the dynamic SQL's query context (origin objectType
        // "EXECUTE IMMEDIATE"); `v` is a column so the division is not constant-folded and fails
        // during execution.
        val e = intercept[Exception] {
          spark.sql("EXECUTE IMMEDIATE 'INSERT INTO ei_ctx_dst SELECT 1 div v FROM ei_ctx_src'")
        }
        // The SparkThrowable may be wrapped in an execution/task exception, so scan the cause chain
        // for the first one carrying a query context.
        val contexts = Iterator.iterate(e: Throwable)(_.getCause).takeWhile(_ != null)
          .collect { case st: SparkThrowable if st.getQueryContext.nonEmpty => st.getQueryContext }
          .toSeq
        assert(contexts.nonEmpty, s"runtime error should carry a query context, but was: $e")
        val ctx = contexts.head.head
        assert(ctx.objectType() == "EXECUTE IMMEDIATE",
          s"context should reference the EXECUTE IMMEDIATE origin, but was '${ctx.objectType()}'")
        assert(ctx.fragment().contains("div"),
          s"context should point at the failing dynamic-SQL fragment, but was '${ctx.fragment()}'")
      }
    }
  }

  test("EXPLAIN of EXECUTE IMMEDIATE INTO does not assign, and INTO rejects command payloads") {
    withSessionVariable("ei_into_v") {
      spark.sql("DECLARE ei_into_v INT")
      // The INTO clause becomes a deferred SetVariable; EXPLAIN must analyze but not assign it.
      spark.sql("EXPLAIN EXECUTE IMMEDIATE 'SELECT 1' INTO ei_into_v").collect()
      checkAnswer(spark.sql("SELECT ei_into_v"), Row(null))
      // Executing it assigns the variable.
      spark.sql("EXECUTE IMMEDIATE 'SELECT 1' INTO ei_into_v")
      checkAnswer(spark.sql("SELECT ei_into_v"), Row(1))
      // A command payload with INTO is rejected during analysis.
      withTable("ei_into_cmd") {
        spark.sql("CREATE TABLE ei_into_cmd (id INT) USING parquet")
        checkError(
          exception = intercept[AnalysisException] {
            spark.sql("EXECUTE IMMEDIATE 'INSERT INTO ei_into_cmd VALUES (1)' INTO ei_into_v")
          },
          condition = "INVALID_STATEMENT_FOR_EXECUTE_INTO",
          parameters = Map("sqlString" -> "INSERT INTO EI_INTO_CMD VALUES (1)"))
      }
    }
  }

  test("EXECUTE IMMEDIATE preserves a multi-column command's output schema and rows") {
    withTable("ei_show_tbl") {
      spark.sql("CREATE TABLE ei_show_tbl (id INT) USING parquet")
      // SHOW TABLES is a command with multi-column output. ExecuteImmediateExec.output is
      // sourceStatement.output, so the deferred command must expose the same schema and rows as a
      // direct SHOW TABLES (guards output stability across the execution-level re-plan).
      val direct = spark.sql("SHOW TABLES")
      val viaEI = spark.sql("EXECUTE IMMEDIATE 'SHOW TABLES'")
      assert(viaEI.schema == direct.schema,
        s"schema mismatch: EI=${viaEI.schema} direct=${direct.schema}")
      checkAnswer(viaEI, direct.collect().toIndexedSeq)
    }
  }

  test("EXECUTE IMMEDIATE defers a multi-INSERT (Union of commands) to the execution level") {
    withTable("ei_multi_src", "ei_multi_a", "ei_multi_b") {
      spark.sql("CREATE TABLE ei_multi_src (id INT) USING parquet")
      spark.sql("INSERT INTO ei_multi_src VALUES (1), (2)")
      spark.sql("CREATE TABLE ei_multi_a (id INT) USING parquet")
      spark.sql("CREATE TABLE ei_multi_b (id INT) USING parquet")
      // A multi-INSERT analyzes to a Union of INSERT commands, which is not itself a Command.
      // ExecuteImmediate must still wrap it (isEagerlyExecutedCommand covers a Union of commands),
      // so EXPLAIN shows the ExecuteImmediate node and does not run the inserts.
      val multiInsert = "FROM ei_multi_src " +
        "INSERT INTO ei_multi_a SELECT id INSERT INTO ei_multi_b SELECT id"
      val plan = spark.sql(s"EXPLAIN EXECUTE IMMEDIATE '$multiInsert'")
        .collect().map(_.getString(0)).mkString("\n")
      assert(plan.contains("ExecuteImmediate"),
        s"multi-INSERT payload should be wrapped, not spliced, but was:\n$plan")
      assert(spark.table("ei_multi_a").isEmpty && spark.table("ei_multi_b").isEmpty,
        "EXPLAIN must not run the multi-INSERT command payload")
      // Running it performs both inserts.
      spark.sql(s"EXECUTE IMMEDIATE '$multiInsert'")
      checkAnswer(spark.table("ei_multi_a"), Seq(Row(1), Row(2)))
      checkAnswer(spark.table("ei_multi_b"), Seq(Row(1), Row(2)))
    }
  }
}
