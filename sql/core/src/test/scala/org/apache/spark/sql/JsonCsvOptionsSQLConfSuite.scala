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

import java.sql.{Date, Timestamp}
import java.time.Instant

import org.apache.spark.sql.catalyst.{FunctionIdentifier, QueryPlanningTracker}
import org.apache.spark.sql.catalyst.analysis.{SQLFunctionExpression, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.{Alias, EquivalentExpressions, Expression, JsonToStructs, Literal, NamedArgumentExpression, StructsToCsv}
import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, OneRowRelation}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

class JsonCsvOptionsSQLConfSuite extends QueryTest with SharedSparkSession {
  private val captureKey = SQLConf.JSON_CSV_EXPRESSION_USE_CAPTURED_OPTION_DEFAULTS.key
  private val fallbackKey = SQLConf.LEGACY_JSON_ENABLE_DATE_TIME_PARSING_FALLBACK.key

  private def withFallbackConf(value: Option[Boolean])(f: => Unit): Unit =
    withBooleanConf(fallbackKey, value)(f)

  private def withBooleanConf(key: String, value: Option[Boolean])(f: => Unit): Unit = {
    val saved = spark.sessionState.conf.getAllConfs.get(key)
    def setValue(value: Option[String]): Unit = value match {
      case Some(v) => spark.conf.set(key, v)
      case None => spark.conf.unset(key)
    }
    try {
      setValue(value.map(_.toString))
      f
    } finally {
      setValue(saved)
    }
  }

  private def withDefinition(
      kind: String,
      body: String,
      returnType: String)(f: String => Unit): Unit = {
    if (kind == "view") {
      withView("captured_options_view") {
        sql(s"CREATE VIEW captured_options_view AS SELECT $body AS value FROM range(1, 3)")
        f("SELECT value FROM captured_options_view")
      }
    } else {
      withUserDefinedFunction("captured_options_fn" -> false) {
        sql(s"CREATE FUNCTION captured_options_fn(id BIGINT) RETURNS $returnType RETURN $body")
        f("SELECT captured_options_fn(id) AS value FROM range(1, 3)")
      }
    }
  }

  private def withDefaultFunction(
      kind: String,
      default: String,
      dataType: String)(f: (String => DataFrame) => Unit): Unit = {
    withUserDefinedFunction("captured_default_fn" -> false) {
      val returns = if (kind == "scalar") dataType else s"TABLE(value $dataType)"
      val body = if (kind == "scalar") "s" else "SELECT s AS value"
      sql(s"CREATE FUNCTION captured_default_fn(a INT, s $dataType DEFAULT $default) " +
        s"RETURNS $returns RETURN $body")
      f { arguments =>
        val call = s"captured_default_fn($arguments)"
        sql(if (kind == "scalar") s"SELECT $call" else s"SELECT value FROM $call")
      }
    }
  }

  for {
    kind <- Seq("view", "function")
    format <- Seq("json", "csv")
    creatorValue <- Seq(true, false)
  } {
    test(s"$kind captures to_$format defaults: $creatorValue, with explicit option precedence") {
      val (key, option) = if (format == "json") {
        (SQLConf.JSON_GENERATOR_IGNORE_NULL_FIELDS.key, "IgNoReNuLlFiElDs")
      } else {
        (SQLConf.LEGACY_NULL_VALUE_WRITTEN_AS_QUOTED_EMPTY_STRING_CSV.key,
          "TrEaTnUlLaSeMpTyStRiNg")
      }
      val input = "named_struct('id', id, 'n', CAST(NULL AS INT))"
      val body = s"""named_struct(
                    |  'configured', to_$format($input),
                    |  'enabled', to_$format($input, map('$option', 'true')),
                    |  'disabled', to_$format($input, map('$option', 'false')))""".stripMargin
      def rendered(id: Int, enabled: Boolean): String = {
        if (format == "json") {
          if (enabled) s"""{"id":$id}""" else s"""{"id":$id,"n":null}"""
        } else {
          if (enabled) s"$id,\"\"" else s"$id,"
        }
      }
      val expected = (1 to 2).map { id =>
        Row(Row(rendered(id, creatorValue), rendered(id, true), rendered(id, false)))
      }
      val returnType = "STRUCT<configured: STRING, enabled: STRING, disabled: STRING>"
      withSQLConf(key -> creatorValue.toString) {
        withDefinition(kind, body, returnType) { query =>
          // Analyze a new query for each reader, rather than reuse the creator's DataFrame.
          Seq(creatorValue, !creatorValue).foreach { readerValue =>
            withSQLConf(key -> readerValue.toString) {
              checkAnswer(sql(query), expected)
            }
          }
        }
      }
    }
  }

  for {
    kind <- Seq("view", "function")
    creatorValue <- Seq(None, Some(true), Some(false))
    customDateFormat <- Seq(true, false)
    customTimestampFormat <- Seq(true, false)
    policy <- (if (customDateFormat || customTimestampFormat) {
      Seq("CORRECTED", "LEGACY")
    } else {
      Seq("CORRECTED")
    })
  } {
    test(s"$kind captures from_json fallback $creatorValue, " +
      s"custom date: $customDateFormat, timestamp: $customTimestampFormat, policy: $policy") {
      // Without a custom date format, unset fallback accepts legacy epoch-day strings.
      // This distinguishes unset (enabled by default) from an explicit false.
      val dateInput = if (customDateFormat) "2024-01-02" else "1"
      val input = s"""concat('{"id":', id, ',"d":"$dateInput","t":"2024-01-02"}')"""
      val schema = "id BIGINT, d DATE, t TIMESTAMP"
      val formats = (if (customDateFormat) Seq("'dateFormat'", "'yyyy/MM/dd'") else Nil) ++
        (if (customTimestampFormat) {
          Seq("'timestampFormat'", "'yyyy/MM/dd HH:mm:ss'")
        } else Nil)
      def parsed(explicit: Option[Boolean]): String = {
        val options = formats ++ explicit.toSeq.flatMap { enabled =>
          Seq("'EnAbLeDaTeTiMePaRsInGfAlLbAcK'", s"'$enabled'")
        }
        val argument = if (options.isEmpty) "" else s", map(${options.mkString(", ")})"
        s"from_json($input, '$schema'$argument)"
      }
      val body = s"""named_struct(
                    |  'configured', ${parsed(None)},
                    |  'enabled', ${parsed(Some(true))},
                    |  'disabled', ${parsed(Some(false))})""".stripMargin
      val parsedType = "STRUCT<id: BIGINT, d: DATE, t: TIMESTAMP>"
      val returnType =
        s"STRUCT<configured: $parsedType, enabled: $parsedType, disabled: $parsedType>"
      def result(id: Int, dateFallback: Boolean, timestampFallback: Boolean): Row = {
        val date = if (dateFallback) {
          Date.valueOf(if (customDateFormat) "2024-01-02" else "1970-01-02")
        } else {
          null
        }
        val timestamp = if ((!customTimestampFormat && policy != "LEGACY") || timestampFallback) {
          Timestamp.from(Instant.parse("2024-01-02T00:00:00Z"))
        } else {
          null
        }
        Row(id.toLong, date, timestamp)
      }
      val expected = (1 to 2).map { id =>
        val dateFallback = creatorValue.getOrElse(policy == "LEGACY" || !customDateFormat)
        val timestampFallback =
          creatorValue.getOrElse(policy == "LEGACY" || !customTimestampFormat)
        Row(Row(result(id, dateFallback, timestampFallback),
          result(id, true, true), result(id, false, false)))
      }
      withSQLConf(
          SQLConf.LEGACY_TIME_PARSER_POLICY.key -> policy,
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
        withFallbackConf(creatorValue) {
          withDefinition(kind, body, returnType) { query =>
            Seq(None, Some(true), Some(false)).foreach { readerValue =>
              withFallbackConf(readerValue) {
                checkAnswer(sql(query), expected)
              }
            }
          }
        }
      }
    }
  }

  test("omitted defaults preserve generated function column names") {
    withUserDefinedFunction("named_args_fn" -> false) {
      sql("CREATE FUNCTION named_args_fn(a BIGINT DEFAULT 11, b BIGINT DEFAULT 22) " +
        "RETURNS BIGINT RETURN a + b")
      val queries = Seq(
        "SELECT named_args_fn()",
        "SELECT named_args_fn(id) FROM range(3)",
        "SELECT named_args_fn(sum(id)) FROM range(3)",
        "SELECT named_args_fn(sum(id)) FROM range(3) GROUP BY id % 2",
        "SELECT named_args_fn(id), count(*) FROM range(3) GROUP BY id",
        "SELECT named_args_fn(id), count(*) FROM range(3) GROUP BY named_args_fn(id)")
      queries.foreach { query =>
        val expected = withSQLConf(captureKey -> "false") { sql(query).columns.toSeq }
        withSQLConf(captureKey -> "true") {
          assert(sql(query).columns.toSeq == expected, query)
        }
      }
    }
  }

  test("named omitted defaults preserve released column names and outer references") {
    withSQLConf(SQLConf.STABLE_DERIVED_COLUMN_ALIAS_ENABLED.key -> "false") {
      withUserDefinedFunction("named_args_fn" -> false) {
        sql("CREATE FUNCTION named_args_fn(a INT DEFAULT 1, b INT DEFAULT 2) " +
          "RETURNS INT RETURN a + b")
        for (capture <- Seq(true, false)) {
          withSQLConf(captureKey -> capture.toString) {
            for ((args, suffix) <- Seq("b => 3" -> "named_args_fn(1, 3)",
                "a => 3" -> "named_args_fn(3, 2)")) {
              val name = s"spark_catalog.default.$suffix"
              val query = s"SELECT named_args_fn($args)"
              assert(sql(query).columns.toSeq == Seq(name))
              val expected = if (args == "b => 3") 4 else 5
              checkAnswer(sql(s"SELECT `$name` FROM ($query)"), Row(expected))
            }
          }
        }
      }
    }
  }

  test("omitted default column names survive function expression copies") {
    withSQLConf(captureKey -> "true") {
      withUserDefinedFunction("named_args_fn" -> false) {
        sql("CREATE FUNCTION named_args_fn(a BIGINT, b BIGINT DEFAULT 22) " +
          "RETURNS BIGINT RETURN a + b")
        val calls = Seq(Seq(Literal(3L)), Seq(NamedArgumentExpression("a", Literal(3L))))
          .map { inputs =>
            spark.sessionState.catalog.lookupFunction(
              FunctionIdentifier("named_args_fn"), inputs).asInstanceOf[SQLFunctionExpression]
          }
        calls.foreach { call =>
          // Leave the aggregate unresolved so the rule defers expansion to a later iteration.
          val plan = Aggregate(Seq(UnresolvedAttribute("missing")),
            Seq(Alias(call, "value")()), OneRowRelation())
          val deferred = spark.sessionState.analyzer.ResolveSQLFunctions(plan)
            .expressions.flatMap(_.collect { case f: SQLFunctionExpression => f }).head
          val serialized = Utils.deserialize[SQLFunctionExpression](Utils.serialize(deferred))
          Seq(deferred.copy(), serialized).foreach { copied =>
            assert(copied.sql == deferred.sql)
            assert(copied.toString == deferred.toString)
            assert(copied.semanticEquals(deferred))
          }
        }
      }
    }
  }

  test("named omitted string defaults preserve pretty names and SQL quoting") {
    withSQLConf(SQLConf.STABLE_DERIVED_COLUMN_ALIAS_ENABLED.key -> "false") {
      withUserDefinedFunction("named_args_fn" -> false) {
        sql("CREATE FUNCTION named_args_fn(a STRING DEFAULT 'hello', b INT DEFAULT 2) " +
          "RETURNS STRING RETURN concat(a, CAST(b AS STRING))")
        val query = "SELECT named_args_fn(b => 3)"
        val name = withSQLConf(captureKey -> "false") { sql(query).columns.head }
        withSQLConf(captureKey -> "true") {
          assert(sql(query).columns.toSeq == Seq(name))
          checkAnswer(sql(s"SELECT `$name` FROM ($query)"), Row("hello3"))
          val call = spark.sessionState.catalog.lookupFunction(FunctionIdentifier("named_args_fn"),
            Seq(NamedArgumentExpression("b", Literal(3))))
          checkAnswer(sql(s"SELECT ${call.sql}"), Row("hello3"))
        }
      }
    }
  }

  test("named default dependencies preserve qualified generated column names") {
    withSQLConf(SQLConf.STABLE_DERIVED_COLUMN_ALIAS_ENABLED.key -> "false") {
      withUserDefinedFunction("default_dep" -> false, "named_args_fn" -> false) {
        sql("CREATE FUNCTION default_dep() RETURNS STRING RETURN 'hello'")
        sql("CREATE FUNCTION named_args_fn(a STRING DEFAULT default_dep(), b INT DEFAULT 2) " +
          "RETURNS STRING RETURN concat(a, CAST(b AS STRING))")
        val query = "SELECT named_args_fn(b => 3)"
        val name = withSQLConf(captureKey -> "false") { sql(query).columns.head }
        withSQLConf(captureKey -> "true") {
          assert(sql(query).columns.toSeq == Seq(name))
          checkAnswer(sql(s"SELECT `$name` FROM ($query)"), Row("hello3"))
        }
      }
    }
  }

  test("incompatible grouping inputs avoid comparison-only default analysis") {
    withUserDefinedFunction("named_args_fn" -> false) {
      sql("CREATE FUNCTION named_args_fn(a BIGINT, b STRING DEFAULT 'x') " +
        "RETURNS BIGINT RETURN a")
      def analyze(capture: Boolean): Long = withSQLConf(captureKey -> capture.toString) {
        val plan = spark.sessionState.sqlParser.parsePlan(
          "SELECT sum(named_args_fn(id)), sum(named_args_fn(id + 1)) " +
            "FROM range(3) GROUP BY named_args_fn(id % 2), named_args_fn(id % 3)")
        val tracker = new QueryPlanningTracker
        QueryPlanningTracker.withTracker(tracker) {
          spark.sessionState.analyzer.execute(plan)
        }
        val invocations = tracker.rules.valuesIterator.map(_.numInvocations).sum
        assert(invocations > 0)
        invocations
      }
      assert(analyze(capture = true) == analyze(capture = false))
    }
  }

  for (capture <- Seq(true, false); caller <- Seq(true, false); reverse <- Seq(true, false)) {
    test(s"completed grouping comparisons keep captured conversion defaults: " +
        s"$capture, $caller, $reverse") {
      withSQLConf(captureKey -> capture.toString,
          SQLConf.JSON_GENERATOR_IGNORE_NULL_FIELDS.key -> "false") {
        withUserDefinedFunction("named_args_fn" -> false) {
          val conversion = "to_json(named_struct('n', CAST(NULL AS INT)))"
          sql(s"CREATE FUNCTION named_args_fn(a BIGINT, b STRING DEFAULT $conversion) " +
            "RETURNS STRING RETURN concat(CAST(a AS STRING), b)")
          withSQLConf(SQLConf.JSON_GENERATOR_IGNORE_NULL_FIELDS.key -> caller.toString) {
            val omitted = "named_args_fn(id)"
            val explicit = s"named_args_fn(id, CAST($conversion AS STRING))"
            val (selected, grouped) = if (reverse) (omitted, explicit) else (explicit, omitted)
            val query = s"SELECT $selected, count(*) FROM range(3) GROUP BY $grouped"
            if (capture && !caller) {
              checkAnswer(sql(query), (0L to 2L).map(id => Row(s"""$id{"n":null}""", 1L)))
            } else {
              val error = intercept[AnalysisException] { sql(query) }
              assert(error.getCondition == "MISSING_AGGREGATION")
            }
          }
        }
      }
    }
  }

  for (capture <- Seq(true, false); replaceSaved <- Seq(true, false);
      supplied <- Seq(true, false) if capture || !supplied) {
    test("table omitted default dependencies keep their saved path: " +
        s"$capture, $replaceSaved, $supplied") {
      withSQLConf(captureKey -> "true", SQLConf.PATH_ENABLED.key -> "true") {
        withDatabase("cycle_path_a", "cycle_path_b") {
          sql("CREATE DATABASE cycle_path_a")
          sql("CREATE DATABASE cycle_path_b")
          withUserDefinedFunction("cycle_path_a.named_args_fn" -> false,
              "cycle_path_a.default_dep" -> false, "cycle_path_b.default_dep" -> false) {
            sql("CREATE FUNCTION cycle_path_a.default_dep() RETURNS INT RETURN 11")
            sql("CREATE FUNCTION cycle_path_b.default_dep() RETURNS INT RETURN 22")
            try {
              sql("USE cycle_path_a")
              sql("SET PATH = spark_catalog.cycle_path_a, system.builtin")
              sql("CREATE FUNCTION cycle_path_a.named_args_fn(" +
                "x INT DEFAULT default_dep(), y INT DEFAULT 2) " +
                "RETURNS TABLE(v INT) RETURN SELECT x + y AS v")
              sql("SET PATH = spark_catalog.cycle_path_b, system.builtin")
              withSQLConf(captureKey -> capture.toString) {
                val target = if (replaceSaved) "cycle_path_a" else "cycle_path_b"
                val args = if (supplied) "x => default_dep(), y => 3" else "y => 3"
                val replacement = s"CREATE OR REPLACE FUNCTION $target.default_dep() " +
                  s"RETURNS INT RETURN (SELECT v FROM cycle_path_a.named_args_fn($args))"
                val cyclic = if (capture && supplied) !replaceSaved else replaceSaved
                if (cyclic) {
                  val error = intercept[AnalysisException] { sql(replacement) }
                  assert(error.getCondition.contains("CYCLIC_FUNCTION_REFERENCE"))
                } else {
                  sql(replacement)
                  val expected = if (capture && supplied) 25 else 14
                  checkAnswer(sql(s"SELECT $target.default_dep()"), Row(expected))
                }
              }
            } finally {
              sql("USE default")
              sql("SET PATH = DEFAULT_PATH")
            }
          }
        }
      }
    }
  }

  for (capture <- Seq(true, false); clause <- Seq("filter", "within group")) {
    test(s"cycle validation visits aggregate $clause dependencies: $capture") {
      withSQLConf(captureKey -> capture.toString) {
        withUserDefinedFunction("cycle_target" -> false, "cycle_caller" -> false) {
          sql("CREATE FUNCTION cycle_target() RETURNS INT RETURN 1")
          val aggregate = if (clause == "filter") {
            "count(*) FILTER (WHERE cycle_target() > 0)"
          } else {
            "percentile_cont(0.5) WITHIN GROUP (ORDER BY cycle_target())"
          }
          sql(s"CREATE FUNCTION cycle_caller() RETURNS INT RETURN " +
            s"(SELECT $aggregate FROM range(3))")
          val error = intercept[AnalysisException] {
            sql("CREATE OR REPLACE FUNCTION cycle_target() RETURNS INT RETURN cycle_caller()")
          }
          assert(error.getCondition.contains("CYCLIC_FUNCTION_REFERENCE"))
          checkAnswer(sql("SELECT cycle_target()"), Row(1))
        }
      }
    }
  }

  for ((kind, dataType, default) <- Seq(
      ("numeric", "BIGINT", "1"),
      ("true boolean", "BOOLEAN", "true"),
      ("false boolean", "BOOLEAN", "false"),
      ("date", "DATE", "DATE '2025-01-02'"),
      ("binary", "BINARY", "X'CAFE'"),
      ("timestamp", "TIMESTAMP", "TIMESTAMP '2025-01-02 03:04:05'"),
      ("timestamp_ntz", "TIMESTAMP_NTZ", "TIMESTAMP_NTZ '2025-01-02 03:04:05'"))) {
    test(s"$kind named defaults avoid a nested analyzer pipeline") {
      withSQLConf(captureKey -> "true") {
        withUserDefinedFunction("named_args_fn" -> false) {
          sql(s"CREATE FUNCTION named_args_fn(a BIGINT, b $dataType DEFAULT $default) " +
            s"RETURNS $dataType RETURN b")
          val plan = spark.sessionState.sqlParser.parsePlan("SELECT " +
            Seq.fill(8)("named_args_fn(a => CAST(0 AS BIGINT))").mkString(", "))
          val tracker = new QueryPlanningTracker
          val resolved = QueryPlanningTracker.withTracker(tracker) {
            spark.sessionState.analyzer.ResolveFunctions(plan)
          }
          val functions = resolved.expressions.flatMap(_.collect {
            case f: SQLFunctionExpression => f
          })
          assert(functions.size == 8)
          assert(functions.forall(f =>
            f.defaultInputs.nonEmpty && f.defaultInputs.values.forall(_.resolved)))
          assert(tracker.rules.isEmpty)
        }
      }
    }
  }

  for (capture <- Seq(true, false)) {
    test(s"named default cycle validation follows table dependency paths: $capture") {
      withSQLConf(captureKey -> "true", SQLConf.PATH_ENABLED.key -> "true") {
        withDatabase("cycle_path_a", "cycle_path_b") {
          sql("CREATE DATABASE cycle_path_a")
          sql("CREATE DATABASE cycle_path_b")
          withUserDefinedFunction("cycle_path_a.named_args_fn" -> false,
              "cycle_path_a.default_dep" -> false, "cycle_path_b.default_dep" -> false,
              "cycle_path_a.dep_tvf" -> false, "cycle_path_b.dep_tvf" -> false) {
            sql("CREATE FUNCTION cycle_path_a.dep_tvf() RETURNS TABLE(v INT) RETURN SELECT 11")
            sql("CREATE FUNCTION cycle_path_b.dep_tvf() RETURNS TABLE(v INT) RETURN SELECT 22")
            sql("CREATE FUNCTION cycle_path_b.default_dep() RETURNS INT RETURN 22")
            try {
              sql("USE cycle_path_a")
              sql("SET PATH = spark_catalog.cycle_path_a, system.builtin")
              sql("CREATE FUNCTION cycle_path_a.default_dep() RETURNS INT " +
                "RETURN (SELECT v FROM dep_tvf())")
              sql("CREATE FUNCTION cycle_path_a.named_args_fn(" +
                "x INT DEFAULT default_dep(), y INT DEFAULT 2) RETURNS INT RETURN x + y")
              sql("USE cycle_path_b")
              sql("SET PATH = spark_catalog.cycle_path_b, system.builtin")
              withSQLConf(captureKey -> capture.toString) {
                val replacement = "CREATE OR REPLACE FUNCTION cycle_path_a.dep_tvf() " +
                  "RETURNS TABLE(v INT) RETURN SELECT cycle_path_a.named_args_fn(y => 3) AS v"
                if (capture) {
                  val error = intercept[AnalysisException] { sql(replacement) }
                  assert(error.getCondition.contains("CYCLIC_FUNCTION_REFERENCE"))
                } else {
                  sql(replacement)
                  checkAnswer(sql("SELECT * FROM cycle_path_a.dep_tvf()"), Row(25))
                }
              }
            } finally {
              sql("USE default")
              sql("SET PATH = DEFAULT_PATH")
            }
          }
        }
      }
    }

    test(s"named random defaults retain table argument evaluation boundaries: $capture") {
      withSQLConf(captureKey -> capture.toString) {
        withUserDefinedFunction("named_args_fn" -> false, "repeat_arg" -> false) {
          sql("CREATE FUNCTION named_args_fn(a INT, b DOUBLE DEFAULT rand(0)) " +
            "RETURNS DOUBLE RETURN b")
          sql("CREATE FUNCTION repeat_arg(x DOUBLE) RETURNS TABLE(v DOUBLE) " +
            "RETURN SELECT x AS v FROM range(10)")
          val expected = Seq.fill(10)(sql("SELECT rand(0)").head())
          checkAnswer(sql("SELECT * FROM repeat_arg(named_args_fn(a => 1, b => rand(0)))"),
            expected)
          checkAnswer(sql("SELECT * FROM repeat_arg(named_args_fn(a => 1))"), expected)
        }
      }
    }

    for (reverse <- Seq(true, false)) {
      test(s"positional defaults complete grouping comparisons: $capture, $reverse") {
        withSQLConf(captureKey -> capture.toString) {
          withUserDefinedFunction("named_args_fn" -> false) {
            sql("CREATE FUNCTION named_args_fn(a BIGINT, b BIGINT DEFAULT 1) " +
              "RETURNS BIGINT RETURN a + b")
            val omitted = "named_args_fn(id)"
            val explicit = "named_args_fn(id, CAST(1 AS BIGINT))"
            val (selected, grouped) = if (reverse) (omitted, explicit) else (explicit, omitted)
            val query = s"SELECT $selected, count(*) FROM range(3) GROUP BY $grouped"
            if (capture) {
              checkAnswer(sql(query), (0L to 2L).map(id => Row(id + 1L, 1L)))
            } else {
              val error = intercept[AnalysisException] { sql(query) }
              assert(error.getCondition == "MISSING_AGGREGATION")
            }
          }
        }
      }
    }

    for (replaceSaved <- Seq(true, false); supplied <- Seq(true, false)) {
      test("named default cycle validation follows the effective path: " +
          s"$capture, $replaceSaved, $supplied") {
        withSQLConf(captureKey -> "true", SQLConf.PATH_ENABLED.key -> "true") {
          withDatabase("cycle_path_a", "cycle_path_b") {
            sql("CREATE DATABASE cycle_path_a")
            sql("CREATE DATABASE cycle_path_b")
            withUserDefinedFunction("cycle_path_a.named_args_fn" -> false,
                "cycle_path_a.default_dep" -> false, "cycle_path_b.default_dep" -> false) {
              sql("CREATE FUNCTION cycle_path_a.default_dep() RETURNS INT RETURN 11")
              sql("CREATE FUNCTION cycle_path_b.default_dep() RETURNS INT RETURN 22")
              try {
                sql("USE cycle_path_a")
                sql("SET PATH = spark_catalog.cycle_path_a, system.builtin")
                sql("CREATE FUNCTION cycle_path_a.named_args_fn(" +
                  "x INT DEFAULT default_dep(), y INT DEFAULT 2) RETURNS INT RETURN x + y")
                sql("USE cycle_path_b")
                sql("SET PATH = spark_catalog.cycle_path_b, system.builtin")
                withSQLConf(captureKey -> capture.toString) {
                  val target = if (replaceSaved) "cycle_path_a" else "cycle_path_b"
                  val args = if (supplied) "x => default_dep(), y => 3" else "y => 3"
                  val replacement = s"CREATE OR REPLACE FUNCTION $target.default_dep() " +
                    s"RETURNS INT RETURN cycle_path_a.named_args_fn($args)"
                  val cyclic = if (supplied) !replaceSaved else capture == replaceSaved
                  if (cyclic) {
                    val error = intercept[AnalysisException] { sql(replacement) }
                    assert(error.getCondition.contains("CYCLIC_FUNCTION_REFERENCE"))
                  } else {
                    sql(replacement)
                    val expected = if (capture && !supplied) 14 else 25
                    checkAnswer(sql(s"SELECT $target.default_dep()"), Row(expected))
                  }
                }
              } finally {
                sql("USE default")
                sql("SET PATH = DEFAULT_PATH")
              }
            }
          }
        }
      }
    }

    test(s"named random defaults preserve independent grouping keys: $capture") {
      withSQLConf(captureKey -> capture.toString) {
        withUserDefinedFunction("named_args_fn" -> false) {
          sql("CREATE FUNCTION named_args_fn(a INT, " +
            "b INT DEFAULT CAST(rand() * 2 AS INT)) RETURNS INT RETURN b")
          val query = sql("SELECT count(*) FROM range(128) " +
            "GROUP BY named_args_fn(a => 0), named_args_fn(a => 0)")
          val grouping = query.queryExecution.analyzed.collect {
            case a: Aggregate => a.groupingExpressions
          }.head
          assert(grouping.map(_.canonicalized).distinct.size == 2)
          assert(query.collect().map(_.getLong(0)).sum == 128L)
        }
      }
    }

    test(s"unequal omitted and explicit defaults cannot share a grouping key: $capture") {
      withSQLConf(captureKey -> capture.toString) {
        withUserDefinedFunction("named_args_fn" -> false) {
          sql("CREATE FUNCTION named_args_fn(a BIGINT, b BIGINT DEFAULT 1) " +
            "RETURNS BIGINT RETURN a + b")
          val omitted = "named_args_fn(a => id)"
          val explicit = "named_args_fn(id, CAST(2 AS BIGINT))"
          Seq((explicit, omitted), (omitted, explicit)).foreach { case (selected, grouped) =>
            val error = intercept[AnalysisException] {
              sql(s"SELECT $selected, count(*) FROM range(3) GROUP BY $grouped")
            }
            assert(error.getCondition == "MISSING_AGGREGATION")
          }
        }
      }
    }

    test(s"named default dependencies distinguish saved and caller SQL paths: $capture") {
      withSQLConf(captureKey -> capture.toString, SQLConf.PATH_ENABLED.key -> "true") {
        withDatabase("path_defaults_a", "path_defaults_b") {
          sql("CREATE DATABASE path_defaults_a")
          sql("CREATE DATABASE path_defaults_b")
          withUserDefinedFunction("path_defaults_a.named_args_fn" -> false,
              "path_defaults_a.default_dep" -> false, "path_defaults_b.default_dep" -> false) {
            sql("CREATE FUNCTION path_defaults_a.default_dep() RETURNS BIGINT RETURN 11")
            sql("CREATE FUNCTION path_defaults_b.default_dep() RETURNS BIGINT RETURN 22")
            try {
              sql("USE path_defaults_a")
              sql("SET PATH = spark_catalog.path_defaults_a, system.builtin")
              sql("CREATE FUNCTION path_defaults_a.named_args_fn(" +
                "a BIGINT DEFAULT default_dep(), " +
                "b BIGINT DEFAULT 2) RETURNS BIGINT RETURN a + b")
              sql("USE default")
              sql("SET PATH = spark_catalog.path_defaults_b, system.builtin")
              checkAnswer(sql("SELECT default_dep()"), Row(22L))
              val expected = if (capture) 14L else 25L
              checkAnswer(sql("SELECT path_defaults_a.named_args_fn(b => CAST(3 AS BIGINT))"),
                Row(expected))
            } finally {
              sql("USE default")
              sql("SET PATH = DEFAULT_PATH")
            }
          }
        }
      }
    }

    test(s"an unrelated aggregate preserves a closed lambda in a positional default: $capture") {
      withSQLConf(captureKey -> capture.toString) {
        withUserDefinedFunction("named_args_fn" -> false) {
          sql("CREATE FUNCTION named_args_fn(a BIGINT, " +
            "b ARRAY<INT> DEFAULT transform(array(1), x -> x + 1)) " +
            "RETURNS BIGINT RETURN a")
          checkAnswer(sql("SELECT named_args_fn(id) " +
            "FROM (SELECT max(id) AS id FROM range(3))"), Row(2L))
          checkAnswer(sql("SELECT named_args_fn(id), count(*) FROM range(3) " +
            "GROUP BY named_args_fn(id)"), (0L to 2L).map(id => Row(id, 1L)))
          val error = intercept[AnalysisException] {
            sql("SELECT named_args_fn(a => CAST(2 AS BIGINT))")
          }
          assert(error.getCondition == "UNSUPPORTED_FEATURE.LAMBDA_FUNCTION_WITH_SQL_UDF")
        }
      }
    }

    test(s"grouped positional calls preserve identity with an unused random default: $capture") {
      withSQLConf(captureKey -> capture.toString) {
        withUserDefinedFunction("named_args_fn" -> false) {
          sql("CREATE FUNCTION named_args_fn(a BIGINT, b DOUBLE DEFAULT rand()) " +
            "RETURNS BIGINT RETURN a")
          checkAnswer(sql("SELECT named_args_fn(id), count(*) FROM range(3) " +
            "GROUP BY named_args_fn(id)"), (0L to 2L).map(id => Row(id, 1L)))
        }
      }
    }

    for (sparse <- Seq(true, false)) {
      test(s"named defaults match explicit grouping defaults: $capture, $sparse") {
        withSQLConf(captureKey -> capture.toString) {
          withUserDefinedFunction("named_args_fn" -> false) {
            val parameters = if (sparse) {
              "a BIGINT DEFAULT 1, b BIGINT DEFAULT 2, c BIGINT DEFAULT 3, d BIGINT DEFAULT 4"
            } else {
              "a BIGINT, b BIGINT DEFAULT 1"
            }
            val body = if (sparse) "a + b + c + d" else "a + b"
            sql(s"CREATE FUNCTION named_args_fn($parameters) RETURNS BIGINT RETURN $body")
            val supplied = if (sparse) {
              "d => id, b => CAST(7 AS BIGINT), a => CAST(1 AS BIGINT), c => CAST(3 AS BIGINT)"
            } else {
              "a => id, b => CAST(1 AS BIGINT)"
            }
            val omitted = if (sparse) "b => CAST(7 AS BIGINT), d => id" else "a => id"
            val offset = if (sparse) 11L else 1L
            checkAnswer(sql(s"SELECT named_args_fn($supplied), count(*) FROM range(3) " +
              s"GROUP BY named_args_fn($omitted)"), (0L to 2L).map(id => Row(id + offset, 1L)))
          }
        }
      }
    }

    test(s"named omitted defaults preserve multiple supplied argument associations: $capture") {
      withSQLConf(captureKey -> capture.toString) {
        withUserDefinedFunction("named_args_fn" -> false) {
          sql("CREATE FUNCTION named_args_fn(a BIGINT DEFAULT 11, b BIGINT DEFAULT 22, " +
            "c BIGINT DEFAULT 33, d BIGINT DEFAULT 44, e BIGINT DEFAULT 55) " +
            "RETURNS STRUCT<a: BIGINT, b: BIGINT, c: BIGINT, d: BIGINT, e: BIGINT> " +
            "RETURN named_struct('a', a, 'b', b, 'c', c, 'd', d, 'e', e)")
          val query = "SELECT named_args_fn(e => id + 200, b => id + 100, d => id + 300) " +
            "FROM range(3)"
          val expected = (0L to 2L).map(id => Row(Row(11L, id + 100, 33L,
            id + 300, id + 200)))
          checkAnswer(sql(query), expected)
          checkAnswer(sql(query + " GROUP BY id"), expected)
          checkAnswer(sql("SELECT named_args_fn(e => 8L, b => CAST(NULL AS BIGINT), d => 9L)"),
            Row(Row(11L, null, 33L, 9L, 8L)))
        }
      }
    }

    for (creator <- Seq(true, false)) {
      test("named omitted defaults mix saved and supplied conversion settings: " +
          s"$capture, $creator") {
        val jsonKey = SQLConf.JSON_GENERATOR_IGNORE_NULL_FIELDS.key
        val csvKey = SQLConf.LEGACY_NULL_VALUE_WRITTEN_AS_QUOTED_EMPTY_STRING_CSV.key
        val input = "named_struct('id', 1, 'n', CAST(NULL AS INT))"
        withSQLConf(captureKey -> "true", jsonKey -> creator.toString,
            csvKey -> creator.toString) {
          withUserDefinedFunction("named_args_fn" -> false) {
            sql(s"CREATE FUNCTION named_args_fn(a STRING DEFAULT to_json($input), " +
              s"b STRING DEFAULT '', c STRING DEFAULT to_csv($input), d STRING DEFAULT '') " +
              "RETURNS STRUCT<a: STRING, b: STRING, c: STRING, d: STRING> " +
              "RETURN named_struct('a', a, 'b', b, 'c', c, 'd', d)")
            withSQLConf(captureKey -> capture.toString, jsonKey -> (!creator).toString,
                csvKey -> (!creator).toString) {
              def json(value: Boolean): String =
                if (value) """{"id":1}""" else """{"id":1,"n":null}"""
              def csv(value: Boolean): String = if (value) "1,\"\"" else "1,"
              val saved = if (capture) creator else !creator
              val call = s"named_args_fn(d => to_csv($input), b => to_json($input))"
              val expected = Row(Row(json(saved), json(!creator), csv(saved), csv(!creator)))
              checkAnswer(sql(s"SELECT $call"), expected)
              checkAnswer(sql(s"SELECT $call FROM range(1) GROUP BY id"), expected)
            }
          }
        }
      }
    }

    test(s"named omitted defaults resolve SQL function dependencies and cycles: $capture") {
      withSQLConf(captureKey -> capture.toString) {
        withUserDefinedFunction("default_dep" -> false, "named_args_fn" -> false) {
          sql("CREATE FUNCTION default_dep() RETURNS BIGINT RETURN 1")
          sql("CREATE FUNCTION named_args_fn(a BIGINT DEFAULT default_dep(), " +
            "b BIGINT DEFAULT 2) RETURNS BIGINT RETURN a + b")
          checkAnswer(sql("SELECT named_args_fn(b => 3)"), Row(4L))
          val error = intercept[AnalysisException] {
            sql("CREATE OR REPLACE FUNCTION default_dep() RETURNS BIGINT " +
              "RETURN named_args_fn(b => 3)")
          }
          assert(error.getCondition.contains("CYCLIC_FUNCTION_REFERENCE"))
        }
      }
    }

    test(s"named scalar arguments normalize before aggregate extraction: $capture") {
      withSQLConf(captureKey -> capture.toString) {
        withUserDefinedFunction("named_args_fn" -> false) {
          sql("CREATE FUNCTION named_args_fn(a BIGINT) RETURNS BIGINT RETURN a")
          checkAnswer(sql("SELECT named_args_fn(a => sum(id)) FROM range(3)"), Row(3L))
        }
      }
    }

    for (reordered <- Seq(true, false)) {
      test(s"named scalar arguments normalize grouping keys: $capture, $reordered") {
        withSQLConf(captureKey -> capture.toString) {
          withUserDefinedFunction("named_args_fn" -> false) {
            val second = if (reordered) ", b BIGINT" else ""
            val body = if (reordered) "a + b" else "a + 1"
            sql(s"CREATE FUNCTION named_args_fn(a BIGINT$second) RETURNS BIGINT RETURN $body")
            val positional = if (reordered) "id, 1" else "id"
            val named = if (reordered) "b => 1, a => id" else "a => id"
            checkAnswer(sql(s"SELECT named_args_fn($positional), count(*) FROM range(3) " +
              s"GROUP BY named_args_fn($named)"), Seq(Row(1L, 1L), Row(2L, 1L), Row(3L, 1L)))
          }
        }
      }
    }

    test(s"named scalar calls preserve nontrailing omitted defaults: $capture") {
      val key = SQLConf.JSON_GENERATOR_IGNORE_NULL_FIELDS.key
      withSQLConf(captureKey -> "true", key -> "false") {
        withUserDefinedFunction("named_args_fn" -> false) {
          sql("CREATE FUNCTION named_args_fn(" +
            "a STRING DEFAULT to_json(named_struct('n', CAST(NULL AS INT))), " +
            "b BIGINT DEFAULT 1) RETURNS STRING RETURN concat(a, CAST(b AS STRING))")
          withSQLConf(captureKey -> capture.toString, key -> "true") {
            val json = if (capture) """{"n":null}""" else "{}"
            checkAnswer(sql("SELECT named_args_fn(b => sum(id)) FROM range(3)"), Row(json + "3"))
            checkAnswer(sql("SELECT named_args_fn(b => id), count(*) FROM range(3) " +
              "GROUP BY named_args_fn(b => id)"),
              (0 to 2).map(id => Row(json + id, 1L)))
          }
        }
      }
    }

    test(s"named scalar default parsing honors capture rollback: $capture") {
      withSQLConf(captureKey -> "true", SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
        withDefaultFunction("scalar", "TIMESTAMP_LTZ '2024-01-02 00:00:00'",
            "TIMESTAMP_LTZ") { query =>
          withSQLConf(captureKey -> capture.toString,
              SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
            val saved = Timestamp.from(Instant.parse("2024-01-02T00:00:00Z"))
            val caller = Timestamp.from(Instant.parse("2024-01-02T08:00:00Z"))
            checkAnswer(query("1"), Row(saved))
            checkAnswer(query("a => 1"), Row(if (capture) saved else caller))
            checkAnswer(query("s => TIMESTAMP_LTZ '2024-01-02 00:00:00', a => 1"), Row(caller))
          }
        }
      }
    }
  }

  test("named function defaults retain the caller's parameter-name matching") {
    val key = SQLConf.JSON_GENERATOR_IGNORE_NULL_FIELDS.key
    val default = "to_json(named_struct('n', CAST(NULL AS INT)))"
    for (savedCaseSensitive <- Seq(true, false)) {
      withSQLConf(captureKey -> "true", key -> "false",
          SQLConf.CASE_SENSITIVE.key -> savedCaseSensitive.toString) {
        withDefaultFunction("scalar", default, "STRING") { query =>
          withSQLConf(key -> "true",
              SQLConf.CASE_SENSITIVE.key -> (!savedCaseSensitive).toString) {
            checkAnswer(query("a => 1"), Row("""{"n":null}"""))
            if (savedCaseSensitive) {
              checkAnswer(query("A => 1"), Row("""{"n":null}"""))
            } else {
              val error = intercept[AnalysisException] { query("A => 1") }
              assert(error.getCondition == "UNRECOGNIZED_PARAMETER_NAME")
            }
          }
        }
      }
    }
  }

  for (kind <- Seq("scalar", "table"); format <- Seq("json", "csv");
      creator <- Seq(true, false)) {
    test(s"$kind function defaults capture to_$format settings for named calls: $creator") {
      val (key, option) = if (format == "json") {
        (SQLConf.JSON_GENERATOR_IGNORE_NULL_FIELDS.key, "IgNoReNuLlFiElDs")
      } else {
        (SQLConf.LEGACY_NULL_VALUE_WRITTEN_AS_QUOTED_EMPTY_STRING_CSV.key,
          "TrEaTnUlLaSeMpTyStRiNg")
      }
      val input = "named_struct('id', 1, 'n', CAST(NULL AS INT))"
      val default = s"""named_struct(
                      |'configured', to_$format($input),
                      |'enabled', to_$format($input, map('$option', 'true')),
                      |'disabled', to_$format($input, map('$option', 'false')))""".stripMargin
      val dataType = "STRUCT<configured: STRING, enabled: STRING, disabled: STRING>"
      def rendered(enabled: Boolean): String = if (format == "json") {
        if (enabled) """{"id":1}""" else """{"id":1,"n":null}"""
      } else {
        if (enabled) "1,\"\"" else "1,"
      }
      def value(enabled: Boolean): Row =
        Row(rendered(enabled), rendered(true), rendered(false))
      withSQLConf(captureKey -> "true", key -> creator.toString) {
        withDefaultFunction(kind, default, dataType) { query =>
          for (capture <- Seq(true, false); reader <- Seq(true, false)) {
            withSQLConf(captureKey -> capture.toString, key -> reader.toString) {
              val expected = value(if (capture) creator else reader)
              checkAnswer(query("1"), Row(expected))
              checkAnswer(query("a => 1"), Row(expected))
              // Supplied arguments belong to the caller, even when reordered by name.
              checkAnswer(query(s"s => $default, a => 1"), Row(value(reader)))
            }
          }
        }
      }
    }
  }

  for (kind <- Seq("scalar", "table"); creator <- Seq(None, Some(true), Some(false))) {
    test(s"$kind function defaults capture JSON fallback for named calls: $creator") {
      val input = """'{"d":"1","ts":"2024-01-02"}'"""
      val parsedType = "STRUCT<d: DATE, ts: TIMESTAMP>"
      def parsed(explicit: Option[Boolean]): String = {
        val option = explicit.map(value => s", 'EnAbLeDaTeTiMePaRsInGfAlLbAcK', '$value'")
          .getOrElse("")
        s"from_json($input, '$parsedType', " +
          s"map('timestampFormat', 'yyyy/MM/dd HH:mm:ss'$option))"
      }
      val default = s"named_struct('configured', ${parsed(None)}, " +
        s"'enabled', ${parsed(Some(true))}, 'disabled', ${parsed(Some(false))})"
      val dataType =
        s"STRUCT<configured: $parsedType, enabled: $parsedType, disabled: $parsedType>"
      def result(fallback: Option[Boolean]): Row = {
        val date = if (fallback.contains(false)) null else Date.valueOf("1970-01-02")
        val timestamp = if (fallback.contains(true)) {
          Timestamp.from(Instant.parse("2024-01-02T00:00:00Z"))
        } else null
        Row(date, timestamp)
      }
      def value(fallback: Option[Boolean]): Row =
        Row(result(fallback), result(Some(true)), result(Some(false)))
      withSQLConf(captureKey -> "true", SQLConf.LEGACY_TIME_PARSER_POLICY.key -> "CORRECTED",
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
        withFallbackConf(creator) {
          withDefaultFunction(kind, default, dataType) { query =>
            for (capture <- Seq(true, false); reader <- Seq(None, Some(true), Some(false))) {
              withSQLConf(captureKey -> capture.toString) {
                withFallbackConf(reader) {
                  val expected = value(if (capture) creator else reader)
                  checkAnswer(query("1"), Row(expected))
                  checkAnswer(query("a => 1"), Row(expected))
                  checkAnswer(query(s"s => $default, a => 1"), Row(value(reader)))
                }
              }
            }
          }
        }
      }
    }
  }

  for (format <- Seq("json", "csv")) {
    test(s"SQL functions share $format conversions with identical explicit options") {
      val key = if (format == "json") fallbackKey else {
        SQLConf.LEGACY_NULL_VALUE_WRITTEN_AS_QUOTED_EMPTY_STRING_CSV.key
      }
      val body = if (format == "json") {
        """from_json(concat('{"id":', id, '}'), 'id BIGINT, ts TIMESTAMP',
          |map('enableDateTimeParsingFallback', 'false'))""".stripMargin
      } else {
        """to_csv(named_struct('id', id, 'n', CAST(NULL AS INT)),
          |map('treatNullAsEmptyString', 'false'))""".stripMargin
      }
      val returnType = if (format == "json") "STRUCT<id: BIGINT, ts: TIMESTAMP>" else "STRING"
      withUserDefinedFunction("captured_fn_1" -> false, "captured_fn_2" -> false) {
        Seq("captured_fn_1" -> true, "captured_fn_2" -> false).foreach { case (name, value) =>
          withSQLConf(key -> value.toString) {
            sql(s"CREATE FUNCTION $name(id BIGINT) RETURNS $returnType RETURN $body")
          }
        }
        val result = sql("SELECT captured_fn_1(id), captured_fn_2(id) FROM range(1, 3)")
        val conversions = result.queryExecution.optimizedPlan.flatMap { plan =>
          plan.expressions.flatMap(_.collect {
            case j: JsonToStructs => j: Expression
            case c: StructsToCsv => c: Expression
          })
        }
        assert(conversions.size == 2)
        val equivalents = new EquivalentExpressions
        conversions.foreach(equivalents.addExprTree(_))
        assert(equivalents.getCommonSubexpressions.exists(_.semanticEquals(conversions.head)))
        val expected = (1 to 2).map { id =>
          if (format == "json") Row(Row(id.toLong, null), Row(id.toLong, null))
          else Row(s"$id,", s"$id,")
        }
        checkAnswer(result, expected)
      }
    }
  }

  for (codegen <- Seq(true, false)) {
    test(s"saved CSV functions share plain null markers with default settings: codegen=$codegen") {
      val key = SQLConf.LEGACY_NULL_VALUE_WRITTEN_AS_QUOTED_EMPTY_STRING_CSV.key
      val body = """to_csv(named_struct('id', id, 'n', CAST(NULL AS STRING)),
                   |map('nullValue', 'NULL'))""".stripMargin
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegen.toString) {
        withUserDefinedFunction("captured_fn_1" -> false, "captured_fn_2" -> false) {
          for ((name, captured) <- Seq("captured_fn_1" -> true, "captured_fn_2" -> false)) {
            withSQLConf(key -> captured.toString) {
              sql(s"CREATE FUNCTION $name(id BIGINT) RETURNS STRING RETURN $body")
            }
          }
          val result = sql("SELECT captured_fn_1(id), captured_fn_2(id) FROM range(1, 3)")
          val conversions = result.queryExecution.optimizedPlan.flatMap { plan =>
            plan.expressions.flatMap(_.collect { case c: StructsToCsv => c })
          }
          assert(conversions.size == 2)
          val equivalents = new EquivalentExpressions
          conversions.foreach(equivalents.addExprTree(_))
          assert(equivalents.getCommonSubexpressions.exists(_.semanticEquals(conversions.head)))
          checkAnswer(result, (1 to 2).map(id => Row(s"$id,NULL", s"$id,NULL")))
        }
      }
    }
  }

  test("cached CSV projections cannot substitute a different captured null-marker rendering") {
    val key = SQLConf.LEGACY_NULL_VALUE_WRITTEN_AS_QUOTED_EMPTY_STRING_CSV.key
    val query = """SELECT to_csv(named_struct('n', CAST(NULL AS STRING), 'id', CAST(id AS STRING)),
                  |map('nullValue', ' ')) AS value FROM range(1, 3)""".stripMargin
    def analyzed(value: Boolean): DataFrame = withSQLConf(key -> value.toString) {
      val df = sql(query)
      df.queryExecution.analyzed
      df
    }
    // Both captures must precede cache registration, and cache lookup uses one common live conf.
    val quoted = analyzed(true)
    val unquoted = analyzed(false)
    withSQLConf(key -> "false") {
      quoted.cache()
      try {
        quoted.count()
        checkAnswer(quoted, Seq(Row("\"\",1"), Row("\"\",2")))
        checkAnswer(unquoted, Seq(Row(",1"), Row(",2")))
      } finally {
        quoted.unpersist(blocking = true)
      }
    }
  }

  test("cached CSV custom quote interaction preserves distinct captured null markers") {
    val key = SQLConf.LEGACY_NULL_VALUE_WRITTEN_AS_QUOTED_EMPTY_STRING_CSV.key
    val query = """SELECT to_csv(named_struct('n', CAST(NULL AS STRING), 'id', CAST(id AS STRING)),
                  |map('nullValue', 'NN', 'quote', 'N')) AS value FROM range(1, 3)""".stripMargin
    def analyzed(value: Boolean): DataFrame = withSQLConf(key -> value.toString) {
      val df = sql(query)
      df.queryExecution.analyzed
      df
    }
    withSQLConf(captureKey -> "true") {
      val quoted = analyzed(true)
      val unquoted = analyzed(false)
      withSQLConf(key -> "false") {
        quoted.cache()
        try {
          quoted.count()
          val quotedPrefix = """N\N\NN,"""
          checkAnswer(quoted, (1 to 2).map(id => Row(quotedPrefix + id)))
          checkAnswer(unquoted, (1 to 2).map(id => Row(s"NN,$id")))
        } finally {
          quoted.unpersist(blocking = true)
        }
      }
    }
  }

  for (conversion <- Seq("from_json", "to_csv", "to_json_array", "to_json_map",
      "to_json_struct", "to_json_variant", "to_json_struct_key")) {
    test(s"saved functions share $conversion when captured defaults are ineffective") {
      val (key, body, returnType) = conversion match {
        case "from_json" =>
          (fallbackKey, """from_json(concat('{"id":', id, ',"d":"1"}'), 'id BIGINT, d DATE')""",
            "STRUCT<id: BIGINT, d: DATE>")
        case "to_csv" =>
          (SQLConf.LEGACY_NULL_VALUE_WRITTEN_AS_QUOTED_EMPTY_STRING_CSV.key,
            "to_csv(named_struct('id', id))", "STRING")
        case "to_json_array" =>
          (SQLConf.JSON_GENERATOR_IGNORE_NULL_FIELDS.key,
            "to_json(array(id, CAST(NULL AS BIGINT)))", "STRING")
        case "to_json_map" =>
          (SQLConf.JSON_GENERATOR_IGNORE_NULL_FIELDS.key,
            "to_json(map('id', id, 'n', CAST(NULL AS BIGINT)))", "STRING")
        case "to_json_struct" =>
          (SQLConf.JSON_GENERATOR_IGNORE_NULL_FIELDS.key,
            "to_json(named_struct('id', id))", "STRING")
        case "to_json_variant" =>
          (SQLConf.JSON_GENERATOR_IGNORE_NULL_FIELDS.key,
            """to_json(parse_json(concat('{"id":', id, ',"n":null}')))""", "STRING")
        case "to_json_struct_key" =>
          (SQLConf.JSON_GENERATOR_IGNORE_NULL_FIELDS.key,
            "to_json(map(named_struct('key', id), id))", "STRING")
      }
      withUserDefinedFunction("captured_fn_1" -> false, "captured_fn_2" -> false) {
        val firstValue = if (conversion == "from_json") None else Some(false)
        val definitions = Seq("captured_fn_1" -> firstValue, "captured_fn_2" -> Some(true))
        for ((name, value) <- definitions) {
          withBooleanConf(key, value) {
            sql(s"CREATE FUNCTION $name(id BIGINT) RETURNS $returnType RETURN $body")
          }
        }
        withSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key -> "CORRECTED") {
          val result = sql("SELECT captured_fn_1(id), captured_fn_2(id) FROM range(1, 3)")
          val conversions = result.queryExecution.optimizedPlan.flatMap { plan =>
            plan.expressions.flatMap(_.collect {
              case j: JsonToStructs => j: Expression
              case c: StructsToCsv => c: Expression
              case i: Invoke if i.functionName == "evaluate" => i: Expression
            })
          }
          assert(conversions.size == 2)
          val equivalents = new EquivalentExpressions
          conversions.foreach(equivalents.addExprTree(_))
          assert(equivalents.getCommonSubexpressions.exists(_.semanticEquals(conversions.head)))
          val expected = (1 to 2).map { id =>
            val value = conversion match {
              case "from_json" => Row(id.toLong, Date.valueOf("1970-01-02"))
              case "to_csv" => id.toString
              case "to_json_array" => s"[$id,null]"
              case "to_json_map" => s"""{"id":$id,"n":null}"""
              case "to_json_struct" => s"""{"id":$id}"""
              case "to_json_variant" => s"""{"id":$id,"n":null}"""
              case "to_json_struct_key" => s"""{"[$id]":$id}"""
            }
            Row(value, value)
          }
          checkAnswer(result, expected)
        }
      }
    }
  }

  for (dataType <- Seq("DATE", "TIMESTAMP")) {
    test(s"saved $dataType parsers share with only the other datetime type's format") {
      val format = if (dataType == "DATE") "TiMeStAmPfOrMaT" else "DaTeFoRmAt"
      val value = if (dataType == "DATE") "1" else "2024-01-02"
      val body = s"""from_json(concat('{"id":', id, ',"v":"$value"}'),
                   |'id BIGINT, v $dataType', map('$format', 'yyyy/MM/dd'))""".stripMargin
      withSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key -> "CORRECTED",
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
        withUserDefinedFunction("captured_fn_1" -> false, "captured_fn_2" -> false) {
          for ((name, captured) <- Seq("captured_fn_1" -> None, "captured_fn_2" -> Some(true))) {
            withFallbackConf(captured) {
              sql(s"CREATE FUNCTION $name(id BIGINT) " +
                s"RETURNS STRUCT<id: BIGINT, v: $dataType> RETURN $body")
            }
          }
          val result = sql("SELECT captured_fn_1(id), captured_fn_2(id) FROM range(1, 3)")
          val conversions = result.queryExecution.optimizedPlan.flatMap { plan =>
            plan.expressions.flatMap(_.collect { case j: JsonToStructs => j })
          }
          assert(conversions.size == 2)
          val equivalents = new EquivalentExpressions
          conversions.foreach(equivalents.addExprTree(_))
          assert(equivalents.getCommonSubexpressions.exists(_.semanticEquals(conversions.head)))
          val parsed = if (dataType == "DATE") Date.valueOf("1970-01-02") else {
            Timestamp.from(Instant.parse("2024-01-02T00:00:00Z"))
          }
          checkAnswer(result, (1 to 2).map { id =>
            Row(Row(id.toLong, parsed), Row(id.toLong, parsed))
          })
        }
      }
    }
  }

  test("saved functions keep distinct full results for unset and false JSON fallback") {
    val body = """from_json(concat('{"id":', id, ',"d":"1"}'), 'id BIGINT, d DATE')"""
    withSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key -> "CORRECTED") {
      withUserDefinedFunction("captured_fn_1" -> false, "captured_fn_2" -> false) {
        for ((name, value) <- Seq("captured_fn_1" -> None, "captured_fn_2" -> Some(false))) {
          withFallbackConf(value) {
            sql(s"CREATE FUNCTION $name(id BIGINT) " +
              s"RETURNS STRUCT<id: BIGINT, d: DATE> RETURN $body")
          }
        }
        val result = sql("SELECT captured_fn_1(id), captured_fn_2(id) FROM range(1, 3)")
        val conversions = result.queryExecution.optimizedPlan.flatMap { plan =>
          plan.expressions.flatMap(_.collect { case j: JsonToStructs => j })
        }
        assert(conversions.size == 2)
        assert(!conversions.head.semanticEquals(conversions.last))
        checkAnswer(result, (1 to 2).map { id =>
          Row(Row(id.toLong, Date.valueOf("1970-01-02")), Row(id.toLong, null))
        })
      }
    }
  }

  for {
    kind <- Seq("view", "function")
    format <- Seq("json", "csv")
    creatorValue <- Seq(true, false)
    overlays <- Seq(true, false)
  } {
    test(s"$kind can restore lazy to_$format defaults: $creatorValue, overlays: $overlays") {
      val key = if (format == "json") SQLConf.JSON_GENERATOR_IGNORE_NULL_FIELDS.key else {
        SQLConf.LEGACY_NULL_VALUE_WRITTEN_AS_QUOTED_EMPTY_STRING_CSV.key
      }
      withSQLConf(captureKey -> "true", key -> creatorValue.toString) {
        withDefinition(kind, s"to_$format(named_struct('id', id, 'n', CAST(NULL AS INT)))",
            "STRING") { query =>
          withSQLConf(captureKey -> "false", key -> (!creatorValue).toString,
              SQLConf.APPLY_SESSION_CONF_OVERRIDES_TO_FUNCTION_RESOLUTION.key ->
                overlays.toString) {
            val expected = (1 to 2).map { id =>
              if (format == "json") {
                Row(if (creatorValue) s"""{"id":$id,"n":null}""" else s"""{"id":$id}""")
              } else {
                Row(if (creatorValue) s"$id," else s"$id,\"\"")
              }
            }
            checkAnswer(sql(query), expected)
          }
        }
      }
    }
  }

  for (kind <- Seq("view", "function"); creatorValue <- Seq(None, Some(true), Some(false))) {
    test(s"$kind rollback retains live unset/true/false JSON fallback: $creatorValue") {
      val input = """concat('{"id":', id, ',"d":"1","t":"2024-01-02"}')"""
      val schema = "id BIGINT, d DATE, t TIMESTAMP"
      def parsed(explicit: Option[Boolean]): String = {
        val option = explicit.map(value => s", 'enableDateTimeParsingFallback', '$value'")
          .getOrElse("")
        s"from_json($input, '$schema', map('timestampFormat', 'yyyy/MM/dd HH:mm:ss'$option))"
      }
      val body = s"named_struct('configured', ${parsed(None)}, " +
        s"'enabled', ${parsed(Some(true))}, 'disabled', ${parsed(Some(false))})"
      val parsedType = "STRUCT<id: BIGINT, d: DATE, t: TIMESTAMP>"
      val returnType =
        s"STRUCT<configured: $parsedType, enabled: $parsedType, disabled: $parsedType>"
      def result(id: Int, fallback: Option[Boolean]): Row = {
        val date = if (fallback.contains(false)) null else Date.valueOf("1970-01-02")
        val timestamp = if (fallback.contains(true)) {
          Timestamp.from(Instant.parse("2024-01-02T00:00:00Z"))
        } else null
        Row(id.toLong, date, timestamp)
      }
      withSQLConf(captureKey -> "true", SQLConf.LEGACY_TIME_PARSER_POLICY.key -> "CORRECTED",
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
        withFallbackConf(creatorValue) {
          withDefinition(kind, body, returnType) { query =>
            for (reader <- Seq(None, Some(true), Some(false)); overlays <- Seq(true, false)) {
              withSQLConf(captureKey -> "false",
                  SQLConf.APPLY_SESSION_CONF_OVERRIDES_TO_FUNCTION_RESOLUTION.key ->
                    overlays.toString) {
                withFallbackConf(reader) {
                  val expected = (1 to 2).map { id =>
                    Row(Row(result(id, reader), result(id, Some(true)), result(id, Some(false))))
                  }
                  checkAnswer(sql(query), expected)
                }
              }
            }
          }
        }
      }
    }
  }

  for (kind <- Seq("view", "function"); savedMode <- Seq(true, false)) {
    test(s"$kind nested definitions follow the caller's effective binding mode: $savedMode") {
      val jsonKey = SQLConf.JSON_GENERATOR_IGNORE_NULL_FIELDS.key
      val body = "to_json(named_struct('id', id, 'n', CAST(NULL AS INT)))"
      def check(query: String): Unit = {
        for (mode <- Seq(None, Some(true), Some(false)); overlays <- Seq(true, false)) {
          withSQLConf(jsonKey -> "false",
              SQLConf.APPLY_SESSION_CONF_OVERRIDES_TO_FUNCTION_RESOLUTION.key ->
                overlays.toString) {
            withBooleanConf(captureKey, mode) {
              val expected = (1 to 2).map { id =>
                Row(if (mode.contains(false)) s"""{"id":$id,"n":null}""" else s"""{"id":$id}""")
              }
              checkAnswer(sql(query), expected)
            }
          }
        }
      }
      withSQLConf(jsonKey -> "true", captureKey -> savedMode.toString) {
        if (kind == "view") {
          withView("captured_inner", "captured_outer") {
            sql(s"CREATE VIEW captured_inner AS SELECT $body AS value FROM range(1, 3)")
            withSQLConf(captureKey -> (!savedMode).toString) {
              sql("CREATE VIEW captured_outer AS SELECT value FROM captured_inner")
            }
            check("SELECT value FROM captured_outer")
          }
        } else {
          withUserDefinedFunction("captured_inner" -> false, "captured_outer" -> false) {
            sql(s"CREATE FUNCTION captured_inner(id BIGINT) RETURNS STRING RETURN $body")
            withSQLConf(captureKey -> (!savedMode).toString) {
              sql("CREATE FUNCTION captured_outer(id BIGINT) RETURNS STRING " +
                "RETURN captured_inner(id)")
            }
            check("SELECT captured_outer(id) FROM range(1, 3)")
          }
        }
      }
    }
  }
}
