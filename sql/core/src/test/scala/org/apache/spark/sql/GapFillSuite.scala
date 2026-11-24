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

import java.io.File
import java.time.{Duration, LocalDateTime}

import org.apache.spark.{SparkArithmeticException, SparkRuntimeException, SparkUnsupportedOperationException}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, GapFillGenerator, Literal}
import org.apache.spark.sql.catalyst.optimizer.InlineCTE
import org.apache.spark.sql.catalyst.plans.logical.{GapFillMethod, Generate, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.trees.TreePattern.GAP_FILL_GENERATOR
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.catalyst.util.DateTimeConstants.{MICROS_PER_DAY, MICROS_PER_MINUTE}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{CharType, DateType, DayTimeIntervalType, TimestampNTZType, VarcharType}
import org.apache.spark.unsafe.types.UTF8String

class GapFillSuite extends QueryTest with SharedSparkSession {

  private val readings =
    """SELECT * FROM VALUES
      |  ('p1', TIMESTAMP_NTZ '2026-01-01 00:00:00', 90),
      |  ('p1', TIMESTAMP_NTZ '2026-01-01 00:45:00', 75)
      |  AS t(p, ts, v)""".stripMargin

  private val gapFill = "GAP FILL (PARTITION BY p ORDER BY ts BIN WIDTH INTERVAL 15 MINUTES)"

  /** Runs `f` with GAP FILL on and the temporary view `readings`. */
  private def withReadings(f: => Unit): Unit =
    withSQLConf(SQLConf.GAP_FILL_ENABLED.key -> "true") {
      withTempView("readings") {
        sql(readings).createOrReplaceTempView("readings")
        f
      }
    }

  private def generateOf(plan: LogicalPlan): Generate =
    plan.collectFirst { case g @ Generate(_: GapFillGenerator, _, _, _, _, _) => g }.get

  private def generatorOf(sqlText: String): GapFillGenerator =
    generateOf(sql(sqlText).queryExecution.analyzed).generator.asInstanceOf[GapFillGenerator]

  /** Checks the error of `sqlText`, and that it points at `fragment` when one is given. */
  private def checkAnalysisError(
      sqlText: String,
      condition: String,
      parameters: Map[String, String],
      fragment: String = ""): Unit = {
    val start = sqlText.indexOf(fragment)
    checkError(
      exception = intercept[AnalysisException](sql(sqlText)),
      condition = condition,
      parameters = parameters,
      queryContext = if (fragment.isEmpty) Array.empty else {
        Array(ExpectedContext(fragment, start, start + fragment.length - 1))
      })
  }

  test("GAP FILL in SQL text is rejected when the operator is disabled") {
    withSQLConf(SQLConf.GAP_FILL_ENABLED.key -> "false") {
      withTempView("metrics") {
        spark.sql("SELECT TIMESTAMP_NTZ '2024-01-01 00:00:00' AS ts, 1 AS v")
          .createOrReplaceTempView("metrics")
        checkError(
          exception = intercept[AnalysisException] {
            spark.sql("SELECT * FROM metrics GAP FILL (ORDER BY ts BIN WIDTH INTERVAL '5' MINUTE)")
          },
          condition = "UNSUPPORTED_FEATURE.GAP_FILL",
          parameters = Map.empty[String, String])
      }
    }
  }

  test("GAP FILL analyzes, but physical execution is not yet implemented") {
    withReadings {
      Seq(s"SELECT * FROM readings $gapFill", s"FROM readings |> $gapFill").foreach { query =>
        val df = sql(query)
        df.queryExecution.assertAnalyzed()
        assert(df.columns.toSeq == Seq("p", "ts", "v"))
        checkError(
          exception = intercept[SparkUnsupportedOperationException](df.collect()),
          condition = "UNSUPPORTED_FEATURE.GAP_FILL",
          parameters = Map.empty)
      }
    }
  }

  test("an error that GAP FILL hits while it evaluates a constant is raised as it is") {
    withReadings {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
        val query =
          "SELECT * FROM readings GAP FILL (ORDER BY ts BIN WIDTH INTERVAL 1 MINUTE * (1 / 0))"
        // A CONSTANT is evaluated in its turn too, before the range is checked.
        val constant = "SELECT * FROM readings GAP FILL (ORDER BY ts BIN WIDTH INTERVAL 1 MINUTE " +
          "RANGE BETWEEN TIMESTAMP_NTZ '2026-01-02 00:00:00' " +
          "AND TIMESTAMP_NTZ '2026-01-01 00:00:00' " +
          "FILL (v CONSTANT 1 / 0))"
        Seq(query, constant).foreach { sqlText =>
          val start = sqlText.indexOf("1 / 0")
          checkError(
            exception = intercept[SparkArithmeticException](sql(sqlText)),
            condition = "DIVIDE_BY_ZERO",
            parameters = Map("config" -> "\"spark.sql.ansi.enabled\""),
            context = ExpectedContext("1 / 0", start, start + 4))
        }
      }
    }
  }

  test("GAP FILL is rejected when its input resolves after the Resolution batch") {
    // HAVING over a GROUP BY with a generator and a window function resolves only in the last
    // analyzer batch, after ResolveGapFill has run.
    val input = "SELECT explode(array(a)) AS col, count(*) OVER () AS cnt, a " +
      "FROM VALUES (1), (2), (3), (NULL) AS t(a) GROUP BY a HAVING a IS NOT NULL"
    withSQLConf(SQLConf.GAP_FILL_ENABLED.key -> "true") {
      checkError(
        exception = intercept[AnalysisException] {
          spark.sql(s"SELECT * FROM ($input) GAP FILL (ORDER BY a BIN WIDTH 1)")
        },
        condition = "UNSUPPORTED_FEATURE.GAP_FILL",
        parameters = Map.empty[String, String])
      // A clause column that the input does not have is reported as unresolved.
      checkAnalysisError(
        s"SELECT * FROM ($input) GAP FILL (ORDER BY a BIN WIDTH 1 FILL (nope NULL))",
        "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        Map("objectName" -> "`nope`", "proposal" -> "`a`, `col`, `cnt`"), "nope")
      // A name in a value, such as a function or a session variable, also waits for the input.
      // It is not an input column, so it is not reported as unresolved.
      withSessionVariable("w") {
        sql("DECLARE VARIABLE w INT DEFAULT 1")
        Seq("BIN WIDTH w", "BIN WIDTH 1 ALIGN TO current_date",
            "BIN WIDTH 1 FILL (cnt CONSTANT current_date)").foreach { clause =>
          checkError(
            exception = intercept[AnalysisException] {
              spark.sql(s"SELECT * FROM ($input) GAP FILL (ORDER BY a $clause)")
            },
            condition = "UNSUPPORTED_FEATURE.GAP_FILL",
            parameters = Map.empty[String, String])
        }
      }
      // The missing column is reported, not the function name that comes before it.
      checkAnalysisError(
        s"SELECT * FROM ($input) GAP FILL (ORDER BY a BIN WIDTH 1 ALIGN TO current_date " +
          "FILL (nope NULL))",
        "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        Map("objectName" -> "`nope`", "proposal" -> "`a`, `col`, `cnt`"), "nope")
      // A missing ORDER BY column is reported, also when a value names an input column.
      checkAnalysisError(
        s"SELECT * FROM ($input) GAP FILL (ORDER BY nope BIN WIDTH a)",
        "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        Map("objectName" -> "`nope`", "proposal" -> "`a`, `col`, `cnt`"), "nope")
    }
  }

  test("a session variable or a function name is not a GAP FILL column") {
    withReadings {
      withSessionVariable("w") {
        sql("DECLARE VARIABLE w INT DEFAULT 1")
        val bins = "BIN WIDTH INTERVAL 15 MINUTES"
        Seq(
          (s"PARTITION BY w ORDER BY ts $bins", "w", "`w`", "`p`, `v`, `ts`"),
          (s"PARTITION BY system.session.w ORDER BY ts $bins", "system.session.w",
            "`system`.`session`.`w`", "`readings`.`p`, `readings`.`v`, `readings`.`ts`"),
          (s"ORDER BY w $bins", "w", "`w`", "`p`, `v`, `ts`"),
          (s"ORDER BY ts $bins FILL (w FFILL)", "w", "`w`", "`p`, `v`, `ts`"),
          (s"PARTITION BY session_user ORDER BY ts $bins", "session_user", "`session_user`",
            "`ts`, `p`, `v`")).foreach { case (clause, fragment, name, proposal) =>
          checkAnalysisError(
            s"SELECT * FROM readings GAP FILL ($clause)",
            "UNRESOLVED_COLUMN.WITH_SUGGESTION",
            Map("objectName" -> name, "proposal" -> proposal),
            fragment)
        }
        // An input column with the name of a session variable is a column.
        val generator = generatorOf(
          s"SELECT * FROM (SELECT p, ts, v AS w FROM readings) GAP FILL (ORDER BY ts $bins " +
            "FILL (w FFILL))")
        assert(generator.fillMethods == Seq(GapFillMethod.Null, GapFillMethod.ForwardFill))
      }
    }
  }

  test("a GAP FILL error names a column with a backtick or a dot as one name") {
    withSQLConf(SQLConf.GAP_FILL_ENABLED.key -> "true") {
      val backtick = "GAP FILL (ORDER BY `a``b` BIN WIDTH INTERVAL 1 HOUR)"
      checkAnalysisError(s"SELECT * FROM (SELECT 1 AS `a``b`) $backtick",
        "GAP_FILL_TIME_TYPE_MISMATCH", Map("columnName" -> "`a``b`", "columnType" -> "\"INT\""),
        backtick)
      val dot = "GAP FILL (ORDER BY `x.y` BIN WIDTH INTERVAL 1 HOUR FILL (`x.y` NULL))"
      checkAnalysisError(
        s"SELECT * FROM (SELECT TIMESTAMP_NTZ '2026-01-01 00:00:00' AS `x.y`, 1 AS v) $dot",
        "GAP_FILL_INVALID_FILL_COLUMN", Map("columnName" -> "`x.y`"), dot)
    }
  }

  test("a whole clause value can be a session variable or a function name") {
    withReadings {
      withSessionVariable("bw", "mg") {
        sql("DECLARE VARIABLE bw INTERVAL DAY TO SECOND DEFAULT INTERVAL '15' MINUTE")
        sql("DECLARE VARIABLE mg INTERVAL DAY TO SECOND DEFAULT INTERVAL '1' HOUR")
        // Parentheses around the name keep the name as the whole value.
        Seq("bw", "(bw)").foreach { binWidth =>
          val generator = generatorOf(
            s"SELECT * FROM readings GAP FILL (ORDER BY ts BIN WIDTH $binWidth MAX GAP mg)")
          assert(generator.binWidth == Literal(Duration.ofMinutes(15)))
          assert(generator.maxGap.contains(Literal(Duration.ofHours(1))))
        }
      }
      val query = "SELECT current_date() AS today, * FROM (SELECT CAST(ts AS DATE) AS d " +
        "FROM readings) GAP FILL (ORDER BY d BIN WIDTH INTERVAL 1 DAY ALIGN TO current_date " +
        "RANGE BETWEEN current_date AND current_date)"
      val plan = sql(query).queryExecution.optimizedPlan
      val today = plan.collectFirst { case p: Project => p.projectList.head }.get match {
        case Alias(Literal(days: Int, DateType), "today") => days
      }
      val generator = generateOf(plan).generator.asInstanceOf[GapFillGenerator]
      assert(generator.alignTo == Literal(today, DateType))
      assert(generator.rangeStart.contains(Literal(today, DateType)))
      assert(generator.rangeEnd.contains(Literal(today, DateType)))
    }
  }

  test("a view knows the session variables that GAP FILL clause values read") {
    withReadings {
      withSessionVariable("bw", "n") {
        sql("DECLARE VARIABLE bw INTERVAL DAY TO SECOND DEFAULT INTERVAL '15' MINUTE")
        sql("DECLARE VARIABLE n INT DEFAULT 3")
        // A temporary view analyzes its text again when it is read, so it reads the variables
        // again, as a view with SELECT n does.
        Seq("bw", "bw + INTERVAL '0' MINUTE").foreach { binWidth =>
          withTempView("gap_fill_view") {
            sql("CREATE TEMPORARY VIEW gap_fill_view AS SELECT * FROM readings GAP FILL (" +
              s"PARTITION BY p ORDER BY ts BIN WIDTH $binWidth FILL (v CONSTANT n))")
            sql("SET VAR bw = INTERVAL '30' MINUTE")
            sql("SET VAR n = 5")
            val generator = generatorOf("SELECT * FROM gap_fill_view")
            assert(generator.binWidth == Literal(Duration.ofMinutes(30)))
            assert(generator.fillMethods == Seq(GapFillMethod.Constant(Literal(5))))
          }
        }
        // A persisted view cannot read a session variable.
        withTable("gap_fill_view_input") {
          sql(s"CREATE TABLE gap_fill_view_input USING parquet AS $readings")
          Seq(
            "BIN WIDTH bw" -> "bw",
            "BIN WIDTH bw + INTERVAL '0' MINUTE" -> "bw",
            "BIN WIDTH INTERVAL 15 MINUTES FILL (v CONSTANT n)" -> "n").foreach {
            case (values, variable) =>
              withView("gap_fill_persisted_view") {
                checkError(
                  exception = intercept[AnalysisException] {
                    sql("CREATE VIEW gap_fill_persisted_view AS SELECT * FROM " +
                      s"gap_fill_view_input GAP FILL (PARTITION BY p ORDER BY ts $values)")
                  },
                  condition = "INVALID_TEMP_OBJ_REFERENCE",
                  parameters = Map(
                    "obj" -> "VIEW",
                    "objName" -> "`spark_catalog`.`default`.`gap_fill_persisted_view`",
                    "tempObj" -> "VARIABLE",
                    "tempObjName" -> s"`$variable`"))
              }
          }
        }
      }
    }
  }

  test("a constant can be an expression that is replaced before it runs") {
    withReadings {
      val generator = generatorOf(
        "SELECT * FROM readings GAP FILL (ORDER BY ts BIN WIDTH INTERVAL 15 MINUTES " +
          "ALIGN TO TIMESTAMP_NTZ '2026-01-01 00:10:00' - INTERVAL 5 MINUTES " +
          "RANGE BETWEEN TIMESTAMP_NTZ '2026-01-02 00:00:00' - INTERVAL 1 DAY " +
          "AND to_timestamp_ntz('2026-01-01 01:00:00') FILL (v CONSTANT nvl(NULL, 1)))")
      def ntz(s: String): Literal = Literal(LocalDateTime.parse(s))
      assert(generator.alignTo == ntz("2026-01-01T00:05:00"))
      assert(generator.rangeStart.contains(ntz("2026-01-01T00:00:00")))
      assert(generator.rangeEnd.contains(ntz("2026-01-01T01:00:00")))
      assert(generator.fillMethods == Seq(GapFillMethod.Null, GapFillMethod.Constant(Literal(1))))
      sql("SELECT * FROM (SELECT CAST(ts AS DATE) AS d FROM readings) GAP FILL (ORDER BY d " +
        "BIN WIDTH INTERVAL 1 DAY RANGE BETWEEN to_date('2026-01-01') AND DATE '2026-01-03')")
    }
  }

  test("the clause values that read the current time see the time of the whole query") {
    withReadings {
      withTempView("gap_fill_now") {
        val now = "localtimestamp()"
        val gapFillNow = "SELECT * FROM (SELECT p, ts, v, ts AS t FROM readings) GAP FILL (" +
          s"PARTITION BY p ORDER BY ts BIN WIDTH $now - ($now - INTERVAL 15 MINUTES) " +
          s"ALIGN TO $now RANGE BETWEEN $now - INTERVAL 1 DAY AND $now " +
          s"MAX GAP $now - ($now - INTERVAL 1 HOUR) FILL (t CONSTANT $now))"
        // A temporary view of a DataFrame keeps its analyzed plan, so each query that reads it
        // must still get its own time.
        sql(gapFillNow).createOrReplaceTempView("gap_fill_now")
        Seq(s"($gapFillNow)", "gap_fill_now").foreach { input =>
          val plan = sql(s"SELECT $now AS now, * FROM $input").queryExecution.optimizedPlan
          val time = plan.collectFirst { case p: Project => p.projectList.head }.get match {
            case Alias(Literal(micros: Long, TimestampNTZType), "now") => micros
          }
          val generator = generateOf(plan).generator
          // ALIGN TO, range_start, range_end and the CONSTANT, in clause order.
          assert(generator.collect { case Literal(micros: Long, TimestampNTZType) => micros } ==
            Seq(time, time - MICROS_PER_DAY, time, time))
          // BIN WIDTH and MAX GAP.
          assert(generator.collect { case Literal(micros: Long, _: DayTimeIntervalType) => micros }
            == Seq(15 * MICROS_PER_MINUTE, 60 * MICROS_PER_MINUTE))
        }
      }
      // Such a value is checked when the optimizer evaluates it. It is the current time, so
      // only the form of the values is checked.
      val query = "SELECT * FROM readings GAP FILL (ORDER BY ts BIN WIDTH INTERVAL 15 MINUTES " +
        "RANGE BETWEEN localtimestamp() + INTERVAL 1 MICROSECOND AND localtimestamp())"
      val df = sql(query)
      // The error points at the GAP FILL clause, as an error of the analyzer does.
      val start = query.indexOf("GAP FILL")
      checkError(
        exception = intercept[AnalysisException](df.queryExecution.optimizedPlan),
        condition = "GAP_FILL_INVALID_RANGE",
        parameters = Map("rangeStart" -> "TIMESTAMP_NTZ '.+'", "rangeEnd" -> "TIMESTAMP_NTZ '.+'"),
        matchPVals = true,
        queryContext = Array(ExpectedContext(query.substring(start), start, query.length - 1)))
      // A cast of a special datetime string gets the date of the whole query too, both in a
      // clause value and when a CONSTANT is cast to its column type. So does a TIME CONSTANT
      // that is cast to the TIMESTAMP_NTZ type of its column.
      Seq("true", "false").foreach { ansi =>
        withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
          val query = "SELECT current_date() AS today, * FROM (SELECT CAST(ts AS DATE) AS d, " +
            "CAST(ts AS DATE) AS d2, ts AS t FROM readings) GAP FILL (ORDER BY d " +
            "BIN WIDTH INTERVAL 1 DAY RANGE BETWEEN CAST('yesterday' AS DATE) AND " +
            "CAST('today' AS DATE) FILL (d2 CONSTANT 'today', t CONSTANT TIME '00:05:00'))"
          // Analysis keeps them as expressions, so that the optimizer evaluates them.
          val analyzed = generatorOf(query)
          val kept = (analyzed.rangeStart ++ analyzed.rangeEnd).toSeq ++
            analyzed.fillMethods.collect { case GapFillMethod.Constant(value) => value }
          assert(kept.size == 4 && kept.forall(!_.isInstanceOf[Literal]))
          // The optimizer gives the same values to a GAP FILL in a subquery.
          Seq(query, s"SELECT current_date() AS today WHERE EXISTS ($query)",
              s"SELECT current_date() AS today, (SELECT max(d2) FROM ($query)) AS d2").foreach {
            sqlText =>
              val plan = sql(sqlText).queryExecution.optimizedPlan
              val today = plan.collectFirst { case p: Project => p.projectList.head }.get match {
                case Alias(Literal(days: Int, DateType), "today") => days
              }
              val generators = plan.collectWithSubqueries {
                case Generate(generator: GapFillGenerator, _, _, _, _, _) => generator
              }
              assert(generators.size == 1)
              val generator = generators.head
              assert(generator.rangeStart.contains(Literal(today - 1, DateType)))
              assert(generator.rangeEnd.contains(Literal(today, DateType)))
              assert(generator.fillMethods == Seq(
                GapFillMethod.Constant(Literal(today, DateType)),
                GapFillMethod.Constant(Literal(
                  today * MICROS_PER_DAY + 5 * MICROS_PER_MINUTE, TimestampNTZType))))
          }
        }
      }
    }
  }

  test("only a plan that holds GAP FILL has the GAP FILL generator tree pattern") {
    withReadings {
      // EvalGapFillCurrentTimeValues prunes on this pattern, so other generators skip the rule.
      Seq("SELECT explode(array(1, 2))",
        "SELECT * FROM readings LATERAL VIEW explode(array(1)) e AS x").foreach { query =>
        assert(!sql(query).queryExecution.analyzed.containsPattern(GAP_FILL_GENERATOR), query)
      }
      Seq(s"SELECT p FROM readings $gapFill",
        s"SELECT * FROM (SELECT * FROM readings $gapFill) g JOIN readings r ON g.p = r.p")
        .foreach { query =>
          assert(sql(query).queryExecution.analyzed.containsPattern(GAP_FILL_GENERATOR), query)
        }
      // In a subquery, the pattern is at the root of the subquery plan that holds GAP FILL.
      Seq(s"SELECT (SELECT count(*) FROM readings $gapFill) AS n",
        s"SELECT 1 WHERE EXISTS (SELECT 1 FROM readings $gapFill)").foreach { query =>
        val analyzed = sql(query).queryExecution.analyzed
        assert(analyzed.collectWithSubqueries { case p => p.subqueries }.flatten
          .exists(_.containsPattern(GAP_FILL_GENERATOR)), query)
      }
    }
  }

  test("GAP FILL output columns keep the table name of their input column") {
    withReadings {
      sql(s"SELECT t.p, t.ts, t.v, bin_start, is_generated FROM ($readings) AS t GAP FILL (" +
        "PARTITION BY p ORDER BY ts BIN WIDTH INTERVAL 15 MINUTES BIN_START IS_GENERATED)")
      val join = "VALUES ('p1', TIMESTAMP_NTZ '2026-01-01 00:00:00', 1) AS a(p, ts, v) " +
        "JOIN VALUES ('p1', 2) AS b(p, v) ON a.p = b.p " +
        "GAP FILL (PARTITION BY a.p ORDER BY ts BIN WIDTH INTERVAL 15 MINUTES)"
      sql(s"SELECT a.p, b.p, a.v, b.v FROM $join")
      checkAnalysisError(s"SELECT v FROM $join", "AMBIGUOUS_REFERENCE",
        Map("name" -> "`v`", "referenceNames" -> "[`a`.`v`, `b`.`v`]"), "v")
      // A table alias replaces every table name.
      sql(s"SELECT g.p, g.v FROM readings $gapFill AS g")
      checkAnalysisError(s"SELECT readings.v FROM readings $gapFill AS g",
        "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        Map("objectName" -> "`readings`.`v`", "proposal" -> "`g`.`ts`, `g`.`v`, `g`.`p`"),
        "readings.v")
    }
  }

  test("GAP FILL keeps the CHAR type and the comment of its input columns") {
    withSQLConf(SQLConf.GAP_FILL_ENABLED.key -> "true") {
      withTable("gap_fill_char") {
        sql("CREATE TABLE gap_fill_char (p STRING, ts TIMESTAMP_NTZ, c CHAR(5), " +
          "w STRING COMMENT 'hello') USING parquet")
        val df = sql(s"SELECT * FROM gap_fill_char $gapFill WHERE c = 'ab'")
        assert(CharVarcharUtils.getRawType(df.schema("c").metadata).contains(CharType(5)))
        assert(df.schema("w").getComment().contains("hello"))
        // The comparison pads 'ab' to the CHAR length, as it does without GAP FILL.
        assert(df.queryExecution.optimizedPlan.exists(_.expressions.exists(_.exists {
          case Literal(v: UTF8String, _) => v.toString == "ab   "
          case _ => false
        })))
      }
    }
  }

  test("a CONSTANT on a CHAR or VARCHAR column is padded and checked as an INSERT does") {
    val createTable = "CREATE TABLE gap_fill_char_constant (ts TIMESTAMP_NTZ, c CHAR(5), " +
      "w VARCHAR(2)) USING parquet"
    def fill(columns: String, input: String = "gap_fill_char_constant"): String =
      s"SELECT * FROM $input GAP FILL (ORDER BY ts BIN WIDTH INTERVAL 15 MINUTES FILL ($columns))"
    // The last two keep CHAR and VARCHAR as their own types in the plan.
    Seq(
      Nil,
      Seq(SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO.key -> "true"),
      Seq(SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "true")).foreach { confs =>
      withSQLConf((SQLConf.GAP_FILL_ENABLED.key -> "true") +: confs: _*) {
        withTable("gap_fill_char_constant") {
          sql(createTable)
          val schema = spark.table("gap_fill_char_constant").schema
          assert(generatorOf(fill("c CONSTANT 'ab', w CONSTANT 'xy'")).fillMethods == Seq(
            GapFillMethod.Constant(Literal.create("ab   ", schema("c").dataType)),
            GapFillMethod.Constant(Literal.create("xy", schema("w").dataType))))
          assert(generatorOf(fill("c CONSTANT NULL")).fillMethods.head ==
            GapFillMethod.Constant(Literal(null, schema("c").dataType)))
          checkError(
            exception = intercept[SparkRuntimeException](sql(fill("w CONSTANT 'xyz'"))),
            condition = "EXCEED_LIMIT_LENGTH",
            parameters = Map("limit" -> "2"))
        }
      }
    }
    // With first-class types a CAST also makes a CHAR or VARCHAR column. Its type is then only in
    // its data type, not in its metadata as for a table column.
    val castInput =
      "(SELECT ts, CAST('a' AS CHAR(5)) AS c, CAST('b' AS VARCHAR(2)) AS w FROM readings)"
    Seq(SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO, SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS)
      .foreach { conf =>
        withReadings {
          withSQLConf(conf.key -> "true") {
            assert(generatorOf(fill("c CONSTANT 'ab', w CONSTANT 'xy'", castInput)).fillMethods ==
              Seq(
                GapFillMethod.Constant(Literal.create("ab   ", CharType(5))),
                GapFillMethod.Constant(Literal.create("xy", VarcharType(2)))))
            checkError(
              exception = intercept[SparkRuntimeException](
                sql(fill("w CONSTANT 'xyz'", castInput))),
              condition = "EXCEED_LIMIT_LENGTH",
              parameters = Map("limit" -> "2"))
          }
        }
      }
    // The legacy conf alone takes the value as a STRING, as an INSERT does. The columns still
    // have CHAR and VARCHAR as their raw types, as the table is made with the default confs.
    withSQLConf(SQLConf.GAP_FILL_ENABLED.key -> "true") {
      withTable("gap_fill_char_constant") {
        sql(createTable)
        withSQLConf(SQLConf.LEGACY_CHAR_VARCHAR_AS_STRING.key -> "true") {
          val schema = spark.table("gap_fill_char_constant").schema
          assert(CharVarcharUtils.getRawType(schema("c").metadata).contains(CharType(5)))
          assert(generatorOf(fill("c CONSTANT 'ab'")).fillMethods == Seq(
            GapFillMethod.Constant(Literal.create("ab", schema("c").dataType)),
            GapFillMethod.Null))
          assert(generatorOf(fill("w CONSTANT 'xyz'")).fillMethods == Seq(
            GapFillMethod.Null,
            GapFillMethod.Constant(Literal.create("xyz", schema("w").dataType))))
          // First-class CHAR and VARCHAR types still check the value.
          withSQLConf(SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO.key -> "true") {
            checkError(
              exception = intercept[SparkRuntimeException](sql(fill("w CONSTANT 'xyz'"))),
              condition = "EXCEED_LIMIT_LENGTH",
              parameters = Map("limit" -> "2"))
          }
        }
      }
    }
  }

  test("FFILL and BFILL take a value of any type") {
    withReadings {
      val point = "X'0101000000000000000000F03F0000000000000040'"
      val values = Seq("map('a', v)", "parse_json('1')", s"st_geomfromwkb($point)",
        s"st_geogfromwkb($point)", "make_interval(0, 0, 0, 1, 0, 0, 0)",
        "named_struct('m', map('a', v))")
      val columns = values.indices.map(i => s"c$i")
      val input = values.zip(columns).map { case (v, c) => s"$v AS $c" }.mkString(", ")
      Seq("FFILL", "BFILL").foreach { method =>
        val fill = columns.map(c => s"$c $method").mkString(", ")
        sql(s"SELECT * FROM (SELECT ts, $input FROM readings) GAP FILL (ORDER BY ts " +
          s"BIN WIDTH INTERVAL 15 MINUTES FILL ($fill))").queryExecution.assertAnalyzed()
      }
    }
  }

  test("a hidden column of a USING join cannot be used in or after GAP FILL") {
    withSQLConf(SQLConf.GAP_FILL_ENABLED.key -> "true") {
      val using = "VALUES ('p1', TIMESTAMP_NTZ '2026-01-01 00:00:00', 1) AS t1(k, ts, v) " +
        "JOIN VALUES ('p1', 2) AS t2(k, w) USING (k)"
      val partitionBy = "GAP FILL (PARTITION BY t2.k ORDER BY ts BIN WIDTH INTERVAL 1 HOUR)"
      checkAnalysisError(
        s"SELECT * FROM $using $partitionBy",
        "GAP_FILL_COLUMNS_NOT_IN_INPUT",
        Map("columnName" -> "`t2`.`k`", "clause" -> "PARTITION BY"), partitionBy)
      // A column that is not in the input comes before a nested column in an earlier clause.
      val fill =
        "GAP FILL (PARTITION BY s.a ORDER BY ts BIN WIDTH INTERVAL 1 HOUR FILL (t2.k NULL))"
      checkAnalysisError(
        "SELECT * FROM VALUES ('p1', named_struct('a', 1), TIMESTAMP_NTZ '2026-01-01 00:00:00') " +
          s"AS t1(k, s, ts) JOIN VALUES ('p1') AS t2(k) USING (k) $fill",
        "GAP_FILL_COLUMNS_NOT_IN_INPUT",
        Map("columnName" -> "`t2`.`k`", "clause" -> "FILL"), fill)
      val gapFilled = s"$using GAP FILL (PARTITION BY k ORDER BY ts BIN WIDTH INTERVAL 1 HOUR)"
      assert(sql(s"SELECT * FROM $gapFilled").columns.toSeq == Seq("k", "ts", "v", "w"))
      sql(s"SELECT t1.k, k FROM $gapFilled")
      val allColumns = "`t1`.`k`, `t2`.`w`, `t1`.`v`, `t1`.`ts`"
      Seq(
        s"SELECT t2.k FROM $gapFilled" -> allColumns,
        s"SELECT k FROM $gapFilled WHERE t2.k = 'p1'" -> allColumns,
        s"SELECT k FROM $gapFilled ORDER BY t2.k" -> "`t1`.`k`").foreach { case (query, proposal) =>
        checkAnalysisError(query, "UNRESOLVED_COLUMN.WITH_SUGGESTION",
          Map("objectName" -> "`t2`.`k`", "proposal" -> proposal), "t2.k")
      }
    }
  }

  test("a metadata column of the input cannot be used in or after GAP FILL") {
    withSQLConf(SQLConf.GAP_FILL_ENABLED.key -> "true") {
      withTempDir { dir =>
        val path = new File(dir, "readings").getCanonicalPath
        sql(readings).write.parquet(path)
        val input = s"parquet.`$path`"
        val fillMetadata =
          "GAP FILL (ORDER BY ts BIN WIDTH INTERVAL 15 MINUTES FILL (_metadata FFILL))"
        checkAnalysisError(s"SELECT * FROM $input $fillMetadata", "GAP_FILL_COLUMNS_NOT_IN_INPUT",
          Map("columnName" -> "`_metadata`", "clause" -> "FILL"), fillMetadata)
        val gapFilled = s"$input $gapFill"
        Seq(
          s"SELECT _metadata FROM $gapFilled",
          s"SELECT p FROM $gapFilled WHERE _metadata.file_size > 0",
          s"SELECT p FROM $gapFilled ORDER BY _metadata.file_name",
          s"SELECT p FROM $gapFilled GROUP BY p HAVING max(_metadata.file_size) > 0")
          .foreach { query =>
            checkAnalysisError(query, "GAP_FILL_METADATA_COLUMN_NOT_SUPPORTED",
              Map("columnName" -> "`_metadata`"), gapFill)
          }
        // Selected before GAP FILL, it is a normal column.
        sql(s"SELECT _metadata.file_size FROM (SELECT *, _metadata FROM $input) $gapFill")
        // The file can be on either side of a join.
        val clause = "GAP FILL (ORDER BY ts BIN WIDTH INTERVAL 15 MINUTES)"
        Seq(
          s"$input AS a JOIN VALUES ('p1', 2) AS b(k, w) ON a.p = b.k",
          s"VALUES ('p1', 2) AS b(k, w) JOIN $input AS a ON a.p = b.k").foreach { join =>
          checkAnalysisError(s"SELECT _metadata FROM $join $clause",
            "GAP_FILL_METADATA_COLUMN_NOT_SUPPORTED", Map("columnName" -> "`_metadata`"), clause)
        }
      }
    }
  }

  test("a DataFrame sort on an input column does not pass the column through GAP FILL") {
    withReadings {
      val input = spark.table("readings")
      val gapFilled = sql(s"SELECT * FROM readings $gapFill")
      // A partition column passes through, so it can be used above GAP FILL.
      gapFilled.orderBy(input("p")).queryExecution.assertAnalyzed()
      // The other parameters of this error show the plan, so they are not checked.
      val e = intercept[AnalysisException](gapFilled.orderBy(input("v")))
      assert(e.getCondition == "MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION")
      assert(e.getMessageParameters.get("missingAttributes") == "\"v\"")
      assert(e.getMessageParameters.get("input") == "\"p\", \"ts\", \"v\"")
    }
  }

  test("a column of a GAP FILL DataFrame resolves by its plan id") {
    withReadings {
      Seq(s"SELECT * FROM readings $gapFill", s"FROM readings |> $gapFill").foreach { query =>
        // Spark Connect puts the plan id of a DataFrame on its parsed plan and on its columns.
        val plan = spark.sessionState.sqlParser.parsePlan(query)
        plan.setTagValue(LogicalPlan.PLAN_ID_TAG, 0L)
        val v = UnresolvedAttribute("v")
        v.setTagValue(LogicalPlan.PLAN_ID_TAG, 0L)
        assert(classic.Dataset.ofRows(spark, Project(Seq(v), plan)).columns.toSeq == Seq("v"))
      }
    }
  }

  test("GAP FILL in a LATERAL subquery over one row stays below the join") {
    withReadings {
      Seq("ts2", "w, ts2").foreach { columns =>
        val df = sql(s"SELECT * FROM readings, LATERAL (SELECT $columns FROM " +
          "(SELECT TIMESTAMP_NTZ '2026-01-01 00:00:00' AS ts2, 1 AS w) GAP FILL (ORDER BY ts2 " +
          "BIN WIDTH INTERVAL 15 MINUTES RANGE BETWEEN TIMESTAMP_NTZ '2026-01-01 00:00:00' " +
          "AND TIMESTAMP_NTZ '2026-01-01 01:00:00'))")
        // GAP FILL reads its one row, not the rows of readings.
        assert(!generateOf(df.queryExecution.optimizedPlan).child.output.exists(_.name == "p"))
      }
    }
  }

  test("GAP FILL accepts an input whose own subquery refers to the input") {
    withReadings {
      Seq(
        "SELECT * FROM readings r WHERE EXISTS (SELECT 1 FROM readings u WHERE u.v = r.v)",
        "SELECT p, ts, (SELECT max(u.v) FROM readings u WHERE u.p = r.p) AS v FROM readings r",
        "SELECT r.p, r.ts, l.v FROM readings r, LATERAL (SELECT r.v + 1 AS v) l").foreach {
        input => sql(s"SELECT * FROM ($input) $gapFill").queryExecution.assertAnalyzed()
      }
    }
  }

  test("GAP FILL rejects an input that references an outer query") {
    withReadings {
      Seq("", "PARTITION BY p").foreach { partitionBy =>
        val clause = s"GAP FILL ($partitionBy ORDER BY ts BIN WIDTH INTERVAL 15 MINUTES)"
        val query = "SELECT * FROM VALUES ('p1') AS o(p), LATERAL (" +
          s"SELECT * FROM (SELECT * FROM readings WHERE readings.p = o.p) $clause)"
        checkAnalysisError(query, "GAP_FILL_CORRELATED_INPUT", Map.empty, clause)
        // The message says what to do instead.
        assert(intercept[AnalysisException](sql(query)).getMessage.startsWith(
          "[GAP_FILL_CORRELATED_INPUT] GAP FILL is not supported on an input that references " +
          "an outer query. Remove the outer reference from the GAP FILL input. SQLSTATE: 0A000;"))
      }
      // A clause column of the outer query is not a column of the input.
      val outerPartitionBy = "GAP FILL (PARTITION BY o.p ORDER BY ts BIN WIDTH INTERVAL 15 MINUTES)"
      checkAnalysisError(
        s"SELECT * FROM VALUES ('p1') AS o(p), LATERAL (SELECT * FROM readings $outerPartitionBy)",
        "GAP_FILL_COLUMNS_NOT_IN_INPUT",
        Map("columnName" -> "`o`.`p`", "clause" -> "PARTITION BY"), outerPartitionBy)
      // So is a nested field of one. Under HAVING the field resolves without an alias. It comes
      // before a nested column of the input in an earlier clause.
      val outer = "(SELECT named_struct('a', 'p1') AS s) AS o"
      val nestedClause =
        "GAP FILL (PARTITION BY n.a ORDER BY ts BIN WIDTH INTERVAL 15 MINUTES FILL (s.a NULL))"
      val sub = s"SELECT 1 FROM (SELECT *, named_struct('a', 1) AS n FROM readings) $nestedClause"
      Seq(
        s"SELECT * FROM $outer WHERE EXISTS ($sub)",
        s"SELECT count(*) FROM $outer GROUP BY s HAVING EXISTS ($sub)").foreach { query =>
        checkAnalysisError(query, "GAP_FILL_COLUMNS_NOT_IN_INPUT",
          Map("columnName" -> "`o`.`s`", "clause" -> "FILL"), nestedClause)
      }
      // A clause error comes first.
      val timeClause = "GAP FILL (ORDER BY v BIN WIDTH INTERVAL 15 MINUTES)"
      checkAnalysisError(
        "SELECT * FROM VALUES ('p1') AS o(p), LATERAL (" +
          s"SELECT * FROM (SELECT * FROM readings WHERE readings.p = o.p) $timeClause)",
        "GAP_FILL_TIME_TYPE_MISMATCH", Map("columnName" -> "`v`", "columnType" -> "\"INT\""),
        timeClause)
      // An outer reference above GAP FILL is fine.
      sql(s"SELECT * FROM VALUES (90) AS o(v) WHERE EXISTS (" +
        s"SELECT 1 FROM readings $gapFill AS g WHERE g.v = o.v)")
    }
  }

  test("GAP FILL rejects a streaming input") {
    withSQLConf(SQLConf.GAP_FILL_ENABLED.key -> "true") {
      withTempView("rates") {
        spark.readStream.format("rate").load().createOrReplaceTempView("rates")
        val clause = "GAP FILL (ORDER BY timestamp BIN WIDTH INTERVAL 1 MINUTE)"
        checkAnalysisError(s"SELECT * FROM rates $clause",
          "GAP_FILL_STREAMING_NOT_SUPPORTED", Map.empty, clause)
        // The message says what to do instead.
        assert(intercept[AnalysisException](sql(s"SELECT * FROM rates $clause"))
          .getMessage.startsWith("[GAP_FILL_STREAMING_NOT_SUPPORTED] GAP FILL is not supported " +
          "on streaming input. Use GAP FILL in a batch query. SQLSTATE: 0A000;"))
        // A clause error comes first.
        val timeClause = "GAP FILL (ORDER BY value BIN WIDTH INTERVAL 1 MINUTE)"
        checkAnalysisError(
          s"SELECT * FROM rates $timeClause",
          "GAP_FILL_TIME_TYPE_MISMATCH",
          Map("columnName" -> "`value`", "columnType" -> "\"BIGINT\""), timeClause)
      }
    }
  }

  test("a recursive reference cannot be below GAP FILL") {
    withSQLConf(SQLConf.GAP_FILL_ENABLED.key -> "true") {
      def recursive(step: String, query: String): String =
        s"""WITH RECURSIVE r(ts, v) AS (
           |  SELECT TIMESTAMP_NTZ '2026-01-01 00:00:00', 1
           |  UNION ALL
           |  SELECT ts + INTERVAL 1 HOUR, v + 1 FROM $step WHERE v < 3)
           |$query""".stripMargin
      val clause = "GAP FILL (ORDER BY ts BIN WIDTH INTERVAL 15 MINUTES)"
      checkError(
        exception = intercept[AnalysisException] {
          sql(recursive(s"(SELECT * FROM r $clause)", "SELECT * FROM r"))
        },
        condition = "INVALID_RECURSIVE_REFERENCE.PLACE",
        parameters = Map.empty)
      // GAP FILL over the result of the recursion is fine.
      sql(recursive("r", s"SELECT * FROM r $clause")).queryExecution.assertAnalyzed()
    }
  }

  test("one GAP FILL used twice in a self-join or a self-union gets its own columns") {
    withReadings {
      withTempView("g") {
        val clause = "GAP FILL (PARTITION BY p ORDER BY ts BIN WIDTH INTERVAL 15 MINUTES " +
          "BIN_START IS_GENERATED)"
        sql(s"SELECT * FROM readings $clause AS x").createOrReplaceTempView("g")
        val cte = s"WITH g AS (SELECT * FROM readings $clause AS x) "
        val join = "SELECT a.v, b.v FROM g a JOIN g b ON a.p = b.p AND a.bin_start = b.bin_start"
        val union = "SELECT * FROM g UNION ALL SELECT * FROM g"
        Seq(join, union, cte + join, cte + union).foreach { query =>
          val df = sql(query)
          // CheckAnalysis and the optimizer see each CTE reference inlined.
          val plan = InlineCTE(alwaysInline = true)(df.queryExecution.analyzed)
          val Seq(g1, g2) =
            plan.collect { case g @ Generate(_: GapFillGenerator, _, _, _, _, _) => g }
          assert(g1.outputSet.intersect(g2.outputSet).isEmpty)
          assert(g1.child.outputSet.intersect(g2.child.outputSet).isEmpty)
          checkError(
            exception = intercept[SparkUnsupportedOperationException](df.collect()),
            condition = "UNSUPPORTED_FEATURE.GAP_FILL",
            parameters = Map.empty)
        }
      }
    }
  }

  test("over an input that resolved in time, clause errors come in clause order") {
    withReadings {
      checkAnalysisError(
        "SELECT * FROM readings GAP FILL (ORDER BY ts BIN WIDTH 1 + true FILL (nope NULL))",
        "DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES",
        Map("sqlExpr" -> "\"(1 + true)\"", "left" -> "\"INT\"", "right" -> "\"BOOLEAN\""),
        "1 + true")
      val mapKey = "SELECT * FROM readings GAP FILL (ORDER BY ts BIN WIDTH INTERVAL 15 MINUTES " +
        "FILL (v CONSTANT map('a', 1)[a]))"
      val key = mapKey.indexOf("[a]") + 1
      checkError(
        exception = intercept[AnalysisException](sql(mapKey)),
        condition = "UNRESOLVED_MAP_KEY.WITH_SUGGESTION",
        parameters = Map("objectName" -> "`a`", "proposal" -> "`p`, `v`, `ts`"),
        context = ExpectedContext("a", key, key))
    }
  }
}
