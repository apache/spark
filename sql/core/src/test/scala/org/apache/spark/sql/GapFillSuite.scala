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
import java.time.LocalDateTime

import org.apache.spark.{SparkArithmeticException, SparkRuntimeException, SparkUnsupportedOperationException}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{GapFillGenerator, Literal}
import org.apache.spark.sql.catalyst.optimizer.InlineCTE
import org.apache.spark.sql.catalyst.plans.logical.{GapFillMethod, Generate, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, DateTimeUtils}
import org.apache.spark.sql.catalyst.util.DateTimeConstants.MICROS_PER_MINUTE
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.CharType
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
    }
  }

  test("a constant can be an expression that is replaced before it runs") {
    withReadings {
      val generator = generatorOf(
        "SELECT * FROM readings GAP FILL (ORDER BY ts BIN WIDTH INTERVAL 15 MINUTES " +
          "ALIGN TO TIMESTAMP_NTZ '2026-01-01 00:10:00' - INTERVAL 5 MINUTES " +
          "RANGE BETWEEN TIMESTAMP_NTZ '2026-01-02 00:00:00' - INTERVAL 1 DAY " +
          "AND to_timestamp_ntz('2026-01-01 01:00:00') FILL (v CONSTANT nvl(NULL, 1)))")
      val start = DateTimeUtils.localDateTimeToMicros(LocalDateTime.parse("2026-01-01T00:00:00"))
      assert(generator.alignTo == start + 5 * MICROS_PER_MINUTE)
      assert(generator.rangeStart.contains(start))
      assert(generator.rangeEnd.contains(start + 60 * MICROS_PER_MINUTE))
      assert(generator.fillMethods == Seq(GapFillMethod.Null, GapFillMethod.Constant(Literal(1))))
      sql("SELECT * FROM (SELECT CAST(ts AS DATE) AS d FROM readings) GAP FILL (ORDER BY d " +
        "BIN WIDTH INTERVAL 1 DAY RANGE BETWEEN to_date('2026-01-01') AND DATE '2026-01-03')")
    }
  }

  test("the current time functions of a clause see one time") {
    withReadings {
      def query(range: String): String =
        s"SELECT * FROM readings GAP FILL (ORDER BY ts BIN WIDTH INTERVAL 15 MINUTES $range)"
      val generator = generatorOf(query("RANGE BETWEEN localtimestamp() AND localtimestamp()"))
      assert(generator.rangeStart.isDefined && generator.rangeStart == generator.rangeEnd)
      // The values are the current time, so only their form is checked.
      checkError(
        exception = intercept[AnalysisException](sql(
          query("RANGE BETWEEN localtimestamp() + INTERVAL 1 MICROSECOND AND localtimestamp()"))),
        condition = "GAP_FILL_INVALID_RANGE",
        parameters = Map("rangeStart" -> "TIMESTAMP_NTZ '.+'", "rangeEnd" -> "TIMESTAMP_NTZ '.+'"),
        matchPVals = true)
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
    // The last two keep CHAR and VARCHAR as their own types in the plan.
    Seq(
      Nil,
      Seq(SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO.key -> "true"),
      Seq(SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "true")).foreach { confs =>
      withSQLConf((SQLConf.GAP_FILL_ENABLED.key -> "true") +: confs: _*) {
        withTable("gap_fill_char_constant") {
          sql("CREATE TABLE gap_fill_char_constant (ts TIMESTAMP_NTZ, c CHAR(5), w VARCHAR(2)) " +
            "USING parquet")
          val schema = spark.table("gap_fill_char_constant").schema
          def fill(columns: String): String = "SELECT * FROM gap_fill_char_constant GAP FILL (" +
            s"ORDER BY ts BIN WIDTH INTERVAL 15 MINUTES FILL ($columns))"
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
      checkAnalysisError(
        s"SELECT * FROM $using GAP FILL (PARTITION BY t2.k ORDER BY ts BIN WIDTH INTERVAL 1 HOUR)",
        "GAP_FILL_COLUMNS_NOT_IN_INPUT",
        Map("columnName" -> "`t2`.`k`", "clause" -> "PARTITION BY"))
      // A column that is not in the input comes before a nested column in an earlier clause.
      checkAnalysisError(
        "SELECT * FROM VALUES ('p1', named_struct('a', 1), TIMESTAMP_NTZ '2026-01-01 00:00:00') " +
          "AS t1(k, s, ts) JOIN VALUES ('p1') AS t2(k) USING (k) GAP FILL (PARTITION BY s.a " +
          "ORDER BY ts BIN WIDTH INTERVAL 1 HOUR FILL (t2.k NULL))",
        "GAP_FILL_COLUMNS_NOT_IN_INPUT",
        Map("columnName" -> "`t2`.`k`", "clause" -> "FILL"))
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
        checkAnalysisError(
          s"SELECT * FROM $input GAP FILL (ORDER BY ts BIN WIDTH INTERVAL 15 MINUTES " +
            "FILL (_metadata FFILL))",
          "GAP_FILL_COLUMNS_NOT_IN_INPUT", Map("columnName" -> "`_metadata`", "clause" -> "FILL"))
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
        checkAnalysisError(
          "SELECT * FROM VALUES ('p1') AS o(p), LATERAL (" +
            s"SELECT * FROM (SELECT * FROM readings WHERE readings.p = o.p) $clause)",
          "GAP_FILL_CORRELATED_INPUT", Map.empty, clause)
      }
      // A clause column of the outer query is not a column of the input.
      checkAnalysisError(
        "SELECT * FROM VALUES ('p1') AS o(p), LATERAL (" +
          "SELECT * FROM readings " +
          "GAP FILL (PARTITION BY o.p ORDER BY ts BIN WIDTH INTERVAL 15 MINUTES))",
        "GAP_FILL_COLUMNS_NOT_IN_INPUT",
        Map("columnName" -> "`o`.`p`", "clause" -> "PARTITION BY"))
      // A clause error comes first.
      checkAnalysisError(
        "SELECT * FROM VALUES ('p1') AS o(p), LATERAL (" +
          "SELECT * FROM (SELECT * FROM readings WHERE readings.p = o.p) " +
          "GAP FILL (ORDER BY v BIN WIDTH INTERVAL 15 MINUTES))",
        "GAP_FILL_TIME_TYPE_MISMATCH", Map("columnName" -> "`v`", "columnType" -> "\"INT\""))
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
        // A clause error comes first.
        checkAnalysisError(
          "SELECT * FROM rates GAP FILL (ORDER BY value BIN WIDTH INTERVAL 1 MINUTE)",
          "GAP_FILL_TIME_TYPE_MISMATCH",
          Map("columnName" -> "`value`", "columnType" -> "\"BIGINT\""))
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
