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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.execution.{FileSourceScanExec, ProjectExec}
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, FileScan}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.functions.{expr, udf}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class KllDeterminismRegressionSuite extends QueryTest with SharedSparkSession {
  import testImplicits._
  private val scalar = """(SELECT kll_sketch_get_n_bigint(kll_sketch_agg_bigint(id))
      |FROM VALUES (0), (1), (2) AS t(id))""".stripMargin

  test("aggregate admission rejects residual correlated scalar volatility") {
    val outcomes = for (singlePass <- Seq(false, true); legacy <- Seq(false, true);
        filter <- Seq(false, true)) yield {
      withSQLConf(
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString,
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
          SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false",
          SQLConf.LEGACY_KLL_AGGREGATE_DETERMINISTIC.key -> legacy.toString) {
        val correlated = "(SELECT uuid() FROM VALUES (1) AS i(k) WHERE i.k = o.k)"
        val aggregate = if (filter) s"count(*) FILTER (WHERE $correlated IS NOT NULL)"
          else s"count(DISTINCT $correlated)"
        val text = s"SELECT $aggregate FROM VALUES (1), (1) AS o(k)"
        val expected = if (filter) "INVALID_AGGREGATE_FILTER.NON_DETERMINISTIC"
          else "AGGREGATE_FUNCTION_WITH_NONDETERMINISTIC_EXPRESSION"
        val actual = try {
          val query = sql(text)
          query.queryExecution.analyzed
          val result = query.queryExecution.executedPlan.executeCollect().map(_.getLong(0)).toSeq
          s"ACCEPTED result=$result: ${query.queryExecution.optimizedPlan}"
        } catch {
          case e: AnalysisException => e.getCondition
        }
        (singlePass, legacy, filter, actual, expected)
      }
    }
    assert(outcomes.forall { case (_, _, _, actual, expected) => actual == expected },
      outcomes.mkString("\n"))
  }

  test("aggregate correlated admission retains deterministic and KLL-only inputs") {
    for (singlePass <- Seq(false, true); legacy <- Seq(false, true)) {
      val queries = withSQLConf(
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString,
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
          SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false",
          SQLConf.LEGACY_KLL_AGGREGATE_DETERMINISTIC.key -> legacy.toString) {
        val deterministic = "(SELECT i.k FROM VALUES (1L), (2L) AS i(k) WHERE i.k = o.k)"
        val correlated = """(SELECT kll_sketch_get_n_bigint(kll_sketch_agg_bigint(i.k))
            |FROM VALUES (1L), (2L) AS i(k) WHERE i.k = o.k)""".stripMargin
        val source = "FROM VALUES (1L), (1L), (2L) AS o(k)"
        val cases = Seq(
          s"SELECT count(DISTINCT $deterministic) $source" -> Row(2L),
          s"SELECT count(*) FILTER (WHERE $deterministic > 0) $source" -> Row(3L),
          s"SELECT sum($correlated) $source" -> Row(3L),
          s"SELECT count(*) FILTER (WHERE $correlated > 0) $source" -> Row(3L),
          s"SELECT sum($scalar) $source" -> Row(9L))
        cases.map { case (text, expected) =>
          val query = sql(text)
          val analyzed = query.queryExecution.analyzed
          def modes(plan: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan) =
            plan.collectWithSubqueries { case p => p.expressions.flatMap(_.collect {
              case k: org.apache.spark.sql.catalyst.expressions.aggregate.KllSketchAggBigint =>
                k.legacyDeterministic
            }) }.flatten
          assert(modes(analyzed).forall(_ == legacy))
          val prepared = org.apache.spark.sql.execution.streaming.runtime.KllCheckpointPreparation(
            analyzed, analyzed, legacySharing = !legacy)
          assert(modes(prepared).forall(_ == !legacy))
          Seq(query -> expected,
            org.apache.spark.sql.classic.Dataset.ofRows(spark, prepared) -> expected)
        }.flatten
      }
      queries.foreach { case (query, expected) => checkAnswer(query, expected) }
    }
  }

  test("two exported HAVING dependencies retain their asymmetric value association") {
    for (singlePass <- Seq(false, true); legacy <- Seq(false, true)) {
      val queries = withSQLConf(
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString,
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
          SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false",
          SQLConf.LEGACY_KLL_AGGREGATE_DETERMINISTIC.key -> legacy.toString) {
        val all = "kll_sketch_get_n_bigint(kll_sketch_agg_bigint(t.id))"
        val half = "kll_sketch_get_n_bigint(kll_sketch_agg_bigint(IF(t.id < 5L, t.id, NULL)))"
        val source = (0 until 10).map(i => s"(${i}L)").mkString(", ")
        Seq(">", "<").map { comparison =>
          val query = sql(s"""SELECT min(t.id) AS ordinary_key, count(*) AS ordinary_count
              |FROM VALUES $source AS t(id)
              |HAVING EXISTS (SELECT 1 WHERE $all $comparison $half)""".stripMargin)
          query.queryExecution.analyzed
          query -> comparison
        }
      }
      queries.foreach { case (query, comparison) =>
        checkAnswer(query, if (comparison == ">") Seq(Row(0L, 10L)) else Nil)
      }
    }
  }

  test("ordinary KLL scalar joins retain outer rows and semi anti inclusion") {
    val source = "VALUES (1), (2), (3) AS l(id)"
    val right = "VALUES (1, 1L), (2, 0L) AS r(id, allowed)"
    val condition = s"l.id = r.id AND r.allowed = $scalar - 2L"
    checkAnswer(sql(s"SELECT l.id, r.id FROM $source LEFT OUTER JOIN $right ON $condition"),
      Seq(Row(1, 1), Row(2, null), Row(3, null)))
    checkAnswer(sql(s"SELECT l.id FROM $source LEFT SEMI JOIN $right ON $condition"), Row(1))
    checkAnswer(sql(s"SELECT l.id FROM $source LEFT ANTI JOIN $right ON $condition"),
      Seq(Row(2), Row(3)))
  }

  test("safe numeric KLL bindings keep partition membership below a live Window") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val binary = """(SELECT kll_sketch_agg_bigint(id)
          |FROM VALUES (0L), (1L), (2L) AS t(id))""".stripMargin
      for (join <- Seq("SEMI", "ANTI")) {
        val query = sql(s"""SELECT f.p, f.rn, d.id FROM
            |(SELECT id, p, row_number() OVER (PARTITION BY p ORDER BY id) AS rn
            | FROM VALUES (0L, 0), (1L, 1), (2L, 0), (3L, 1) AS facts(id, p)) f
            |JOIN VALUES (3L), (4L), (5L), (6L) AS d(id)
            |ON f.id + kll_sketch_get_n_bigint($binary) = d.id
            |LEFT $join JOIN VALUES (1) AS members(p) ON f.p = members.p""".stripMargin)
        val plan = query.queryExecution.optimizedPlan
        assert(plan.exists {
          case window: org.apache.spark.sql.catalyst.plans.logical.Window =>
            window.child.exists {
              case j: org.apache.spark.sql.catalyst.plans.logical.Join =>
                j.joinType == (if (join == "SEMI") org.apache.spark.sql.catalyst.plans.LeftSemi
                  else org.apache.spark.sql.catalyst.plans.LeftAnti)
              case _ => false
            }
          case _ => false
        }, plan)
        assert(plan.collect { case p: org.apache.spark.sql.catalyst.plans.logical.Project =>
          p.projectList.collect {
            case a: org.apache.spark.sql.catalyst.expressions.Alias
                if a.isNondeterministicSubqueryBinding => a.dataType
          }
        }.flatten.contains(org.apache.spark.sql.types.LongType), plan)
        checkAnswer(query, if (join == "SEMI") Seq(Row(1, 1, 4L), Row(1, 2, 6L))
          else Seq(Row(0, 1, 3L), Row(0, 2, 5L)))
      }
    }
  }

  test("KLL aggregate bindings preserve scalar results outside the updater") {
    withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "false",
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false") {
      val query = sql("""SELECT kll_sketch_get_n_bigint(kll_sketch_agg_bigint(id)) AS n
          |FROM range(3)""".stripMargin)
      val value = org.apache.spark.sql.classic.ClassicConversions.column(
        org.apache.spark.sql.classic.ColumnConversions.expression(query.scalar()))
      checkAnswer(spark.range(2).agg((org.apache.spark.sql.functions.sum(value) + value).as("v")),
        Row(9L))
      val identity = udf((n: Long) => n).asNondeterministic()
      for ((key, expectedKey) <- Seq(
          (value + org.apache.spark.sql.functions.lit(1L), 4L), (identity(value), 3L))) {
        checkAnswer(spark.range(2).groupBy(key.as("key")).agg(
          org.apache.spark.sql.functions.sum(value).as("total"), value.as("outside")),
          Row(expectedKey, 6L, 3L))
      }
    }
  }

  test("selected KLL scalar bytes stay above shuffled membership joins") {
    withTempView("kll_project_fact", "kll_project_members") {
      spark.sparkContext.parallelize(0L until 32L).toDF("key")
        .createOrReplaceTempView("kll_project_fact")
      spark.sparkContext.parallelize(Seq(3L)).toDF("member")
        .createOrReplaceTempView("kll_project_members")
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
        for (negation <- Seq("", "NOT ")) {
          val query = sql(s"""SELECT key, sketch FROM
              |(SELECT key, (SELECT kll_sketch_agg_bigint(id)
              |FROM VALUES (0L), (1L), (2L) AS t(id)) AS sketch FROM kll_project_fact) q
              |WHERE key ${negation}IN (SELECT member FROM kll_project_members)""".stripMargin)
          val joins = query.queryExecution.executedPlan.collect { case join: SortMergeJoinExec =>
            join
          }
          assert(joins.size == 1, query.queryExecution.toString)
          assert(joins.head.left.output.forall(
            _.dataType != org.apache.spark.sql.types.BinaryType), query.queryExecution.toString)
          val rows = query.collect().toSeq
          val expected = (0L until 32L).filter(k => (k == 3L) == negation.isEmpty)
          assert(rows.map(_.getLong(0)).sorted == expected)
          assert(rows.forall(_.getAs[Array[Byte]](1).nonEmpty))
        }
      }
    }
  }

  test("KLL alternatives retain live public outputs across CTE and UNION boundaries") {
    for (singlePass <- Seq(false, true); legacy <- Seq(false, true)) {
      val queries = withSQLConf(
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString,
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
          SQLConf.LEGACY_KLL_AGGREGATE_DETERMINISTIC.key -> legacy.toString) {
        val aggregate = """SELECT kll_sketch_agg_bigint(id) AS s FROM
            |VALUES (0L), (1L), (2L), (3L), (4L), (5L), (6L), (7L), (8L), (9L) AS t(id)
            |HAVING kll_sketch_get_n_bigint(kll_sketch_agg_bigint(id)) > 0""".stripMargin
        val cte = sql(s"WITH c AS ($aggregate) SELECT kll_sketch_get_n_bigint(s) FROM c")
        val union = sql(s"""SELECT kll_sketch_get_n_bigint(s) FROM
            |(($aggregate) UNION ALL ($aggregate)) AS u""".stripMargin)
        Seq(cte, union).foreach(_.queryExecution.assertAnalyzed())
        (cte, union)
      }
      // The query itself is analyzed strictly; the Row test encoder still needs fallback.
      checkAnswer(queries._1, Row(10L))
      checkAnswer(queries._2, Seq(Row(10L), Row(10L)))
    }
  }

  for (operator <- Seq("/", "DIV", "%")) {
    test(s"KLL membership retains the non-ANSI zero divisor guard: $operator") {
      for (legacy <- Seq(false, true)) {
        withSQLConf(SQLConf.ANSI_ENABLED.key -> "false",
            SQLConf.LEGACY_KLL_AGGREGATE_DETERMINISTIC.key -> legacy.toString) {
          checkAnswer(sql(s"""SELECT IF(CAST(raise_error('numerator') AS BIGINT) > 0,
              |CAST(id IN (SELECT $scalar) AS BIGINT), 0L) $operator id
              |FROM range(2) WHERE id = 0""".stripMargin), Row(null))
        }
      }
    }
  }

  test("KLL membership retains ordinary IN first-match and NULL guards") {
    for (legacy <- Seq(false, true)) {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "true",
          SQLConf.LEGACY_KLL_AGGREGATE_DETERMINISTIC.key -> legacy.toString) {
        checkAnswer(sql(s"""SELECT IF(id = 0, true, CAST(NULL AS BOOLEAN)) IN
            |(true, IF(CAST(raise_error('list element') AS INT) > 0,
            |id IN (SELECT $scalar), false)) FROM range(2)""".stripMargin),
          Seq(Row(true), Row(null)))
      }
    }
  }

  test("KLL membership retains pmod right-first NULL evaluation") {
    for (legacy <- Seq(false, true); ansi <- Seq(false, true)) {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi.toString,
          SQLConf.LEGACY_KLL_AGGREGATE_DETERMINISTIC.key -> legacy.toString) {
        checkAnswer(sql(s"""SELECT pmod(CAST(raise_error('left') AS BIGINT),
            |IF(id IN (SELECT $scalar), CAST(NULL AS BIGINT), 1L))
            |FROM range(3, 4)""".stripMargin), Row(null))
      }
    }
  }

  test("batch KLL alternatives retire producers with optional pruning disabled") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
        "org.apache.spark.sql.catalyst.optimizer.ColumnPruning") {
      val query = sql("""SELECT count(*) FROM range(10)
          |HAVING kll_sketch_get_n_bigint(kll_sketch_agg_bigint(id)) > 0""".stripMargin)
      checkAnswer(query, Row(10L))
      val widths = query.queryExecution.executedPlan.collect {
        case aggregate: org.apache.spark.sql.execution.aggregate.BaseAggregateExec =>
          aggregate.aggregateExpressions.count(_.containsPattern(
            org.apache.spark.sql.catalyst.trees.TreePattern.KLL_AGGREGATE))
      }
      assert(widths.nonEmpty && widths.forall(_ == 1), widths)
    }
  }

  for (host <- Seq("arithmetic", "division", "array", "try", "nested-try")) {
    test(s"KLL membership retains its enclosing evaluation domain: $host") {
      for (legacy <- Seq(false, true); codegen <- Seq("NO_CODEGEN", "CODEGEN_ONLY")) {
        withSQLConf(SQLConf.LEGACY_KLL_AGGREGATE_DETERMINISTIC.key -> legacy.toString,
            SQLConf.ANSI_ENABLED.key -> "true", SQLConf.CODEGEN_FACTORY_MODE.key -> codegen) {
          val member = s"id IN (SELECT $scalar)"
          val conditional = s"IF(1L / b > 0, CAST($member AS BIGINT), 0L)"
          val value = host match {
            case "arithmetic" => s"a + $conditional"
            case "division" => s"$conditional / a"
            case "array" => s"array_contains(IF(id = 0, CAST(NULL AS ARRAY<BIGINT>), " +
              s"array(1L)), $conditional)"
            case "try" => s"try_divide(INTERVAL '1' DAY, $conditional)"
            case "nested-try" => s"coalesce(try_divide(INTERVAL '1' DAY, $conditional), " +
              "INTERVAL '2' DAY)"
          }
          val source = "(SELECT id, IF(id = 0, CAST(NULL AS BIGINT), 1L) AS a, " +
            "id AS b FROM range(2)) q"
          val result = sql(s"SELECT $value FROM $source WHERE id = 0")
          val expected = host match {
            case "nested-try" => java.time.Duration.ofDays(2)
            case _ => null
          }
          checkAnswer(result, Row(expected))
          if (Seq("arithmetic", "array", "division").contains(host)) {
            val active: Any = host match {
              case "arithmetic" => 1L
              case "division" => 0D
              case "array" => false
            }
            checkAnswer(sql(s"SELECT $value FROM $source WHERE id = 1"), Row(active))
          }
          if (Seq("try", "nested-try").contains(host)) {
            val activeSource = "(SELECT id, 1L AS a, id AS b FROM range(3, 4)) q"
            checkAnswer(sql(s"SELECT $value FROM $activeSource"),
              Row(java.time.Duration.ofDays(1)))
          }
        }
      }
    }
  }

  test("KLL membership guards retain their aggregate host") {
    for (singlePass <- Seq(false, true); codegen <- Seq("NO_CODEGEN", "CODEGEN_ONLY")) {
      withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> codegen) {
        val membership = s"1L IN (SELECT $scalar)"
        for (value <- Seq(s"IF(sum(id) > 0, $membership, false)",
            s"CASE WHEN sum(id) > 0 THEN $membership ELSE false END",
            s"sum(id) <= 0 OR $membership",
            s"coalesce(IF(sum(id) > 0, CAST(NULL AS BOOLEAN), false), $membership)")) {
          val query = withSQLConf(
              SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString,
              SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
              SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false") {
            val result = sql(s"SELECT $value FROM VALUES (0L), (1L), (2L) t(id)")
            result.queryExecution.analyzed
            result
          }
          checkAnswer(query, Row(false))
        }
      }
    }
  }

  test("KLL count-only HAVING retains every outer alternative") {
    for (legacy <- Seq(false, true); singlePass <- Seq(false, true)) {
      val query = withSQLConf(
          SQLConf.LEGACY_KLL_AGGREGATE_DETERMINISTIC.key -> legacy.toString,
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString,
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
          SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false") {
        val result = sql("""SELECT count(*) FROM VALUES (0L), (1L), (2L) t(id)
            |HAVING EXISTS (SELECT 1 WHERE
            |kll_sketch_get_n_bigint(kll_sketch_agg_bigint(t.id)) > 0)""".stripMargin)
        result.queryExecution.analyzed
        result
      }
      checkAnswer(query, Row(3L))
    }
  }

  test("KLL membership grouping guards observe normalized keys") {
    checkAnswer(sql(s"""SELECT IF(CAST(k AS STRING) = '-0.0', true,
        |1L IN (SELECT $scalar)) FROM VALUES
        |(CAST('-0.0' AS DOUBLE)), (CAST('0.0' AS DOUBLE)) t(k) GROUP BY k""".stripMargin),
      Row(false))
    for (legacy <- Seq(false, true)) {
      withSQLConf(SQLConf.LEGACY_KLL_AGGREGATE_DETERMINISTIC.key -> legacy.toString) {
        checkAnswer(sql(s"""SELECT count(IF(id IN (SELECT $scalar), 1, NULL))
            |FROM VALUES (0L), (3L) t(id)""".stripMargin), Row(1L))
      }
    }
  }

  test("floating membership pushdown preserves KLL groups for NaNs and signed zeros") {
    withTempView("kll_float_groups", "kll_float_members") {
      Seq((-0.0D, 1D), (0D, 2D), (Double.NaN, 3D), (Double.NaN, 4D), (2D, 5D))
        .toDF("k", "v").createOrReplaceTempView("kll_float_groups")
      Seq(0D, Double.NaN).toDF("k").createOrReplaceTempView("kll_float_members")
      for (codegen <- Seq("NO_CODEGEN", "CODEGEN_ONLY")) {
        withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> codegen) {
          val source = """(SELECT k,
              |kll_sketch_get_n_double(kll_sketch_agg_double(v)) AS n
              |FROM kll_float_groups GROUP BY k) a""".stripMargin
          checkAnswer(sql(s"SELECT /*+ BROADCAST(r) */ a.k, a.n FROM $source " +
            "LEFT SEMI JOIN kll_float_members r ON a.k = r.k"),
            Seq(Row(0D, 2L), Row(Double.NaN, 2L)))
          checkAnswer(sql(s"SELECT /*+ BROADCAST(r) */ a.k, a.n FROM $source " +
            "LEFT ANTI JOIN kll_float_members r ON a.k <=> r.k"), Row(2D, 1L))
        }
      }
    }
  }

  test("KLL membership preserves Coalesce demand") {
    withTempView("kll_coalesce_input") {
      spark.sparkContext.parallelize(Seq((Some(true), Some(1L))), 1).toDF("flag", "id")
        .createOrReplaceTempView("kll_coalesce_input")
      for (legacy <- Seq(true, false); codegen <- Seq("NO_CODEGEN", "CODEGEN_ONLY")) {
        withSQLConf(SQLConf.LEGACY_KLL_AGGREGATE_DETERMINISTIC.key -> legacy.toString,
            SQLConf.ANSI_ENABLED.key -> "true", SQLConf.CODEGEN_FACTORY_MODE.key -> codegen) {
          val member = s"id IN (SELECT $scalar)"
          val danger = s"IF(id / 0L > 0, $member, false)"
          checkAnswer(sql(s"SELECT coalesce(flag, $danger) FROM kll_coalesce_input"), Row(true))
          checkAnswer(sql(s"SELECT coalesce(CAST(NULL AS BOOLEAN), flag, $danger) " +
            "FROM kll_coalesce_input"), Row(true))
          checkAnswer(sql(s"SELECT coalesce($member, $danger) FROM kll_coalesce_input"),
            Row(false))
          checkAnswer(sql(s"SELECT coalesce(flag, $member, $danger) FROM kll_coalesce_input"),
            Row(true))
          checkAnswer(sql(s"SELECT IF($member, false, true) FROM kll_coalesce_input"), Row(true))
          val error = intercept[Exception] {
            sql(s"SELECT coalesce(CAST(NULL AS BOOLEAN), $danger) " +
              "FROM kll_coalesce_input").collect()
          }
          assert(error.toString.contains("DIVIDE_BY_ZERO"))
        }
      }
    }
  }

  test("strict single-pass HAVING keeps independent outer KLL identities") {
    for (legacy <- Seq(false, true); singlePass <- Seq(true, false)) {
      withSQLConf(SQLConf.LEGACY_KLL_AGGREGATE_DETERMINISTIC.key -> legacy.toString,
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString,
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
          SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false") {
        val call = "kll_sketch_agg_bigint(t.id)"
        val source = "FROM VALUES (1L), (2L), (3L) t(id)"
        for (having <- Seq(s"EXISTS (SELECT 1 WHERE $call = $call)",
            s"EXISTS (SELECT 1 WHERE $call = s) AND EXISTS (SELECT 1 WHERE $call = s)")) {
          val plan = sql(s"SELECT $call AS s $source HAVING $having").queryExecution.optimizedPlan
          val aggregates = plan.collect {
            case a: org.apache.spark.sql.catalyst.plans.logical.Aggregate => a
          }.head.aggregateExpressions.flatMap(_.collect { case a: AggregateExpression => a })
          assert(aggregates.map(_.resultId).distinct.size == (if (legacy) 1 else 3), plan)
        }
      }
    }
  }

  test("KLL exported HAVING aggregates retain their bound output identity") {
    for (legacy <- Seq(true, false)) {
      withSQLConf(SQLConf.LEGACY_KLL_AGGREGATE_DETERMINISTIC.key -> legacy.toString,
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "false",
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false") {
        val query = sql("""SELECT kll_sketch_agg_bigint(t.id) AS s FROM range(10) t
            |HAVING EXISTS (SELECT 1 WHERE kll_sketch_agg_bigint(t.id) != s)""".stripMargin)
        val subquery = query.queryExecution.analyzed.flatMap(_.expressions).flatMap(_.collect {
          case exists: org.apache.spark.sql.catalyst.expressions.Exists => exists
        }).head
        val references = subquery.plan.flatMap(_.expressions).flatMap(_.collect {
          case reference: org.apache.spark.sql.catalyst.expressions.OuterReference =>
            reference.e.exprId
        }).distinct
        assert(references.size == (if (legacy) 1 else 2), query.queryExecution.analyzed)
      }
    }
  }

  test("KLL aggregate inputs retain nested aggregate validation") {
    for (function <- Seq("sum", "count")) {
      withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "false",
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
          SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false") {
        val error = intercept[AnalysisException] {
          sql(s"SELECT $function(sum(id) + $scalar) FROM range(2)").queryExecution.analyzed
        }
        assert(error.getCondition == "NESTED_AGGREGATE_FUNCTION")
      }
    }
  }

  test("KLL aggregate pairwise NULL guards and DISTINCT retain original input values") {
    withTempView("kll_pairwise_inputs") {
      spark.sparkContext.parallelize(Seq((Option.empty[Double], 0L), (None, 1L)), 1)
        .toDF("x", "id").createOrReplaceTempView("kll_pairwise_inputs")
      for (legacy <- Seq(true, false); codegen <- Seq("NO_CODEGEN", "CODEGEN_ONLY")) {
        withSQLConf(SQLConf.LEGACY_KLL_AGGREGATE_DETERMINISTIC.key -> legacy.toString,
            SQLConf.ANSI_ENABLED.key -> "true", SQLConf.CODEGEN_FACTORY_MODE.key -> codegen,
            SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> (codegen == "CODEGEN_ONLY").toString) {
          checkAnswer(sql(s"SELECT regr_count(x, $scalar / 0D) FROM kll_pairwise_inputs"), Row(0L))
          checkAnswer(sql(s"SELECT count(DISTINCT id + $scalar) FROM kll_pairwise_inputs"), Row(2L))
        }
      }
    }
  }

  test("KLL Count binding retains distinct primitive UDF null dependencies") {
    val add = udf((n: Long, s: Long) => n + s).asNonNullable()
    val input = Seq((Some(1L), Some(1L)), (None, Some(2L))).toDF("a", "b")
    for (legacy <- Seq(false, true);
        codegen <- Seq("NO_CODEGEN", "CODEGEN_ONLY")) {
      withSQLConf(
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "false",
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
          SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false",
          SQLConf.LEGACY_KLL_AGGREGATE_DETERMINISTIC.key -> legacy.toString,
          SQLConf.CODEGEN_FACTORY_MODE.key -> codegen,
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> (codegen == "CODEGEN_ONLY").toString) {
        val query = sql(s"SELECT $scalar AS n")
        val value = org.apache.spark.sql.classic.ClassicConversions.column(
          org.apache.spark.sql.classic.ColumnConversions.expression(query.scalar()))
        val result = input.agg(
          org.apache.spark.sql.functions.count(add($"a", value)),
          org.apache.spark.sql.functions.count(add($"b", value)),
          org.apache.spark.sql.functions.count(add($"a", value)))
        result.queryExecution.analyzed
        checkAnswer(result, Row(1L, 2L, 1L))
      }
    }
  }

  test("KLL DISTINCT retains every exposed key during alternative retirement") {
    val queries = for (singlePass <- Seq(false, true); legacy <- Seq(false, true)) yield {
      withSQLConf(
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString,
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
          SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false",
          SQLConf.LEGACY_KLL_AGGREGATE_DETERMINISTIC.key -> legacy.toString) {
        val keys = Seq("(1L), (2L)" -> Seq(Row(1), Row(1)), "(1L), (1L)" -> Seq(Row(1)))
          .flatMap { case (values, expected) =>
            val query = sql(s"""SELECT c FROM (
                |SELECT DISTINCT 1 AS c, kll_sketch_agg_bigint(id, 8) AS sketch
                |FROM VALUES $values AS t(id) GROUP BY id
                |HAVING kll_sketch_get_n_bigint(kll_sketch_agg_bigint(id, 8)) > 0) q""".stripMargin)
            val analyzed = query.queryExecution.analyzed
            Seq(query -> expected) ++ Seq(false, true).map { checkpointLegacy =>
              val prepared = org.apache.spark.sql.execution.streaming.runtime
                .KllCheckpointPreparation(analyzed, analyzed, checkpointLegacy)
              org.apache.spark.sql.classic.Dataset.ofRows(spark, prepared) -> expected
            }
          }
        val control = sql("""SELECT c FROM (
            |SELECT DISTINCT 1 AS c FROM VALUES (1L), (2L) AS t(id) GROUP BY id
            |HAVING kll_sketch_get_n_bigint(kll_sketch_agg_bigint(id, 8)) > 0) q""".stripMargin)
        control.queryExecution.analyzed
        keys :+ (control -> Seq(Row(1)))
      }
    }
    queries.flatten.foreach { case (query, expected) => checkAnswer(query, expected) }
  }

  Seq("first", "any_value", "max_by", "min_by").foreach { function =>
    test(s"KLL aggregate input extraction preserves buffer demand: $function") {
      val value = s"CASE WHEN id = 0 THEN $scalar " +
        "ELSE CAST(raise_error('unused aggregate input') AS BIGINT) END"
      val argument = if (function.endsWith("_by")) {
        s"$value, CASE WHEN id = 0 THEN 1L ELSE CAST(NULL AS BIGINT) END"
      } else value
      for (legacy <- Seq(true, false); codegen <- Seq("NO_CODEGEN", "CODEGEN_ONLY")) {
        withSQLConf(SQLConf.LEGACY_KLL_AGGREGATE_DETERMINISTIC.key -> legacy.toString,
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
            SQLConf.CODEGEN_FACTORY_MODE.key -> codegen,
            SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> (codegen == "CODEGEN_ONLY").toString) {
          checkAnswer(sql(s"SELECT $function($argument) FROM range(0, 2, 1, 1)"), Row(3L))
        }
      }
    }
  }

  Seq(false, true).foreach { filtered =>
    test(s"KLL Count input extraction preserves skipped inputs: filtered=$filtered") {
      withTempView("kll_nullable_count") {
        spark.sparkContext.parallelize(Seq((Option.empty[Long], Some(1L))), 1)
          .toDF("a", "b").createOrReplaceTempView("kll_nullable_count")
        val malformed = """kll_sketch_get_n_bigint(substring(
            |(SELECT kll_sketch_agg_bigint(id) FROM range(1)), 1, 1))""".stripMargin
        val count = if (filtered) s"count(coalesce($malformed, 0L)) FILTER (WHERE b > 0)"
          else s"count(a, $malformed + b)"
        for (legacy <- Seq(true, false)) {
          withSQLConf(SQLConf.LEGACY_KLL_AGGREGATE_DETERMINISTIC.key -> legacy.toString,
              SQLConf.CODEGEN_FACTORY_MODE.key -> "NO_CODEGEN",
              SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
            checkAnswer(sql(s"SELECT $count FROM kll_nullable_count"),
              Row(if (filtered) 1L else 0L))
          }
        }
      }
    }
  }

  test("KLL extraction retains the interval TRY exception catcher") {
    val one = """(SELECT kll_sketch_get_n_bigint(kll_sketch_agg_bigint(id))
        |FROM range(1))""".stripMargin
    for (legacy <- Seq(true, false)) {
      withSQLConf(SQLConf.LEGACY_KLL_AGGREGATE_DETERMINISTIC.key -> legacy.toString,
          SQLConf.CODEGEN_FACTORY_MODE.key -> "NO_CODEGEN",
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
        checkAnswer(sql(s"""SELECT count(*)
            |FROM range(2) GROUP BY try_divide(INTERVAL '1' DAY, $one - 1)""".stripMargin),
          Row(2L))
      }
    }
  }

  test("KLL all-outer aggregates resolve their HAVING subquery references") {
    for (legacy <- Seq(true, false)) {
      withSQLConf(SQLConf.LEGACY_KLL_AGGREGATE_DETERMINISTIC.key -> legacy.toString,
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "false",
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false") {
        checkAnswer(sql("""SELECT kll_sketch_get_n_bigint(kll_sketch_agg_bigint(t.id))
            |FROM range(10) t HAVING EXISTS
            |(SELECT 1 WHERE kll_sketch_get_n_bigint(kll_sketch_agg_bigint(t.id)) > 0)
            |""".stripMargin), Row(10L))
      }
    }
  }

  test("KLL guarded inputs evaluate deterministic aggregate FILTER predicates once") {
    val evaluations = spark.sparkContext.longAccumulator("kll-deterministic-filter")
    withUserDefinedFunction("kll_counted_filter" -> true) {
      spark.udf.register("kll_counted_filter", (n: Long) => {
        evaluations.add(1L)
        n % 2 == 0
      })
      for (legacy <- Seq(true, false)) {
        evaluations.reset()
        withSQLConf(SQLConf.LEGACY_KLL_AGGREGATE_DETERMINISTIC.key -> legacy.toString,
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
            SQLConf.CODEGEN_FACTORY_MODE.key -> "NO_CODEGEN",
            SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
          val result = sql(s"""SELECT sum($scalar + id)
              |FILTER (WHERE kll_counted_filter(id)), count(*) FROM range(0, 4, 1, 1)
              |""".stripMargin).collect().toSeq
          assert(result == Seq(Row(8L, 4L)))
          assert(evaluations.value == 4L, s"legacy=$legacy evaluations=${evaluations.value}")
        }
      }
    }
  }

  test("KLL HAVING and ordering resolve checkpoint alternatives once in both analyzers") {
    for (singlePass <- Seq(false, true); legacy <- Seq(false, true)) {
      withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString,
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
          SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false",
          SQLConf.LEGACY_KLL_AGGREGATE_DETERMINISTIC.key -> legacy.toString) {
        val getter = "kll_sketch_get_n_bigint(kll_sketch_agg_bigint(id))"
        for (clause <- Seq(s"HAVING $getter > 0", s"ORDER BY $getter")) {
          QueryTest.checkAnswer(
            sql(s"SELECT $getter AS n FROM VALUES (0L), (1L), (2L) AS t(id) $clause"),
            Seq(Row(3L)), checkToRDD = false)
        }
        QueryTest.checkAnswer(
          sql(s"SELECT count(*) FROM VALUES (0L), (1L), (2L) AS t(id) HAVING $scalar > 0"),
          Seq(Row(3L)), checkToRDD = false)
      }
    }
  }

  test("KLL batch HAVING and ordering retain the selected sharing alternative") {
    for (singlePass <- Seq(false, true); legacy <- Seq(false, true);
        clause <- Seq("HAVING", "ORDER BY")) {
      withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString,
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
          SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false",
          SQLConf.LEGACY_KLL_AGGREGATE_DETERMINISTIC.key -> legacy.toString) {
        val getter = "kll_sketch_get_n_bigint(kll_sketch_agg_bigint(id))"
        val tail = if (clause == "HAVING") s"HAVING $getter > 0" else s"ORDER BY $getter"
        val query = sql(s"""SELECT id % 2 AS g, $getter AS n
            |FROM VALUES (0L), (1L), (2L), (3L), (4L) AS t(id)
            |GROUP BY id % 2 $tail""".stripMargin)
        val functions = query.queryExecution.optimizedPlan.collect {
          case aggregate: org.apache.spark.sql.catalyst.plans.logical.Aggregate =>
            aggregate.aggregateExpressions.flatMap(_.collect {
              case expression: AggregateExpression => expression
            }).size
        }
        assert(functions == Seq(if (legacy) 1 else 2),
          s"singlePass=$singlePass legacy=$legacy clause=$clause ${query.queryExecution}")
        val expected = if (clause == "ORDER BY") Seq(Row(1L, 2L), Row(0L, 3L))
          else Seq(Row(0L, 3L), Row(1L, 2L))
        QueryTest.checkAnswer(query, expected, checkToRDD = false)
      }
    }
  }

  for (singlePass <- Seq(false, true); clause <- Seq("HAVING", "ORDER BY")) {
    test(s"independent batch KLL calls retain identity: $clause singlePass=$singlePass") {
      for (legacy <- Seq(false, true)) {
        withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString,
            SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
            SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false",
            SQLConf.LEGACY_KLL_AGGREGATE_DETERMINISTIC.key -> legacy.toString) {
          val aggregate = "kll_sketch_agg_bigint(id)"
          val tail = if (clause == "HAVING") s"HAVING $aggregate = $aggregate"
            else s"ORDER BY $aggregate, $aggregate"
          val query = sql(s"""SELECT id % 2 AS g, count(*)
              |FROM VALUES (0L), (1L), (2L), (3L) AS t(id)
              |GROUP BY id % 2 $tail""".stripMargin)
          val functions = query.queryExecution.optimizedPlan.collect {
            case a: org.apache.spark.sql.catalyst.plans.logical.Aggregate =>
              a.aggregateExpressions.flatMap(_.collect {
                case expression: AggregateExpression
                    if expression.aggregateFunction.isInstanceOf[
                      org.apache.spark.sql.catalyst.expressions.aggregate.KllSketchAggBigint] =>
                  expression.resultId
              })
          }.flatten.distinct
          assert(if (legacy) functions.size <= 1 else functions.size == 2,
            query.queryExecution.toString)
        }
      }
    }
  }

  Seq("deterministic", "KLL").foreach { queryKind =>
    test(s"guarded $queryKind subqueries preserve a shared seeded random grouping value") {
      for (legacy <- Seq(false, true); codegen <- Seq("NO_CODEGEN", "CODEGEN_ONLY")) {
        withSQLConf(SQLConf.LEGACY_KLL_AGGREGATE_DETERMINISTIC.key -> legacy.toString,
            SQLConf.CODEGEN_FACTORY_MODE.key -> codegen) {
          val random = org.apache.spark.sql.functions.rand(0L)
          val query = if (queryKind == "KLL") s"CAST(($scalar - 3) AS DOUBLE)"
            else "(SELECT 0D)"
          val input = spark.range(0, 4, 1, 1)
          val result = input.groupBy($"id", random.as("r"),
            org.apache.spark.sql.functions.when($"id" % 2 === 0,
              random + expr(query)).otherwise(0D).as("guarded")).count()
            .orderBy("id").collect()
          assert(result.length == 4)
          result.foreach { row =>
            assert(row.getDouble(2) == (if (row.getLong(0) % 2 == 0) row.getDouble(1) else 0D),
              s"legacy=$legacy codegen=$codegen row=$row")
          }
        }
      }
    }
  }

  test("guarded KLL grouping does not reuse a collected-only random descendant") {
    for (codegen <- Seq("NO_CODEGEN", "CODEGEN_ONLY")) {
      withSQLConf(SQLConf.LEGACY_KLL_AGGREGATE_DETERMINISTIC.key -> "false",
          SQLConf.CODEGEN_FACTORY_MODE.key -> codegen) {
        val random = org.apache.spark.sql.functions.rand(0L)
        val identity = udf((value: Double) => value).asNondeterministic()
        val selected = $"id" % 2 === 0
        val ordinary = identity(
          org.apache.spark.sql.functions.when(selected, random).otherwise(0D))
        val guarded = org.apache.spark.sql.functions.when(selected,
          random + expr(s"($scalar - 3)")).otherwise(0D)
        val rows = spark.range(0, 8, 1, 1).groupBy($"id", ordinary.as("ordinary"),
          guarded.as("guarded")).count().orderBy("id").collect()
        assert(rows.length == 8)
        rows.foreach(row => assert(row.getDouble(1) == row.getDouble(2),
          s"codegen=$codegen row=$row"))
      }
    }
  }

  test("aggregate FILTER stages one predicate evaluation for its guarded inputs") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false",
        SQLConf.SUBQUERY_REUSE_ENABLED.key -> "false") {
      val query = sql(s"""SELECT sum($scalar) FILTER (WHERE id IN
          |(SELECT kll_sketch_get_quantile_bigint(kll_sketch_agg_bigint(id, 8), 0.5D)
          |FROM range(256))) FROM range(256)""".stripMargin)
      val joins = query.queryExecution.optimizedPlan.collect {
        case join: org.apache.spark.sql.catalyst.plans.logical.Join
            if join.joinType.isInstanceOf[org.apache.spark.sql.catalyst.plans.ExistenceJoin] => join
      }
      assert(joins.size == 1, query.queryExecution.toString)
      checkAnswer(query, Row(3L))
    }
  }

  test("KLL window scalars retain independent partition pruning") {
    for (source <- Seq("parquet", "")) {
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> source) {
        withTempPath { path =>
          spark.range(8).selectExpr("id", "CAST(id % 2 AS INT) AS p")
            .write.partitionBy("p").parquet(path.getAbsolutePath)
          withTempView("kll_window_partitions") {
            spark.read.parquet(path.getAbsolutePath)
              .createOrReplaceTempView("kll_window_partitions")
            val window = """(SELECT kll_sketch_agg_bigint(id, 8) OVER ()
                |FROM range(3) LIMIT 1)""".stripMargin
            val query = sql(s"""SELECT id, n FROM (SELECT id, p,
                |kll_sketch_get_n_bigint($window) AS n FROM kll_window_partitions) q
                |WHERE p = 1""".stripMargin)
            val scans = query.queryExecution.sparkPlan.collect {
              case scan: FileSourceScanExec =>
                scan.selectedPartitions.filePartitionIterator.map(_.values.getInt(0)).toSet
              case scan: BatchScanExec =>
                val file = scan.scan.asInstanceOf[FileScan]
                file.fileIndex.listFiles(file.partitionFilters, file.dataFilters)
                  .map(_.values.getInt(0)).toSet
            }
            assert(scans == Seq(Set(1)), query.queryExecution.toString)
            checkAnswer(query, Seq(1L, 3L, 5L, 7L).map(id => Row(id, 3L)))
          }
        }
      }
    }
  }

  test("KLL aggregate arguments retain their FILTER evaluation domain") {
    for (legacy <- Seq(true, false); codegen <- Seq("NO_CODEGEN", "CODEGEN_ONLY")) {
      withSQLConf(SQLConf.LEGACY_KLL_AGGREGATE_DETERMINISTIC.key -> legacy.toString,
          SQLConf.CODEGEN_FACTORY_MODE.key -> codegen, SQLConf.ANSI_ENABLED.key -> "true") {
        checkAnswer(sql(s"""SELECT sum($scalar / id) FILTER (WHERE id > 0), count(*)
            |FROM VALUES (0), (1) AS t(id)""".stripMargin), Row(3D, 2L))
      }
    }
  }

  Seq("primitive UDF", "sequence", "array concat").foreach { host =>
    test(s"KLL extraction retains future and variable-arity NULL guards: $host") {
      withUserDefinedFunction("future_null_identity" -> true, "later_kll_throw" -> true) {
        spark.udf.register("future_null_identity", udf((n: Long) => n).asNonNullable())
        spark.udf.register("later_kll_throw", (n: java.lang.Long) => {
          require(n == null, "later NULL-guarded operand was evaluated")
          1L
        })
        withTempView("kll_variable_null_guard") {
          spark.sparkContext.parallelize(
            Seq((Option.empty[Long], Option.empty[Seq[Long]])), 1).toDF("c", "a")
            .createOrReplaceTempView("kll_variable_null_guard")
          val throwing = s"later_kll_throw($scalar)"
          val expression = host match {
            case "primitive UDF" => s"future_null_identity(c) + $throwing"
            case "sequence" => s"sequence(c, $throwing)"
            case _ => s"concat(a, array($throwing))"
          }
          for (codegen <- Seq("NO_CODEGEN", "CODEGEN_ONLY")) {
            withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> codegen) {
              checkAnswer(sql(s"SELECT c FROM kll_variable_null_guard ORDER BY $expression"),
                Row(null))
            }
          }
        }
      }
    }
  }
  test("live KLL build outputs retain runtime partition pruning on stable keys") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_USE_STATS.key -> "false",
        SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
        SQLConf.DYNAMIC_PARTITION_PRUNING_FALLBACK_FILTER_RATIO.key -> "1.0") {
      withTempPath { path =>
        val factPath = s"${path.getAbsolutePath}/fact"
        val dimPath = s"${path.getAbsolutePath}/dim"
        spark.range(12).selectExpr("id", "CAST(id % 4 AS INT) AS p")
          .write.partitionBy("p").parquet(factPath)
        spark.range(4).selectExpr("CAST(id AS INT) AS k", "IF(id = 1, 1L, 0L) AS v")
          .write.parquet(dimPath)
        withTempView("kll_dpp_fact", "kll_dpp_dim") {
          spark.read.parquet(factPath).createOrReplaceTempView("kll_dpp_fact")
          spark.read.parquet(dimPath).createOrReplaceTempView("kll_dpp_dim")
          val query = sql("""SELECT /*+ BROADCAST(d) */ f.id, kll_sketch_get_n_bigint(d.s)
              |FROM kll_dpp_fact f JOIN (SELECT k, kll_sketch_agg_bigint(v) AS s
              |FROM kll_dpp_dim WHERE v > 0 GROUP BY k) d ON f.p = d.k""".stripMargin)
          QueryTest.checkAnswer(query, Seq(Row(1L, 1L), Row(5L, 1L), Row(9L, 1L)),
            checkToRDD = false)
          val scans = query.queryExecution.executedPlan.collect {
            case scan: FileSourceScanExec if scan.output.exists(_.name == "id") => scan
          }
          assert(scans.size == 1, query.queryExecution.toString)
          assert(scans.head.metrics("numPartitions").value == 1, query.queryExecution.toString)
          val multipleKeys = sql("""SELECT /*+ BROADCAST(d) */ f.id,
              |kll_sketch_get_n_bigint(d.s) FROM kll_dpp_fact f JOIN
              |(SELECT k, kll_sketch_agg_bigint(v) AS s
              |FROM kll_dpp_dim WHERE v > 0 GROUP BY k) d
              |ON f.id = kll_sketch_get_n_bigint(d.s) AND f.p = d.k""".stripMargin)
          QueryTest.checkAnswer(multipleKeys, Seq(Row(1L, 1L)), checkToRDD = false)
          val multipleScans = multipleKeys.queryExecution.executedPlan.collect {
            case scan: FileSourceScanExec if scan.output.exists(_.name == "id") => scan
          }
          assert(multipleScans.size == 1, multipleKeys.queryExecution.toString)
          assert(multipleScans.head.metrics("numPartitions").value == 1,
            multipleKeys.queryExecution.toString)
        }
      }
    }
  }

  test("SELECT-only KLL scalars retain grouping-key HAVING partition pruning") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
      withTempPath { path =>
        spark.range(8).selectExpr("id", "CAST(id % 2 AS INT) AS p")
          .write.partitionBy("p").parquet(path.getAbsolutePath)
        withTempView("kll_having_partitions") {
          spark.read.parquet(path.getAbsolutePath).createOrReplaceTempView("kll_having_partitions")
          val query = sql(s"""SELECT p, count(*), $scalar AS n FROM kll_having_partitions
              |GROUP BY p HAVING p = 1""".stripMargin)
          val scan = query.queryExecution.sparkPlan.collect { case s: FileSourceScanExec => s }
          assert(scan.size == 1)
          assert(scan.head.selectedPartitions.filePartitionIterator
            .map(_.values.getInt(0)).toSet == Set(1), query.queryExecution.toString)
          checkAnswer(query, Row(1, 4L, 3L))
        }
      }
    }
  }

  test("throwable sibling aliases retain ordered KLL-independent partition pruning") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
      withTempPath { path =>
        spark.range(8).selectExpr("id", "CAST(id % 2 AS INT) AS p")
          .write.partitionBy("p").parquet(path.getAbsolutePath)
        withTempView("kll_alias_partitions") {
          spark.read.parquet(path.getAbsolutePath).createOrReplaceTempView("kll_alias_partitions")
          val query = sql(s"""SELECT id, n FROM (SELECT id,
              |array_contains(sequence(p, p, 1), 1) AS keep, $scalar AS n
              |FROM kll_alias_partitions) q WHERE keep""".stripMargin)
          val scan = query.queryExecution.sparkPlan.collect { case s: FileSourceScanExec => s }
          assert(scan.size == 1)
          assert(scan.head.selectedPartitions.filePartitionIterator
            .map(_.values.getInt(0)).toSet == Set(1), query.queryExecution.toString)
          checkAnswer(query, Seq(1L, 3L, 5L, 7L).map(id => Row(id, 3L)))
          checkAnswer(sql(s"""SELECT id, n FROM (SELECT id,
              |CASE WHEN p = 0 THEN raise_error('guarded alias') ELSE true END AS keep,
              |$scalar AS n FROM kll_alias_partitions) q WHERE n < 0 AND keep""".stripMargin),
            Seq.empty)
        }
      }
    }
  }

  test("KLL sort extraction preserves ordered NULL evaluation") {
    withUserDefinedFunction("null_guarded_kll_throw" -> true) {
      spark.udf.register("null_guarded_kll_throw", (bytes: Array[Byte]) => {
        require(bytes.isEmpty, "ordered NULL guard was bypassed")
        1L
      })
      val throwing = """null_guarded_kll_throw(
          |(SELECT kll_sketch_agg_bigint(id) FROM range(1)))""".stripMargin
      val seconds = s"CAST($throwing AS DECIMAL(16, 6))"
      val year = "CAST(c AS INT)"
      val expressions = Seq(
        s"substring(c, 1, CAST($throwing AS INT))",
        s"overlay(c, 'x', 1, CAST($throwing AS INT))",
        s"make_timestamp($year, 1, 1, 0, 0, $seconds)",
        s"make_timestamp($year, 1, 1, 0, 0, $seconds, 'UTC')",
        s"CAST(make_interval($year, 0, 0, 0, 0, 0, $seconds) AS STRING)")
      withTempView("null_guarded_kll_order") {
        spark.sparkContext.parallelize(Seq(Tuple1(Option.empty[String])), 1).toDF("c")
          .createOrReplaceTempView("null_guarded_kll_order")
        for (codegen <- Seq("NO_CODEGEN", "CODEGEN_ONLY"); expression <- expressions) {
          withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> codegen) {
            checkAnswer(sql(s"SELECT c FROM null_guarded_kll_order ORDER BY $expression"),
              Row(null))
          }
        }
      }
    }
  }

  test("throwable partition predicates retain pruning beside KLL scalars") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
      withTempPath { path =>
        spark.range(8).selectExpr("id", "CAST(id % 2 AS INT) AS p")
          .write.partitionBy("p").parquet(path.getAbsolutePath)
        withTempView("throwable_kll_pruning") {
          spark.read.parquet(path.getAbsolutePath).createOrReplaceTempView("throwable_kll_pruning")
          val query = sql(s"""SELECT id, n FROM
              |(SELECT id, p, $scalar AS n FROM throwable_kll_pruning) q
              |WHERE array_contains(sequence(p, p, 1), 1)""".stripMargin)
          val scans = query.queryExecution.sparkPlan.collect { case s: FileSourceScanExec => s }
          assert(scans.size == 1)
          assert(scans.head.selectedPartitions.filePartitionIterator
            .map(_.values.getInt(0)).toSet == Set(1), query.queryExecution.toString)
          checkAnswer(query, Seq(1L, 3L, 5L, 7L).map(id => Row(id, 3L)))
        }
      }
    }
  }

  test("nullable arithmetic preserves the evaluation domain of random dependencies") {
    withUserDefinedFunction("boxed_kll_identity" -> true) {
      spark.udf.register("boxed_kll_identity", (value: java.lang.Double) => value)
      for (legacy <- Seq(false, true); codegen <- Seq("NO_CODEGEN", "CODEGEN_ONLY");
          kll <- Seq("", s" + 0D * $scalar")) {
        withSQLConf(SQLConf.LEGACY_KLL_AGGREGATE_DETERMINISTIC.key -> legacy.toString,
            SQLConf.CODEGEN_FACTORY_MODE.key -> codegen, SQLConf.ANSI_ENABLED.key -> "true") {
          checkAnswer(sql(s"""SELECT count(*) FROM range(2) GROUP BY boxed_kll_identity(
              |IF(id = 0, CAST(NULL AS DOUBLE), 1D) +
              |element_at(shuffle(array(1D / id, 2D)), 1)$kll)""".stripMargin),
            Seq(Row(1L), Row(1L)))
        }
      }
    }
  }

  test("KLL-dependent UDFs retain their enclosing conditional guard") {
    withUserDefinedFunction("guarded_kll_throw" -> true) {
      spark.udf.register("guarded_kll_throw", (bytes: Array[Byte]) => {
        require(bytes.isEmpty, "guarded UDF was evaluated")
        0L
      })
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
        checkAnswer(sql("""SELECT count(*) FROM range(3) GROUP BY CASE WHEN id < 0 THEN
            |guarded_kll_throw(IF(id < 0,
            |(SELECT kll_sketch_agg_bigint(id) FROM range(10)), X'00')) ELSE 0 END
            |""".stripMargin), Row(3L))
      }
    }
  }

  test("ordinary nondeterministic UDF chains retain one extraction projection") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
      val identity = udf((value: String) => value).asNondeterministic()
      val input = spark.range(3).selectExpr("CAST(id AS STRING) AS value")
      val deterministicScalar = expr("""concat(value,
          |(SELECT CAST(count(*) AS STRING) FROM VALUES (0) AS t(id)))""".stripMargin)
      for (initial <- Seq(input("value"), deterministicScalar)) {
        val argument = (1 to 4).foldLeft(initial) { (child, _) => identity(child) }
        val query = input.groupBy(argument).count()
        val projects = query.queryExecution.executedPlan.collect { case p: ProjectExec => p }
        assert(projects.size == 1, query.queryExecution.toString)
        checkAnswer(query.select("count"), Seq.fill(3)(Row(1L)))
      }
    }
  }

  for (asof <- Seq(false, true)) {
    test(s"nested KLL predicate operands retain JOIN analysis: asof=$asof") {
      val exists = "EXISTS (SELECT kll_sketch_agg_bigint(id) " +
        "FROM VALUES (0L), (1L) AS t(id))"
      val bools = """SELECT true FROM
          |(SELECT kll_sketch_agg_bigint(id) AS s
          |FROM VALUES (0L), (1L) AS t(id)) q WHERE s IS NOT NULL
          |""".stripMargin
      for (singlePass <- Seq(false, true)) {
        withSQLConf(SQLConf.SQL_ASOF_JOIN_ENABLED.key -> "true",
            SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString,
            SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
            SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false") {
          if (!asof) {
            QueryTest.checkAnswer(sql(s"""SELECT l.id, r.id FROM
                |VALUES (0L), (1L) AS l(id) JOIN VALUES (0L), (1L) AS r(id)
                |ON ($exists) IN ($bools)""".stripMargin),
              Seq(Row(0L, 0L), Row(0L, 1L), Row(1L, 0L), Row(1L, 1L)), checkToRDD = false)
          } else {
            QueryTest.checkAnswer(sql(s"""SELECT l.k, r.k FROM VALUES (3), (4) AS l(k)
                |ASOF JOIN VALUES (0), (1) AS r(k) MATCH_CONDITION (l.k >= r.k)
                |ON $scalar IN (SELECT $scalar)""".stripMargin),
              Seq(Row(3, 1), Row(4, 1)), checkToRDD = false)
          }
        }
      }
    }
  }

  test("retained KLL CTEs preserve COUNT zero after constant-empty optimization") {
    checkAnswer(sql("""SELECT o.id, (WITH k AS
        |(SELECT id, kll_sketch_agg_bigint(id) AS s FROM range(2) GROUP BY id)
        |SELECT count(*) FROM k a JOIN k b ON a.id = b.id
        |WHERE a.id = o.id AND 1 = 2) AS c FROM range(1) o""".stripMargin), Row(0L, 0L))
  }

  test("persisted KLL view bodies inherit caller rollback when the saved key is absent") {
    val key = SQLConf.LEGACY_KLL_AGGREGATE_DETERMINISTIC.key
    val correlated = """(SELECT kll_sketch_get_n_bigint(kll_sketch_agg_bigint(i.id))
        |FROM VALUES (0L), (1L) AS i(id) WHERE o.id >= 0)""".stripMargin
    withView("old_kll_rollback_view") {
      withSQLConf(key -> "true") {
        sql(s"""CREATE VIEW old_kll_rollback_view AS SELECT $correlated AS a,
            |$correlated AS b FROM VALUES (0), (1) AS o(id) GROUP BY 1""".stripMargin)
        val catalog = spark.sessionState.catalog
        val table = catalog.getTableMetadata(TableIdentifier("old_kll_rollback_view"))
        catalog.alterTable(table.copy(properties = table.properties.filterNot {
          case (property, _) => property.endsWith(key)
        }))
        checkAnswer(spark.table("old_kll_rollback_view"), Row(2L, 2L))
      }
    }
  }

  test("computed siblings retain KLL-independent partition pruning") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
      withTempPath { path =>
        spark.range(8).selectExpr("id", "IF(id % 2 = 0, 'a', 'b') AS p")
          .write.partitionBy("p").parquet(path.getAbsolutePath)
        withTempView("computed_kll_pruning") {
          spark.read.parquet(path.getAbsolutePath).createOrReplaceTempView("computed_kll_pruning")
          val query = sql(s"""SELECT p2, n FROM (SELECT upper(p) AS p2,
              |$scalar AS n FROM computed_kll_pruning) q WHERE p2 = 'A'""".stripMargin)
          val scans = query.queryExecution.sparkPlan.collect { case s: FileSourceScanExec => s }
          assert(scans.size == 1)
          assert(scans.head.selectedPartitions.filePartitionIterator
            .map(_.values.getUTF8String(0).toString).toSet == Set("a"),
            query.queryExecution.toString)
          checkAnswer(query, Seq.fill(4)(Row("A", 3L)))
        }
      }
    }
  }

  test("KLL join decoders remain inside dynamic conditional guards") {
    withTempView("guarded_kll_left", "guarded_kll_right") {
      spark.sparkContext.parallelize(Seq(0L), 1).toDF("id")
        .createOrReplaceTempView("guarded_kll_left")
      spark.sparkContext.parallelize(Seq(0L), 1).toDF("id")
        .createOrReplaceTempView("guarded_kll_right")
      val invalidBuffers = Seq(
        """kll_sketch_get_n_bigint((SELECT
          |substring(kll_sketch_agg_bigint(id, 8), 1, 1) FROM range(10)))""".stripMargin,
        """kll_sketch_get_n_bigint((SELECT
          |kll_sketch_agg_float(CAST(id AS FLOAT)) FROM range(1)))""".stripMargin)
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
          SQLConf.CODEGEN_FACTORY_MODE.key -> "NO_CODEGEN",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        invalidBuffers.foreach { invalid =>
          val query = sql(s"""SELECT l.id, r.id FROM guarded_kll_left l
              |JOIN guarded_kll_right r ON l.id = r.id AND
              |CASE WHEN l.id + r.id < 0 THEN $invalid > 0 ELSE true END""".stripMargin)
          checkAnswer(query, Row(0L, 0L))
        }
      }
    }
  }

  test("renamed sibling columns retain KLL-independent partition pruning") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
      withTempPath { path =>
        spark.range(8).selectExpr("id", "CAST(id % 2 AS INT) AS p")
          .write.partitionBy("p").parquet(path.getAbsolutePath)
        withTempView("renamed_kll_pruning") {
          spark.read.parquet(path.getAbsolutePath).createOrReplaceTempView("renamed_kll_pruning")
          val query = sql(s"""SELECT p2, n FROM
              |(SELECT p AS p2, $scalar AS n FROM renamed_kll_pruning) q WHERE p2 = 1
              |""".stripMargin)
          val scans = query.queryExecution.sparkPlan.collect {
            case scan: FileSourceScanExec => scan
          }
          assert(scans.size == 1)
          assert(scans.head.selectedPartitions.filePartitionIterator
            .map(_.values.getInt(0)).toSet == Set(1), query.queryExecution.toString)
          checkAnswer(query, Seq.fill(4)(Row(1, 3L)))
        }
      }
    }
  }

  test("primitive Scala UDF null guards retain conditional random arguments") {
    val first = udf((n: Int, xs: Seq[Double], count: java.lang.Long) => n + xs.head)
    withUserDefinedFunction("null_guarded_first" -> true) {
      spark.udf.register("null_guarded_first", first)
      for (codegen <- Seq("NO_CODEGEN", "CODEGEN_ONLY")) {
        withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> codegen,
            SQLConf.ANSI_ENABLED.key -> "true") {
          checkAnswer(sql(s"""SELECT count(*) FROM range(2) GROUP BY
              |null_guarded_first(IF(id = 0, CAST(NULL AS INT), 1),
              |shuffle(array(1D / id, 1D / id)), $scalar)""".stripMargin),
            Seq(Row(1L), Row(1L)))
        }
      }
    }
  }

  test("KLL ASOF predicate subqueries retain analyzer parity") {
    for (singlePass <- Seq(false, true); valueType <- Seq("bigint", "float", "double");
        merge <- Seq(false, true); exists <- Seq(false, true)) {
      val values = "VALUES (0), (1), (2) AS input(id)"
      val sketch = s"kll_sketch_agg_$valueType(CAST(id AS $valueType))"
      val aggregate = if (merge) s"kll_merge_agg_$valueType(s)" else sketch
      val from = if (merge) s"(SELECT $sketch AS s FROM $values GROUP BY id)" else values
      val count = s"SELECT kll_sketch_get_n_$valueType($aggregate) FROM $from"
      val condition = if (exists) s"EXISTS (SELECT * FROM ($count) q)" else s"l.k IN ($count)"
      withSQLConf(SQLConf.SQL_ASOF_JOIN_ENABLED.key -> "true",
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString,
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
          SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false") {
        QueryTest.checkAnswer(sql(s"""SELECT l.k, r.k FROM VALUES (3), (4) AS l(k)
            |ASOF JOIN VALUES (0), (1) AS r(k) MATCH_CONDITION (l.k >= r.k)
            |ON $condition""".stripMargin),
          if (exists) Seq(Row(3, 1), Row(4, 1)) else Seq(Row(3, 1)), checkToRDD = false)
      }
    }
  }

  test("strict ASOF ON rejects unrelated scalar and direct randomness") {
    withSQLConf(SQLConf.SQL_ASOF_JOIN_ENABLED.key -> "true",
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true",
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
        SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false") {
      for (condition <- Seq("l.k = r.k + (SELECT rand())", s"l.k + rand() = r.k + $scalar",
          "l.k IN (SELECT rand())", "EXISTS (SELECT rand())",
          s"l.k + rand() IN (SELECT $scalar)")) {
        val error = intercept[AnalysisException] {
          sql(s"""SELECT l.k FROM VALUES (3), (4) AS l(k)
              |ASOF JOIN VALUES (0), (1) AS r(k) MATCH_CONDITION (l.k >= r.k)
              |ON $condition""".stripMargin)
        }
        assert(error.getCondition == "INVALID_NON_DETERMINISTIC_EXPRESSIONS")
      }
    }
  }

  test("KLL ASOF tuple IN validates randomness in every operand") {
    for (singlePass <- Seq(false, true)) {
      withSQLConf(SQLConf.SQL_ASOF_JOIN_ENABLED.key -> "true",
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString,
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
          SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false") {
        // VALUES is supported by both analyzers; strict resolution does not yet support range().
        val query = """SELECT l.k FROM VALUES (3), (4) AS l(k)
            |ASOF JOIN VALUES (0), (1) AS r(k) MATCH_CONDITION (l.k >= r.k)
            |ON (CAST(l.k AS DOUBLE), rand(0)) IN
            |(SELECT CAST(kll_sketch_get_n_bigint(kll_sketch_agg_bigint(id)) AS DOUBLE), 0D
            | FROM VALUES (0L), (1L), (2L) AS t(id))""".stripMargin
        val error = intercept[AnalysisException](sql(query).queryExecution.analyzed)
        assert(error.getCondition == "INVALID_NON_DETERMINISTIC_EXPRESSIONS")
        sql(query.replace("rand(0)", "0D")).queryExecution.assertAnalyzed()
      }
    }
  }

  test("user-aliased independent KLL scalars retain partition pruning") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
      withTempPath { path =>
        spark.range(8).selectExpr("id", "CAST(id % 2 AS INT) AS p")
          .write.partitionBy("p").parquet(path.getAbsolutePath)
        withTempView("kll_user_pruning") {
          spark.read.parquet(path.getAbsolutePath).createOrReplaceTempView("kll_user_pruning")
          val query = sql(s"""SELECT p, n FROM
              |(SELECT p, $scalar AS n FROM kll_user_pruning) q WHERE p = 1""".stripMargin)
          val scans = query.queryExecution.sparkPlan.collect {
            case scan: FileSourceScanExec => scan
          }
          assert(scans.size == 1)
          assert(scans.head.selectedPartitions.filePartitionIterator
            .map(_.values.getInt(0)).toSet == Set(1), query.queryExecution.toString)
          checkAnswer(query, Seq.fill(4)(Row(1, 3L)))
        }
      }
    }
  }

  test("mixed KLL and seeded row randomness retains the pre-filter draw sequence") {
    withUserDefinedFunction("mixed_kll_bool" -> true) {
      spark.udf.register("mixed_kll_bool", (b: java.lang.Boolean) => b)
      val argument = s"mixed_kll_bool(CASE WHEN id % 2 = 0 THEN $scalar > 0 " +
        "ELSE rand(17) > 0.5 END)"
      val materialized = spark.range(0, 128, 1, 1)
        .selectExpr("id % 3 AS p", s"$argument AS b").cache()
      try {
        materialized.count()
        val expected = materialized.where("p = 1").groupBy("p", "b").count().collect().toSeq
        checkAnswer(sql(s"""SELECT id % 3 AS p, $argument AS b, count(*)
            |FROM range(0, 128, 1, 1) GROUP BY 1, 2 HAVING p = 1""".stripMargin), expected)
      } finally {
        materialized.unpersist()
      }
    }
  }

  test("KLL ASOF ON scalars retain analyzer parity") {
    for (singlePass <- Seq(false, true); valueType <- Seq("bigint", "float", "double");
        merge <- Seq(false, true)) {
      val sketch = s"kll_sketch_agg_$valueType(CAST(id AS $valueType))"
      val aggregate = if (merge) s"kll_merge_agg_$valueType(s)" else sketch
      val values = "VALUES (0), (1), (2) AS input(id)"
      val from = if (merge) s"(SELECT $sketch AS s FROM $values GROUP BY id)" else values
      val count = s"(SELECT kll_sketch_get_n_$valueType($aggregate) FROM $from)"
      withSQLConf(SQLConf.SQL_ASOF_JOIN_ENABLED.key -> "true",
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString,
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
          SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false") {
        val query = sql(s"""SELECT l.k, r.k FROM VALUES (3), (4) AS l(k)
            |ASOF JOIN VALUES (0), (1) AS r(k) MATCH_CONDITION (l.k >= r.k)
            |ON l.k = r.k + $count""".stripMargin)
        QueryTest.checkAnswer(query, Seq(Row(3, 0), Row(4, 1)), checkToRDD = false)
      }
    }
  }

  test("Boolean short circuiting retains conditional random UDF dependencies") {
    withUserDefinedFunction("bool_identity" -> true) {
      spark.udf.register("bool_identity", (b: java.lang.Boolean, n: java.lang.Long) => b)
      for (codegen <- Seq("CODEGEN_ONLY", "NO_CODEGEN");
          condition <- Seq(
            "id = 0 OR array_contains(shuffle(array(1D / id, 2D)), 2D)",
            "id <> 0 AND array_contains(shuffle(array(1D / id, 2D)), 2D)")) {
        withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> codegen,
            SQLConf.ANSI_ENABLED.key -> "true") {
          val query = sql(s"""SELECT count(*) FROM range(2)
              |GROUP BY bool_identity($condition, $scalar)""".stripMargin)
          val staged = query.queryExecution.analyzed.collect {
            case project: org.apache.spark.sql.catalyst.plans.logical.Project =>
              project.projectList.exists(_.exists {
                case udf: org.apache.spark.sql.catalyst.expressions.ScalaUDF =>
                  udf.udfName.contains("bool_identity") &&
                    udf.children.last.isInstanceOf[
                      org.apache.spark.sql.catalyst.expressions.Attribute]
                case _ => false
              })
          }
          assert(staged.contains(true), query.queryExecution.toString)
          val expected = if (condition.startsWith("id =")) Seq(Row(2L))
            else Seq(Row(1L), Row(1L))
          checkAnswer(query, expected)
        }
      }
    }
  }

  test("KLL-only correlated lateral queries retain per-key results") {
    for (legacy <- Seq(true, false); valueType <- Seq("bigint", "float", "double");
        merge <- Seq(false, true)) {
      withSQLConf("spark.sql.legacy.kllAggregateDeterministic.enabled" -> legacy.toString) {
        val sketch = s"kll_sketch_agg_$valueType(CAST(i.id AS $valueType))"
        val aggregate = if (merge) s"kll_merge_agg_$valueType(s)" else sketch
        val values = "VALUES (0), (1) AS i(id) WHERE i.id = o.id"
        val from = if (merge) s"(SELECT $sketch AS s FROM $values GROUP BY i.id)" else values
        def query: DataFrame = sql(s"""SELECT o.id, q.n FROM VALUES (0), (0), (1), (9) AS o(id),
            |LATERAL (SELECT kll_sketch_get_n_$valueType($aggregate) AS n
            |FROM $from) q""".stripMargin)
        if (legacy) {
          checkAnswer(query, Seq(Row(0, 1L), Row(0, 1L), Row(1, 1L), Row(9, 0L)))
        } else {
          val error = intercept[AnalysisException](query.queryExecution.analyzed)
          assert(error.getCondition == "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY." +
            "NON_DETERMINISTIC_LATERAL_SUBQUERIES")
        }
      }
    }
    val outer = spark.range(1).as("o")
    val inner = spark.range(2).toDF().as("i").where($"i.id" === $"o.id".outer())
      .select(expr("kll_sketch_get_n_bigint(kll_sketch_agg_bigint(id))").as("n"))
    checkAnswer(outer.lateralJoin(inner), Seq(Row(0L, 1L)))
  }

  test("conditional Boolean UDF grouping preserves seeded draw and NULL behavior") {
    withUserDefinedFunction("nullable_bool_identity" -> true) {
      spark.udf.register("nullable_bool_identity", (b: java.lang.Boolean) => b)
      for (codegen <- Seq("CODEGEN_ONLY", "NO_CODEGEN"); operator <- Seq("AND", "OR")) {
        withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> codegen) {
          val left = "CASE WHEN id % 3 = 0 THEN CAST(NULL AS BOOLEAN) ELSE id % 2 = 0 END"
          val expression = s"nullable_bool_identity(($left) $operator (rand(7) > 0.5))"
          val expected = spark.range(0, 128, 1, 1).selectExpr(s"$expression AS b")
            .groupBy("b").count().collect().toSeq
          checkAnswer(sql(s"""SELECT $expression, count(*) FROM range(0, 128, 1, 1)
              |GROUP BY 1""".stripMargin), expected)
        }
      }
    }
  }

  test("compiler KLL bindings retain throwable conjunct ordering") {
    checkAnswer(sql(s"""SELECT id, $scalar AS n, count(*)
        |FROM VALUES (0), (4) AS t(id) GROUP BY 1, 2
        |HAVING n < id AND IF(id = 0, raise_error('bad row'), id) > 0""".stripMargin),
      Row(4, 3L, 1L))
  }

  test("correlated scalars retain independent KLL CTE scopes") {
    for (legacy <- Seq(true, false); legacyCount <- Seq(false, true);
        (aggregate, having, unmatched, matched) <- Seq(
          ("max(a.n + b.n)", "", null, 12L),
          ("sum(a.n + b.n)", "", null, 12L),
          ("count(*)", "", 0L, 1L),
          ("count(*)", "HAVING count(*) > 0", null, 1L),
          ("count(*)", "HAVING count(*) = 0", 0L, null))) {
      withSQLConf("spark.sql.legacy.kllAggregateDeterministic.enabled" -> legacy.toString,
          SQLConf.DECORRELATE_SUBQUERY_LEGACY_INCORRECT_COUNT_HANDLING_ENABLED.key ->
            legacyCount.toString,
          SQLConf.DECORRELATE_INNER_QUERY_ENABLED.key -> (!legacyCount).toString) {
        checkAnswer(sql(s"""SELECT o.id, (WITH c AS
            |(SELECT kll_sketch_get_n_bigint(kll_sketch_agg_bigint(id)) AS n FROM range(3)),
            |d AS (SELECT a.n + b.n AS n FROM c a CROSS JOIN c b)
            |SELECT $aggregate FROM d a CROSS JOIN d b WHERE a.n = o.id $having) AS n
            |FROM VALUES (0), (6) AS o(id)""".stripMargin),
          Seq(Row(0, unmatched), Row(6, matched)))
      }
    }
  }

  test("invalid direct KLL grouping retains a structured diagnostic") {
    for (valueType <- Seq("bigint", "float", "double"); merge <- Seq(false, true);
        wrapped <- Seq(false, true)) {
      val sketch = s"kll_sketch_agg_$valueType(CAST(id AS $valueType))"
      val aggregate = if (merge) s"kll_merge_agg_$valueType(s)" else sketch
      val from = if (merge) s"(SELECT $sketch AS s FROM range(3) GROUP BY id)" else "range(3)"
      val grouping = if (wrapped) s"kll_sketch_get_n_$valueType($aggregate)" else aggregate
      val error = intercept[AnalysisException] {
        sql(s"SELECT count(*) FROM $from GROUP BY $grouping").collect()
      }
      assert(error.getCondition == "GROUP_BY_AGGREGATE", error.toString)
    }
  }

  test("KLL scalar placement keeps a selective inner join ahead of a Cartesian product") {
    withTempView("kll_a", "kll_b", "kll_c") {
      spark.sparkContext.parallelize(0L until 100L).toDF("id")
        .createOrReplaceTempView("kll_a")
      spark.sparkContext.parallelize(0L until 100L).toDF("id")
        .createOrReplaceTempView("kll_b")
      spark.sparkContext.parallelize(Seq(3L)).toDF("id")
        .createOrReplaceTempView("kll_c")
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
        val query = sql(s"""SELECT a.id, b.id FROM kll_a a CROSS JOIN kll_b b
            |JOIN kll_c c ON a.id = c.id AND b.id + $scalar = c.id""".stripMargin)
        assert(!query.queryExecution.executedPlan.exists(_.nodeName == "CartesianProduct"),
          query.queryExecution.toString)
        checkAnswer(query, Row(3L, 0L))
      }
    }
  }

  test("KLL scalar placement crosses a pruned random projection") {
    withTempView("kll_a", "kll_b", "kll_c") {
      spark.sparkContext.parallelize(0L until 100L).toDF("a_id")
        .createOrReplaceTempView("kll_a")
      spark.sparkContext.parallelize(0L until 100L).toDF("b_id")
        .createOrReplaceTempView("kll_b")
      spark.sparkContext.parallelize(Seq(3L)).toDF("c_id")
        .createOrReplaceTempView("kll_c")
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
        val query = sql(s"""SELECT a_id, b_id FROM
            |(SELECT a_id, b_id, rand(0) AS unused FROM kll_a CROSS JOIN kll_b) ab
            |JOIN kll_c c ON a_id = c_id AND b_id + $scalar = c_id""".stripMargin)
        assert(!query.queryExecution.executedPlan.exists(_.nodeName == "CartesianProduct"),
          query.queryExecution.toString)
        checkAnswer(query, Row(3L, 0L))
      }
    }
  }

  test("KLL scalar placement keeps a conditional join component visible to CBO") {
    withTempView("kll_cbo_a", "kll_cbo_b", "kll_cbo_c") {
      val a = spark.sparkContext.parallelize(0L until 100L).toDF("a_key").cache()
      val b = spark.sparkContext.parallelize((0L until 1000L).map(i => (i, i % 100)))
        .toDF("b_id", "b_key").cache()
      val c = spark.sparkContext.parallelize(Seq(3L)).toDF("c_key").cache()
      try {
        a.count()
        b.count()
        c.count()
        a.createOrReplaceTempView("kll_cbo_a")
        b.createOrReplaceTempView("kll_cbo_b")
        c.createOrReplaceTempView("kll_cbo_c")
        withSQLConf(SQLConf.CBO_ENABLED.key -> "true",
            SQLConf.JOIN_REORDER_ENABLED.key -> "true",
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
          for (wrapped <- Seq(false, true)) {
            val component = "kll_cbo_a JOIN kll_cbo_b ON a_key = b_key"
            val input = if (wrapped) s"(SELECT a_key, b_id, b_key FROM $component) ab"
              else component
            val query = sql(s"""SELECT a_key, b_id FROM $input
                |JOIN kll_cbo_c ON b_id + $scalar = c_key""".stripMargin)
            assert(query.queryExecution.optimizedPlan.exists {
              case join: org.apache.spark.sql.catalyst.plans.logical.Join =>
                val names = join.output.map(_.name).toSet
                names.contains("b_id") && names.contains("c_key") && !names.contains("a_key")
              case _ => false
            }, query.queryExecution.toString)
            checkAnswer(query, Row(0L, 0L))
          }
        }
      } finally {
        a.unpersist()
        b.unpersist()
        c.unpersist()
      }
    }
  }

  for (groupedScalar <- Seq(false, true)) {
    test(s"KLL aggregates retain selective pruning: groupedScalar=$groupedScalar") {
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
        withTempPath { path =>
          spark.range(8).selectExpr("id", "CAST(id % 2 AS INT) AS p")
            .write.partitionBy("p").parquet(path.getAbsolutePath)
          withTempView("kll_pruning") {
            spark.read.parquet(path.getAbsolutePath).createOrReplaceTempView("kll_pruning")
            val grouped = """(SELECT kll_sketch_get_n_bigint(kll_sketch_agg_bigint(id))
                |FROM range(6) GROUP BY id % 2 LIMIT 1)""".stripMargin
            val queryText = if (groupedScalar) {
              s"SELECT p, count(*) FROM kll_pruning GROUP BY p, $grouped HAVING p = 1"
            } else {
              "SELECT p, kll_sketch_get_n_bigint(kll_sketch_agg_bigint(id)) " +
                "FROM kll_pruning GROUP BY p HAVING p = 1"
            }
            val query = sql(queryText)
            val scans = query.queryExecution.sparkPlan.collect { case s: FileSourceScanExec => s }
            assert(scans.size == 1)
            assert(scans.head.selectedPartitions.filePartitionIterator
              .map(_.values.getInt(0)).toSet == Set(1), query.queryExecution.toString)
            checkAnswer(query, Row(1, 4L))
          }
        }
      }
    }
  }

  test("extracted UDFs preserve conditional nondeterministic evaluation") {
    val first = udf((a: Seq[Double]) => a.head)
    withUserDefinedFunction("branch_first" -> true) {
      spark.udf.register("branch_first", (a: Seq[Double]) => a.head)
      for (codegen <- Seq("CODEGEN_ONLY", "NO_CODEGEN")) {
        withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> codegen,
            SQLConf.ANSI_ENABLED.key -> "true") {
          val data = spark.range(2)
          val key = first(expr("IF(id = 0, array(0D), shuffle(array(1D / id, 2D)))"))
          assert(data.sort(key).collect().toSeq == Seq(0L, 1L))
          checkAnswer(sql("""SELECT count(*) FROM range(2) GROUP BY
              |branch_first(IF(id = 0, array(0D), shuffle(array(1D / id, 2D))))""".stripMargin),
            Seq(Row(1L), Row(1L)))
        }
      }
    }
  }

  test("boolean wrappers retain KLL scalar equi join keys") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      for (condition <- Seq(s"(l.id = r.id + $scalar) OR false",
          s"NOT NOT (l.id = r.id + $scalar)")) {
        val query = sql(s"""SELECT l.id, r.id FROM range(5) l JOIN range(2) r
            |ON $condition""".stripMargin)
        assert(query.queryExecution.executedPlan.exists {
          case _: BroadcastHashJoinExec | _: SortMergeJoinExec => true
          case _ => false
        }, query.queryExecution.toString)
        checkAnswer(query, Seq(Row(3L, 0L), Row(4L, 1L)))
      }
    }
  }

  test("observed aggregate wrappers accept an independent KLL scalar sibling") {
    val observation = Observation()
    val data = spark.range(2).observe("kll_scalar_metric", expr(s"count(*) + $scalar").as("n"))
      .observe(observation, expr(s"count(*) + $scalar").as("n"))
    data.collect()
    assert(observation.get == Map("n" -> 5L))
    assert(data.queryExecution.observedMetrics("kll_scalar_metric") == Row(5L))
  }

  test("KLL observed metrics reject an ordinary volatile scalar sibling") {
    val error = intercept[AnalysisException] {
      spark.range(2).observe("volatile_scalar_metric",
        expr("count(*) + (SELECT rand(7))").as("n")).queryExecution.analyzed
    }
    assert(error.getCondition ==
      "INVALID_OBSERVED_METRICS.NON_AGGREGATE_FUNC_ARG_IS_NON_DETERMINISTIC")
  }

  test("a shared KLL scalar preserves both SQL function equi join operands") {
    withUserDefinedFunction("kll_equal" -> true) {
      sql("""CREATE TEMPORARY FUNCTION kll_equal(a BIGINT, b BIGINT, s BIGINT)
          |RETURNS BOOLEAN RETURN a + s = b + s""".stripMargin)
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val query = sql(s"""SELECT l.id, r.id FROM range(3) l JOIN range(3) r
            |ON kll_equal(l.id, r.id, $scalar)""".stripMargin)
        assert(query.queryExecution.executedPlan.exists {
          case _: BroadcastHashJoinExec | _: SortMergeJoinExec => true
          case _ => false
        }, query.queryExecution.toString)
        checkAnswer(query, Seq(Row(0L, 0L), Row(1L, 1L), Row(2L, 2L)))
      }
    }
  }

  test("KLL IN bindings preserve early V2 selective partition pruning") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
      withTempPath { path =>
        spark.range(8).selectExpr("id", "CAST(id % 2 AS INT) AS p")
          .write.partitionBy("p").parquet(path.getAbsolutePath)
        withTempView("kll_v2_partitioned") {
          spark.read.parquet(path.getAbsolutePath).createOrReplaceTempView("kll_v2_partitioned")
          val query = sql(s"""SELECT p, count(*) FROM kll_v2_partitioned
              |GROUP BY p, id IN (SELECT $scalar) HAVING p = 1""".stripMargin)
          val scans = query.queryExecution.sparkPlan.collect {
            case scan: BatchScanExec if scan.scan.isInstanceOf[FileScan] =>
              scan.scan.asInstanceOf[FileScan]
          }
          assert(scans.size == 1)
          val scan = scans.head
          val selected = scan.fileIndex.listFiles(scan.partitionFilters, scan.dataFilters)
          assert(selected.size == 1 && selected.head.values.getInt(0) == 1,
            query.queryExecution.toString)
          checkAnswer(query, Seq(Row(1, 1L), Row(1, 3L)))
        }
      }
    }
  }

  test("random IN operands retain their input-row evaluation boundary") {
    val kllBooleans = s"SELECT $scalar = 3 AS b"
    for (codegen <- Seq("CODEGEN_ONLY", "NO_CODEGEN"); not <- Seq("", "NOT ");
        values <- Seq("SELECT true AS b", kllBooleans)) {
      withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> codegen) {
        val expected = sql(s"""SELECT $not (rand(0) >= 0.5) AS b
            |FROM range(0, 1000, 1, 2)""".stripMargin).groupBy("b").count()
          .select("count").collect().toSeq
        assert(expected.size == 2 && expected.forall(_.getLong(0) > 0))
        checkAnswer(sql(s"""SELECT count(*) FROM range(0, 1000, 1, 2)
            |GROUP BY (rand(0) >= 0.5) ${not}IN ($values)""".stripMargin), expected)
      }
    }
  }

  test("nested KLL predicate dependencies retain consumed Boolean results") {
    val nested = (1 to 4).foldLeft("id = 0") { (child, depth) =>
      val not = if (depth % 2 == 0) "NOT " else ""
      s"($child) ${not}IN (SELECT $scalar = 3)"
    }
    checkAnswer(sql(s"""SELECT $nested AS b, count(*)
        |FROM VALUES (0), (1), (2) AS t(id) GROUP BY b""".stripMargin),
      Seq(Row(true, 1L), Row(false, 2L)))
    checkAnswer(sql(s"""SELECT count(*) FILTER (WHERE $nested)
        |FROM VALUES (0), (1), (2) AS t(id)""".stripMargin), Row(1L))
  }

  test("boxed UDF chains stage transitive dependencies above KLL scalars") {
    withUserDefinedFunction("kll_inner" -> true, "kll_outer" -> true) {
      spark.udf.register("kll_inner", (n: java.lang.Long) => java.lang.Long.valueOf(n + 1L))
      spark.udf.register("kll_outer", (n: java.lang.Long) => java.lang.Long.valueOf(n + 2L))
      val query = spark.range(3).groupBy(expr(s"kll_outer(kll_inner($scalar))").as("n")).count()
      val projects = query.queryExecution.analyzed.collect {
        case project: org.apache.spark.sql.catalyst.plans.logical.Project => project
      }
      import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, ScalaUDF, ScalarSubquery}
      val bindings = projects.flatMap(_.projectList.collect {
        case alias: Alias if alias.isNondeterministicSubqueryBinding => alias
      })
      val inner = bindings.find(_.child match {
        case udf: ScalaUDF => udf.udfName.contains("kll_inner")
        case _ => false
      }).get
      val outer = bindings.find(_.child match {
        case udf: ScalaUDF => udf.udfName.contains("kll_outer")
        case _ => false
      }).get
      val scalarBinding = bindings.find(_.child.isInstanceOf[ScalarSubquery]).get
      assert(inner.child.children.head.asInstanceOf[Attribute].exprId == scalarBinding.exprId)
      assert(outer.child.children.head.asInstanceOf[Attribute].exprId == inner.exprId)
      assert(projects.forall(p => p.projectList.forall(_.references.subsetOf(p.child.outputSet))))
      checkAnswer(query, Row(6L, 3L))
    }
  }

  test("tuple KLL IN rejects direct randomness in later operands") {
    for (singlePass <- Seq(false, true); filter <- Seq(false, true); not <- Seq("", "NOT ")) {
      withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString,
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
          SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false") {
        val predicate = s"(CAST($scalar AS DOUBLE), rand(0)) ${not}IN " +
          s"(SELECT CAST($scalar AS DOUBLE), CAST($scalar AS DOUBLE))"
        val aggregate = if (filter) s"count(*) FILTER (WHERE $predicate)"
          else s"sum(CAST($predicate AS INT))"
        val error = intercept[AnalysisException] {
          sql(s"SELECT $aggregate FROM VALUES (0), (1) AS t(id)")
        }
        assert(error.getCondition == (if (filter) "INVALID_AGGREGATE_FILTER.NON_DETERMINISTIC"
          else "AGGREGATE_FUNCTION_WITH_NONDETERMINISTIC_EXPRESSION"))
      }
    }
  }

  test("kll scalars in lateral ON preserve matched and unmatched rows") {
    for (tentative <- Seq(false, true); join <- Seq("JOIN", "LEFT JOIN");
        dataType <- Seq("bigint", "float", "double"); merge <- Seq(false, true)) {
      val sketch = s"kll_sketch_agg_$dataType(CAST(id AS $dataType), 8)"
      val values = "VALUES (0), (1), (2) AS t(id)"
      val aggregate = if (merge) s"kll_merge_agg_$dataType(s, 8)" else sketch
      val source = if (merge) s"(SELECT $sketch AS s FROM $values GROUP BY id)" else values
      val n = s"(SELECT kll_sketch_get_n_$dataType($aggregate) FROM $source)"
      withSQLConf(
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "false",
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> tentative.toString) {
        val expected = if (join == "JOIN") Seq(Row(0, 1)) else Seq(Row(0, 1), Row(3, null))
        checkAnswer(sql(s"""SELECT o.id, l.x FROM VALUES (0), (3) AS o(id)
            |$join LATERAL (SELECT 1 AS x) l ON o.id < $n""".stripMargin), expected)
      }
    }
  }

  test("lateral ON retains direct and non-KLL randomness restrictions") {
    Seq(s"o.id < $scalar + rand(0)", "o.id < (SELECT rand(0))").foreach { condition =>
      val error = intercept[AnalysisException] {
        sql(s"""SELECT * FROM VALUES (0), (1) AS o(id)
            |JOIN LATERAL (SELECT 1 AS x) l ON $condition""".stripMargin)
      }
      assert(error.getCondition ==
        "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.LATERAL_JOIN_CONDITION_NON_DETERMINISTIC")
    }
  }

  test("kll mixed random aggregate diagnostics agree across analyzers") {
    val errors = Seq(false, true).map { singlePass =>
      withSQLConf(
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString,
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
          SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false") {
        intercept[AnalysisException] {
          sql(s"SELECT sum(rand(0) + $scalar) FROM VALUES (0), (1) AS o(id)")
        }
      }
    }
    assert(errors.map(_.getCondition).distinct ==
      Seq("AGGREGATE_FUNCTION_WITH_NONDETERMINISTIC_EXPRESSION"))
    assert(errors.map(_.getMessageParameters.asScala.toMap).distinct.size == 1)
  }

  test("kll predicate analysis rejects direct randomness inside IN operands") {
    for (singlePass <- Seq(false, true); join <- Seq(false, true); not <- Seq("", "NOT ")) {
      val predicate = s"rand(0) ${not}IN (SELECT CAST($scalar AS DOUBLE))"
      val query = if (join) {
        s"SELECT * FROM VALUES (0) AS l(id) JOIN VALUES (0) AS r(id) ON $predicate"
      } else {
        s"SELECT count(*) FILTER (WHERE $predicate) FROM VALUES (0), (1) AS o(id)"
      }
      withSQLConf(
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString,
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
          SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false") {
        val error = intercept[AnalysisException] { sql(query) }
        assert(error.getCondition == (if (join) "INVALID_NON_DETERMINISTIC_EXPRESSIONS"
          else "INVALID_AGGREGATE_FILTER.NON_DETERMINISTIC"))
      }
    }
  }

  test("kll join scalar staging preserves independent partition pruning") {
    withTempPath { path =>
      spark.range(8).selectExpr("id", "CAST(id % 2 AS INT) AS p")
        .write.partitionBy("p").parquet(path.getAbsolutePath)
      withTempView("kll_partitioned") {
        spark.read.parquet(path.getAbsolutePath).createOrReplaceTempView("kll_partitioned")
        for (predicateInOn <- Seq(false, true)) {
          val predicate = "r.p = 1"
          val query = sql(s"""SELECT r.id FROM VALUES (1L), (3L) AS l(id)
              |JOIN kll_partitioned r ON l.id = r.id AND r.id < $scalar
              |${if (predicateInOn) s"AND $predicate" else s"WHERE $predicate"}
              |""".stripMargin)
          val scans = query.queryExecution.sparkPlan.collect {
            case scan: FileSourceScanExec => scan
          }
          assert(scans.size == 1)
          assert(scans.head.partitionFilters.nonEmpty, query.queryExecution.toString)
          val partitions = scans.head.selectedPartitions.filePartitionIterator
            .map(_.values.getInt(0)).toSet
          assert(partitions == Set(1),
            query.queryExecution.toString)
          checkAnswer(query, Row(1L))
        }
      }
    }
  }
}
